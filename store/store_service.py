"""
Store Service - FIXED VERSION with EWMA Access Tracking

Critical fixes:
1. GC queries leader directly for consistent reads
2. Compaction blocks writes during critical section
3. Store registration retries indefinitely until successful
4. Request ID tracking for debugging
5. EWMA-based sliding window access tracking

IMPROVEMENTS:
- Sliding window instead of tumbling window
- EWMA smoothing for stable rate calculation
- Per-photo event tracking with timestamps
- Automatic old event pruning
"""

from fastapi import FastAPI, File, UploadFile, Form, HTTPException, BackgroundTasks, Request
from fastapi.responses import Response
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import uvicorn
import os
import struct
import hashlib
import time
import json
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import deque, defaultdict
import requests
import logging
import zlib
import redis
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] %(message)s'
)

logger = logging.getLogger("StoreService")

# Configuration
STORE_ID = os.getenv("STORE_ID", "store-1")
STORE_URL = os.getenv("STORE_URL", "http://store-1:8000")
VOLUME_DIRECTORY = os.getenv("VOLUME_DIRECTORY", "/data/volumes")
MAX_VOLUME_SIZE = int(os.getenv("MAX_VOLUME_SIZE", "4294967296"))  # 4GB
DIRECTORY_SERVICE_URL = os.getenv("DIRECTORY_SERVICE_URL", "http://nginx")
REPLICATION_MANAGER_URL = os.getenv("REPLICATION_MANAGER_URL", "http://replication:9003")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
GC_INTERVAL = int(os.getenv("GC_INTERVAL", "21600"))  # 6 hours
COMPACTION_INTERVAL = int(os.getenv("COMPACTION_INTERVAL", "3600"))  # 1 hour
COMPACTION_EFFICIENCY_THRESHOLD = float(os.getenv("COMPACTION_EFFICIENCY_THRESHOLD", "0.60"))

# NEW: Access tracking configuration
ACCESS_WINDOW_SECONDS = int(os.getenv("ACCESS_WINDOW_SECONDS", "300"))  # 5 minutes
EWMA_ALPHA = float(os.getenv("EWMA_ALPHA", "0.3"))  # Smoothing factor
STATS_REPORT_INTERVAL = int(os.getenv("STATS_REPORT_INTERVAL", "60"))  # 60 seconds

# Initialize Redis client (for push-on-write caching)
try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=False, socket_timeout=2)
    redis_client.ping()
    logger.info("Connected to Redis successfully")
except Exception as e:
    logger.warning(f"Redis connection failed: {e}. Push-on-write caching disabled.")
    redis_client = None

# Create FastAPI app
app = FastAPI(title="Store Service")

# Rate limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Global state
volumes: Dict[str, 'Volume'] = {}
photo_metadata: Dict[str, Tuple[str, int]] = {}  # photo_id -> (volume_id, offset)
current_volume_id = 0
compaction_in_progress = False
compaction_lock = threading.Lock()

# ============================================================================
# NEW: Access Tracking System with EWMA
# ============================================================================

@dataclass
class AccessEvent:
    """Single access event with timestamp"""
    timestamp: float
    count: int = 1
    
    def __repr__(self):
        return f"AccessEvent(ts={self.timestamp:.2f}, count={self.count})"


class AccessTracker:
    """
    Thread-safe access tracking with sliding window and EWMA smoothing.
    
    Algorithm:
    1. Records each access with timestamp
    2. Maintains sliding window (prunes events older than window_seconds)
    3. Calculates raw rate from events in window
    4. Applies EWMA smoothing: EWMA_t = α * raw_t + (1-α) * EWMA_{t-1}
    """
    
    def __init__(self, window_seconds: int = 300, alpha: float = 0.3):
        self.window_seconds = window_seconds
        self.alpha = alpha
        
        # Core data structures
        self.events: Dict[str, deque] = {}
        self.ewma_rates: Dict[str, float] = {}
        self.running_sums: Dict[str, int] = {}  # Optimization: cached sum
        self.last_update: Dict[str, float] = {}
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Metrics
        self.total_events_recorded = 0
        self.total_events_pruned = 0
        self.photos_tracked = 0
        
        logger.info(f"AccessTracker initialized: window={window_seconds}s, alpha={alpha}")
    
    def record_access(self, photo_id: str, count: int = 1) -> None:
        """Record an access event for a photo"""
        now = time.time()
        
        with self.lock:
            # Initialize tracking for new photo
            if photo_id not in self.events:
                self.events[photo_id] = deque()
                self.ewma_rates[photo_id] = 0.0
                self.running_sums[photo_id] = 0
                self.last_update[photo_id] = now
                self.photos_tracked += 1
            
            # Record event
            self.events[photo_id].append(AccessEvent(timestamp=now, count=count))
            self.running_sums[photo_id] += count
            self.last_update[photo_id] = now
            self.total_events_recorded += count
            
            # Prune old events
            self._prune_old_events(photo_id, now)
            
            # Update EWMA
            self._update_ewma(photo_id, now)
    
    def _prune_old_events(self, photo_id: str, current_time: float) -> None:
        """Remove events outside the sliding window"""
        cutoff = current_time - self.window_seconds
        events = self.events[photo_id]
        
        pruned_count = 0
        while events and events[0].timestamp < cutoff:
            event = events.popleft()
            self.running_sums[photo_id] -= event.count
            pruned_count += 1
        
        if pruned_count > 0:
            self.total_events_pruned += pruned_count
        
        # Cleanup: Remove photo if no events in window
        if not events:
            del self.events[photo_id]
            del self.ewma_rates[photo_id]
            del self.running_sums[photo_id]
            del self.last_update[photo_id]
            self.photos_tracked -= 1
    
    def _update_ewma(self, photo_id: str, current_time: float) -> None:
        """Calculate and update EWMA rate"""
        events = self.events[photo_id]
        
        if not events:
            self.ewma_rates[photo_id] = 0.0
            return
        
        # Use cached sum for efficiency
        total_accesses = self.running_sums[photo_id]
        
        # Calculate window duration
        window_start = events[0].timestamp
        window_duration_seconds = current_time - window_start
        window_duration_minutes = max(window_duration_seconds / 60.0, 0.01)
        
        # Raw rate
        raw_rate = total_accesses / window_duration_minutes
        
        # Apply EWMA smoothing
        old_ewma = self.ewma_rates[photo_id]
        new_ewma = (self.alpha * raw_rate) + ((1 - self.alpha) * old_ewma)
        self.ewma_rates[photo_id] = new_ewma
    
    def get_stats_for_reporting(self) -> List[dict]:
        """Generate statistics snapshot for replication manager"""
        with self.lock:
            now = time.time()
            stats = []
            
            for photo_id in list(self.events.keys()):
                self._prune_old_events(photo_id, now)
                
                events = self.events.get(photo_id)
                if not events:
                    continue
                
                self._update_ewma(photo_id, now)
                
                total_accesses = self.running_sums[photo_id]
                window_start = events[0].timestamp
                window_end = now
                window_duration_minutes = max((window_end - window_start) / 60.0, 0.01)
                
                raw_rate = total_accesses / window_duration_minutes
                
                stats.append({
                    'photo_id': photo_id,
                    'ewma_rate_per_minute': round(self.ewma_rates[photo_id], 2),
                    'raw_rate_per_minute': round(raw_rate, 2),
                    'total_accesses_in_window': total_accesses,
                    'window_start': window_start,
                    'window_end': window_end,
                    'window_duration_seconds': round(window_end - window_start, 2),
                    'event_count': len(events)
                })
            
            logger.info(f"Generated stats for {len(stats)} photos "
                       f"(tracked={self.photos_tracked}, recorded={self.total_events_recorded})")
            
            return stats
    
    def get_current_rate(self, photo_id: str) -> float:
        """Get current EWMA rate for a specific photo"""
        with self.lock:
            return self.ewma_rates.get(photo_id, 0.0)
    
    def get_tracker_stats(self) -> dict:
        """Get internal tracker statistics"""
        with self.lock:
            return {
                'photos_tracked': self.photos_tracked,
                'total_events_recorded': self.total_events_recorded,
                'total_events_pruned': self.total_events_pruned,
                'window_seconds': self.window_seconds,
                'alpha': self.alpha
            }


# Global access tracker instance
access_tracker = AccessTracker(
    window_seconds=ACCESS_WINDOW_SECONDS,
    alpha=EWMA_ALPHA
)

# ============================================================================
# Volume Management (Needle-based storage)
# ============================================================================

class Needle:
    """
    Haystack needle format:
    [Header][Photo ID][Photo Data][Checksum]
    
    Header: magic(4) + photo_id_len(4) + flags(1) + timestamp(8) + size(4)
    """
    MAGIC_NUMBER = 0xDEADBEEF
    HEADER_SIZE = 4 + 4 + 1 + 8 + 4  # 21 bytes
    
    def __init__(self, photo_id: str, photo_data: bytes, flags: int = 0):
        self.photo_id = photo_id
        self.photo_data = photo_data
        self.flags = flags
        self.timestamp = time.time()
    
    def serialize(self) -> bytes:
        """Serialize needle to bytes"""
        photo_id_bytes = self.photo_id.encode('utf-8')
        photo_id_len = len(photo_id_bytes)
        data_size = len(self.photo_data)
        
        header = struct.pack(
            '>I I B Q I',
            self.MAGIC_NUMBER,
            photo_id_len,
            self.flags,
            int(self.timestamp),
            data_size
        )
        
        needle_bytes = header + photo_id_bytes + self.photo_data
        checksum = struct.pack('>I', zlib.crc32(needle_bytes) & 0xFFFFFFFF)
        
        return needle_bytes + checksum
    
    @classmethod
    def deserialize(cls, data: bytes, offset: int = 0) -> 'Needle':
        """Deserialize needle from bytes"""
        # Parse header
        header = struct.unpack('>I I B Q I', data[offset:offset + cls.HEADER_SIZE])
        magic, photo_id_len, flags, timestamp, data_size = header
        
        if magic != cls.MAGIC_NUMBER:
            raise ValueError(f"Invalid magic number: {magic:x}")
        
        # Extract photo ID and data
        photo_id_start = offset + cls.HEADER_SIZE
        photo_id = data[photo_id_start:photo_id_start + photo_id_len].decode('utf-8')
        
        data_start = photo_id_start + photo_id_len
        photo_data = data[data_start:data_start + data_size]
        
        # Verify checksum
        checksum_offset = data_start + data_size
        expected_checksum = struct.unpack('>I', data[checksum_offset:checksum_offset + 4])[0]
        actual_checksum = zlib.crc32(data[offset:checksum_offset]) & 0xFFFFFFFF
        
        if expected_checksum != actual_checksum:
            raise ValueError(f"Checksum mismatch for {photo_id}")
        
        needle = cls(photo_id, photo_data, flags)
        needle.timestamp = timestamp
        return needle
    
    def total_size(self) -> int:
        """Calculate total serialized size"""
        return self.HEADER_SIZE + len(self.photo_id.encode('utf-8')) + len(self.photo_data) + 4


class Volume:
    """Append-only volume with needle-based storage"""
    
    FLAG_DELETED = 0x01
    
    def __init__(self, volume_id: str, directory: str, max_size: int):
        self.volume_id = volume_id
        self.directory = Path(directory)
        self.max_size = max_size
        self.file_path = self.directory / f"{volume_id}.dat"
        self.index: Dict[str, int] = {}  # photo_id -> offset
        self.current_size = 0
        self.lock = threading.Lock()
        
        # Create directory and file
        self.directory.mkdir(parents=True, exist_ok=True)
        if not self.file_path.exists():
            self.file_path.touch()
        
        # Build index from existing data
        self._build_index()
        
        logger.info(f"Volume {volume_id} initialized: {len(self.index)} photos, {self.current_size} bytes")
    
    def _build_index(self):
        """Build in-memory index from volume file"""
        if self.file_path.stat().st_size == 0:
            return
        
        with open(self.file_path, 'rb') as f:
            offset = 0
            while True:
                # Read header
                header_data = f.read(Needle.HEADER_SIZE)
                if len(header_data) < Needle.HEADER_SIZE:
                    break
                
                magic, photo_id_len, flags, timestamp, data_size = struct.unpack(
                    '>I I B Q I', header_data
                )
                
                if magic != Needle.MAGIC_NUMBER:
                    logger.error(f"Invalid magic at offset {offset}, stopping index build")
                    break
                
                # Read photo ID
                photo_id = f.read(photo_id_len).decode('utf-8')
                
                # Skip photo data and checksum
                f.seek(data_size + 4, 1)
                
                # Index non-deleted photos
                if not (flags & self.FLAG_DELETED):
                    self.index[photo_id] = offset
                else:
                    # Remove deleted photos from index
                    self.index.pop(photo_id, None)
                
                # Update offset
                needle_size = Needle.HEADER_SIZE + photo_id_len + data_size + 4
                offset += needle_size
            
            self.current_size = offset
    
    def write(self, photo_id: str, photo_data: bytes) -> int:
        """Write photo to volume, returns offset"""
        with self.lock:
            # Check if already exists
            if photo_id in self.index:
                # Idempotent write: verify it's the same data
                existing_needle = self._read_needle_at_offset(self.index[photo_id])
                existing_checksum = hashlib.md5(existing_needle.photo_data).hexdigest()
                new_checksum = hashlib.md5(photo_data).hexdigest()
                
                if existing_checksum == new_checksum:
                    logger.info(f"Idempotent write for {photo_id}, returning existing offset")
                    return self.index[photo_id]
                else:
                    raise ValueError(f"Photo {photo_id} exists with different content")
            
            # Create needle
            needle = Needle(photo_id, photo_data)
            serialized = needle.serialize()
            
            # Check capacity
            if self.current_size + len(serialized) > self.max_size:
                raise Exception("Volume full")
            
            # Write to file
            with open(self.file_path, 'ab') as f:
                offset = self.current_size
                f.write(serialized)
                f.flush()
                os.fsync(f.fileno())
            
            # Update index
            self.index[photo_id] = offset
            self.current_size += len(serialized)
            
            logger.info(f"Wrote {photo_id} at offset {offset} ({len(photo_data)} bytes)")
            return offset
    
    def read(self, photo_id: str) -> bytes:
        """Read photo from volume"""
        with self.lock:
            if photo_id not in self.index:
                raise KeyError(f"Photo {photo_id} not found in volume {self.volume_id}")
            
            offset = self.index[photo_id]
            needle = self._read_needle_at_offset(offset)
            
            if needle.flags & self.FLAG_DELETED:
                raise KeyError(f"Photo {photo_id} is deleted")
            
            return needle.photo_data
    
    def _read_needle_at_offset(self, offset: int) -> Needle:
        """Read needle from specific offset"""
        with open(self.file_path, 'rb') as f:
            f.seek(offset)
            
            # Read enough data
            header = f.read(Needle.HEADER_SIZE)
            magic, photo_id_len, flags, timestamp, data_size = struct.unpack(
                '>I I B Q I', header
            )
            
            # Read full needle
            f.seek(offset)
            needle_size = Needle.HEADER_SIZE + photo_id_len + data_size + 4
            data = f.read(needle_size)
            
            return Needle.deserialize(data, 0)
    
    def mark_deleted(self, photo_id: str):
        """Mark photo as deleted (write tombstone)"""
        with self.lock:
            if photo_id not in self.index:
                return
            
            # Read original needle
            offset = self.index[photo_id]
            needle = self._read_needle_at_offset(offset)
            
            # Set deleted flag
            needle.flags |= self.FLAG_DELETED
            
            # Write back with deleted flag
            with open(self.file_path, 'r+b') as f:
                f.seek(offset + 8)  # Skip magic and photo_id_len
                f.write(struct.pack('B', needle.flags))
                f.flush()
                os.fsync(f.fileno())
            
            # Remove from index
            del self.index[photo_id]
            logger.info(f"Marked {photo_id} as deleted in volume {self.volume_id}")
    
    def get_efficiency(self) -> float:
        """Calculate storage efficiency (live data / total size)"""
        if self.current_size == 0:
            return 1.0
        
        live_size = sum(
            self._read_needle_at_offset(offset).total_size()
            for offset in self.index.values()
        )
        
        return live_size / self.current_size
    
    def compact(self) -> bool:
        """Compact volume by removing deleted needles"""
        global compaction_in_progress
        
        with compaction_lock:
            if compaction_in_progress:
                logger.warning("Compaction already in progress")
                return False
            
            compaction_in_progress = True
        
        try:
            logger.info(f"Starting compaction of volume {self.volume_id}")
            
            # Create new volume file
            new_file_path = self.file_path.with_suffix('.new')
            new_index = {}
            new_offset = 0
            
            with open(new_file_path, 'wb') as new_file:
                # Copy live needles
                for photo_id, old_offset in self.index.items():
                    needle = self._read_needle_at_offset(old_offset)
                    serialized = needle.serialize()
                    
                    new_file.write(serialized)
                    new_index[photo_id] = new_offset
                    new_offset += len(serialized)
                
                new_file.flush()
                os.fsync(new_file.fileno())
            
            # Atomic swap
            with self.lock:
                old_file = self.file_path.with_suffix('.old')
                self.file_path.rename(old_file)
                new_file_path.rename(self.file_path)
                
                self.index = new_index
                self.current_size = new_offset
                
                # Delete old file
                old_file.unlink()
            
            logger.info(f"Compaction completed: {len(new_index)} photos, {new_offset} bytes")
            return True
        
        except Exception as e:
            logger.error(f"Compaction failed: {e}", exc_info=True)
            return False
        
        finally:
            with compaction_lock:
                compaction_in_progress = False


def get_or_create_volume() -> Volume:
    """Get current volume or create new one if full"""
    global current_volume_id
    
    volume_id = f"volume-{current_volume_id:04d}"
    
    if volume_id not in volumes:
        volumes[volume_id] = Volume(volume_id, VOLUME_DIRECTORY, MAX_VOLUME_SIZE)
    
    volume = volumes[volume_id]
    
    # Check if full
    if volume.current_size >= MAX_VOLUME_SIZE * 0.95:
        logger.info(f"Volume {volume_id} is full, creating new volume")
        current_volume_id += 1
        return get_or_create_volume()
    
    return volume

# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "store_id": STORE_ID,
        "volumes": len(volumes),
        "photos": len(photo_metadata)
    }


@app.post("/write")
@limiter.limit("100/minute")
async def write_photo(
    photo: UploadFile = File(...),
    photo_id: str = Form(...),
    request: Request = None
):
    """Write photo to store"""
    request_id = str(uuid.uuid4())[:8]
    logger.info(f"[{request_id}] Write request for {photo_id}")
    
    # Check compaction
    with compaction_lock:
        if compaction_in_progress:
            raise HTTPException(status_code=503, detail="Volume compaction in progress")
    
    try:
        # Read photo data
        photo_data = await photo.read()
        checksum = hashlib.md5(photo_data).hexdigest()
        
        # Get volume
        volume = get_or_create_volume()
        
        # Write to volume
        offset = volume.write(photo_id, photo_data)
        
        # Update metadata
        photo_metadata[photo_id] = (volume.volume_id, offset)
        
        # Push to cache
        if redis_client:
            try:
                redis_client.setex(f"photo:{photo_id}", 3600, photo_data)
                logger.info(f"[{request_id}] Pushed {photo_id} to cache")
            except Exception as e:
                logger.warning(f"[{request_id}] Cache push failed: {e}")
        
        logger.info(f"[{request_id}] Successfully wrote {photo_id} to {volume.volume_id}")
        
        return {
            "success": True,
            "store_id": STORE_ID,
            "volume_id": volume.volume_id,
            "offset": offset,
            "checksum": checksum,
            "size_bytes": len(photo_data)
        }
    
    except Exception as e:
        logger.error(f"[{request_id}] Write failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/read/{photo_id}")
@limiter.limit("1000/minute")
async def read_photo(photo_id: str, request: Request):
    """Read photo from store"""
    request_id = str(uuid.uuid4())[:8]
    logger.info(f"[{request_id}] Read request for {photo_id}")
    
    if photo_id not in photo_metadata:
        logger.warning(f"[{request_id}] Photo {photo_id} not found")
        raise HTTPException(status_code=404, detail="Photo not found")
    
    volume_id, offset = photo_metadata[photo_id]
    volume = volumes.get(volume_id)
    
    if not volume:
        logger.error(f"[{request_id}] Volume {volume_id} not found")
        raise HTTPException(status_code=500, detail="Volume not found")
    
    try:
        photo_data = volume.read(photo_id)
        
        # Record access for tracking
        access_tracker.record_access(photo_id)
        
        logger.info(f"[{request_id}] Successfully read {photo_id} ({len(photo_data)} bytes)")
        
        return Response(content=photo_data, media_type="image/jpeg")
    
    except Exception as e:
        logger.error(f"[{request_id}] Read failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/delete/{photo_id}")
async def delete_photo(photo_id: str, request: Request):
    """Delete photo from store"""
    request_id = str(uuid.uuid4())[:8]
    logger.info(f"[{request_id}] Delete request for {photo_id}")
    
    if photo_id not in photo_metadata:
        raise HTTPException(status_code=404, detail="Photo not found")
    
    volume_id, offset = photo_metadata[photo_id]
    volume = volumes.get(volume_id)
    
    if not volume:
        raise HTTPException(status_code=500, detail="Volume not found")
    
    try:
        volume.mark_deleted(photo_id)
        del photo_metadata[photo_id]
        
        # Remove from cache
        if redis_client:
            try:
                redis_client.delete(f"photo:{photo_id}")
            except Exception as e:
                logger.warning(f"Cache delete failed: {e}")
        
        logger.info(f"[{request_id}] Successfully deleted {photo_id}")
        
        return {"success": True}
    
    except Exception as e:
        logger.error(f"[{request_id}] Delete failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
async def get_stats():
    """Get store statistics"""
    total_size = sum(v.current_size for v in volumes.values())
    total_photos = len(photo_metadata)
    
    volume_stats = [
        {
            "volume_id": v.volume_id,
            "photos": len(v.index),
            "size_bytes": v.current_size,
            "efficiency": round(v.get_efficiency(), 2)
        }
        for v in volumes.values()
    ]
    
    return {
        "store_id": STORE_ID,
        "total_photos": total_photos,
        "total_size_bytes": total_size,
        "volumes": volume_stats,
        "access_tracker": access_tracker.get_tracker_stats()
    }


@app.get("/debug/access_stats")
async def get_access_stats(photo_id: Optional[str] = None):
    """Debug endpoint to view access tracking state"""
    if photo_id:
        with access_tracker.lock:
            if photo_id not in access_tracker.events:
                raise HTTPException(status_code=404, detail="Photo not tracked")
            
            events = list(access_tracker.events[photo_id])
            return {
                'photo_id': photo_id,
                'ewma_rate_per_minute': access_tracker.ewma_rates.get(photo_id, 0.0),
                'last_update': access_tracker.last_update.get(photo_id, 0.0),
                'events': [{'timestamp': e.timestamp, 'count': e.count} for e in events],
                'event_count': len(events)
            }
    else:
        stats = access_tracker.get_stats_for_reporting()
        tracker_stats = access_tracker.get_tracker_stats()
        
        return {
            'tracker_stats': tracker_stats,
            'photos': stats
        }


# ============================================================================
# Background Workers
# ============================================================================

def register_with_directory():
    """Register store with directory service"""
    logger.info("Attempting to register with directory service...")
    
    while True:
        try:
            response = requests.post(
                f"{DIRECTORY_SERVICE_URL}/stores/register",
                json={
                    "store_id": STORE_ID,
                    "store_url": STORE_URL,
                    "capacity": MAX_VOLUME_SIZE
                },
                timeout=5
            )
            
            if response.status_code == 200:
                logger.info("Successfully registered with directory")
                return
            else:
                logger.warning(f"Registration failed: HTTP {response.status_code}")
        
        except Exception as e:
            logger.error(f"Failed to register: {e}")
        
        time.sleep(5)


def report_stats_worker():
    """Background worker to report access statistics"""
    logger.info(f"Stats reporting worker started (interval={STATS_REPORT_INTERVAL}s)")
    
    while True:
        try:
            time.sleep(STATS_REPORT_INTERVAL)
            
            stats = access_tracker.get_stats_for_reporting()
            
            if not stats:
                logger.debug("No active photos to report")
                continue
            
            report = {
                'store_id': STORE_ID,
                'timestamp': time.time(),
                'access_stats': stats,
                'tracker_info': access_tracker.get_tracker_stats()
            }
            
            try:
                response = requests.post(
                    f"{REPLICATION_MANAGER_URL}/stats/report",
                    json=report,
                    timeout=5
                )
                
                if response.status_code == 200:
                    logger.info(f"Reported stats for {len(stats)} photos")
                else:
                    logger.warning(f"Stats report failed: HTTP {response.status_code}")
            
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to send stats: {e}")
        
        except Exception as e:
            logger.error(f"Stats reporting error: {e}", exc_info=True)
            time.sleep(10)


def compaction_worker():
    """Background worker for volume compaction"""
    logger.info("Compaction worker started")
    
    while True:
        try:
            time.sleep(COMPACTION_INTERVAL)
            
            for volume in list(volumes.values()):
                efficiency = volume.get_efficiency()
                
                if efficiency < COMPACTION_EFFICIENCY_THRESHOLD:
                    logger.info(f"Volume {volume.volume_id} efficiency {efficiency:.2f} "
                               f"below threshold {COMPACTION_EFFICIENCY_THRESHOLD}, compacting")
                    volume.compact()
        
        except Exception as e:
            logger.error(f"Compaction worker error: {e}", exc_info=True)
            time.sleep(60)


def garbage_collection_worker():
    """Background worker for garbage collection"""
    logger.info("Garbage collection worker started")
    
    while True:
        try:
            time.sleep(GC_INTERVAL)
            
            logger.info("Starting garbage collection")
            
            # Get leader URL
            try:
                response = requests.get(f"{DIRECTORY_SERVICE_URL}/stats", timeout=5)
                leader_url = response.json().get('current_leader_url') or DIRECTORY_SERVICE_URL
            except:
                leader_url = DIRECTORY_SERVICE_URL
            
            # Collect all local photos
            local_photos = set(photo_metadata.keys())
            
            # Verify with directory (batch 100)
            registered_photos = set()
            for i in range(0, len(local_photos), 100):
                batch = list(local_photos)[i:i+100]
                
                try:
                    response = requests.post(
                        f"{leader_url}/verify_photos",
                        json={"photo_ids": batch, "store_id": STORE_ID},
                        timeout=10
                    )
                    
                    if response.status_code == 200:
                        registered_photos.update(response.json()['registered_photo_ids'])
                
                except Exception as e:
                    logger.error(f"Verification failed: {e}")
            
            # Find orphans
            orphaned_photos = local_photos - registered_photos
            
            if orphaned_photos:
                logger.warning(f"Found {len(orphaned_photos)} orphaned photos")
                
                # Delete orphans older than 1 hour
                now = time.time()
                for photo_id in orphaned_photos:
                    try:
                        volume_id, offset = photo_metadata[photo_id]
                        volume = volumes[volume_id]
                        needle = volume._read_needle_at_offset(offset)
                        
                        if now - needle.timestamp > 3600:  # 1 hour grace period
                            logger.warning(f"Deleting orphan photo {photo_id}")
                            volume.mark_deleted(photo_id)
                            del photo_metadata[photo_id]
                    
                    except Exception as e:
                        logger.error(f"Failed to delete orphan {photo_id}: {e}")
            
            logger.info("Garbage collection completed")
        
        except Exception as e:
            logger.error(f"GC worker error: {e}", exc_info=True)
            time.sleep(60)


# ============================================================================
# Application Lifecycle
# ============================================================================

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    
    # Startup
    logger.info(f"Starting Store Service: {STORE_ID}")
    
    # Register with directory
    register_with_directory()
    
    # Start background workers
    threading.Thread(target=report_stats_worker, daemon=True).start()
    threading.Thread(target=compaction_worker, daemon=True).start()
    threading.Thread(target=garbage_collection_worker, daemon=True).start()
    
    logger.info("Store service fully initialized")
    
    yield
    
    # Shutdown
    logger.info("Shutting down store service")


app = FastAPI(title="Store Service", lifespan=lifespan)

# Re-add rate limiting after app recreation
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
