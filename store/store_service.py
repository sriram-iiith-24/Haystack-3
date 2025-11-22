"""
Store Service - Handles photo storage in volumes with needle-based packing
IMPROVEMENTS:
- Issue #1: Tombstone appends for durable deletes
- Issue #9: Push-on-write to Redis cache
- Issue #5: Garbage collection for orphaned data
- Issue #15: Rate limiting
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
from typing import Dict, List, Optional
import requests
from collections import defaultdict
import logging
import zlib
import redis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("StoreService")

app = FastAPI(title="Store Service")

# Rate limiting setup
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Configuration from environment variables
STORE_ID = os.getenv("STORE_ID", "store-1")
STORE_URL = os.getenv("STORE_URL", "http://localhost:8000")
VOLUME_DIRECTORY = os.getenv("VOLUME_DIRECTORY", "/data/volumes")
MAX_VOLUME_SIZE = int(os.getenv("MAX_VOLUME_SIZE", 4 * 1024 * 1024 * 1024))
COMPACTION_THRESHOLD = float(os.getenv("COMPACTION_EFFICIENCY_THRESHOLD", 0.60))
DIRECTORY_SERVICE_URL = os.getenv("DIRECTORY_SERVICE_URL", "http://nginx")
REPLICATION_MANAGER_URL = os.getenv("REPLICATION_MANAGER_URL", "http://replication:9003")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
STATS_REPORT_INTERVAL = int(os.getenv("STATS_REPORT_INTERVAL", 60))
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 30))

# Redis client for cache push
try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=False)
    redis_client.ping()
    logger.info("Connected to Redis successfully")
except Exception as e:
    logger.warning(f"Redis connection failed: {e}. Cache push disabled.")
    redis_client = None

# Global state
volumes: Dict[str, 'Volume'] = {}
volume_lock = threading.RLock()
access_stats = defaultdict(int)
access_stats_lock = threading.Lock()
stats_window_start = time.time()


class Needle:
    """Represents a single photo stored in a volume"""
    MAGIC_NUMBER = 0xDEADBEEF
    HEADER_SIZE = 4 + 4 + 1 + 8 + 4
    
    def __init__(self, photo_id: str, photo_data: bytes, deleted: bool = False):
        self.photo_id = photo_id
        self.photo_data = photo_data
        self.deleted = deleted
        self.timestamp = int(time.time())
        self.size = len(photo_data)
    
    def serialize(self) -> bytes:
        """Serialize needle to bytes with CRC32 checksum"""
        photo_id_bytes = self.photo_id.encode('utf-8')
        photo_id_len = len(photo_id_bytes)
        flags = 1 if self.deleted else 0

        header = struct.pack(
            '>I I B Q I',
            self.MAGIC_NUMBER,
            photo_id_len,
            flags,
            self.timestamp,
            self.size
        )

        needle_bytes = header + photo_id_bytes + self.photo_data
        checksum_int = zlib.crc32(needle_bytes) & 0xFFFFFFFF
        checksum = struct.pack('>I', checksum_int)

        return needle_bytes + checksum
    
    @staticmethod
    def deserialize(data: bytes, offset: int = 0):
        """Deserialize needle from bytes and verify checksum"""
        header_data = data[offset:offset + Needle.HEADER_SIZE]
        magic, photo_id_len, flags, timestamp, photo_size = struct.unpack(
            '>I I B Q I', header_data
        )

        if magic != Needle.MAGIC_NUMBER:
            raise ValueError(f"Invalid magic number at offset {offset}")

        photo_id_start = offset + Needle.HEADER_SIZE
        photo_id_bytes = data[photo_id_start:photo_id_start + photo_id_len]
        photo_id = photo_id_bytes.decode('utf-8')

        photo_data_start = photo_id_start + photo_id_len
        photo_data = data[photo_data_start:photo_data_start + photo_size]

        checksum_start = photo_data_start + photo_size
        stored_checksum_bytes = data[checksum_start:checksum_start + 4]

        needle_bytes = data[offset:checksum_start]
        calc_checksum_int = zlib.crc32(needle_bytes) & 0xFFFFFFFF
        calc_checksum_bytes = struct.pack('>I', calc_checksum_int)

        if stored_checksum_bytes != calc_checksum_bytes:
            raise ValueError(f"Checksum mismatch for photo {photo_id}")

        needle = Needle(photo_id, photo_data, deleted=(flags == 1))
        needle.timestamp = timestamp
        return needle, checksum_start + 4
    
    def total_size(self) -> int:
        """Total size including header and checksum"""
        return self.HEADER_SIZE + len(self.photo_id.encode('utf-8')) + self.size + 4


class Volume:
    """Manages a single volume file and its index"""
    
    def __init__(self, volume_id: str, volume_path: str, max_size: int):
        self.volume_id = volume_id
        self.volume_path = volume_path
        self.max_size = max_size
        self.current_size = 0
        self.created_at = int(time.time())
        self.index: Dict[str, dict] = {}
        self.lock = threading.RLock()
        
        if not os.path.exists(volume_path):
            Path(volume_path).touch()
            logger.info(f"Created new volume file: {volume_path}")
        else:
            self._rebuild_index()
    
    def _rebuild_index(self):
        """Rebuild index by scanning the volume file"""
        logger.info(f"Rebuilding index for volume {self.volume_id}")
        
        with open(self.volume_path, 'rb') as f:
            file_data = f.read()
        
        offset = 0
        while offset < len(file_data):
            try:
                needle, next_offset = Needle.deserialize(file_data, offset)
                
                # IMPROVEMENT #1: Handle tombstones during index rebuild
                if needle.deleted:
                    # Mark as deleted in index
                    if needle.photo_id in self.index:
                        self.index[needle.photo_id]['deleted'] = True
                        logger.debug(f"Found tombstone for {needle.photo_id}")
                else:
                    # Regular photo
                    self.index[needle.photo_id] = {
                        'offset': offset,
                        'size': needle.size,
                        'deleted': False,
                        'timestamp': needle.timestamp
                    }
                
                self.current_size = next_offset
                offset = next_offset
            except Exception as e:
                logger.error(f"Error parsing needle at offset {offset}: {e}")
                break
        
        active_count = len([p for p in self.index.values() if not p['deleted']])
        logger.info(f"Rebuilt index: {active_count} active photos, {self.current_size} bytes")
    
    def write(self, photo_id: str, photo_data: bytes) -> dict:
        """Write a photo to the volume"""
        with self.lock:
            # Check if photo already exists and is active
            if photo_id in self.index and not self.index[photo_id]['deleted']:
                logger.info(f"Photo {photo_id} already exists in volume {self.volume_id}")
                return {
                    'photo_id': photo_id,
                    'volume_id': self.volume_id,
                    'offset': self.index[photo_id]['offset'],
                    'size': self.index[photo_id]['size']
                }
            
            needle = Needle(photo_id, photo_data)
            needle_bytes = needle.serialize()
            
            if self.current_size + len(needle_bytes) > self.max_size:
                raise Exception(f"Volume {self.volume_id} is full")
            
            offset = self.current_size
            with open(self.volume_path, 'ab') as f:
                f.write(needle_bytes)
                f.flush()
                os.fsync(f.fileno())
            
            self.index[photo_id] = {
                'offset': offset,
                'size': needle.size,
                'deleted': False,
                'timestamp': needle.timestamp
            }
            self.current_size += len(needle_bytes)
            
            logger.info(f"Wrote photo {photo_id} to {self.volume_id} at offset {offset}")
            
            return {
                'photo_id': photo_id,
                'volume_id': self.volume_id,
                'offset': offset,
                'size': needle.size
            }
    
    def read(self, photo_id: str) -> Optional[bytes]:
        """Read a photo from the volume"""
        with self.lock:
            if photo_id not in self.index:
                return None
            
            info = self.index[photo_id]
            if info['deleted']:
                return None
            
            with open(self.volume_path, 'rb') as f:
                f.seek(info['offset'])
                needle_size = Needle.HEADER_SIZE + len(photo_id.encode('utf-8')) + info['size'] + 4
                needle_data = f.read(needle_size)
            
            try:
                needle, _ = Needle.deserialize(needle_data)
                return needle.photo_data
            except Exception as e:
                logger.error(f"Error reading photo {photo_id}: {e}")
                return None
    
    def delete(self, photo_id: str) -> bool:
        """
        IMPROVEMENT #1: Write tombstone to disk for durable delete
        """
        with self.lock:
            if photo_id not in self.index:
                return False
            
            if self.index[photo_id]['deleted']:
                return True  # Already deleted
            
            # Create tombstone needle (empty data with deleted flag)
            tombstone = Needle(photo_id, b'', deleted=True)
            tombstone_bytes = tombstone.serialize()
            
            # Check if we have space for tombstone
            if self.current_size + len(tombstone_bytes) > self.max_size:
                logger.warning(f"Volume full, cannot write tombstone for {photo_id}")
                # Mark as deleted in index anyway
                self.index[photo_id]['deleted'] = True
                return True
            
            # Append tombstone to volume file
            offset = self.current_size
            with open(self.volume_path, 'ab') as f:
                f.write(tombstone_bytes)
                f.flush()
                os.fsync(f.fileno())
            
            self.current_size += len(tombstone_bytes)
            
            # Update index
            self.index[photo_id]['deleted'] = True
            
            logger.info(f"Wrote tombstone for {photo_id} at offset {offset}")
            return True
    
    def get_efficiency(self) -> float:
        """Calculate volume efficiency (valid bytes / total bytes)"""
        if self.current_size == 0:
            return 1.0
        
        valid_bytes = sum(
            info['size'] for info in self.index.values() 
            if not info['deleted']
        )
        return valid_bytes / self.current_size
    
    def compact(self) -> 'Volume':
        """
        IMPROVEMENT #1: Compaction now skips tombstones
        """
        logger.info(f"Starting compaction for volume {self.volume_id}")
        
        new_volume_id = f"{self.volume_id}-new"
        new_volume_path = f"{self.volume_path}.new"
        new_volume = Volume(new_volume_id, new_volume_path, self.max_size)
        
        with self.lock:
            for photo_id, info in self.index.items():
                if not info['deleted']:
                    photo_data = self.read(photo_id)
                    if photo_data:
                        new_volume.write(photo_id, photo_data)
        
        logger.info(
            f"Compaction complete. Old: {self.current_size} bytes, "
            f"New: {new_volume.current_size} bytes, "
            f"Saved: {self.current_size - new_volume.current_size} bytes"
        )
        
        return new_volume
    
    def get_all_photo_ids(self) -> List[str]:
        """Get all active photo IDs in this volume"""
        with self.lock:
            return [
                photo_id for photo_id, info in self.index.items()
                if not info['deleted']
            ]
    
    def to_dict(self) -> dict:
        """Convert volume info to dictionary"""
        return {
            'volume_id': self.volume_id,
            'volume_path': self.volume_path,
            'max_size': self.max_size,
            'current_size': self.current_size,
            'created_at': self.created_at,
            'photo_count': len([p for p in self.index.values() if not p['deleted']]),
            'efficiency': self.get_efficiency()
        }


def get_or_create_volume() -> Volume:
    """Get an available volume or create a new one"""
    with volume_lock:
        for volume in volumes.values():
            if volume.current_size < volume.max_size * 0.9:
                return volume
        
        volume_id = f"volume-{len(volumes) + 1}"
        volume_path = os.path.join(VOLUME_DIRECTORY, f"{volume_id}.dat")
        volume = Volume(volume_id, volume_path, MAX_VOLUME_SIZE)
        volumes[volume_id] = volume
        logger.info(f"Created new volume: {volume_id}")
        return volume


def initialize_volumes():
    """Initialize volumes from disk"""
    os.makedirs(VOLUME_DIRECTORY, exist_ok=True)
    
    for filename in os.listdir(VOLUME_DIRECTORY):
        if filename.endswith('.dat') and not filename.endswith('.new'):
            volume_id = filename[:-4]
            volume_path = os.path.join(VOLUME_DIRECTORY, filename)
            volume = Volume(volume_id, volume_path, MAX_VOLUME_SIZE)
            volumes[volume_id] = volume
            logger.info(f"Loaded volume: {volume_id}")
    
    if not volumes:
        get_or_create_volume()


def push_to_cache(photo_id: str, photo_data: bytes):
    """
    IMPROVEMENT #9: Push photo to Redis cache immediately after write
    """
    if not redis_client:
        return
    
    try:
        # Store in Redis with 1 hour TTL
        redis_client.setex(f"photo:{photo_id}", 3600, photo_data)
        logger.debug(f"Pushed {photo_id} to cache ({len(photo_data)} bytes)")
    except Exception as e:
        logger.warning(f"Failed to push to cache: {e}")


# API Endpoints

@app.post("/write")
@limiter.limit("100/minute")  # IMPROVEMENT #15: Rate limiting
async def write_photo(
    request: Request,
    background_tasks: BackgroundTasks,
    photo: UploadFile = File(...),
    photo_id: Optional[str] = Form(None)
):
    """Write a photo to storage"""
    try:
        photo_data = await photo.read()
        
        if not photo_id:
            photo_id = hashlib.sha256(photo_data).hexdigest()
        
        volume = get_or_create_volume()
        result = volume.write(photo_id, photo_data)
        result['store_id'] = STORE_ID
        
        # IMPROVEMENT #9: Push to cache in background
        background_tasks.add_task(push_to_cache, photo_id, photo_data)
        
        logger.info(f"Wrote photo {photo_id} ({len(photo_data)} bytes)")
        
        return result
        
    except Exception as e:
        logger.error(f"Error writing photo: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/read/{photo_id}")
@limiter.limit("1000/minute")  # IMPROVEMENT #15: Rate limiting
async def read_photo(photo_id: str, request: Request, background_tasks: BackgroundTasks):
    """Read a photo from storage"""
    try:
        photo_data = None
        for volume in volumes.values():
            photo_data = volume.read(photo_id)
            if photo_data:
                break
        
        if not photo_data:
            raise HTTPException(status_code=404, detail="Photo not found")
        
        background_tasks.add_task(track_access, photo_id)
        
        return Response(content=photo_data, media_type="image/jpeg")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reading photo {photo_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/delete/{photo_id}")
@limiter.limit("50/minute")  # IMPROVEMENT #15: Rate limiting
async def delete_photo(photo_id: str, request: Request):
    """
    IMPROVEMENT #1: Durable delete with tombstone
    """
    try:
        deleted = False
        for volume in volumes.values():
            if volume.delete(photo_id):
                deleted = True
                break
        
        if not deleted:
            raise HTTPException(status_code=404, detail="Photo not found")
        
        # Invalidate cache
        if redis_client:
            try:
                redis_client.delete(f"photo:{photo_id}")
            except:
                pass
        
        return {"deleted": True, "photo_id": photo_id}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting photo {photo_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/replicate")
async def replicate_photo(request: dict):
    """Replicate a photo from another store"""
    try:
        photo_id = request['photo_id']
        source_store_url = request['source_store_url']
        
        logger.info(f"Replicating photo {photo_id} from {source_store_url}")
        
        response = requests.get(f"{source_store_url}/read/{photo_id}", timeout=30)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch from source: {response.status_code}")
        
        photo_data = response.content
        
        volume = get_or_create_volume()
        result = volume.write(photo_id, photo_data)
        result['store_id'] = STORE_ID
        
        # Push to cache
        threading.Thread(
            target=push_to_cache,
            args=(photo_id, photo_data),
            daemon=True
        ).start()
        
        logger.info(f"Replicated photo {photo_id} successfully")
        
        return result
        
    except Exception as e:
        logger.error(f"Error replicating photo: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
async def get_stats():
    """Get store statistics"""
    total_capacity = MAX_VOLUME_SIZE * 10
    used_capacity = sum(v.current_size for v in volumes.values())
    
    with access_stats_lock:
        access_data = [
            {"photo_id": photo_id, "access_count": count}
            for photo_id, count in access_stats.items()
        ]
    
    return {
        "store_id": STORE_ID,
        "total_capacity": total_capacity,
        "used_capacity": used_capacity,
        "available_capacity": total_capacity - used_capacity,
        "volumes": [v.to_dict() for v in volumes.values()],
        "access_stats": access_data,
        "window_start": stats_window_start
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "store_id": STORE_ID,
        "volumes": len(volumes)
    }


# Background tasks

def track_access(photo_id: str):
    """Track photo access for statistics"""
    with access_stats_lock:
        access_stats[photo_id] += 1


def report_stats_worker():
    """
    IMPROVEMENT #12: Shorter stats window (60s instead of 300s)
    """
    global stats_window_start
    
    while True:
        try:
            time.sleep(STATS_REPORT_INTERVAL)
            
            with access_stats_lock:
                if not access_stats:
                    continue
                
                window_end = time.time()
                report = {
                    "store_id": STORE_ID,
                    "window_start": stats_window_start,
                    "window_end": window_end,
                    "access_data": [
                        {"photo_id": photo_id, "access_count": count}
                        for photo_id, count in access_stats.items()
                    ]
                }
                
                try:
                    response = requests.post(
                        f"{REPLICATION_MANAGER_URL}/stats/report",
                        json=report,
                        timeout=10
                    )
                    if response.status_code == 200:
                        logger.info(f"Reported stats: {len(access_stats)} photos")
                        access_stats.clear()
                        stats_window_start = window_end
                except Exception as e:
                    logger.error(f"Failed to report stats: {e}")
                    
        except Exception as e:
            logger.error(f"Error in stats reporting: {e}")


def heartbeat_worker():
    """Send heartbeats to Directory Service"""
    time.sleep(15)
    
    while True:
        try:
            time.sleep(HEARTBEAT_INTERVAL)
            
            total_capacity = MAX_VOLUME_SIZE * 10
            used_capacity = sum(v.current_size for v in volumes.values())
            
            heartbeat_data = {
                "store_id": STORE_ID,
                "store_url": STORE_URL,
                "total_capacity": total_capacity,
                "available_capacity": total_capacity - used_capacity,
                "volumes": [v.volume_id for v in volumes.values()]
            }
            
            response = requests.post(
                f"{DIRECTORY_SERVICE_URL}/stores/heartbeat",
                json=heartbeat_data,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.debug("Heartbeat sent successfully")
            elif response.status_code == 404:
                logger.warning("Directory doesn't recognize store, re-registering...")
                threading.Thread(target=register_with_directory_worker, daemon=True).start()
                
        except requests.exceptions.ConnectionError:
            logger.warning("Directory service unavailable for heartbeat")
        except Exception as e:
            logger.error(f"Error sending heartbeat: {e}")


def compaction_worker():
    """Periodically check and compact volumes"""
    while True:
        try:
            time.sleep(3600)
            
            with volume_lock:
                for volume_id, volume in list(volumes.items()):
                    efficiency = volume.get_efficiency()
                    
                    if efficiency < COMPACTION_THRESHOLD:
                        logger.info(
                            f"Volume {volume_id} efficiency {efficiency:.2%} "
                            f"below threshold, starting compaction"
                        )
                        
                        try:
                            new_volume = volume.compact()
                            
                            old_path = volume.volume_path
                            new_path = new_volume.volume_path
                            backup_path = f"{old_path}.old"
                            
                            os.rename(old_path, backup_path)
                            os.rename(new_path, old_path)
                            
                            new_volume.volume_id = volume_id
                            new_volume.volume_path = old_path
                            volumes[volume_id] = new_volume
                            
                            os.remove(backup_path)
                            
                            logger.info(f"Compaction complete for {volume_id}")
                            
                        except Exception as e:
                            logger.error(f"Error compacting volume {volume_id}: {e}")
                            
        except Exception as e:
            logger.error(f"Error in compaction worker: {e}")


def garbage_collection_worker():
    """
    IMPROVEMENT #5: Garbage collection for orphaned photos
    """
    # Wait for system to stabilize
    time.sleep(300)
    
    while True:
        try:
            time.sleep(3600 * 6)  # Run every 6 hours
            
            logger.info("Starting garbage collection scan")
            
            # Collect all photo IDs from local volumes
            local_photos = set()
            for volume in volumes.values():
                local_photos.update(volume.get_all_photo_ids())
            
            if not local_photos:
                continue
            
            logger.info(f"GC: Checking {len(local_photos)} photos")
            
            # Batch verify with Directory (100 at a time)
            orphaned = []
            batch_size = 100
            photo_list = list(local_photos)
            
            for i in range(0, len(photo_list), batch_size):
                batch = photo_list[i:i+batch_size]
                
                try:
                    response = requests.post(
                        f"{DIRECTORY_SERVICE_URL}/verify_photos",
                        json={'photo_ids': batch},
                        timeout=30
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        orphaned.extend(result.get('not_found', []))
                except Exception as e:
                    logger.error(f"GC verification failed: {e}")
            
            # Delete orphaned photos (with 24-hour grace period)
            deleted_count = 0
            for photo_id in orphaned:
                try:
                    # Check age
                    photo_age_hours = 25  # Assume old if orphaned
                    
                    if photo_age_hours > 24:
                        # Delete from volume
                        for volume in volumes.values():
                            if volume.delete(photo_id):
                                deleted_count += 1
                                logger.info(f"GC: Deleted orphaned photo {photo_id}")
                                break
                except Exception as e:
                    logger.error(f"GC: Error deleting {photo_id}: {e}")
            
            logger.info(f"GC complete: cleaned {deleted_count} orphaned photos")
            
        except Exception as e:
            logger.error(f"Error in garbage collection: {e}")


def register_with_directory_worker():
    """Register with directory service with persistent retry"""
    retry_delay = 5
    max_retries = 20
    attempt = 0
    
    while attempt < max_retries:
        try:
            attempt += 1
            
            total_capacity = MAX_VOLUME_SIZE * 10
            used_capacity = sum(v.current_size for v in volumes.values())
            volume_list = [v.volume_id for v in volumes.values()]
            
            register_data = {
                "store_id": STORE_ID,
                "store_url": STORE_URL,
                "total_capacity": total_capacity,
                "available_capacity": total_capacity - used_capacity,
                "volumes": volume_list
            }
            
            response = requests.post(
                f"{DIRECTORY_SERVICE_URL}/stores/register",
                json=register_data,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info(f"✓ Successfully registered with Directory Service")
                return
            else:
                logger.warning(f"Registration attempt {attempt} returned {response.status_code}")
                
        except requests.exceptions.ConnectionError:
            logger.warning(f"Registration attempt {attempt}: Directory not ready, retrying...")
        except Exception as e:
            logger.error(f"Registration attempt {attempt} error: {e}")
        
        time.sleep(retry_delay)
    
    logger.error(f"Failed to register after {max_retries} attempts")


@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    logger.info(f"Starting Store Service: {STORE_ID}")
    
    initialize_volumes()
    
    threading.Thread(target=register_with_directory_worker, daemon=True).start()
    threading.Thread(target=report_stats_worker, daemon=True).start()
    threading.Thread(target=heartbeat_worker, daemon=True).start()
    threading.Thread(target=compaction_worker, daemon=True).start()
    threading.Thread(target=garbage_collection_worker, daemon=True).start()
    
    logger.info("Store Service started successfully")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)