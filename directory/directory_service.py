"""
Directory Service - Manages photo location mappings and store registry
IMPROVEMENTS:
- Issue #2: Redis-based leader election
- Issue #3: Health-aware location filtering
- Issue #4: Full photo listing for cold data monitoring
- Issue #8: Push notifications to followers
- Issue #11: Dynamic store discovery endpoint
- Issue #16: SHA256 checksums at directory level
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import uvicorn
import os
import json
import time
import threading
import logging
from pathlib import Path
from typing import Dict, List, Optional
import requests
from collections import defaultdict
from dataclasses import dataclass, asdict
from enum import Enum
import redis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DirectoryService")

app = FastAPI(title="Directory Service")

# Rate limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Configuration
INSTANCE_ID = os.getenv("INSTANCE_ID", "directory-1")
INSTANCE_URL = os.getenv("INSTANCE_URL", "http://localhost:9000")
DATA_DIRECTORY = os.getenv("DATA_DIRECTORY", "/data/directory")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
LEADER_TIMEOUT = int(os.getenv("LEADER_TIMEOUT", 10))
LEADER_HEARTBEAT_INTERVAL = int(os.getenv("LEADER_HEARTBEAT_INTERVAL", 3))
FOLLOWER_SYNC_INTERVAL = float(os.getenv("FOLLOWER_SYNC_INTERVAL", 0.5))  # IMPROVEMENT #8
SNAPSHOT_INTERVAL = int(os.getenv("SNAPSHOT_INTERVAL", 60))
REPLICATION_MANAGER_URL = os.getenv("REPLICATION_MANAGER_URL", "http://replication:9003")

# Redis client for leader election
try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.ping()
    logger.info("Connected to Redis successfully")
except Exception as e:
    logger.error(f"Redis connection failed: {e}")
    redis_client = None


class ServiceStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    DOWN = "down"


@dataclass
class PhotoLocation:
    store_id: str
    store_url: str
    volume_id: str
    created_at: int
    status: str = "active"


@dataclass
class StoreInfo:
    store_id: str
    store_url: str
    total_capacity: int
    available_capacity: int
    volumes: List[str]
    last_heartbeat: int
    status: str = ServiceStatus.HEALTHY.value


@dataclass
class PhotoMetadata:
    """IMPROVEMENT #16: Added checksum and size"""
    photo_id: str
    replicas: List[PhotoLocation]
    target_replica_count: int
    created_at: int
    sha256_checksum: Optional[str] = None
    size_bytes: Optional[int] = None


class LeadershipState:
    """Tracks leadership information"""
    def __init__(self):
        self.is_leader = False
        self.current_leader_id = None
        self.current_leader_url = None
        self.term_number = 0
        self.last_heartbeat = 0


class DirectoryState:
    """Manages the directory's core state"""
    def __init__(self):
        self.photos: Dict[str, PhotoMetadata] = {}
        self.stores: Dict[str, StoreInfo] = {}
        self.lock = threading.RLock()
        self.write_ahead_log: List[dict] = []
        self.log_index = 0
        # IMPROVEMENT #8: Track followers for push notifications
        self.followers = []


# Global state
directory_state = DirectoryState()
leadership_state = LeadershipState()
state_lock = threading.RLock()

# IMPROVEMENT #8: Discover other directory instances
def discover_followers():
    """Discover peer directory instances"""
    all_instances = [
        'http://directory-1:9000',
        'http://directory-2:9000',
        'http://directory-3:9000'
    ]
    
    followers = [url for url in all_instances if url != INSTANCE_URL]
    directory_state.followers = followers
    logger.info(f"Discovered followers: {followers}")


# IMPROVEMENT #2: Redis-based leader election

class RedisLeaderElection:
    """Redis-based leader election using SET NX with TTL"""
    
    def __init__(self, redis_client, ttl=10):
        self.redis = redis_client
        self.ttl = ttl
        self.lock_key = "haystack:leader:lock"
        self.term_key = "haystack:leader:term"
    
    def get_next_term(self) -> int:
        """Get next term number"""
        try:
            current_term = self.redis.get(self.term_key)
            if current_term:
                return int(current_term) + 1
            return 1
        except:
            return 1
    
    def try_claim_leadership(self, instance_id: str, instance_url: str) -> bool:
        """Attempt to claim leadership atomically"""
        try:
            term = self.get_next_term()
            
            claim = json.dumps({
                'leader_id': instance_id,
                'leader_url': instance_url,
                'term_number': term,
                'timestamp': time.time()
            })
            
            # SET NX = Set if Not eXists, with TTL
            result = self.redis.set(
                self.lock_key,
                claim,
                nx=True,  # Only set if doesn't exist
                ex=self.ttl  # Expires in 10 seconds
            )
            
            if result:
                # Successfully claimed, update term
                self.redis.set(self.term_key, term)
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error claiming leadership: {e}")
            return False
    
    def maintain_leadership(self, instance_id: str) -> bool:
        """Refresh TTL if still leader"""
        try:
            current = self.redis.get(self.lock_key)
            if current:
                data = json.loads(current)
                if data['leader_id'] == instance_id:
                    # Update timestamp and refresh TTL
                    data['timestamp'] = time.time()
                    self.redis.set(self.lock_key, json.dumps(data), ex=self.ttl)
                    return True
            return False
        except Exception as e:
            logger.error(f"Error maintaining leadership: {e}")
            return False
    
    def read_leadership(self) -> Optional[dict]:
        """Read current leadership record"""
        try:
            current = self.redis.get(self.lock_key)
            if current:
                return json.loads(current)
            return None
        except Exception as e:
            logger.error(f"Error reading leadership: {e}")
            return None


# Initialize Redis leader election
leader_election = RedisLeaderElection(redis_client) if redis_client else None


# Persistence functions

def save_snapshot():
    """Save directory state to disk"""
    try:
        os.makedirs(DATA_DIRECTORY, exist_ok=True)
        
        with directory_state.lock:
            snapshot = {
                'photos': {
                    photo_id: {
                        'photo_id': meta.photo_id,
                        'replicas': [asdict(loc) for loc in meta.replicas],
                        'target_replica_count': meta.target_replica_count,
                        'created_at': meta.created_at,
                        'sha256_checksum': meta.sha256_checksum,
                        'size_bytes': meta.size_bytes
                    }
                    for photo_id, meta in directory_state.photos.items()
                },
                'stores': {
                    store_id: asdict(store)
                    for store_id, store in directory_state.stores.items()
                },
                'log_index': directory_state.log_index
            }
        
        snapshot_path = os.path.join(DATA_DIRECTORY, 'snapshot.json')
        temp_path = snapshot_path + '.tmp'
        
        with open(temp_path, 'w') as f:
            json.dump(snapshot, f, indent=2)
        
        os.replace(temp_path, snapshot_path)
        logger.info(f"Saved snapshot with {len(directory_state.photos)} photos")
        
    except Exception as e:
        logger.error(f"Error saving snapshot: {e}")


def load_snapshot():
    """Load directory state from disk"""
    try:
        snapshot_path = os.path.join(DATA_DIRECTORY, 'snapshot.json')
        
        if not os.path.exists(snapshot_path):
            logger.info("No snapshot found, starting fresh")
            return
        
        with open(snapshot_path, 'r') as f:
            snapshot = json.load(f)
        
        with directory_state.lock:
            for photo_id, photo_data in snapshot.get('photos', {}).items():
                replicas = [PhotoLocation(**loc) for loc in photo_data['replicas']]
                directory_state.photos[photo_id] = PhotoMetadata(
                    photo_id=photo_data['photo_id'],
                    replicas=replicas,
                    target_replica_count=photo_data.get('target_replica_count', 3),
                    created_at=photo_data['created_at'],
                    sha256_checksum=photo_data.get('sha256_checksum'),
                    size_bytes=photo_data.get('size_bytes')
                )
            
            for store_id, store_data in snapshot.get('stores', {}).items():
                directory_state.stores[store_id] = StoreInfo(**store_data)
            
            directory_state.log_index = snapshot.get('log_index', 0)
        
        logger.info(f"Loaded snapshot: {len(directory_state.photos)} photos, {len(directory_state.stores)} stores")
        
    except Exception as e:
        logger.error(f"Error loading snapshot: {e}")


def append_to_wal(operation: dict):
    """Append operation to write-ahead log"""
    try:
        os.makedirs(DATA_DIRECTORY, exist_ok=True)
        
        wal_path = os.path.join(DATA_DIRECTORY, 'wal.jsonl')
        
        with open(wal_path, 'a') as f:
            operation['log_index'] = directory_state.log_index
            operation['timestamp'] = time.time()
            f.write(json.dumps(operation) + '\n')
            f.flush()
            os.fsync(f.fileno())
        
        directory_state.log_index += 1
        
    except Exception as e:
        logger.error(f"Error appending to WAL: {e}")


def load_wal():
    """Load and replay write-ahead log"""
    try:
        wal_path = os.path.join(DATA_DIRECTORY, 'wal.jsonl')
        
        if not os.path.exists(wal_path):
            return
        
        with open(wal_path, 'r') as f:
            for line in f:
                if line.strip():
                    operation = json.loads(line)
                    apply_operation(operation, from_wal=True)
        
        logger.info(f"Replayed WAL to log index {directory_state.log_index}")
        
    except Exception as e:
        logger.error(f"Error loading WAL: {e}")


def apply_operation(operation: dict, from_wal: bool = False):
    """Apply an operation to the directory state"""
    try:
        op_type = operation['type']
        
        if op_type == 'register_photo':
            photo_id = operation['photo_id']
            store_id = operation['store_id']
            store_url = operation['store_url']
            volume_id = operation['volume_id']
            
            with directory_state.lock:
                if photo_id not in directory_state.photos:
                    directory_state.photos[photo_id] = PhotoMetadata(
                        photo_id=photo_id,
                        replicas=[],
                        target_replica_count=3,
                        created_at=int(time.time()),
                        sha256_checksum=operation.get('checksum'),
                        size_bytes=operation.get('size_bytes')
                    )
                
                existing = [
                    loc for loc in directory_state.photos[photo_id].replicas 
                    if loc.store_id == store_id and loc.volume_id == volume_id
                ]
                
                if not existing:
                    location = PhotoLocation(
                        store_id=store_id,
                        store_url=store_url,
                        volume_id=volume_id,
                        created_at=int(time.time())
                    )
                    directory_state.photos[photo_id].replicas.append(location)
                    logger.info(f"Registered photo {photo_id} at {store_id}/{volume_id}")
        
        elif op_type == 'delete_photo':
            photo_id = operation['photo_id']
            
            with directory_state.lock:
                if photo_id in directory_state.photos:
                    del directory_state.photos[photo_id]
                    logger.info(f"Deleted photo {photo_id}")
        
        elif op_type == 'remove_replica':
            photo_id = operation['photo_id']
            store_id = operation['store_id']
            
            with directory_state.lock:
                if photo_id in directory_state.photos:
                    metadata = directory_state.photos[photo_id]
                    metadata.replicas = [
                        loc for loc in metadata.replicas
                        if loc.store_id != store_id
                    ]
                    logger.info(f"Removed replica {photo_id} from {store_id}")
        
    except Exception as e:
        logger.error(f"Error applying operation: {e}")


# IMPROVEMENT #8: Push notifications to followers
def notify_followers(operation: dict):
    """Non-blocking notification to followers about new operation"""
    for follower_url in directory_state.followers:
        threading.Thread(
            target=lambda url=follower_url: _send_sync_notification(url, operation),
            daemon=True
        ).start()


def _send_sync_notification(follower_url: str, operation: dict):
    """Send sync notification to a single follower"""
    try:
        requests.post(
            f"{follower_url}/internal/sync",
            json={'operations': [operation]},
            timeout=1
        )
    except Exception as e:
        logger.debug(f"Failed to notify {follower_url}: {e}")


# Leader Election functions

def try_claim_leadership():
    """Attempt to claim leadership via Redis"""
    if not leader_election:
        return False
    
    try:
        if leader_election.try_claim_leadership(INSTANCE_ID, INSTANCE_URL):
            with state_lock:
                leadership_state.is_leader = True
                leadership_state.current_leader_id = INSTANCE_ID
                leadership_state.current_leader_url = INSTANCE_URL
                leadership_state.last_heartbeat = time.time()
            
            logger.info(f"✓ Became leader")
            return True
        
        return False
        
    except Exception as e:
        logger.error(f"Error claiming leadership: {e}")
        return False


def maintain_leadership():
    """Maintain leadership by refreshing Redis TTL"""
    if not leader_election:
        return
    
    try:
        if leader_election.maintain_leadership(INSTANCE_ID):
            with state_lock:
                leadership_state.last_heartbeat = time.time()
            logger.debug("Leadership heartbeat sent")
        else:
            logger.warning("Failed to maintain leadership")
            with state_lock:
                leadership_state.is_leader = False
        
    except Exception as e:
        logger.error(f"Error maintaining leadership: {e}")


def check_leadership():
    """Check current leadership state from Redis"""
    if not leader_election:
        return
    
    try:
        current_leadership = leader_election.read_leadership()
        
        if not current_leadership:
            with state_lock:
                leadership_state.is_leader = False
                leadership_state.current_leader_id = None
                leadership_state.current_leader_url = None
            return
        
        with state_lock:
            leadership_state.current_leader_id = current_leadership['leader_id']
            leadership_state.current_leader_url = current_leadership['leader_url']
            leadership_state.term_number = current_leadership['term_number']
            
            if current_leadership['leader_id'] == INSTANCE_ID:
                leadership_state.is_leader = True
            else:
                leadership_state.is_leader = False
        
    except Exception as e:
        logger.error(f"Error checking leadership: {e}")


# Store selection functions

def select_store_for_write() -> Optional[StoreInfo]:
    """Select a healthy store with available capacity for writing"""
    with directory_state.lock:
        available_stores = [
            store for store in directory_state.stores.values()
            if store.status == ServiceStatus.HEALTHY.value
            and store.available_capacity > 100 * 1024 * 1024
            and time.time() - store.last_heartbeat < 60
        ]
        
        if not available_stores:
            return None
        
        return max(available_stores, key=lambda s: s.available_capacity)


# API Endpoints

@app.post("/allocate")
@limiter.limit("1000/minute")
async def allocate_write(request: Request):
    """Allocate a store for writing a new photo"""
    req_data = await request.json()
    
    # Forward to leader if we're a follower
    if not leadership_state.is_leader:
        if leadership_state.current_leader_url and leadership_state.current_leader_url != INSTANCE_URL:
            try:
                response = requests.post(
                    f"{leadership_state.current_leader_url}/allocate",
                    json=req_data,
                    timeout=10
                )
                return JSONResponse(content=response.json(), status_code=response.status_code)
            except Exception as e:
                logger.error(f"Error forwarding to leader: {e}")
                raise HTTPException(status_code=503, detail="Leader unavailable")
        else:
            raise HTTPException(status_code=503, detail="No leader available")
    
    try:
        photo_id = req_data.get('photo_id')
        
        store = select_store_for_write()
        
        if not store:
            raise HTTPException(status_code=503, detail="No available stores")
        
        return {
            'photo_id': photo_id,
            'primary_store_id': store.store_id,
            'primary_store_url': store.store_url,
            'allocated_at': time.time()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error allocating write: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/register")
@limiter.limit("1000/minute")
async def register_photo(request: Request):
    """Register a photo location after successful write"""
    req_data = await request.json()
    
    if not leadership_state.is_leader:
        if leadership_state.current_leader_url and leadership_state.current_leader_url != INSTANCE_URL:
            try:
                response = requests.post(
                    f"{leadership_state.current_leader_url}/register",
                    json=req_data,
                    timeout=10
                )
                return JSONResponse(content=response.json(), status_code=response.status_code)
            except Exception as e:
                logger.error(f"Error forwarding to leader: {e}")
                raise HTTPException(status_code=503, detail="Leader unavailable")
        else:
            raise HTTPException(status_code=503, detail="No leader available")
    
    try:
        photo_id = req_data['photo_id']
        store_id = req_data['store_id']
        volume_id = req_data['volume_id']
        checksum = req_data.get('checksum')
        size_bytes = req_data.get('size_bytes')
        
        store_url = None
        with directory_state.lock:
            if store_id in directory_state.stores:
                store_url = directory_state.stores[store_id].store_url
        
        if not store_url:
            raise HTTPException(status_code=400, detail="Unknown store")
        
        operation = {
            'type': 'register_photo',
            'photo_id': photo_id,
            'store_id': store_id,
            'store_url': store_url,
            'volume_id': volume_id,
            'checksum': checksum,
            'size_bytes': size_bytes
        }
        
        apply_operation(operation)
        append_to_wal(operation)
        
        # IMPROVEMENT #8: Push notification to followers
        if leadership_state.is_leader:
            notify_followers(operation)
        
        # Notify Replication Manager
        try:
            threading.Thread(
                target=notify_replication_manager,
                args=(photo_id,),
                daemon=True
            ).start()
        except Exception as e:
            logger.warning(f"Failed to notify replication manager: {e}")
        
        return {'success': True, 'photo_id': photo_id}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error registering photo: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/locate/{photo_id}")
async def locate_photo(photo_id: str):
    """
    IMPROVEMENT #3: Filter out unhealthy stores
    Get locations where a photo is stored (only healthy stores)
    """
    try:
        with directory_state.lock:
            if photo_id not in directory_state.photos:
                raise HTTPException(status_code=404, detail="Photo not found")
            
            metadata = directory_state.photos[photo_id]
            
            # Filter for active replicas on HEALTHY stores only
            healthy_locations = []
            for loc in metadata.replicas:
                if loc.status != 'active':
                    continue
                
                # Check if the store itself is healthy
                store = directory_state.stores.get(loc.store_id)
                if not store:
                    continue
                
                # Only include if store is healthy and has recent heartbeat
                if (store.status == ServiceStatus.HEALTHY.value and 
                    time.time() - store.last_heartbeat < 60):
                    healthy_locations.append({
                        'store_id': loc.store_id,
                        'store_url': loc.store_url,
                        'volume_id': loc.volume_id
                    })
            
            if not healthy_locations:
                raise HTTPException(status_code=404, detail="No healthy replicas found")
            
            return {
                'photo_id': photo_id,
                'locations': healthy_locations,
                'replica_count': len(healthy_locations)
            }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error locating photo {photo_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/delete/{photo_id}")
@limiter.limit("100/minute")
async def delete_photo(photo_id: str, request: Request):
    """Delete a photo from the system"""
    
    if not leadership_state.is_leader:
        if leadership_state.current_leader_url and leadership_state.current_leader_url != INSTANCE_URL:
            try:
                response = requests.delete(
                    f"{leadership_state.current_leader_url}/delete/{photo_id}",
                    timeout=10
                )
                return JSONResponse(content=response.json(), status_code=response.status_code)
            except Exception as e:
                logger.error(f"Error forwarding to leader: {e}")
                raise HTTPException(status_code=503, detail="Leader unavailable")
        else:
            raise HTTPException(status_code=503, detail="No leader available")
    
    try:
        with directory_state.lock:
            if photo_id not in directory_state.photos:
                raise HTTPException(status_code=404, detail="Photo not found")
            
            locations = directory_state.photos[photo_id].replicas
        
        operation = {
            'type': 'delete_photo',
            'photo_id': photo_id
        }
        
        apply_operation(operation)
        append_to_wal(operation)
        
        # IMPROVEMENT #8: Notify followers
        if leadership_state.is_leader:
            notify_followers(operation)
        
        # Asynchronously delete from stores
        for loc in locations:
            threading.Thread(
                target=delete_from_store,
                args=(loc.store_url, photo_id),
                daemon=True
            ).start()
        
        return {'success': True, 'photo_id': photo_id, 'deleted': True}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting photo {photo_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/stores/register")
async def register_store(request: dict):
    """Register a new store or update existing store"""
    try:
        store_id = request['store_id']
        store_url = request['store_url']
        total_capacity = request['total_capacity']
        volumes = request.get('volumes', [])
        
        with directory_state.lock:
            if store_id in directory_state.stores:
                store = directory_state.stores[store_id]
                store.store_url = store_url
                store.total_capacity = total_capacity
                store.volumes = volumes
                store.last_heartbeat = int(time.time())
                store.status = ServiceStatus.HEALTHY.value
            else:
                store = StoreInfo(
                    store_id=store_id,
                    store_url=store_url,
                    total_capacity=total_capacity,
                    available_capacity=total_capacity,
                    volumes=volumes,
                    last_heartbeat=int(time.time()),
                    status=ServiceStatus.HEALTHY.value
                )
                directory_state.stores[store_id] = store
        
        logger.info(f"Registered store: {store_id}")
        
        return {'success': True, 'store_id': store_id}
        
    except Exception as e:
        logger.error(f"Error registering store: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/stores/heartbeat")
async def store_heartbeat(request: dict):
    """Receive heartbeat from a store"""
    try:
        store_id = request['store_id']
        
        with directory_state.lock:
            if store_id in directory_state.stores:
                store = directory_state.stores[store_id]
                store.last_heartbeat = int(time.time())
                store.available_capacity = request.get('available_capacity', store.available_capacity)
                store.status = ServiceStatus.HEALTHY.value
                logger.debug(f"Heartbeat from {store_id}")
            else:
                logger.warning(f"Heartbeat from unknown store: {store_id}")
                raise HTTPException(status_code=404, detail="Store not registered")
        
        return {'success': True}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing heartbeat: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stores")
async def list_stores(healthy_only: bool = True):
    """
    IMPROVEMENT #11: Dynamic store discovery
    List all registered stores
    """
    with directory_state.lock:
        stores = []
        for store in directory_state.stores.values():
            if healthy_only and store.status != ServiceStatus.HEALTHY.value:
                continue
            
            stores.append({
                'store_id': store.store_id,
                'store_url': store.store_url,
                'total_capacity': store.total_capacity,
                'available_capacity': store.available_capacity,
                'status': store.status
            })
    
    return {'stores': stores, 'count': len(stores)}


@app.get("/photos/list")
async def list_all_photos(offset: int = 0, limit: int = 1000):
    """
    IMPROVEMENT #4: Full photo listing for cold data monitoring
    Paginated photo listing for full scans
    """
    try:
        with directory_state.lock:
            all_photo_ids = sorted(directory_state.photos.keys())
            page = all_photo_ids[offset:offset+limit]
            
            photos = []
            for photo_id in page:
                metadata = directory_state.photos[photo_id]
                
                # Count only healthy replicas
                healthy_count = 0
                for loc in metadata.replicas:
                    if loc.status != 'active':
                        continue
                    store = directory_state.stores.get(loc.store_id)
                    if store and store.status == ServiceStatus.HEALTHY.value:
                        healthy_count += 1
                
                photos.append({
                    'photo_id': photo_id,
                    'replica_count': healthy_count,
                    'target_replicas': metadata.target_replica_count,
                    'created_at': metadata.created_at,
                    'checksum': metadata.sha256_checksum,
                    'size_bytes': metadata.size_bytes
                })
            
            return {
                'photos': photos,
                'total': len(all_photo_ids),
                'offset': offset,
                'limit': limit
            }
        
    except Exception as e:
        logger.error(f"Error listing photos: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/verify_photos")
async def verify_photos(request: dict):
    """
    IMPROVEMENT #5: Batch photo verification for GC
    Check if photos are registered
    """
    try:
        photo_ids = request['photo_ids']
        
        with directory_state.lock:
            registered = set(directory_state.photos.keys())
        
        not_found = [pid for pid in photo_ids if pid not in registered]
        
        return {
            'requested': len(photo_ids),
            'registered': len(photo_ids) - len(not_found),
            'not_found': not_found
        }
        
    except Exception as e:
        logger.error(f"Error verifying photos: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/internal/remove_replica")
async def remove_replica(request: dict):
    """
    IMPROVEMENT #13: Remove a replica (for de-replication)
    """
    try:
        photo_id = request['photo_id']
        store_id = request['store_id']
        
        operation = {
            'type': 'remove_replica',
            'photo_id': photo_id,
            'store_id': store_id
        }
        
        apply_operation(operation)
        append_to_wal(operation)
        
        if leadership_state.is_leader:
            notify_followers(operation)
        
        return {'success': True}
        
    except Exception as e:
        logger.error(f"Error removing replica: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
async def get_stats():
    """Get directory statistics"""
    with directory_state.lock:
        total_photos = len(directory_state.photos)
        total_stores = len(directory_state.stores)
        healthy_stores = len([
            s for s in directory_state.stores.values() 
            if s.status == ServiceStatus.HEALTHY.value
        ])
        
        replica_distribution = defaultdict(int)
        for photo in directory_state.photos.values():
            count = len([loc for loc in photo.replicas if loc.status == 'active'])
            replica_distribution[count] += 1
    
    return {
        'instance_id': INSTANCE_ID,
        'is_leader': leadership_state.is_leader,
        'current_leader': leadership_state.current_leader_id,
        'term_number': leadership_state.term_number,
        'total_photos': total_photos,
        'total_stores': total_stores,
        'healthy_stores': healthy_stores,
        'replica_distribution': dict(replica_distribution)
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        'status': 'healthy',
        'instance_id': INSTANCE_ID,
        'is_leader': leadership_state.is_leader,
        'current_leader': leadership_state.current_leader_id
    }


@app.get("/internal/updates")
async def get_updates(since_index: int = 0):
    """Get updates since a specific log index (for followers to sync)"""
    try:
        updates = []
        
        wal_path = os.path.join(DATA_DIRECTORY, 'wal.jsonl')
        if os.path.exists(wal_path):
            with open(wal_path, 'r') as f:
                for line in f:
                    if line.strip():
                        operation = json.loads(line)
                        if operation.get('log_index', 0) > since_index:
                            updates.append(operation)
        
        return {
            'updates': updates,
            'current_index': directory_state.log_index
        }
        
    except Exception as e:
        logger.error(f"Error getting updates: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/internal/sync")
async def receive_sync(request: dict):
    """
    IMPROVEMENT #8: Receive real-time sync from leader
    """
    try:
        operations = request['operations']
        for operation in operations:
            apply_operation(operation, from_wal=True)
        
        return {'success': True, 'operations_applied': len(operations)}
        
    except Exception as e:
        logger.error(f"Error receiving sync: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Helper functions

def notify_replication_manager(photo_id: str):
    """Notify replication manager about new photo"""
    try:
        with directory_state.lock:
            if photo_id not in directory_state.photos:
                return
            
            metadata = directory_state.photos[photo_id]
            current_replicas = len([loc for loc in metadata.replicas if loc.status == 'active'])
        
        notification = {
            'photo_id': photo_id,
            'current_replica_count': current_replicas,
            'target_replica_count': metadata.target_replica_count
        }
        
        response = requests.post(
            f"{REPLICATION_MANAGER_URL}/replication/trigger",
            json=notification,
            timeout=5
        )
        
        if response.status_code == 200:
            logger.debug(f"Notified replication manager about {photo_id}")
        
    except Exception as e:
        logger.warning(f"Failed to notify replication manager: {e}")


def delete_from_store(store_url: str, photo_id: str):
    """Delete photo from a specific store"""
    try:
        response = requests.post(
            f"{store_url}/delete/{photo_id}",
            timeout=10
        )
        
        if response.status_code == 200:
            logger.debug(f"Deleted {photo_id} from {store_url}")
        
    except Exception as e:
        logger.warning(f"Failed to delete from store {store_url}: {e}")


# Background workers

def leader_election_worker():
    """Continuously manage leader election"""
    while True:
        try:
            time.sleep(LEADER_HEARTBEAT_INTERVAL)
            
            if leadership_state.is_leader:
                maintain_leadership()
            else:
                check_leadership()
                
                if not leadership_state.current_leader_id:
                    try_claim_leadership()
            
        except Exception as e:
            logger.error(f"Error in leader election worker: {e}")


def follower_sync_worker():
    """
    IMPROVEMENT #8: Faster sync (0.5s instead of 5s)
    Sync state from leader (for followers)
    """
    last_synced_index = 0
    
    while True:
        try:
            time.sleep(FOLLOWER_SYNC_INTERVAL)
            
            if leadership_state.is_leader:
                continue
            
            if not leadership_state.current_leader_url or leadership_state.current_leader_url == INSTANCE_URL:
                continue
            
            response = requests.get(
                f"{leadership_state.current_leader_url}/internal/updates",
                params={'since_index': last_synced_index},
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                updates = data.get('updates', [])
                
                for operation in updates:
                    apply_operation(operation, from_wal=True)
                    last_synced_index = max(last_synced_index, operation.get('log_index', 0))
                
                if updates:
                    logger.info(f"Synced {len(updates)} updates from leader")
            
        except Exception as e:
            logger.error(f"Error in follower sync worker: {e}")


def snapshot_worker():
    """Periodically save snapshots"""
    while True:
        try:
            time.sleep(SNAPSHOT_INTERVAL)
            
            if leadership_state.is_leader:
                save_snapshot()
            
        except Exception as e:
            logger.error(f"Error in snapshot worker: {e}")


def store_health_monitor():
    """Monitor store health and mark unhealthy stores"""
    while True:
        try:
            time.sleep(30)
            
            current_time = int(time.time())
            
            with directory_state.lock:
                for store in directory_state.stores.values():
                    if current_time - store.last_heartbeat > 60:
                        if store.status != ServiceStatus.DOWN.value:
                            store.status = ServiceStatus.DOWN.value
                            logger.warning(f"Store {store.store_id} marked as DOWN")
                    elif current_time - store.last_heartbeat > 30:
                        if store.status != ServiceStatus.DEGRADED.value:
                            store.status = ServiceStatus.DEGRADED.value
                            logger.warning(f"Store {store.store_id} marked as DEGRADED")
            
        except Exception as e:
            logger.error(f"Error in store health monitor: {e}")


@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    logger.info(f"Starting Directory Service: {INSTANCE_ID}")
    
    # Discover peer instances
    discover_followers()
    
    # Load persisted state
    load_snapshot()
    load_wal()
    
    # Start leader election
    check_leadership()
    if not leadership_state.current_leader_id:
        try_claim_leadership()
    
    # Start background workers
    threading.Thread(target=leader_election_worker, daemon=True).start()
    threading.Thread(target=follower_sync_worker, daemon=True).start()
    threading.Thread(target=snapshot_worker, daemon=True).start()
    threading.Thread(target=store_health_monitor, daemon=True).start()
    
    logger.info(f"Directory Service started. Leader: {leadership_state.is_leader}")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9000)