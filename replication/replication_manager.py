"""
Replication Manager - FINAL PRODUCTION VERSION (HEAP ORDERING FIX)

CRITICAL FIX: Use (time, counter, task) tuples in heaps to prevent comparison errors
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from contextlib import asynccontextmanager
import uvicorn
import os
import json
import time
import threading
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field, asdict
import requests
from collections import defaultdict
from enum import Enum
import queue
import heapq
from datetime import datetime, timedelta
import uuid
import hashlib
import itertools  # For counter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] %(message)s'
)

logger = logging.getLogger("ReplicationManager")

# Configuration
DIRECTORY_SERVICE_URL = os.getenv("DIRECTORY_SERVICE_URL", "http://nginx")
DEFAULT_REPLICA_COUNT = int(os.getenv("DEFAULT_REPLICA_COUNT", "3"))
MIN_REPLICA_COUNT = int(os.getenv("MIN_REPLICA_COUNT", "2"))
MAX_REPLICA_COUNT = int(os.getenv("MAX_REPLICA_COUNT", "5"))
REPLICATION_WORKERS = int(os.getenv("REPLICATION_WORKERS", "5"))
DE_REPLICATION_WORKERS = int(os.getenv("DE_REPLICATION_WORKERS", "2"))

STATS_STALENESS_THRESHOLD = int(os.getenv("STATS_STALENESS_THRESHOLD", "300"))
ACCESS_RATE_THRESHOLD = float(os.getenv("ACCESS_RATE_THRESHOLD", "200"))
HIGH_ACCESS_THRESHOLD = float(os.getenv("HIGH_ACCESS_THRESHOLD", "500"))
COOL_DOWN_THRESHOLD = float(os.getenv("COOL_DOWN_THRESHOLD", "50"))
DE_REPLICATION_GRACE_PERIOD = int(os.getenv("DE_REPLICATION_GRACE_PERIOD", "3600"))

# ============================================================================
# CRITICAL FIX: Global monotonic counter for heap tie-breaking
# ============================================================================

heap_counter = itertools.count()  # Thread-safe monotonic counter
heap_counter_lock = threading.Lock()

def get_next_heap_id() -> int:
    """Get next unique ID for heap ordering (thread-safe)"""
    with heap_counter_lock:
        return next(heap_counter)

# ============================================================================
# Data Structures
# ============================================================================

class TaskPriority(Enum):
    LOW = 3
    NORMAL = 2
    HIGH = 1
    CRITICAL = 0


@dataclass
class ReplicationTask:
    task_id: str
    photo_id: str
    source_store_id: str
    source_store_url: str
    target_store_id: str
    target_store_url: str
    priority: TaskPriority
    created_at: float
    attempts: int = 0
    last_attempt: Optional[float] = None
    next_retry_time: float = 0.0
    
    def __lt__(self, other):
        if self.priority.value != other.priority.value:
            return self.priority.value < other.priority.value
        return self.created_at < other.created_at


@dataclass
class DeReplicationTask:
    """
    CRITICAL: No __lt__ method defined (and we don't need one anymore)
    Heap ordering is handled by (time, counter, task) tuples
    """
    task_id: str
    photo_id: str
    target_store_id: str
    target_store_url: str
    created_at: float
    attempts: int = 0
    next_retry_time: float = 0.0


@dataclass
class PhotoMetadata:
    photo_id: str
    current_replica_count: int
    target_replica_count: int
    locations: List[dict]
    last_checked: float
    created_at: float
    cool_down_start: Optional[float] = None


@dataclass
class ReplicationState:
    task_queue: queue.PriorityQueue = field(default_factory=queue.PriorityQueue)
    
    # CRITICAL FIX: Heaps now store (time, counter, task) tuples
    retry_heap: List[Tuple[float, int, ReplicationTask]] = field(default_factory=list)
    retry_heap_lock: threading.Lock = field(default_factory=threading.Lock)
    
    de_replication_queue: queue.Queue = field(default_factory=queue.Queue)
    de_replication_heap: List[Tuple[float, int, DeReplicationTask]] = field(default_factory=list)
    de_replication_heap_lock: threading.Lock = field(default_factory=threading.Lock)
    
    pending_task_keys: Set[str] = field(default_factory=set)
    pending_de_replication_keys: Set[str] = field(default_factory=set)
    completed_tasks: Dict[str, float] = field(default_factory=dict)
    failed_tasks: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    failed_stores: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    photos: Dict[str, PhotoMetadata] = field(default_factory=dict)
    invalid_photos: Set[str] = field(default_factory=set)
    stats_lock: threading.RLock = field(default_factory=threading.RLock)


replication_state = ReplicationState()


@dataclass
class PhotoAccessAggregator:
    photo_id: str
    store_rates: Dict[str, float] = field(default_factory=dict)
    last_updated: Dict[str, float] = field(default_factory=dict)
    aggregate_rate: float = 0.0
    last_aggregation_time: float = 0.0
    
    def update_store_rate(self, store_id: str, ewma_rate: float, timestamp: float) -> None:
        self.store_rates[store_id] = ewma_rate
        self.last_updated[store_id] = timestamp
        self._recalculate_aggregate()
    
    def _recalculate_aggregate(self) -> None:
        now = time.time()
        total_rate = 0.0
        stale_stores = []
        
        for store_id, rate in self.store_rates.items():
            last_update = self.last_updated.get(store_id, 0)
            if (now - last_update) < STATS_STALENESS_THRESHOLD:
                total_rate += rate
            else:
                stale_stores.append(store_id)
        
        for store_id in stale_stores:
            del self.store_rates[store_id]
            del self.last_updated[store_id]
        
        self.aggregate_rate = total_rate
        self.last_aggregation_time = now
    
    def get_aggregate_rate(self, force_recalculate: bool = False) -> float:
        if force_recalculate:
            self._recalculate_aggregate()
        return self.aggregate_rate
    
    def get_store_rate(self, store_id: str) -> float:
        return self.store_rates.get(store_id, 0.0)


photo_aggregators: Dict[str, PhotoAccessAggregator] = {}
aggregators_lock = threading.RLock()

# ============================================================================
# Helper Functions (unchanged - omitted for brevity)
# ============================================================================

def get_leader_url() -> str:
    try:
        response = requests.get(f"{DIRECTORY_SERVICE_URL}/stats", timeout=5)
        stats = response.json()
        return stats.get('current_leader_url') or DIRECTORY_SERVICE_URL
    except Exception as e:
        logger.error(f"Failed to get leader URL: {e}")
        return DIRECTORY_SERVICE_URL


def get_available_stores() -> List[dict]:
    try:
        leader_url = get_leader_url()
        response = requests.get(f"{leader_url}/stores", timeout=5)
        if response.status_code == 200:
            stores = response.json()
            with replication_state.stats_lock:
                available = [s for s in stores if replication_state.failed_stores.get(s['store_id'], 0) < 5]
            return available
        return []
    except Exception as e:
        logger.error(f"Error getting stores: {e}")
        return []


def get_photo_locations(photo_id: str) -> Optional[List[dict]]:
    try:
        leader_url = get_leader_url()
        response = requests.get(f"{leader_url}/locate/{photo_id}", timeout=5)
        if response.status_code != 200:
            return None
        
        data = response.json()
        if isinstance(data, list):
            locations = data
        elif isinstance(data, dict) and 'locations' in data:
            locations = data['locations']
        else:
            return None
        
        if not isinstance(locations, list):
            return None
        
        valid_locations = [loc for loc in locations if isinstance(loc, dict) and 'store_id' in loc and 'store_url' in loc]
        return valid_locations if valid_locations else None
    except Exception as e:
        logger.error(f"Error getting locations: {e}")
        return None


def is_photo_valid(photo_id: str, locations: Optional[List[dict]]) -> bool:
    with replication_state.stats_lock:
        if photo_id in replication_state.invalid_photos:
            return False
    if not locations or len(locations) == 0:
        with replication_state.stats_lock:
            replication_state.invalid_photos.add(photo_id)
        return False
    return True


def select_stores_for_de_replication(photo_id: str, locations: List[dict], count: int) -> List[dict]:
    if count >= len(locations):
        return locations[:count]
    
    store_scores = []
    for loc in locations:
        store_id = loc['store_id']
        score = 0.0
        
        with replication_state.stats_lock:
            failure_count = replication_state.failed_stores.get(store_id, 0)
        if failure_count > 0:
            score -= 1000 * failure_count
        
        with aggregators_lock:
            aggregator = photo_aggregators.get(photo_id)
            if aggregator:
                store_rate = aggregator.get_store_rate(store_id)
                score += store_rate
        
        available_capacity = loc.get('available_capacity', 0)
        score -= available_capacity / 1e9
        store_scores.append((score, loc))
    
    store_scores.sort(key=lambda x: x[0])
    return [loc for score, loc in store_scores[:count]]


def create_replication_task(photo_id: str, source_locations: List[dict], priority: TaskPriority = TaskPriority.NORMAL) -> Optional[ReplicationTask]:
    if not is_photo_valid(photo_id, source_locations):
        return None
    
    source = source_locations[0]
    available_stores = get_available_stores()
    if not available_stores:
        return None
    
    existing_store_ids = {loc['store_id'] for loc in source_locations}
    candidate_stores = [s for s in available_stores if s['store_id'] not in existing_store_ids]
    if not candidate_stores:
        return None
    
    target = max(candidate_stores, key=lambda s: s.get('available_capacity', 0))
    task_key = f"{photo_id}:{target['store_id']}"
    
    with replication_state.stats_lock:
        if task_key in replication_state.pending_task_keys:
            return None
        replication_state.pending_task_keys.add(task_key)
    
    task = ReplicationTask(
        task_id=str(uuid.uuid4()), photo_id=photo_id,
        source_store_id=source['store_id'], source_store_url=source['store_url'],
        target_store_id=target['store_id'], target_store_url=target['store_url'],
        priority=priority, created_at=time.time()
    )
    
    replication_state.task_queue.put((task.priority.value, task.created_at, task))
    return task


def create_de_replication_task(photo_id: str, target_store_id: str, target_store_url: str) -> Optional[DeReplicationTask]:
    task_key = f"{photo_id}:{target_store_id}"
    with replication_state.stats_lock:
        if task_key in replication_state.pending_de_replication_keys:
            return None
        replication_state.pending_de_replication_keys.add(task_key)
    
    task = DeReplicationTask(task_id=str(uuid.uuid4()), photo_id=photo_id, target_store_id=target_store_id, target_store_url=target_store_url, created_at=time.time())
    replication_state.de_replication_queue.put(task)
    return task


def cancel_tasks_for_photo(photo_id: str):
    with replication_state.stats_lock:
        keys_to_remove = [key for key in replication_state.pending_task_keys if key.startswith(f"{photo_id}:")]
        for key in keys_to_remove:
            replication_state.pending_task_keys.discard(key)
        de_keys_to_remove = [key for key in replication_state.pending_de_replication_keys if key.startswith(f"{photo_id}:")]
        for key in de_keys_to_remove:
            replication_state.pending_de_replication_keys.discard(key)


def execute_replication_task(task: ReplicationTask) -> bool:
    with replication_state.stats_lock:
        if task.photo_id in replication_state.invalid_photos:
            task_key = f"{task.photo_id}:{task.target_store_id}"
            replication_state.pending_task_keys.discard(task_key)
            return False
    
    task.attempts += 1
    task.last_attempt = time.time()
    
    try:
        response = requests.get(f"{task.source_store_url}/read/{task.photo_id}", timeout=30)
        if response.status_code == 404:
            with replication_state.stats_lock:
                replication_state.invalid_photos.add(task.photo_id)
            return False
        if response.status_code != 200:
            return False
        
        photo_data = response.content
        checksum = hashlib.md5(photo_data).hexdigest()
        
        files = {'photo': ('photo.jpg', photo_data, 'image/jpeg')}
        data = {'photo_id': task.photo_id}
        response = requests.post(f"{task.target_store_url}/write", files=files, data=data, timeout=30)
        if response.status_code not in [200, 409]:
            return False
        
        result = response.json()
        volume_id = result['volume_id']
        
        leader_url = get_leader_url()
        response = requests.post(f"{leader_url}/register", json={'photo_id': task.photo_id, 'store_id': task.target_store_id, 'volume_id': volume_id, 'checksum': checksum, 'size_bytes': len(photo_data)}, timeout=10)
        if response.status_code != 200:
            return False
        
        with replication_state.stats_lock:
            replication_state.completed_tasks[task.task_id] = time.time()
            task_key = f"{task.photo_id}:{task.target_store_id}"
            replication_state.pending_task_keys.discard(task_key)
            if task.target_store_id in replication_state.failed_stores:
                replication_state.failed_stores[task.target_store_id] = max(0, replication_state.failed_stores[task.target_store_id] - 1)
        
        return True
    except Exception as e:
        logger.error(f"Task failed: {e}")
        with replication_state.stats_lock:
            if task.photo_id not in replication_state.invalid_photos:
                replication_state.failed_stores[task.target_store_id] += 1
        return False


def execute_de_replication_task(task: DeReplicationTask) -> bool:
    task.attempts += 1
    try:
        response = requests.delete(f"{task.target_store_url}/delete/{task.photo_id}", timeout=10)
        if response.status_code not in [200, 404]:
            return False
        
        leader_url = get_leader_url()
        response = requests.post(f"{leader_url}/unregister", json={'photo_id': task.photo_id, 'store_id': task.target_store_id}, timeout=10)
        if response.status_code not in [200, 404, 409]:
            return False
        
        with replication_state.stats_lock:
            task_key = f"{task.photo_id}:{task.target_store_id}"
            replication_state.pending_de_replication_keys.discard(task_key)
        
        return True
    except Exception as e:
        logger.error(f"De-replication failed: {e}")
        return False


def analyze_replication_needs(photo_id: str) -> Optional[int]:
    with aggregators_lock:
        aggregator = photo_aggregators.get(photo_id)
        if not aggregator:
            return DEFAULT_REPLICA_COUNT
        aggregate_rate = aggregator.get_aggregate_rate(force_recalculate=True)
        if aggregate_rate < COOL_DOWN_THRESHOLD:
            return DEFAULT_REPLICA_COUNT
        elif aggregate_rate >= HIGH_ACCESS_THRESHOLD:
            return min(MAX_REPLICA_COUNT, DEFAULT_REPLICA_COUNT + 2)
        elif aggregate_rate >= ACCESS_RATE_THRESHOLD:
            return DEFAULT_REPLICA_COUNT + 1
        else:
            return DEFAULT_REPLICA_COUNT


def update_photo_metadata(photo_id: str):
    try:
        locations = get_photo_locations(photo_id)
        if not is_photo_valid(photo_id, locations):
            return
        
        current_replica_count = len(locations)
        target_replica_count = analyze_replication_needs(photo_id) or DEFAULT_REPLICA_COUNT
        
        with replication_state.stats_lock:
            existing_metadata = replication_state.photos.get(photo_id)
            cool_down_start = None
            if existing_metadata and target_replica_count < existing_metadata.target_replica_count:
                cool_down_start = existing_metadata.cool_down_start or time.time()
            
            replication_state.photos[photo_id] = PhotoMetadata(
                photo_id=photo_id, current_replica_count=current_replica_count,
                target_replica_count=target_replica_count, locations=locations,
                last_checked=time.time(), created_at=existing_metadata.created_at if existing_metadata else time.time(),
                cool_down_start=cool_down_start
            )
        
        if current_replica_count > target_replica_count:
            if cool_down_start and (time.time() - cool_down_start) > DE_REPLICATION_GRACE_PERIOD:
                deficit = current_replica_count - target_replica_count
                stores_to_remove = select_stores_for_de_replication(photo_id, locations, deficit)
                for store in stores_to_remove:
                    create_de_replication_task(photo_id, store['store_id'], store['store_url'])
        
        elif current_replica_count < target_replica_count:
            deficit = target_replica_count - current_replica_count
            for _ in range(deficit):
                priority = TaskPriority.HIGH if target_replica_count > DEFAULT_REPLICA_COUNT else TaskPriority.NORMAL
                create_replication_task(photo_id, locations, priority)
    except Exception as e:
        logger.error(f"Error updating metadata: {e}")


# ============================================================================
# Background Workers with CRITICAL FIX
# ============================================================================

def replication_worker(worker_id: int):
    """
    CRITICAL FIX: Use (time, counter, task) tuples in heap
    """
    logger.info(f"Replication worker {worker_id} started")
    
    while True:
        try:
            task = None
            
            # Check retry heap
            with replication_state.retry_heap_lock:
                if replication_state.retry_heap:
                    # CRITICAL FIX: Unpack (time, counter, task)
                    next_retry_time, heap_id, next_task = replication_state.retry_heap[0]
                    
                    if time.time() >= next_retry_time:
                        heapq.heappop(replication_state.retry_heap)
                        task = next_task
            
            if not task:
                try:
                    priority, created_at, task = replication_state.task_queue.get(timeout=0.5)
                except queue.Empty:
                    time.sleep(0.1)
                    continue
            
            with replication_state.stats_lock:
                if task.photo_id in replication_state.invalid_photos:
                    task_key = f"{task.photo_id}:{task.target_store_id}"
                    replication_state.pending_task_keys.discard(task_key)
                    continue
            
            success = execute_replication_task(task)
            
            if not success and task.attempts < 3:
                backoff_time = min(5 * (2 ** (task.attempts - 1)), 60)
                task.next_retry_time = time.time() + backoff_time
                
                # CRITICAL FIX: Push (time, counter, task)
                with replication_state.retry_heap_lock:
                    heapq.heappush(
                        replication_state.retry_heap,
                        (task.next_retry_time, get_next_heap_id(), task)
                    )
            elif not success:
                with replication_state.stats_lock:
                    replication_state.failed_tasks[task.task_id] = task.attempts
                    task_key = f"{task.photo_id}:{task.target_store_id}"
                    replication_state.pending_task_keys.discard(task_key)
        
        except Exception as e:
            logger.error(f"Worker {worker_id} error: {e}", exc_info=True)
            time.sleep(1)


def de_replication_worker(worker_id: int):
    """
    CRITICAL FIX: Use (time, counter, task) tuples in heap
    This prevents TypeError when DeReplicationTask objects are compared
    """
    logger.info(f"De-replication worker {worker_id} started")
    
    while True:
        try:
            task = None
            
            # Check retry heap
            with replication_state.de_replication_heap_lock:
                if replication_state.de_replication_heap:
                    # CRITICAL FIX: Unpack (time, counter, task)
                    next_retry_time, heap_id, next_task = replication_state.de_replication_heap[0]
                    
                    if time.time() >= next_retry_time:
                        heapq.heappop(replication_state.de_replication_heap)
                        task = next_task
            
            if not task:
                try:
                    task = replication_state.de_replication_queue.get(timeout=0.5)
                except queue.Empty:
                    time.sleep(0.1)
                    continue
            
            with replication_state.stats_lock:
                if task.photo_id in replication_state.invalid_photos:
                    task_key = f"{task.photo_id}:{task.target_store_id}"
                    replication_state.pending_de_replication_keys.discard(task_key)
                    continue
            
            success = execute_de_replication_task(task)
            
            if not success and task.attempts < 3:
                backoff_time = min(5 * (2 ** (task.attempts - 1)), 60)
                task.next_retry_time = time.time() + backoff_time
                
                # CRITICAL FIX: Push (time, counter, task)
                with replication_state.de_replication_heap_lock:
                    heapq.heappush(
                        replication_state.de_replication_heap,
                        (task.next_retry_time, get_next_heap_id(), task)
                    )
            elif not success:
                with replication_state.stats_lock:
                    task_key = f"{task.photo_id}:{task.target_store_id}"
                    replication_state.pending_de_replication_keys.discard(task_key)
        
        except Exception as e:
            logger.error(f"De-rep worker {worker_id} error: {e}", exc_info=True)
            time.sleep(1)


def stats_analyzer_worker():
    logger.info("Stats analyzer started")
    while True:
        try:
            time.sleep(60)
            with aggregators_lock:
                photo_ids = list(photo_aggregators.keys())
            if not photo_ids:
                continue
            for photo_id in photo_ids:
                try:
                    with replication_state.stats_lock:
                        if photo_id in replication_state.invalid_photos:
                            continue
                    update_photo_metadata(photo_id)
                except Exception as e:
                    logger.error(f"Error analyzing {photo_id}: {e}")
        except Exception as e:
            logger.error(f"Stats analyzer error: {e}")
            time.sleep(10)


def monitoring_worker():
    logger.info("Monitoring worker started")
    while True:
        try:
            time.sleep(60)
            leader_url = get_leader_url()
            offset = 0
            while True:
                response = requests.get(f"{leader_url}/photos/list", params={"offset": offset, "limit": 1000}, timeout=10)
                if response.status_code != 200:
                    break
                photos = response.json().get('photos', [])
                if not photos:
                    break
                for photo in photos:
                    photo_id = photo['photo_id']
                    with replication_state.stats_lock:
                        if photo_id in replication_state.invalid_photos:
                            continue
                    locations = photo.get('locations', [])
                    if not isinstance(locations, list) or len(locations) == 0:
                        continue
                    if len(locations) < MIN_REPLICA_COUNT:
                        create_replication_task(photo_id, locations, TaskPriority.CRITICAL)
                offset += 1000
        except Exception as e:
            logger.error(f"Monitoring error: {e}")
            time.sleep(30)


def nightly_audit_worker():
    logger.info("Nightly audit started")
    while True:
        try:
            now = datetime.now()
            target = now.replace(hour=2, minute=0, second=0, microsecond=0)
            if now > target:
                target += timedelta(days=1)
            time.sleep((target - now).total_seconds())
            
            logger.info("Starting nightly audit")
            leader_url = get_leader_url()
            offset = 0
            while True:
                response = requests.get(f"{leader_url}/photos/list", params={"offset": offset, "limit": 1000}, timeout=30)
                if response.status_code != 200:
                    break
                photos = response.json().get('photos', [])
                if not photos:
                    break
                for photo in photos:
                    photo_id = photo['photo_id']
                    with replication_state.stats_lock:
                        if photo_id in replication_state.invalid_photos:
                            continue
                    locations = photo.get('locations', [])
                    if not isinstance(locations, list) or len(locations) == 0:
                        continue
                    current_count = len(locations)
                    target_count = analyze_replication_needs(photo_id) or DEFAULT_REPLICA_COUNT
                    if current_count < target_count:
                        deficit = target_count - current_count
                        for _ in range(deficit):
                            create_replication_task(photo_id, locations, TaskPriority.LOW)
                offset += 1000
        except Exception as e:
            logger.error(f"Nightly audit error: {e}")
            time.sleep(3600)


def circuit_breaker_reset_worker():
    while True:
        try:
            time.sleep(600)
            with replication_state.stats_lock:
                stores_to_reset = []
                for store_id, failure_count in replication_state.failed_stores.items():
                    if failure_count > 0:
                        new_count = failure_count - 1
                        if new_count > 0:
                            replication_state.failed_stores[store_id] = new_count
                        else:
                            stores_to_reset.append(store_id)
                for store_id in stores_to_reset:
                    del replication_state.failed_stores[store_id]
        except Exception as e:
            logger.error(f"Circuit breaker reset error: {e}")
            time.sleep(60)


def invalid_photos_cleanup_worker():
    while True:
        try:
            time.sleep(3600)
            with replication_state.stats_lock:
                invalid_photo_ids = list(replication_state.invalid_photos)
            recovered = 0
            for photo_id in invalid_photo_ids:
                try:
                    locations = get_photo_locations(photo_id)
                    if locations and len(locations) > 0:
                        with replication_state.stats_lock:
                            replication_state.invalid_photos.discard(photo_id)
                        recovered += 1
                except:
                    pass
            if recovered > 0:
                logger.info(f"Recovered {recovered} photos")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
            time.sleep(600)


# ============================================================================
# Application Lifecycle
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Replication Manager - FINAL PRODUCTION VERSION")
    
    for i in range(REPLICATION_WORKERS):
        threading.Thread(target=replication_worker, args=(i,), daemon=True).start()
    
    for i in range(DE_REPLICATION_WORKERS):
        threading.Thread(target=de_replication_worker, args=(i,), daemon=True).start()
    
    threading.Thread(target=stats_analyzer_worker, daemon=True).start()
    threading.Thread(target=monitoring_worker, daemon=True).start()
    threading.Thread(target=nightly_audit_worker, daemon=True).start()
    threading.Thread(target=circuit_breaker_reset_worker, daemon=True).start()
    threading.Thread(target=invalid_photos_cleanup_worker, daemon=True).start()
    
    logger.info("✅ All fixes applied - system production-ready")
    
    yield
    
    logger.info("Shutting down")


app = FastAPI(title="Replication Manager", lifespan=lifespan)


# API Endpoints (same as before)

@app.get("/health")
async def health_check():
    with replication_state.stats_lock:
        return {
            "status": "healthy",
            "pending_replication_tasks": replication_state.task_queue.qsize(),
            "pending_de_replication_tasks": replication_state.de_replication_queue.qsize(),
            "retry_heap_size": len(replication_state.retry_heap),
            "de_replication_heap_size": len(replication_state.de_replication_heap),
            "completed_tasks": len(replication_state.completed_tasks),
            "invalid_photos": len(replication_state.invalid_photos),
            "failed_stores": dict(replication_state.failed_stores)
        }


@app.post("/notify_new_photo")
async def notify_new_photo(request: dict):
    photo_id = request.get('photo_id')
    if not photo_id:
        raise HTTPException(status_code=400, detail="Missing photo_id")
    
    logger.info(f"New photo notification: {photo_id}")
    
    try:
        # Remove from invalid list if present (re-upload case)
        with replication_state.stats_lock:
            if photo_id in replication_state.invalid_photos:
                logger.info(f"Removing {photo_id} from invalid list (re-uploaded)")
                replication_state.invalid_photos.discard(photo_id)
        
        # DEFENSIVE: Retry with exponential backoff if photo not yet visible
        max_retries = 3
        for attempt in range(max_retries):
            locations = get_photo_locations(photo_id)
            
            if locations and len(locations) > 0:
                # Success - photo is visible
                logger.info(f"Photo {photo_id} visible with {len(locations)} locations")
                update_photo_metadata(photo_id)
                return {"success": True}
            
            if attempt < max_retries - 1:
                # Not visible yet, retry after brief delay
                retry_delay = 0.1 * (2 ** attempt)  # 0.1s, 0.2s, 0.4s
                logger.warning(f"Photo {photo_id} not yet visible, retrying in {retry_delay}s")
                time.sleep(retry_delay)
            else:
                # Final attempt failed
                logger.error(f"Photo {photo_id} not visible after {max_retries} attempts")
                # Don't mark as invalid - let cleanup worker handle it
                return {"success": False, "message": "Photo not yet visible, will retry later"}
        
        return {"success": True}
    
    except Exception as e:
        logger.error(f"Error handling new photo: {e}")
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/notify_deleted_photo")
async def notify_deleted_photo(request: dict):
    photo_id = request.get('photo_id')
    if not photo_id:
        raise HTTPException(status_code=400, detail="Missing photo_id")
    
    try:
        cancel_tasks_for_photo(photo_id)
        with replication_state.stats_lock:
            replication_state.invalid_photos.add(photo_id)
            replication_state.photos.pop(photo_id, None)
        with aggregators_lock:
            photo_aggregators.pop(photo_id, None)
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/stats/report")
async def report_stats(request: dict):
    try:
        store_id = request.get('store_id')
        timestamp = request.get('timestamp')
        access_stats = request.get('access_stats', [])
        
        if not store_id or not timestamp:
            raise HTTPException(status_code=400, detail="Missing store_id or timestamp")
        
        with aggregators_lock:
            for stat in access_stats:
                photo_id = stat.get('photo_id')
                ewma_rate = stat.get('ewma_rate_per_minute', 0.0)
                if not photo_id:
                    continue
                if photo_id not in photo_aggregators:
                    photo_aggregators[photo_id] = PhotoAccessAggregator(photo_id=photo_id)
                photo_aggregators[photo_id].update_store_rate(store_id, ewma_rate, timestamp)
        
        return {'success': True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
async def get_stats():
    with replication_state.stats_lock:
        return {
            "pending_replication_tasks": replication_state.task_queue.qsize(),
            "pending_de_replication_tasks": replication_state.de_replication_queue.qsize(),
            "retry_heap_size": len(replication_state.retry_heap),
            "de_replication_heap_size": len(replication_state.de_replication_heap),
            "completed_tasks": len(replication_state.completed_tasks),
            "failed_tasks": len(replication_state.failed_tasks),
            "invalid_photos": len(replication_state.invalid_photos),
            "failed_stores": dict(replication_state.failed_stores),
            "tracked_photos": len(replication_state.photos)
        }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9000)
