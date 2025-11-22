"""
Replication Manager - Handles photo replication and dynamic replica adjustment
IMPROVEMENTS:
- Issue #4: Nightly audit for cold data monitoring
- Issue #11: Dynamic store discovery (FIXED: Queries Leader directly)
- Issue #12: Shorter stats window (60s)
- Issue #13: De-replication implementation
- FIX: Migrated to lifespan events
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from contextlib import asynccontextmanager
import uvicorn
import os
import json
import time
import threading
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set
import requests
from collections import defaultdict
from dataclasses import dataclass, asdict
from enum import Enum
import queue
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ReplicationManager")

# Configuration
DIRECTORY_SERVICE_URL = os.getenv("DIRECTORY_SERVICE_URL", "http://nginx")
DEFAULT_REPLICA_COUNT = int(os.getenv("DEFAULT_REPLICA_COUNT", 3))
MAX_REPLICA_COUNT = int(os.getenv("MAX_REPLICA_COUNT", 5))
MIN_REPLICA_COUNT = int(os.getenv("MIN_REPLICA_COUNT", 2))
ACCESS_RATE_THRESHOLD = int(os.getenv("ACCESS_RATE_THRESHOLD", 200))
HIGH_ACCESS_THRESHOLD = int(os.getenv("HIGH_ACCESS_THRESHOLD", 500))
WORKER_THREAD_COUNT = int(os.getenv("WORKER_THREAD_COUNT", 5))
STATS_COLLECTION_INTERVAL = int(os.getenv("STATS_COLLECTION_INTERVAL", 60))  # IMPROVEMENT #12
MONITORING_INTERVAL = int(os.getenv("MONITORING_INTERVAL", 60))
NIGHTLY_AUDIT_HOUR = int(os.getenv("NIGHTLY_AUDIT_HOUR", 2))  # IMPROVEMENT #4
DATA_DIRECTORY = os.getenv("DATA_DIRECTORY", "/data/replication")
TASK_QUEUE_FILE = os.path.join(DATA_DIRECTORY, "task_queue.json")


class TaskPriority(Enum):
    URGENT = 1
    HIGH = 2
    MEDIUM = 3
    LOW = 4


class TaskStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class ReplicationTask:
    task_id: str
    photo_id: str
    source_store_id: str
    source_store_url: str
    source_volume_id: str
    target_store_id: str
    target_store_url: str
    priority: int
    created_at: float
    retry_count: int = 0
    status: str = TaskStatus.PENDING.value
    last_attempt: Optional[float] = None
    error_message: Optional[str] = None


@dataclass
class DeReplicationTask:
    """IMPROVEMENT #13: Task for removing excess replicas"""
    task_id: str
    photo_id: str
    store_id: str
    store_url: str
    created_at: float
    status: str = TaskStatus.PENDING.value


@dataclass
class PhotoAccessStats:
    photo_id: str
    total_accesses: int
    window_start: float
    window_end: float
    current_replica_count: int
    target_replica_count: int
    access_rate_per_minute: float


class ReplicationState:
    """Manages replication manager state"""
    def __init__(self):
        self.task_queue = queue.PriorityQueue()
        self.tasks: Dict[str, ReplicationTask] = {}
        self.tasks_lock = threading.RLock()
        
        self.access_stats: Dict[str, PhotoAccessStats] = {}
        self.access_stats_lock = threading.RLock()
        
        # IMPROVEMENT #11: Dynamic store cache
        self.stores: Dict[str, dict] = {}
        self.stores_lock = threading.RLock()
        self.last_store_update = 0
        
        self.total_replications_completed = 0
        self.total_replications_failed = 0
        self.total_dereplication_completed = 0
        self.metrics_lock = threading.Lock()


# Global state
replication_state = ReplicationState()


# Persistence functions

def save_task_queue():
    """Persist pending tasks to disk"""
    try:
        os.makedirs(DATA_DIRECTORY, exist_ok=True)
        
        with replication_state.tasks_lock:
            pending_tasks = [
                asdict(task) for task in replication_state.tasks.values()
                if task.status in [TaskStatus.PENDING.value, TaskStatus.IN_PROGRESS.value]
            ]
        
        temp_file = TASK_QUEUE_FILE + ".tmp"
        with open(temp_file, 'w') as f:
            json.dump(pending_tasks, f, indent=2)
        
        os.replace(temp_file, TASK_QUEUE_FILE)
        logger.debug(f"Saved {len(pending_tasks)} pending tasks to disk")
        
    except Exception as e:
        logger.error(f"Error saving task queue: {e}", exc_info=True)


def load_task_queue():
    """Load pending tasks from disk"""
    try:
        if not os.path.exists(TASK_QUEUE_FILE):
            logger.info("No task queue file found, starting fresh")
            return
        
        with open(TASK_QUEUE_FILE, 'r') as f:
            tasks_data = json.load(f)
        
        for task_data in tasks_data:
            task = ReplicationTask(**task_data)
            with replication_state.tasks_lock:
                replication_state.tasks[task.task_id] = task
                replication_state.task_queue.put((task.priority, task.task_id))
        
        logger.info(f"Loaded {len(tasks_data)} tasks from disk")
        
    except Exception as e:
        logger.error(f"Error loading task queue: {e}", exc_info=True)


# IMPROVEMENT #11: Dynamic store discovery

def update_store_cache():
    """
    Fetch current list of healthy stores.
    CRITICAL FIX: Always queries the LEADER directly to avoid stale follower data.
    """
    try:
        # 1. Ask Nginx "Who is the leader?"
        try:
            response = requests.get(f"{DIRECTORY_SERVICE_URL}/stats", timeout=5)
            if response.status_code != 200:
                logger.warning(f"⚠ Failed to get directory stats from Nginx: {response.status_code}")
                # Fallback to querying Nginx for stores directly if stats fail
                target_url = DIRECTORY_SERVICE_URL
            else:
                stats = response.json()
                leader_url = stats.get('current_leader_url')
                
                if leader_url:
                    # We found the specific leader URL, use it!
                    target_url = leader_url
                else:
                    # Fallback to Nginx if leader URL is not exposed
                    target_url = DIRECTORY_SERVICE_URL
        except Exception as e:
            logger.error(f"Error fetching stats: {e}")
            target_url = DIRECTORY_SERVICE_URL

        # 2. Query the target (Leader) for healthy stores
        # This bypasses the stale follower problem
        response = requests.get(
            f"{target_url}/stores",
            params={'healthy_only': True},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            stores = data['stores']
            
            with replication_state.stores_lock:
                replication_state.stores.clear()
                for store in stores:
                    replication_state.stores[store['store_id']] = store
                replication_state.last_store_update = time.time()
            
            # Simplified logging
            if len(stores) > 0:
                logger.info(f"✓ Updated store cache from {target_url}: {len(stores)} accessible stores")
            else:
                logger.warning(f"⚠ Updated store cache from {target_url}: 0 accessible stores (System might be initializing)")
                
            return True
        else:
            logger.warning(f"⚠ Failed to fetch stores from {target_url}: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"✗ Error updating store cache: {e}")
        return False


def get_available_stores() -> List[dict]:
    """Get list of healthy stores (with caching)"""
    # Refresh cache if stale (> 30 seconds old)
    if time.time() - replication_state.last_store_update > 30:
        update_store_cache()
    
    with replication_state.stores_lock:
        return list(replication_state.stores.values())


def select_target_store(photo_id: str, existing_store_ids: Set[str]) -> Optional[dict]:
    """Select the best store for a new replica"""
    try:
        available_stores = get_available_stores()
        
        if not available_stores:
            logger.warning("No available stores for replication")
            return None
        
        candidate_stores = [
            store for store in available_stores
            if store['store_id'] not in existing_store_ids
        ]
        
        if not candidate_stores:
            logger.warning(f"No candidate stores for photo {photo_id}")
            return None
        
        best_store = max(candidate_stores, key=lambda s: s.get('available_capacity', 0))
        
        logger.info(f"Selected {best_store['store_id']} for replication")
        
        return best_store
        
    except Exception as e:
        logger.error(f"Error selecting target store: {e}", exc_info=True)
        return None


# Task management functions

def create_replication_task(
    photo_id: str,
    source_store_id: str,
    source_store_url: str,
    source_volume_id: str,
    target_store_id: str,
    target_store_url: str,
    priority: TaskPriority
) -> ReplicationTask:
    """Create a new replication task"""
    
    task_id = f"repl-{photo_id[:8]}-{target_store_id}-{int(time.time() * 1000)}"
    
    task = ReplicationTask(
        task_id=task_id,
        photo_id=photo_id,
        source_store_id=source_store_id,
        source_store_url=source_store_url,
        source_volume_id=source_volume_id,
        target_store_id=target_store_id,
        target_store_url=target_store_url,
        priority=priority.value,
        created_at=time.time()
    )
    
    with replication_state.tasks_lock:
        replication_state.tasks[task_id] = task
        replication_state.task_queue.put((priority.value, task_id))
    
    logger.info(f"Created task {task_id}: {photo_id} {source_store_id}→{target_store_id} (priority: {priority.name})")
    
    return task


def execute_replication_task(task: ReplicationTask) -> bool:
    """Execute a single replication task"""
    logger.info(f"Executing {task.task_id}: replicating {task.photo_id} from {task.source_store_id} to {task.target_store_id}")
    
    try:
        with replication_state.tasks_lock:
            task.status = TaskStatus.IN_PROGRESS.value
            task.last_attempt = time.time()
        
        replicate_request = {
            'photo_id': task.photo_id,
            'source_store_url': task.source_store_url,
            'source_volume_id': task.source_volume_id
        }
        
        response = requests.post(
            f"{task.target_store_url}/replicate",
            json=replicate_request,
            timeout=60
        )
        
        if response.status_code != 200:
            raise Exception(f"Replication failed with status {response.status_code}: {response.text}")
        
        result = response.json()
        logger.info(f"Successfully replicated {task.photo_id} to {task.target_store_id}")
        
        # Register the new location with Directory
        register_request = {
            'photo_id': task.photo_id,
            'store_id': result['store_id'],
            'volume_id': result['volume_id']
        }
        
        response = requests.post(
            f"{DIRECTORY_SERVICE_URL}/register",
            json=register_request,
            timeout=10
        )
        
        if response.status_code != 200:
            logger.error(f"Failed to register replica with directory: {response.status_code}")
        else:
            logger.info(f"Registered new replica location for {task.photo_id}")
        
        with replication_state.tasks_lock:
            task.status = TaskStatus.COMPLETED.value
        
        with replication_state.metrics_lock:
            replication_state.total_replications_completed += 1
        
        logger.info(f"✓ Task {task.task_id} completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"✗ Task {task.task_id} failed: {e}", exc_info=True)
        
        with replication_state.tasks_lock:
            task.status = TaskStatus.FAILED.value
            task.retry_count += 1
            task.error_message = str(e)
        
        with replication_state.metrics_lock:
            replication_state.total_replications_failed += 1
        
        # Retry logic
        if task.retry_count < 3:
            delay = 2 ** task.retry_count
            logger.info(f"Will retry task {task.task_id} in {delay}s (attempt {task.retry_count + 1}/3)")
            
            threading.Timer(
                delay,
                lambda: replication_state.task_queue.put((task.priority, task.task_id))
            ).start()
            
            with replication_state.tasks_lock:
                task.status = TaskStatus.PENDING.value
        else:
            logger.error(f"Task {task.task_id} failed after {task.retry_count} retries")
        
        return False


def execute_dereplication(photo_id: str, store_id: str, store_url: str) -> bool:
    """
    IMPROVEMENT #13: Execute de-replication task
    """
    logger.info(f"De-replicating {photo_id} from {store_id}")
    
    try:
        # Delete from store
        response = requests.post(
            f"{store_url}/delete/{photo_id}",
            timeout=10
        )
        
        if response.status_code not in [200, 404]:
            raise Exception(f"Delete failed with status {response.status_code}")
        
        # Update directory to remove this replica
        response = requests.post(
            f"{DIRECTORY_SERVICE_URL}/internal/remove_replica",
            json={'photo_id': photo_id, 'store_id': store_id},
            timeout=10
        )
        
        if response.status_code != 200:
            logger.warning(f"Failed to update directory for de-replication: {response.status_code}")
        
        with replication_state.metrics_lock:
            replication_state.total_dereplication_completed += 1
        
        logger.info(f"✓ De-replicated {photo_id} from {store_id}")
        return True
        
    except Exception as e:
        logger.error(f"De-replication failed: {e}")
        return False


# Photo analysis functions

def get_photo_locations(photo_id: str) -> List[dict]:
    """
    Get all locations where a photo is stored
    IMPROVEMENT: Always queries through Nginx which routes to leader/followers
    """
    try:
        logger.debug(f"→ Fetching locations for photo {photo_id[:16]}...")
        
        response = requests.get(
            f"{DIRECTORY_SERVICE_URL}/locate/{photo_id}",
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            locations = data.get('locations', [])
            logger.debug(f"✓ Found {len(locations)} locations for {photo_id[:16]}...")
            return locations
        elif response.status_code == 404:
            logger.debug(f"⚠ Photo {photo_id[:16]}... not found in directory")
            return []
        else:
            logger.error(f"✗ Failed to locate photo {photo_id[:16]}...: {response.status_code}")
            return []
        
    except Exception as e:
        logger.error(f"✗ Error locating photo {photo_id[:16]}...: {e}", exc_info=True)
        return []


def analyze_replication_needs(photo_id: str, access_stats: Optional[PhotoAccessStats] = None):
    """Analyze if a photo needs more or fewer replicas"""
    try:
        logger.debug(f"→ Analyzing replication needs for {photo_id[:16]}...")
        
        locations = get_photo_locations(photo_id)
        current_replica_count = len(locations)
        
        if current_replica_count == 0:
            logger.warning(f"⚠ Photo {photo_id[:16]}... has no replicas, cannot replicate")
            return
        
        logger.info(f"Photo {photo_id[:16]}... currently has {current_replica_count} replica(s)")
        
        # Determine target replica count
        target_replica_count = DEFAULT_REPLICA_COUNT
        
        if access_stats:
            if access_stats.access_rate_per_minute >= HIGH_ACCESS_THRESHOLD:
                target_replica_count = min(MAX_REPLICA_COUNT, DEFAULT_REPLICA_COUNT + 2)
                logger.info(f"  ✓ Very hot photo ({access_stats.access_rate_per_minute:.1f} req/min), "
                          f"increasing target to {target_replica_count}")
            elif access_stats.access_rate_per_minute >= ACCESS_RATE_THRESHOLD:
                target_replica_count = min(MAX_REPLICA_COUNT, DEFAULT_REPLICA_COUNT + 1)
                logger.info(f"  ✓ Hot photo ({access_stats.access_rate_per_minute:.1f} req/min), "
                          f"increasing target to {target_replica_count}")
        
        replicas_needed = target_replica_count - current_replica_count
        
        if replicas_needed > 0:
            # Need more replicas
            logger.info(f"  → Photo {photo_id[:16]}... needs {replicas_needed} more replica(s) "
                       f"(current: {current_replica_count}, target: {target_replica_count})")
            
            if current_replica_count == 1:
                priority = TaskPriority.URGENT
                logger.warning(f"  ⚠ URGENT: Only 1 replica exists!")
            elif current_replica_count < MIN_REPLICA_COUNT:
                priority = TaskPriority.HIGH
                logger.warning(f"  ⚠ HIGH: Below minimum replica count ({MIN_REPLICA_COUNT})")
            elif access_stats and access_stats.access_rate_per_minute >= HIGH_ACCESS_THRESHOLD:
                priority = TaskPriority.HIGH
            elif current_replica_count < DEFAULT_REPLICA_COUNT:
                priority = TaskPriority.MEDIUM
            else:
                priority = TaskPriority.LOW
            
            existing_store_ids = {loc['store_id'] for loc in locations}
            logger.debug(f"  Existing stores: {existing_store_ids}")
            
            for i in range(replicas_needed):
                source = locations[0]
                target_store = select_target_store(photo_id, existing_store_ids)
                
                if not target_store:
                    logger.warning(f"  ✗ Cannot create replica {i+1}/{replicas_needed} for {photo_id[:16]}...: no available target store")
                    break
                
                logger.info(f"  → Creating replication task {i+1}/{replicas_needed}: "
                          f"{source['store_id']} → {target_store['store_id']} (priority: {priority.name})")
                
                create_replication_task(
                    photo_id=photo_id,
                    source_store_id=source['store_id'],
                    source_store_url=source['store_url'],
                    source_volume_id=source['volume_id'],
                    target_store_id=target_store['store_id'],
                    target_store_url=target_store['store_url'],
                    priority=priority
                )
                
                existing_store_ids.add(target_store['store_id'])
        
        elif replicas_needed < 0:
            # IMPROVEMENT #13: De-replication
            excess_count = abs(replicas_needed)
            logger.info(f"  → Photo {photo_id[:16]}... has {excess_count} excess replica(s) "
                       f"(current: {current_replica_count}, target: {target_replica_count})")
            
            # Get store capacities
            store_capacities = {}
            available_stores = get_available_stores()
            
            for loc in locations:
                store = next((s for s in available_stores if s['store_id'] == loc['store_id']), None)
                if store:
                    store_capacities[loc['store_id']] = store['available_capacity']
            
            # Remove replicas from stores with LEAST available space
            sorted_locations = sorted(
                locations,
                key=lambda loc: store_capacities.get(loc['store_id'], float('inf'))
            )
            
            for i in range(min(excess_count, len(sorted_locations) - MIN_REPLICA_COUNT)):
                loc_to_remove = sorted_locations[i]
                
                logger.info(f"  → Scheduling de-replication {i+1}/{excess_count}: "
                          f"remove {photo_id[:16]}... from {loc_to_remove['store_id']}")
                
                # Execute de-replication in background
                threading.Thread(
                    target=execute_dereplication,
                    args=(photo_id, loc_to_remove['store_id'], loc_to_remove['store_url']),
                    daemon=True
                ).start()
        else:
            logger.debug(f"  ✓ Photo {photo_id[:16]}... has optimal replica count ({current_replica_count})")
        
    except Exception as e:
        logger.error(f"✗ Error analyzing replication needs for {photo_id[:16]}...: {e}", exc_info=True)

# Background workers

def replication_worker(worker_id: int):
    """Worker thread that processes replication tasks"""
    logger.info(f"Replication worker {worker_id} started")
    
    while True:
        try:
            priority, task_id = replication_state.task_queue.get(timeout=5)
            
            with replication_state.tasks_lock:
                if task_id not in replication_state.tasks:
                    logger.warning(f"Worker {worker_id}: Task {task_id} not found")
                    continue
                
                task = replication_state.tasks[task_id]
            
            logger.info(f"Worker {worker_id}: Processing task {task_id}")
            success = execute_replication_task(task)
            
            if success:
                logger.info(f"Worker {worker_id}: ✓ Task {task_id} completed")
            else:
                logger.warning(f"Worker {worker_id}: ✗ Task {task_id} failed")
            
        except queue.Empty:
            continue
        except Exception as e:
            logger.error(f"Worker {worker_id}: Unexpected error: {e}", exc_info=True)
            time.sleep(1)


def stats_analyzer_worker():
    """Periodically analyze access statistics"""
    logger.info("Stats analyzer worker started")
    
    while True:
        try:
            time.sleep(STATS_COLLECTION_INTERVAL)
            
            logger.info("Starting access statistics analysis")
            
            with replication_state.access_stats_lock:
                photos_to_analyze = list(replication_state.access_stats.items())
            
            logger.info(f"Analyzing {len(photos_to_analyze)} photos with access statistics")
            
            for photo_id, stats in photos_to_analyze:
                try:
                    analyze_replication_needs(photo_id, stats)
                except Exception as e:
                    logger.error(f"Error analyzing {photo_id}: {e}", exc_info=True)
            
            # Reset stats for next window
            with replication_state.access_stats_lock:
                current_time = time.time()
                for stats in replication_state.access_stats.values():
                    stats.total_accesses = 0
                    stats.window_start = current_time
                    stats.access_rate_per_minute = 0
            
            logger.info("Access statistics analysis complete")
            
        except Exception as e:
            logger.error(f"Error in stats analyzer: {e}", exc_info=True)


def monitoring_worker():
    """Periodically check known photos for under-replication"""
    logger.info("Monitoring worker started")
    
    time.sleep(30)
    
    while True:
        try:
            time.sleep(MONITORING_INTERVAL)
            
            logger.info("Starting replication monitoring sweep")
            
            with replication_state.access_stats_lock:
                known_photos = list(replication_state.access_stats.keys())
            
            if not known_photos:
                logger.debug("No photos to monitor yet")
                continue
            
            logger.info(f"Monitoring {len(known_photos)} known photos")
            
            under_replicated_count = 0
            
            for photo_id in known_photos:
                try:
                    locations = get_photo_locations(photo_id)
                    replica_count = len(locations)
                    
                    if replica_count < MIN_REPLICA_COUNT:
                        logger.warning(f"Photo {photo_id} under-replicated: {replica_count}/{MIN_REPLICA_COUNT}")
                        analyze_replication_needs(photo_id, None)
                        under_replicated_count += 1
                    elif replica_count < DEFAULT_REPLICA_COUNT:
                        logger.info(f"Photo {photo_id} below target: {replica_count}/{DEFAULT_REPLICA_COUNT}")
                        analyze_replication_needs(photo_id, None)
                        under_replicated_count += 1
                    
                except Exception as e:
                    logger.error(f"Error monitoring {photo_id}: {e}", exc_info=True)
            
            if under_replicated_count > 0:
                logger.info(f"Found {under_replicated_count} under-replicated photos")
            else:
                logger.info("All monitored photos are properly replicated")
            
        except Exception as e:
            logger.error(f"Error in monitoring worker: {e}", exc_info=True)


def nightly_audit_worker():
    """
    IMPROVEMENT #4: Full scan of all photos (nightly at 2 AM)
    """
    logger.info(f"Nightly audit worker started (runs at {NIGHTLY_AUDIT_HOUR}:00)")
    
    while True:
        try:
            # Calculate seconds until next audit time
            now = datetime.now()
            target = now.replace(hour=NIGHTLY_AUDIT_HOUR, minute=0, second=0, microsecond=0)
            
            if now >= target:
                target += timedelta(days=1)
            
            sleep_seconds = (target - now).total_seconds()
            logger.info(f"Next audit in {sleep_seconds / 3600:.1f} hours")
            
            time.sleep(sleep_seconds)
            
            logger.info("=" * 80)
            logger.info("Starting NIGHTLY FULL AUDIT of all photos")
            logger.info("=" * 80)
            
            offset = 0
            limit = 1000
            total_checked = 0
            under_replicated = 0
            
            while True:
                try:
                    response = requests.get(
                        f"{DIRECTORY_SERVICE_URL}/photos/list",
                        params={'offset': offset, 'limit': limit},
                        timeout=30
                    )
                    
                    if response.status_code != 200:
                        logger.error(f"Failed to fetch photo list: {response.status_code}")
                        break
                    
                    data = response.json()
                    photos = data['photos']
                    total = data['total']
                    
                    logger.info(f"Auditing batch {offset}-{offset+len(photos)} of {total}")
                    
                    for photo in photos:
                        photo_id = photo['photo_id']
                        replica_count = photo['replica_count']
                        target_replicas = photo['target_replicas']
                        
                        total_checked += 1
                        
                        if replica_count < MIN_REPLICA_COUNT:
                            logger.warning(
                                f"AUDIT ALERT: {photo_id} has only {replica_count} replica(s)! "
                                f"(target: {target_replicas})"
                            )
                            analyze_replication_needs(photo_id, None)
                            under_replicated += 1
                        elif replica_count < target_replicas:
                            logger.info(f"AUDIT: {photo_id} below target ({replica_count}/{target_replicas})")
                            analyze_replication_needs(photo_id, None)
                            under_replicated += 1
                    
                    offset += limit
                    
                    if offset >= total:
                        break
                    
                    time.sleep(1)  # Small delay between batches
                    
                except Exception as e:
                    logger.error(f"Error in audit batch: {e}", exc_info=True)
                    break
            
            logger.info("=" * 80)
            logger.info(f"NIGHTLY AUDIT COMPLETE")
            logger.info(f"  Total photos checked: {total_checked}")
            logger.info(f"  Under-replicated: {under_replicated}")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"Error in nightly audit worker: {e}", exc_info=True)
            time.sleep(3600)  # Wait 1 hour before retry


def store_info_updater():
    """
    IMPROVEMENT #11: Periodically update store information cache
    """
    logger.info("Store info updater started")
    
    while True:
        try:
            time.sleep(30)
            update_store_cache()
            
        except Exception as e:
            logger.error(f"Error in store info updater: {e}", exc_info=True)


def persistence_worker():
    """Periodically save task queue to disk"""
    logger.info("Persistence worker started")
    
    while True:
        try:
            time.sleep(60)
            save_task_queue()
        except Exception as e:
            logger.error(f"Error in persistence worker: {e}", exc_info=True)


# Lifespan Manager (Replaces on_event)
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize background tasks and clean up on shutdown"""
    logger.info("=" * 80)
    logger.info("Starting Replication Manager")
    logger.info("=" * 80)
    logger.info(f"Configuration:")
    logger.info(f"  - Directory Service: {DIRECTORY_SERVICE_URL}")
    logger.info(f"  - Default Replica Count: {DEFAULT_REPLICA_COUNT}")
    logger.info(f"  - Min/Max Replica Count: {MIN_REPLICA_COUNT}/{MAX_REPLICA_COUNT}")
    logger.info(f"  - Access Rate Threshold: {ACCESS_RATE_THRESHOLD} req/min")
    logger.info(f"  - Worker Threads: {WORKER_THREAD_COUNT}")
    logger.info(f"  - Stats Collection Interval: {STATS_COLLECTION_INTERVAL}s")
    logger.info(f"  - Nightly Audit Hour: {NIGHTLY_AUDIT_HOUR}:00")
    
    load_task_queue()
    
    # Initial store cache update
    update_store_cache()
    
    # Start worker threads
    logger.info(f"Starting {WORKER_THREAD_COUNT} replication worker threads")
    for i in range(WORKER_THREAD_COUNT):
        worker_thread = threading.Thread(
            target=replication_worker,
            args=(i + 1,),
            daemon=True,
            name=f"ReplicationWorker-{i+1}"
        )
        worker_thread.start()
    
    threading.Thread(target=stats_analyzer_worker, daemon=True, name="StatsAnalyzer").start()
    threading.Thread(target=monitoring_worker, daemon=True, name="Monitor").start()
    threading.Thread(target=nightly_audit_worker, daemon=True, name="NightlyAudit").start()
    threading.Thread(target=store_info_updater, daemon=True, name="StoreInfoUpdater").start()
    threading.Thread(target=persistence_worker, daemon=True, name="Persistence").start()
    
    logger.info("=" * 80)
    logger.info("✓ Replication Manager started successfully")
    logger.info("=" * 80)
    
    yield
    
    # Shutdown
    logger.info("Shutting down Replication Manager")
    save_task_queue()
    logger.info("✓ Replication Manager shut down")


app = FastAPI(title="Replication Manager", lifespan=lifespan)

# API Endpoints

@app.post("/stats/report")
async def report_stats(request: dict):
    """Receive access statistics from Store Services"""
    try:
        store_id = request['store_id']
        window_start = request['window_start']
        window_end = request['window_end']
        access_data = request['access_data']
        
        logger.info(f"Received stats from {store_id}: {len(access_data)} photos")
        
        with replication_state.access_stats_lock:
            for item in access_data:
                photo_id = item['photo_id']
                access_count = item['access_count']
                
                if photo_id not in replication_state.access_stats:
                    replication_state.access_stats[photo_id] = PhotoAccessStats(
                        photo_id=photo_id,
                        total_accesses=0,
                        window_start=window_start,
                        window_end=window_end,
                        current_replica_count=0,
                        target_replica_count=DEFAULT_REPLICA_COUNT,
                        access_rate_per_minute=0
                    )
                
                stats = replication_state.access_stats[photo_id]
                stats.total_accesses += access_count
                stats.window_end = max(stats.window_end, window_end)
                
                window_duration_minutes = (stats.window_end - stats.window_start) / 60.0
                if window_duration_minutes > 0:
                    stats.access_rate_per_minute = stats.total_accesses / window_duration_minutes
        
        return {"success": True, "message": f"Processed stats for {len(access_data)} photos"}
        
    except Exception as e:
        logger.error(f"Error processing stats report: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/replication/trigger")
async def trigger_replication(request: dict):
    """Manually trigger replication for a photo"""
    try:
        photo_id = request['photo_id']
        
        logger.info(f"Replication triggered for {photo_id}")
        
        threading.Thread(
            target=analyze_replication_needs,
            args=(photo_id, None),
            daemon=True
        ).start()
        
        return {"success": True, "photo_id": photo_id, "message": "Replication analysis scheduled"}
        
    except Exception as e:
        logger.error(f"Error triggering replication: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/replication/status/{photo_id}")
async def get_replication_status(photo_id: str):
    """Get replication status for a specific photo"""
    try:
        locations = get_photo_locations(photo_id)
        
        with replication_state.tasks_lock:
            pending_tasks = [
                {
                    'task_id': task.task_id,
                    'target_store_id': task.target_store_id,
                    'status': task.status,
                    'priority': task.priority
                }
                for task in replication_state.tasks.values()
                if task.photo_id == photo_id and task.status in [TaskStatus.PENDING.value, TaskStatus.IN_PROGRESS.value]
            ]
        
        with replication_state.access_stats_lock:
            stats = replication_state.access_stats.get(photo_id)
            access_info = None
            if stats:
                access_info = {
                    'total_accesses': stats.total_accesses,
                    'access_rate_per_minute': stats.access_rate_per_minute
                }
        
        return {
            'photo_id': photo_id,
            'current_replica_count': len(locations),
            'locations': locations,
            'pending_tasks': pending_tasks,
            'access_stats': access_info
        }
        
    except Exception as e:
        logger.error(f"Error getting replication status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
async def get_stats():
    """Get Replication Manager statistics"""
    with replication_state.tasks_lock:
        pending_count = len([t for t in replication_state.tasks.values() if t.status == TaskStatus.PENDING.value])
        in_progress_count = len([t for t in replication_state.tasks.values() if t.status == TaskStatus.IN_PROGRESS.value])
    
    with replication_state.access_stats_lock:
        photos_tracked = len(replication_state.access_stats)
        hot_photos = len([s for s in replication_state.access_stats.values() if s.access_rate_per_minute >= ACCESS_RATE_THRESHOLD])
    
    with replication_state.metrics_lock:
        completed = replication_state.total_replications_completed
        failed = replication_state.total_replications_failed
        derepli = replication_state.total_dereplication_completed
    
    return {
        'tasks': {
            'pending': pending_count,
            'in_progress': in_progress_count,
            'completed': completed,
            'failed': failed,
            'dereplicated': derepli,
            'queue_depth': replication_state.task_queue.qsize()
        },
        'access_stats': {
            'photos_tracked': photos_tracked,
            'hot_photos': hot_photos
        },
        'configuration': {
            'default_replica_count': DEFAULT_REPLICA_COUNT,
            'min_replica_count': MIN_REPLICA_COUNT,
            'max_replica_count': MAX_REPLICA_COUNT,
            'access_rate_threshold': ACCESS_RATE_THRESHOLD,
            'worker_threads': WORKER_THREAD_COUNT
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        'status': 'healthy',
        'worker_threads': WORKER_THREAD_COUNT,
        'queue_depth': replication_state.task_queue.qsize()
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9003)