"""
Cache Service - Redis-based caching wrapper
IMPROVEMENTS:
- Issue #6 & #10: Redis replaces Python LRU cache for better performance
- Automatic LRU eviction, no GIL contention, guaranteed memory limits
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response, JSONResponse
import uvicorn
import os
import logging
import redis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("CacheService")

app = FastAPI(title="Cache Service")

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
CACHE_TTL = int(os.getenv("CACHE_TTL", 3600))  # 1 hour default

# Redis client
try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=False)
    redis_client.ping()
    logger.info("Connected to Redis successfully")
except Exception as e:
    logger.error(f"Redis connection failed: {e}")
    redis_client = None


@app.get("/cache/{photo_id}")
async def get_cached_photo(photo_id: str):
    """
    Get photo from Redis cache
    Returns photo data if found, 404 if not in cache
    """
    if not redis_client:
        raise HTTPException(status_code=503, detail="Cache unavailable")
    
    try:
        photo_data = redis_client.get(f"photo:{photo_id}")
        
        if photo_data is None:
            raise HTTPException(status_code=404, detail="Photo not in cache")
        
        # Increment hit counter
        redis_client.incr("cache:hits")
        
        return Response(
            content=photo_data,
            media_type="image/jpeg",
            headers={
                "X-Cache-Hit": "true",
                "X-Cache-Size": str(len(photo_data))
            }
        )
        
    except HTTPException:
        redis_client.incr("cache:misses")
        raise
    except Exception as e:
        logger.error(f"Error retrieving from cache: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/cache/{photo_id}")
async def set_cached_photo(photo_id: str, request: Request):
    """
    Add photo to Redis cache with TTL
    """
    if not redis_client:
        raise HTTPException(status_code=503, detail="Cache unavailable")
    
    try:
        photo_data = await request.body()
        
        if not photo_data:
            raise HTTPException(status_code=400, detail="No photo data provided")
        
        # Store in Redis with TTL
        redis_client.setex(f"photo:{photo_id}", CACHE_TTL, photo_data)
        
        # Update stats
        redis_client.incr("cache:sets")
        
        return {
            "success": True,
            "photo_id": photo_id,
            "size": len(photo_data),
            "ttl": CACHE_TTL,
            "message": "Photo cached successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error caching photo: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/cache/{photo_id}")
async def delete_cached_photo(photo_id: str):
    """
    Remove photo from cache (cache invalidation)
    """
    if not redis_client:
        raise HTTPException(status_code=503, detail="Cache unavailable")
    
    try:
        deleted = redis_client.delete(f"photo:{photo_id}")
        
        if deleted > 0:
            redis_client.incr("cache:deletes")
            return {
                "success": True,
                "photo_id": photo_id,
                "message": "Photo removed from cache"
            }
        else:
            return {
                "success": False,
                "photo_id": photo_id,
                "message": "Photo was not in cache"
            }
        
    except Exception as e:
        logger.error(f"Error deleting from cache: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/cache/clear")
async def clear_cache():
    """
    Clear entire cache (for administrative/testing purposes)
    """
    if not redis_client:
        raise HTTPException(status_code=503, detail="Cache unavailable")
    
    try:
        # Delete all photo keys
        cursor = 0
        deleted_count = 0
        
        while True:
            cursor, keys = redis_client.scan(cursor, match="photo:*", count=100)
            if keys:
                deleted_count += redis_client.delete(*keys)
            
            if cursor == 0:
                break
        
        return {
            "success": True,
            "message": f"Cache cleared: {deleted_count} photos removed"
        }
        
    except Exception as e:
        logger.error(f"Error clearing cache: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/cache/stats")
async def get_cache_stats():
    """
    Get detailed cache statistics from Redis
    """
    if not redis_client:
        raise HTTPException(status_code=503, detail="Cache unavailable")
    
    try:
        # Get Redis info
        info = redis_client.info('memory')
        
        # Get operation counts
        hits = int(redis_client.get("cache:hits") or 0)
        misses = int(redis_client.get("cache:misses") or 0)
        sets = int(redis_client.get("cache:sets") or 0)
        deletes = int(redis_client.get("cache:deletes") or 0)
        
        total_requests = hits + misses
        hit_rate = (hits / total_requests * 100) if total_requests > 0 else 0
        
        # Count photo keys
        photo_count = 0
        cursor = 0
        while True:
            cursor, keys = redis_client.scan(cursor, match="photo:*", count=100)
            photo_count += len(keys)
            if cursor == 0:
                break
        
        return {
            'size': {
                'current_bytes': info.get('used_memory', 0),
                'current_mb': round(info.get('used_memory', 0) / (1024**2), 2),
                'current_gb': round(info.get('used_memory', 0) / (1024**3), 2),
                'max_bytes': info.get('maxmemory', 0),
                'max_gb': round(info.get('maxmemory', 0) / (1024**3), 2),
                'utilization_percent': round(
                    (info.get('used_memory', 0) / info.get('maxmemory', 1)) * 100, 2
                ) if info.get('maxmemory', 0) > 0 else 0
            },
            'entries': {
                'count': photo_count
            },
            'operations': {
                'total_requests': total_requests,
                'hits': hits,
                'misses': misses,
                'hit_rate_percent': round(hit_rate, 2),
                'sets': sets,
                'deletes': deletes
            },
            'config': {
                'ttl_seconds': CACHE_TTL,
                'eviction_policy': 'allkeys-lru',
                'backend': 'Redis'
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting cache stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/cache/info")
async def get_cache_info():
    """
    Get basic cache information
    """
    if not redis_client:
        raise HTTPException(status_code=503, detail="Cache unavailable")
    
    try:
        info = redis_client.info('memory')
        
        # Count photos
        photo_count = 0
        cursor = 0
        while True:
            cursor, keys = redis_client.scan(cursor, match="photo:*", count=100)
            photo_count += len(keys)
            if cursor == 0:
                break
        
        return {
            'photo_count': photo_count,
            'current_size_gb': round(info.get('used_memory', 0) / (1024**3), 2),
            'max_size_gb': round(info.get('maxmemory', 0) / (1024**3), 2),
            'utilization_percent': round(
                (info.get('used_memory', 0) / info.get('maxmemory', 1)) * 100, 2
            ) if info.get('maxmemory', 0) > 0 else 0
        }
        
    except Exception as e:
        logger.error(f"Error getting cache info: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    if not redis_client:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "reason": "Redis unavailable"}
        )
    
    try:
        redis_client.ping()
        info = get_cache_info()
        
        return {
            "status": "healthy",
            "cache_size_gb": info['current_size_gb'],
            "photo_count": info['photo_count'],
            "utilization_percent": info['utilization_percent'],
            "backend": "Redis"
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "reason": str(e)}
        )


@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    logger.info("=" * 80)
    logger.info("Starting Cache Service (Redis-based)")
    logger.info("=" * 80)
    logger.info(f"Configuration:")
    logger.info(f"  - Redis URL: {REDIS_URL}")
    logger.info(f"  - TTL: {CACHE_TTL}s")
    logger.info(f"  - Eviction Policy: allkeys-lru (Redis managed)")
    
    if redis_client:
        logger.info("✓ Cache Service started successfully")
    else:
        logger.error("✗ Cache Service started with Redis unavailable")
    
    logger.info("=" * 80)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8100)