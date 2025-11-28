# Copyright (C) 2025 Sasha Shipka <sasha.shipka@copyleft.no>
# You may use, distribute and modify this code under the terms of the GNU General Public License v3.0

import asyncio
import json
import time
from urllib.parse import unquote

import pendulum
import uvloop
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .cache import CacheManager
from .config import config
from .precache import detect_change_tournaments_and_matches, clear_precache_data
from .prometheus import (
    REQUEST_COUNT,
    REQUEST_LATENCY,
    CACHE_HITS,
    CACHE_MISSES,
    CACHE_HIT_RATIO,
    record_cache_request,
    update_cache_size_metrics,
)
from .stats import StatsCollector
from .token import TokenManager
from .util import (
    extract_base_endpoint,
    get_http_client,
    get_logger,
    get_redis_client,
    process_query_params,
    redis_pool,
    refresh_base_data,
    background_tasks,
)

logger = get_logger(__name__)

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

app = FastAPI(
    title="NIF API Gateway",
    version="0.1.2",
    description="API Gateway for NIF API. This gateway is responsible for caching and logging requests to the external API.",
    openapi_url=f"{config.BASE_PATH}/openapi.json",
)

allowed_origins = [
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

token_manager = TokenManager()
cache_manager = CacheManager(token_manager)
stats_collector = StatsCollector()

# Global variable to track the precache task
precache_task = None


async def update_cache_metrics_periodically():
    """Periodically update cache size metrics"""
    while True:
        try:
            # Wait 60 seconds between updates
            await asyncio.sleep(60)
            
            # Get cache information
            redis_client = get_redis_client()
            keys = await redis_client.keys("GET:*")
            
            # Count total items and unique endpoints
            total_items = len(keys)
            unique_endpoints = set()
            
            for key in keys:
                key_str = key.decode("utf-8") if isinstance(key, bytes) else key
                endpoint = key_str.replace("GET:", "")
                unique_endpoints.add(endpoint)
            
            # Update metrics
            update_cache_size_metrics(total_items, len(unique_endpoints))
            
            await redis_client.close()
            
        except Exception as e:
            logger.error(f"Error updating cache metrics: {e}")
            # Continue the loop even if there's an error
            continue


@app.on_event("startup")
async def startup_event():
    global precache_task
    
    # Only start precache if configured to do so
    if config.PRECACHE_AUTO_START:
        logger.info("PRECACHE_AUTO_START is enabled - starting precache task on startup")
        precache_task = asyncio.create_task(detect_change_tournaments_and_matches(cache_manager, token_manager))
        background_tasks.add(precache_task)
        precache_task.add_done_callback(background_tasks.discard)
    else:
        logger.info("PRECACHE_AUTO_START is disabled - precache task not started on startup")
    
    # Start cache metrics update task
    cache_metrics_task = asyncio.create_task(update_cache_metrics_periodically())
    background_tasks.add(cache_metrics_task)
    cache_metrics_task.add_done_callback(background_tasks.discard)


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time

    # Add timing header
    response.headers["X-Process-Time"] = str(process_time)

    # Log stats about endpoint and timing to Redis
    if not request.url.path.startswith("/metrics") and not request.url.path.startswith(
        "/health"
    ):
        await stats_collector.log_request_stats(
            request, request.url.path, response.status_code, process_time
        )

    return response


@app.get("/robots.txt", summary="Robots.txt", tags=["Misc"])
async def robots():
    return Response(content="User-agent: *\nDisallow: /", media_type="text/plain")


@app.get("/stats", summary="Request statistics", tags=["Statistic"])
async def get_stats():
    """Get request statistics for all endpoints."""
    return await stats_collector.get_stats()


@app.get("/metrics", summary="Prometheus metrics", tags=["Statistic"])
async def metrics():
    """Endpoint for Prometheus metrics."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/cache/list", summary="List all cached endpoints", tags=["Cache"])
async def list_cache():
    return await cache_manager.list_cache()


@app.get("/cache/production-metrics", summary="Get production cache metrics", tags=["Cache", "Monitoring"])
async def get_production_metrics():
    """Get comprehensive cache metrics for production monitoring"""
    return await cache_manager.get_production_metrics()


@app.get("/redis/status", summary="Get quick Redis status", tags=["Cache", "Monitoring"])
async def get_redis_status():
    """Get quick Redis status for development monitoring"""
    redis = None
    try:
        redis = get_redis_client()
        
        # Get basic stats
        memory_info = await redis.info("memory")
        stats_info = await redis.info("stats")
        clients_info = await redis.info("clients")
        
        # Quick key counts
        total_keys = await redis.dbsize()
        
        # Hit ratio
        hits = stats_info.get('keyspace_hits', 0)
        misses = stats_info.get('keyspace_misses', 0)
        hit_ratio = (hits / (hits + misses) * 100) if (hits + misses) > 0 else 0
        
        status = {
            "status": "healthy",
            "memory": {
                "used_mb": round(memory_info.get('used_memory', 0) / 1024 / 1024, 2),
                "fragmentation_ratio": memory_info.get('mem_fragmentation_ratio', 0)
            },
            "performance": {
                "total_keys": total_keys,
                "hit_ratio_percent": round(hit_ratio, 2),
                "ops_per_sec": stats_info.get('instantaneous_ops_per_sec', 0),
                "connected_clients": clients_info.get('connected_clients', 0)
            },
            "timestamp": pendulum.now().isoformat()
        }
        
        return status
        
    except Exception as e:
        logger.error(f"Error getting Redis status: {e}")
        return {
            "status": "error",
            "error": str(e),
            "timestamp": pendulum.now().isoformat()
        }
    finally:
        if redis:
            await redis.close()


@app.post("/cache/optimize", summary="Optimize cache memory usage", tags=["Cache", "Monitoring"])
async def optimize_cache():
    """Optimize cache memory usage without hard limits"""
    return await cache_manager.optimize_memory_usage()


@app.delete(
    "/cache/{path:path}", summary="Clear cache for a specific endpoint", tags=["Cache"]
)
async def clear_cache(path: str):
    return await cache_manager.clear_cache(path)


@app.delete(
    "/cache",
    summary="Clear all cache",
    description="Clear all cached responses and autorefresh tasks",
    tags=["Cache"],
)
async def clear_all_cache():
    return await cache_manager.clear_all_cache()


@app.delete(
    "/precache",
    summary="Clear precache data",
    description="Clear all precache data (seasons, tournaments, matches, team IDs) from Redis",
    tags=["Cache"],
)
async def clear_precache():
    """Clear all precache data from Redis."""
    return await clear_precache_data()


@app.post(
    "/stop_precache",
    summary="Stop precache process",
    description="Stop the running precache periodic task",
    tags=["Cache"],
)
async def stop_precache():
    """Stop the running precache task."""
    global precache_task
    
    start_time = pendulum.now()
    logger.info(f"POST /stop_precache called at {start_time.isoformat()}")
    
    if precache_task is None:
        error_msg = "No precache task is currently running"
        logger.warning(f"POST /stop_precache failed: {error_msg}")
        return {
            "status": "error",
            "message": error_msg
        }
    
    if precache_task.done():
        error_msg = "Precache task is not running (already completed or cancelled)"
        logger.warning(f"POST /stop_precache failed: {error_msg}")
        return {
            "status": "error", 
            "message": error_msg
        }
    
    # Log task details before cancellation
    task_id = id(precache_task)
    logger.info(f"Attempting to cancel precache task with ID {task_id}")
    
    # Cancel the task
    precache_task.cancel()
    logger.info(f"Cancel signal sent to precache task {task_id}")
    
    try:
        # Wait for the task to be cancelled (with a short timeout)
        await asyncio.wait_for(precache_task, timeout=2.0)
        logger.info(f"Precache task {task_id} completed gracefully after cancel signal")
    except asyncio.CancelledError:
        # Task was successfully cancelled
        logger.info(f"Precache task {task_id} was successfully cancelled")
        pass
    except asyncio.TimeoutError:
        # Task didn't respond to cancellation quickly
        logger.warning(f"Precache task {task_id} did not respond to cancellation within 2 seconds")
        pass
    
    # Remove from background_tasks set
    background_tasks.discard(precache_task)
    logger.info(f"Precache task {task_id} removed from background_tasks, remaining count: {len(background_tasks)}")
    precache_task = None
    
    success_msg = "Precache task has been stopped"
    logger.info(f"POST /stop_precache completed successfully: {success_msg}")
    
    log_item = {
        "action": "stop_precache_endpoint",
        "timestamp": start_time.isoformat(),
        "status": "success", 
        "task_id": task_id,
        "background_tasks_count": len(background_tasks)
    }
    logger.info(f"logEndpoint: {json.dumps(log_item, ensure_ascii=False)}")
    
    return {
        "status": "success",
        "message": success_msg
    }


@app.post(
    "/start_precache",
    summary="Start precache process",
    description="Start the precache periodic task",
    tags=["Cache"],
)
async def start_precache():
    """Start the precache task."""
    global precache_task
    
    start_time = pendulum.now()
    logger.info(f"POST /start_precache called at {start_time.isoformat()}")
    
    # Check if a task is already running
    if precache_task is not None and not precache_task.done():
        error_msg = "A precache task is already running"
        logger.warning(f"POST /start_precache failed: {error_msg}")
        return {
            "status": "error",
            "message": error_msg
        }
    
    # Start a new precache task
    logger.info(f"Starting new precache task with detect_change_tournaments_and_matches function")
    precache_task = asyncio.create_task(detect_change_tournaments_and_matches(cache_manager, token_manager))
    background_tasks.add(precache_task)
    precache_task.add_done_callback(background_tasks.discard)
    
    # Log task creation details
    task_id = id(precache_task)
    logger.info(f"Precache task created successfully with ID {task_id}")
    logger.info(f"Task added to background_tasks, current background_tasks count: {len(background_tasks)}")
    
    success_msg = "Precache task has been started"
    logger.info(f"POST /start_precache completed successfully: {success_msg}")
    
    log_item = {
        "action": "start_precache_endpoint",
        "timestamp": start_time.isoformat(),
        "status": "success",
        "task_id": task_id,
        "background_tasks_count": len(background_tasks)
    }
    logger.info(f"logEndpoint: {json.dumps(log_item, ensure_ascii=False)}")
    
    return {
        "status": "success",
        "message": success_msg
    }


@app.get(
    "/health",
    summary="Health check",
    description="Check the health of the service, including Redis and external API",
    tags=["Health"],
)
async def health_check():
    """Health check endpoint."""
    client = None
    redis_client = None
    try:
        # Check Redis connection
        redis_client = get_redis_client()
        redis_ping = await redis_client.ping()

        # Check external API connection with a simple request
        try:
            external_api_status = "unknown"
            client = get_http_client()
            response = await client.get(config.API_URL)
            external_api_status = "connected" if response.status_code < 500 else "error"
        except Exception as api_e:
            external_api_status = f"error: {str(api_e)}"
        finally:
            if client:
                await client.aclose()

        return {
            "status": "healthy",
            "redis": "connected" if redis_ping else "error",
            "external_api": external_api_status,
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return Response(
            status_code=503,
            content=json.dumps({"status": "unhealthy", "error": str(e)}),
        )
    finally:
        if redis_client:
            await redis_client.close()


@app.get("/debug/connections", summary="Debug Redis connections", tags=["Debug"])
async def debug_connections():
    if hasattr(redis_pool, "_in_use_connections"):
        in_use = len(redis_pool._in_use_connections)
        available = redis_pool.max_connections - in_use
        return {
            "redis_connections": {
                "in_use": in_use,
                "available": available,
                "max": redis_pool.max_connections,
            }
        }
    return {"error": "Connection pool stats not available"}


@app.get(
    "/get-base-data",
    summary="Get combined base data",
    description="Get cached base data including sport info, match incident types, and seasons",
    tags=["Base Data"],
)
async def get_base_data():
    """Get the permanently stored base data"""
    cache_key = "permanent:base_data"

    # Track the request
    base_endpoint = "/get-base-data"
    REQUEST_COUNT.labels(method="GET", endpoint=base_endpoint, has_id="False").inc()
    redis_client = None

    with REQUEST_LATENCY.labels(method="GET", endpoint=base_endpoint).time():
        try:
            # Get data from Redis
            redis_client = get_redis_client()
            cached_data = await redis_client.get(cache_key)
            if cached_data:
                return Response(
                    content=cached_data,
                    media_type="application/json",
                    headers={"X-Cache": "HIT"},
                )
            else:
                # If data doesn't exist yet, return empty response
                return Response(
                    content=json.dumps(
                        {
                            "error": "Base data not found. Run the refresh task first.",
                            "status": "not_found",
                        },
                        ensure_ascii=False,
                    ),
                    status_code=404,
                    media_type="application/json",
                )
        except Exception as e:
            logger.error(f"Error in get_base_data: {e}")
            logger.exception(e)
            return Response(
                content=json.dumps({"error": str(e)}, ensure_ascii=False),
                status_code=500,
                media_type="application/json",
            )
        finally:
            if redis_client:
                await redis_client.close()


@app.post(
    "/refresh-base-data",
    summary="Trigger base data refresh",
    description="Trigger a refresh of the combined base data (sport, match incident types, seasons)",
    tags=["Base Data"],
)
async def trigger_refresh():
    """Trigger a refresh of the base data"""
    # Track the request
    base_endpoint = "/refresh-base-data"
    REQUEST_COUNT.labels(method="POST", endpoint=base_endpoint, has_id="False").inc()
    redis_client = None
    client = None

    try:
        # Run the refresh in a background task
        redis_client = get_redis_client()
        client = get_http_client()
        asyncio.create_task(refresh_base_data(redis_client, client, token_manager))

        return {
            "status": "refresh_started",
            "message": "Base data refresh has been started in the background",
        }
    except Exception as e:
        logger.error(f"Error triggering base data refresh: {e}")
        logger.exception(e)
        return Response(
            content=json.dumps({"error": str(e)}),
            status_code=500,
            media_type="application/json",
        )
    finally:
        if redis_client:
            await redis_client.close()
        if client:
            await client.aclose()


@app.get(
    "/direct/{path:path}",
    summary="Direct proxy to external API (no cache)",
    description="Forward requests directly to the external API without any caching",
    tags=["Proxy"],
)
async def direct_proxy(request: Request, path: str):
    """Forward requests directly to the external API without caching"""
    logger.info(f"Direct request for {path}")

    # Process path and parameters
    path = unquote(path.lower())
    params = dict(request.query_params) or {}

    # Handle path with embedded query params
    url_params = None
    paths = path.split("?")
    if len(paths) > 1:
        path, url_params = paths
    else:
        path = paths[0]

    # Merge URL params with query params
    if url_params:
        try:
            params.update(
                {k: v for k, v in [param.split("=") for param in url_params.split("&")]}
            )
        except ValueError:
            logger.warning(f"Invalid URL parameters: {url_params}")

    # Clean the path and build target URL
    path = path.replace("//", "/")
    target_url = f"{config.API_URL}/{path}"
    base_endpoint, has_id = extract_base_endpoint(path)

    logger.info(f"Direct request to: {target_url}")

    # Update request metrics
    REQUEST_COUNT.labels(
        method=request.method, endpoint=f"direct:{base_endpoint}", has_id=str(has_id)
    ).inc()

    with REQUEST_LATENCY.labels(method=request.method, endpoint=f"direct:{base_endpoint}").time():
        try:
            token = await token_manager.get_token()
            headers = dict(request.headers)
            headers["Authorization"] = f"Bearer {token['access_token']}"
            method = request.method.lower()
            request_kwargs = {
                "url": target_url,
                "headers": {
                    k: v
                    for k, v in headers.items()
                    if k.lower() not in ["host", "connection"]
                },
            }

            # Add body for POST/PUT requests
            if method in ["post", "put"]:
                body = await request.body()
                request_kwargs["content"] = body

            # Forward query parameters
            if params:
                request_kwargs["params"] = params

            # Make the direct request to external API (no caching)
            client = get_http_client()
            response = await getattr(client, method)(**request_kwargs)
            await client.aclose()

            # Create response headers, removing content-encoding
            response_headers = dict(response.headers)
            if "content-encoding" in response_headers:
                del response_headers["content-encoding"]

            # Return the response from the external API with direct indicator
            return Response(
                content=response.content,
                status_code=response.status_code,
                headers={**response_headers, "X-Cache": "DIRECT"},
            )
        except Exception as e:
            logger.error(f"Error in direct proxy request to {target_url}: {e}")
            logger.exception(e)
            return Response(
                content=json.dumps({"error": str(e)}),
                status_code=500,
                media_type="application/json",
            )


@app.get(
    "/{path:path}",
    summary="Proxy requests to external API",
    description="Forward requests to the external API as they are, by processing querystring to extract caching instructions",
    tags=["Proxy"],
)
async def proxy(request: Request, path: str):
    """Build the external API URL"""
    logger.info(f"Received request for {path}")

    # Process path and parameters
    path = unquote(path.lower())
    params = dict(request.query_params) or {}

    # Handle path with embedded query params
    url_params = None
    paths = path.split("?")
    if len(paths) > 1:
        path, url_params = paths
    else:
        path = paths[0]

    # Merge URL params with query params
    if url_params:
        try:
            params.update(
                {k: v for k, v in [param.split("=") for param in url_params.split("&")]}
            )
        except ValueError:
            logger.warning(f"Invalid URL parameters: {url_params}")

    # Extract special parameters (TTL, refresh, no_cache)
    params, ttl, autorefresh_until, no_cache = process_query_params(params)
    ttl = ttl or config.DEFAULT_TTL

    logger.info(
        f"Processed query params: {params}, TTL: {ttl}, autorefresh: {autorefresh_until}, no_cache: {no_cache}"
    )

    # Clean the path and build target URL
    path = path.replace("//", "/")
    target_url = f"{config.API_URL}/{path}"
    base_endpoint, has_id = extract_base_endpoint(path)

    # Check if this is a refresh request
    is_refresh = (
        autorefresh_until and autorefresh_until > pendulum.now()
    ) or no_cache == "refresh"

    # Build cache key including query parameters
    key_path = path
    if params:
        key_path += "?" + "&".join([f"{k}={v}" for k, v in params.items()])
    cache_key = f"{request.method}:{key_path}"

    # Update request metrics
    REQUEST_COUNT.labels(
        method=request.method, endpoint=base_endpoint, has_id=str(has_id)
    ).inc()

    # Check cache first
    if not no_cache and is_refresh:
        cache_data = await cache_manager.get_cached_response(cache_key)

        if cache_data:
            # Record cache hit with time-based metrics
            record_cache_request(base_endpoint, hit=True)

            # handle autorefresh
            if autorefresh_until and not is_refresh:
                await cache_manager.setup_refresh(
                    cache_key, target_url, ttl, autorefresh_until, params
                )

            headers = cache_data["headers"]
            if "content-encoding" in headers:
                del headers["content-encoding"]

            # Reconstruct the response with original status code and headers
            return Response(
                content=cache_data["content"],
                status_code=cache_data["status_code"],
                headers={**headers, "X-Cache": "HIT"},
            )

    # Record cache miss with time-based metrics
    record_cache_request(base_endpoint, hit=False)

    with REQUEST_LATENCY.labels(method=request.method, endpoint=base_endpoint).time():
        try:
            token = await token_manager.get_token()
            headers = dict(request.headers)
            headers["Authorization"] = f"Bearer {token['access_token']}"
            method = request.method.lower()
            request_kwargs = {
                "url": target_url,
                "headers": {
                    k: v
                    for k, v in headers.items()
                    if k.lower() not in ["host", "connection"]
                },
            }

            # Add body for POST/PUT requests
            if method in ["post", "put"]:
                body = await request.body()
                request_kwargs["content"] = body

            # Forward query parameters
            if params:
                request_kwargs["params"] = params

            # Make the request to external API
            client = get_http_client()
            response = await getattr(client, method)(**request_kwargs)
            await client.aclose()

            # Cache the result with TTL or 30s by default
            if request.method == "GET" and 200 <= response.status_code < 300:
                cached = await cache_manager.cache_response(cache_key, response, ttl)

                if cached and autorefresh_until:
                    await cache_manager.setup_refresh(
                        cache_key, target_url, ttl, autorefresh_until, params
                    )

            # Create response headers, removing content-encoding
            response_headers = dict(response.headers)
            if "content-encoding" in response_headers:
                del response_headers["content-encoding"]

            # Return the response from the external API
            return Response(
                content=response.content,
                status_code=response.status_code,
                headers={**response_headers, "X-Cache": "MISS"},
            )
        except Exception as e:
            logger.error(f"Error proxying request to {target_url}: {e}")
            logger.exception(e)

            # we need to check if there is cached data, and return cached data if available
            try:
                cache_data = await cache_manager.get_cached_response(cache_key)
                # also we want to apply the same logic as above of there is cached_data, set setup_refresh
                if cache_data:
                    logger.info("Returning cached data due to error")
                    if autorefresh_until and not is_refresh:
                        await cache_manager.setup_refresh(
                            cache_key, target_url, ttl, autorefresh_until, params
                        )

                    headers = cache_data["headers"]
                    if "content-encoding" in headers:
                        del headers["content-encoding"]

                    # Reconstruct the response with original status code and headers
                    return Response(
                        content=cache_data["content"],
                        status_code=cache_data["status_code"],
                        headers={**headers, "X-Cache": "HIT"},
                    )
            except Exception as ex:
                logger.error(f"Error getting cached data: {ex}")
                logger.exception(ex)

            return Response(
                content=json.dumps({"error": str(e)}),
                status_code=500,
                media_type="application/json",
            )
