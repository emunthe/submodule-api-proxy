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
from .precache import detect_change_tournaments_and_matches, get_tournaments_and_matches
from .prometheus import (
    CACHE_HITS,
    CACHE_MISSES,
    REQUEST_COUNT,
    REQUEST_LATENCY,
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
    version="0.2",
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
    task = asyncio.create_task(detect_change_tournaments_and_matches(cache_manager, token_manager))
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)
    
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


@app.get(
    "/get_all_stored_data",
    summary="Get cached season/tournament data",
    tags=["Cache"],
)
async def get_all_stored_data():
    redis_client = get_redis_client()
    try:
        async def _get_cached(key: str):
            raw = await redis_client.get(key)
            if not raw:
                return None
            try:
                return json.loads(raw).get("data")
            except Exception:
                return None

        cached_valid_seasons = await _get_cached("valid_seasons")
        cached_tournaments = await _get_cached("tournaments_in_season")
        cached_matches = await _get_cached("tournament_matches")
        cached_team_ids = await _get_cached("unique_team_ids") or []

        return {
            "valid_seasons": cached_valid_seasons,
            "tournaments_in_season": cached_tournaments,
            "tournament_matches": cached_matches,
            "unique_team_ids": cached_team_ids,
        }
    finally:
        await redis_client.close()


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
    "/get_tournaments_and_matches",
    summary="Get tournaments and matches",
    description="Fetch tournaments and scheduled matches data from the external API and cache the result.",
    tags=["Tournament"],
)
async def get_tournaments_and_matches_endpoint():
    """Get tournaments and scheduled matches"""
    base_endpoint = "/get_tournaments_and_matches"
    REQUEST_COUNT.labels(method="GET", endpoint=base_endpoint, has_id="False").inc()

    with REQUEST_LATENCY.labels(method="GET", endpoint=base_endpoint).time():
        result = await get_tournaments_and_matches(token_manager)
        
        return Response(
            content=result["content"],
            status_code=result["status_code"],
            media_type="application/json",
            headers=result["headers"]
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
