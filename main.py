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
from .prometheus import (
    CACHE_HITS,
    CACHE_MISSES,
    REQUEST_COUNT,
    REQUEST_LATENCY,
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
    title="NIF API Gateway 3",
    version="0.1",
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


async def detect_change_tournaments_and_matches():
    """Periodically fetch season, tournament, match and team data.

    The routine mirrors the behaviour of the legacy PHP pre cache manager by
    comparing freshly fetched data with what is stored in Redis.  When changes
    are detected the corresponding cache keys are updated and cache warming
    tasks are scheduled for the affected tournaments and teams.
    """

    while True:
        redis_client = None
        client = None
        start_time = pendulum.now()
        api_calls = {"seasons": 0, "tournaments": 0, "matches": 0}
        changes_detected = {}
        # Ensure these are defined even if related blocks are commented out
        changed_tournament_ids = set()
        changed_team_ids = []

        try:
            redis_client = get_redis_client()

            async def _get_cached(key):
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

            token = await token_manager.get_token()
            headers = {"Authorization": f"Bearer {token['access_token']}"}
            client = get_http_client()

            # -----------------------------------------------------------------
            # Fetch season list for relevant sports and years
            seasons = []
            current_year = pendulum.now().year

            for year in range(2003, current_year + 1):
                for sport_id in [72, 151, 71, 73]:
                    url = f"{config.API_URL}/api/v1/ta/Seasons/"
                    logger.info(f"Fetching seasons for year {year}, sport {sport_id} from {url}")
                    resp = await client.get(
                        url,
                        headers=headers,
                        params={"year": year, "sportId": sport_id},
                    )
                    api_calls["seasons"] += 1
                    if resp.status_code < 400:
                        try:
                            raw = resp.json()
                        except Exception as e:
                            # Parsing failed, skip this response safely
                            logger.warning(
                                f"Failed to parse seasons response for year={year} sport={sport_id}: {e}"
                            )
                            continue

                        # Log size / type, not full payload (avoid huge logs)
                        logger.info(
                            f"Fetched seasons payload type={type(raw).__name__} year={year} sport={sport_id}"
                        )

                        # add the raw return even if it is a dict to the seasons list
                        data = raw.get("seasons", [])
                        if isinstance(data, dict):
                            seasons.append(data)
                        elif isinstance(data, list):
                            seasons.extend(data)
                        else:
                            logger.warning(
                                f"Unexpected seasons data type for year={year} sport={sport_id}: {type(data).__name__}"
                            )
                            # skip adding anything
                            continue

                    # If response failed, skip adding anything (do not add empty array)
                    await asyncio.sleep(0.2)


            # log the final seasons fetched
            logger.info(f"Total seasons fetched: {len(seasons)}")
            logger.debug(f"Fetched seasons: {seasons}")

            # if cached valid_seasons is different from seasons, we have changes
            # so we need to mark changes_detected and store the new seasons

            if json.dumps(seasons, sort_keys=True) != json.dumps(cached_valid_seasons or [], sort_keys=True):
                changes_detected["seasons"] = len(seasons)
                await redis_client.set(
                    "valid_seasons",
                    json.dumps(
                        {"data": seasons, "last_updated": start_time.isoformat()},
                        ensure_ascii=False,
                    ),
                )

            valid_seasons = []
            # for season in seasons:
            #     try:
            #         season_year = pendulum.parse(season["seasonDateFrom"]).year
            #     except Exception:
            #         continue
            #     sport_id = season.get("sportId")
            #     if season_year >= 2024:
            #         if sport_id != 72:
            #             valid_seasons.append(season)
            #         else:
            #             name = season.get("seasonName", "").lower()
            #             if "bedrift" not in name:
            #                 valid_seasons.append(season)

            # if json.dumps(valid_seasons, sort_keys=True) != json.dumps(
            #     cached_valid_seasons or [], sort_keys=True
            # ):
            #     await redis_client.set(
            #         "valid_seasons",
            #         json.dumps(
            #             {"data": valid_seasons, "last_updated": start_time.isoformat()},
            #             ensure_ascii=False,
            #         ),
            #     )
            #     changes_detected["valid_seasons"] = len(valid_seasons)

            # -----------------------------------------------------------------
            # Fetch tournaments per season
            tournaments = []
            # root_tournaments = []
            # for season in valid_seasons:
            #     season_id = season.get("seasonId")
            #     url = f"{config.API_URL}/api/v1/ta/Tournament/Season/{season_id}/"
            #     resp = await client.get(url, headers=headers, params={"hierarchy": True})
            #     api_calls["tournaments"] += 1
            #     if resp.status_code < 400:
            #         data = resp.json().get("tournamentsInSeason", [])
            #         for t in data:
            #             t["apiPath"] = url
            #             tournaments.append(t)
            #             if not t.get("parentTournamentId"):
            #                 root_tournaments.append(t)
            #     await asyncio.sleep(0.2)

            # if json.dumps(tournaments, sort_keys=True) != json.dumps(
            #     cached_tournaments or [], sort_keys=True
            # ):
            #     await redis_client.set(
            #         "tournaments_in_season",
            #         json.dumps(
            #             {"data": tournaments, "last_updated": start_time.isoformat()},
            #             ensure_ascii=False,
            #         ),
            #     )
            #     cached_ids = {t["tournamentId"] for t in (cached_tournaments or [])}
            #     new_ids = {t["tournamentId"] for t in tournaments}
            #     changes_detected["tournaments_in_season"] = len(
            #         new_ids.symmetric_difference(cached_ids)
            #     )

            # -----------------------------------------------------------------
            # Fetch tournament matches for root tournaments
            matches = []
            # for tournament in root_tournaments:
            #     tid = tournament.get("tournamentId")
            #     url = f"{config.API_URL}/api/v1/ta/TournamentMatches/"
            #     params = {"tournamentId": tid}
            #     resp = await client.get(url, headers=headers, params=params)
            #     api_calls["matches"] += 1
            #     if resp.status_code < 400:
            #         for m in resp.json().get("matches", []):
            #             m["apiPath"] = f"{url}?tournamentId={tid}"
            #             matches.append(m)
            #     await asyncio.sleep(0.2)

            # if json.dumps(matches, sort_keys=True) != json.dumps(
            #     cached_matches or [], sort_keys=True
            # ):
            #     await redis_client.set(
            #         "tournament_matches",
            #         json.dumps(
            #             {"data": matches, "last_updated": start_time.isoformat()},
            #             ensure_ascii=False,
            #         ),
            #     )

            #     def _group_by_tournament(data):
            #         grouped = {}
            #         for item in data:
            #             tid = item.get("tournamentId")
            #             if tid is None:
            #                 continue
            #             grouped.setdefault(tid, []).append(item)
            #         return grouped

            #     new_group = _group_by_tournament(matches)
            #     old_group = _group_by_tournament(cached_matches or [])
            #     changed_tournament_ids = set()
            #     for tid, new_list in new_group.items():
            #         old_list = old_group.get(tid, [])
            #         if json.dumps(new_list, sort_keys=True) != json.dumps(
            #             old_list, sort_keys=True
            #         ):
            #             changed_tournament_ids.add(tid)
            #     for tid in old_group:
            #         if tid not in new_group:
            #             changed_tournament_ids.add(tid)
            #     changes_detected["tournament_matches"] = len(changed_tournament_ids)
            # else:
            #     changed_tournament_ids = set()

            # -----------------------------------------------------------------
            # Unique team identifiers
            team_ids = set()
            # for match in matches:
            #     for key in ("hometeamId", "awayteamId"):
            #         tid = match.get(key)
            #         if tid is not None:
            #             team_ids.add(tid)

            # if team_ids != set(cached_team_ids):
            #     await redis_client.set(
            #         "unique_team_ids",
            #         json.dumps(
            #             {"data": list(team_ids), "last_updated": start_time.isoformat()},
            #             ensure_ascii=False,
            #         ),
            #     )
            #     changed_team_ids = list(team_ids.symmetric_difference(set(cached_team_ids)))
            #     changes_detected["unique_team_ids"] = len(changed_team_ids)
            # else:
            #     changed_team_ids = []

            # -----------------------------------------------------------------
            # Warm caches for changed tournaments and teams
            refresh_until = pendulum.now().add(days=7)
            ttl = 7 * 24 * 60 * 60

            for tid in changed_tournament_ids:
                url = f"{config.API_URL}/api/v1/ta/TournamentMatches"
                cache_key = f"GET:{url}?tournamentId={tid}"
                await cache_manager.setup_refresh(
                    cache_key, url, ttl, refresh_until, params={"tournamentId": tid}
                )

            for team_id in changed_team_ids:
                url = f"{config.API_URL}/api/v1/ta/Team"
                cache_key = f"GET:{url}?teamId={team_id}"
                await cache_manager.setup_refresh(
                    cache_key, url, ttl, refresh_until, params={"teamId": team_id}
                )

            end_time = pendulum.now()
            log_item = {
                "action": "pre_cache_process",
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "process_time_seconds": (end_time - start_time).total_seconds(),
                "api_calls_made": api_calls,
                "changes_detected": {k: int(v) for k, v in changes_detected.items()},
            }
            logger.info(f"logProcess: {json.dumps(log_item, ensure_ascii=False)}")

        except Exception as e:
            logger.error(f"Error in detect_change_tournaments_and_matches: {e}")
            logger.exception(e)
        finally:
            if client:
                await client.aclose()
            if redis_client:
                await redis_client.close()

        await asyncio.sleep(180)


@app.on_event("startup")
async def startup_event():
    task = asyncio.create_task(detect_change_tournaments_and_matches())
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)


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
async def get_tournaments_and_matches():
    """Get tournaments and scheduled matches"""
    cache_key = "tournaments_matches"
    base_endpoint = "/get_tournaments_and_matches"
    REQUEST_COUNT.labels(method="GET", endpoint=base_endpoint, has_id="False").inc()
    redis_client = None
    client = None

    with REQUEST_LATENCY.labels(method="GET", endpoint=base_endpoint).time():
        try:
            redis_client = get_redis_client()
            cached_data = await redis_client.get(cache_key)
            if cached_data:
                return Response(
                    content=cached_data,
                    media_type="application/json",
                    headers={"X-Cache": "HIT"},
                )

            client = get_http_client()

            tournaments_url = f"{config.API_URL}/api/v1/ta/Tournaments"
            matches_url = f"{config.API_URL}/api/v1/ta/ScheduledMatches"

            # tournaments_response = await client.get(tournaments_url, headers=headers)
            # matches_response = await client.get(matches_url, headers=headers)

            # if tournaments_response.status_code >= 400 or matches_response.status_code >= 400:
            #     raise Exception("Failed to fetch tournaments or matches data")

            params = {
                "tournaments_url": tournaments_url,
                "matches_url": matches_url,
            }

            logger.info(
                f"Tournaments and matches params: {params}"
            )

            data = {
                "tournaments": [],  # tournaments_response.json(),
                "matches": [],  # matches_response.json(),
                "last_updated": pendulum.now().isoformat(),
            }

            await redis_client.setex(
                cache_key, config.DEFAULT_TTL, json.dumps(data, ensure_ascii=False)
            )

            return Response(
                content=json.dumps(data, ensure_ascii=False),
                media_type="application/json",
                headers={"X-Cache": "MISS"},
            )
        except Exception as e:
            logger.error(f"Error in get_tournaments_and_matches: {e}")
            logger.exception(e)
            return Response(
                content=json.dumps({"error": str(e)}, ensure_ascii=False),
                status_code=500,
                media_type="application/json",
            )
        finally:
            if client:
                await client.aclose()
            if redis_client:
                await redis_client.close()


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
            CACHE_HITS.labels(endpoint=base_endpoint).inc()

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

    CACHE_MISSES.labels(endpoint=base_endpoint).inc()

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
