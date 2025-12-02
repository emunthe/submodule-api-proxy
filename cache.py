# Copyright (C) 2025 Sasha Shipka <sasha.shipka@copyleft.no>
# You may use, distribute and modify this code under the terms of the GNU General Public License v3.0

import asyncio
import json

import pendulum

from .config import config
from .prometheus import CACHE_REFRESH
from .util import (
    extract_base_endpoint,
    get_http_client,
    get_logger,
    get_redis_client,
    parse_datetime,
)

logger = get_logger(__name__)

background_tasks = set()


class CacheManager:
    def __init__(self, token_manager):
        self.token_manager = token_manager
        self.background_tasks = background_tasks


    async def get_cached_response(self, cache_key):
        """Get a response from cache if it exists and is valid"""

        redis = None
        try:
            redis = get_redis_client()
            cached_data = await redis.get(cache_key)

            if not cached_data:
                return None

            cache_data = json.loads(cached_data)

            # Don't use cache for error responses
            if cache_data["status_code"] >= 400:
                logger.info(
                    f"Not using cached error response with status {cache_data['status_code']}"
                )
                await redis.delete(cache_key)
                return None

            return cache_data
        except (json.JSONDecodeError, KeyError) as e:
            # Invalid cache data
            logger.error(f"Cache error for {cache_key}: {e}")
            if redis:
                await redis.delete(cache_key)
            return None

        finally:
            if redis:
                await redis.close()

    async def cache_response(self, cache_key, response, ttl, maxstale=None, hardttl=None):
        """Cache a response with staleness tracking.

        Args:
            cache_key: Redis key for the cached response
            response: HTTP response to cache
            ttl: Soft TTL - when to trigger background refresh (seconds)
            maxstale: Max age before flagging as "very stale" (seconds)
            hardttl: Redis expiration time (seconds)
        """
        if 200 <= response.status_code < 300:
            headers = dict(response.headers)
            if "content-encoding" in headers:
                del headers["content-encoding"]

            # Use defaults if not provided
            maxstale = maxstale or config.DEFAULT_MAXSTALE
            hardttl = hardttl or config.DEFAULT_HARDTTL

            cache_data = {
                "content": response.content.decode("utf-8") if response.content else "",
                "status_code": response.status_code,
                "headers": headers,
                "cached_at": pendulum.now().timestamp(),
                "soft_ttl": ttl,
                "max_stale": maxstale,
            }

            redis = get_redis_client()
            # Use hardttl for actual Redis expiration
            await redis.setex(cache_key, hardttl, json.dumps(cache_data))
            await redis.close()
            return True
        return False

    def get_cache_staleness(self, cache_data):
        """Determine the staleness level of cached data.

        Returns:
            tuple: (status, data_age_seconds)
                - status: "HIT" (fresh), "STALE", or "VERY-STALE"
                - data_age_seconds: age of data in seconds
        """
        cached_at = cache_data.get("cached_at")
        if not cached_at:
            # Legacy cache entry without timestamp - treat as fresh
            return "HIT", 0

        now = pendulum.now().timestamp()
        data_age = int(now - cached_at)
        soft_ttl = cache_data.get("soft_ttl", config.DEFAULT_TTL)
        max_stale = cache_data.get("max_stale", config.DEFAULT_MAXSTALE)

        if data_age <= soft_ttl:
            return "HIT", data_age
        elif data_age <= max_stale:
            return "STALE", data_age
        else:
            return "VERY-STALE", data_age

    async def list_cache(self):
        """List all cached endpoints with their expiration times"""
        redis = get_redis_client()
        keys = await redis.keys("GET:*")
        cache_info = []

        for key in keys:
            # Decode key to string if it is in bytes
            key_str = key.decode("utf-8") if isinstance(key, bytes) else key
            ttl = await redis.ttl(key)
            # Get refresh task info if exists
            refresh_info_key = f"refresh:{key_str}"
            refresh_info = await redis.get(refresh_info_key)

            cache_entry = {
                "endpoint": key_str.replace("GET:", ""),
                "ttl_remaining": ttl,
            }

            if refresh_info:
                refresh_data = json.loads(refresh_info)
                cache_entry["refresh_until"] = refresh_data.get("refresh_until")
                cache_entry["refresh_ttl"] = refresh_data.get("ttl")

            cache_info.append(cache_entry)

        await redis.close()
        return sorted(cache_info, key=lambda x: x["endpoint"])

    async def clear_cache(self, path):
        """Clear cache for a specific endpoint"""
        cache_key = f"GET:{config.API_URL}/{path}"

        # Also clear refresh task if exists
        refresh_info_key = f"refresh:{cache_key}"

        # Execute in pipeline
        redis = get_redis_client()
        pipeline = redis.pipeline()
        pipeline.delete(cache_key)
        pipeline.delete(refresh_info_key)
        results = await pipeline.execute()

        logger.info(f"Cleared cache for {path}")

        await redis.close()
        return {"cleared": bool(results[0]), "message": f"Cache cleared for {path}"}

    async def clear_all_cache(self):
        """Clear all cached endpoints"""
        redis = get_redis_client()
        keys = await redis.keys("GET:*")
        refresh_keys = await redis.keys("refresh:GET:*")

        if not keys and not refresh_keys:
            return {"message": "No cache entries found"}

        # Execute in pipeline
        pipeline = redis.pipeline()
        for key in keys:
            pipeline.delete(key)
        for key in refresh_keys:
            pipeline.delete(key)
        await pipeline.execute()

        logger.info(
            f"Cleared {len(keys)} cache entries and {len(refresh_keys)} refresh tasks"
        )

        await redis.close()

        return {
            "message": f"Cleared {len(keys)} cache entries and {len(refresh_keys)} refresh tasks"
        }

    async def background_refresh(self, cache_key, target_url, ttl, params=None, maxstale=None, hardttl=None):
        """Trigger a single background refresh for stale-while-revalidate pattern.

        This method creates a background task that refreshes the cache once
        without the autorefresh scheduling loop.
        """
        task = asyncio.create_task(
            self._do_background_refresh(cache_key, target_url, ttl, params, maxstale, hardttl)
        )
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)
        logger.info(f"Started background refresh for {cache_key}")

    async def _do_background_refresh(self, cache_key, target_url, ttl, params=None, maxstale=None, hardttl=None):
        """Perform a single background refresh."""
        client = None
        try:
            token = await self.token_manager.get_token()
            headers = {"Authorization": f"Bearer {token['access_token']}"}

            kwargs = {"headers": headers}
            if params:
                kwargs["params"] = params

            client = get_http_client()
            response = await client.get(target_url, **kwargs)

            if 200 <= response.status_code < 300:
                await self.cache_response(cache_key, response, ttl, maxstale, hardttl)

                base_endpoint = extract_base_endpoint(target_url)[0]
                CACHE_REFRESH.labels(endpoint=base_endpoint).inc()
                logger.info(f"Background refresh successful for {cache_key}")
            else:
                logger.warning(f"Background refresh failed for {cache_key}: HTTP {response.status_code}")

        except Exception as e:
            logger.error(f"Error in background refresh for {cache_key}: {e}")
        finally:
            if client:
                await client.aclose()

    async def setup_refresh(
        self, cache_key, target_url, ttl, autorefresh_until, params=None
    ):
        """Set up auto-refresh for a cached endpoint"""
        redis = None
        try:
            redis = get_redis_client()
            refresh_until_date = (
                autorefresh_until
                if isinstance(autorefresh_until, pendulum.DateTime)
                else parse_datetime(autorefresh_until)
            )

            # Store refresh information
            refresh_info = {
                "refresh_until": refresh_until_date.isoformat(),
                "ttl": ttl,
                "last_refresh": pendulum.now().isoformat(),
                "params": params,
            }

            refresh_info_key = f"refresh:{cache_key}"
            await redis.set(refresh_info_key, json.dumps(refresh_info))

            # Schedule refresh if needed
            if pendulum.now() < refresh_until_date:
                task = asyncio.create_task(
                    self.refresh_cache_task(
                        target_url, ttl, refresh_until_date, params, cache_key
                    )
                )
                self.background_tasks.add(task)
                task.add_done_callback(self.background_tasks.remove)
                logger.info(
                    f"Scheduled cache refresh for {cache_key} until {refresh_until_date}"
                )
                return True
        except Exception as e:
            logger.error(f"Error setting up refresh for {cache_key}: {e}")

        finally:
            if redis:
                await redis.close()

        return False

    async def refresh_cache_task(
        self, url, ttl, refresh_until_date, params=None, cache_key=None
    ):
        """Background task to periodically refresh a cached endpoint"""
        client = None
        try:
            # Determine cache key from URL if not provided
            if not cache_key:
                cache_key = f"GET:{url}"

            # Stop if we've reached the refresh_until date
            current_time = pendulum.now()
            if current_time >= refresh_until_date:
                logger.info(
                    f"Stopping cache refresh for {url}: refresh until date reached"
                )
                return

            logger.info(f"Refreshing cache for {url}, next refresh in {ttl} seconds")

            # Make request to refresh the data
            token = await self.token_manager.get_token()
            headers = {"Authorization": f"Bearer {token['access_token']}"}

            kwargs = {"headers": headers}
            if params:
                kwargs["params"] = params

            client = get_http_client()
            response = await client.get(url, **kwargs)

            if 200 <= response.status_code < 300:
                # Store the fresh response
                await self.cache_response(cache_key, response, ttl)

                base_endpoint = extract_base_endpoint(url)[0]
                CACHE_REFRESH.labels(endpoint=base_endpoint).inc()
                logger.info(f"Successfully refreshed cache for {url}")

                # Schedule next refresh
                next_refresh = min(
                    ttl - 1, ttl * 0.9
                )  # 90% of TTL or 1 second before expiry
                await asyncio.sleep(max(1, next_refresh))  # At least 1 second delay

                # Create new task for next refresh if still needed
                if pendulum.now() < refresh_until_date:
                    task = asyncio.create_task(
                        self.refresh_cache_task(
                            url, ttl, refresh_until_date, params, cache_key
                        )
                    )
                    self.background_tasks.add(task)
                    task.add_done_callback(self.background_tasks.remove)
            else:
                logger.error(
                    f"Failed to refresh cache for {url}: {response.status_code}"
                )
                # Try again after a delay
                await asyncio.sleep(min(60, ttl / 2))
                if pendulum.now() < refresh_until_date:
                    task = asyncio.create_task(
                        self.refresh_cache_task(
                            url, ttl, refresh_until_date, params, cache_key
                        )
                    )
                    self.background_tasks.add(task)
                    task.add_done_callback(self.background_tasks.remove)

        except Exception as e:
            logger.error(f"Error in refresh_cache_task for {url}: {e}")
            logger.exception(e)

            # Try again after a delay
            await asyncio.sleep(60)
            if pendulum.now() < refresh_until_date:
                task = asyncio.create_task(
                    self.refresh_cache_task(
                        url, ttl, refresh_until_date, params, cache_key
                    )
                )
                self.background_tasks.add(task)
                task.add_done_callback(self.background_tasks.remove)
        finally:
            if client:
                await client.aclose()
