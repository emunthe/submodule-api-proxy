# Copyright (C) 2025 Sasha Shipka <sasha.shipka@copyleft.no>
# You may use, distribute and modify this code under the terms of the GNU General Public License v3.0

import asyncio
import json

import pendulum

from .config import config
from .util import get_http_client, get_logger, get_redis_client

logger = get_logger(__name__)

HEALTH_KEY = "nif_api:health"
CHECK_INTERVAL = 5  # seconds


class NifHealthChecker:
    """Singleton background health checker for NIF API."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._task = None
        self._running = False

    async def start(self):
        """Start the background health checker."""
        if self._running:
            logger.warning("Health checker already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._health_check_loop())
        logger.info(f"NIF API health checker started (interval: {CHECK_INTERVAL}s)")

    async def stop(self):
        """Stop the background health checker."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("NIF API health checker stopped")

    async def _health_check_loop(self):
        """Main loop that periodically checks NIF API health."""
        while self._running:
            try:
                await self._do_health_check()
            except Exception as e:
                logger.error(f"Error in health check loop: {e}")

            await asyncio.sleep(CHECK_INTERVAL)

    async def _do_health_check(self):
        """Perform a single health check against NIF API."""
        client = None
        redis = None
        success = False

        try:
            # Make a lightweight probe to NIF API base URL
            client = get_http_client()
            response = await client.get(config.API_URL, timeout=5.0)
            success = response.status_code < 500

        except Exception as e:
            logger.debug(f"NIF API health check failed: {e}")
            success = False

        finally:
            if client:
                await client.aclose()

        # Update health status in Redis
        try:
            redis = get_redis_client()
            now = pendulum.now().isoformat()

            health_data = await redis.get(HEALTH_KEY)
            if health_data:
                health = json.loads(health_data)
            else:
                health = {
                    "status": "healthy",
                    "last_success": None,
                    "last_failure": None,
                    "consecutive_failures": 0,
                    "consecutive_successes": 0,
                }

            if success:
                health["last_success"] = now
                health["consecutive_failures"] = 0
                health["consecutive_successes"] = health.get("consecutive_successes", 0) + 1
                health["status"] = "healthy"
            else:
                health["last_failure"] = now
                health["consecutive_successes"] = 0
                health["consecutive_failures"] = health.get("consecutive_failures", 0) + 1

                if health["consecutive_failures"] >= 4:
                    health["status"] = "down"
                elif health["consecutive_failures"] >= 1:
                    health["status"] = "degraded"

            health["last_check"] = now
            await redis.set(HEALTH_KEY, json.dumps(health))
            logger.debug(f"NIF API health: {health['status']}")

        except Exception as e:
            logger.error(f"Error updating health status in Redis: {e}")
        finally:
            if redis:
                await redis.close()


# Singleton instance
health_checker = NifHealthChecker()


async def get_nif_health():
    """
    Get the current NIF API health status and metadata.

    Returns:
        dict with keys: status, last_success, last_failure,
                       consecutive_failures, consecutive_successes
    """
    redis = None
    try:
        redis = get_redis_client()
        health_data = await redis.get(HEALTH_KEY)

        if health_data:
            return json.loads(health_data)

        # Default healthy status if no data exists yet
        return {
            "status": "healthy",
            "last_success": None,
            "last_failure": None,
            "consecutive_failures": 0,
            "consecutive_successes": 0,
        }

    except Exception as e:
        logger.error(f"Error getting NIF API health: {e}")
        return {"status": "unknown", "error": str(e)}
    finally:
        if redis:
            await redis.close()
