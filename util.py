# Copyright (C) 2025 Sasha Shipka <sasha.shipka@copyleft.no>
# You may use, distribute and modify this code under the terms of the GNU General Public License v3.0

import asyncio
import json
import logging
import os
import re

import httpx
import pendulum
import redis.asyncio as redis
from redis.asyncio import ConnectionPool

from .config import config

background_tasks = set()

log_dir = config.LOG_PATH or "logs"

if not os.path.exists(log_dir):
    os.makedirs(log_dir)


redis_pool = ConnectionPool(
    host=config.REDIS_URL,
    port=config.REDIS_PORT,
    db=config.REDIS_DB,
    max_connections=config.MAX_REDIS_CONNECTIONS,  # Limit max connections
    decode_responses=True,
)


def get_redis_client():
    return redis.Redis(connection_pool=redis_pool)


def get_http_client():
    return httpx.AsyncClient(
        timeout=config.HTTP_TIMEOUT,
        limits=httpx.Limits(
            max_connections=config.MAX_CONNECTIONS,
            max_keepalive_connections=config.MAX_KEEPALIVE_CONNECTIONS,
        ),
    )


def get_logger(name):
    formatter = logging.Formatter(
        "%(asctime)s:(PID: %(process)d) %(name)s [%(levelname)s] - %(message)s"
    )
    file_handler = logging.FileHandler(os.path.join(log_dir, config.LOG_FILE))
    file_handler.setFormatter(formatter)

    logger = logging.getLogger(name)

    logger.addHandler(file_handler)
    logger.setLevel(getattr(logging, config.LOG_LEVEL, logging.DEBUG))
    return logger


logger = get_logger(__name__)


def extract_base_endpoint(path):
    """
    Extract base endpoint and determine if it has an ID.
    E.g., /users/123/posts -> (/users, True)
    """
    if path.startswith(config.API_URL):
        path = path.replace(config.API_URL, "")

    parts = path.strip("/").split("/")
    has_id = False
    base_parts = []

    for i, part in enumerate(parts):
        # Simple heuristic: if a part is numeric and not the first part,
        # consider it an ID
        if part.isdigit() and i > 0:
            has_id = True
            continue
        base_parts.append(part)

    base_endpoint = "/" + "/".join(base_parts)
    return base_endpoint, has_id


def process_query_params(params):
    """Extract special parameters from query params"""
    ttl = params.pop("TTL", None) or params.pop("ttl", None)
    autorefreshuntil = params.pop("autorefreshuntil", None)
    no_cache = params.pop("_nocache", None)

    # Convert TTL to int if present
    if ttl:
        try:
            ttl = int(ttl)
        except ValueError:
            ttl = config.DEFAULT_TTL

    # Parse autorefresh date if present
    refresh_until = None
    if autorefreshuntil:
        try:
            refresh_until = parse_datetime(autorefreshuntil)
        except Exception:
            # Log but continue without auto-refresh
            logger.warning(f"Invalid autorefreshuntil format: {autorefreshuntil}")

    return params, ttl, refresh_until, no_cache


def parse_datetime(datetime_str):
    """
    Parse a datetime string in various formats using Pendulum.

    Supports:
    - ISO format: "2023-12-31T23:59:59" or "2023-12-31"
    - Human-readable: "1d" (1 day from now), "4h" (4 hours from now), "30m" (30 minutes from now)

    Returns a Pendulum datetime object
    """
    if not datetime_str:
        return None

    match = re.match(r"(\d+)([dhm])", datetime_str)
    if match:
        value, unit = match.groups()
        if unit == "d":
            return pendulum.now().add(days=int(value))
        elif unit == "h":
            return pendulum.now().add(hours=int(value))
        elif unit == "m":
            return pendulum.now().add(minutes=int(value))

    # Try to parse as ISO format
    return pendulum.parse(datetime_str)


async def refresh_base_data(redis_client, http_client, token_manager):
    """
    Fetch combined base data and store it permanently in Redis

    Args:
        redis_client: Redis client instance
        http_client: HTTP client instance
        token_manager: Token manager instance

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        logger.info("Starting base data refresh")
        http_client = get_http_client()

        # Get token from the token manager
        token = await token_manager.get_token()

        # Fetch sport data
        logger.info("Fetching sport data")
        sport_url = f"{config.API_URL}/api/v1/org/sport/352"
        sport_response = await http_client.get(
            sport_url, headers={"Authorization": f"Bearer {token['access_token']}"}
        )
        if sport_response.status_code >= 400:
            logger.error(f"Failed to fetch sport data: {sport_response.status_code}")
            return False
        sport_data = sport_response.json()

        # Fetch match incident types
        logger.info("Fetching match incident types")
        incident_url = f"{config.API_URL}/api/v1/ta/MatchIncidentTypes"
        incident_response = await http_client.get(
            incident_url,
            headers={"Authorization": f"Bearer {token['access_token']}"},
            params={"sfOrgId": 352},
        )
        if incident_response.status_code >= 400:
            logger.error(
                f"Failed to fetch incident types: {incident_response.status_code}"
            )
            return False
        incident_types = incident_response.json()

        # Fetch seasons data
        logger.info("Fetching seasons data")
        current_year = pendulum.now().year
        years = list(range(2003, current_year + 1))
        sport_ids = [72, 151, 71, 73]

        seasons_results = []
        for year in years:
            for sport_id in sport_ids:
                logger.info(f"Fetching season data for year {year}, sport {sport_id}")
                season_url = f"{config.API_URL}/api/v1/ta/Seasons/"
                season_response = await http_client.get(
                    season_url,
                    headers={"Authorization": f"Bearer {token['access_token']}"},
                    params={"year": year, "sportId": sport_id},
                )

                if season_response.status_code >= 400:
                    logger.warning(
                        f"Failed to fetch season data for year {year}, sport {sport_id}: {season_response.status_code}"
                    )
                    seasons_results.append(None)
                else:
                    try:
                        seasons_results.append(season_response.json())
                    except Exception as e:
                        logger.error(
                            f"Failed to parse season data for year {year}, sport {sport_id}: {e}. Data probably missing."
                        )

                # Small delay between requests to not overwhelm the API
                await asyncio.sleep(0.2)

        # Combine all data
        base_data = {
            "ta_sport_data": sport_data,
            "ta_match_incident_types_data": incident_types,
            "ta_seasons_data": seasons_results,
            "last_updated": pendulum.now().isoformat(),
        }

        # Store in Redis permanently (no expiry)
        cache_key = "permanent:base_data"
        await redis_client.set(cache_key, json.dumps(base_data))

        logger.info(f"Base data refreshed successfully at {base_data['last_updated']}")
        return True
    except Exception as e:
        logger.error(f"Failed to refresh base data: {e}")
        logger.exception(e)
        return False
