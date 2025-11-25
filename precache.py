# Copyright (C) 2025 Sasha Shipka <sasha.shipka@copyleft.no>
# You may use, distribute and modify this code under the terms of the GNU General Public License v3.0

import asyncio
import json
import os
import re
import shutil
import uuid
from datetime import datetime

import pendulum
from prometheus_client import Counter, Gauge, Histogram

from .config import config
from .token import TokenManager
from .util import get_http_client, get_logger, get_redis_client


logger = get_logger(__name__)

# Batch processing configuration to prevent overwhelming the system
MAX_CONCURRENT_CACHE_SETUPS = 10  # Max simultaneous cache setup operations
BATCH_SIZE = 50  # Process cache entries in batches of 50
BATCH_DELAY = 2.0  # Delay between batches in seconds
CACHE_SETUP_DELAY = 0.1  # Delay between individual cache setups
MAX_MATCHES_TO_CACHE = 1000  # Maximum number of matches to cache in one run

# Global flag to track if cache warming is in progress
CACHE_WARMING_IN_PROGRESS = False

# Prometheus metrics for pre-cache operations
PRECACHE_RUNS_TOTAL = Counter(
    "precache_runs_total",
    "Total number of pre-cache periodic runs",
    ["status"]  # success, error
)

PRECACHE_CHANGES_DETECTED = Counter(
    "precache_changes_detected_total",
    "Total number of changes detected by category",
    ["category"]  # seasons, tournaments_in_season, tournament_matches, unique_team_ids
)

PRECACHE_CHANGES_THIS_RUN = Gauge(
    "precache_changes_this_run",
    "Number of changes detected in this specific run by category",
    ["category", "run_id"]  # seasons, tournaments_in_season, tournament_matches, individual_matches, unique_team_ids
)

PRECACHE_RUN_CHANGES_SUMMARY = Gauge(
    "precache_run_changes_summary",
    "Summary of all changes detected in a specific run with run metadata",
    ["run_id", "run_timestamp", "category"]
)

PRECACHE_API_CALLS = Counter(
    "precache_api_calls_total", 
    "Total API calls made during pre-cache operations",
    ["call_type"]  # seasons, tournament_season, tournament_matches
)

PRECACHE_DURATION_SECONDS = Histogram(
    "precache_duration_seconds",
    "Time spent in pre-cache operations"
)

PRECACHE_LAST_RUN_TIMESTAMP = Gauge(
    "precache_last_run_timestamp",
    "Timestamp of the last pre-cache run"
)

PRECACHE_ITEMS_PROCESSED = Gauge(
    "precache_items_processed",
    "Number of items processed in last pre-cache run",
    ["item_type"]  # seasons, tournaments, matches, teams
)

PRECACHE_CACHED_DATA_SIZE = Gauge(
    "precache_cached_data_size_bytes",
    "Size in bytes of cached data",
    ["data_type"]  # valid_seasons, tournaments_in_season, tournament_matches, unique_team_ids
)

PRECACHE_API_CALL_SUCCESS_RATE = Gauge(
    "precache_api_call_success_rate",
    "Success rate of precache API calls",
    ["call_type"]  # seasons, tournament_season, tournament_matches
)

PRECACHE_VALID_SEASONS_COUNT = Gauge(
    "precache_valid_seasons_count",
    "Total number of valid seasons currently cached"
)

PRECACHE_VALID_SEASONS_INFO = Gauge(
    "precache_valid_seasons_info",
    "Information about valid seasons with season details as labels",
    ["season_id", "season_name", "season_year", "sport_id", "sport_name"]
)

PRECACHE_TOURNAMENTS_IN_SEASON_COUNT = Gauge(
    "precache_tournaments_in_season_count",
    "Total number of tournaments in season currently cached"
)

PRECACHE_TOURNAMENTS_BY_SEASON = Gauge(
    "precache_tournaments_by_season",
    "Number of tournaments per season",
    ["season_id", "sport_id", "sport_name"]
)

PRECACHE_TOURNAMENTS_BY_TYPE = Gauge(
    "precache_tournaments_by_type",
    "Number of tournaments by root/child type per sport",
    ["sport_id", "sport_name", "is_root"]
)

PRECACHE_TOURNAMENT_MATCHES_COUNT = Gauge(
    "precache_tournament_matches_count",
    "Total number of tournament matches currently cached"
)

PRECACHE_TOURNAMENT_MATCHES_BY_TOURNAMENT = Gauge(
    "precache_tournament_matches_by_tournament",
    "Number of matches per tournament",
    ["tournament_id", "tournament_name", "season_id"]
)

PRECACHE_TOURNAMENT_MATCHES_BY_SEASON = Gauge(
    "precache_tournament_matches_by_season",
    "Number of matches per season",
    ["season_id"]
)

PRECACHE_API_URLS_CALLED = Gauge(
    "precache_api_urls_called",
    "URLs called to data.nif.no API during precache runs - tracks detailed API call information per run",
    ["run_id", "url_path", "method", "params"]
)

PRECACHE_UPSTREAM_STATUS = Gauge(
    "precache_upstream_status",
    "Status of upstream data.nif.no API - 1 for UP, 0 for DOWN",
    ["endpoint"]  # data.nif.no
)


async def _update_cached_data_size_metrics(redis_client):
    """Update metrics for the size of cached data"""
    try:
        cache_keys = {
            "valid_seasons": "valid_seasons",
            "tournaments_in_season": "tournaments_in_season", 
            "root_tournaments": "root_tournaments",
            "tournament_matches": "tournament_matches",
            "unique_team_ids": "unique_team_ids"
        }
        
        for data_type, redis_key in cache_keys.items():
            raw_data = await redis_client.get(redis_key)
            if raw_data:
                # Calculate size in bytes
                size_bytes = len(raw_data.encode('utf-8') if isinstance(raw_data, str) else raw_data)
                PRECACHE_CACHED_DATA_SIZE.labels(data_type=data_type).set(size_bytes)
            else:
                # Set to 0 if no data exists
                PRECACHE_CACHED_DATA_SIZE.labels(data_type=data_type).set(0)
    except Exception as e:
        logger.error(f"Error updating cached data size metrics: {e}")


async def _update_valid_seasons_metrics(redis_client):
    """Update metrics for valid seasons content and count"""
    try:
        # Clear existing metrics first
        PRECACHE_VALID_SEASONS_INFO.clear()
        
        # Get valid seasons data from Redis
        raw_data = await redis_client.get("valid_seasons")
        if raw_data:
            try:
                data = json.loads(raw_data)
                valid_seasons = data.get("data", [])
                
                # Update count metric
                PRECACHE_VALID_SEASONS_COUNT.set(len(valid_seasons))
                
                # Map sport IDs to names for better readability
                sport_names = {
                    72: "bandy",
                    151: "innebandy", 
                    71: "landhockey",
                    73: "rinkbandy"
                }
                
                # Update info metric with labels for each season
                for season in valid_seasons:
                    if not isinstance(season, dict):
                        continue
                        
                    season_id = str(season.get("seasonId", "unknown"))
                    season_name = season.get("seasonName", "unknown")[:50]  # Limit length for labels
                    sport_id = season.get("sportId", 0)
                    sport_name = sport_names.get(sport_id, f"sport_{sport_id}")
                    
                    # Extract year from seasonDateFrom if available
                    season_year = "unknown"
                    season_date_from = season.get("seasonDateFrom")
                    if season_date_from:
                        try:
                            # Try the most common format first
                            if "/" in season_date_from:
                                parsed_date = datetime.strptime(season_date_from.split()[0], "%m/%d/%Y")
                                season_year = str(parsed_date.year)
                            else:
                                import pendulum
                                parsed_date = pendulum.parse(season_date_from)
                                season_year = str(parsed_date.year)
                        except:
                            # Keep unknown if parsing fails
                            pass
                    
                    # Set metric with season information as labels
                    PRECACHE_VALID_SEASONS_INFO.labels(
                        season_id=season_id,
                        season_name=season_name,
                        season_year=season_year,
                        sport_id=str(sport_id),
                        sport_name=sport_name
                    ).set(1)  # Value of 1 indicates this season exists
                    
                logger.debug(f"Updated valid seasons metrics: {len(valid_seasons)} seasons")
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse valid_seasons data: {e}")
                PRECACHE_VALID_SEASONS_COUNT.set(0)
        else:
            # No data available
            PRECACHE_VALID_SEASONS_COUNT.set(0)
            
    except Exception as e:
        logger.error(f"Error updating valid seasons metrics: {e}")


async def _update_tournaments_in_season_metrics(redis_client):
    """Update metrics for tournaments in season content and count"""
    try:
        # Clear existing metrics first
        PRECACHE_TOURNAMENTS_BY_SEASON.clear()
        PRECACHE_TOURNAMENTS_BY_TYPE.clear()
        
        # Get tournaments in season data from Redis
        raw_data = await redis_client.get("tournaments_in_season")
        if raw_data:
            try:
                data = json.loads(raw_data)
                tournaments = data.get("data", [])
                
                # Update count metric
                PRECACHE_TOURNAMENTS_IN_SEASON_COUNT.set(len(tournaments))
                
                # Map sport IDs to names for better readability
                sport_names = {
                    72: "bandy",
                    151: "innebandy",
                    71: "landhockey",
                    73: "rinkbandy"
                }
                
                # Aggregate tournaments by season and by type
                tournaments_by_season = {}
                tournaments_by_type = {}
                
                for tournament in tournaments:
                    if not isinstance(tournament, dict):
                        continue
                    
                    season_id = str(tournament.get("seasonId", "unknown"))
                    sport_id = tournament.get("sportId", 0)
                    sport_name = sport_names.get(sport_id, f"sport_{sport_id}")
                    is_root = "true" if not tournament.get("parentTournamentId") else "false"
                    
                    # Count by season
                    season_key = (season_id, str(sport_id), sport_name)
                    if season_key not in tournaments_by_season:
                        tournaments_by_season[season_key] = 0
                    tournaments_by_season[season_key] += 1
                    
                    # Count by type
                    type_key = (str(sport_id), sport_name, is_root)
                    if type_key not in tournaments_by_type:
                        tournaments_by_type[type_key] = 0
                    tournaments_by_type[type_key] += 1
                
                # Update season metrics
                for (season_id, sport_id, sport_name), count in tournaments_by_season.items():
                    PRECACHE_TOURNAMENTS_BY_SEASON.labels(
                        season_id=season_id,
                        sport_id=sport_id,
                        sport_name=sport_name
                    ).set(count)
                
                # Update type metrics
                for (sport_id, sport_name, is_root), count in tournaments_by_type.items():
                    PRECACHE_TOURNAMENTS_BY_TYPE.labels(
                        sport_id=sport_id,
                        sport_name=sport_name,
                        is_root=is_root
                    ).set(count)
                    
                logger.debug(f"Updated tournaments in season metrics: {len(tournaments)} tournaments across {len(tournaments_by_season)} season-sport combinations")
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse tournaments_in_season data: {e}")
                PRECACHE_TOURNAMENTS_IN_SEASON_COUNT.set(0)
        else:
            # No data available
            PRECACHE_TOURNAMENTS_IN_SEASON_COUNT.set(0)
            
    except Exception as e:
        logger.error(f"Error updating tournaments in season metrics: {e}")


async def _update_tournament_matches_metrics(redis_client):
    """Update metrics for tournament matches content and count"""
    try:
        # Clear existing metrics first
        PRECACHE_TOURNAMENT_MATCHES_BY_TOURNAMENT.clear()
        PRECACHE_TOURNAMENT_MATCHES_BY_SEASON.clear()
        
        # Get tournament matches data from Redis
        raw_data = await redis_client.get("tournament_matches")
        if raw_data:
            try:
                data = json.loads(raw_data)
                matches = data.get("data", [])
                
                # Update count metric
                PRECACHE_TOURNAMENT_MATCHES_COUNT.set(len(matches))
                
                # Aggregate matches by tournament and season
                matches_by_tournament = {}
                matches_by_season = {}
                
                for match in matches:
                    if not isinstance(match, dict):
                        continue
                    
                    tournament_id = str(match.get("tournamentId", "unknown"))
                    season_id = str(match.get("seasonId", "unknown"))
                    
                    # Count by tournament
                    if tournament_id not in matches_by_tournament:
                        matches_by_tournament[tournament_id] = {
                            "count": 0,
                            "tournament_name": match.get("tournamentName", "unknown")[:50],
                            "season_id": season_id
                        }
                    matches_by_tournament[tournament_id]["count"] += 1
                    
                    # Count by season
                    if season_id not in matches_by_season:
                        matches_by_season[season_id] = 0
                    matches_by_season[season_id] += 1
                
                # Update tournament metrics
                for tournament_id, info in matches_by_tournament.items():
                    PRECACHE_TOURNAMENT_MATCHES_BY_TOURNAMENT.labels(
                        tournament_id=tournament_id,
                        tournament_name=info["tournament_name"],
                        season_id=info["season_id"]
                    ).set(info["count"])
                
                # Update season metrics
                for season_id, count in matches_by_season.items():
                    PRECACHE_TOURNAMENT_MATCHES_BY_SEASON.labels(
                        season_id=season_id
                    ).set(count)
                    
                logger.debug(f"Updated tournament matches metrics: {len(matches)} matches across {len(matches_by_tournament)} tournaments and {len(matches_by_season)} seasons")
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse tournament_matches data: {e}")
                PRECACHE_TOURNAMENT_MATCHES_COUNT.set(0)
        else:
            # No data available
            PRECACHE_TOURNAMENT_MATCHES_COUNT.set(0)
            
    except Exception as e:
        logger.error(f"Error updating tournament matches metrics: {e}")


async def clear_precache_data():
    """Clear all precache data from Redis"""
    redis_client = None
    try:
        redis_client = get_redis_client()
        
        # List of precache data keys to clear
        precache_keys = [
            "valid_seasons",
            "tournaments_in_season", 
            "root_tournaments",
            "tournament_matches",
            "unique_team_ids"
        ]
        
        # Add individual season tournament cache keys
        try:
            async for key in redis_client.scan_iter(match="tournaments_season_*"):
                precache_keys.append(key)
        except Exception as e:
            logger.debug(f"Could not scan for season tournament cache keys during clear: {e}")
        
        cleared_keys = []
        for key in precache_keys:
            result = await redis_client.delete(key)
            if result > 0:
                cleared_keys.append(key)
        
        # Update metrics to reflect cleared data
        await _update_cached_data_size_metrics(redis_client)
        await _update_valid_seasons_metrics(redis_client)
        await _update_tournaments_in_season_metrics(redis_client)
        await _update_tournament_matches_metrics(redis_client)
        
        # Reset last run timestamp
        PRECACHE_LAST_RUN_TIMESTAMP.set(0)
        
        # Delete debug log files
        deleted_files = []
        try:
            log_dir = "logs/precache_debug_logs"
            if os.path.exists(log_dir):
                # Remove all JSON files in the debug logs directory
                for filename in os.listdir(log_dir):
                    if filename.endswith('.json'):
                        filepath = os.path.join(log_dir, filename)
                        try:
                            os.remove(filepath)
                            deleted_files.append(filename)
                        except Exception as e:
                            logger.warning(f"Could not delete {filepath}: {e}")
                
                logger.info(f"Deleted {len(deleted_files)} debug log files: {deleted_files}")
        except Exception as e:
            logger.warning(f"Error deleting debug log files: {e}")
        
        logger.info(f"Cleared precache data keys: {cleared_keys}")
        
        return {
            "status": "success",
            "message": f"Cleared {len(cleared_keys)} precache data keys and {len(deleted_files)} debug log files",
            "cleared_keys": cleared_keys,
            "deleted_files": deleted_files
        }
        
    except Exception as e:
        logger.error(f"Error clearing precache data: {e}")
        return {
            "status": "error", 
            "message": f"Failed to clear precache data: {str(e)}"
        }
    finally:
        if redis_client:
            await redis_client.close()


async def get_season_tournaments(season_id):
    """Get cached tournament data for a specific season"""
    redis_client = None
    try:
        redis_client = get_redis_client()
        
        season_cache_key = f"tournaments_season_{season_id}"
        raw_data = await redis_client.get(season_cache_key)
        
        if raw_data:
            try:
                cached_data = json.loads(raw_data)
                return {
                    "status": "success",
                    "data": cached_data,
                    "cache_key": season_cache_key,
                    "source": "cache"
                }
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse cached season tournament data for season {season_id}: {e}")
                return {
                    "status": "error",
                    "message": f"Invalid cached data for season {season_id}",
                    "cache_key": season_cache_key
                }
        else:
            return {
                "status": "not_found",
                "message": f"No cached tournament data found for season {season_id}",
                "cache_key": season_cache_key
            }
            
    except Exception as e:
        logger.error(f"Error retrieving season tournament data for season {season_id}: {e}")
        return {
            "status": "error",
            "message": f"Failed to retrieve tournament data for season {season_id}: {str(e)}"
        }
    finally:
        if redis_client:
            await redis_client.close()


def _log_cache_data_to_file(cache_name, data, run_id):
    """Helper function to log cache data to debug files"""
    try:
        # Create logs directory if it doesn't exist
        log_dir = "logs/precache_debug_logs"
        os.makedirs(log_dir, exist_ok=True)
        
        # Prepare log data
        log_entry = {
            "run_id": run_id,
            "timestamp": pendulum.now().isoformat(),
            "cache_name": cache_name,
            "data_type": type(data).__name__,
            "data_count": len(data) if data and hasattr(data, '__len__') else 0,
            "data": data
        }
        
        # Write directly to the "latest" file (no timestamped versions)
        latest_filepath = os.path.join(log_dir, f"{cache_name}_latest.json")
        with open(latest_filepath, 'w', encoding='utf-8') as f:
            json.dump(log_entry, f, indent=2, ensure_ascii=False, default=str)
        
        logger.debug(f"Run {run_id}: Logged {cache_name} cache data to {latest_filepath} ({log_entry['data_count']} items)")
            
    except Exception as e:
        logger.warning(f"Failed to log {cache_name} cache data to file: {e}")


def _group_by_tournament(data):
    """Group match data by tournament ID"""
    grouped = {}
    for item in data:
        tid = item.get("tournamentId")
        if tid is None:
            continue
        grouped.setdefault(tid, []).append(item)
    return grouped


def _group_by_match_id(data):
    """Group match data by match ID"""
    grouped = {}
    for item in data:
        mid = item.get("matchId")
        if mid is None:
            continue
        grouped[mid] = item
    return grouped


async def pre_cache_getter(token_manager, run_id=None):
    """PRE CACHE GETTER - Fetches API data without creating cache entries.
    
    This function gets fresh data from the external API and returns it as raw data
    for use in the periodically running function. It does NOT create cache entries.
    
    Args:
        token_manager: Token manager for API authentication
        run_id: Optional run ID for tracking API calls
    
    Returns:
        dict: Contains fresh data from API including seasons, tournaments, and matches
    """
    if not run_id:
        run_id = str(uuid.uuid4())[:8]
    
    logger.info(f"Run {run_id}: PRE CACHE GETTER starting - fetching fresh data from API")
    
    client = None
    api_calls = {"seasons": 0, "tournament_season": 0, "tournament_matches": 0}
    api_urls_called = []  # Track all URLs called during this run
    
    try:
        # Get authentication token
        token = await token_manager.get_token()
        headers = {"Authorization": f"Bearer {token['access_token']}"}
        client = get_http_client()
        
        # Helper function to track API calls
        def _track_api_call(url, method="GET", params=None, status=None):
            try:
                url_path = url.replace(config.API_URL, "") if config.API_URL in url else url
                params_str = json.dumps(params or {}, sort_keys=True)
                PRECACHE_API_URLS_CALLED.labels(
                    run_id=run_id,
                    url_path=url_path,
                    method=method,
                    params=params_str
                ).set(1)
                
                # Add to tracking list for logging
                api_urls_called.append({
                    "url": url,
                    "method": method,
                    "params": params,
                    "status": status,
                    "timestamp": pendulum.now().isoformat()
                })
            except Exception as e:
                logger.debug(f"Error tracking API call: {e}")
        
        # -----------------------------------------------------------------
        # Fetch season list for relevant sports and years
        seasons = []
        current_year = pendulum.now().year
        sport_ids = [72, 151]  # 72=Innebandy, 151=Bandy
        # start_year = max(2024, current_year - 1)  # this is a debug option
        start_year = current_year # fetch only the current year
        
        logger.info(f"Run {run_id}: PRE CACHE GETTER fetching seasons for years {start_year}-{current_year}, sports {sport_ids}")
        
        # Fetch seasons for each year and sport combination
        async def fetch_seasons_for_year_sport(year, sport_id):
            try:
                url = f"{config.API_URL}/api/v1/ta/Seasons/"
                params = {"year": year, "sportId": sport_id}
                
                resp = await client.get(url, headers=headers, params=params)
                _track_api_call(url, "GET", params, status=resp.status_code)
                api_calls["seasons"] += 1
                PRECACHE_API_CALLS.labels(call_type="seasons").inc()
                
                if resp.status_code < 400:
                    try:
                        raw = resp.json()
                        data = raw.get("seasons", [])
                        
                        # Normalize data structure
                        if isinstance(data, dict):
                            return [data]
                        elif isinstance(data, list):
                            return data
                        else:
                            logger.warning(f"Run {run_id}: Unexpected seasons data type for year={year} sport={sport_id}: {type(data).__name__}")
                            return []
                    except Exception as e:
                        logger.warning(f"Run {run_id}: Failed to parse seasons response for year={year} sport={sport_id}: {e}")
                        return []
                else:
                    logger.warning(f"Run {run_id}: Failed to fetch seasons for year={year} sport={sport_id}: {resp.status_code}")
                    return []
            except Exception as e:
                logger.error(f"Run {run_id}: Error fetching seasons for year={year} sport={sport_id}: {e}")
                return []
        
        # Fetch all season combinations in parallel
        season_tasks = []
        for year in range(start_year, current_year + 1):
            for sport_id in sport_ids:
                season_tasks.append(fetch_seasons_for_year_sport(year, sport_id))
        
        season_results = await asyncio.gather(*season_tasks, return_exceptions=True)
        
        # Flatten and filter seasons
        for result in season_results:
            if isinstance(result, list):
                seasons.extend(result)
            elif isinstance(result, Exception):
                logger.error(f"Run {run_id}: Season fetch task failed: {result}")
        
        # Filter valid seasons (2024+ and active)
        valid_seasons = []
        for season in seasons:
            if not season or not isinstance(season, dict):
                continue
                
            season_date_from = season.get("seasonDateFrom")
            if not season_date_from:
                continue
                
            # add a filter to exclude season.seasonName that includes the string "bedrift" 
            if "bedrift" in season.get("seasonName", "").lower():
                continue
                
            try:
                if "/" in season_date_from:
                    parsed_date = datetime.strptime(season_date_from.split()[0], "%m/%d/%Y")
                    season_year = parsed_date.year
                else:
                    parsed_date = pendulum.parse(season_date_from)
                    season_year = parsed_date.year
                
                if season_year >= 2024:
                    valid_seasons.append(season)
            except Exception as e:
                logger.debug(f"Run {run_id}: Could not parse season date for season {season.get('seasonId')}: {e}")
        
        logger.info(f"Run {run_id}: PRE CACHE GETTER found {len(valid_seasons)} valid seasons")
        
        # -----------------------------------------------------------------
        # Fetch tournaments for each valid season
        tournaments = []
        
        async def fetch_tournaments_for_season(season):
            try:
                season_id = season.get("seasonId")
                url = f"{config.API_URL}/api/v1/ta/Tournament/Season/{season_id}/"
                params = {"hierarchy": True}
                
                resp = await client.get(url, headers=headers, params=params)
                _track_api_call(url, "GET", params, status=resp.status_code)
                api_calls["tournament_season"] += 1
                PRECACHE_API_CALLS.labels(call_type="tournament_season").inc()
                
                if resp.status_code < 400:
                    try:
                        raw = resp.json()
                        data = raw.get("tournamentsInSeason", [])  # Fixed key name
                        
                        if isinstance(data, dict):
                            return [data]
                        elif isinstance(data, list):
                            return data
                        else:
                            logger.warning(f"Run {run_id}: Unexpected tournaments data type for season={season_id}: {type(data).__name__}")
                            return []
                    except Exception as e:
                        logger.warning(f"Run {run_id}: Failed to parse tournaments response for season={season_id}: {e}")
                        return []
                else:
                    logger.warning(f"Run {run_id}: Failed to fetch tournaments for season={season_id}: {resp.status_code}")
                    return []
            except Exception as e:
                logger.error(f"Run {run_id}: Error fetching tournaments for season={season.get('seasonId')}: {e}")
                return []
        
        # Fetch tournaments for all seasons in parallel
        tournament_tasks = [fetch_tournaments_for_season(season) for season in valid_seasons]
        tournament_results = await asyncio.gather(*tournament_tasks, return_exceptions=True)


        for result in tournament_results:
            if isinstance(result, list):
                tournaments.extend(result)
            elif isinstance(result, Exception):
                logger.error(f"Run {run_id}: Tournament fetch task failed: {result}")
        
        logger.info(f"Run {run_id}: PRE CACHE GETTER found {len(tournaments)} tournaments")
        
        # -----------------------------------------------------------------
        # Filter root tournaments (tournaments without parent)
        root_tournaments = [t for t in tournaments if not t.get("parentTournamentId")]
        logger.info(f"Run {run_id}: PRE CACHE GETTER found {len(root_tournaments)} root tournaments (out of {len(tournaments)} total)")
        
        # Fetch matches only for root tournaments
        matches = []
        unique_team_ids = set()
        
        async def fetch_matches_for_tournament(tournament):
            try:
                tournament_id = tournament.get("tournamentId")
                url = f"{config.API_URL}/api/v1/ta/TournamentMatches"
                params = {"tournamentId": tournament_id}
                
                resp = await client.get(url, headers=headers, params=params)
                _track_api_call(url, "GET", params, status=resp.status_code)
                api_calls["tournament_matches"] += 1
                PRECACHE_API_CALLS.labels(call_type="tournament_matches").inc()
                
                if resp.status_code < 400:
                    try:
                        # Check if response has content
                        response_text = resp.text
                        if not response_text or response_text.strip() == "":
                            logger.debug(f"Run {run_id}: Empty response for root tournament={tournament_id} matches (this may be normal)")
                            return []
                        
                        raw = resp.json()
                        
                        # Debug: Log the actual response structure
                        # if raw:
                        #     logger.debug(f"Run {run_id}: Tournament {tournament_id} API response keys: {list(raw.keys())}")
                        #     logger.debug(f"Run {run_id}: Tournament {tournament_id} full response: {json.dumps(raw, indent=2)[:500]}")
                        
                        data = raw.get("matches", [])  # Corrected: API returns "matches", not "tournamentMatches"
                        
                        if isinstance(data, dict):
                            return [data]
                        elif isinstance(data, list):
                            return data
                        else:
                            logger.warning(f"Run {run_id}: Unexpected matches data type for root tournament={tournament_id}: {type(data).__name__}")
                            return []
                    except Exception as e:
                        # Log more details about the parsing error
                        response_preview = resp.text[:200] if hasattr(resp, 'text') else 'N/A'
                        logger.warning(f"Run {run_id}: Failed to parse matches response for root tournament={tournament_id}: {e}")
                        logger.debug(f"Run {run_id}: Response preview for root tournament={tournament_id}: '{response_preview}'")
                        return []
                else:
                    logger.warning(f"Run {run_id}: Failed to fetch matches for root tournament={tournament_id}: {resp.status_code}")
                    return []
            except Exception as e:
                logger.error(f"Run {run_id}: Error fetching matches for root tournament={tournament.get('tournamentId')}: {e}")
                return []
        
        # Fetch matches only for root tournaments in parallel
        match_tasks = [fetch_matches_for_tournament(tournament) for tournament in root_tournaments]
        match_results = await asyncio.gather(*match_tasks, return_exceptions=True)
        
        for result in match_results:
            if isinstance(result, list):
                matches.extend(result)
                # Extract team IDs from matches
                for match in result:
                    if isinstance(match, dict):
                        home_team_id = match.get("homeTeamId")
                        away_team_id = match.get("awayTeamId")
                        if home_team_id:
                            unique_team_ids.add(home_team_id)
                        if away_team_id:
                            unique_team_ids.add(away_team_id)
            elif isinstance(result, Exception):
                logger.error(f"Run {run_id}: Match fetch task failed: {result}")
        
        team_ids_list = sorted(list(unique_team_ids))
        
        logger.info(f"Run {run_id}: PRE CACHE GETTER found {len(matches)} matches and {len(team_ids_list)} unique team IDs")
        
        # Return all fetched data
        result = {
            "run_id": run_id,
            "timestamp": pendulum.now().isoformat(),
            "api_calls": api_calls,
            "data": {
                "valid_seasons": valid_seasons,
                "tournaments_in_season": tournaments,
                "tournament_matches": matches,
                "unique_team_ids": team_ids_list
            }
        }
        
        logger.info(f"Run {run_id}: PRE CACHE GETTER completed successfully - fetched fresh data for {len(valid_seasons)} seasons, {len(tournaments)} tournaments, {len(matches)} matches, {len(team_ids_list)} teams")
        
        # Save API calls to log file
        try:
            log_dir = "logs/precache_debug_logs"
            os.makedirs(log_dir, exist_ok=True)
            
            calls_log = {
                "run_id": run_id,
                "timestamp": pendulum.now().isoformat(),
                "total_calls": len(api_urls_called),
                "calls_by_type": api_calls,
                "api_calls": api_urls_called
            }
            
            calls_filepath = os.path.join(log_dir, "precache_run_calls.json")
            with open(calls_filepath, 'w', encoding='utf-8') as f:
                json.dump(calls_log, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"Run {run_id}: Saved {len(api_urls_called)} API calls to {calls_filepath}")
        except Exception as e:
            logger.warning(f"Failed to save API calls log: {e}")
        
        return result
        
    except Exception as e:
        logger.error(f"Run {run_id}: PRE CACHE GETTER failed: {e}")
        logger.exception(e)
        raise
        
    finally:
        if client:
            await client.aclose()


async def compare_and_update_cache(fresh_data_result, cache_manager, run_id, start_time, run_timestamp):
    """Compare fresh data with cached data and update cache if changes detected.
    
    This function takes fresh data from pre_cache_getter, compares it with cached data,
    detects changes, updates the cache, and sets up cache warming for changed items.
    
    Args:
        fresh_data_result: Result dict from pre_cache_getter containing fresh data
        cache_manager: Cache manager for setting up cache refresh
        run_id: Run ID for tracking
        start_time: Start time of the run (pendulum datetime)
        run_timestamp: ISO format timestamp string
    
    Returns:
        dict: Contains changes detected, API calls made, and upstream status
    """
    redis_client = None
    changes_detected = {}
    changed_tournament_ids = set()
    changed_team_ids = []
    changed_match_ids = set()
    upstream_status = "UP"
    
    try:
        redis_client = get_redis_client()
        
        # Extract data from fresh_data_result
        api_calls = fresh_data_result["api_calls"]
        fresh_data = fresh_data_result["data"]
        
        valid_seasons = fresh_data["valid_seasons"]
        tournaments = fresh_data["tournaments_in_season"]
        tournament_matches = fresh_data["tournament_matches"]
        team_ids_list = fresh_data["unique_team_ids"]
        
        root_tournaments = [t for t in tournaments if not t.get("parentTournamentId")]
        
        logger.info(f"Run {run_id}: COMPARE AND UPDATE CACHE starting - comparing fresh data with cached data")
        
        # Helper function to record changes
        def _record_changes(category, count):
            """Helper function to record changes in both old and new metrics"""
            if count > 0:
                changes_detected[category] = count
                PRECACHE_CHANGES_DETECTED.labels(category=category).inc(count)
                PRECACHE_CHANGES_THIS_RUN.labels(category=category, run_id=run_id).set(count)
                PRECACHE_RUN_CHANGES_SUMMARY.labels(
                    run_id=run_id, 
                    run_timestamp=run_timestamp,
                    category=category
                ).set(count)
                logger.info(f"Run {run_id}: Detected {count} changes in {category}")
        
        # Helper function to get cached data
        async def _get_cached(key):
            raw = await redis_client.get(key)
            if not raw:
                return None
            try:
                return json.loads(raw).get("data")
            except Exception:
                return None
        
        # Load cached data
        cached_valid_seasons = await _get_cached("valid_seasons")
        cached_tournaments = await _get_cached("tournaments_in_season")
        cached_tournament_matches = await _get_cached("tournament_matches")
        cached_team_ids = await _get_cached("unique_team_ids") or []
        
        # Log cached data to debug files
        _log_cache_data_to_file("valid_seasons", cached_valid_seasons, run_id)
        _log_cache_data_to_file("tournaments_in_season", cached_tournaments, run_id)
        _log_cache_data_to_file("tournament_matches", cached_tournament_matches, run_id)
        _log_cache_data_to_file("unique_team_ids", cached_team_ids, run_id)
        
        logger.info(f"Run {run_id}: Loaded cached data - {len(cached_valid_seasons or [])} seasons, {len(cached_tournaments or [])} tournaments, {len(cached_tournament_matches or [])} matches, {len(cached_team_ids)} team IDs")
        
        # Log newly fetched data to debug files
        _log_cache_data_to_file("valid_seasons_fetched", valid_seasons, run_id)
        _log_cache_data_to_file("tournaments_in_season_fetched", tournaments, run_id)
        _log_cache_data_to_file("root_tournaments_fetched", root_tournaments, run_id)
        _log_cache_data_to_file("tournament_matches_fetched", tournament_matches, run_id)
        _log_cache_data_to_file("unique_team_ids_fetched", team_ids_list, run_id)
        
        # Update metrics for items processed
        PRECACHE_ITEMS_PROCESSED.labels(item_type="seasons").set(len(valid_seasons))
        PRECACHE_ITEMS_PROCESSED.labels(item_type="valid_seasons").set(len(valid_seasons))
        PRECACHE_ITEMS_PROCESSED.labels(item_type="tournaments").set(len(tournaments))
        PRECACHE_ITEMS_PROCESSED.labels(item_type="root_tournaments").set(len(root_tournaments))
        PRECACHE_ITEMS_PROCESSED.labels(item_type="tournament_matches").set(len(tournament_matches))
        PRECACHE_ITEMS_PROCESSED.labels(item_type="teams").set(len(team_ids_list))
        
        # Update cached data size metrics
        await _update_cached_data_size_metrics(redis_client)
        await _update_valid_seasons_metrics(redis_client)
        await _update_tournaments_in_season_metrics(redis_client)
        await _update_tournament_matches_metrics(redis_client)
        
        # -----------------------------------------------------------------
        # Compare and update valid_seasons
        valid_seasons_changed = json.dumps(valid_seasons, sort_keys=True) != json.dumps(cached_valid_seasons or [], sort_keys=True)
        
        logger.info(f"Run {run_id}: Data comparison - fetched {len(valid_seasons)} valid seasons, cached {len(cached_valid_seasons or [])} valid seasons")
        logger.info(f"Run {run_id}: Valid seasons changed: {valid_seasons_changed}")
        
        # Force update if we have valid seasons but cache is empty
        if len(valid_seasons) > 0 and (not cached_valid_seasons or len(cached_valid_seasons) == 0):
            logger.info(f"Forcing valid_seasons cache update: have {len(valid_seasons)} valid seasons but cache is empty")
            valid_seasons_changed = True
        
        if valid_seasons_changed:
            await redis_client.set(
                "valid_seasons",
                json.dumps(
                    {"data": valid_seasons, "last_updated": start_time.isoformat()},
                    ensure_ascii=False,
                ),
            )
            _record_changes("valid_seasons", len(valid_seasons))
            logger.info(f"Valid seasons cache updated with {len(valid_seasons)} seasons (was {len(cached_valid_seasons or [])} seasons)")
            
            # Verify the data was stored
            verification = await redis_client.get("valid_seasons")
            if verification:
                try:
                    parsed_verification = json.loads(verification)
                    stored_count = len(parsed_verification.get("data", []))
                    logger.info(f"VERIFICATION: Successfully stored {stored_count} valid seasons in Redis")
                    await _update_valid_seasons_metrics(redis_client)
                    logger.debug(f"Updated valid seasons content and count metrics")
                except Exception as e:
                    logger.error(f"VERIFICATION FAILED: Could not parse stored valid_seasons data: {e}")
            else:
                logger.error("VERIFICATION FAILED: No data found in Redis after attempted storage")
        else:
            logger.debug(f"No valid seasons changes detected - cache has {len(cached_valid_seasons or [])} seasons, fetched {len(valid_seasons)}")
            await _update_valid_seasons_metrics(redis_client)
        
        # -----------------------------------------------------------------
        # Compare and update tournaments
        tournaments_changed = json.dumps(tournaments, sort_keys=True) != json.dumps(cached_tournaments or [], sort_keys=True)
        
        logger.info(f"Run {run_id}: Data comparison - fetched {len(tournaments)} tournaments, cached {len(cached_tournaments or [])} tournaments")
        logger.info(f"Run {run_id}: Tournaments changed: {tournaments_changed}")
        
        if tournaments_changed:
            await redis_client.set(
                "tournaments_in_season",
                json.dumps(
                    {"data": tournaments, "last_updated": start_time.isoformat()},
                    ensure_ascii=False,
                ),
            )
            
            # Store root tournaments separately
            await redis_client.set(
                "root_tournaments",
                json.dumps(
                    {"data": root_tournaments, "last_updated": start_time.isoformat()},
                    ensure_ascii=False,
                ),
            )
            
            # Calculate which tournaments changed
            cached_ids = {t["tournamentId"] for t in (cached_tournaments or []) if "tournamentId" in t}
            new_ids = {t["tournamentId"] for t in tournaments if "tournamentId" in t}
            changed_tournament_ids = new_ids.symmetric_difference(cached_ids)
            
            _record_changes("tournaments_in_season", len(changed_tournament_ids))
            logger.info(f"Tournament changes detected: {len(changed_tournament_ids)} tournaments changed")
            logger.info(f"Updated tournaments_in_season cache with {len(tournaments)} tournaments")
            await _update_tournaments_in_season_metrics(redis_client)
            await _update_tournament_matches_metrics(redis_client)
        else:
            changed_tournament_ids = set()
            logger.debug(f"No tournament changes detected - cache has {len(cached_tournaments or [])} tournaments, fetched {len(tournaments)}")
            await _update_tournaments_in_season_metrics(redis_client)
            await _update_tournament_matches_metrics(redis_client)
        
        # -----------------------------------------------------------------
        # Compare and update matches
        matches_changed = json.dumps(tournament_matches, sort_keys=True) != json.dumps(cached_tournament_matches or [], sort_keys=True)
        
        logger.info(f"Run {run_id}: Data comparison - fetched {len(tournament_matches)} matches, cached {len(cached_tournament_matches or [])} matches")
        logger.info(f"Run {run_id}: Matches changed: {matches_changed}")
        
        if matches_changed:
            await redis_client.set(
                "tournament_matches",
                json.dumps(
                    {"data": tournament_matches, "last_updated": start_time.isoformat()},
                    ensure_ascii=False,
                ),
            )
            
            # Detect changed tournaments (by comparing match lists per tournament)
            new_group = _group_by_tournament(tournament_matches)
            old_group = _group_by_tournament(cached_tournament_matches or [])
            
            for tid, new_list in new_group.items():
                old_list = old_group.get(tid, [])
                if json.dumps(new_list, sort_keys=True) != json.dumps(old_list, sort_keys=True):
                    changed_tournament_ids.add(tid)
            
            for tid in old_group:
                if tid not in new_group:
                    changed_tournament_ids.add(tid)
            
            # Detect changed individual matches
            new_matches_by_id = _group_by_match_id(tournament_matches)
            old_matches_by_id = _group_by_match_id(cached_tournament_matches or [])
            
            for match_id, new_match in new_matches_by_id.items():
                old_match = old_matches_by_id.get(match_id, {})
                if json.dumps(new_match, sort_keys=True) != json.dumps(old_match, sort_keys=True):
                    changed_match_ids.add(match_id)
            
            for match_id in old_matches_by_id:
                if match_id not in new_matches_by_id:
                    changed_match_ids.add(match_id)
            
            _record_changes("tournament_matches", len(changed_tournament_ids))
            _record_changes("individual_matches", len(changed_match_ids))
            logger.info(f"Matches changes detected: {len(changed_tournament_ids)} tournaments, {len(changed_match_ids)} individual matches (after date filtering)")
        else:
            changed_match_ids = set()
            logger.debug("No match changes detected")
        
        # -----------------------------------------------------------------
        # Compare and update team IDs
        team_ids = set()
        for match in tournament_matches:
            for key in ("hometeamId", "awayteamId"):
                tid = match.get(key)
                if tid is not None:
                    team_ids.add(tid)
        
        _log_cache_data_to_file("unique_team_ids_fetched", list(team_ids), run_id)
        
        if team_ids != set(cached_team_ids):
            await redis_client.set(
                "unique_team_ids",
                json.dumps(
                    {"data": list(team_ids), "last_updated": start_time.isoformat()},
                    ensure_ascii=False,
                ),
            )
            changed_team_ids = list(team_ids.symmetric_difference(set(cached_team_ids)))
            _record_changes("unique_team_ids", len(changed_team_ids))
            logger.info(f"Team changes detected: {len(changed_team_ids)} teams")
        
        # Update metrics after all changes
        await _update_cached_data_size_metrics(redis_client)
        await _update_valid_seasons_metrics(redis_client)
        await _update_tournaments_in_season_metrics(redis_client)
        await _update_tournament_matches_metrics(redis_client)
        
        # Ensure all categories are recorded (even with 0 changes) for consistent time-series data
        all_categories = ["valid_seasons", "tournaments_in_season", "tournament_matches", "individual_matches", "unique_team_ids"]
        for category in all_categories:
            if category not in changes_detected:
                PRECACHE_CHANGES_THIS_RUN.labels(category=category, run_id=run_id).set(0)
                PRECACHE_RUN_CHANGES_SUMMARY.labels(
                    run_id=run_id, 
                    run_timestamp=run_timestamp,
                    category=category
                ).set(0)
        
        logger.info(f"Run {run_id}: COMPARE AND UPDATE CACHE completed - detected changes: {changes_detected}")
        
        return {
            "changes_detected": changes_detected,
            "changed_tournament_ids": changed_tournament_ids,
            "changed_team_ids": changed_team_ids,
            "changed_match_ids": changed_match_ids,
            "api_calls": api_calls,
            "upstream_status": upstream_status
        }
        
    except Exception as e:
        logger.error(f"Run {run_id}: COMPARE AND UPDATE CACHE failed: {e}")
        logger.exception(e)
        raise
    finally:
        if redis_client:
            await redis_client.close()


async def process_match_cache_batch(match_batch, match_endpoint_configs, cache_manager, ttl, refresh_until, run_id):
    """Process a batch of matches for cache warming with rate limiting"""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_CACHE_SETUPS)
    
    async def setup_match_cache(match_id):
        async with semaphore:
            for endpoint_config in match_endpoint_configs:
                try:
                    if endpoint_config.get("path_param"):
                        # Path includes match_id (e.g., matchteammembers)
                        path = endpoint_config["path"].format(match_id=match_id)
                        target_url = f"{config.API_URL}/{path}"
                        cache_key = f"GET:{path}?{endpoint_config['param_key']}={endpoint_config['param_value']}"
                        params = {endpoint_config["param_key"]: endpoint_config["param_value"]}
                    else:
                        # Query parameter includes match_id
                        path = endpoint_config["path"]
                        target_url = f"{config.API_URL}/{path}"
                        cache_key = f"GET:{path}?{endpoint_config['param_key']}={match_id}"
                        params = {endpoint_config["param_key"]: match_id}
                    
                    await cache_manager.setup_refresh(
                        cache_key, target_url, ttl, refresh_until, params=params
                    )
                    logger.debug(f"Added cache refresh for changed match {match_id}: {cache_key}")
                    
                    # Small delay between cache setups to prevent overwhelming Redis
                    await asyncio.sleep(CACHE_SETUP_DELAY)
                    
                except Exception as e:
                    logger.error(f"Error setting up cache for match {match_id} endpoint {endpoint_config['path']}: {e}")
    
    # Process all matches in this batch concurrently but with limited concurrency
    tasks = [setup_match_cache(match_id) for match_id in match_batch]
    await asyncio.gather(*tasks, return_exceptions=True)


async def process_team_cache_batch(team_batch, cache_manager, ttl, refresh_until, run_id):
    """Process a batch of teams for cache warming with rate limiting"""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_CACHE_SETUPS)
    
    async def setup_team_cache(team_id):
        async with semaphore:
            try:
                # Construct URL and cache key to match format from main.py
                # main.py uses: cache_key = f"{request.method}:{key_path}" where key_path includes query params
                path = f"api/v1/org/teams"
                target_url = f"{config.API_URL}/{path}"
                # Cache key format: GET:path?params (matching main.py line 642)
                cache_key = f"GET:{path}?orgId={team_id}"
                await cache_manager.setup_refresh(
                    cache_key, target_url, ttl, refresh_until, params={"orgId": team_id}
                )
                logger.debug(f"Added cache refresh for changed team: {team_id} with cache_key: {cache_key}")
                
                # Small delay between cache setups to prevent overwhelming Redis
                await asyncio.sleep(CACHE_SETUP_DELAY)
                
            except Exception as e:
                logger.error(f"Error setting up cache for team {team_id}: {e}")
    
    # Process all teams in this batch concurrently but with limited concurrency
    tasks = [setup_team_cache(team_id) for team_id in team_batch]
    await asyncio.gather(*tasks, return_exceptions=True)


async def detect_change_tournaments_and_matches(cache_manager, token_manager):
    """Periodically fetch season, tournament, match and team data.

    This function fetches data from the DATA.NIF.NO API, compares it to cached data,
    """
    
    global CACHE_WARMING_IN_PROGRESS
    
    try:
        # Log function entry
        logger.info(f"detect_change_tournaments_and_matches function called with cache_manager={type(cache_manager).__name__}, token_manager={type(token_manager).__name__}")
    except Exception as e:
        logger.error(f"Error logging function entry: {e}")
    
    loop_iteration = 0

    while True:
        try:
            loop_iteration += 1
            
            # Check if cache warming is in progress
            if CACHE_WARMING_IN_PROGRESS:
                logger.info(f"Precache iteration {loop_iteration} skipped - cache warming in progress")
                await asyncio.sleep(30)  # Wait 30 seconds before checking again
                continue
                
            logger.info(f"Starting precache loop iteration {loop_iteration}")
            
            start_time = pendulum.now()
            run_id = str(uuid.uuid4())[:8]  # Short unique ID for this run
            run_timestamp = start_time.isoformat()
            api_calls = {"seasons": 0, "tournament_season": 0, "tournament_matches": 0}
            changes_detected = {}
            changed_tournament_ids = set()
            changed_team_ids = []
            changed_match_ids = set()
            upstream_status = "UP"

            logger.info(f"Starting precache run {run_id} at {run_timestamp} (iteration {loop_iteration})")
            
            # Clear previous run metrics to ensure fresh data for this run
            PRECACHE_CHANGES_THIS_RUN.clear()
            PRECACHE_RUN_CHANGES_SUMMARY.clear()
            
        except Exception as loop_init_error:
            logger.error(f"Critical error in precache loop initialization: {loop_init_error}")
            logger.exception(loop_init_error)
            # Sleep and continue to prevent rapid error loops
            await asyncio.sleep(30)
            continue

        # Start timing for metrics
        with PRECACHE_DURATION_SECONDS.time():
            try:
                # Step 1: Fetch fresh data from upstream API (without creating cache entries)
                logger.info(f"Run {run_id}: Step 1 - Fetching fresh data from upstream API")
                
                try:
                    fresh_data_result = await pre_cache_getter(token_manager, run_id)
                    api_calls = fresh_data_result["api_calls"]
                except Exception as e:
                    logger.error(f"Run {run_id}: Failed to fetch fresh data: {e}")
                    # Set upstream status to DOWN and continue to next iteration
                    PRECACHE_UPSTREAM_STATUS.labels(endpoint="data.nif.no").set(0)
                    raise
                
                # Step 2: Compare fresh data with cached data and update cache
                logger.info(f"Run {run_id}: Step 2 - Comparing data and updating cache")
                
                try:
                    comparison_result = await compare_and_update_cache(
                        fresh_data_result, 
                        cache_manager, 
                        run_id, 
                        start_time, 
                        run_timestamp
                    )
                    
                    changes_detected = comparison_result["changes_detected"]
                    changed_tournament_ids = comparison_result["changed_tournament_ids"]
                    changed_team_ids = comparison_result["changed_team_ids"]
                    changed_match_ids = comparison_result["changed_match_ids"]
                    upstream_status = comparison_result["upstream_status"]

                    # These are the endpoints we want to warm for a match page:
                    # api/v1/org/teams?orgId=
                    # api/v1/org/teams?orgId=
                    # api/v1/ta/match/?matchId=
                    # api/v1/ta/matchincidents/?matchId=
                    # api/v1/ta/matchreferee?matchId=
                    # api/v1/ta/matchteammembers/8224291/?images=false
                    # api/v1/ta/tournament/?tournamentId=440904
                    # api/v1/ta/tournament/season/201065/?hierarchy=true
                    # api/v1/ta/venue/?venueId=10143
                    # api/v1/ta/venueunit/?venueUnitId=33866

                    # if upstream_status is DOWN, do not replace data stored in any cache
                    if upstream_status == "DOWN":
                        logger.error(f"Run {run_id}: Upstream status is DOWN, aborting cache update")
                        raise Exception("Upstream status is DOWN, aborting cache update")

                    # Step 3: Warm caches for changed items
                    logger.info(f"Run {run_id}: Step 3 - Warming caches for changed items")
                    
                    # Set cache warming flag to prevent other iterations from running
                    cache_warming_needed = bool(changed_tournament_ids or changed_team_ids or changed_match_ids)
                    
                    if cache_warming_needed:
                        CACHE_WARMING_IN_PROGRESS = True
                        logger.info(f"Run {run_id}: Cache warming started - future precache iterations will be suspended")
                    
                    try:
                        # Define cache warming parameters
                        refresh_until = pendulum.now().add(days=7)
                        ttl = 7 * 24 * 60 * 60

                        if changed_tournament_ids:
                            logger.info(f"Run {run_id}: Detected changes in {len(changed_tournament_ids)} tournaments")

                        if changed_team_ids:
                            logger.info(f"Run {run_id}: Detected changes in {len(changed_team_ids)} teams")
                            
                            # Process teams in batches to prevent overwhelming the system
                            team_ids_list = list(changed_team_ids)
                            for batch_start in range(0, len(team_ids_list), BATCH_SIZE):
                                batch_end = min(batch_start + BATCH_SIZE, len(team_ids_list))
                                team_batch = team_ids_list[batch_start:batch_end]
                                
                                logger.info(f"Run {run_id}: Processing team batch {batch_start//BATCH_SIZE + 1}/{(len(team_ids_list) + BATCH_SIZE - 1)//BATCH_SIZE} ({len(team_batch)} teams)")
                                
                                # Process this batch of teams
                                await process_team_cache_batch(team_batch, cache_manager, ttl, refresh_until, run_id)
                                
                                # Delay between batches to prevent overwhelming the system
                                if batch_end < len(team_ids_list):
                                    logger.debug(f"Run {run_id}: Waiting {BATCH_DELAY}s before next batch")
                                    await asyncio.sleep(BATCH_DELAY)

                        if changed_match_ids:
                            # Limit the number of matches to cache to prevent system overload
                            if len(changed_match_ids) > MAX_MATCHES_TO_CACHE:
                                logger.warning(f"Run {run_id}: Too many changed matches ({len(changed_match_ids)}), limiting to {MAX_MATCHES_TO_CACHE} most recent ones")
                                changed_match_ids = set(list(changed_match_ids)[:MAX_MATCHES_TO_CACHE])
                            
                            logger.info(f"Run {run_id}: Detected changes in {len(changed_match_ids)} matches")
                            total_cache_entries = len(changed_match_ids) * 4  # 4 endpoints per match
                            logger.info(f"Run {run_id}: Will create {total_cache_entries} cache entries in batches")
                            
                            # Set up cache entries for match-related endpoints
                            match_endpoint_configs = [
                                {"path": "api/v1/ta/match/", "param_key": "matchId", "path_param": False},
                                {"path": "api/v1/ta/matchincidents/", "param_key": "matchId", "path_param": False},
                                {"path": "api/v1/ta/matchreferee", "param_key": "matchId", "path_param": False},
                                {"path": "api/v1/ta/matchteammembers/{match_id}/", "param_key": "images", "param_value": "false", "path_param": True}
                            ]
                            
                            # Process matches in batches to prevent overwhelming the system
                            match_ids_list = list(changed_match_ids)
                            for batch_start in range(0, len(match_ids_list), BATCH_SIZE):
                                batch_end = min(batch_start + BATCH_SIZE, len(match_ids_list))
                                match_batch = match_ids_list[batch_start:batch_end]
                                
                                logger.info(f"Run {run_id}: Processing match batch {batch_start//BATCH_SIZE + 1}/{(len(match_ids_list) + BATCH_SIZE - 1)//BATCH_SIZE} ({len(match_batch)} matches)")
                                
                                # Process this batch of matches
                                await process_match_cache_batch(match_batch, match_endpoint_configs, cache_manager, ttl, refresh_until, run_id)
                                
                                # Delay between batches to prevent overwhelming the system
                                if batch_end < len(match_ids_list):
                                    logger.debug(f"Run {run_id}: Waiting {BATCH_DELAY}s before next batch")
                                    await asyncio.sleep(BATCH_DELAY)
                        
                        logger.info(f"Run {run_id}: Cache warming setup complete")
                        
                    finally:
                        # Always clear the cache warming flag when done
                        if cache_warming_needed:
                            CACHE_WARMING_IN_PROGRESS = False
                            logger.info(f"Run {run_id}: Cache warming completed - precache iterations can resume")
                    
                except Exception as e:
                    logger.error(f"Run {run_id}: Failed to compare and update cache: {e}")
                    raise

                # Record successful run
                PRECACHE_RUNS_TOTAL.labels(status="success").inc()
                PRECACHE_LAST_RUN_TIMESTAMP.set(start_time.timestamp())
                
                # Set upstream status to UP if we made it this far
                PRECACHE_UPSTREAM_STATUS.labels(endpoint="data.nif.no").set(1)
                
                end_time = pendulum.now()
                
                # Log summary of this run
                total_changes = sum(changes_detected.values())
                logger.info(f"Run {run_id} completed: {total_changes} total changes across {len(changes_detected)} categories in {(end_time - start_time).total_seconds():.2f}s")
                logger.info(f"Run {run_id} summary: Made {sum(api_calls.values())} API calls to data.nif.no, found changes: {changes_detected}")
                logger.info(f"Run {run_id} upstream status: {upstream_status}")
                
                log_item = {
                    "action": "pre_cache_process",
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "process_time_seconds": (end_time - start_time).total_seconds(),
                    "api_calls_made": api_calls,
                    "changes_detected": {k: int(v) for k, v in changes_detected.items()},
                    "run_id": run_id,
                    "total_changes": total_changes,
                    "upstream_status": upstream_status
                }
                logger.info(f"logProcess: {json.dumps(log_item, ensure_ascii=False)}")
                
                # Save precache run stats to debug file
                try:
                    # Create logs directory if it doesn't exist
                    log_dir = "logs/precache_debug_logs"
                    os.makedirs(log_dir, exist_ok=True)
                    
                    # Save current run stats to the latest file
                    stats_filepath = os.path.join(log_dir, "precache_run_stats.json")
                    with open(stats_filepath, 'w', encoding='utf-8') as f:
                        json.dump(log_item, f, indent=2, ensure_ascii=False, default=str)
                    
                    logger.info(f"Run {run_id}: Saved run stats to {stats_filepath}")
                    
                    # Also maintain a history file with the last 10 runs
                    history_filepath = os.path.join(log_dir, "precache_run_history.json")
                    
                    # Load existing history
                    run_history = []
                    if os.path.exists(history_filepath):
                        try:
                            with open(history_filepath, 'r', encoding='utf-8') as f:
                                run_history = json.load(f)
                        except Exception as e:
                            logger.warning(f"Could not load existing run history: {e}")
                            run_history = []
                    
                    # Add current run to history
                    run_history.append(log_item)
                    
                    # Keep only last 10 runs
                    run_history = run_history[-10:]
                    
                    # Save updated history
                    with open(history_filepath, 'w', encoding='utf-8') as f:
                        json.dump(run_history, f, indent=2, ensure_ascii=False, default=str)
                    
                    logger.debug(f"Run {run_id}: Updated run history with {len(run_history)} entries")
                    
                except Exception as e:
                    logger.warning(f"Failed to save precache run stats to file: {e}")
                
                # Note: Prometheus metrics will be automatically cleaned up by metric expiration
                # The PRECACHE_CHANGES_THIS_RUN and PRECACHE_RUN_CHANGES_SUMMARY metrics
                # will persist for the configured scrape intervals to allow graphing

            except Exception as e:
                # Set upstream status to DOWN on any critical error
                PRECACHE_UPSTREAM_STATUS.labels(endpoint="data.nif.no").set(0)
                logger.error(f"Run {run_id} failed - upstream status set to DOWN due to error")
                
                # Record failed run
                PRECACHE_RUNS_TOTAL.labels(status="error").inc()
                
                # Record error metrics with zero changes for this run to maintain time-series continuity
                all_categories = ["valid_seasons", "tournaments_in_season", "tournament_matches", "individual_matches", "unique_team_ids"]
                for category in all_categories:
                    PRECACHE_CHANGES_THIS_RUN.labels(category=category, run_id=run_id).set(-1)  # -1 indicates error
                    PRECACHE_RUN_CHANGES_SUMMARY.labels(
                        run_id=run_id, 
                        run_timestamp=run_timestamp,
                        category=category
                    ).set(-1)  # -1 indicates error
                
                # Clear URL metrics for this run in case of error
                try:
                    # Clear any partial URL metrics for this run_id
                    pass  # The PRECACHE_API_URLS_CALLED metric will naturally expire
                except Exception as cleanup_error:
                    logger.debug(f"Error during metric cleanup: {cleanup_error}")
                
                logger.error(f"Run {run_id} failed: {e}")
                logger.error(f"Error in detect_change_tournaments_and_matches: {e}")
                logger.exception(e)
                
                # Save error run stats to debug file
                try:
                    end_time = pendulum.now()
                    error_log_item = {
                        "action": "pre_cache_process",
                        "start_time": start_time.isoformat(),
                        "end_time": end_time.isoformat(),
                        "process_time_seconds": (end_time - start_time).total_seconds(),
                        "api_calls_made": api_calls if 'api_calls' in locals() else {},
                        "changes_detected": {},
                        "run_id": run_id,
                        "total_changes": 0,
                        "upstream_status": "DOWN",
                        "error": str(e),
                        "status": "failed"
                    }
                    
                    # Create logs directory if it doesn't exist
                    log_dir = "logs/precache_debug_logs"
                    os.makedirs(log_dir, exist_ok=True)
                    
                    # Save error run stats to the latest file
                    stats_filepath = os.path.join(log_dir, "precache_run_stats.json")
                    with open(stats_filepath, 'w', encoding='utf-8') as f:
                        json.dump(error_log_item, f, indent=2, ensure_ascii=False, default=str)
                    
                    logger.info(f"Run {run_id}: Saved error run stats to {stats_filepath}")
                    
                    # Also add to history file
                    history_filepath = os.path.join(log_dir, "precache_run_history.json")
                    
                    # Load existing history
                    run_history = []
                    if os.path.exists(history_filepath):
                        try:
                            with open(history_filepath, 'r', encoding='utf-8') as f:
                                run_history = json.load(f)
                        except Exception as hist_e:
                            logger.warning(f"Could not load existing run history: {hist_e}")
                            run_history = []
                    
                    # Add error run to history
                    run_history.append(error_log_item)
                    
                    # Keep only last 10 runs
                    run_history = run_history[-10:]
                    
                    # Save updated history
                    with open(history_filepath, 'w', encoding='utf-8') as f:
                        json.dump(run_history, f, indent=2, ensure_ascii=False, default=str)
                    
                    logger.debug(f"Run {run_id}: Added error run to history with {len(run_history)} total entries")
                    
                except Exception as save_error:
                    logger.warning(f"Failed to save error run stats to file: {save_error}")
            finally:
                pass  # Resources are cleaned up inside pre_cache_getter and compare_and_update_cache

        logger.info(f"Precache run {run_id} completed, sleeping for 180 seconds before next iteration {loop_iteration + 1}")
        await asyncio.sleep(180)
        logger.info(f"Sleep completed, starting next precache iteration {loop_iteration + 1}")
