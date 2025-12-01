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
from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, REGISTRY

from .config import config
from .token import TokenManager
from .util import get_http_client, get_logger, get_redis_client


logger = get_logger(__name__)

# Helper function to safely create metrics (handles hot reloading)
def safe_metric(metric_type, name, description, labelnames=None):
    """Safely create a Prometheus metric, handling re-registration during hot reloading."""
    try:
        return metric_type(name, description, labelnames or [])
    except ValueError as e:
        if "Duplicated timeseries" in str(e):
            # Metric already exists, try to retrieve it
            for collector in REGISTRY._collector_to_names:
                if hasattr(collector, '_name') and collector._name == name:
                    return collector
            # If we can't find it, create with a modified name
            return metric_type(f"{name}_reloaded", description, labelnames or [])
        else:
            raise

# Batch processing configuration to prevent overwhelming the system
MAX_CONCURRENT_CACHE_SETUPS = 10  # Max simultaneous cache setup operations
BATCH_SIZE = 50  # Process cache entries in batches of 50
BATCH_DELAY = 2.0  # Delay between batches in seconds
CACHE_SETUP_DELAY = 0.1  # Delay between individual cache setups
MAX_MATCHES_TO_CACHE = 1000  # Maximum number of matches to cache in one run

# Global flag to track if cache warming is in progress
CACHE_WARMING_IN_PROGRESS = False

# Global flag to track if a full run is needed (after clear/delete operations)
FULL_RUN_NEEDED = False

# Prometheus metrics for pre-cache operations
PRECACHE_RUNS_TOTAL = safe_metric(Counter,
    "precache_runs_total",
    "Total number of pre-cache periodic runs",
    ["status"]  # success, error
)

PRECACHE_CHANGES_DETECTED = safe_metric(Counter,
    "precache_changes_detected_total",
    "Total number of changes detected by category",
    ["category"]  # seasons, tournaments_in_season, tournament_matches, unique_team_ids
)

PRECACHE_CHANGES_THIS_RUN = safe_metric(Gauge,
    "precache_changes_this_run",
    "Number of changes detected in this specific run by category",
    ["category", "run_id"]  # seasons, tournaments_in_season, tournament_matches, individual_matches, unique_team_ids
)

PRECACHE_RUN_CHANGES_SUMMARY = safe_metric(Gauge,
    "precache_run_changes_summary",
    "Summary of all changes detected in a specific run with run metadata",
    ["run_id", "run_timestamp", "category"]
)

PRECACHE_API_CALLS = safe_metric(Counter,
    "precache_api_calls_total", 
    "Total API calls made during pre-cache operations",
    ["call_type"]  # seasons, tournament_season, tournament_matches
)

PRECACHE_DURATION_SECONDS = safe_metric(Histogram,
    "precache_duration_seconds",
    "Time spent in pre-cache operations"
)

PRECACHE_LAST_RUN_TIMESTAMP = safe_metric(Gauge,
    "precache_last_run_timestamp",
    "Timestamp of the last pre-cache run"
)

PRECACHE_ITEMS_PROCESSED = safe_metric(Gauge,
    "precache_items_processed",
    "Number of items processed in last pre-cache run",
    ["item_type"]  # seasons, tournaments, matches, teams
)

PRECACHE_CACHED_DATA_SIZE = safe_metric(Gauge,
    "precache_cached_data_size_bytes",
    "Size in bytes of cached data",
    ["data_type"]  # valid_seasons, tournaments_in_season, tournament_matches, unique_team_ids
)

PRECACHE_API_CALL_SUCCESS_RATE = safe_metric(Gauge,
    "precache_api_call_success_rate",
    "Success rate of precache API calls",
    ["call_type"]  # seasons, tournament_season, tournament_matches
)

PRECACHE_VALID_SEASONS_COUNT = safe_metric(Gauge,
    "precache_valid_seasons_count",
    "Total number of valid seasons currently cached"
)

PRECACHE_VALID_SEASONS_INFO = safe_metric(Gauge,
    "precache_valid_seasons_info",
    "Information about valid seasons with season details as labels",
    ["season_id", "season_name", "season_year", "sport_id", "sport_name"]
)

# New metrics for different run types
PRECACHE_RUN_TYPE_COUNTER = safe_metric(Counter,
    "precache_run_type_total",
    "Total number of each type of precache run",
    ["run_type"]  # primary, secondary, tertiary
)

PRECACHE_RUN_TYPE_DURATION = safe_metric(Histogram,
    "precache_run_type_duration_seconds",
    "Duration of each type of precache run",
    ["run_type"]  # primary, secondary, tertiary
)

PRECACHE_RUN_TYPE_LAST_SUCCESS = safe_metric(Gauge,
    "precache_run_type_last_success_timestamp",
    "Timestamp of last successful run for each type",
    ["run_type"]  # primary, secondary, tertiary
)

PRECACHE_SECONDARY_COUNTER = safe_metric(Gauge,
    "precache_secondary_run_counter",
    "Counter for secondary runs executed"
)

PRECACHE_TERTIARY_COUNTER = safe_metric(Gauge,
    "precache_tertiary_run_counter", 
    "Counter for tertiary runs executed"
)

PRECACHE_FULL_RUNS_TOTAL = safe_metric(Counter,
    "precache_full_runs_total",
    "Total number of full runs (PRIMARY+SECONDARY+TERTIARY) executed after clear operations"
)

PRECACHE_TOURNAMENTS_IN_SEASON_COUNT = safe_metric(Gauge,
    "precache_tournaments_in_season_count",
    "Total number of tournaments in season currently cached"
)

PRECACHE_TOURNAMENTS_BY_SEASON = safe_metric(Gauge,
    "precache_tournaments_by_season",
    "Number of tournaments per season",
    ["season_id", "sport_id", "sport_name"]
)

PRECACHE_TOURNAMENTS_BY_TYPE = safe_metric(Gauge,
    "precache_tournaments_by_type",
    "Number of tournaments by root/child type per sport",
    ["sport_id", "sport_name", "is_root"]
)

PRECACHE_TOURNAMENT_MATCHES_COUNT = safe_metric(Gauge,
    "precache_tournament_matches_count",
    "Total number of tournament matches currently cached"
)

PRECACHE_TOURNAMENT_MATCHES_BY_TOURNAMENT = safe_metric(Gauge,
    "precache_tournament_matches_by_tournament",
    "Number of matches per tournament",
    ["tournament_id", "tournament_name", "season_id"]
)

PRECACHE_TOURNAMENT_MATCHES_BY_SEASON = safe_metric(Gauge,
    "precache_tournament_matches_by_season",
    "Number of matches per season",
    ["season_id"]
)

PRECACHE_API_URLS_CALLED = safe_metric(Gauge,
    "precache_api_urls_called",
    "URLs called to data.nif.no API during precache runs - tracks detailed API call information per run",
    ["run_id", "url_path", "method", "params"]
)

PRECACHE_UPSTREAM_STATUS = safe_metric(Gauge,
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
            "unique_team_ids",
            "clubs_data",
            "teams_data",
            "venues_data",
            "venue_details"
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
        
        # Set flag to trigger a full run on next iteration
        global FULL_RUN_NEEDED
        FULL_RUN_NEEDED = True
        logger.info("Set FULL_RUN_NEEDED flag - next precache iteration will run PRIMARY, SECONDARY, and TERTIARY")
        
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


async def pre_cache_getter_primary(token_manager, run_id=None):
    """PRIMARY RUN PRE CACHE GETTER - Fetches basic API data (seasons, tournaments, matches).
    
    This is the fast primary data fetch that runs every 3 minutes (~30 seconds execution time).
    Gets essential data: seasons, tournaments, and matches from DATA.NIF.NO.
    
    Args:
        token_manager: Token manager for API authentication
        run_id: Optional run ID for tracking API calls
    
    Returns:
        dict: Contains fresh basic data from API including seasons, tournaments, and matches
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

        # fetch teams from root tournaments - as with match
        

        
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
            
            calls_filepath = os.path.join(log_dir, "precache_primary_calls.json")
            with open(calls_filepath, 'w', encoding='utf-8') as f:
                json.dump(calls_log, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"Run {run_id}: Saved {len(api_urls_called)} PRIMARY API calls to {calls_filepath}")
        except Exception as e:
            logger.warning(f"Failed to save API calls log: {e}")
        
        return result
        
    except Exception as e:
        logger.error(f"Run {run_id}: PRE CACHE GETTER PRIMARY failed: {e}")
        logger.exception(e)
        raise
        
    finally:
        if client:
            await client.aclose()


async def pre_cache_getter_secondary(token_manager, run_id=None):
    """SECONDARY RUN PRE CACHE GETTER - Fetches teams data.
    
    This runs every 9 minutes (3 runs) and takes ~5 minutes execution time.
    Gets teams data from DATA.NIF.NO.
    
    Args:
        token_manager: Token manager for API authentication
        run_id: Optional run ID for tracking API calls
    
    Returns:
        dict: Contains fresh teams data from API
    """
    if not run_id:
        run_id = str(uuid.uuid4())[:8]
    
    logger.info(f"Run {run_id}: PRE CACHE GETTER SECONDARY starting - fetching teams data from API")
    
    client = None
    api_calls = {"teams": 0}
    api_urls_called = []
    
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
        # Get teams data from unique team IDs (if available from previous primary run)
        teams_data = []
        redis_client = None
        
        try:
            redis_client = get_redis_client()
            cached_team_ids_raw = await redis_client.get("unique_team_ids")
            if cached_team_ids_raw:
                cached_team_ids_data = json.loads(cached_team_ids_raw)
                team_ids_list = cached_team_ids_data.get("data", [])
                
                logger.info(f"Run {run_id}: PRE CACHE GETTER SECONDARY fetching team data for {len(team_ids_list)} teams")
                
                # Process all teams to ensure accurate change detection
                # Note: Batch processing below will handle performance concerns
                team_ids_to_process = team_ids_list
                
                async def fetch_team_data(team_id):
                    try:
                        url = f"{config.API_URL}/api/v1/org/teams"
                        params = {"orgId": team_id}
                        
                        resp = await client.get(url, headers=headers, params=params)
                        _track_api_call(url, "GET", params, status=resp.status_code)
                        api_calls["teams"] += 1
                        
                        if resp.status_code < 400:
                            try:
                                raw = resp.json()
                                return raw
                            except Exception as e:
                                logger.warning(f"Run {run_id}: Failed to parse team response for team={team_id}: {e}")
                                return None
                        else:
                            logger.warning(f"Run {run_id}: Failed to fetch team data for team={team_id}: {resp.status_code}")
                            return None
                    except Exception as e:
                        logger.error(f"Run {run_id}: Error fetching team data for team={team_id}: {e}")
                        return None
                
                # Process teams in smaller batches to avoid overwhelming the API
                batch_size = 10
                for i in range(0, len(team_ids_to_process), batch_size):
                    batch = team_ids_to_process[i:i + batch_size]
                    team_tasks = [fetch_team_data(team_id) for team_id in batch]
                    team_results = await asyncio.gather(*team_tasks, return_exceptions=True)
                    
                    for result in team_results:
                        if result and not isinstance(result, Exception):
                            teams_data.append(result)
                        elif isinstance(result, Exception):
                            logger.error(f"Run {run_id}: Team fetch task failed: {result}")
                    
                    # Small delay between batches
                    if i + batch_size < len(team_ids_to_process):
                        await asyncio.sleep(0.5)
                
                logger.info(f"Run {run_id}: PRE CACHE GETTER SECONDARY found {len(teams_data)} teams data")
            else:
                logger.info(f"Run {run_id}: No cached team IDs found, skipping team data fetch")
        except Exception as e:
            logger.error(f"Run {run_id}: Error fetching team data: {e}")
        finally:
            if redis_client:
                await redis_client.aclose()
        
        # Return all fetched data
        result = {
            "run_id": run_id,
            "timestamp": pendulum.now().isoformat(),
            "api_calls": api_calls,
            "data": {
                "teams": teams_data
            }
        }
        
        logger.info(f"Run {run_id}: PRE CACHE GETTER SECONDARY completed successfully - fetched {len(teams_data)} teams (clubs moved to tertiary)")
        
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
            
            calls_filepath = os.path.join(log_dir, f"precache_secondary_calls_{run_id}.json")
            with open(calls_filepath, 'w', encoding='utf-8') as f:
                json.dump(calls_log, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"Run {run_id}: Saved {len(api_urls_called)} SECONDARY API calls to {calls_filepath}")
        except Exception as e:
            logger.warning(f"Failed to save SECONDARY API calls log: {e}")
        
        return result
        
    except Exception as e:
        logger.error(f"Run {run_id}: PRE CACHE GETTER SECONDARY failed: {e}")
        logger.exception(e)
        raise
        
    finally:
        if client:
            await client.aclose()


async def pre_cache_getter_tertiary(token_manager, run_id=None):
    """TERTIARY RUN PRE CACHE GETTER - Fetches venues, clubs and other long-term data.
    
    This runs every 15 hours (300 runs) and handles longer execution time.
    Gets venues, clubs and other less frequently changing data from DATA.NIF.NO.
    
    Args:
        token_manager: Token manager for API authentication
        run_id: Optional run ID for tracking API calls
    
    Returns:
        dict: Contains fresh venues, clubs and other long-term data from API
    """
    if not run_id:
        run_id = str(uuid.uuid4())[:8]
    
    logger.info(f"Run {run_id}: PRE CACHE GETTER TERTIARY starting - fetching venues, clubs and long-term data from API")
    
    client = None
    api_calls = {"venues": 0, "venue_units": 0, "clubs": 0}
    api_urls_called = []
    
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
        # Fetch all venues data
        venues_data = []
        
        logger.info(f"Run {run_id}: PRE CACHE GETTER TERTIARY fetching all venues")
        
        try:
            url = f"{config.API_URL}/api/v1/venues/allvenues"
            
            resp = await client.get(url, headers=headers)
            _track_api_call(url, "GET", None, status=resp.status_code)
            api_calls["venues"] += 1
            
            if resp.status_code < 400:
                try:
                    raw = resp.json()
                    venues_data = raw.get("venues", [])
                    
                    if isinstance(venues_data, dict):
                        venues_data = [venues_data]
                    elif not isinstance(venues_data, list):
                        logger.warning(f"Run {run_id}: Unexpected venues data type: {type(venues_data).__name__}")
                        venues_data = []
                        
                except Exception as e:
                    logger.warning(f"Run {run_id}: Failed to parse venues response: {e}")
                    venues_data = []
            else:
                logger.warning(f"Run {run_id}: Failed to fetch venues: {resp.status_code}")
                
        except Exception as e:
            logger.error(f"Run {run_id}: Error fetching venues: {e}")
        
        logger.info(f"Run {run_id}: PRE CACHE GETTER TERTIARY found {len(venues_data)} venues")
        
        # -----------------------------------------------------------------
        # Fetch clubs data for relevant sports
        sport_ids = [72, 151]  # 72=Innebandy, 151=Bandy
        clubs_data = []
        
        logger.info(f"Run {run_id}: PRE CACHE GETTER TERTIARY fetching clubs for sports {sport_ids}")
        
        async def fetch_clubs_for_sport(sport_id):
            try:
                url = f"{config.API_URL}/api/v1/org/clubsbysport/{sport_id}/"
                
                resp = await client.get(url, headers=headers)
                _track_api_call(url, "GET", None, status=resp.status_code)
                api_calls["clubs"] += 1
                
                if resp.status_code < 400:
                    try:
                        raw = resp.json()
                        
                        # Handle different response formats
                        if isinstance(raw, dict):
                            data = raw.get("clubs", [])
                        elif isinstance(raw, list):
                            # Direct list response
                            data = raw
                        else:
                            logger.warning(f"Run {run_id}: Unexpected raw response type for sport={sport_id}: {type(raw).__name__}")
                            return []
                        
                        # Ensure data is a list
                        if isinstance(data, dict):
                            return [data]
                        elif isinstance(data, list):
                            return data
                        else:
                            logger.warning(f"Run {run_id}: Unexpected clubs data type for sport={sport_id}: {type(data).__name__}")
                            return []
                    except Exception as e:
                        logger.warning(f"Run {run_id}: Failed to parse clubs response for sport={sport_id}: {e}")
                        return []
                else:
                    logger.warning(f"Run {run_id}: Failed to fetch clubs for sport={sport_id}: {resp.status_code}")
                    return []
            except Exception as e:
                logger.error(f"Run {run_id}: Error fetching clubs for sport={sport_id}: {e}")
                return []
        
        # Fetch clubs for all sports in parallel
        clubs_tasks = [fetch_clubs_for_sport(sport_id) for sport_id in sport_ids]
        clubs_results = await asyncio.gather(*clubs_tasks, return_exceptions=True)
        
        for result in clubs_results:
            if isinstance(result, list):
                clubs_data.extend(result)
            elif isinstance(result, Exception):
                logger.error(f"Run {run_id}: Clubs fetch task failed: {result}")
        
        logger.info(f"Run {run_id}: PRE CACHE GETTER TERTIARY found {len(clubs_data)} clubs")
        
        # -----------------------------------------------------------------
        # Fetch detailed venue data for specific venues (if needed)
        venue_details = []
        
        if venues_data:
            # Limit venue details to process to avoid very long execution
            max_venues = 50  # Limit for tertiary run
            venues_to_process = venues_data[:max_venues]
            
            logger.info(f"Run {run_id}: PRE CACHE GETTER TERTIARY fetching details for {len(venues_to_process)} venues")
            
            async def fetch_venue_detail(venue):
                try:
                    venue_id = venue.get("venueId")
                    if not venue_id:
                        return None
                        
                    url = f"{config.API_URL}/api/v1/ta/venue/"
                    params = {"venueId": venue_id}
                    
                    resp = await client.get(url, headers=headers, params=params)
                    _track_api_call(url, "GET", params, status=resp.status_code)
                    api_calls["venue_units"] += 1
                    
                    if resp.status_code < 400:
                        try:
                            raw = resp.json()
                            return raw
                        except Exception as e:
                            logger.warning(f"Run {run_id}: Failed to parse venue detail response for venue={venue_id}: {e}")
                            return None
                    else:
                        logger.warning(f"Run {run_id}: Failed to fetch venue detail for venue={venue_id}: {resp.status_code}")
                        return None
                except Exception as e:
                    logger.error(f"Run {run_id}: Error fetching venue detail for venue={venue.get('venueId')}: {e}")
                    return None
            
            # Process venues in batches to avoid overwhelming the API
            batch_size = 5
            for i in range(0, len(venues_to_process), batch_size):
                batch = venues_to_process[i:i + batch_size]
                venue_tasks = [fetch_venue_detail(venue) for venue in batch]
                venue_results = await asyncio.gather(*venue_tasks, return_exceptions=True)
                
                for result in venue_results:
                    if result and not isinstance(result, Exception):
                        venue_details.append(result)
                    elif isinstance(result, Exception):
                        logger.error(f"Run {run_id}: Venue detail fetch task failed: {result}")
                
                # Delay between batches for tertiary run
                if i + batch_size < len(venues_to_process):
                    await asyncio.sleep(2.0)  # Longer delay for tertiary
            
            logger.info(f"Run {run_id}: PRE CACHE GETTER TERTIARY found {len(venue_details)} venue details")
        
        # Return all fetched data
        result = {
            "run_id": run_id,
            "timestamp": pendulum.now().isoformat(),
            "api_calls": api_calls,
            "data": {
                "venues": venues_data,
                "venue_details": venue_details,
                "clubs": clubs_data
            }
        }
        
        logger.info(f"Run {run_id}: PRE CACHE GETTER TERTIARY completed successfully - fetched {len(venues_data)} venues, {len(venue_details)} venue details, {len(clubs_data)} clubs")
        
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
            
            calls_filepath = os.path.join(log_dir, f"precache_tertiary_calls_{run_id}.json")
            with open(calls_filepath, 'w', encoding='utf-8') as f:
                json.dump(calls_log, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"Run {run_id}: Saved {len(api_urls_called)} TERTIARY API calls to {calls_filepath}")
        except Exception as e:
            logger.warning(f"Failed to save TERTIARY API calls log: {e}")
        
        return result
        
    except Exception as e:
        logger.error(f"Run {run_id}: PRE CACHE GETTER TERTIARY failed: {e}")
        logger.exception(e)
        raise
        
    finally:
        if client:
            await client.aclose()


# Keep the original function as an alias to primary for backward compatibility
async def pre_cache_getter(token_manager, run_id=None):
    """Legacy function - redirects to pre_cache_getter_primary for backward compatibility."""
    return await pre_cache_getter_primary(token_manager, run_id)


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
    changed_season_ids = set()
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
        
        # what to do with         # 
        # api/v1/org/clubsbysport/72/ 
        # api/v1/org/clubsbysport/151/
        # venues

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
        cached_root_tournaments = await _get_cached("root_tournaments")
        cached_tournament_matches = await _get_cached("tournament_matches")
        cached_team_ids = await _get_cached("unique_team_ids") or []
        
        # Log cached data to debug files
        _log_cache_data_to_file("valid_seasons", cached_valid_seasons, run_id)
        _log_cache_data_to_file("tournaments_in_season", cached_tournaments, run_id)
        _log_cache_data_to_file("root_tournaments", cached_root_tournaments, run_id)
        _log_cache_data_to_file("tournament_matches", cached_tournament_matches, run_id)
        _log_cache_data_to_file("unique_team_ids", cached_team_ids, run_id)
        
        logger.info(f"Run {run_id}: Loaded cached data - {len(cached_valid_seasons or [])} seasons, {len(cached_tournaments or [])} tournaments, {len(cached_root_tournaments or [])} root tournaments, {len(cached_tournament_matches or [])} matches, {len(cached_team_ids)} team IDs")
        
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
            
            # Calculate which seasons changed
            cached_season_ids = {s["seasonId"] for s in (cached_valid_seasons or []) if "seasonId" in s}
            new_season_ids = {s["seasonId"] for s in valid_seasons if "seasonId" in s}
            changed_season_ids = new_season_ids.symmetric_difference(cached_season_ids)
            
            _record_changes("valid_seasons", len(changed_season_ids))
            logger.info(f"Valid seasons cache updated with {len(valid_seasons)} seasons (was {len(cached_valid_seasons or [])} seasons)")
            logger.info(f"Season changes detected: {len(changed_season_ids)} seasons changed")
            
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
            changed_season_ids = set()  # Ensure changed_season_ids is empty when no changes
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
        # Compare and update root tournaments
        root_tournaments_changed = json.dumps(root_tournaments, sort_keys=True) != json.dumps(cached_root_tournaments or [], sort_keys=True)
        
        logger.info(f"Run {run_id}: Data comparison - fetched {len(root_tournaments)} root tournaments, cached {len(cached_root_tournaments or [])} root tournaments")
        logger.info(f"Run {run_id}: Root tournaments changed: {root_tournaments_changed}")
        
        changed_root_tournament_ids = set()
        if root_tournaments_changed:
            await redis_client.set(
                "root_tournaments",
                json.dumps(
                    {"data": root_tournaments, "last_updated": start_time.isoformat()},
                    ensure_ascii=False,
                ),
            )
            
            # Calculate which root tournaments changed
            cached_root_ids = {t["tournamentId"] for t in (cached_root_tournaments or []) if "tournamentId" in t}
            new_root_ids = {t["tournamentId"] for t in root_tournaments if "tournamentId" in t}
            changed_root_tournament_ids = new_root_ids.symmetric_difference(cached_root_ids)
            
            _record_changes("root_tournaments", len(changed_root_tournament_ids))
            logger.info(f"Root tournament changes detected: {len(changed_root_tournament_ids)} root tournaments changed")
            logger.info(f"Updated root_tournaments cache with {len(root_tournaments)} root tournaments")
        else:
            logger.debug(f"No root tournament changes detected - cache has {len(cached_root_tournaments or [])} root tournaments, fetched {len(root_tournaments)}")
        
        if changed_root_tournament_ids:
            logger.info(f"Run {run_id}: Detected changes in {len(changed_root_tournament_ids)} root_tournaments")
        
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
        all_categories = ["valid_seasons", "tournaments_in_season", "root_tournaments", "tournament_matches", "individual_matches", "unique_team_ids"]
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
            "changed_season_ids": changed_season_ids,
            "changed_tournament_ids": changed_tournament_ids,
            "changed_root_tournament_ids": changed_root_tournament_ids,
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
            successful_endpoints = 0
            total_endpoints = len(match_endpoint_configs)
            
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
                    
                    endpoint_success = False
                    if refresh_until is not False:
                        # Use setup_refresh for auto-refresh functionality
                        await cache_manager.setup_refresh(
                            cache_key, target_url, ttl, refresh_until, params=params
                        )
                        logger.debug(f"Set up auto-refresh cache for match {match_id}: {cache_key}")
                        endpoint_success = True
                    else:
                        # Fetch and cache data directly without auto-refresh
                        success = await cache_manager.fetch_and_cache(cache_key, target_url, ttl, params)
                        if success:
                            logger.debug(f"Created TTL-only cache entry for match {match_id}: {cache_key}")
                            endpoint_success = True
                        else:
                            logger.warning(f"Failed to create cache entry for match {match_id}: {cache_key}")
                    
                    if endpoint_success:
                        successful_endpoints += 1
                    
                    # Small delay between cache setups to prevent overwhelming Redis
                    await asyncio.sleep(CACHE_SETUP_DELAY)
                    
                except Exception as e:
                    logger.error(f"Error setting up cache for match {match_id} endpoint {endpoint_config['path']}: {e}")
            
            # Return True if all endpoints succeeded
            return successful_endpoints == total_endpoints
    
    # Process all matches in this batch concurrently but with limited concurrency
    tasks = [setup_match_cache(match_id) for match_id in match_batch]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Check results and log any failures
    successful_count = 0
    failed_count = 0
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            failed_count += 1
            logger.error(f"Run {run_id}: Match cache setup failed for match {match_batch[i]} with exception: {result}")
        elif result is False:
            failed_count += 1
            logger.warning(f"Run {run_id}: Match cache setup failed for match {match_batch[i]} (some endpoints failed)")
        else:
            successful_count += 1
    
    logger.info(f"Run {run_id}: Match cache batch completed - {successful_count} successful, {failed_count} failed out of {len(match_batch)} matches")


async def process_root_tournament_cache_batch(root_tournament_batch, cache_manager, ttl, refresh_until, run_id):
    """Process a batch of root tournaments for cache warming with rate limiting"""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_CACHE_SETUPS)
    
    async def setup_root_tournament_cache(root_tournament_id):
        async with semaphore:
            try:
                # Construct URL and cache key for root tournament hierarchy endpoint
                path = f"api/v1/ta/tournament/season/{root_tournament_id}/"
                target_url = f"{config.API_URL}/{path}"
                cache_key = f"GET:{path}?hierarchy=true"
                
                success = False
                if refresh_until is not False:
                    # Use setup_refresh for auto-refresh functionality
                    await cache_manager.setup_refresh(
                        cache_key, target_url, ttl, refresh_until, params={"hierarchy": "true"}
                    )
                    logger.debug(f"Set up auto-refresh cache for root tournament {root_tournament_id}: {cache_key}")
                    success = True
                else:
                    # Fetch and cache data directly without auto-refresh
                    success = await cache_manager.fetch_and_cache(cache_key, target_url, ttl, {"hierarchy": "true"})
                    if success:
                        logger.debug(f"Created TTL-only cache entry for root tournament {root_tournament_id}: {cache_key}")
                    else:
                        logger.warning(f"Failed to create cache entry for root tournament {root_tournament_id}: {cache_key}")
                
                # Small delay between cache setups to prevent overwhelming Redis
                await asyncio.sleep(CACHE_SETUP_DELAY)
                return success
                
            except Exception as e:
                logger.error(f"Error setting up cache for root tournament {root_tournament_id}: {e}")
                return False
    
    # Process all root tournaments in this batch concurrently but with limited concurrency
    tasks = [setup_root_tournament_cache(root_tournament_id) for root_tournament_id in root_tournament_batch]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Check results and log any failures
    successful_count = 0
    failed_count = 0
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            failed_count += 1
            logger.error(f"Run {run_id}: Root tournament cache setup failed for tournament {root_tournament_batch[i]} with exception: {result}")
        elif result is False:
            failed_count += 1
            logger.warning(f"Run {run_id}: Root tournament cache setup failed for tournament {root_tournament_batch[i]} (returned False)")
        else:
            successful_count += 1
    
    logger.info(f"Run {run_id}: Root tournament cache batch completed - {successful_count} successful, {failed_count} failed out of {len(root_tournament_batch)} root tournaments")


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
                
                success = False
                if refresh_until is not False:
                    # Use setup_refresh for auto-refresh functionality
                    await cache_manager.setup_refresh(
                        cache_key, target_url, ttl, refresh_until, params={"orgId": team_id}
                    )
                    logger.debug(f"Set up auto-refresh cache for team {team_id} with cache_key: {cache_key}")
                    success = True
                else:
                    # Fetch and cache data directly without auto-refresh
                    success = await cache_manager.fetch_and_cache(cache_key, target_url, ttl, {"orgId": team_id})
                    if success:
                        logger.debug(f"[DEBUG] - Successfully cached data for team {team_id}: {cache_key}")
                    else:
                        logger.warning(f"Failed to create cache entry for team {team_id}: {cache_key}")
                
                # Small delay between cache setups to prevent overwhelming Redis
                await asyncio.sleep(CACHE_SETUP_DELAY)
                return success
                
            except Exception as e:
                logger.error(f"Error setting up cache for team {team_id}: {e}")
                return False
    
    # Process all teams in this batch concurrently but with limited concurrency
    tasks = [setup_team_cache(team_id) for team_id in team_batch]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Check results and log any failures
    successful_count = 0
    failed_count = 0
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            failed_count += 1
            logger.error(f"Run {run_id}: Team cache setup failed for team {team_batch[i]} with exception: {result}")
        elif result is False:
            failed_count += 1
            logger.warning(f"Run {run_id}: Team cache setup failed for team {team_batch[i]} (returned False)")
        else:
            successful_count += 1
    
    logger.info(f"Run {run_id}: Team cache batch completed - {successful_count} successful, {failed_count} failed out of {len(team_batch)} teams")


async def compare_and_update_secondary_cache(secondary_data_result, cache_manager, run_id, start_time, run_timestamp):
    """Compare secondary data (teams) with cached data and update cache if changes detected.
    
    Args:
        secondary_data_result: Result dict from pre_cache_getter_secondary containing fresh data
        cache_manager: Cache manager for setting up cache refresh
        run_id: Run ID for tracking
        start_time: Start time of the run (pendulum datetime)
        run_timestamp: ISO format timestamp string
    
    Returns:
        dict: Contains changes detected and upstream status
    """
    redis_client = None
    changes_detected = {}
    changed_team_ids = set()
    upstream_status = "UP"
    
    try:
        redis_client = get_redis_client()
        
        # Extract data from secondary_data_result
        api_calls = secondary_data_result["api_calls"]
        fresh_data = secondary_data_result["data"]
        
        teams_data = fresh_data["teams"]
        
        logger.info(f"Run {run_id}: COMPARE SECONDARY CACHE starting - comparing fresh secondary data with cached data")
        
        # Helper function to record changes
        def _record_changes(category, count):
            """Helper function to record changes in metrics"""
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
        
        # Load cached secondary data
        cached_teams = await _get_cached("teams_data") or []
        
        logger.info(f"Run {run_id}: Loaded cached secondary data - {len(cached_teams)} teams")
        
        # Compare teams data - Always perform individual team comparison to detect changes
        logger.info(f"Run {run_id}: Secondary data comparison - fetched {len(teams_data)} teams, cached {len(cached_teams)} teams")
        
        # Always compare individual teams to build changed team IDs list
        if teams_data or cached_teams:
            # Compare old vs new team data to find changes - with validation
            cached_teams_by_id = {}
            new_teams_by_id = {}
            
            # Process cached teams with validation
            for team in cached_teams:
                if isinstance(team, dict) and team.get("orgId"):
                    cached_teams_by_id[team["orgId"]] = team
                elif isinstance(team, list):
                    # Handle nested list structure in cached data
                    for nested_team in team:
                        if isinstance(nested_team, dict) and nested_team.get("orgId"):
                            cached_teams_by_id[nested_team["orgId"]] = nested_team
                else:
                    logger.warning(f"Run {run_id}: Invalid cached team format: {type(team)}")
            
            # Process new teams with validation - handle list wrapping
            for team_item in teams_data:
                if isinstance(team_item, dict) and team_item.get("orgId"):
                    # Direct team object
                    new_teams_by_id[team_item["orgId"]] = team_item
                elif isinstance(team_item, list):
                    # Team data wrapped in a list - flatten it
                    for team in team_item:
                        if isinstance(team, dict) and team.get("orgId"):
                            new_teams_by_id[team["orgId"]] = team
                        else:
                            logger.warning(f"Run {run_id}: Invalid nested team format: {type(team)}")
                else:
                    logger.warning(f"Run {run_id}: Invalid teams_data format: {type(team_item)}")
            
            logger.debug(f"Run {run_id}: Processed {len(new_teams_by_id)} valid new teams, {len(cached_teams_by_id)} valid cached teams")
            
            # Find teams that are new, modified, or removed
            for team_id, new_team in new_teams_by_id.items():
                if team_id not in cached_teams_by_id:
                    changed_team_ids.add(team_id)  # New team
                    logger.debug(f"Run {run_id}: New team detected: {team_id}")
                elif json.dumps(new_team, sort_keys=True) != json.dumps(cached_teams_by_id[team_id], sort_keys=True):
                    changed_team_ids.add(team_id)  # Modified team
                    logger.debug(f"Run {run_id}: Modified team detected: {team_id}")
            
            # Find removed teams
            for team_id in cached_teams_by_id:
                if team_id not in new_teams_by_id:
                    changed_team_ids.add(team_id)  # Removed team
                    logger.debug(f"Run {run_id}: Removed team detected: {team_id}")
        
        # Check if overall teams data changed (for cache update decision)
        teams_changed = json.dumps(teams_data, sort_keys=True) != json.dumps(cached_teams, sort_keys=True)
        logger.info(f"Run {run_id}: Teams data changed: {teams_changed}, Individual team changes: {len(changed_team_ids)} teams")
        
        # Update cache if there are changes or if cache is empty
        if teams_changed or (len(teams_data) > 0 and len(cached_teams) == 0):
            await redis_client.set(
                "teams_data",
                json.dumps(
                    {"data": teams_data, "last_updated": start_time.isoformat()},
                    ensure_ascii=False,
                ),
            )
            logger.info(f"Run {run_id}: Updated teams cache with {len(teams_data)} teams")
        
        # Record changes (always record, even if 0)
        _record_changes("teams", len(changed_team_ids))
        
        if changed_team_ids:
            logger.info(f"Run {run_id}: Detected {len(changed_team_ids)} team changes for cache warming: {list(changed_team_ids)[:10]}{'...' if len(changed_team_ids) > 10 else ''}")
        else:
            logger.info(f"Run {run_id}: No team changes detected - no cache warming needed")
        
        # Update metrics for secondary data
        PRECACHE_ITEMS_PROCESSED.labels(item_type="teams_secondary").set(len(teams_data))
        
        return {
            "changes_detected": changes_detected,
            "changed_team_ids": changed_team_ids,
            "upstream_status": upstream_status,
            "api_calls": api_calls
        }
        
    except Exception as e:
        logger.error(f"Run {run_id}: COMPARE SECONDARY CACHE failed: {e}")
        logger.exception(e)
        return {
            "changes_detected": {},
            "changed_team_ids": set(),
            "upstream_status": "DOWN",
            "api_calls": {}
        }
        
    finally:
        if redis_client:
            await redis_client.aclose()


async def compare_and_update_tertiary_cache(tertiary_data_result, cache_manager, run_id, start_time, run_timestamp):
    """Compare tertiary data (venues) with cached data and update cache if changes detected.
    
    Args:
        tertiary_data_result: Result dict from pre_cache_getter_tertiary containing fresh data
        cache_manager: Cache manager for setting up cache refresh
        run_id: Run ID for tracking
        start_time: Start time of the run (pendulum datetime)
        run_timestamp: ISO format timestamp string
    
    Returns:
        dict: Contains changes detected and upstream status
    """
    redis_client = None
    changes_detected = {}
    changed_venue_ids = set()
    changed_club_ids = set()
    upstream_status = "UP"
    
    try:
        redis_client = get_redis_client()
        
        # Extract data from tertiary_data_result
        api_calls = tertiary_data_result["api_calls"]
        fresh_data = tertiary_data_result["data"]
        
        venues_data = fresh_data["venues"]
        venue_details = fresh_data["venue_details"]
        clubs_data = fresh_data["clubs"]
        
        logger.info(f"Run {run_id}: COMPARE TERTIARY CACHE starting - comparing fresh tertiary data with cached data")
        
        # Helper function to record changes
        def _record_changes(category, count):
            """Helper function to record changes in metrics"""
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
        
        # Load cached tertiary data
        cached_venues = await _get_cached("venues_data") or []
        cached_venue_details = await _get_cached("venue_details") or []
        cached_clubs = await _get_cached("clubs_data") or []
        
        logger.info(f"Run {run_id}: Loaded cached tertiary data - {len(cached_venues)} venues, {len(cached_venue_details)} venue details, {len(cached_clubs)} clubs")
        
        # Compare venues data
        venues_changed = json.dumps(venues_data, sort_keys=True) != json.dumps(cached_venues, sort_keys=True)
        
        logger.info(f"Run {run_id}: Tertiary data comparison - fetched {len(venues_data)} venues, cached {len(cached_venues)} venues")
        logger.info(f"Run {run_id}: Venues data changed: {venues_changed}")
        
        if venues_changed or (len(venues_data) > 0 and len(cached_venues) == 0):
            await redis_client.set(
                "venues_data",
                json.dumps(
                    {"data": venues_data, "last_updated": start_time.isoformat()},
                    ensure_ascii=False,
                ),
            )
            
            # Extract venue IDs that changed for cache warming
            if venues_data:
                for venue in venues_data:
                    if isinstance(venue, dict) and venue.get("venueId"):
                        changed_venue_ids.add(venue["venueId"])
            
            _record_changes("venues", len(changed_venue_ids))
            logger.info(f"Run {run_id}: Updated venues cache with {len(venues_data)} venues")
        
        # Compare venue details data
        venue_details_changed = json.dumps(venue_details, sort_keys=True) != json.dumps(cached_venue_details, sort_keys=True)
        
        logger.info(f"Run {run_id}: Tertiary data comparison - fetched {len(venue_details)} venue details, cached {len(cached_venue_details)} venue details")
        logger.info(f"Run {run_id}: Venue details changed: {venue_details_changed}")
        
        if venue_details_changed or (len(venue_details) > 0 and len(cached_venue_details) == 0):
            await redis_client.set(
                "venue_details",
                json.dumps(
                    {"data": venue_details, "last_updated": start_time.isoformat()},
                    ensure_ascii=False,
                ),
            )
            
            _record_changes("venue_details", len(venue_details))
            logger.info(f"Run {run_id}: Updated venue details cache with {len(venue_details)} venue details")
        
        # Compare clubs data
        clubs_changed = json.dumps(clubs_data, sort_keys=True) != json.dumps(cached_clubs, sort_keys=True)
        
        logger.info(f"Run {run_id}: Tertiary data comparison - fetched {len(clubs_data)} clubs, cached {len(cached_clubs)} clubs")
        logger.info(f"Run {run_id}: Clubs data changed: {clubs_changed}")
        
        if clubs_changed or (len(clubs_data) > 0 and len(cached_clubs) == 0):
            await redis_client.set(
                "clubs_data",
                json.dumps(
                    {"data": clubs_data, "last_updated": start_time.isoformat()},
                    ensure_ascii=False,
                ),
            )
            
            # Extract club IDs that changed for cache warming
            if clubs_data:
                for club in clubs_data:
                    if isinstance(club, dict) and club.get("clubId"):
                        changed_club_ids.add(club["clubId"])
            
            _record_changes("clubs", len(changed_club_ids))
            logger.info(f"Run {run_id}: Updated clubs cache with {len(clubs_data)} clubs")
        
        # Update metrics for tertiary data
        PRECACHE_ITEMS_PROCESSED.labels(item_type="venues").set(len(venues_data))
        PRECACHE_ITEMS_PROCESSED.labels(item_type="venue_details").set(len(venue_details))
        PRECACHE_ITEMS_PROCESSED.labels(item_type="clubs").set(len(clubs_data))
        
        return {
            "changes_detected": changes_detected,
            "changed_venue_ids": changed_venue_ids,
            "changed_club_ids": changed_club_ids,
            "upstream_status": upstream_status,
            "api_calls": api_calls
        }
        
    except Exception as e:
        logger.error(f"Run {run_id}: COMPARE TERTIARY CACHE failed: {e}")
        logger.exception(e)
        return {
            "changes_detected": {},
            "changed_venue_ids": set(),
            "changed_club_ids": set(),
            "upstream_status": "DOWN",
            "api_calls": {}
        }
        
    finally:
        if redis_client:
            await redis_client.aclose()


async def precache_orchestrator(cache_manager, token_manager):
    """Main orchestrator for the three-tier precache system.

    This function coordinates PRIMARY, SECONDARY, and TERTIARY precache runs,
    ensuring proper execution order and data dependencies. PRIMARY runs every
    3 minutes, SECONDARY every 9 minutes, and TERTIARY every 15 hours.
    
    PRIMARY: Basic tournament and match data (seasons, tournaments, matches)
    SECONDARY: Team data that depends on PRIMARY results
    TERTIARY: Venue and club data that may use PRIMARY cached data
    """
    
    global CACHE_WARMING_IN_PROGRESS, FULL_RUN_NEEDED
    
    try:
        # Log function entry
        logger.info(f"precache_orchestrator function called with cache_manager={type(cache_manager).__name__}, token_manager={type(token_manager).__name__}")
    except Exception as e:
        logger.error(f"Error logging function entry: {e}")
    
    loop_iteration = 0
    secondary_run_counter = 0  # Track runs for SECONDARY execution (every 3 runs)
    tertiary_run_counter = 0  # Track runs for TERTIARY execution (every 300 runs)

    while True:
        try:
            loop_iteration += 1
            
            # Check if a full run is needed (after clear/delete operations)
            if FULL_RUN_NEEDED:
                # Force all types to run on this iteration
                run_primary = True
                run_secondary = True
                run_tertiary = True
                FULL_RUN_NEEDED = False  # Reset the flag
                PRECACHE_FULL_RUNS_TOTAL.inc()  # Track full runs metric
                logger.info(f"Precache iteration {loop_iteration}: FULL RUN triggered after clear/delete - running PRIMARY, SECONDARY, and TERTIARY")
            else:
                # Normal run determination logic
                # PRIMARY runs every iteration (every 3 minutes)
                # SECONDARY runs every 3 iterations (every 9 minutes)
                # TERTIARY runs every 300 iterations (every 15 hours)
                
                run_primary = True  # Always run primary
                run_secondary = (loop_iteration % 3 == 0)  # Every 3rd iteration
                run_tertiary = (loop_iteration % 300 == 0)  # Every 300th iteration
            
            run_types = []
            if run_primary:
                run_types.append("PRIMARY")
                PRECACHE_RUN_TYPE_COUNTER.labels(run_type="primary").inc()
            if run_secondary:
                run_types.append("SECONDARY")
                secondary_run_counter += 1
                PRECACHE_RUN_TYPE_COUNTER.labels(run_type="secondary").inc()
            if run_tertiary:
                run_types.append("TERTIARY")
                tertiary_run_counter += 1
                PRECACHE_RUN_TYPE_COUNTER.labels(run_type="tertiary").inc()
            
            # Update counters in metrics
            PRECACHE_SECONDARY_COUNTER.set(secondary_run_counter)
            PRECACHE_TERTIARY_COUNTER.set(tertiary_run_counter)
            
            logger.info(f"Precache iteration {loop_iteration}: Running {', '.join(run_types)} (Secondary: {secondary_run_counter}, Tertiary: {tertiary_run_counter})")
            
            # Check if cache warming is in progress
            if CACHE_WARMING_IN_PROGRESS:
                logger.info(f"Precache iteration {loop_iteration} skipped - cache warming in progress")
                await asyncio.sleep(30)  # Wait 30 seconds before checking again
                continue
                
            logger.info(f"Starting precache loop iteration {loop_iteration}")
            
            start_time = pendulum.now()
            run_id = str(uuid.uuid4())[:8]  # Short unique ID for this run
            run_timestamp = start_time.isoformat()
            api_calls = {"seasons": 0, "tournament_season": 0, "tournament_matches": 0, "clubs": 0, "teams": 0, "venues": 0}
            run_timings = {}  # Track timing for each run type
            run_api_calls = {}  # Track API calls for each run type
            changes_detected = {}
            changed_season_ids = set()
            changed_tournament_ids = set()
            changed_root_tournament_ids = set()
            changed_team_ids = []
            changed_match_ids = set()
            upstream_status = "UP"

            logger.info(f"Starting precache run {run_id} at {run_timestamp} (iteration {loop_iteration}) - Types: {', '.join(run_types)}")
            
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
                # Step 1: Fetch fresh data from upstream API based on run type
                fresh_data_results = {}
                
                # Always run PRIMARY first
                if run_primary:
                    logger.info(f"Run {run_id}: Step 1a - Fetching PRIMARY data (basic data) from upstream API")
                    primary_start_time = pendulum.now()
                    try:
                        with PRECACHE_RUN_TYPE_DURATION.labels(run_type="primary").time():
                            primary_result = await pre_cache_getter_primary(token_manager, run_id)
                            fresh_data_results["primary"] = primary_result
                            # Track API calls for primary
                            primary_api_calls = primary_result["api_calls"].copy()
                            run_api_calls["primary"] = primary_api_calls
                            # Update global api_calls with primary data
                            for key, value in primary_result["api_calls"].items():
                                if key in api_calls:
                                    api_calls[key] += value
                                else:
                                    api_calls[key] = value
                        primary_end_time = pendulum.now()
                        primary_duration = (primary_end_time - primary_start_time).total_seconds()
                        run_timings["primary"] = {
                            "start_time": primary_start_time.isoformat(),
                            "end_time": primary_end_time.isoformat(),
                            "duration_seconds": primary_duration
                        }
                        PRECACHE_RUN_TYPE_LAST_SUCCESS.labels(run_type="primary").set(start_time.timestamp())
                        logger.info(f"Run {run_id}: PRIMARY completed in {primary_duration:.2f}s with {sum(primary_api_calls.values())} API calls")
                        
                        # Immediately compare and update PRIMARY cache to make data available for SECONDARY/TERTIARY
                        logger.info(f"Run {run_id}: Step 2a - Comparing PRIMARY data and updating cache")
                        
                        primary_comparison_result = await compare_and_update_cache(
                            primary_result, 
                            cache_manager, 
                            run_id, 
                            start_time, 
                            run_timestamp
                        )
                        
                        # Store PRIMARY results for later aggregation
                        changes_detected = primary_comparison_result["changes_detected"]
                        changed_season_ids = primary_comparison_result["changed_season_ids"]
                        changed_tournament_ids = primary_comparison_result["changed_tournament_ids"]
                        changed_root_tournament_ids = primary_comparison_result["changed_root_tournament_ids"]
                        changed_team_ids = primary_comparison_result["changed_team_ids"]
                        changed_match_ids = primary_comparison_result["changed_match_ids"]
                        upstream_status = primary_comparison_result["upstream_status"]
                        
                        logger.info(f"Run {run_id}: PRIMARY cache updated - data now available for SECONDARY/TERTIARY")
                        
                    except Exception as e:
                        primary_end_time = pendulum.now()
                        primary_duration = (primary_end_time - primary_start_time).total_seconds()
                        run_timings["primary"] = {
                            "start_time": primary_start_time.isoformat(),
                            "end_time": primary_end_time.isoformat(),
                            "duration_seconds": primary_duration,
                            "error": str(e)
                        }
                        run_api_calls["primary"] = {}
                        logger.error(f"Run {run_id}: PRIMARY failed after {primary_duration:.2f}s: {e}")
                        # Set upstream status to DOWN and continue to next iteration
                        PRECACHE_UPSTREAM_STATUS.labels(endpoint="data.nif.no").set(0)
                        raise
                
                # Run SECONDARY only after PRIMARY completes successfully and cache is updated
                if run_secondary and "primary" in fresh_data_results:
                    logger.info(f"Run {run_id}: Step 1b - Fetching SECONDARY data (clubs, teams) from upstream API after PRIMARY cache update")
                    secondary_start_time = pendulum.now()
                    try:
                        with PRECACHE_RUN_TYPE_DURATION.labels(run_type="secondary").time():
                            secondary_result = await pre_cache_getter_secondary(token_manager, run_id)
                            fresh_data_results["secondary"] = secondary_result
                            # Track API calls for secondary
                            secondary_api_calls = secondary_result["api_calls"].copy()
                            run_api_calls["secondary"] = secondary_api_calls
                            # Update global api_calls with secondary data
                            for key, value in secondary_result["api_calls"].items():
                                if key in api_calls:
                                    api_calls[key] += value
                                else:
                                    api_calls[key] = value
                        secondary_end_time = pendulum.now()
                        secondary_duration = (secondary_end_time - secondary_start_time).total_seconds()
                        run_timings["secondary"] = {
                            "start_time": secondary_start_time.isoformat(),
                            "end_time": secondary_end_time.isoformat(),
                            "duration_seconds": secondary_duration
                        }
                        PRECACHE_RUN_TYPE_LAST_SUCCESS.labels(run_type="secondary").set(start_time.timestamp())
                        logger.info(f"Run {run_id}: SECONDARY completed in {secondary_duration:.2f}s with {sum(secondary_api_calls.values())} API calls")
                    except Exception as e:
                        secondary_end_time = pendulum.now()
                        secondary_duration = (secondary_end_time - secondary_start_time).total_seconds()
                        run_timings["secondary"] = {
                            "start_time": secondary_start_time.isoformat(),
                            "end_time": secondary_end_time.isoformat(),
                            "duration_seconds": secondary_duration,
                            "error": str(e)
                        }
                        run_api_calls["secondary"] = {}
                        logger.error(f"Run {run_id}: SECONDARY failed after {secondary_duration:.2f}s: {e}")
                        # Continue with primary data even if secondary fails
                
                # Run TERTIARY only after PRIMARY completes successfully and cache is updated
                if run_tertiary and "primary" in fresh_data_results:
                    logger.info(f"Run {run_id}: Step 1c - Fetching TERTIARY data (venues, long-term) from upstream API after PRIMARY cache update")
                    tertiary_start_time = pendulum.now()
                    try:
                        with PRECACHE_RUN_TYPE_DURATION.labels(run_type="tertiary").time():
                            tertiary_result = await pre_cache_getter_tertiary(token_manager, run_id)
                            fresh_data_results["tertiary"] = tertiary_result
                            # Track API calls for tertiary
                            tertiary_api_calls = tertiary_result["api_calls"].copy()
                            run_api_calls["tertiary"] = tertiary_api_calls
                            # Update global api_calls with tertiary data
                            for key, value in tertiary_result["api_calls"].items():
                                if key in api_calls:
                                    api_calls[key] += value
                                else:
                                    api_calls[key] = value
                        tertiary_end_time = pendulum.now()
                        tertiary_duration = (tertiary_end_time - tertiary_start_time).total_seconds()
                        run_timings["tertiary"] = {
                            "start_time": tertiary_start_time.isoformat(),
                            "end_time": tertiary_end_time.isoformat(),
                            "duration_seconds": tertiary_duration
                        }
                        PRECACHE_RUN_TYPE_LAST_SUCCESS.labels(run_type="tertiary").set(start_time.timestamp())
                        logger.info(f"Run {run_id}: TERTIARY completed in {tertiary_duration:.2f}s with {sum(tertiary_api_calls.values())} API calls")
                    except Exception as e:
                        tertiary_end_time = pendulum.now()
                        tertiary_duration = (tertiary_end_time - tertiary_start_time).total_seconds()
                        run_timings["tertiary"] = {
                            "start_time": tertiary_start_time.isoformat(),
                            "end_time": tertiary_end_time.isoformat(),
                            "duration_seconds": tertiary_duration,
                            "error": str(e)
                        }
                        run_api_calls["tertiary"] = {}
                        logger.error(f"Run {run_id}: TERTIARY failed after {tertiary_duration:.2f}s: {e}")
                        # Continue with primary/secondary data even if tertiary fails
                
                # Ensure PRIMARY was processed successfully
                if "primary" not in fresh_data_results:
                    raise Exception("PRIMARY data fetch failed - cannot proceed")
                
                # =================================================================
                # STEP 2: PROCESS SECONDARY DATA (Teams )
                # =================================================================
                secondary_changed_teams = set()
                if run_secondary and "secondary" in fresh_data_results:
                    logger.info(f"Run {run_id}: Step 2 - Processing SECONDARY data (teams)")
                    try:
                        secondary_comparison = await compare_and_update_secondary_cache(
                            fresh_data_results["secondary"], 
                            cache_manager, 
                            run_id, 
                            start_time, 
                            run_timestamp
                        )
                        
                        # Merge secondary changes into main changes_detected
                        secondary_changes = secondary_comparison["changes_detected"]
                        for key, value in secondary_changes.items():
                            changes_detected[f"secondary_{key}"] = value
                        
                        secondary_changed_teams = secondary_comparison["changed_team_ids"]
                        
                        logger.info(f"Run {run_id}: SECONDARY processing complete - {len(secondary_changed_teams)} changed teams detected")
                        
                        # Handle SECONDARY cache warming (teams) immediately
                        if secondary_changed_teams:
                            logger.info(f"Run {run_id}: SECONDARY cache warming - Processing {len(secondary_changed_teams)} changed teams")
                            
                            # Process teams in batches to prevent overwhelming the system
                            team_ids_list = list(secondary_changed_teams)
                            
                            # Define ttl and refresh_until if not already defined
                            if 'ttl' not in locals():
                                ttl = 3600  # 1 hour TTL for team data
                            if 'refresh_until' not in locals():
                                refresh_until = False  # No auto-refresh for SECONDARY
                            
                            for batch_start in range(0, len(team_ids_list), BATCH_SIZE):
                                batch_end = min(batch_start + BATCH_SIZE, len(team_ids_list))
                                team_batch = team_ids_list[batch_start:batch_end]
                                
                                logger.info(f"Run {run_id}: SECONDARY - Processing team batch {batch_start//BATCH_SIZE + 1}/{(len(team_ids_list) + BATCH_SIZE - 1)//BATCH_SIZE} ({len(team_batch)} teams)")
                                
                                await process_team_cache_batch(team_batch, cache_manager, ttl, refresh_until, run_id)
                                
                                # Delay between batches to prevent overwhelming the system
                                if batch_end < len(team_ids_list):
                                    logger.debug(f"Run {run_id}: SECONDARY - Waiting {BATCH_DELAY}s before next batch")
                                    await asyncio.sleep(BATCH_DELAY)
                            
                            logger.info(f"Run {run_id}: SECONDARY cache warming complete")
                        else:
                            logger.info(f"Run {run_id}: SECONDARY - No cache warming needed (no team changes detected)")
                        
                    except Exception as e:
                        logger.error(f"Run {run_id}: Failed to process SECONDARY data: {e}")

                # =================================================================
                # STEP 3: PROCESS TERTIARY DATA (Venues and clubs)
                # =================================================================
                changed_venue_ids = set()
                changed_club_ids = set()
                if run_tertiary and "tertiary" in fresh_data_results:
                    logger.info(f"Run {run_id}: Step 3 - Processing TERTIARY data (venues, clubs)")
                    try:
                        tertiary_comparison = await compare_and_update_tertiary_cache(
                            fresh_data_results["tertiary"], 
                            cache_manager, 
                            run_id, 
                            start_time, 
                            run_timestamp
                        )
                        
                        # Merge tertiary changes into main changes_detected
                        tertiary_changes = tertiary_comparison["changes_detected"]
                        for key, value in tertiary_changes.items():
                            changes_detected[f"tertiary_{key}"] = value
                        
                        changed_venue_ids = tertiary_comparison["changed_venue_ids"]
                        changed_club_ids = tertiary_comparison["changed_club_ids"]
                        
                        logger.info(f"Run {run_id}: TERTIARY processing complete - {len(changed_venue_ids)} changed venues, {len(changed_club_ids)} changed clubs detected")
                        
                        # Handle TERTIARY cache warming immediately
                        if changed_venue_ids:
                            logger.info(f"Run {run_id}: TERTIARY cache warming - Processing {len(changed_venue_ids)} changed venues")
                            
                            # Warm the general venues cache
                            path = f"api/v1/venues/allvenues"
                            target_url = f"{config.API_URL}/{path}"
                            cache_key = f"GET:{path}"
                            
                            success = await cache_manager.fetch_and_cache(cache_key, target_url, ttl, {})
                            if success:
                                logger.debug(f"TERTIARY - Created cache entry for all venues: {cache_key}")
                            else:
                                logger.warning(f"TERTIARY - Failed to create cache entry for all venues: {cache_key}")
                            
                            # Warm specific venue detail caches
                            for venue_id in list(changed_venue_ids)[:20]:  # Limit to prevent overload
                                path = f"api/v1/ta/venue/"
                                target_url = f"{config.API_URL}/{path}"
                                cache_key = f"GET:{path}?venueId={venue_id}"
                                
                                success = await cache_manager.fetch_and_cache(cache_key, target_url, ttl, {"venueId": venue_id})
                                if success:
                                    logger.debug(f"TERTIARY - Created cache entry for venue {venue_id}: {cache_key}")
                                else:
                                    logger.warning(f"TERTIARY - Failed to create cache entry for venue {venue_id}: {cache_key}")
                                
                                # Small delay between venue cache setups
                                await asyncio.sleep(0.1)
                        
                        if changed_club_ids:
                            logger.info(f"Run {run_id}: TERTIARY cache warming - Processing {len(changed_club_ids)} changed clubs")
                            
                            for club_id in changed_club_ids:
                                # Cache club-specific endpoints
                                for sport_id in [72, 151]:  # 72=Innebandy, 151=Bandy
                                    path = f"api/v1/org/clubsbysport/{sport_id}/"
                                    target_url = f"{config.API_URL}/{path}"
                                    cache_key = f"GET:{path}"
                                    
                                    success = await cache_manager.fetch_and_cache(cache_key, target_url, ttl, {})
                                    if success:
                                        logger.debug(f"TERTIARY - Created cache entry for clubs sport {sport_id}: {cache_key}")
                                    else:
                                        logger.warning(f"TERTIARY - Failed to create cache entry for clubs sport {sport_id}: {cache_key}")
                        
                        if changed_venue_ids or changed_club_ids:
                            logger.info(f"Run {run_id}: TERTIARY cache warming complete")
                        
                    except Exception as e:
                        logger.error(f"Run {run_id}: Failed to process TERTIARY data: {e}")

                # if upstream_status is DOWN, do not replace data stored in any cache
                if upstream_status == "DOWN":
                    logger.error(f"Run {run_id}: Upstream status is DOWN, aborting cache update")
                    raise Exception("Upstream status is DOWN, aborting cache update")

                # =================================================================
                # STEP 4: PRIMARY CACHE WARMING (Seasons and tournaments)
                # =================================================================
                logger.info(f"Run {run_id}: Step 4 - PRIMARY cache warming for changed items")
                
                # Set cache warming flag to prevent other iterations from running
                cache_warming_needed = bool(changed_season_ids or changed_tournament_ids or changed_root_tournament_ids or changed_team_ids or changed_match_ids or changed_club_ids or changed_venue_ids)
                
                if cache_warming_needed:
                    CACHE_WARMING_IN_PROGRESS = True
                    logger.info(f"Run {run_id}: Cache warming started - future precache iterations will be suspended")
                
                try:
                    # Define cache warming parameters
                    ttl = 7 * 24 * 60 * 60  # 7 days in seconds
                    refresh_until = False  # TTL-only, no auto-refresh

                    # PRIMARY cache warming - Seasons
                    if changed_season_ids:
                        logger.info(f"Run {run_id}: PRIMARY cache warming - Processing {len(changed_season_ids)} changed seasons")
                        
                        for season_id in changed_season_ids:
                            path = f"api/v1/ta/tournament/season/{season_id}/"
                            target_url = f"{config.API_URL}/{path}"
                            cache_key = f"GET:{path}?hierarchy=true"
                            
                            success = await cache_manager.fetch_and_cache(cache_key, target_url, ttl, {"hierarchy": "true"})
                            if success:
                                logger.debug(f"PRIMARY - Created cache entry for season {season_id}: {cache_key}")
                            else:
                                logger.warning(f"PRIMARY - Failed to create cache entry for season {season_id}: {cache_key}")
                        
                        logger.info(f"Run {run_id}: PRIMARY season cache warming complete")

                    # PRIMARY cache warming - Tournaments
                    if changed_tournament_ids:
                        logger.info(f"Run {run_id}: PRIMARY cache warming - Processing {len(changed_tournament_ids)} changed tournaments")
                        
                        for tournament_id in changed_tournament_ids:
                            path = f"api/v1/ta/tournament/"
                            target_url = f"{config.API_URL}/{path}"
                            cache_key = f"GET:{path}?tournamentId={tournament_id}"
                            if refresh_until is not False:
                                # Use setup_refresh for auto-refresh functionality
                                await cache_manager.setup_refresh(
                                    cache_key, target_url, ttl, refresh_until, params={"tournamentId": tournament_id}
                                )
                                logger.debug(f"PRIMARY - Set up auto-refresh cache for tournament {tournament_id}: {cache_key}")
                            else:
                                # Fetch and cache data directly without auto-refresh
                                success = await cache_manager.fetch_and_cache(cache_key, target_url, ttl, {"tournamentId": tournament_id})
                                if success:
                                    logger.debug(f"PRIMARY - Created cache entry for tournament {tournament_id}: {cache_key}")
                                else:
                                    logger.warning(f"PRIMARY - Failed to create cache entry for tournament {tournament_id}: {cache_key}")
                        
                        logger.info(f"Run {run_id}: PRIMARY tournament cache warming complete")

                        # if changed_match_ids:
                        #     # Limit the number of matches to cache to prevent system overload
                        #     if len(changed_match_ids) > MAX_MATCHES_TO_CACHE:
                        #         logger.warning(f"Run {run_id}: Too many changed matches ({len(changed_match_ids)}), limiting to {MAX_MATCHES_TO_CACHE} most recent ones")
                        #         changed_match_ids = set(list(changed_match_ids)[:MAX_MATCHES_TO_CACHE])
                            
                        #     logger.info(f"Run {run_id}: Detected changes in {len(changed_match_ids)} matches")
                        #     total_cache_entries = len(changed_match_ids) * 4  # 4 endpoints per match
                        #     logger.info(f"Run {run_id}: Will create {total_cache_entries} cache entries in batches")
                            
                        #     # Set up cache entries for match-related endpoints
                        #     match_endpoint_configs = [
                        #         {"path": "api/v1/ta/match/", "param_key": "matchId", "path_param": False},
                        #         # {"path": "api/v1/ta/matchincidents/", "param_key": "matchId", "path_param": False},
                        #         # {"path": "api/v1/ta/matchreferee", "param_key": "matchId", "path_param": False},
                        #         # {"path": "api/v1/ta/matchteammembers/{match_id}/", "param_key": "images", "param_value": "false", "path_param": True},
                        #         # {"path": "api/v1/ta/venue/", "param_key": "venueId", "path_param": False},
                        #         # {"path": "api/v1/ta/venueunit/", "param_key": "venueId", "path_param": False}
                        #     ]
                            
                        #     # Process matches in batches to prevent overwhelming the system
                        #     match_ids_list = list(changed_match_ids)
                        #     for batch_start in range(0, len(match_ids_list), BATCH_SIZE):
                        #         batch_end = min(batch_start + BATCH_SIZE, len(match_ids_list))
                        #         match_batch = match_ids_list[batch_start:batch_end]
                                
                        #         logger.info(f"Run {run_id}: Processing match batch {batch_start//BATCH_SIZE + 1}/{(len(match_ids_list) + BATCH_SIZE - 1)//BATCH_SIZE} ({len(match_batch)} matches)")
                                
                        #         # Process this batch of matches
                        #         await process_match_cache_batch(match_batch, match_endpoint_configs, cache_manager, ttl, refresh_until, run_id)
                                
                        #         # Delay between batches to prevent overwhelming the system
                        #         if batch_end < len(match_ids_list):
                        #             logger.debug(f"Run {run_id}: Waiting {BATCH_DELAY}s before next batch")
                        #             await asyncio.sleep(BATCH_DELAY)
                        
                        logger.info(f"Run {run_id}: Cache warming setup complete")
                        
                except Exception as e:
                    logger.error(f"Run {run_id}: Cache warming failed: {e}")
                    logger.exception(e)
                finally:
                    # Always clear the cache warming flag when done
                    if cache_warming_needed:
                        CACHE_WARMING_IN_PROGRESS = False
                        logger.info(f"Run {run_id}: Cache warming completed - precache iterations can resume")

                # Record successful run
                PRECACHE_RUNS_TOTAL.labels(status="success").inc()
                PRECACHE_LAST_RUN_TIMESTAMP.set(start_time.timestamp())
                
                # Set upstream status to UP if we made it this far
                PRECACHE_UPSTREAM_STATUS.labels(endpoint="data.nif.no").set(1)
                
                end_time = pendulum.now()
                
                # Log summary of this run
                total_changes = sum(changes_detected.values())
                total_duration = (end_time - start_time).total_seconds()
                total_api_calls = sum(api_calls.values())
                
                # Create detailed timing summary
                timing_summary = []
                api_call_summary = []
                for run_type in run_types:
                    rt_lower = run_type.lower()
                    if rt_lower in run_timings:
                        timing_info = run_timings[rt_lower]
                        duration = timing_info.get("duration_seconds", 0)
                        error_status = " (FAILED)" if "error" in timing_info else ""
                        timing_summary.append(f"{run_type}: {duration:.2f}s{error_status}")
                        
                        if rt_lower in run_api_calls:
                            rt_api_calls = run_api_calls[rt_lower]
                            rt_total_calls = sum(rt_api_calls.values())
                            api_call_summary.append(f"{run_type}: {rt_total_calls} calls")
                
                logger.info(f"Run {run_id} completed: {total_changes} total changes across {len(changes_detected)} categories in {total_duration:.2f}s")
                logger.info(f"Run {run_id} timing breakdown: {', '.join(timing_summary)}")
                logger.info(f"Run {run_id} API calls: {total_api_calls} total ({', '.join(api_call_summary)})")
                logger.info(f"Run {run_id} changes: {changes_detected}")
                logger.info(f"Run {run_id} upstream status: {upstream_status}")
                
                # Create separate log entries for each run type
                for run_type in run_types:
                    rt_lower = run_type.lower()
                    
                    # Get run-specific data
                    run_timing_info = run_timings.get(rt_lower, {})
                    run_api_call_info = run_api_calls.get(rt_lower, {})
                    run_changes = {k: v for k, v in changes_detected.items() if k.startswith(f"{rt_lower}_") or (rt_lower == "primary" and not k.startswith(("secondary_", "tertiary_")))}
                    
                    log_item = {
                        "action": "pre_cache_process",
                        "run_type": run_type,
                        "start_time": run_timing_info.get("start_time", start_time.isoformat()),
                        "end_time": run_timing_info.get("end_time", end_time.isoformat()),
                        "process_time_seconds": run_timing_info.get("duration_seconds", 0),
                        "api_calls_made": run_api_call_info,
                        "api_calls_total": sum(run_api_call_info.values()) if run_api_call_info else 0,
                        "changes_detected": {k.replace(f"{rt_lower}_", "") if k.startswith(f"{rt_lower}_") else k: int(v) for k, v in run_changes.items()},
                        "changes_total": sum(run_changes.values()),
                        "run_id": run_id,
                        "upstream_status": upstream_status,
                        "loop_iteration": loop_iteration,
                        "secondary_run_counter": secondary_run_counter if run_type == "SECONDARY" else None,
                        "tertiary_run_counter": tertiary_run_counter if run_type == "TERTIARY" else None,
                        "error": run_timing_info.get("error") if "error" in run_timing_info else None,
                        "status": "failed" if "error" in run_timing_info else "success"
                    }
                    
                    # Remove None values
                    log_item = {k: v for k, v in log_item.items() if v is not None}
                    
                    logger.info(f"logProcess{run_type}: {json.dumps(log_item, ensure_ascii=False)}")
                
                # Save precache run stats to debug file with separate files for each run type
                try:
                    # Create logs directory if it doesn't exist
                    log_dir = "logs/precache_debug_logs"
                    os.makedirs(log_dir, exist_ok=True)
                    
                    # Save separate files for each run type that was executed
                    for run_type in run_types:
                        rt_lower = run_type.lower()
                        
                        # Get run-specific data for file saving
                        run_timing_info = run_timings.get(rt_lower, {})
                        run_api_call_info = run_api_calls.get(rt_lower, {})
                        run_changes = {k: v for k, v in changes_detected.items() if k.startswith(f"{rt_lower}_") or (rt_lower == "primary" and not k.startswith(("secondary_", "tertiary_")))}
                        
                        file_log_item = {
                            "action": "pre_cache_process",
                            "run_type": run_type,
                            "start_time": run_timing_info.get("start_time", start_time.isoformat()),
                            "end_time": run_timing_info.get("end_time", end_time.isoformat()),
                            "process_time_seconds": run_timing_info.get("duration_seconds", 0),
                            "api_calls_made": run_api_call_info,
                            "api_calls_total": sum(run_api_call_info.values()) if run_api_call_info else 0,
                            "changes_detected": {k.replace(f"{rt_lower}_", "") if k.startswith(f"{rt_lower}_") else k: int(v) for k, v in run_changes.items()},
                            "changes_total": sum(run_changes.values()),
                            "run_id": run_id,
                            "upstream_status": upstream_status,
                            "loop_iteration": loop_iteration,
                            "secondary_run_counter": secondary_run_counter if run_type == "SECONDARY" else None,
                            "tertiary_run_counter": tertiary_run_counter if run_type == "TERTIARY" else None,
                            "error": run_timing_info.get("error") if "error" in run_timing_info else None,
                            "status": "failed" if "error" in run_timing_info else "success"
                        }
                        
                        # Remove None values
                        file_log_item = {k: v for k, v in file_log_item.items() if v is not None}
                        
                        # Save current run stats to the latest file for this run type
                        stats_filepath = os.path.join(log_dir, f"precache_{rt_lower}_stats.json")
                        with open(stats_filepath, 'w', encoding='utf-8') as f:
                            json.dump(file_log_item, f, indent=2, ensure_ascii=False, default=str)
                        
                        logger.info(f"Run {run_id}: Saved {run_type} run stats to {stats_filepath}")
                        
                        # Also maintain a history file with the last 10 runs for each type
                        history_filepath = os.path.join(log_dir, f"precache_{rt_lower}_history.json")
                        
                        # Load existing history
                        run_history = []
                        if os.path.exists(history_filepath):
                            try:
                                with open(history_filepath, 'r', encoding='utf-8') as f:
                                    run_history = json.load(f)
                            except Exception as e:
                                logger.warning(f"Could not load existing {rt_lower} run history: {e}")
                                run_history = []
                        
                        # Add current run to history
                        run_history.append(file_log_item)
                        
                        # Keep only last 10 runs
                        run_history = run_history[-10:]
                        
                        # Save updated history
                        with open(history_filepath, 'w', encoding='utf-8') as f:
                            json.dump(run_history, f, indent=2, ensure_ascii=False, default=str)
                        
                        logger.debug(f"Run {run_id}: Updated {run_type} run history with {len(run_history)} entries")
                    
                    # Also save to a combined overview file that tracks all run types
                    overview_filepath = os.path.join(log_dir, "precache_all_runs_overview.json")
                    
                    # Create a combined log item for overview
                    combined_log_item = {
                        "action": "pre_cache_process",
                        "start_time": start_time.isoformat(),
                        "end_time": end_time.isoformat(),
                        "process_time_seconds": (end_time - start_time).total_seconds(),
                        "api_calls_made": api_calls,
                        "api_calls_by_run_type": run_api_calls,
                        "run_type_timings": run_timings,
                        "changes_detected": {k: int(v) for k, v in changes_detected.items()},
                        "run_id": run_id,
                        "total_changes": total_changes,
                        "upstream_status": upstream_status,
                        "run_types": run_types,
                        "loop_iteration": loop_iteration,
                        "secondary_run_counter": secondary_run_counter,
                        "tertiary_run_counter": tertiary_run_counter
                    }
                    
                    # Load existing overview
                    all_runs_overview = []
                    if os.path.exists(overview_filepath):
                        try:
                            with open(overview_filepath, 'r', encoding='utf-8') as f:
                                all_runs_overview = json.load(f)
                        except Exception as e:
                            logger.warning(f"Could not load existing all runs overview: {e}")
                            all_runs_overview = []
                    
                    # Add current run to overview
                    all_runs_overview.append(combined_log_item)
                    
                    # Keep only last 50 runs across all types
                    all_runs_overview = all_runs_overview[-50:]
                    
                    # Save updated overview
                    with open(overview_filepath, 'w', encoding='utf-8') as f:
                        json.dump(all_runs_overview, f, indent=2, ensure_ascii=False, default=str)
                    
                except Exception as e:
                    logger.warning(f"Failed to save precache run stats to file: {e}")
                    
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
                all_categories = ["valid_seasons", "tournaments_in_season", "root_tournaments", "tournament_matches", "individual_matches", "unique_team_ids"]
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
                logger.error(f"Error in precache_orchestrator: {e}")
                logger.exception(e)
                
                # Create separate error log entries for each run type
                if 'run_types' in locals():
                    for run_type in run_types:
                        rt_lower = run_type.lower()
                        
                        # Get run-specific error data
                        run_timing_info = run_timings.get(rt_lower, {}) if 'run_timings' in locals() else {}
                        run_api_call_info = run_api_calls.get(rt_lower, {}) if 'run_api_calls' in locals() else {}
                        
                        error_log_item = {
                            "action": "pre_cache_process",
                            "run_type": run_type,
                            "start_time": run_timing_info.get("start_time", start_time.isoformat()),
                            "end_time": run_timing_info.get("end_time", pendulum.now().isoformat()),
                            "process_time_seconds": run_timing_info.get("duration_seconds", 0),
                            "api_calls_made": run_api_call_info,
                            "api_calls_total": sum(run_api_call_info.values()) if run_api_call_info else 0,
                            "changes_detected": {},
                            "changes_total": 0,
                            "run_id": run_id,
                            "upstream_status": "DOWN",
                            "loop_iteration": loop_iteration if 'loop_iteration' in locals() else 0,
                            "secondary_run_counter": secondary_run_counter if run_type == "SECONDARY" and 'secondary_run_counter' in locals() else None,
                            "tertiary_run_counter": tertiary_run_counter if run_type == "TERTIARY" and 'tertiary_run_counter' in locals() else None,
                            "error": run_timing_info.get("error", str(e)),
                            "status": "failed"
                        }
                        
                        # Remove None values
                        error_log_item = {k: v for k, v in error_log_item.items() if v is not None}
                        
                        logger.info(f"logProcess{run_type}: {json.dumps(error_log_item, ensure_ascii=False)}")
                
                # Save error run stats to debug file
                try:
                    end_time = pendulum.now()
                    combined_error_log_item = {
                        "action": "pre_cache_process",
                        "start_time": start_time.isoformat(),
                        "end_time": end_time.isoformat(),
                        "process_time_seconds": (end_time - start_time).total_seconds(),
                        "api_calls_made": api_calls if 'api_calls' in locals() else {},
                        "api_calls_by_run_type": run_api_calls if 'run_api_calls' in locals() else {},
                        "run_type_timings": run_timings if 'run_timings' in locals() else {},
                        "changes_detected": {},
                        "run_id": run_id,
                        "total_changes": 0,
                        "upstream_status": "DOWN",
                        "error": str(e),
                        "status": "failed",
                        "run_types": run_types if 'run_types' in locals() else [],
                        "loop_iteration": loop_iteration if 'loop_iteration' in locals() else 0
                    }
                    
                    # Create logs directory if it doesn't exist
                    log_dir = "logs/precache_debug_logs"
                    os.makedirs(log_dir, exist_ok=True)
                    
                    # Determine run type for error file naming
                    error_run_types_str = "_".join(run_types).lower() if 'run_types' in locals() else "unknown"
                    
                    # Save error run stats to the latest file
                    stats_filepath = os.path.join(log_dir, f"precache_{error_run_types_str}_stats.json")
                    with open(stats_filepath, 'w', encoding='utf-8') as f:
                        json.dump(combined_error_log_item, f, indent=2, ensure_ascii=False, default=str)
                    
                    logger.info(f"Run {run_id}: Saved {error_run_types_str.upper()} error run stats to {stats_filepath}")
                    
                    # Also add to history file
                    history_filepath = os.path.join(log_dir, f"precache_{error_run_types_str}_history.json")
                    
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
                    run_history.append(combined_error_log_item)
                    
                    # Keep only last 10 runs
                    run_history = run_history[-10:]
                    
                    # Save updated history
                    with open(history_filepath, 'w', encoding='utf-8') as f:
                        json.dump(run_history, f, indent=2, ensure_ascii=False, default=str)
                    
                    logger.debug(f"Run {run_id}: Added {error_run_types_str.upper()} error run to history with {len(run_history)} total entries")
                    
                    # Also add to combined overview for error runs
                    overview_filepath = os.path.join(log_dir, "precache_all_runs_overview.json")
                    
                    # Load existing overview
                    all_runs_overview = []
                    if os.path.exists(overview_filepath):
                        try:
                            with open(overview_filepath, 'r', encoding='utf-8') as f:
                                all_runs_overview = json.load(f)
                        except Exception as e:
                            logger.warning(f"Could not load existing all runs overview for error: {e}")
                            all_runs_overview = []
                    
                    # Add error run to overview
                    all_runs_overview.append(combined_error_log_item)
                    
                    # Keep only last 50 runs across all types
                    all_runs_overview = all_runs_overview[-50:]
                    
                    # Save updated overview
                    with open(overview_filepath, 'w', encoding='utf-8') as f:
                        json.dump(all_runs_overview, f, indent=2, ensure_ascii=False, default=str)
                    
                except Exception as save_error:
                    logger.warning(f"Failed to save error run stats to file: {save_error}")
            finally:
                pass  # Resources are cleaned up inside pre_cache_getter and compare_and_update_cache

        logger.info(f"Precache run {run_id} completed, sleeping for 180 seconds before next iteration {loop_iteration + 1}")
        await asyncio.sleep(180)
        logger.info(f"Sleep completed, starting next precache iteration {loop_iteration + 1}")
