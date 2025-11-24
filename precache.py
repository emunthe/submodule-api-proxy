# Copyright (C) 2025 Sasha Shipka <sasha.shipka@copyleft.no>
# You may use, distribute and modify this code under the terms of the GNU General Public License v3.0

import asyncio
import json
from datetime import datetime

import pendulum
from prometheus_client import Counter, Gauge, Histogram

from .config import config
from .token import TokenManager
from .util import get_http_client, get_logger, get_redis_client


logger = get_logger(__name__)

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

PRECACHE_API_CALLS = Counter(
    "precache_api_calls_total", 
    "Total API calls made during pre-cache operations",
    ["call_type"]  # seasons, tournaments, matches
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
    ["call_type"]  # seasons, tournaments, matches
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

PRECACHE_TOURNAMENTS_IN_SEASON_INFO = Gauge(
    "precache_tournaments_in_season_info",
    "Information about tournaments in season with tournament details as labels",
    ["tournament_id", "tournament_name", "season_id", "sport_id", "is_root"]
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
        
        # Add individual season tournament cache keys
        try:
            # Get all keys that match the season tournament pattern
            season_tournament_keys = []
            async for key in redis_client.scan_iter(match="tournaments_season_*"):
                season_tournament_keys.append(key)
            
            # Add individual season caches to metrics
            for key in season_tournament_keys:
                cache_keys[f"season_cache_{key}"] = key
                
        except Exception as e:
            logger.debug(f"Could not scan for season tournament cache keys: {e}")
        
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
                # sport_names = {
                #     72: "bandy",
                #     151: "innebandy", 
                #     71: "landhockey",
                #     73: "rinkbandy"
                # }
                sport_names = {
                    72: "bandy",
                    151: "innebandy"
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
        PRECACHE_TOURNAMENTS_IN_SEASON_INFO.clear()
        
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
                    151: "innebandy"
                }
                
                # Update info metric with labels for each tournament
                for tournament in tournaments:
                    if not isinstance(tournament, dict):
                        continue
                        
                    tournament_id = str(tournament.get("tournamentId", "unknown"))
                    tournament_name = tournament.get("tournamentName", "unknown")[:50]  # Limit length for labels
                    season_id = str(tournament.get("seasonId", "unknown"))
                    sport_id = tournament.get("sportId", 0)
                    sport_name = sport_names.get(sport_id, f"sport_{sport_id}")
                    is_root = "true" if not tournament.get("parentTournamentId") else "false"
                    
                    # Set metric with tournament information as labels
                    PRECACHE_TOURNAMENTS_IN_SEASON_INFO.labels(
                        tournament_id=tournament_id,
                        tournament_name=tournament_name,
                        season_id=season_id,
                        sport_id=str(sport_id),
                        is_root=is_root
                    ).set(1)  # Value of 1 indicates this tournament exists
                    
                logger.debug(f"Updated tournaments in season metrics: {len(tournaments)} tournaments")
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse tournaments_in_season data: {e}")
                PRECACHE_TOURNAMENTS_IN_SEASON_COUNT.set(0)
        else:
            # No data available
            PRECACHE_TOURNAMENTS_IN_SEASON_COUNT.set(0)
            
    except Exception as e:
        logger.error(f"Error updating tournaments in season metrics: {e}")


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
        
        # Reset last run timestamp
        PRECACHE_LAST_RUN_TIMESTAMP.set(0)
        
        logger.info(f"Cleared precache data keys: {cleared_keys}")
        
        return {
            "status": "success",
            "message": f"Cleared {len(cleared_keys)} precache data keys",
            "cleared_keys": cleared_keys
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


async def detect_change_tournaments_and_matches(cache_manager, token_manager):
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
        changed_match_ids = set()

        # Start timing for metrics
        with PRECACHE_DURATION_SECONDS.time():
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

                # Update cached data size metrics
                await _update_cached_data_size_metrics(redis_client)
                
                # Update valid seasons metrics
                await _update_valid_seasons_metrics(redis_client)
                
                # Update tournaments in season metrics
                await _update_tournaments_in_season_metrics(redis_client)

                token = await token_manager.get_token()
                headers = {"Authorization": f"Bearer {token['access_token']}"}
                client = get_http_client()

                # -----------------------------------------------------------------
                # Fetch season list for relevant sports and years
                # Only fetch seasons from 2024+ to reduce API calls and improve performance
                seasons = []
                current_year = pendulum.now().year
                # sport_ids = [72, 151, 71, 73]  # Configurable sport IDs
                sport_ids = [72, 151]  # Configurable sport IDs
                
                # Optimize by only fetching recent years since we filter to 2024+
                # start_year = max(2024, current_year - 2)  # Only fetch last 2-3 years for efficiency
                start_year = current_year  # Only fetch last 2-3 years for efficiency
                
                logger.info(f"Fetching seasons for years {start_year}-{current_year}, sports {sport_ids}")
                
                # Use async gathering for parallel requests
                async def fetch_seasons_for_year_sport(year, sport_id):
                    try:
                        url = f"{config.API_URL}/api/v1/ta/Seasons/"
                        logger.debug(f"Fetching seasons for year {year}, sport {sport_id}")
                        
                        resp = await client.get(
                            url,
                            headers=headers,
                            params={"year": year, "sportId": sport_id},
                        )
                        
                        # Track API call metrics
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
                                    logger.warning(
                                        f"Unexpected seasons data type for year={year} sport={sport_id}: {type(data).__name__}"
                                    )
                                    return []
                                    
                            except Exception as e:
                                logger.warning(
                                    f"Failed to parse seasons response for year={year} sport={sport_id}: {e}"
                                )
                                return []
                        else:
                            logger.warning(f"API request failed for year={year} sport={sport_id}: {resp.status_code}")
                            return []
                            
                    except Exception as e:
                        logger.error(f"Error fetching seasons for year={year} sport={sport_id}: {e}")
                        return []
                
                # Create tasks for parallel execution
                tasks = []
                total_expected_calls = 0
                
                for year in range(start_year, current_year + 1):
                    for sport_id in sport_ids:
                        tasks.append(fetch_seasons_for_year_sport(year, sport_id))
                        total_expected_calls += 1
                
                successful_calls = 0
                
                # Execute all requests in parallel with limited concurrency
                batch_size = 5  # Limit concurrent requests
                for i in range(0, len(tasks), batch_size):
                    batch = tasks[i:i + batch_size]
                    results = await asyncio.gather(*batch, return_exceptions=True)
                    
                    # Process results
                    for result in results:
                        if isinstance(result, list):
                            seasons.extend(result)
                            successful_calls += 1
                        elif isinstance(result, Exception):
                            logger.error(f"Task failed: {result}")
                    
                    # Small delay between batches to be nice to the API
                    if i + batch_size < len(tasks):
                        await asyncio.sleep(0.1)
                
                # Update API call metrics
                api_calls["seasons"] = total_expected_calls
                if total_expected_calls > 0:
                    success_rate = (successful_calls / total_expected_calls) * 100
                    PRECACHE_API_CALL_SUCCESS_RATE.labels(call_type="seasons").set(success_rate)
                    logger.info(f"Seasons API calls: {successful_calls}/{total_expected_calls} successful ({success_rate:.1f}%)")

                # log the final seasons fetched
                logger.info(f"Total seasons fetched: {len(seasons)}")
                
                if len(seasons) == 0:
                    logger.warning("No seasons were fetched from any API calls - this may indicate API issues or no data available")
                elif len(seasons) < 5:
                    logger.warning(f"Very few seasons fetched ({len(seasons)}) - this may indicate API issues")
                    # Log all seasons when we have very few
                    for i, season in enumerate(seasons):
                        logger.info(f"Season {i+1}: {season}")
                else:
                    # Log a sample of seasons when we have many
                    logger.info(f"Sample of fetched seasons (first 3):")
                    for i in range(min(3, len(seasons))):
                        season = seasons[i]
                        logger.info(f"Season {i+1}: ID={season.get('seasonId')}, name='{season.get('seasonName')}', sport={season.get('sportId')}, dateFrom={season.get('seasonDateFrom')}")

                # Filter seasons to get valid ones with improved logic
                valid_seasons = []
                invalid_count = 0
                debug_stats = {
                    "total_processed": 0,
                    "missing_date": 0,
                    "invalid_year": 0,
                    "bedrift_excluded": 0,
                    "parse_errors": 0,
                    "valid_added": 0
                }
                
                logger.info(f"Starting season filtering on {len(seasons)} total seasons")
                
                for season in seasons:
                    debug_stats["total_processed"] += 1
                    
                    if not isinstance(season, dict):
                        invalid_count += 1
                        continue
                        
                    try:
                        # Parse season date - handle multiple formats
                        season_date_from = season.get("seasonDateFrom")
                        if not season_date_from:
                            debug_stats["missing_date"] += 1
                            invalid_count += 1
                            logger.debug(f"Season {season.get('seasonId', 'unknown')} missing seasonDateFrom")
                            continue
                        
                        # Try different date formats that the API might return
                        season_year = None
                        date_formats_to_try = [
                            "%m/%d/%Y %H:%M:%S",  # MM/DD/YYYY HH:MM:SS (what we're seeing in logs)
                            "%Y-%m-%d %H:%M:%S",  # YYYY-MM-DD HH:MM:SS  
                            "%Y-%m-%dT%H:%M:%S",  # ISO format with T
                            "%Y-%m-%d",           # YYYY-MM-DD
                            "%m/%d/%Y"            # MM/DD/YYYY
                        ]
                        
                        for date_format in date_formats_to_try:
                            try:
                                # Try pendulum first if available
                                parsed_date = pendulum.from_format(season_date_from, date_format)
                                season_year = parsed_date.year
                                logger.debug(f"Successfully parsed date '{season_date_from}' using pendulum format '{date_format}' -> year {season_year}")
                                break
                            except (ValueError, TypeError) as e:
                                logger.debug(f"Pendulum failed for '{season_date_from}' with format '{date_format}': {e}")
                                # Try datetime as fallback
                                try:
                                    parsed_date = datetime.strptime(season_date_from, date_format)
                                    season_year = parsed_date.year
                                    logger.debug(f"Successfully parsed date '{season_date_from}' using datetime format '{date_format}' -> year {season_year}")
                                    break
                                except (ValueError, TypeError) as e2:
                                    logger.debug(f"Datetime also failed for '{season_date_from}' with format '{date_format}': {e2}")
                                    continue
                        
                        # If none of the formats worked, try pendulum.parse as fallback
                        if season_year is None:
                            try:
                                parsed_date = pendulum.parse(season_date_from)
                                season_year = parsed_date.year
                                logger.debug(f"Successfully parsed date '{season_date_from}' using pendulum.parse fallback -> year {season_year}")
                            except (ValueError, TypeError, NameError) as e:
                                logger.debug(f"pendulum.parse fallback also failed for '{season_date_from}': {e}")
                                pass
                        
                        if season_year is None:
                            debug_stats["parse_errors"] += 1
                            invalid_count += 1
                            logger.warning(f"Failed to parse season date for season {season.get('seasonId', 'unknown')}: Unable to parse '{season_date_from}' with any known format")
                            logger.debug(f"Raw season date string: {repr(season_date_from)} (type: {type(season_date_from)})")
                            continue
                        else:
                            logger.debug(f"Successfully parsed season {season.get('seasonId')} date '{season_date_from}' -> year {season_year}")
                            
                        sport_id = season.get("sportId")
                        
                        # Log sample of seasons for debugging
                        if debug_stats["total_processed"] <= 5:
                            logger.info(f"Sample season: ID={season.get('seasonId')}, year={season_year}, sport={sport_id}, name='{season.get('seasonName', '')}', dateFrom='{season_date_from}'")
                        
                        # Apply filtering criteria
                        if season_year >= 2024:
                            if sport_id != 72:
                                # Include all non-bandy sports from 2024+
                                valid_seasons.append(season)
                                debug_stats["valid_added"] += 1
                                logger.debug(f"Added non-bandy season: {season.get('seasonName')} (year: {season_year}, sport: {sport_id})")
                            else:
                                # For bandy (sport_id 72), exclude "bedrift" (company) leagues
                                season_name = season.get("seasonName", "").lower()
                                if "bedrift" not in season_name:
                                    valid_seasons.append(season)
                                    debug_stats["valid_added"] += 1
                                    logger.debug(f"Added bandy season: {season.get('seasonName')} (year: {season_year})")
                                else:
                                    debug_stats["bedrift_excluded"] += 1
                                    logger.debug(f"Excluded bedrift season: {season.get('seasonName')}")
                        else:
                            debug_stats["invalid_year"] += 1
                            logger.debug(f"Excluded season due to year < 2024: {season.get('seasonName')} (year: {season_year})")
                                    
                    except Exception as e:
                        # Log parsing errors but continue processing
                        debug_stats["parse_errors"] += 1
                        logger.warning(f"Unexpected error parsing season {season.get('seasonId', 'unknown')}: {e} - data: {season}")
                        invalid_count += 1
                        continue

                logger.info(f"Season filtering complete - Debug stats: {debug_stats}")
                logger.info(f"Filtered to {len(valid_seasons)} valid seasons (skipped {invalid_count} invalid)")

                # Update metrics for items processed (both total and filtered)
                PRECACHE_ITEMS_PROCESSED.labels(item_type="seasons").set(len(seasons))
                PRECACHE_ITEMS_PROCESSED.labels(item_type="valid_seasons").set(len(valid_seasons))

                # Always update cache for valid_seasons to ensure consistency
                valid_seasons_changed = json.dumps(valid_seasons, sort_keys=True) != json.dumps(cached_valid_seasons or [], sort_keys=True)
                
                # Force update for debugging if we have valid seasons but cache is empty
                if len(valid_seasons) > 0 and (not cached_valid_seasons or len(cached_valid_seasons) == 0):
                    logger.info(f"Forcing valid_seasons cache update: have {len(valid_seasons)} valid seasons but cache is empty")
                    valid_seasons_changed = True
                
                if valid_seasons_changed or True:  # Temporarily always update for debugging
                    await redis_client.set(
                        "valid_seasons",
                        json.dumps(
                            {"data": valid_seasons, "last_updated": start_time.isoformat()},
                            ensure_ascii=False,
                        ),
                    )
                    changes_detected["valid_seasons"] = len(valid_seasons)
                    PRECACHE_CHANGES_DETECTED.labels(category="valid_seasons").inc()
                    logger.info(f"Valid seasons cache updated with {len(valid_seasons)} seasons (was {len(cached_valid_seasons or [])} seasons)")
                    
                    # Verify the data was stored
                    verification = await redis_client.get("valid_seasons")
                    if verification:
                        try:
                            parsed_verification = json.loads(verification)
                            stored_count = len(parsed_verification.get("data", []))
                            logger.info(f"VERIFICATION: Successfully stored {stored_count} valid seasons in Redis")
                            
                            # Update the new valid seasons metrics
                            await _update_valid_seasons_metrics(redis_client)
                            logger.debug(f"Updated valid seasons content and count metrics")
                            
                        except Exception as e:
                            logger.error(f"VERIFICATION FAILED: Could not parse stored valid_seasons data: {e}")
                    else:
                        logger.error("VERIFICATION FAILED: No data found in Redis after attempted storage")
                else:
                    logger.debug(f"No valid seasons changes detected - cache has {len(cached_valid_seasons or [])} seasons, fetched {len(valid_seasons)}")
                    
                    # Still update metrics even if no changes (for consistency)
                    await _update_valid_seasons_metrics(redis_client)

                # Log the final result for debugging
                if len(valid_seasons) == 0:
                    logger.warning("ALERT: No valid seasons after filtering! This will result in empty tournaments_in_season data.")
                    logger.info(f"Filtering criteria: year >= 2024, exclude bandy 'bedrift' leagues")
                    logger.info(f"Debug stats from filtering: {debug_stats}")
                else:
                    logger.info(f"Successfully filtered {len(valid_seasons)} valid seasons for tournament fetching")

                # -----------------------------------------------------------------
                # Fetch tournaments per season
                logger.info(f"Fetching tournaments for {len(valid_seasons)} seasons")
                
                # Initialize tournaments lists
                tournaments = []
                root_tournaments = []
                
                # Skip tournament fetching if no valid seasons
                if not valid_seasons:
                    logger.info("No valid seasons found, skipping tournament fetching")
                    api_calls["tournaments"] = 0
                    PRECACHE_API_CALL_SUCCESS_RATE.labels(call_type="tournaments").set(100)
                else:
                    async def fetch_tournaments_for_season(season):
                        """Fetch tournaments for a single season"""
                        try:
                            season_id = season.get("seasonId")
                            if not season_id:
                                logger.warning(f"Season missing seasonId: {season}")
                                return {"tournaments_in_season": [], "root_tournaments": [], "success": False}
                                
                            url = f"{config.API_URL}/api/v1/ta/Tournament/Season/{season_id}/"
                            logger.debug(f"Fetching tournaments for season {season_id}")
                            
                            resp = await client.get(url, headers=headers, params={"hierarchy": True})
                            PRECACHE_API_CALLS.labels(call_type="tournaments").inc()
                            
                            if resp.status_code < 400:
                                try:
                                    raw_response = resp.json()
                                    data = raw_response.get("tournamentsInSeason", [])
                                    tournaments_for_season = []
                                    root_tournaments_for_season = []
                                    
                                    for t in data:
                                        # Validate tournament structure
                                        if not isinstance(t, dict) or not t.get("tournamentId"):
                                            logger.debug(f"Skipping invalid tournament data: {t}")
                                            continue
                                            
                                        t["apiPath"] = url
                                        tournaments_for_season.append(t)
                                        if not t.get("parentTournamentId"):
                                            root_tournaments_for_season.append(t)
                                    
                                    # Cache individual season tournament data
                                    season_cache_key = f"tournaments_season_{season_id}"
                                    season_cache_data = {
                                        "tournamentsInSeason": tournaments_for_season,
                                        "raw_response": raw_response,
                                        "api_url": url,
                                        "season_id": season_id,
                                        "last_updated": pendulum.now().isoformat()
                                    }
                                    
                                    try:
                                        await redis_client.set(
                                            season_cache_key,
                                            json.dumps(season_cache_data, ensure_ascii=False)
                                        )
                                        logger.debug(f"Cached tournaments for season {season_id} in key: {season_cache_key}")
                                    except Exception as cache_error:
                                        logger.warning(f"Failed to cache season {season_id} tournament data: {cache_error}")
                                    
                                    return {
                                        "tournaments_in_season": tournaments_for_season,
                                        "root_tournaments": root_tournaments_for_season,
                                        "success": True,
                                        "season_cache_key": season_cache_key
                                    }
                                    
                                except Exception as e:
                                    logger.warning(f"Failed to parse tournaments response for season {season_id}: {e}")
                                    return {"tournaments_in_season": [], "root_tournaments": [], "success": False}
                            else:
                                logger.warning(f"Tournaments API request failed for season {season_id}: {resp.status_code}")
                                return {"tournaments_in_season": [], "root_tournaments": [], "success": False}
                                
                        except Exception as e:
                            logger.error(f"Error fetching tournaments for season {season.get('seasonId', 'unknown')}: {e}")
                            return {"tournaments_in_season": [], "root_tournaments": [], "success": False}

                    async def ensure_cache_entries_for_api_paths(tournaments_list):
                        """Ensure cache entries exist for all unique apiPath values in tournaments and individual tournament endpoints"""
                        if not tournaments_list:
                            return
                        
                        # Extract unique apiPath values (season-based tournament lists)
                        api_paths = set()
                        tournament_ids = set()
                        
                        for tournament in tournaments_list:
                            if isinstance(tournament, dict):
                                if tournament.get("apiPath"):
                                    api_paths.add(tournament["apiPath"])
                                if tournament.get("tournamentId"):
                                    tournament_ids.add(tournament["tournamentId"])
                        
                        logger.info(f"Found {len(api_paths)} unique API paths and {len(tournament_ids)} tournament IDs")
                        
                        refresh_until = pendulum.now().add(days=7)
                        ttl = 7 * 24 * 60 * 60  # 7 days
                        
                        # 1. Set up cache for season-based tournament listing APIs
                        for api_path in api_paths:
                            try:
                                cache_key = f"GET:{api_path}"
                                existing_cache = await redis_client.get(cache_key)
                                
                                if not existing_cache:
                                    logger.debug(f"Setting up cache refresh for missing API path: {api_path}")
                                    
                                    # Parse URL to extract season_id for the params
                                    # API path format: https://data.nif.no/api/v1/ta/Tournament/Season/{season_id}/?hierarchy=true
                                    import re
                                    season_match = re.search(r'/Tournament/Season/(\d+)/', api_path)
                                    if season_match:
                                        season_id = season_match.group(1)
                                        params = {"hierarchy": True}
                                        
                                        await cache_manager.setup_refresh(
                                            cache_key, api_path, ttl, refresh_until, params=params
                                        )
                                        logger.debug(f"Added cache refresh for API path: {api_path} (season {season_id})")
                                    else:
                                        await cache_manager.setup_refresh(
                                            cache_key, api_path, ttl, refresh_until
                                        )
                                        logger.debug(f"Added cache refresh for API path: {api_path} (no season extracted)")
                                else:
                                    logger.debug(f"Cache entry already exists for API path: {api_path}")
                                    
                            except Exception as e:
                                logger.error(f"Error setting up cache for API path {api_path}: {e}")
                        
                        # 2. Set up cache for individual tournament-specific endpoints
                        tournament_endpoint_templates = [
                            f"{config.API_URL}/api/v1/ta/Tournament/",
                            f"{config.API_URL}/api/v1/ta/TournamentMatches/",
                            f"{config.API_URL}/api/v1/ta/TournamentTeams"
                        ]
                        
                        for tournament_id in tournament_ids:
                            for base_url in tournament_endpoint_templates:
                                try:
                                    # Create params with tournament ID
                                    params = {"tournamentId": tournament_id}
                                    
                                    # Create cache key - matches your API endpoint format exactly
                                    cache_key = f"GET:{base_url}?tournamentId={tournament_id}"
                                    
                                    # Check if cache entry exists
                                    existing_cache = await redis_client.get(cache_key)
                                    
                                    if not existing_cache:
                                        await cache_manager.setup_refresh(
                                            cache_key, base_url, ttl, refresh_until, params=params
                                        )
                                        logger.debug(f"Added cache refresh for tournament endpoint: {cache_key}")
                                    else:
                                        logger.debug(f"Cache entry already exists for tournament endpoint: {cache_key}")
                                        
                                except Exception as e:
                                    logger.error(f"Error setting up cache for tournament {tournament_id} endpoint {base_url}: {e}")
                        
                        logger.info(f"Completed cache setup check for {len(api_paths)} season API paths and {len(tournament_ids)} * 3 tournament endpoints")
                    
                    # Create tasks for parallel execution
                    tournament_tasks = [fetch_tournaments_for_season(season) for season in valid_seasons]
                    total_tournament_calls = len(valid_seasons)
                    successful_tournament_calls = 0
                    
                    tournaments = []
                    root_tournaments = []
                    season_cache_keys = []
                    
                    # Execute requests in batches to avoid overwhelming the API
                    batch_size = 3  # Smaller batch for tournaments since they can be larger responses
                    
                    for i in range(0, len(tournament_tasks), batch_size):
                        batch = tournament_tasks[i:i + batch_size]
                        results = await asyncio.gather(*batch, return_exceptions=True)
                        
                        # Process batch results
                        for result in results:
                            if isinstance(result, dict) and result.get("success"):
                                tournaments.extend(result["tournaments_in_season"])
                                root_tournaments.extend(result["root_tournaments"])
                                successful_tournament_calls += 1
                                
                                # Track individual season cache keys
                                if result.get("season_cache_key"):
                                    season_cache_keys.append(result["season_cache_key"])
                                    
                            elif isinstance(result, dict):
                                # Failed but handled gracefully
                                pass
                            elif isinstance(result, Exception):
                                logger.error(f"Tournament fetch task failed: {result}")
                        
                        # Small delay between batches
                        if i + batch_size < len(tournament_tasks):
                            await asyncio.sleep(0.1)
                    
                    # Log individual season cache entries created
                    if season_cache_keys:
                        logger.info(f"Created individual season cache entries: {len(season_cache_keys)} entries")
                        logger.debug(f"Season cache keys: {season_cache_keys}")
                    
                    # Update API call tracking
                    api_calls["tournaments"] = total_tournament_calls
                    if total_tournament_calls > 0:
                        success_rate = (successful_tournament_calls / total_tournament_calls) * 100
                        PRECACHE_API_CALL_SUCCESS_RATE.labels(call_type="tournaments").set(success_rate)
                        logger.info(f"Tournaments API calls: {successful_tournament_calls}/{total_tournament_calls} successful ({success_rate:.1f}%)")
                    
                    logger.info(f"Fetched {len(tournaments)} tournaments ({len(root_tournaments)} root tournaments)")

                # Update metrics for tournaments processed
                PRECACHE_ITEMS_PROCESSED.labels(item_type="tournaments").set(len(tournaments))
                PRECACHE_ITEMS_PROCESSED.labels(item_type="root_tournaments").set(len(root_tournaments))

                # Always update cache for tournaments to ensure consistency
                tournaments_changed = json.dumps(tournaments, sort_keys=True) != json.dumps(cached_tournaments or [], sort_keys=True)
                
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
                    
                    # Ensure cache entries exist for all unique apiPath values
                    await ensure_cache_entries_for_api_paths(tournaments)
                    
                    # Calculate which tournaments changed
                    cached_ids = {t["tournamentId"] for t in (cached_tournaments or []) if "tournamentId" in t}
                    new_ids = {t["tournamentId"] for t in tournaments if "tournamentId" in t}
                    changed_tournament_ids = new_ids.symmetric_difference(cached_ids)
                    
                    changes_detected["tournaments_in_season"] = len(changed_tournament_ids)
                    PRECACHE_CHANGES_DETECTED.labels(category="tournaments_in_season").inc()
                    
                    logger.info(f"Tournament changes detected: {len(changed_tournament_ids)} tournaments changed")
                    logger.info(f"Updated tournaments_in_season cache with {len(tournaments)} tournaments")
                    
                    # Update tournaments in season metrics after data change
                    await _update_tournaments_in_season_metrics(redis_client)
                else:
                    changed_tournament_ids = set()
                    logger.debug(f"No tournament changes detected - cache has {len(cached_tournaments or [])} tournaments, fetched {len(tournaments)}")
                    
                    # Still update metrics even if no changes (for consistency)
                    await _update_tournaments_in_season_metrics(redis_client)

                
                
                # -----------------------------------------------------------------
                # Fetch tournament matches for root tournaments
                # get data with calls to /api/v1/ta/TournamentMatches?tournamentId={tournamentId} based on root_tournaments only
                matches = []
                for tournament in root_tournaments:
                    tournament_id = tournament.get("tournamentId")
                    if not tournament_id:
                        continue
                    try:
                        url = f"{config.API_URL}/api/v1/ta/TournamentMatches"
                        resp = await client.get(url, headers=headers, params={"tournamentId": tournament_id})
                        PRECACHE_API_CALLS.labels(call_type="matches").inc()
                        
                        # store raw response for detecting changes
                        if resp.status_code < 400:
                            try:
                                raw_response = resp.json()
                                tournament_matches = raw_response.get("matches", [])
                                
                                for match in tournament_matches:
                                    # Validate match structure
                                    if not isinstance(match, dict) or not match.get("matchId"):
                                        logger.debug(f"Skipping invalid match data: {match}")
                                        continue
                                    
                                    match["apiPath"] = url
                                    match["tournamentId"] = tournament_id
                                    matches.append(match)
                                
                                logger.debug(f"Fetched {len(tournament_matches)} matches for tournament {tournament_id}")
                            except Exception as e:
                                logger.warning(f"Failed to parse matches response for tournament {tournament_id}: {e}")
                        else:
                            logger.warning(f"Matches API request failed for tournament {tournament_id}: {resp.status_code}")
                    except Exception as e:
                        logger.error(f"Error fetching matches for tournament {tournament_id}: {e}") 
                


                # Update metrics for matches processed
                PRECACHE_ITEMS_PROCESSED.labels(item_type="matches").set(len(matches))

                # Store raw match data and detect changes
                matches_changed = json.dumps(matches, sort_keys=True) != json.dumps(
                    cached_matches or [], sort_keys=True
                )
                
                changed_match_ids = set()
                changed_tournament_ids = set()
                
                if matches_changed:
                    await redis_client.set(
                        "tournament_matches",
                        json.dumps(
                            {"data": matches, "last_updated": start_time.isoformat()},
                            ensure_ascii=False,
                        ),
                    )

                    def _group_by_tournament(data):
                        grouped = {}
                        for item in data:
                            tid = item.get("tournamentId")
                            if tid is None:
                                continue
                            grouped.setdefault(tid, []).append(item)
                        return grouped

                    def _group_by_match_id(data):
                        grouped = {}
                        for item in data:
                            mid = item.get("matchId")
                            if mid is None:
                                continue
                            grouped[mid] = item
                        return grouped

                    # Detect changed tournaments
                    new_group = _group_by_tournament(matches)
                    old_group = _group_by_tournament(cached_matches or [])
                    
                    for tid, new_list in new_group.items():
                        old_list = old_group.get(tid, [])
                        if json.dumps(new_list, sort_keys=True) != json.dumps(
                            old_list, sort_keys=True
                        ):
                            changed_tournament_ids.add(tid)
                    
                    for tid in old_group:
                        if tid not in new_group:
                            changed_tournament_ids.add(tid)
                    
                    # Detect changed individual matches
                    new_matches_by_id = _group_by_match_id(matches)
                    old_matches_by_id = _group_by_match_id(cached_matches or [])
                    
                    for match_id, new_match in new_matches_by_id.items():
                        old_match = old_matches_by_id.get(match_id, {})
                        if json.dumps(new_match, sort_keys=True) != json.dumps(
                            old_match, sort_keys=True
                        ):
                            changed_match_ids.add(match_id)
                    
                    for match_id in old_matches_by_id:
                        if match_id not in new_matches_by_id:
                            changed_match_ids.add(match_id)
                    
                    changes_detected["tournament_matches"] = len(changed_tournament_ids)
                    changes_detected["individual_matches"] = len(changed_match_ids)
                    PRECACHE_CHANGES_DETECTED.labels(category="tournament_matches").inc()
                    PRECACHE_CHANGES_DETECTED.labels(category="individual_matches").inc()
                    
                    logger.info(f"Matches changes detected: {len(changed_tournament_ids)} tournaments, {len(changed_match_ids)} individual matches")
                else:
                    changed_match_ids = set()
                    changed_tournament_ids = set()
                    logger.debug("No match changes detected")

                # -----------------------------------------------------------------
                # Unique team identifiers
                team_ids = set()
                for match in matches:
                    for key in ("hometeamId", "awayteamId"):
                        tid = match.get(key)
                        if tid is not None:
                            team_ids.add(tid)

                # Update metrics for teams processed
                PRECACHE_ITEMS_PROCESSED.labels(item_type="teams").set(len(team_ids))

                changed_team_ids = []
                if team_ids != set(cached_team_ids):
                    await redis_client.set(
                        "unique_team_ids",
                        json.dumps(
                            {"data": list(team_ids), "last_updated": start_time.isoformat()},
                            ensure_ascii=False,
                        ),
                    )
                    changed_team_ids = list(team_ids.symmetric_difference(set(cached_team_ids)))
                    changes_detected["unique_team_ids"] = len(changed_team_ids)
                    PRECACHE_CHANGES_DETECTED.labels(category="unique_team_ids").inc()
                    logger.info(f"Team changes detected: {len(changed_team_ids)} teams")

                # -----------------------------------------------------------------
                # Warm caches for changed tournaments, teams, and individual matches
                refresh_until = pendulum.now().add(days=7)
                ttl = 7 * 24 * 60 * 60

                # Set up cache entries for changed individual matches (limited to 10 for debugging)
                # limited_match_ids = list(changed_match_ids)[:10]
                # if len(changed_match_ids) > 10:
                #     logger.info(f"Limiting match cache setup to 10 entries (total changed: {len(changed_match_ids)})")
                
                # Filter matches to only include those within 2 weeks of reference date
                reference_date = pendulum.parse("2025-06-15T00:00:00")
                two_weeks = pendulum.duration(weeks=2)
                date_range_start = reference_date.subtract(weeks=2)
                date_range_end = reference_date.add(weeks=2)
                
                limited_match_ids = []
                for match_id in changed_match_ids:
                    # Find the match data to check its date
                    match_data = None
                    for match in matches:
                        if match.get("matchId") == match_id:
                            match_data = match
                            break
                    
                    if match_data and match_data.get("matchDate"):
                        try:
                            match_date = pendulum.parse(match_data["matchDate"])
                            if date_range_start <= match_date <= date_range_end:
                                limited_match_ids.append(match_id)
                        except Exception as e:
                            logger.debug(f"Failed to parse match date for match {match_id}: {e}")
                            # Include matches with unparseable dates to be safe
                            limited_match_ids.append(match_id)
                    else:
                        # Include matches without dates to be safe
                        limited_match_ids.append(match_id)
                
                if len(changed_match_ids) > 0:
                    logger.info(f"Filtered matches by date range: {len(limited_match_ids)}/{len(changed_match_ids)} matches within 1 weeks of {reference_date.format('YYYY-MM-DD')}")

                # Set up cache entries for all match-related endpoints:
                # /api/v1/ta/Match/?matchId=<id>
                # /api/v1/ta/MatchIncidents/?matchId=<id>
                # /api/v1/ta/MatchReferee?matchId=<id>
                # /api/v1/ta/MatchTeamMembers/<id>/?images=false

                match_endpoint_templates = [
                    f"{config.API_URL}/api/v1/ta/Match/",
                    f"{config.API_URL}/api/v1/ta/MatchIncidents/",
                    f"{config.API_URL}/api/v1/ta/MatchReferee",
                    f"{config.API_URL}/api/v1/ta/MatchTeamMembers/"
                ]

                for match_id in limited_match_ids:
                    for base_url in match_endpoint_templates:
                        try:
                            # Special handling for MatchTeamMembers endpoint
                            if "MatchTeamMembers" in base_url:
                                url = f"{config.API_URL}/api/v1/ta/MatchTeamMembers/{match_id}/"
                                cache_key = f"GET:{url}?images=false"
                                params = {"images": "false"}
                            else:
                                url = base_url
                                cache_key = f"GET:{url}?matchId={match_id}"
                                params = {"matchId": match_id}
                            
                            await cache_manager.setup_refresh(
                                cache_key, url, ttl, refresh_until, params=params
                            )
                            logger.debug(f"Added cache refresh for changed match {match_id}: {cache_key}")
                        except Exception as e:
                            logger.error(f"Error setting up cache for match {match_id} endpoint {base_url}: {e}")

                # Set up cache entries for changed teams
                for team_id in changed_team_ids:
                    try:
                        url = f"{config.API_URL}/api/v1/ta/Team"
                        cache_key = f"GET:{url}?teamId={team_id}"
                        await cache_manager.setup_refresh(
                            cache_key, url, ttl, refresh_until, params={"teamId": team_id}
                        )
                        logger.debug(f"Added cache refresh for changed team: {team_id}")
                    except Exception as e:
                        logger.error(f"Error setting up cache for team {team_id}: {e}")

                logger.info(f"Cache warming setup complete: {len(changed_match_ids)} matches, {len(changed_team_ids)} teams")

                # Record successful run
                PRECACHE_RUNS_TOTAL.labels(status="success").inc()
                PRECACHE_LAST_RUN_TIMESTAMP.set(start_time.timestamp())

                # Update cached data size metrics after potential data updates
                await _update_cached_data_size_metrics(redis_client)
                
                # Update valid seasons metrics after potential data updates
                await _update_valid_seasons_metrics(redis_client)
                
                # Update tournaments in season metrics after potential data updates
                await _update_tournaments_in_season_metrics(redis_client)

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
                # Record failed run
                PRECACHE_RUNS_TOTAL.labels(status="error").inc()
                logger.error(f"Error in detect_change_tournaments_and_matches: {e}")
                logger.exception(e)
            finally:
                if client:
                    await client.aclose()
                if redis_client:
                    await redis_client.close()

        await asyncio.sleep(180)
