# Precache Module Documentation

## Overview

The `precache.py` module implements a periodic data synchronization system that fetches sports data from the data.nif.no API, detects changes, and maintains a Redis cache for improved performance. It runs continuously in the background, checking for updates every 3 minutes.

## Architecture

The module follows a **separation of concerns** pattern with three main functional components:

### 1. Data Fetching: `pre_cache_getter()`
**Purpose**: Fetches fresh data from upstream API without creating cache entries.

**What it does**:
- Authenticates with data.nif.no API using token manager
- Fetches seasons for configured sports (Bandy=72, Innebandy=151) from 2024 onwards
- Fetches tournaments for each valid season with hierarchy
- Fetches matches only for root tournaments (tournaments without parent)
- Extracts unique team IDs from match data
- Tracks all API calls with URLs, parameters, status codes, and timestamps
- Logs API calls to `logs/precache_debug_logs/precache_run_calls.json`

**Important**: This function **does not store data** anywhere. It only returns the fresh data in memory as its return value. The data is passed to `compare_and_update_cache()` for comparison against already-stored data.

**Returns**: Dictionary containing:
- `run_id`: Unique identifier for this run
- `timestamp`: ISO format timestamp
- `api_calls`: Count of API calls by type (seasons, tournaments, matches)
- `data`: Object containing valid_seasons, tournaments_in_season, tournament_matches, unique_team_ids

### 2. Change Detection & Cache Update: `compare_and_update_cache()`
**Purpose**: Compares fresh data with cached data, detects changes, and updates Redis cache.

**What it does**:
- Receives fresh data from `pre_cache_getter()` via the `fresh_data_result` parameter
- Loads existing cached data from Redis using `_get_cached()` helper:
  - `valid_seasons` from Redis key "valid_seasons"
  - `tournaments_in_season` from Redis key "tournaments_in_season"
  - `tournament_matches` from Redis key "tournament_matches"
  - `unique_team_ids` from Redis key "unique_team_ids"
- Compares fresh data with cached data using JSON serialization
- Detects changes in:
  - Valid seasons
  - Tournaments in season
  - Tournament matches
  - Individual matches
  - Unique team IDs
- Updates Redis cache when changes are detected
- Sets up cache warming for changed items (matches, teams)
- Updates Prometheus metrics
- Logs cache data to debug files in `logs/precache_debug_logs/`

**Change Detection Logic**:
- **Seasons**: Full JSON comparison
- **Tournaments**: Symmetric difference of tournament IDs
- **Matches**: Comparison by tournament ID and individual match ID
- **Teams**: Set comparison of team IDs

**Cache Warming**: For changed items, sets up automatic cache refresh for related endpoints:
- Match endpoints: `/api/v1/ta/match/`, `/api/v1/ta/matchincidents/`, `/api/v1/ta/matchreferee`, `/api/v1/ta/matchteammembers/`
- Team endpoints: `/api/v1/ta/Team`
- Only matches within 2 weeks of reference date (2025-06-15) are warmed
- TTL: 7 days

**Returns**: Dictionary containing:
- `changes_detected`: Changes by category
- `changed_tournament_ids`: Set of tournament IDs that changed
- `changed_team_ids`: List of team IDs that changed
- `changed_match_ids`: Set of match IDs that changed
- `api_calls`: API call statistics
- `upstream_status`: Status of upstream API ("UP" or "DOWN")

### 3. Orchestration: `detect_change_tournaments_and_matches()`
**Purpose**: Main loop that orchestrates the precache process.

**What it does**:
- Runs in an infinite loop with 180-second intervals
- Step 1: Calls `pre_cache_getter()` to fetch fresh data
- Step 2: Calls `compare_and_update_cache()` to detect changes and update cache
- Records run metrics (duration, success/failure, changes detected)
- Saves run statistics to `logs/precache_debug_logs/precache_run_stats.json`
- Maintains history of last 10 runs in `precache_run_history.json`
- Sets upstream status metrics for monitoring
- Handles errors gracefully with comprehensive logging

## Redis Cache Structure

### Cache Keys:
- `valid_seasons`: Array of season objects from 2024+
- `tournaments_in_season`: Array of all tournaments
- `root_tournaments`: Array of tournaments without parentTournamentId
- `tournament_matches`: Array of all match data
- `unique_team_ids`: Array of unique team IDs
- `tournaments_season_{season_id}`: Per-season tournament cache (future use)

### Cache Entry Format:
```json
{
  "data": [...],
  "last_updated": "2025-11-25T12:34:56+00:00"
}
```

## Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│  detect_change_tournaments_and_matches() - Main Loop        │
│  (Runs every 180 seconds)                                   │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 1: pre_cache_getter()                                 │
│  • Fetch seasons from data.nif.no API                       │
│  • Fetch tournaments for each season                        │
│  • Fetch matches for root tournaments                       │
│  • Extract team IDs                                         │
│  • Track all API calls                                      │
│  • Return fresh data                                        │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 2: compare_and_update_cache()                         │
│  • Load cached data from Redis                              │
│  • Compare fresh vs cached data                             │
│  • Detect changes                                           │
│  • Update Redis cache                                       │
│  • Set up cache warming for changed items                   │
│  • Update Prometheus metrics                                │
│  • Log debug information                                    │
└─────────────────────────────────────────────────────────────┘
```

## Prometheus Metrics

### Counters (cumulative):
- `precache_runs_total{status}`: Total runs (success/error)
- `precache_changes_detected_total{category}`: Total changes detected
- `precache_api_calls_total{call_type}`: Total API calls made

### Gauges (current state):
- `precache_changes_this_run{category, run_id}`: Changes in specific run
- `precache_run_changes_summary{run_id, run_timestamp, category}`: Run summary
- `precache_last_run_timestamp`: Last successful run timestamp
- `precache_items_processed{item_type}`: Items processed in last run
- `precache_cached_data_size_bytes{data_type}`: Size of cached data
- `precache_valid_seasons_count`: Number of valid seasons
- `precache_valid_seasons_info{season_id, season_name, season_year, sport_id, sport_name}`: Season details
- `precache_tournaments_in_season_count`: Number of tournaments
- `precache_tournaments_in_season_info{tournament_id, tournament_name, season_id, sport_id, is_root}`: Tournament details
- `precache_api_urls_called{run_id, url_path, method, params}`: Detailed API call tracking
- `precache_upstream_status{endpoint}`: API health (1=UP, 0=DOWN)

### Histograms:
- `precache_duration_seconds`: Run duration distribution

## Debug Logging

### Log Files (in `logs/precache_debug_logs/`):

1. **API Call Tracking**:
   - `precache_run_calls.json`: All API calls with URLs, parameters, status codes, timestamps

2. **Run Statistics**:
   - `precache_run_stats.json`: Latest run statistics
   - `precache_run_history.json`: Last 10 runs history

3. **Cache Data Snapshots** (all with `_latest.json` suffix):
   - `valid_seasons_latest.json`: Cached seasons data
   - `tournaments_in_season_latest.json`: Cached tournaments data
   - `tournament_matches_latest.json`: Cached matches data
   - `unique_team_ids_latest.json`: Cached team IDs
   - `valid_seasons_fetched_latest.json`: Fresh seasons data from API
   - `tournaments_in_season_fetched_latest.json`: Fresh tournaments from API
   - `root_tournaments_fetched_latest.json`: Fresh root tournaments
   - `tournament_matches_fetched_latest.json`: Fresh matches from API
   - `unique_team_ids_fetched_latest.json`: Fresh team IDs from API

## Helper Functions

### Module-Level Helpers:
- `_log_cache_data_to_file(cache_name, data, run_id)`: Logs cache data to debug files
- `_group_by_tournament(data)`: Groups match data by tournament ID
- `_group_by_match_id(data)`: Groups match data by match ID

### Metrics Update Functions:
- `_update_cached_data_size_metrics(redis_client)`: Updates cache size metrics
- `_update_valid_seasons_metrics(redis_client)`: Updates season metrics with labels
- `_update_tournaments_in_season_metrics(redis_client)`: Updates tournament metrics with labels

## Utility Functions

### `clear_precache_data()`
Clears all precache data from Redis including:
- Valid seasons
- Tournaments in season
- Root tournaments
- Tournament matches
- Unique team IDs
- Per-season tournament caches

Updates metrics to reflect cleared state.

### `get_season_tournaments(season_id)`
Retrieves cached tournament data for a specific season from Redis.

Returns dictionary with status, data, cache_key, and source.

## Configuration

### Sports Configuration:
- Sport IDs: `[72, 151]` (Bandy, Innebandy)
- Year range: 2024 to current year

### Timing:
- Run interval: 180 seconds (3 minutes)
- Cache TTL: 7 days
- Cache refresh until: 7 days from run time

### Date Filtering:
- Match date filtering: ±2 weeks from reference date (2025-06-15)
- Only matches within this range are warmed in cache

### API Endpoints:
- Base URL: `config.API_URL`
- Seasons: `/api/v1/ta/Seasons/`
- Tournaments: `/api/v1/ta/Tournament/Season/{season_id}/`
- Matches: `/api/v1/ta/TournamentMatches`

## Error Handling

- All API call failures are logged with status codes
- Failed runs set `upstream_status` to "DOWN"
- Prometheus metrics record error state with -1 values
- Error details saved to debug logs
- Loop continues after errors with 180-second wait

## Performance Optimizations

1. **Root Tournament Filtering**: Only fetches matches for root tournaments (not child tournaments), significantly reducing API calls
2. **Parallel Fetching**: Uses `asyncio.gather()` for concurrent API requests
3. **Date Range Filtering**: Only warms cache for matches within 2-week window
4. **Change Detection**: Only updates cache and warms entries when changes detected
5. **Efficient Comparisons**: Uses JSON serialization for quick data comparison

## Dependencies

- `asyncio`: Asynchronous I/O operations
- `pendulum`: DateTime parsing and manipulation
- `prometheus_client`: Metrics collection
- `redis`: Cache storage
- HTTP client from `util.py`
- Token manager from `token.py`
- Logger from `util.py`
- Config from `config.py`

## Usage

The precache module is started automatically by the main application. It runs as a background task:

```python
from .precache import detect_change_tournaments_and_matches
from .cache import CacheManager
from .token import TokenManager

cache_manager = CacheManager()
token_manager = TokenManager()

# Start precache loop
asyncio.create_task(
    detect_change_tournaments_and_matches(cache_manager, token_manager)
)
```

## Monitoring

Monitor the precache system using:

1. **Prometheus Metrics**: Query metrics endpoint for detailed statistics
2. **Log Files**: Check `logs/precache_debug_logs/` for detailed run information
3. **Redis**: Inspect cache keys directly for data verification
4. **Upstream Status**: Monitor `precache_upstream_status` metric for API health

## Future Enhancements

- Per-season tournament cache utilization (`tournaments_season_{season_id}`)
- Configurable date range filtering
- Dynamic sport ID configuration
- API rate limiting and throttling
- Retry logic for failed API calls
- More granular cache warming strategies
