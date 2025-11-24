# Prometheus Metrics Documentation

This document describes all Prometheus metrics exposed by the API Proxy service. These metrics provide comprehensive monitoring and observability for cache performance, request patterns, and system health.

*Last updated: November 24, 2025*

## Table of Contents

1. [Core HTTP Metrics](#core-http-metrics)
2. [Basic Cache Metrics](#basic-cache-metrics)
3. [Time-Based Cache Metrics](#time-based-cache-metrics)
4. [Cache Size Metrics](#cache-size-metrics)
5. [Performance Metrics](#performance-metrics)
6. [Precache Metrics](#precache-metrics)
7. [Precache Data Management](#precache-data-management)
8. [Individual Season Tournament Caching](#individual-season-tournament-caching)
9. [Helper Functions](#helper-functions)
10. [Label Descriptions](#label-descriptions)
11. [Usage Examples](#usage-examples)

---

## Core HTTP Metrics

### `http_requests_total`

-   **Type**: Counter
-   **Description**: Total number of HTTP requests processed by the API proxy
-   **Labels**:
    -   `method`: HTTP method (GET, POST, PUT, DELETE, etc.)
    -   `endpoint`: The API endpoint being accessed
    -   `has_id`: Boolean indicating if the request includes an ID parameter
-   **Use Cases**:
    -   Track overall API usage
    -   Monitor endpoint popularity
    -   Identify traffic patterns by HTTP method

### `http_request_duration_seconds`

-   **Type**: Histogram
-   **Description**: HTTP request latency in seconds, including time to fetch from upstream API
-   **Labels**:
    -   `method`: HTTP method
    -   `endpoint`: The API endpoint
-   **Use Cases**:
    -   Monitor API response times
    -   Identify slow endpoints
    -   Set up SLA alerts
    -   Performance optimization

---

## Basic Cache Metrics

### `cache_hits_total`

-   **Type**: Counter
-   **Description**: Total number of successful cache hits (data served from cache)
-   **Labels**:
    -   `endpoint`: The API endpoint that was served from cache
-   **Use Cases**:
    -   Calculate cache hit ratios
    -   Monitor cache effectiveness
    -   Identify well-cached endpoints

### `cache_misses_total`

-   **Type**: Counter
-   **Description**: Total number of cache misses (data fetched from upstream API)
-   **Labels**:
    -   `endpoint`: The API endpoint that resulted in a cache miss
-   **Use Cases**:
    -   Calculate cache hit ratios
    -   Identify poorly cached endpoints
    -   Monitor cache warming effectiveness

### `cache_refresh_total`

-   **Type**: Counter
-   **Description**: Total number of cache refresh operations (background cache updates)
-   **Labels**:
    -   `endpoint`: The API endpoint being refreshed
-   **Use Cases**:
    -   Monitor cache refresh activity
    -   Track background cache warming
    -   Identify endpoints requiring frequent updates

---

## Time-Based Cache Metrics

These metrics include time labels for historical analysis and trend monitoring:

### `cache_requests_by_time_total`

-   **Type**: Counter
-   **Description**: Total cache requests with detailed time labels for trend analysis
-   **Labels**:
    -   `endpoint`: API endpoint
    -   `hour`: Hour of day (0-23)
    -   `day_of_week`: Day of week (0=Monday, 6=Sunday)
    -   `date`: Date in YYYY-MM-DD format
-   **Use Cases**:
    -   Analyze daily usage patterns
    -   Identify peak hours
    -   Weekly trend analysis
    -   Capacity planning

### `cache_hits_by_time_total`

-   **Type**: Counter
-   **Description**: Cache hits with time labels for detailed hit pattern analysis
-   **Labels**: Same as `cache_requests_by_time_total`
-   **Use Cases**:
    -   Track cache efficiency over time
    -   Identify optimal cache times
    -   Monitor hit rate trends

### `cache_misses_by_time_total`

-   **Type**: Counter
-   **Description**: Cache misses with time labels for miss pattern analysis
-   **Labels**: Same as `cache_requests_by_time_total`
-   **Use Cases**:
    -   Identify cache warming opportunities
    -   Track miss patterns
    -   Optimize cache strategies

### `cache_refresh_by_time_total`

-   **Type**: Counter
-   **Description**: Cache refreshes with time labels for refresh pattern analysis
-   **Labels**: Same as `cache_requests_by_time_total`
-   **Use Cases**:
    -   Monitor refresh frequency
    -   Optimize refresh scheduling
    -   Track background cache activity

---

## Cache Size Metrics

### `cache_size`

-   **Type**: Gauge
-   **Description**: Current number of items in cache (real-time cache size monitoring)
-   **Labels**: None
-   **Update Triggers**:
    -   Real-time updates on cache operations (cache_response, clear_cache, clear_all_cache)
    -   Background updates every 60 seconds
-   **Use Cases**:
    -   Real-time cache monitoring
    -   Alerting on cache size
    -   Memory management
    -   Live dashboard displays

### `cache_items`

-   **Type**: Gauge
-   **Description**: Number of cached items with time labels for historical analysis
-   **Labels**:
    -   `hour`: Hour of day (0-23)
    -   `day_of_week`: Day of week (0=Monday, 6=Sunday)  
    -   `date`: Date in YYYY-MM-DD format
-   **Use Cases**:
    -   Historical cache growth analysis
    -   Time-based cache size patterns
    -   Weekly/daily cache usage trends

### `cache_endpoints`

-   **Type**: Gauge
-   **Description**: Number of unique cached endpoints with time labels for endpoint diversity analysis
-   **Labels**:
    -   `hour`: Hour of day (0-23)
    -   `day_of_week`: Day of week (0=Monday, 6=Sunday)  
    -   `date`: Date in YYYY-MM-DD format
-   **Use Cases**:
    -   Track endpoint coverage diversity
    -   Monitor cache distribution across endpoints
    -   Analyze endpoint usage patterns over time

### `cache_memory_usage_bytes`

-   **Type**: Gauge
-   **Description**: Current cache memory usage in bytes
-   **Labels**: None
-   **Use Cases**:
    -   Memory monitoring
    -   Resource planning
    -   Performance optimization
-   **Note**: Metric is defined but not actively updated in current implementation

---

## Precache Metrics

The precache system periodically fetches season, tournament, match and team data, comparing it with cached versions to detect changes and trigger cache warming operations.

### `precache_runs_total`

-   **Type**: Counter
-   **Description**: Total number of precache periodic runs
-   **Labels**:
    -   `status`: Run outcome ("success" or "error")
-   **Use Cases**:
    -   Monitor precache system health
    -   Calculate success/failure rates
    -   Set up alerting for failed runs
    -   Track overall precache activity

### `precache_changes_detected_total`

-   **Type**: Counter
-   **Description**: Total number of changes detected by data category (cumulative)
-   **Labels**:
    -   `category`: Data type ("valid_seasons", "tournaments_in_season", "tournament_matches", "individual_matches", "unique_team_ids")
-   **Use Cases**:
    -   Monitor data freshness and change frequency over time
    -   Identify which data categories change most often historically
    -   Track cache warming trigger events cumulatively
    -   Analyze long-term data update patterns

### `precache_changes_this_run`

-   **Type**: Gauge
-   **Description**: Number of changes detected in this specific run by category (per-run snapshot)
-   **Labels**:
    -   `category`: Data type ("valid_seasons", "tournaments_in_season", "tournament_matches", "individual_matches", "unique_team_ids")
    -   `run_id`: Unique 8-character identifier for this precache run
-   **Use Cases**:
    -   Create time-series graphs showing changes per run over time
    -   Monitor current run activity and change volumes
    -   Identify runs with unusually high or low change activity
    -   Correlate change patterns with system events
    -   Track change distribution across data categories per run

### `precache_run_changes_summary`

-   **Type**: Gauge
-   **Description**: Summary of all changes detected in a specific run with run metadata for detailed analysis
-   **Labels**:
    -   `run_id`: Unique 8-character identifier for this precache run
    -   `run_timestamp`: ISO timestamp when the run started (for time-series sorting)
    -   `category`: Data type that changed
-   **Value**: Number of changes detected for this category in this run
-   **Special Values**:
    -   `>= 0`: Normal run, value indicates number of changes detected
    -   `-1`: Run failed/errored (maintains time-series continuity)
-   **Use Cases**:
    -   Detailed time-series analysis with precise timestamps
    -   Create comprehensive dashboards showing change trends over time
    -   Build alerts based on change volume thresholds per run
    -   Correlate changes with external events using timestamps
    -   Generate reports on data change patterns and frequencies
    -   Track system health through change detection patterns

### `precache_api_calls_total`

-   **Type**: Counter
-   **Description**: Total API calls made during precache operations
-   **Labels**:
    -   `call_type`: Type of API call ("seasons", "tournaments", "matches")
-   **Use Cases**:
    -   Monitor external API usage
    -   Track API rate limiting potential
    -   Analyze cost implications of API calls
    -   Optimize API call patterns

### `precache_duration_seconds`

-   **Type**: Histogram
-   **Description**: Time spent in precache operations
-   **Labels**: None
-   **Buckets**: Standard histogram buckets for timing analysis
-   **Use Cases**:
    -   Monitor precache performance
    -   Detect slow runs or performance degradation
    -   Set SLA targets for precache operations
    -   Optimize precache efficiency

### `precache_last_run_timestamp`

-   **Type**: Gauge
-   **Description**: Unix timestamp of the last precache run completion
-   **Labels**: None
-   **Use Cases**:
    -   Monitor if precache is running as expected
    -   Alert when precache hasn't run recently
    -   Track precache scheduling accuracy
    -   Detect system issues preventing precache execution
-   **Special Behaviors**:
    -   Set to 0 when precache data is manually cleared via `/precache` endpoint
    -   Updated to current timestamp on successful precache run completion

### `precache_items_processed`

-   **Type**: Gauge
-   **Description**: Number of items processed in the last precache run
-   **Labels**:
    -   `item_type`: Type of data processed ("seasons", "valid_seasons", "tournaments", "root_tournaments", "matches", "teams")
-   **Use Cases**:
    -   Monitor data volume trends
    -   Detect unusual data volume changes
    -   Capacity planning for data processing
    -   Analyze seasonal data patterns

### `precache_cached_data_size_bytes`

-   **Type**: Gauge
-   **Description**: Size in bytes of cached precache data by data type
-   **Labels**:
    -   `data_type`: Type of cached data ("valid_seasons", "tournaments_in_season", "root_tournaments", "tournament_matches", "unique_team_ids")
-   **Use Cases**:
    -   Monitor memory usage of cached precache data
    -   Track data growth over time
    -   Identify which data types consume most storage
    -   Capacity planning for cache storage
    -   Detect data bloat or unusual size changes
-   **Special Behaviors**:
    -   Automatically updated to 0 for all data types when precache data is cleared via `/precache` endpoint
    -   Updated after each successful precache run with actual data sizes

### `precache_api_call_success_rate`

-   **Type**: Gauge
-   **Description**: Success rate percentage of precache API calls by type
-   **Labels**:
    -   `call_type`: Type of API call ("seasons", "tournaments", "matches")
-   **Use Cases**:
    -   Monitor external API reliability
    -   Detect API endpoint issues
    -   Track overall precache health
    -   Set up alerting for low success rates
    -   Optimize retry strategies

### `precache_upstream_status`

-   **Type**: Gauge
-   **Description**: Status of upstream data.nif.no API health - tracks API availability and data quality
-   **Labels**:
    -   `endpoint`: Upstream API endpoint (currently "data.nif.no")
-   **Value**:
    -   `1` = UP (API responding with valid data)
    -   `0` = DOWN (API issues, empty responses, or errors)
-   **Update Triggers**:
    -   Set to DOWN on empty/null API responses
    -   Set to DOWN on HTTP errors (4xx/5xx status codes)
    -   Set to DOWN on network/connection errors
    -   Set to DOWN on JSON parsing errors
    -   Set to DOWN on critical precache failures
    -   Set to UP on successful data retrieval
    -   Confirmed UP at successful precache run completion
-   **Use Cases**:
    -   Monitor upstream API health and availability
    -   Create uptime dashboards and reports
    -   Set up alerts for upstream API issues
    -   Track correlation between upstream problems and cache misses
    -   SLA monitoring and reporting
    -   Incident detection and root cause analysis

### `precache_api_urls_called`

-   **Type**: Gauge
-   **Description**: URLs called to data.nif.no API during precache runs - tracks detailed API call information per run
-   **Labels**:
    -   `run_id`: Unique 8-character identifier for this precache run
    -   `url_path`: API path that was called (without base URL)
    -   `method`: HTTP method used (typically "GET")
    -   `params`: JSON string of parameters passed to the API call
-   **Value**: Always 1 (indicates the API call was made)
-   **Use Cases**:
    -   Track which specific API endpoints are called during precache
    -   Monitor API call patterns and frequency
    -   Debug precache API interactions
    -   Analyze parameter usage patterns
    -   Correlate API calls with changes detected

---

## Time-Series Change Tracking

The precache system now provides detailed per-run change tracking metrics that enable comprehensive time-series analysis of data changes. Each precache run (approximately every 3 minutes) generates a unique run ID and timestamp, allowing you to track exactly when and what changes occurred.

### Change Tracking Workflow

1. **Run Initialization**: Each precache run gets a unique 8-character `run_id` and ISO `run_timestamp`
2. **Change Detection**: Changes are detected by comparing fresh API data with cached versions
3. **Metric Recording**: Both cumulative totals and per-run snapshots are recorded
4. **Comprehensive Coverage**: All categories are recorded (including 0 changes) for consistent time-series data
5. **Error Handling**: Failed runs are marked with `-1` values to maintain timeline continuity

### Run Identification

-   **Run ID**: Short UUID (8 characters) like `a7b3c9d2` - unique identifier for each precache execution
-   **Run Timestamp**: ISO format timestamp like `2025-11-24T10:30:15.123456+00:00` - precise start time
-   **Run Duration**: Tracked via `precache_duration_seconds` histogram for performance monitoring
-   **Run Status**: Success/failure tracked via `precache_runs_total{status}` counter

### Data Categories Tracked

1. **`valid_seasons`**: Number of seasons that meet filtering criteria (2024+, exclude bandy bedrift)
2. **`tournaments_in_season`**: Number of tournaments that changed across all seasons
3. **`tournament_matches`**: Number of tournaments whose match data changed
4. **`individual_matches`**: Number of individual matches that changed (more granular than tournaments)
5. **`unique_team_ids`**: Number of unique team identifiers that changed

### Time-Series Graph Examples

**Example Grafana Queries for Time-Series Visualization:**

```promql
# Changes per run over time (all categories stacked)
precache_changes_this_run

# Changes per run by specific category
precache_changes_this_run{category="individual_matches"}

# Total changes per run (sum across all categories)
sum by (run_id, run_timestamp) (precache_run_changes_summary)

# Change rate over time (changes per minute)
rate(precache_changes_detected_total[5m])

# Average changes per run over 1 hour window
avg_over_time(sum by (run_id) (precache_changes_this_run)[1h:3m])

# Detect runs with high change activity (threshold: 100+ changes)
sum by (run_id, run_timestamp) (precache_run_changes_summary) > 100

# Track failed runs over time
precache_run_changes_summary == -1
```

**Recommended Grafana Dashboard Panels:**

1. **Change Trends Over Time**:

    - Query: `precache_changes_this_run`
    - Visualization: Time series, stacked area chart
    - Group by: `category`
    - Shows how different data types change over time

2. **Total Changes per Run**:

    - Query: `sum by (run_id) (precache_changes_this_run)`
    - Visualization: Time series, bars or lines
    - Shows overall system change activity

3. **Change Distribution by Category**:

    - Query: `sum by (category) (increase(precache_changes_detected_total[24h]))`
    - Visualization: Pie chart or bar chart
    - Shows which data types change most frequently

4. **Run Success vs Failure Timeline**:
    - Query: `precache_runs_total`
    - Visualization: State timeline
    - Group by: `status`
    - Shows precache system health over time

**Time-Series Alerting Examples:**

```yaml
# Alert on unusually high changes in a single run
- alert: HighChangesInRun
  expr: sum by (run_id) (precache_changes_this_run) > 500
  labels:
      severity: warning
  annotations:
      summary: 'Precache run {{ $labels.run_id }} detected {{ $value }} changes'

# Alert when no changes detected for extended period
- alert: NoChangesDetected
  expr: increase(precache_changes_detected_total[6h]) == 0
  labels:
      severity: info
  annotations:
      summary: 'No data changes detected in last 6 hours'

# Alert on failed runs
- alert: PrecacheRunFailed
  expr: precache_run_changes_summary == -1
  labels:
      severity: critical
  annotations:
      summary: 'Precache run {{ $labels.run_id }} failed at {{ $labels.run_timestamp }}'
```

---

## Precache Data Management

### Manual Precache Data Clearing

The API proxy provides an endpoint for manually clearing all precache data:

**Endpoint**: `DELETE /precache`

**Purpose**: Clear all precache data from Redis cache and reset related metrics

**Response**:

```json
{
    "status": "success",
    "message": "Cleared 8 precache data keys",
    "cleared_keys": ["valid_seasons", "tournaments_in_season", "tournament_matches", "unique_team_ids", "..."]
}
```

**Data Cleared**:

-   `valid_seasons` - Aggregate valid seasons data
-   `tournaments_in_season` - Aggregate tournament data from all seasons  
-   `root_tournaments` - Root tournament data
-   `tournament_matches` - Tournament match data
-   `unique_team_ids` - Unique team identifiers
-   `tournaments_season_{season_id}` - Individual season tournament cache entries (auto-discovered)

**Metric Effects**:

-   `precache_cached_data_size_bytes` → Set to 0 for all data types
-   `precache_last_run_timestamp` → Reset to 0
-   All other precache metrics remain unchanged (preserve historical data)

**Use Cases**:

-   Force complete precache rebuild
-   Clear corrupted or stale precache data
-   Reset precache state for testing
-   Manual intervention when automatic precache fails

### Precache Process Control

The API proxy provides endpoints for controlling the precache process:

**Endpoint**: `POST /start_precache`

**Purpose**: Start the precache periodic task

**Response**:

```json
{
    "status": "success",
    "message": "Precache task started successfully"
}
```

**Error Response**:

```json
{
    "status": "error", 
    "message": "A precache task is already running"
}
```

**Use Cases**:

-   Restart precache after manual stop
-   Start precache on system startup
-   Manual control of precache timing

**Endpoint**: `POST /stop_precache`

**Purpose**: Stop the running precache periodic task

**Response**:

```json
{
    "status": "success",
    "message": "Precache task has been stopped"
}
```

**Error Responses**:

```json
{
    "status": "error",
    "message": "No precache task is currently running"
}
```

```json
{
    "status": "error",
    "message": "Precache task is not running (already completed or cancelled)"
}
```

**Use Cases**:

-   Stop precache for maintenance
-   Manual control of system resources
-   Troubleshooting precache issues

---

## Individual Season Tournament Caching

### Overview

In addition to aggregate tournament data, the precache system creates individual cache entries for each season's tournament data. This provides granular access to tournament information while maintaining API compatibility.

### Cache Structure

**Aggregate Cache (existing)**:

-   **Key**: `tournaments_in_season`
-   **Contains**: All tournaments from all seasons combined
-   **Purpose**: Dashboard metrics and aggregate analysis

**Individual Season Caches (new)**:

-   **Key Pattern**: `tournaments_season_{season_id}`
-   **Contains**: Tournaments for that specific season only
-   **Purpose**: Direct API call replication and granular access

### Individual Cache Data Structure

Each individual season cache entry contains:

```json
{
    "tournamentsInSeason": [...],                   // Processed tournament list (matches API response field)
    "raw_response": {...},                          // Original API response
    "api_url": "/api/v1/ta/Tournament/Season/12345/", // API URL that was called
    "season_id": 12345,                            // Season identifier
    "last_updated": "2025-11-22T10:30:00Z"        // Timestamp when cached
}
```

### API Function for Individual Season Access

**Function**: `get_season_tournaments(season_id)`

**Purpose**: Retrieve cached tournament data for a specific season

**Parameters**:

-   `season_id`: The season identifier to retrieve tournaments for

**Note**: This function is available in the precache module but is not currently exposed as a REST API endpoint. It can be used internally within the application.

**Response Format**:

```json
{
    "status": "success",
    "data": {
        "tournamentsInSeason": [...],               // List of tournaments (matches API response)
        "raw_response": {...},                      // Original API response
        "api_url": "...",                          // The API URL that was called
        "season_id": "{season_id}",                // Season identifier
        "last_updated": "2025-11-22T10:30:00Z"
    },
    "cache_key": "tournaments_season_{season_id}",
    "source": "cache"
}
```

**Error Responses**:

```json
{
    "status": "not_found",
    "message": "No cached tournament data found for season {season_id}",
    "cache_key": "tournaments_season_{season_id}"
}
```

```json
{
    "status": "error",
    "message": "Invalid cached data for season {season_id}",
    "cache_key": "tournaments_season_{season_id}"
}
```

### Benefits

1. **Granular Access**: Retrieve tournament data for a specific season without loading all seasons
2. **API Compatibility**: Each cache entry corresponds exactly to one `/api/v1/ta/Tournament/Season/{season_id}/` API call
3. **Raw Response Preservation**: The original API response is preserved for exact replication
4. **Better Performance**: Smaller, targeted cache retrievals instead of large aggregate data

### Monitoring Individual Season Caches

**Logging**: The precache system logs creation of individual cache entries:

```
Created individual season cache entries: 15 entries
Season cache keys: ['tournaments_season_{season_id}', ...]
```

**Clearing**: Individual season caches are automatically discovered and cleared when using the `/precache` endpoint

### Usage Examples

**Direct Function Call**:

```python
# Get tournaments for a specific season
result = await get_season_tournaments(season_id={season_id})
if result["status"] == "success":
    tournament_data = result["data"]["tournamentsInSeason"]  # List of tournaments
    raw_api_response = result["data"]["raw_response"]        # Original API response
    api_url = result["data"]["api_url"]                      # The API URL that was called
```

**Monitoring Queries**:

```promql
# Total cached precache data size
sum(precache_cached_data_size_bytes)

# Largest cached data types
topk(5, precache_cached_data_size_bytes)
```

---

## Performance Metrics

### `request_rate_per_second`

-   **Type**: Gauge
-   **Description**: Current request rate per second by endpoint
-   **Labels**:
    -   `endpoint`: API endpoint
-   **Use Cases**:
    -   Real-time load monitoring
    -   Rate limiting decisions
    -   Performance alerts
-   **Note**: Metric is defined but not actively updated in current implementation

---

## Additional Precache Metrics

### `precache_valid_seasons_count`

-   **Type**: Gauge
-   **Description**: Total number of valid seasons currently cached
-   **Use Cases**:
    -   Monitor valid seasons data availability
    -   Track seasonal data changes
    -   Verify precache data completeness
    -   Set up alerts for missing season data

### `precache_valid_seasons_info`

-   **Type**: Gauge
-   **Description**: Information about valid seasons with season details as labels
-   **Labels**:
    -   `season_id`: Unique season identifier
    -   `season_name`: Season name (truncated to 50 characters)
    -   `season_year`: Year extracted from seasonDateFrom
    -   `sport_id`: Sport identifier (72=bandy, 151=innebandy)
    -   `sport_name`: Human-readable sport name
-   **Value**: Always 1 (indicates season exists)
-   **Use Cases**:
    -   Detailed season monitoring with rich metadata
    -   Sport-specific season tracking
    -   Season year distribution analysis
    -   Create season-specific alerts and dashboards

### `precache_tournaments_in_season_count`

-   **Type**: Gauge
-   **Description**: Total number of tournaments in season currently cached
-   **Use Cases**:
    -   Monitor tournament data volume
    -   Track tournament changes over time
    -   Verify precache data completeness
    -   Set up alerts for missing tournament data

### `precache_tournaments_in_season_info`

-   **Type**: Gauge
-   **Description**: Information about tournaments in season with tournament details as labels
-   **Labels**:
    -   `tournament_id`: Unique tournament identifier
    -   `tournament_name`: Tournament name (truncated to 50 characters)
    -   `season_id`: Associated season identifier
    -   `sport_id`: Sport identifier
    -   `is_root`: "true" for root tournaments, "false" for child tournaments
-   **Value**: Always 1 (indicates tournament exists)
-   **Use Cases**:
    -   Detailed tournament monitoring with rich metadata
    -   Track root vs child tournament distribution
    -   Season-tournament relationship analysis
    -   Sport-specific tournament tracking
    -   Create tournament hierarchy dashboards

**Example Queries:**

```promql
# Total valid seasons by sport
sum by (sport_name) (precache_valid_seasons_info)

# Total tournaments by sport
sum by (sport_id) (precache_tournaments_in_season_info)

# Root tournaments only
sum(precache_tournaments_in_season_info{is_root="true"})

# Tournaments per season
sum by (season_id) (precache_tournaments_in_season_info)

# Monitor data completeness
precache_valid_seasons_count > 0 and precache_tournaments_in_season_count > 0
```

---

## Helper Functions

### `get_time_labels()`

Returns a dictionary with current time labels:

```python
{
    "hour": "14",           # Current hour (0-23)
    "day_of_week": "1",     # Tuesday (0=Monday, 6=Sunday)
    "date": "2025-11-21"    # Current date (YYYY-MM-DD)
}
```

### `record_cache_request(endpoint, hit=True)`

**Purpose**: Record a cache request with full time-based labeling

**Parameters**:

-   `endpoint`: The API endpoint being accessed
-   `hit`: Boolean indicating if it was a cache hit (True) or miss (False)

**Actions**:

-   Increments `cache_requests_by_time_total`
-   Increments either `cache_hits_total` + `cache_hits_by_time_total` or `cache_misses_total` + `cache_misses_by_time_total`

### `record_cache_refresh(endpoint)`

**Purpose**: Record a cache refresh operation with time labels

**Parameters**:

-   `endpoint`: The API endpoint being refreshed

**Actions**:

-   Increments `cache_refresh_total`
-   Increments `cache_refresh_by_time_total` with time labels

### `update_cache_size_metrics(total_items, unique_endpoints)`

**Purpose**: Update cache size metrics with current counts and time labels

**Parameters**: 
-   `total_items`: Total number of cached items
-   `unique_endpoints`: Number of unique cached endpoints

**Actions**:
-   Updates `cache_size` metric with total items
-   Updates `cache_items` metric with time labels
-   Updates `cache_endpoints` metric with time labels

### `update_current_cache_size()`

**Purpose**: Efficiently update just the current cache size for real-time accuracy

**Parameters**: None (async function)

**Actions**:

-   Queries Redis for current cache keys count
-   Updates `cache_size` metric immediately
-   Used for real-time updates during cache operations
-   Graceful error handling if Redis unavailable

**Usage**: Called automatically in:

-   `cache_response()` - when new items are cached
-   `clear_cache()` - when individual cache entries are removed
-   `clear_all_cache()` - when all cache is cleared

---

## Label Descriptions

### Time Labels

-   **`hour`**: Hour of day as string ("0" to "23") - UTC timezone
-   **`day_of_week`**: Day of week as string ("0"=Monday to "6"=Sunday) - based on UTC date
-   **`date`**: Date in ISO format "YYYY-MM-DD" - UTC date

### Request Labels

-   **`method`**: HTTP method (GET, POST, PUT, DELETE, etc.)
-   **`endpoint`**: Full API endpoint path (e.g., "/api/v1/ta/tournaments/123")
-   **`has_id`**: String "true" or "false" indicating if endpoint contains an ID

### Precache Labels

-   **`status`**: Precache run outcome ("success" or "error")
-   **`category`**: Data category for change detection ("valid_seasons", "tournaments_in_season", "tournament_matches", "unique_team_ids")
-   **`call_type`**: Type of API call during precache ("seasons", "tournaments", "matches")
-   **`item_type`**: Type of data items processed ("seasons", "tournaments", "matches", "teams")
-   **`endpoint`**: Upstream API endpoint for status monitoring (currently "data.nif.no")
-   **`run_id`**: Unique 8-character identifier for precache runs
-   **`url_path`**: API path called during precache (without base URL)
-   **`method`**: HTTP method used for API calls (typically "GET")
-   **`params`**: JSON string of parameters passed to API calls
-   **`season_id`**: Unique season identifier
-   **`season_name`**: Season name (truncated to 50 characters)
-   **`season_year`**: Year extracted from seasonDateFrom
-   **`sport_id`**: Sport identifier (72=bandy, 151=innebandy)
-   **`sport_name`**: Human-readable sport name
-   **`tournament_id`**: Unique tournament identifier
-   **`tournament_name`**: Tournament name (truncated to 50 characters)
-   **`is_root`**: "true" for root tournaments, "false" for child tournaments

---

## Usage Examples

### Grafana Queries

**Cache Hit Rate Over Time:**

```promql
rate(cache_hits_by_time_total[5m]) /
(rate(cache_hits_by_time_total[5m]) + rate(cache_misses_by_time_total[5m])) * 100
```

**Top 5 Most Active Endpoints:**

```promql
topk(5, sum by (endpoint) (rate(cache_requests_by_time_total[1h])))
```

**Daily Request Volume:**

```promql
sum by (date) (increase(cache_requests_by_time_total[24h]))
```

**Weekly Pattern Analysis:**

```promql
sum by (day_of_week) (rate(cache_requests_by_time_total[7d]))
```

**Current Cache Size (Real-time):**

```promql
cache_size
```

**Cache Size Trends (Historical):**

```promql
cache_items
```

**Cache Endpoint Diversity (Historical):**

```promql
cache_endpoints
```

**Cache Growth Rate:**

```promql
rate(cache_size[5m])
```

**Request Latency by Endpoint:**

```promql
histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))
```

**Top 20 Most Active Endpoints (24h):**

```promql
topk(20, sum by (endpoint) (rate(cache_requests_by_time_total[24h])))
```

**Cache Efficiency by Endpoint:**

```promql
sum by (endpoint) (rate(cache_hits_total[5m])) /
sum by (endpoint) (rate(cache_hits_total[5m]) + rate(cache_misses_total[5m])) * 100
```

### Precache Queries

**Precache Success Rate:**

```promql
rate(precache_runs_total{status="success"}[5m]) / rate(precache_runs_total[5m]) * 100
```

**Time Since Last Precache Run:**

```promql
time() - precache_last_run_timestamp
```

**Changes Detected Over Time (Cumulative):**

```promql
increase(precache_changes_detected_total[1h])
```

**Changes Per Run (Time-Series):**

```promql
# All changes this run
precache_changes_this_run

# Changes by category this run
precache_changes_this_run{category="individual_matches"}

# Total changes per run over time
sum by (run_id) (precache_changes_this_run)

# Recent changes (last 3 hours)
precache_changes_this_run and on() (time() - on() group_left()
  label_replace(precache_run_changes_summary, "timestamp_seconds", "$1", "run_timestamp", "(.+)") < 10800)
```

**Run Analysis:**

```promql
# Average changes per run over time
avg_over_time(sum by (run_id) (precache_changes_this_run)[1h:3m])

# Maximum changes in any single run (last 24h)
max_over_time(sum by (run_id) (precache_changes_this_run)[24h:3m])

# Runs with zero changes (stable periods)
sum by (run_id) (precache_changes_this_run) == 0

# Most active data categories
topk(5, sum by (category) (precache_changes_this_run))
```

**Change Rate Analysis:**

```promql
# Rate of change detection (changes per minute)
rate(precache_changes_detected_total[5m])

# Change velocity by category
rate(precache_changes_detected_total[5m]) by (category)

# Daily change volume trends
increase(precache_changes_detected_total[24h])
```

**Precache API Call Rate:**

```promql
rate(precache_api_calls_total[5m])
```

**Precache Duration Percentiles:**

```promql
histogram_quantile(0.95, rate(precache_duration_seconds_bucket[5m]))
```

**Data Volume by Type:**

```promql
precache_items_processed
```

**Cached Data Size by Type:**

```promql
precache_cached_data_size_bytes
```

**Largest Cached Data Types:**

```promql
topk(5, precache_cached_data_size_bytes)
```

**Total Cached Data Size:**

```promql
sum(precache_cached_data_size_bytes)
```

**Precache API Success Rate:**

```promql
precache_api_call_success_rate
```

**API Success Rate Alert Condition:**

```promql
precache_api_call_success_rate < 90
```

**Upstream API Status:**

```promql
# Current upstream status (1=UP, 0=DOWN)
precache_upstream_status

# Upstream uptime percentage over time
avg_over_time(precache_upstream_status[1h]) * 100

# Time since upstream went down (seconds)
(time() - precache_upstream_status == 0) * on() group_left()
  (time() - on() max_over_time((precache_upstream_status == 0)[24h:1m]))

# Detect upstream status changes
changes(precache_upstream_status[1h])

# Upstream downtime duration
(1 - precache_upstream_status) * time()
```

**Data Changes Over Time (Including Clears):**

```promql
changes(precache_last_run_timestamp[1h])
```

**Time Since Precache Clear or Run:**

```promql
# Returns time since last precache activity (run or manual clear)
time() - on() group_left() (
  max(precache_last_run_timestamp) or
  vector(time() - 86400)  # Default to 24h ago if never run
)
```

**Most Active Data Categories (Changes):**

```promql
topk(5, increase(precache_changes_detected_total[24h]))
```

**Failed Run Detection:**

```promql
# Current failed runs
precache_run_changes_summary == -1

# Failed runs in last 24 hours
count_over_time((precache_run_changes_summary == -1)[24h:3m])
```

**Precache Performance Trends:**

````promql
rate(precache_duration_seconds_sum[5m]) / rate(precache_duration_seconds_count[5m])
```### Alerting Rules

**Low Cache Hit Rate:**

```yaml
- alert: LowCacheHitRate
  expr: |
      (
        rate(cache_hits_total[5m]) /
        (rate(cache_hits_total[5m]) + rate(cache_misses_total[5m]))
      ) * 100 < 70
  labels:
      severity: warning
````

**High Cache Size:**

```yaml
- alert: HighCacheSize
  expr: cache_size > 10000
  labels:
      severity: warning
```

**Slow Response Time:**

```yaml
- alert: SlowAPIResponse
  expr: histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m])) > 2
  labels:
      severity: critical
```

### Precache Alerting Rules

**Precache Not Running:**

```yaml
- alert: PrecacheNotRunning
  expr: time() - precache_last_run_timestamp > 300
  for: 5m
  labels:
      severity: warning
  annotations:
      summary: "Precache hasn't run recently"
      description: 'Precache last ran {{ $value }} seconds ago'
```

**High Precache Failure Rate:**

```yaml
- alert: PrecacheHighFailureRate
  expr: |
      (
        rate(precache_runs_total{status="error"}[10m]) / 
        rate(precache_runs_total[10m])
      ) * 100 > 50
  for: 5m
  labels:
      severity: critical
  annotations:
      summary: 'High precache failure rate detected'
      description: '{{ $value }}% of precache runs are failing'
```

**Precache Run Failed (Time-Series):**

```yaml
- alert: PrecacheRunFailed
  expr: precache_run_changes_summary == -1
  for: 0m
  labels:
      severity: critical
  annotations:
      summary: 'Precache run {{ $labels.run_id }} failed'
      description: 'Precache run {{ $labels.run_id }} failed at {{ $labels.run_timestamp }}'
```

**High Changes in Single Run:**

```yaml
- alert: HighChangesInSingleRun
  expr: sum by (run_id) (precache_changes_this_run) > 500
  for: 0m
  labels:
      severity: warning
  annotations:
      summary: 'Unusually high changes detected in precache run'
      description: 'Precache run {{ $labels.run_id }} detected {{ $value }} total changes'
```

**High Changes in Category:**

```yaml
- alert: HighCategoryChanges
  expr: precache_changes_this_run > 200
  for: 0m
  labels:
      severity: warning
  annotations:
      summary: 'High changes in {{ $labels.category }}'
      description: 'Run {{ $labels.run_id }} detected {{ $value }} changes in {{ $labels.category }}'
```

**No Changes Detected (Extended Period):**

```yaml
- alert: NoChangesDetectedExtended
  expr: increase(precache_changes_detected_total[6h]) == 0
  for: 1h
  labels:
      severity: info
  annotations:
      summary: 'No data changes detected for extended period'
      description: 'No changes detected in any category for over 6 hours'
```

**Change Rate Anomaly:**

```yaml
- alert: ChangeRateAnomaly
  expr: |
      abs(
        rate(precache_changes_detected_total[5m]) - 
        rate(precache_changes_detected_total[1h] offset 1h)
      ) > 0.5
  for: 10m
  labels:
      severity: info
  annotations:
      summary: 'Unusual change rate detected'
      description: 'Change detection rate differs significantly from 1 hour ago'
```

**Slow Precache Performance:**

```yaml
- alert: SlowPrecachePerformance
  expr: histogram_quantile(0.95, rate(precache_duration_seconds_bucket[5m])) > 120
  for: 5m
  labels:
      severity: warning
  annotations:
      summary: 'Precache operations are slow'
      description: '95th percentile precache duration is {{ $value }} seconds'
```

**Large Cached Data Size:**

```yaml
- alert: LargeCachedDataSize
  expr: precache_cached_data_size_bytes > 10000000 # 10MB
  for: 10m
  labels:
      severity: warning
  annotations:
      summary: 'Cached data size is large'
      description: 'Cached data type {{ $labels.data_type }} is {{ $value | humanize1024 }}B in size'
```

**Excessive Total Cached Data Size:**

```yaml
- alert: ExcessiveTotalCachedDataSize
  expr: sum(precache_cached_data_size_bytes) > 50000000 # 50MB
  for: 10m
  labels:
      severity: critical
  annotations:
      summary: 'Total cached data size is excessive'
      description: 'Total cached data size is {{ $value | humanize1024 }}B'
```

**Low API Success Rate:**

```yaml
- alert: LowPrecacheAPISuccessRate
  expr: precache_api_call_success_rate < 90
  for: 5m
  labels:
      severity: warning
  annotations:
      summary: 'Low precache API success rate'
      description: 'Precache {{ $labels.call_type }} API calls have {{ $value }}% success rate'
```

**Precache Data Manually Cleared:**

```yaml
- alert: PrecacheDataCleared
  expr: precache_last_run_timestamp == 0 and changes(precache_last_run_timestamp[5m]) != 0
  labels:
      severity: info
  annotations:
      summary: 'Precache data was manually cleared'
      description: 'Precache last run timestamp was reset to 0, indicating manual data clearing'
```

**Multiple Failed Runs:**

```yaml
- alert: MultiplePrecacheFailures
  expr: count_over_time((precache_run_changes_summary == -1)[30m:3m]) > 3
  for: 0m
  labels:
      severity: critical
  annotations:
      summary: 'Multiple precache runs have failed recently'
      description: '{{ $value }} precache runs failed in the last 30 minutes'
```

**Upstream API Down:**

```yaml
- alert: UpstreamAPIDown
  expr: precache_upstream_status == 0
  for: 2m
  labels:
      severity: critical
  annotations:
      summary: 'Upstream data.nif.no API is DOWN'
      description: 'Upstream API {{ $labels.endpoint }} has been marked as DOWN for {{ $for }}'
```

**Upstream API Flapping:**

```yaml
- alert: UpstreamAPIFlapping
  expr: changes(precache_upstream_status[10m]) > 4
  for: 0m
  labels:
      severity: warning
  annotations:
      summary: 'Upstream API status is flapping'
      description: 'Upstream API {{ $labels.endpoint }} status changed {{ $value }} times in 10 minutes'
```

**Low Upstream Uptime:**

```yaml
- alert: LowUpstreamUptime
  expr: avg_over_time(precache_upstream_status[1h]) * 100 < 95
  for: 5m
  labels:
      severity: warning
  annotations:
      summary: 'Low upstream API uptime'
      description: 'Upstream API {{ $labels.endpoint }} uptime is {{ $value | printf "%.1f" }}% over last hour'
```

---

## Metric Collection Frequency

-   **Time-based metrics**: Updated on every request with current time labels
-   **Cache size metrics**:
    -   Real-time updates: Immediately when cache operations occur (cache_response, clear_cache, clear_all_cache)
    -   Background updates: Every 60 seconds via background task for comprehensive metrics
-   **Basic counters**: Updated in real-time on each request
-   **Request latency**: Measured for each request

## Cache Operations Integration

### Real-Time Cache Size Updates

The `cache_size` metric is updated immediately when cache operations occur:

**Cache Response Operations** (`cache_response()`):

1. Store response in Redis cache
2. Call `update_current_cache_size()` for immediate cache size update

**Cache Clearing Operations** (`clear_cache()`, `clear_all_cache()`):

1. Remove entries from Redis cache
2. Call `update_current_cache_size()` for immediate cache size update

**Background Sync**:

-   Periodic 60-second updates via `update_cache_size_metrics()`
-   Updates both current metrics and time-based historical metrics
-   Provides fallback accuracy if real-time updates fail

This dual approach ensures both real-time accuracy for live monitoring and comprehensive historical data for trend analysis.

## Dashboard Recommendations

1. **Overview Dashboard**: Current cache size, hit rate, request volume, precache health, upstream status
2. **Performance Dashboard**: Request latency, endpoint performance, error rates, precache duration
3. **Trends Dashboard**: Daily/weekly patterns, growth trends, seasonal analysis, data change patterns
4. **Detailed Analysis**: Per-endpoint metrics, cache efficiency, optimization opportunities
5. **Precache Monitoring**: Precache success rates, data freshness, API usage, change detection patterns
6. **Time-Series Change Tracking**: Per-run change analysis, change trends over time, anomaly detection
7. **Upstream Monitoring**: API uptime tracking, service health correlation, incident analysis

### Suggested Dashboard Panels

**Main Overview:**

-   Current cache size (using `cache_size` metric)
-   Precache last run timestamp and success rate
-   Request volume and error rates
-   Active endpoints count
-   **Upstream API status indicator (UP/DOWN)**

**Precache Health:**

-   Precache runs over time (success/failure)
-   Time since last run
-   Changes detected by category
-   API calls by type
-   **Upstream API status timeline**
-   Precache duration trends
-   Cached data size monitoring

**Time-Series Change Analysis (New):**

-   **Changes Over Time**: `precache_changes_this_run` stacked area chart by category
-   **Run Success Timeline**: `precache_runs_total` with run status over time
-   **Change Volume per Run**: `sum by (run_id) (precache_changes_this_run)` bar chart
-   **Category Activity**: `topk(5, sum by (category) (precache_changes_this_run))` pie chart
-   **Failed Runs**: `precache_run_changes_summary == -1` state timeline
-   **Change Rate Trends**: `rate(precache_changes_detected_total[5m])` line chart
-   **Run Performance**: `precache_duration_seconds` vs total changes correlation

**Change Detection Insights:**

-   **Daily Change Patterns**: Changes by hour of day and day of week
-   **Change Distribution**: Percentage of runs with 0, low, medium, high changes
-   **Anomaly Detection**: Runs with unusually high change volumes
-   **Stability Periods**: Time ranges with consistently low changes
-   **Category Comparison**: Which data types change most/least frequently

**Performance Monitoring:**

-   Request latency percentiles
-   Cache hit rate trends
-   Precache performance metrics
-   Error rate by endpoint

**Data Insights:**

-   Items processed by type
-   Change detection patterns
-   Weekly/daily usage patterns
-   Top active endpoints

**Real-Time Monitoring:**

-   Current run status and progress
-   Live change detection as it happens
-   System health indicators
-   Alert status dashboard

**Upstream API Monitoring:**

-   **Current Status Indicator**: Single stat panel showing `precache_upstream_status` (1=UP/Green, 0=DOWN/Red)
-   **Uptime Percentage**: `avg_over_time(precache_upstream_status[24h]) * 100` over time
-   **Status Timeline**: State timeline showing UP/DOWN periods over time
-   **Status Change Events**: `changes(precache_upstream_status[6h])` to track instability
-   **Correlation Analysis**: Overlay upstream status with precache failures and cache miss rates
-   **SLA Tracking**: Monthly/weekly uptime percentages and downtime duration

### Grafana Dashboard JSON Examples

**Upstream Status Panel Configuration:**

```json
{
    "title": "Upstream API Status",
    "type": "stat",
    "targets": [
        {
            "expr": "precache_upstream_status",
            "legendFormat": "{{ endpoint }}"
        }
    ],
    "fieldConfig": {
        "defaults": {
            "color": {
                "mode": "thresholds"
            },
            "thresholds": {
                "steps": [
                    { "color": "red", "value": 0 },
                    { "color": "green", "value": 1 }
                ]
            },
            "mappings": [
                { "options": { "0": { "text": "DOWN" } }, "type": "value" },
                { "options": { "1": { "text": "UP" } }, "type": "value" }
            ]
        }
    }
}
```

**Uptime Timeline Panel Configuration:**

```json
{
    "title": "Upstream API Status Timeline",
    "type": "state-timeline",
    "targets": [
        {
            "expr": "precache_upstream_status",
            "legendFormat": "{{ endpoint }}"
        }
    ],
    "fieldConfig": {
        "defaults": {
            "color": {
                "mode": "thresholds"
            },
            "thresholds": {
                "steps": [
                    { "color": "red", "value": 0 },
                    { "color": "green", "value": 1 }
                ]
            }
        }
    }
}
```

**Time-Series Change Panel Configuration:**

```json
{
    "title": "Changes Per Run Over Time",
    "type": "timeseries",
    "targets": [
        {
            "expr": "precache_changes_this_run",
            "legendFormat": "{{ category }}"
        }
    ],
    "fieldConfig": {
        "defaults": {
            "custom": {
                "drawStyle": "line",
                "lineInterpolation": "stepAfter",
                "fillOpacity": 10,
                "stacking": {
                    "mode": "normal"
                }
            }
        }
    }
}
```

**Run Status Panel Configuration:**

```json
{
    "title": "Precache Run Status",
    "type": "state-timeline",
    "targets": [
        {
            "expr": "precache_runs_total",
            "legendFormat": "{{ status }}"
        }
    ],
    "options": {
        "mergeValues": false,
        "showValue": "auto",
        "alignValue": "auto"
    }
}
```

This comprehensive metrics collection enables deep insights into API proxy performance, cache effectiveness, usage patterns, and detailed time-series analysis of data changes for optimal system operation and troubleshooting.
