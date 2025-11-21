# Prometheus Metrics Documentation

This document describes all Prometheus metrics exposed by the API Proxy service. These metrics provide comprehensive monitoring and observability for cache performance, request patterns, and system health.

## Table of Contents

1. [Core HTTP Metrics](#core-http-metrics)
2. [Basic Cache Metrics](#basic-cache-metrics)
3. [Time-Based Cache Metrics](#time-based-cache-metrics)
4. [Cache Size Metrics](#cache-size-metrics)
5. [Performance Metrics](#performance-metrics)
6. [Precache Metrics](#precache-metrics)
7. [Helper Functions](#helper-functions)
8. [Label Descriptions](#label-descriptions)
9. [Usage Examples](#usage-examples)

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

### `cache_items`

-   **Type**: Gauge
-   **Description**: Number of cached items at specific times (snapshot metric)
-   **Labels**:
    -   `hour`: Hour of day (0-23)
    -   `day_of_week`: Day of week (0=Monday, 6=Sunday)
    -   `date`: Date in YYYY-MM-DD format
-   **Use Cases**:
    -   Monitor cache growth over time
    -   Track cache size patterns
    -   Capacity planning
    -   Memory usage optimization

### `cache_endpoints`

-   **Type**: Gauge
-   **Description**: Number of unique cached endpoints at specific times
-   **Labels**: Same as `cache_items`
-   **Use Cases**:
    -   Monitor cache diversity
    -   Track endpoint coverage
    -   Analyze caching patterns

### `cache_size`

-   **Type**: Gauge
-   **Description**: Current number of items in cache (real-time metric with live updates)
-   **Labels**: None
-   **Update Triggers**:
    -   Real-time updates on cache operations (cache_response, clear_cache, clear_all_cache)
    -   Background updates every 60 seconds
-   **Use Cases**:
    -   Real-time cache monitoring
    -   Alerting on cache size
    -   Memory management
    -   Live dashboard displays

### `cache_memory_usage_bytes`

-   **Type**: Gauge
-   **Description**: Current cache memory usage in bytes
-   **Labels**: None
-   **Use Cases**:
    -   Memory monitoring
    -   Resource planning
    -   Performance optimization
-   **Note**: Currently defined but not actively updated

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
-   **Description**: Total number of changes detected by data category
-   **Labels**:
    -   `category`: Data type ("seasons", "tournaments_in_season", "tournament_matches", "unique_team_ids")
-   **Use Cases**:
    -   Monitor data freshness and change frequency
    -   Identify which data categories change most often
    -   Track cache warming trigger events
    -   Analyze data update patterns

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

### `precache_items_processed`

-   **Type**: Gauge
-   **Description**: Number of items processed in the last precache run
-   **Labels**:
    -   `item_type`: Type of data processed ("seasons", "tournaments", "matches", "teams")
-   **Use Cases**:
    -   Monitor data volume trends
    -   Detect unusual data volume changes
    -   Capacity planning for data processing
    -   Analyze seasonal data patterns

### `precache_cached_data_size_bytes`

-   **Type**: Gauge
-   **Description**: Size in bytes of cached precache data by data type
-   **Labels**:
    -   `data_type`: Type of cached data ("valid_seasons", "tournaments_in_season", "tournament_matches", "unique_team_ids")
-   **Use Cases**:
    -   Monitor memory usage of cached precache data
    -   Track data growth over time
    -   Identify which data types consume most storage
    -   Capacity planning for cache storage
    -   Detect data bloat or unusual size changes

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
-   **Note**: Currently defined but not actively updated

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

**Purpose**: Update cache size metrics with current state and time labels

**Parameters**:

-   `total_items`: Total number of cached items
-   `unique_endpoints`: Number of unique endpoints in cache

**Actions**:

-   Updates `cache_size`
-   Updates `cache_items` with time labels
-   Updates `cache_endpoints` with time labels

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

-   **`hour`**: Hour of day as string ("0" to "23")
-   **`day_of_week`**: Day of week as string ("0"=Monday to "6"=Sunday)
-   **`date`**: Date in ISO format "YYYY-MM-DD"

### Request Labels

-   **`method`**: HTTP method (GET, POST, PUT, DELETE, etc.)
-   **`endpoint`**: Full API endpoint path (e.g., "/api/v1/ta/tournaments/123")
-   **`has_id`**: String "true" or "false" indicating if endpoint contains an ID

### Precache Labels

-   **`status`**: Precache run outcome ("success" or "error")
-   **`category`**: Data category for change detection ("seasons", "tournaments_in_season", "tournament_matches", "unique_team_ids")
-   **`call_type`**: Type of API call during precache ("seasons", "tournaments", "matches")
-   **`item_type`**: Type of data items processed ("seasons", "tournaments", "matches", "teams")

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

**Cache Size Trend:**

```promql
cache_items
```

**Real-Time Current Cache Size:**

```promql
cache_size
```

**Cache Growth Rate:**

```promql
rate(cache_items[5m])
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

**Changes Detected Over Time:**

```promql
increase(precache_changes_detected_total[1h])
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

**Most Active Data Categories (Changes):**

```promql
topk(5, increase(precache_changes_detected_total[24h]))
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

**No Data Changes Detected:**

```yaml
- alert: NoDataChangesDetected
  expr: increase(precache_changes_detected_total[24h]) == 0
  labels:
      severity: info
  annotations:
      summary: 'No data changes detected in 24 hours'
      description: "Precache hasn't detected any data changes in the last 24 hours"
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

1. **Overview Dashboard**: Current cache size, hit rate, request volume, precache health
2. **Performance Dashboard**: Request latency, endpoint performance, error rates, precache duration
3. **Trends Dashboard**: Daily/weekly patterns, growth trends, seasonal analysis, data change patterns
4. **Detailed Analysis**: Per-endpoint metrics, cache efficiency, optimization opportunities
5. **Precache Monitoring**: Precache success rates, data freshness, API usage, change detection patterns

### Suggested Dashboard Panels

**Main Overview:**

-   Current cache size and hit rate
-   Precache last run timestamp and success rate
-   Request volume and error rates
-   Active endpoints count

**Precache Health:**

-   Precache runs over time (success/failure)
-   Time since last run
-   Changes detected by category
-   API calls by type
-   Precache duration trends
-   Cached data size by type

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

This comprehensive metrics collection enables deep insights into API proxy performance, cache effectiveness, and usage patterns for optimal system operation and troubleshooting.
