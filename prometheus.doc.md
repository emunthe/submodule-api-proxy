# Prometheus Metrics Documentation

This document describes all Prometheus metrics exposed by the API Proxy service. These metrics provide comprehensive monitoring and observability for cache performance, request patterns, and system health.

## Table of Contents

1. [Core HTTP Metrics](#core-http-metrics)
2. [Basic Cache Metrics](#basic-cache-metrics)
3. [Time-Based Cache Metrics](#time-based-cache-metrics)
4. [Cache Size Metrics](#cache-size-metrics)
5. [Performance Metrics](#performance-metrics)
6. [Helper Functions](#helper-functions)
7. [Label Descriptions](#label-descriptions)
8. [Usage Examples](#usage-examples)

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

### Alerting Rules

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
```

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

1. **Overview Dashboard**: Current cache size, hit rate, request volume
2. **Performance Dashboard**: Request latency, endpoint performance, error rates
3. **Trends Dashboard**: Daily/weekly patterns, growth trends, seasonal analysis
4. **Detailed Analysis**: Per-endpoint metrics, cache efficiency, optimization opportunities

This comprehensive metrics collection enables deep insights into API proxy performance, cache effectiveness, and usage patterns for optimal system operation and troubleshooting.
