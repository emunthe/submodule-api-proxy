# Copyright (C) 2025 Sasha Shipka <sasha.shipka@copyleft.no>
# You may use, distribute and modify this code under the terms of the GNU General Public License v3.0

from prometheus_client import Counter, Histogram, Gauge, Info
import time
from datetime import datetime

# Prometheus metrics
REQUEST_COUNT = Counter(
    "http_requests_total", "Total HTTP requests", ["method", "endpoint", "has_id"]
)
REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds", "HTTP request latency", ["method", "endpoint"]
)
CACHE_HITS = Counter("cache_hits_total", "Total cache hits", ["endpoint"])
CACHE_MISSES = Counter("cache_misses_total", "Total cache misses", ["endpoint"])
CACHE_REFRESH = Counter("cache_refresh_total", "Total cache refreshes", ["endpoint"])

# Time-based cache metrics for graphing over time
CACHE_REQUESTS_BY_TIME = Counter(
    "cache_requests_by_time_total", 
    "Total cache requests with time labels", 
    ["endpoint", "hour", "day_of_week", "date"]
)

# Cache hit and miss counters with time labels
CACHE_HITS_BY_TIME = Counter(
    "cache_hits_by_time_total",
    "Cache hits with time labels",
    ["endpoint", "hour", "day_of_week", "date"]
)

CACHE_MISSES_BY_TIME = Counter(
    "cache_misses_by_time_total", 
    "Cache misses with time labels",
    ["endpoint", "hour", "day_of_week", "date"]
)

CACHE_REFRESH_BY_TIME = Counter(
    "cache_refresh_by_time_total",
    "Cache refreshes with time labels", 
    ["endpoint", "hour", "day_of_week", "date"]
)

# Current cache size and status
CACHE_SIZE = Gauge("cache_size", "Number of items currently in cache")
CACHE_MEMORY_USAGE = Gauge("cache_memory_usage_bytes", "Cache memory usage in bytes")

# Request rate gauges (requests per second)
REQUEST_RATE = Gauge(
    "request_rate_per_second",
    "Current request rate per second",
    ["endpoint"]
)

# Helper function to get time labels for metrics
def get_time_labels():
    """Get current time labels for time-based metrics"""
    now = datetime.now()
    return {
        "hour": str(now.hour),
        "day_of_week": str(now.weekday()),  # 0=Monday, 6=Sunday
        "date": now.strftime("%Y-%m-%d")
    }

# Helper functions for updating metrics
def record_cache_request(endpoint, hit=True):
    """Record a cache request with time-based labels"""
    time_labels = get_time_labels()
    
    # Record the time-based counter for all requests
    CACHE_REQUESTS_BY_TIME.labels(
        endpoint=endpoint,
        hour=time_labels["hour"],
        day_of_week=time_labels["day_of_week"],
        date=time_labels["date"]
    ).inc()
    
    # Record hit or miss with time labels
    if hit:
        CACHE_HITS.labels(endpoint=endpoint).inc()
        CACHE_HITS_BY_TIME.labels(
            endpoint=endpoint,
            hour=time_labels["hour"],
            day_of_week=time_labels["day_of_week"],
            date=time_labels["date"]
        ).inc()
    else:
        CACHE_MISSES.labels(endpoint=endpoint).inc()
        CACHE_MISSES_BY_TIME.labels(
            endpoint=endpoint,
            hour=time_labels["hour"],
            day_of_week=time_labels["day_of_week"],
            date=time_labels["date"]
        ).inc()

def record_cache_refresh(endpoint):
    """Record a cache refresh with time-based labels"""
    time_labels = get_time_labels()
    
    # Record the original refresh counter
    CACHE_REFRESH.labels(endpoint=endpoint).inc()
    
    # Record refresh with time labels
    CACHE_REFRESH_BY_TIME.labels(
        endpoint=endpoint,
        hour=time_labels["hour"],
        day_of_week=time_labels["day_of_week"],
        date=time_labels["date"]
    ).inc()

# Remove the problematic update_cache_hit_ratio function for now
# We can calculate hit ratios in Grafana using the time-based counters
