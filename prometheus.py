# Copyright (C) 2025 Sasha Shipka <sasha.shipka@copyleft.no>
# You may use, distribute and modify this code under the terms of the GNU General Public License v3.0

from prometheus_client import Counter, Histogram, Gauge
import time
from datetime import datetime

# ============================================================================
# HTTP Request Metrics
# ============================================================================

REQUEST_COUNT = Counter(
    "http_requests_total", 
    "Total HTTP requests", 
    ["method", "endpoint", "has_id"]
)

REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds", 
    "HTTP request latency", 
    ["method", "endpoint"]
)

REQUEST_RATE = Gauge(
    "request_rate_per_second",
    "Current request rate per second",
    ["endpoint"]
)

# ============================================================================
# Cache Metrics - Consolidated with optional time labels
# ============================================================================

# Core cache operation counters (supports both simple and time-based queries)
CACHE_OPERATIONS = Counter(
    "cache_operations_total",
    "Cache operations by type and endpoint with optional time labels",
    ["operation", "endpoint", "hour", "day_of_week", "date"]
)

# Current cache state
CACHE_SIZE = Gauge(
    "cache_size", 
    "Current number of items in cache with optional time labels",
    ["hour", "day_of_week", "date"]
)

CACHE_ENDPOINTS = Gauge(
    "cache_endpoints",
    "Number of unique cached endpoints with optional time labels", 
    ["hour", "day_of_week", "date"]
)

CACHE_MEMORY_USAGE = Gauge(
    "cache_memory_usage_bytes", 
    "Cache memory usage in bytes"
)

# ============================================================================
# Helper Functions
# ============================================================================

def get_time_labels():
    """Get current time labels for time-based metrics"""
    now = datetime.now()
    return {
        "hour": str(now.hour),
        "day_of_week": str(now.weekday()),  # 0=Monday, 6=Sunday
        "date": now.strftime("%Y-%m-%d")
    }

def record_cache_request(endpoint, hit=True):
    """Record a cache request with time-based labels
    
    Args:
        endpoint: The cache endpoint
        hit: True for cache hit, False for cache miss
    """
    time_labels = get_time_labels()
    
    # Record hit or miss in consolidated metric
    operation = "hit" if hit else "miss"
    CACHE_OPERATIONS.labels(
        operation=operation,
        endpoint=endpoint,
        hour=time_labels["hour"],
        day_of_week=time_labels["day_of_week"],
        date=time_labels["date"]
    ).inc()

def record_cache_refresh(endpoint):
    """Record a cache refresh with time-based labels
    
    Args:
        endpoint: The cache endpoint being refreshed
    """
    time_labels = get_time_labels()
    
    CACHE_OPERATIONS.labels(
        operation="refresh",
        endpoint=endpoint,
        hour=time_labels["hour"],
        day_of_week=time_labels["day_of_week"],
        date=time_labels["date"]
    ).inc()

def update_cache_size_metrics(total_items, unique_endpoints):
    """Update cache size metrics with current counts and time labels
    
    Args:
        total_items: Total number of items in cache
        unique_endpoints: Number of unique cached endpoints
    """
    time_labels = get_time_labels()
    
    # Update cache size with time labels
    CACHE_SIZE.labels(
        hour=time_labels["hour"],
        day_of_week=time_labels["day_of_week"], 
        date=time_labels["date"]
    ).set(total_items)
    
    CACHE_ENDPOINTS.labels(
        hour=time_labels["hour"],
        day_of_week=time_labels["day_of_week"],
        date=time_labels["date"]
    ).set(unique_endpoints)

async def update_current_cache_size():
    """Update just the current cache size efficiently (for real-time updates)"""
    try:
        from .util import get_redis_client
        redis_client = get_redis_client()
        keys = await redis_client.keys("GET:*")
        total_items = len(keys)
        
        time_labels = get_time_labels()
        CACHE_SIZE.labels(
            hour=time_labels["hour"],
            day_of_week=time_labels["day_of_week"],
            date=time_labels["date"]
        ).set(total_items)
        
        await redis_client.close()
    except Exception:
        # If we can't update, just continue - periodic update will catch it
        pass

# ============================================================================
# Migration Notes
# ============================================================================
# 
# Old metrics have been consolidated:
# - cache_hits_total, cache_misses_total, cache_refresh_total 
#   → cache_operations_total{operation="hit|miss|refresh"}
#
# - cache_hits_by_time_total, cache_misses_by_time_total, cache_refresh_by_time_total
#   → cache_operations_total (with time labels)
#
# - cache_items, cache_endpoints (now include time labels consistently)
#   → cache_size{hour,day_of_week,date}, cache_endpoints{hour,day_of_week,date}
#
# Grafana queries should use:
#   rate(cache_operations_total{operation="hit"}[5m]) - for hit rate
#   rate(cache_operations_total{operation="miss"}[5m]) - for miss rate
#   rate(cache_operations_total{operation="refresh"}[5m]) - for refresh rate
#
#   sum(rate(cache_operations_total{operation="hit"}[5m])) / 
#   sum(rate(cache_operations_total[5m])) - for hit ratio
