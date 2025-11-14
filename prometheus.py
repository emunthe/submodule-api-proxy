# Copyright (C) 2025 Sasha Shipka <sasha.shipka@copyleft.no>
# You may use, distribute and modify this code under the terms of the GNU General Public License v3.0

from prometheus_client import Counter, Histogram

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
