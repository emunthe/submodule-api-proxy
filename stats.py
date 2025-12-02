# Copyright (C) 2025 Sasha Shipka <sasha.shipka@copyleft.no>
# You may use, distribute and modify this code under the terms of the GNU General Public License v3.0

from .cache import extract_base_endpoint

from .util import get_logger, get_redis_client

logger = get_logger(__name__)


class StatsCollector:
    async def log_request_stats(self, request, path, status_code, process_time):
        """
        Log request statistics to Redis for the /stats endpoint.
        This is separate from Prometheus metrics.
        """
        # Extract endpoint without query params
        endpoint = request.url.path

        # Check if endpoint contains an ID
        base_endpoint, has_id = extract_base_endpoint(path)

        # Increment counters in Redis
        redis = get_redis_client()
        pipeline = redis.pipeline()
        # Total count for the specific path
        pipeline.incr(f"stats:count:{endpoint}")
        # Count by base endpoint (with/without ID)
        id_type = "with_id" if has_id else "without_id"
        pipeline.incr(f"stats:count:{base_endpoint}:{id_type}")
        # Count by status code
        pipeline.incr(f"stats:status:{status_code}")
        # Store response time
        pipeline.lpush(f"stats:times:{endpoint}", process_time)
        pipeline.ltrim(
            f"stats:times:{endpoint}", 0, 999
        )  # Keep last 1000 timing samples
        await pipeline.execute()
        await redis.close()

    async def get_stats(self):
        """Get all collected statistics"""
        # Get all endpoints that have been tracked
        redis = get_redis_client()
        endpoint_keys = await redis.keys("stats:count:*")

        stats = {}
        for key in endpoint_keys:
            endpoint = key.replace("stats:count:", "")
            count = int(await redis.get(key))

            # Get timing information if available
            times_key = f"stats:times:{endpoint}"
            if await redis.exists(times_key):
                times = [float(t) for t in await redis.lrange(times_key, 0, -1)]
                avg_time = sum(times) / len(times) if times else 0
            else:
                avg_time = 0

            stats[endpoint] = {"count": count, "avg_response_time": avg_time}

        await redis.close()
        return stats
