# Copyright (C) 2025 Sasha Shipka <sasha.shipka@copyleft.no>
# You may use, distribute and modify this code under the terms of the GNU General Public License v3.0

import asyncio
import json

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

                token = await token_manager.get_token()
                headers = {"Authorization": f"Bearer {token['access_token']}"}
                client = get_http_client()

                # -----------------------------------------------------------------
                # Fetch season list for relevant sports and years
                seasons = []
                current_year = pendulum.now().year

                for year in range(2003, current_year + 1):
                    for sport_id in [72, 151, 71, 73]:
                        url = f"{config.API_URL}/api/v1/ta/Seasons/"
                        logger.info(f"Fetching seasons for year {year}, sport {sport_id} from {url}")
                        resp = await client.get(
                            url,
                            headers=headers,
                            params={"year": year, "sportId": sport_id},
                        )
                        api_calls["seasons"] += 1
                        PRECACHE_API_CALLS.labels(call_type="seasons").inc()
                        
                        if resp.status_code < 400:
                            try:
                                raw = resp.json()
                            except Exception as e:
                                # Parsing failed, skip this response safely
                                logger.warning(
                                    f"Failed to parse seasons response for year={year} sport={sport_id}: {e}"
                                )
                                continue

                            # Log size / type, not full payload (avoid huge logs)
                            logger.info(
                                f"Fetched seasons payload type={type(raw).__name__} year={year} sport={sport_id}"
                            )

                            # add the raw return even if it is a dict to the seasons list
                            data = raw.get("seasons", [])
                            if isinstance(data, dict):
                                seasons.append(data)
                            elif isinstance(data, list):
                                seasons.extend(data)
                            else:
                                logger.warning(
                                    f"Unexpected seasons data type for year={year} sport={sport_id}: {type(data).__name__}"
                                )
                                # skip adding anything
                                continue

                        # If response failed, skip adding anything (do not add empty array)
                        await asyncio.sleep(0.2)


                # log the final seasons fetched
                logger.info(f"Total seasons fetched: {len(seasons)}")
                logger.debug(f"Fetched seasons: {seasons}")

                # Update metrics for items processed
                PRECACHE_ITEMS_PROCESSED.labels(item_type="seasons").set(len(seasons))

                # if cached valid_seasons is different from seasons, we have changes
                # so we need to mark changes_detected and store the new seasons

                if json.dumps(seasons, sort_keys=True) != json.dumps(cached_valid_seasons or [], sort_keys=True):
                    changes_detected["seasons"] = len(seasons)
                    PRECACHE_CHANGES_DETECTED.labels(category="seasons").inc()
                    await redis_client.set(
                        "valid_seasons",
                        json.dumps(
                            {"data": seasons, "last_updated": start_time.isoformat()},
                            ensure_ascii=False,
                        ),
                    )

                valid_seasons = []
                # for season in seasons:
                #     try:
                #         season_year = pendulum.parse(season["seasonDateFrom"]).year
                #     except Exception:
                #         continue
                #     sport_id = season.get("sportId")
                #     if season_year >= 2024:
                #         if sport_id != 72:
                #             valid_seasons.append(season)
                #         else:
                #             name = season.get("seasonName", "").lower()
                #             if "bedrift" not in name:
                #                 valid_seasons.append(season)

                # if json.dumps(valid_seasons, sort_keys=True) != json.dumps(
                #     cached_valid_seasons or [], sort_keys=True
                # ):
                #     await redis_client.set(
                #         "valid_seasons",
                #         json.dumps(
                #             {"data": valid_seasons, "last_updated": start_time.isoformat()},
                #             ensure_ascii=False,
                #         ),
                #     )
                #     changes_detected["valid_seasons"] = len(valid_seasons)

                # -----------------------------------------------------------------
                # Fetch tournaments per season
                tournaments = []
                # root_tournaments = []
                # for season in valid_seasons:
                #     season_id = season.get("seasonId")
                #     url = f"{config.API_URL}/api/v1/ta/Tournament/Season/{season_id}/"
                #     resp = await client.get(url, headers=headers, params={"hierarchy": True})
                #     api_calls["tournaments"] += 1
                #     PRECACHE_API_CALLS.labels(call_type="tournaments").inc()
                #     if resp.status_code < 400:
                #         data = resp.json().get("tournamentsInSeason", [])
                #         for t in data:
                #             t["apiPath"] = url
                #             tournaments.append(t)
                #             if not t.get("parentTournamentId"):
                #                 root_tournaments.append(t)
                #     await asyncio.sleep(0.2)

                # Update metrics for tournaments processed
                PRECACHE_ITEMS_PROCESSED.labels(item_type="tournaments").set(len(tournaments))

                # if json.dumps(tournaments, sort_keys=True) != json.dumps(
                #     cached_tournaments or [], sort_keys=True
                # ):
                #     await redis_client.set(
                #         "tournaments_in_season",
                #         json.dumps(
                #             {"data": tournaments, "last_updated": start_time.isoformat()},
                #             ensure_ascii=False,
                #         ),
                #     )
                #     cached_ids = {t["tournamentId"] for t in (cached_tournaments or [])}
                #     new_ids = {t["tournamentId"] for t in tournaments}
                #     changes_detected["tournaments_in_season"] = len(
                #         new_ids.symmetric_difference(cached_ids)
                #     )
                #     PRECACHE_CHANGES_DETECTED.labels(category="tournaments_in_season").inc()

                # -----------------------------------------------------------------
                # Fetch tournament matches for root tournaments
                matches = []
                # for tournament in root_tournaments:
                #     tid = tournament.get("tournamentId")
                #     url = f"{config.API_URL}/api/v1/ta/TournamentMatches/"
                #     params = {"tournamentId": tid}
                #     resp = await client.get(url, headers=headers, params=params)
                #     api_calls["matches"] += 1
                #     PRECACHE_API_CALLS.labels(call_type="matches").inc()
                #     if resp.status_code < 400:
                #         for m in resp.json().get("matches", []):
                #             m["apiPath"] = f"{url}?tournamentId={tid}"
                #             matches.append(m)
                #     await asyncio.sleep(0.2)

                # Update metrics for matches processed
                PRECACHE_ITEMS_PROCESSED.labels(item_type="matches").set(len(matches))

                # if json.dumps(matches, sort_keys=True) != json.dumps(
                #     cached_matches or [], sort_keys=True
                # ):
                #     await redis_client.set(
                #         "tournament_matches",
                #         json.dumps(
                #             {"data": matches, "last_updated": start_time.isoformat()},
                #             ensure_ascii=False,
                #         ),
                #     )

                #     def _group_by_tournament(data):
                #         grouped = {}
                #         for item in data:
                #             tid = item.get("tournamentId")
                #             if tid is None:
                #                 continue
                #             grouped.setdefault(tid, []).append(item)
                #         return grouped

                #     new_group = _group_by_tournament(matches)
                #     old_group = _group_by_tournament(cached_matches or [])
                #     changed_tournament_ids = set()
                #     for tid, new_list in new_group.items():
                #         old_list = old_group.get(tid, [])
                #         if json.dumps(new_list, sort_keys=True) != json.dumps(
                #             old_list, sort_keys=True
                #         ):
                #             changed_tournament_ids.add(tid)
                #     for tid in old_group:
                #         if tid not in new_group:
                #             changed_tournament_ids.add(tid)
                #     changes_detected["tournament_matches"] = len(changed_tournament_ids)
                #     PRECACHE_CHANGES_DETECTED.labels(category="tournament_matches").inc()
                # else:
                #     changed_tournament_ids = set()

                # -----------------------------------------------------------------
                # Unique team identifiers
                team_ids = set()
                # for match in matches:
                #     for key in ("hometeamId", "awayteamId"):
                #         tid = match.get(key)
                #         if tid is not None:
                #             team_ids.add(tid)

                # Update metrics for teams processed
                PRECACHE_ITEMS_PROCESSED.labels(item_type="teams").set(len(team_ids))

                # if team_ids != set(cached_team_ids):
                #     await redis_client.set(
                #         "unique_team_ids",
                #         json.dumps(
                #             {"data": list(team_ids), "last_updated": start_time.isoformat()},
                #             ensure_ascii=False,
                #         ),
                #     )
                #     changed_team_ids = list(team_ids.symmetric_difference(set(cached_team_ids)))
                #     changes_detected["unique_team_ids"] = len(changed_team_ids)
                #     PRECACHE_CHANGES_DETECTED.labels(category="unique_team_ids").inc()
                # else:
                #     changed_team_ids = []

                # -----------------------------------------------------------------
                # Warm caches for changed tournaments and teams
                refresh_until = pendulum.now().add(days=7)
                ttl = 7 * 24 * 60 * 60

                for tid in changed_tournament_ids:
                    url = f"{config.API_URL}/api/v1/ta/TournamentMatches"
                    cache_key = f"GET:{url}?tournamentId={tid}"
                    await cache_manager.setup_refresh(
                        cache_key, url, ttl, refresh_until, params={"tournamentId": tid}
                    )

                for team_id in changed_team_ids:
                    url = f"{config.API_URL}/api/v1/ta/Team"
                    cache_key = f"GET:{url}?teamId={team_id}"
                    await cache_manager.setup_refresh(
                        cache_key, url, ttl, refresh_until, params={"teamId": team_id}
                    )

                # Record successful run
                PRECACHE_RUNS_TOTAL.labels(status="success").inc()
                PRECACHE_LAST_RUN_TIMESTAMP.set(start_time.timestamp())

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


async def get_tournaments_and_matches(token_manager):
    """Get tournaments and scheduled matches"""
    cache_key = "tournaments_matches"
    redis_client = None
    client = None

    try:
        redis_client = get_redis_client()
        cached_data = await redis_client.get(cache_key)
        if cached_data:
            return {
                "content": cached_data,
                "status_code": 200,
                "headers": {"X-Cache": "HIT"}
            }

        client = get_http_client()

        tournaments_url = f"{config.API_URL}/api/v1/ta/Tournaments"
        matches_url = f"{config.API_URL}/api/v1/ta/ScheduledMatches"

        # tournaments_response = await client.get(tournaments_url, headers=headers)
        # matches_response = await client.get(matches_url, headers=headers)

        # if tournaments_response.status_code >= 400 or matches_response.status_code >= 400:
        #     raise Exception("Failed to fetch tournaments or matches data")

        params = {
            "tournaments_url": tournaments_url,
            "matches_url": matches_url,
        }

        logger.info(
            f"Tournaments and matches params: {params}"
        )

        data = {
            "tournaments": [],  # tournaments_response.json(),
            "matches": [],  # matches_response.json(),
            "last_updated": pendulum.now().isoformat(),
        }

        await redis_client.setex(
            cache_key, config.DEFAULT_TTL, json.dumps(data, ensure_ascii=False)
        )

        return {
            "content": json.dumps(data, ensure_ascii=False),
            "status_code": 200,
            "headers": {"X-Cache": "MISS"}
        }
    except Exception as e:
        logger.error(f"Error in get_tournaments_and_matches: {e}")
        logger.exception(e)
        return {
            "content": json.dumps({"error": str(e)}, ensure_ascii=False),
            "status_code": 500,
            "headers": {}
        }
    finally:
        if client:
            await client.aclose()
        if redis_client:
            await redis_client.close()