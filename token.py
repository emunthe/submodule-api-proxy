# Copyright (C) 2025 Sasha Shipka <sasha.shipka@copyleft.no>
# You may use, distribute and modify this code under the terms of the GNU General Public License v3.0

import httpx

from .config import config
from .util import get_logger, get_redis_client

logger = get_logger(__name__)


class TokenManager:
    async def get_token(self):
        """
        Fetch token from Redis, if expired, fetch new one from API.
        Returns a token dict with access_token and expires_in.
        """
        redis = get_redis_client()
        token = await redis.get("token")

        if token:
            # Token is stored as a string in Redis, return as dict
            logger.info("Using cached token")
            return eval(token)

        # Fetch new token
        try:
            async with httpx.AsyncClient() as client:
                data = {
                    "client_id": config.API_CLIENT_ID,
                    "client_secret": config.API_CLIENT_SECRET,
                    "grant_type": "client_credentials",
                    "scope": "data_ta_read data_venue_read data_venuematch_read data_org_read data_ta_scheduledmatches_read data_scheduledmatches_sport_read data_ta_scheduledreferee_read  data_ta_matchincidents_read data_ta_person_read data_bandy_read data_transfers_read",
                }
                response = await client.post(config.TOKEN_URL, data=data)
                response.raise_for_status()
                token = response.json()
                await redis.set("token", str(token), ex=token["expires_in"])
                logger.info("Fetched new token")
                return token
        except Exception as e:
            logger.error(f"Failed to get token: {e}")
            raise

        finally:
            await redis.close()
