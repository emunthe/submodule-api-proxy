# Copyright (C) 2025 Sasha Shipka <sasha.shipka@copyleft.no>
# You may use, distribute and modify this code under the terms of the GNU General Public License v3.0

import os

from box import Box
from dotenv import load_dotenv

load_dotenv()

config = Box(
    {
        "API_URL": os.getenv("API_URL"),
        "TOKEN_URL": os.getenv("TOKEN_URL"),
        "HTTP_TIMEOUT": int(os.getenv("HTTP_TIMEOUT", 10)),
        "REDIS_URL": os.getenv("REDIS_URL"),
        "REDIS_PORT": os.getenv("REDIS_PORT"),
        "REDIS_PASSWORD": os.getenv("REDIS_PASSWORD"),
        "REDIS_DB": int(os.getenv("REDIS_DB", 0)),
        "API_CLIENT_ID": os.getenv("API_CLIENT_ID"),
        "API_CLIENT_SECRET": os.getenv("API_CLIENT_SECRET"),
        "DEFAULT_TTL": int(os.getenv("DEFAULT_TTL", 30)),
        "DEFAULT_MAXSTALE": int(os.getenv("DEFAULT_MAXSTALE", 600)),  # 10 minutes
        "DEFAULT_HARDTTL": int(os.getenv("DEFAULT_HARDTTL", 7200)),  # 2 hours
        "LOG_PATH": os.getenv("LOG_PATH", "logs"),
        "LOG_FILE": os.getenv("LOG_FILE", "app.log"),
        "LOG_LEVEL": os.getenv("LOG_LEVEL", "DEBUG"),
        "BASE_PATH": os.getenv("BASE_PATH", ""),
        "MAX_CONNECTIONS": int(os.getenv("MAX_CONNECTIONS", 100)),
        "MAX_KEEPALIVE_CONNECTIONS": int(os.getenv("MAX_KEEPALIVE_CONNECTIONS", 20)),
        "MAX_REDIS_CONNECTIONS": int(os.getenv("MAX_REDIS_CONNECTIONS", 100)),
    })
    
