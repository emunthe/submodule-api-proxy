# Copyright (C) 2025 Sasha Shipka <sasha.shipka@copyleft.no>
# You may use, distribute and modify this code under the terms of the GNU General Public License v3.0
# Funded by Copyleft Solutions AS

import asyncio
import click
import logging
import sys
from app.config import config
from app.util import (
    get_logger,
    get_redis_client,
    get_http_client,
    refresh_base_data,
)
from app.token import TokenManager
import os

logger = get_logger(__name__)
redis_client = get_redis_client()
http_client = get_http_client()

# Configure logger for CLI
formatter = logging.Formatter("%(asctime)s: %(name)s [%(levelname)s] - %(message)s")
cli_handler = logging.StreamHandler(sys.stdout)
cli_handler.setFormatter(formatter)
logger.addHandler(cli_handler)

file_handler = logging.FileHandler(os.path.join(config.LOG_PATH, "cli.log"))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

logger.setLevel(logging.INFO)


@click.group()
def cli():
    """NIF Proxy CLI tools."""
    pass


@cli.command()
def refresh_data():
    """Refresh the permanent base data in Redis."""
    click.echo("Starting base data refresh...")

    # Create token manager
    token_manager = TokenManager()

    # Create and run the async task
    async def run_refresh():
        try:
            result = await refresh_base_data(redis_client, http_client, token_manager)
            if result:
                click.echo("Base data refresh completed successfully!")
                return 0
            else:
                click.echo("Base data refresh failed.")
                return 1
        finally:
            # Clean up resources
            await http_client.aclose()
            await redis_client.aclose()

    # Run the async function and exit with appropriate code
    return asyncio.run(run_refresh())


if __name__ == "__main__":
    cli()
