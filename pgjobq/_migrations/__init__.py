from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, Optional, Tuple

import asyncpg  # type: ignore

logger = logging.getLogger(__name__)


def get_available() -> Iterable[Tuple[int, Path]]:
    migrations_path = Path(__file__).parent
    suffix = ".up.sql"
    for path in migrations_path.glob(f"*{suffix}"):
        version = int(f"{path.name[:-len(suffix)]}")
        yield (version, path)


async def migrate_to_latest_version(pool: asyncpg.Pool) -> None:
    conn: asyncpg.Connection
    async with pool.acquire() as conn:  # type: ignore
        try:
            current: Optional[int] = await pool.fetchval(  # type: ignore
                "SELECT current_revision FROM pgjobq.migrations"
            )
        except asyncpg.exceptions.UndefinedTableError:
            current = 0

        current = current or 0

        logger.info("Current migration %s", current)

        applied = current
        async with conn.transaction(isolation="serializable"):  # type: ignore
            for version, path in get_available():
                if version > applied:
                    logger.info(f"Applying migration {version}")
                    with path.open() as f:
                        sql = f.read()
                    await conn.execute(sql)  # type: ignore
                    applied = version

            if applied != current:
                await conn.execute(  # type: ignore
                    "UPDATE pgjobq.migrations SET current_revision = $1",
                    applied,
                )
                logger.info(f"Migrations to {applied} from {current} successful")
            else:
                logger.info("No migrations applied. Your db it's at latest version")
