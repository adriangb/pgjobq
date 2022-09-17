import asyncpg  # type: ignore
import pytest

from pgjobq import migrate_to_latest_version


@pytest.mark.anyio
async def test_migrations_idempotent(
    pool: asyncpg.Pool,
) -> None:
    await migrate_to_latest_version(pool)
    await migrate_to_latest_version(pool)
