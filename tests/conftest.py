from random import choices
from string import ascii_lowercase
from typing import Any, AsyncGenerator

import asyncpg  # type: ignore
import pytest
from pydantic import BaseSettings

from pgmq import migrate_to_latest_version


class TestPostgresConnectionConfig(BaseSettings):
    host: str = "127.0.0.1"
    port: int = 5432
    user: str = "postgres"
    password: str = "postgres"
    database: str = "postgres"

    class Config(BaseSettings.Config):
        env_prefix = "POSTGRES_"


connection_config = TestPostgresConnectionConfig()


@pytest.fixture(
    params=[
        pytest.param(("asyncio", {"use_uvloop": False}), id="asyncio"),
    ],
    scope="session",
)
def anyio_backend(request: Any) -> Any:
    return request.param


@pytest.fixture(scope="session")
async def admin_db_conn(
    anyio_backend: Any,
) -> "AsyncGenerator[asyncpg.Connection, None]":
    conn: asyncpg.Connection
    conn = await asyncpg.connect(  # type: ignore
        host=connection_config.host,
        port=connection_config.port,
        user=connection_config.user,
        password=connection_config.password,
        database=connection_config.database,
    )
    try:
        yield conn
    finally:
        await conn.close()  # type: ignore


@pytest.fixture(scope="session")
def test_db_name() -> str:
    return "".join(choices(ascii_lowercase, k=5))


@pytest.fixture(scope="session")
async def pool(
    admin_db_conn: asyncpg.Connection,
    test_db_name: str,
) -> AsyncGenerator[asyncpg.Pool, None]:
    await admin_db_conn.execute(f"CREATE DATABASE {test_db_name}")  # type: ignore
    try:
        async with asyncpg.create_pool(  # type: ignore
            host=connection_config.host,
            port=connection_config.port,
            user=connection_config.user,
            password=connection_config.password,
            database=test_db_name,
        ) as pool:
            await migrate_to_latest_version(pool)
            yield pool
    finally:
        await admin_db_conn.execute(f"DROP DATABASE {test_db_name}")  # type: ignore


@pytest.fixture
async def migrated_pool(
    pool: asyncpg.Pool,
) -> asyncpg.Pool:
    await migrate_to_latest_version(pool)
    try:
        yield pool
    finally:
        await pool.execute("DROP SCHEMA IF EXISTS pgmq CASCADE")
