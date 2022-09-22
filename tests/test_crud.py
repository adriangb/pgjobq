from datetime import timedelta

import asyncpg  # type: ignore
import pytest

from pgmq import create_queue, delete_queue


@pytest.mark.anyio
async def test_create_queue_invalid_name(
    migrated_pool: asyncpg.Pool,
) -> None:
    with pytest.raises(ValueError, match="Invalid Queue Name"):
        await create_queue("not! valid", migrated_pool)


@pytest.mark.anyio
async def test_create_queue_invalid_ack_deadline(
    migrated_pool: asyncpg.Pool,
) -> None:
    with pytest.raises(ValueError, match="Minimum ack deadline is 1 second"):
        await create_queue("q", migrated_pool, ack_deadline=timedelta(seconds=0.1))


@pytest.mark.anyio
async def test_create_duplicate_queue(
    migrated_pool: asyncpg.Pool,
) -> None:
    created = await create_queue("foo", migrated_pool)
    assert created
    created = await create_queue("foo", migrated_pool)
    assert not created


@pytest.mark.anyio
async def test_delete_queue(
    migrated_pool: asyncpg.Pool,
) -> None:
    created = await create_queue("foo", migrated_pool)
    assert created
    deleted = await delete_queue("foo", migrated_pool)
    assert deleted
    created = await create_queue("foo", migrated_pool)
    assert created


@pytest.mark.anyio
async def test_delete_non_existent_queue(
    migrated_pool: asyncpg.Pool,
) -> None:
    deleted = await delete_queue("foo", migrated_pool)
    assert deleted is False
