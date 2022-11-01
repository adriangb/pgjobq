from __future__ import annotations

import sys
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union
from uuid import UUID

if sys.version_info < (3, 8):  # pragma: no cover
    from typing_extensions import TypedDict
else:
    from typing import TypedDict

import asyncpg  # type: ignore

PoolOrConnection = Union[asyncpg.Pool, asyncpg.Connection]
Record = Mapping[str, Any]


class QueueDoesNotExist(LookupError):
    def __init__(self, *, queue_name: str) -> None:
        super().__init__(f"Queue not found: there is no queue named {queue_name}")


@lru_cache
def get_queries() -> Dict[str, str]:
    res: Dict[str, str] = {}
    for file in (Path(__file__).parent / "_sql").glob("*.sql"):
        with file.open() as f:
            res[file.stem] = f.read()
    return res


async def publish_messages_from_bytes(
    conn: PoolOrConnection,
    *,
    queue_name: str,
    ids: List[UUID],
    bodies: List[bytes],
    schedule_at: Optional[datetime],
) -> None:
    res: Optional[int] = await conn.fetchval(  # type: ignore
        get_queries()["publish"],
        queue_name,
        schedule_at,
        ids,
        bodies,
    )
    if res is None:
        raise QueueDoesNotExist(queue_name=queue_name)


class MessageRecord(TypedDict):
    id: UUID
    body: bytes
    next_ack_deadline: datetime


async def poll_for_messages(
    conn: PoolOrConnection,
    *,
    queue_name: str,
    batch_size: int,
) -> Sequence[MessageRecord]:
    return await conn.fetch(  # type: ignore
        get_queries()["poll"],
        queue_name,
        batch_size,
    )


async def ack_message(
    conn: PoolOrConnection,
    queue_name: str,
    message_id: UUID,
) -> None:
    await conn.execute(get_queries()["ack"], queue_name, message_id)  # type: ignore


async def nack_message(
    conn: PoolOrConnection,
    queue_name: str,
    message_id: UUID,
) -> None:
    await conn.execute(get_queries()["nack"], queue_name, message_id)  # type: ignore


async def extend_ack_deadlines(
    conn: PoolOrConnection,
    queue_name: str,
    message_ids: Sequence[UUID],
) -> Optional[datetime]:
    return await conn.fetchval(  # type: ignore
        get_queries()["heartbeat"],
        queue_name,
        list(message_ids),
    )


class QueueStatisticsRecord(TypedDict):
    messages: int


async def get_statistics(
    conn: PoolOrConnection,
    queue_name: str,
) -> QueueStatisticsRecord:
    record: Optional[QueueStatisticsRecord] = await conn.fetchrow(  # type: ignore
        get_queries()["statistics"], queue_name
    )
    if record is None:
        raise QueueDoesNotExist(queue_name=queue_name)
    return record


async def get_completed_messages(
    conn: PoolOrConnection,
    queue_name: str,
    message_ids: List[UUID],
) -> Sequence[UUID]:
    records: List[Record] = await conn.fetch(  # type: ignore
        get_queries()["gather_completed"], queue_name, message_ids
    )
    return [record["id"] for record in records]


async def cleanup_dead_messages(
    conn: PoolOrConnection,
    queue_name: str,
) -> None:
    await conn.execute(  # type: ignore
        get_queries()["cleanup"],
        queue_name,
    )
