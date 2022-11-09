from __future__ import annotations

import sys
from datetime import datetime
from functools import lru_cache
from json import dumps as json_dumps
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union
from uuid import UUID

if sys.version_info < (3, 8):  # pragma: no cover
    from typing_extensions import TypedDict
else:
    from typing import TypedDict

import asyncpg  # type: ignore

from pgjobq._exceptions import JobDoesNotExist, QueueDoesNotExist, ReceiptHandleExpired
from pgjobq._filters import BaseClause
from pgjobq.api import OutgoingJob

PoolOrConnection = Union[asyncpg.Pool, asyncpg.Connection]
Record = Mapping[str, Any]


@lru_cache(None)
def get_queries() -> Dict[str, str]:
    res: Dict[str, str] = {}
    for file in (Path(__file__).parent / "_sql").glob("*.sql"):
        with file.open() as f:
            res[file.stem] = f.read()
    return res


async def publish_jobs(
    conn: PoolOrConnection,
    *,
    queue_name: str,
    ids: List[UUID],
    jobs: List[OutgoingJob],
    schedule_at: Optional[datetime],
) -> None:
    # (child_id, parent_id)
    deps = [
        (ids[idx], parent_id)
        for idx in range(len(ids))
        for parent_id in jobs[idx].dependencies
    ]
    try:
        await conn.fetchrow(  # type: ignore
            get_queries()["publish"],
            queue_name,
            schedule_at,
            ids,
            [m.body for m in jobs],
            [json_dumps(m.attributes) for m in jobs],
            [dep[0] for dep in deps],  # child_id
            [dep[1] for dep in deps],  # parent_id
        )
    except asyncpg.InvalidParameterValueError as e:
        raise QueueDoesNotExist(queue_name=queue_name) from e


class JobRecord(TypedDict):
    id: UUID
    body: bytes
    next_ack_deadline: datetime
    attributes: Optional[str]
    receipt_handle: int


async def poll_for_jobs(
    conn: PoolOrConnection,
    *,
    queue_name: str,
    batch_size: int,
    filter: Optional[BaseClause],
) -> Sequence[JobRecord]:
    params: List[Any] = [queue_name, batch_size]
    if filter:
        where = f"AND ({filter.get_value(params)})"
    else:
        where = ""
    return await conn.fetch(  # type: ignore
        get_queries()["poll"].format(where=where),
        *params,
    )


class AckResult(TypedDict):
    queue_exists: bool
    job_exists: bool
    receipt_handle_expired: bool


async def ack_job(
    conn: PoolOrConnection,
    queue_name: str,
    job_id: UUID,
    receipt_handle: int,
) -> None:
    res: AckResult = await conn.fetchrow(  # type: ignore
        get_queries()["ack"], queue_name, job_id, receipt_handle
    )
    if not res["queue_exists"]:
        raise QueueDoesNotExist(queue_name=queue_name)
    if not res["job_exists"]:
        raise JobDoesNotExist(job=job_id)
    if res["receipt_handle_expired"]:
        raise ReceiptHandleExpired(receipt_handle=receipt_handle)


async def nack_job(
    conn: PoolOrConnection,
    queue_name: str,
    job_id: UUID,
    receipt_handle: int,
) -> None:
    res: AckResult = await conn.fetchrow(  # type: ignore
        get_queries()["nack"], queue_name, job_id, receipt_handle
    )
    if not res["queue_exists"]:
        raise QueueDoesNotExist(queue_name=queue_name)
    if not res["job_exists"]:
        raise JobDoesNotExist(job=job_id)
    if res["receipt_handle_expired"]:
        raise ReceiptHandleExpired(receipt_handle=receipt_handle)


async def cancel_jobs(
    conn: PoolOrConnection,
    queue_name: str,
    filter: BaseClause,
) -> None:
    params = [queue_name]
    if filter:
        where = f"AND ({filter.get_value(params)})"
    else:
        where = ""
    query = get_queries()["cancel"].format(where=where)
    await conn.execute(query, *params)  # type: ignore


class ExtendedAckRecord(TypedDict):
    id: UUID
    next_ack_deadline: datetime


async def extend_ack_deadlines(
    conn: PoolOrConnection,
    queue_name: str,
    job_ids: List[UUID],
    receipt_handles: List[int],
) -> List[ExtendedAckRecord]:
    if not job_ids:
        return []
    res: Optional[List[ExtendedAckRecord]] = await conn.fetchval(  # type: ignore
        get_queries()["heartbeat"],
        queue_name,
        job_ids,
        receipt_handles,
    )
    if res is None:
        raise Exception
    return res


class QueueStatisticsRecord(TypedDict):
    jobs: int
    max_size: Optional[int]


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


async def get_completed_jobs(
    conn: PoolOrConnection,
    queue_name: str,
    job_ids: List[UUID],
) -> Sequence[UUID]:
    records: List[Record] = await conn.fetch(  # type: ignore
        get_queries()["gather_completed"], queue_name, job_ids
    )
    return [record["id"] for record in records]


async def cleanup_dead_jobs(
    conn: PoolOrConnection,
    queue_name: str,
) -> None:
    try:
        await conn.execute(  # type: ignore
            get_queries()["cleanup"],
            queue_name,
        )
    except asyncpg.InvalidParameterValueError:
        pass  # allow cleanup to start running before the queue exists
