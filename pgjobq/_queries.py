from __future__ import annotations

import sys
from datetime import datetime
from functools import lru_cache
from json import dumps as json_dumps
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence
from uuid import UUID

if sys.version_info < (3, 8):  # pragma: no cover
    from typing_extensions import TypedDict
else:
    from typing import TypedDict

import asyncpg  # type: ignore

from pgjobq._exceptions import JobDoesNotExist, QueueDoesNotExist, ReceiptHandleExpired
from pgjobq._filters import BaseClause
from pgjobq._telemetry import TelemetryHook
from pgjobq.api import OutgoingJob
from pgjobq.types import PoolOrConnection

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
    telemetry_hook: TelemetryHook,
) -> None:
    # (child_id, parent_id)
    deps = [
        (ids[idx], parent_id)
        for idx in range(len(ids))
        for parent_id in jobs[idx].dependencies
    ]
    operation = "publish"
    try:
        query = get_queries()[operation]
        args = (
            queue_name,
            schedule_at,
            ids,
            [m.body for m in jobs],
            [json_dumps(m.attributes) for m in jobs],
            [dep[0] for dep in deps],  # child_id
            [dep[1] for dep in deps],  # parent_id
        )
        async with telemetry_hook.on_query(
            queue_name, operation, query, conn, args
        ) as conn1:
            await conn1.fetchrow(query, *args)  # type: ignore
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
    telemetry_hook: TelemetryHook,
) -> Sequence[JobRecord]:
    operation = "poll"
    params: List[Any] = [queue_name, batch_size]
    if filter:
        where = f"AND ({filter.get_value(params)})"
    else:
        where = ""
    query = get_queries()[operation].format(where=where)
    async with telemetry_hook.on_query(
        queue_name, operation, query, conn, params
    ) as conn1:
        return await conn1.fetch(query, *params)  # type: ignore


class AckResult(TypedDict):
    queue_exists: bool
    job_exists: bool
    receipt_handle_expired: bool


async def ack_job(
    conn: PoolOrConnection,
    queue_name: str,
    job_id: UUID,
    receipt_handle: int,
    telemetry_hook: TelemetryHook,
) -> None:
    operation = "ack"
    query = get_queries()[operation]
    params = (queue_name, job_id, receipt_handle)
    async with telemetry_hook.on_query(
        queue_name, operation, query, conn, params
    ) as conn1:
        res: AckResult = await conn1.fetchrow(query, *params)  # type: ignore
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
    telemetry_hook: TelemetryHook,
) -> None:
    operation = "nack"
    query = get_queries()[operation]
    params = (queue_name, job_id, receipt_handle)
    async with telemetry_hook.on_query(
        queue_name, operation, query, conn, params
    ) as conn1:
        res: AckResult = await conn1.fetchrow(query, *params)  # type: ignore
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
    telemetry_hook: TelemetryHook,
) -> None:
    operation = "cancel"
    params = [queue_name]
    if filter:
        where = f"AND ({filter.get_value(params)})"
    else:
        where = ""
    query = get_queries()[operation].format(where=where)
    async with telemetry_hook.on_query(
        queue_name, operation, query, conn, params
    ) as conn1:
        await conn1.execute(query, *params)  # type: ignore


class ExtendedAckRecord(TypedDict):
    id: UUID
    next_ack_deadline: datetime


async def extend_ack_deadlines(
    conn: PoolOrConnection,
    queue_name: str,
    job_ids: List[UUID],
    receipt_handles: List[int],
    telemetry_hook: TelemetryHook,
) -> List[ExtendedAckRecord]:
    operation = "heartbeat"
    if not job_ids:
        return []
    query = get_queries()[operation]
    params = (queue_name, job_ids, receipt_handles)
    async with telemetry_hook.on_query(
        queue_name, operation, query, conn, params
    ) as conn1:
        res: Optional[List[ExtendedAckRecord]] = await conn1.fetchval(query, *params)  # type: ignore
    if res is None:
        raise Exception
    return res


class QueueStatisticsRecord(TypedDict):
    jobs: int
    max_size: Optional[int]


async def get_statistics(
    conn: PoolOrConnection,
    queue_name: str,
    telemetry_hook: TelemetryHook,
) -> QueueStatisticsRecord:
    operation = "statistics"
    query = get_queries()[operation]
    params = (queue_name,)
    async with telemetry_hook.on_query(
        queue_name, operation, query, conn, params
    ) as conn1:
        record: Optional[QueueStatisticsRecord] = await conn1.fetchrow(query, *params)  # type: ignore
    if record is None:
        raise QueueDoesNotExist(queue_name=queue_name)
    return record


async def get_completed_jobs(
    conn: PoolOrConnection,
    queue_name: str,
    job_ids: List[UUID],
    telemetry_hook: TelemetryHook,
) -> Sequence[UUID]:
    operation = "gather_completed"
    query = get_queries()[operation]
    params = (queue_name, job_ids)
    async with telemetry_hook.on_query(
        queue_name, operation, query, conn, params
    ) as conn1:
        records: List[Record] = await conn1.fetch(query, *params)  # type: ignore
    return [record["id"] for record in records]


async def cleanup_dead_jobs(
    conn: PoolOrConnection,
    queue_name: str,
    telemetry_hook: TelemetryHook,
) -> None:
    try:
        operation = "cleanup"
        query = get_queries()[operation]
        params = (queue_name,)
        async with telemetry_hook.on_query(
            queue_name, operation, query, conn, params
        ) as conn1:
            await conn1.execute(query, *params)  # type: ignore
    except asyncpg.InvalidParameterValueError:
        pass  # allow cleanup to start running before the queue exists
