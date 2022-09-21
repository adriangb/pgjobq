from __future__ import annotations

import sys
from datetime import datetime, timedelta
from typing import Any, List, Mapping, Optional, Sequence, Union
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


PUBLISH_MESSAGES = """\
WITH queue_info AS (
    UPDATE pgjobq.queues
    SET
        current_message_count = current_message_count + array_length($2::uuid[], 1),
        undelivered_message_count = undelivered_message_count + array_length($2::uuid[], 1)
    WHERE name = $1
    RETURNING
        id AS queue_id,
        retention_period,
        max_delivery_attempts
)
INSERT INTO pgjobq.messages(
    queue_id,
    id,
    expires_at,
    max_delivery_attempts,
    delivery_attempts_remaining,
    available_at,
    body
)
SELECT
    queue_id,
    unnest($2::uuid[]),
    now() + retention_period,
    max_delivery_attempts,
    max_delivery_attempts,
    -- set next ack to now
    -- somewhat meaningless but avoids nulls
    now() + COALESCE($4, '0 seconds'::interval),
    unnest($3::bytea[])
FROM queue_info
RETURNING (
    SELECT 'true'::bool FROM pg_notify('pgjobq.new_job_' || $1, '')
);  -- NULL if the queue doesn't exist
"""


async def publish_messages(
    conn: PoolOrConnection,
    *,
    queue_name: str,
    message_ids: List[UUID],
    message_bodies: List[bytes],
    delay: Optional[timedelta],
) -> None:
    res: Optional[int] = await conn.fetchval(  # type: ignore
        PUBLISH_MESSAGES,
        queue_name,
        message_ids,
        message_bodies,
        delay,
    )
    if res is None:
        raise QueueDoesNotExist(queue_name=queue_name)


POLL_FOR_MESSAGES = """\
WITH queue_info AS (
    SELECT
        id,
        ack_deadline
    FROM pgjobq.queues
    WHERE name = $1
), selected_messages AS (
    SELECT
        id,
        (SELECT delivery_attempts_remaining = max_delivery_attempts)::int AS first_delivery
    FROM pgjobq.messages
    WHERE (
        delivery_attempts_remaining != 0
        AND
        expires_at > now()
        AND
        available_at < now()
        AND
        queue_id = (SELECT id FROM queue_info)
    )
    ORDER BY id
    FOR UPDATE SKIP LOCKED
    LIMIT $2
), updated_queue_info AS (
    UPDATE pgjobq.queues
    SET
        undelivered_message_count = undelivered_message_count - (SELECT COALESCE(sum(first_delivery), 0) FROM selected_messages)
    WHERE id = (SELECT id FROM queue_info)
)
UPDATE pgjobq.messages
SET
    available_at = now() + (SELECT ack_deadline FROM queue_info),
    delivery_attempts_remaining = delivery_attempts_remaining - 1
FROM selected_messages
WHERE pgjobq.messages.id = selected_messages.id
RETURNING pgjobq.messages.id AS id, available_at AS next_ack_deadline, body;
"""


class JobRecord(TypedDict):
    id: UUID
    body: bytes
    next_ack_deadline: datetime


async def poll_for_messages(
    conn: PoolOrConnection,
    *,
    queue_name: str,
    batch_size: int,
) -> Sequence[JobRecord]:
    return await conn.fetch(  # type: ignore
        POLL_FOR_MESSAGES,
        queue_name,
        batch_size,
    )


ACK_MESSAGE = """\
WITH queue_info AS (
    UPDATE pgjobq.queues
    SET
        current_message_count = current_message_count - 1
    WHERE name = $1
    RETURNING
        id
), msg AS (
    SELECT
        id
    FROM pgjobq.messages
    WHERE queue_id = (SELECT id FROM queue_info) AND id = $2::uuid
)
DELETE
FROM pgjobq.messages
WHERE pgjobq.messages.id = (SELECT id FROM msg)
RETURNING (
    SELECT
        pg_notify('pgjobq.job_completed_' || $1, CAST($2::uuid AS text))
) AS notified;
"""


async def ack_message(
    conn: PoolOrConnection,
    queue_name: str,
    job_id: UUID,
) -> None:
    await conn.execute(ACK_MESSAGE, queue_name, job_id)  # type: ignore


NACK_MESSAGE = """\
UPDATE pgjobq.messages
-- make it available in the past to avoid race conditions with extending acks
-- which check to make sure the message is still available before extending
SET available_at = now() - '1 second'::interval
WHERE queue_id = (SELECT id FROM pgjobq.queues WHERE name = $1) AND id = $2
RETURNING (SELECT pg_notify('pgjobq.new_job_' || $1, ''));
"""


async def nack_message(
    conn: PoolOrConnection,
    queue_name: str,
    job_id: UUID,
) -> None:
    await conn.execute(NACK_MESSAGE, queue_name, job_id)  # type: ignore


EXTEND_ACK_DEADLINES = """\
WITH queue_info AS (
    SELECT
        id,
        ack_deadline
    FROM pgjobq.queues
    WHERE name = $1
)
UPDATE pgjobq.messages
SET available_at = (
    now() + (
        SELECT ack_deadline
        FROM queue_info
    )
)
WHERE (
    queue_id = (SELECT id FROM queue_info)
    AND
    id = any($2::uuid[])
    AND
    -- skip any jobs that already expired
    -- this avoids race conditions between
    -- extending acks and nacking
    available_at > now()
)
RETURNING available_at AS next_ack_deadline;
"""


async def extend_ack_deadlines(
    conn: PoolOrConnection,
    queue_name: str,
    job_ids: Sequence[UUID],
) -> Optional[datetime]:
    return await conn.fetchval(EXTEND_ACK_DEADLINES, queue_name, list(job_ids))  # type: ignore


GET_STATISTICS = """\
SELECT
    current_message_count,
    undelivered_message_count
FROM pgjobq.queues
WHERE name = $1
"""


class QueueStatisticsRecord(TypedDict):
    current_message_count: int
    undelivered_message_count: int


async def get_statistics(
    conn: PoolOrConnection,
    queue_name: str,
) -> QueueStatisticsRecord:
    record: Optional[QueueStatisticsRecord] = await conn.fetchrow(  # type: ignore
        GET_STATISTICS, queue_name
    )
    if record is None:
        raise QueueDoesNotExist(queue_name=queue_name)
    return record


GET_COMPLETED_JOBS = """\
WITH input AS (
    SELECT
        id
    FROM unnest($2::uuid[]) AS t(id)
), queue_info AS (
    SELECT
        id
    FROM pgjobq.queues
    WHERE name = $1
)
SELECT
    id
FROM input
WHERE NOT EXISTS(
    SELECT *
    FROM pgjobq.messages
    WHERE (
        pgjobq.messages.id = input.id
        AND
        pgjobq.messages.queue_id = (SELECT id FROM queue_info)
    )
)
"""


async def get_completed_jobs(
    conn: PoolOrConnection,
    queue_name: str,
    job_ids: List[UUID],
) -> Sequence[UUID]:
    records: List[Record] = await conn.fetch(GET_COMPLETED_JOBS, queue_name, job_ids)  # type: ignore
    return [record["id"] for record in records]
