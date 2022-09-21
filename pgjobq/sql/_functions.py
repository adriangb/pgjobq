from __future__ import annotations

import sys
from datetime import datetime, timedelta
from typing import Any, List, Mapping, Optional, Sequence, Union
from uuid import UUID

if sys.version_info < (3, 8):
    from typing_extensions import TypedDict
else:
    from typing import TypedDict

import asyncpg  # type: ignore

PoolOrConnection = Union[asyncpg.Pool, asyncpg.Connection]
Record = Mapping[str, Any]


PUBLISH_MESSAGES = """\
WITH queue_info AS (
    SELECT
        id AS queue_id,
        retention_period,
        max_delivery_attempts
    FROM pgjobq.queues
    WHERE name = $1
)
INSERT INTO pgjobq.messages(
    queue_id,
    id,
    expires_at,
    delivery_attempts_remaining,
    available_at,
    body
)
SELECT
    queue_id,
    unnest($2::uuid[]),
    now() + retention_period,
    max_delivery_attempts,
    -- set next ack to now
    -- somewhat meaningless but avoids nulls
    now() + COALESCE($4, '0 seconds'::interval),
    unnest($3::bytea[])
FROM queue_info
RETURNING (
    SELECT 'true'::bool FROM pg_notify('pgjobq.new_job', $1)
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
        raise LookupError(f"Queue not found: there is no queue named {queue_name}")


_POLL_FOR_MESSAGES = """\
WITH queue_info AS (
    SELECT
        id,
        ack_deadline
    FROM pgjobq.queues
    WHERE name = $1
), selected_messages AS (
    SELECT
        id
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
    {order_by}
    FOR UPDATE SKIP LOCKED
    LIMIT $2
)
UPDATE pgjobq.messages
SET
    available_at = now() + (SELECT ack_deadline FROM queue_info),
    delivery_attempts_remaining = delivery_attempts_remaining - 1
WHERE pgjobq.messages.id IN (SELECT id FROM selected_messages)
RETURNING pgjobq.messages.id AS id, available_at AS next_ack_deadline, body
"""

POLL_FOR_MESSAGES = _POLL_FOR_MESSAGES.format(order_by="")
POLL_FOR_MESSAGES_FIFO = _POLL_FOR_MESSAGES.format(order_by="ORDER BY id")


class JobRecord(TypedDict):
    id: UUID
    body: bytes
    next_ack_deadline: datetime


async def poll_for_messages(
    conn: PoolOrConnection,
    *,
    queue_name: str,
    batch_size: int,
    fifo: bool,
) -> Sequence[JobRecord]:
    query = POLL_FOR_MESSAGES_FIFO if fifo else POLL_FOR_MESSAGES
    return await conn.fetch(  # type: ignore
        query,
        queue_name,
        batch_size,
    )


ACK_MESSAGE = """\
WITH msg AS (
    SELECT
        id
    FROM pgjobq.messages
    WHERE queue_id = (SELECT id FROM pgjobq.queues WHERE name = $1) AND id = $2::uuid
)
DELETE
FROM pgjobq.messages
WHERE pgjobq.messages.id = (SELECT id FROM msg)
RETURNING (
    SELECT
        pg_notify('pgjobq.job_completed', $1 || ',' || CAST($2::uuid AS text))
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
RETURNING (SELECT pg_notify('pgjobq.new_job', $1));
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
