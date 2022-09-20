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
), published_notification AS (
    SELECT pg_notify('pgjobq.new_job', $1)
)
INSERT INTO pgjobq.messages(
    queue_id,
    batch_id,
    id,
    expires_at,
    delivery_attempts_remaining,
    available_at,
    body
)
SELECT
    queue_id,
    $2,
    unnest($3::uuid[]),
    now() + retention_period,
    max_delivery_attempts,
    -- set next ack to now
    -- somewhat meaningless but avoids nulls
    now() + COALESCE($5, '0 seconds'::interval),
    unnest($4::bytea[])
FROM queue_info
LEFT JOIN published_notification ON 1 = 1
RETURNING 1;  -- NULL if the queue doesn't exist
"""


async def publish_messages(
    conn: PoolOrConnection,
    *,
    queue_name: str,
    batch_id: UUID,
    message_ids: List[UUID],
    message_bodies: List[bytes],
    delay: Optional[timedelta],
) -> None:
    res: Optional[int] = await conn.fetchval(  # type: ignore
        PUBLISH_MESSAGES,
        queue_name,
        batch_id,
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


class Message(TypedDict):
    id: UUID
    body: bytes
    next_ack_deadline: datetime


async def poll_for_messages(
    conn: PoolOrConnection,
    *,
    queue_name: str,
    batch_size: int,
    fifo: bool,
) -> List[Message]:
    query = POLL_FOR_MESSAGES_FIFO if fifo else POLL_FOR_MESSAGES
    return await conn.fetch(  # type: ignore
        query,
        queue_name,
        batch_size,
    )


ACK_MESSAGE = """\
WITH msg AS (
    SELECT
        id,
        batch_id
    FROM pgjobq.messages
    WHERE queue_id = (SELECT id FROM pgjobq.queues WHERE name = $1) AND id = $2::uuid
), msg_notification AS (
    SELECT pg_notify('pgjobq.job_completed', $1 || ',' || CAST($2::uuid AS text))
)
DELETE
FROM pgjobq.messages
WHERE pgjobq.messages.id = (SELECT id FROM msg) AND 1 = (SELECT 1 FROM msg_notification);
"""


async def ack_message(
    conn: PoolOrConnection,
    queue_name: str,
    job_id: UUID,
) -> None:
    await conn.execute(ACK_MESSAGE, queue_name, job_id)  # type: ignore


NACK_MESSAGE = """\
WITH msg AS (
    SELECT pg_notify('pgjobq.new_job', $1)
)
UPDATE pgjobq.messages
SET available_at = now()
WHERE queue_id = (SELECT id FROM pgjobq.queues WHERE name = $1) AND id = $2 AND 1 = (SELECT 1 FROM msg);
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
        id
    FROM pgjobq.queues
    WHERE name = $1
), messages_for_update AS (
    SELECT
        queue_id,
        id,
        available_at
    FROM pgjobq.messages
    WHERE queue_id = (SELECT id FROM queue_info) AND id = any($2::uuid[])
    FOR UPDATE SKIP LOCKED
), updated_messages AS (
    UPDATE pgjobq.messages
    SET available_at = (
        now() + (
            SELECT ack_deadline
            FROM pgjobq.queues
            WHERE pgjobq.queues.id = (
                SELECT id FROM queue_info
            )
        )
    )
    WHERE (
        pgjobq.messages.queue_id = (SELECT id FROM queue_info)
        AND
        pgjobq.messages.id IN (SELECT id FROM messages_for_update)
    )
    RETURNING available_at AS next_ack_deadline
)
SELECT next_ack_deadline
FROM updated_messages
LIMIT 1;
"""


async def extend_ack_deadlines(
    conn: PoolOrConnection,
    queue_name: str,
    job_ids: Sequence[UUID],
) -> Optional[datetime]:
    return await conn.fetchval(EXTEND_ACK_DEADLINES, queue_name, list(job_ids))  # type: ignore
