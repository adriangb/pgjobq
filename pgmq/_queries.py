from __future__ import annotations

import sys
from datetime import datetime
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
    SELECT
        id AS queue_id,
        max_delivery_attempts,
        retention_period
    FROM pgmq.queues
    WHERE name = $1
)
INSERT INTO pgmq.messages(
    queue_id,
    id,
    expires_at,
    delivery_attempts_remaining,
    available_at,
    body
)
SELECT
    queue_id,
    unnest($3::uuid[]),
    COALESCE($2, now()::timestamp) + retention_period,
    max_delivery_attempts,
    COALESCE($2, now()::timestamp),
    unnest($4::bytea[])
FROM queue_info
RETURNING queue_id;
"""


async def publish_messages_from_bytes(
    conn: PoolOrConnection,
    *,
    queue_name: str,
    ids: List[UUID],
    bodies: List[bytes],
    schedule_at: Optional[datetime],
) -> None:
    res: Optional[int] = await conn.fetchval(  # type: ignore
        PUBLISH_MESSAGES,
        queue_name,
        schedule_at,
        ids,
        bodies,
    )
    if res is None:
        raise QueueDoesNotExist(queue_name=queue_name)


POLL_FOR_MESSAGES = """\
WITH queue_info AS (
    SELECT
        id,
        ack_deadline,
        max_delivery_attempts
    FROM pgmq.queues
    WHERE name = $1
), selected_messages AS (
    SELECT
        id,
        delivery_attempts_remaining
    FROM pgmq.messages
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
)
UPDATE pgmq.messages
SET
    available_at = now() + (SELECT ack_deadline FROM queue_info),
    delivery_attempts_remaining = selected_messages.delivery_attempts_remaining - 1
FROM selected_messages
WHERE pgmq.messages.id = selected_messages.id
RETURNING pgmq.messages.id, available_at AS next_ack_deadline, body
"""


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
        POLL_FOR_MESSAGES,
        queue_name,
        batch_size,
    )


ACK_MESSAGE = """\
WITH queue_info AS (
    SELECT
        id
    FROM pgmq.queues
    WHERE name = $1
)
DELETE
FROM pgmq.messages
WHERE pgmq.messages.id = $2
"""


async def ack_message(
    conn: PoolOrConnection,
    queue_name: str,
    message_id: UUID,
) -> None:
    await conn.execute(ACK_MESSAGE, queue_name, message_id)  # type: ignore


NACK_MESSAGE = """\
WITH queue_info AS (
    SELECT
        id,
        max_delivery_attempts,
        retention_period
    FROM pgmq.queues
    WHERE name = $1
), updated_messages AS (
    UPDATE pgmq.messages
    -- make it available in the past to avoid race conditions with extending deadlines
    -- which check to make sure the message is still available before extending
    SET
        available_at = now() - '1 second'::interval
    WHERE (
        queue_id = (SELECT id FROM queue_info)
        AND
        id = $2
        AND
        delivery_attempts_remaining != 0
    )
), deleted_messages AS (
    DELETE FROM pgmq.messages
    WHERE (
        queue_id = (SELECT id FROM queue_info)
        AND
        id = $2
        AND
        delivery_attempts_remaining == 0
    )
    RETURNING id, queue_id, body
), dlq_messages AS (
    SELECT
        deleted_messages.id AS id,
        deleted_messages.body AS body,
        now()::timestamp + dlq.retention_period pgmq.queues.retention_period AS expires_at,
        dlq.max_delivery_attempts AS max_delivery_attempts
    FROM deleted_messages d
    LEFT JOIN pgmq.queues ON d.queue_id = pgmq.queues.id
    LEFT JOIN LATERAL (
        SELECT
            pgmq.queue_links.child_id AS queue_id,
            pgmq.queues.retention_period AS retention_period,
            pgmq.queues.max_delivery_attempts AS max_delivery_attempts
        FROM pgmq.queue_links
        WHERE (
            parent_id = d.queue_id
            AND
            link_type_id = (SELECT id FROM pgmq.queue_link_types WHERE name = 'dlq')
        )
        LEFT JOIN pgmq.queues ON pgmq.queue_links.child_id = pgmq.queues.id
    ) dlq ON dlq.dlq_queue_id = deleted_messages.queue_id
)
INSERT INTO pgmq.messages(
    queue_id,
    id,
    expires_at,
    delivery_attempts_remaining,
    available_at,
    body
)
SELECT
    queue_id,
    id,
   expires_at
    max_delivery_attempts,
    delivery_attempts_remaining,
    body
FROM dlq_messages;
"""


NACK_MESSAGE = """\
WITH queue_info AS (
    SELECT
        id,
        max_delivery_attempts,
        retention_period
    FROM pgmq.queues
    WHERE name = $1
), updated_messages AS (
    UPDATE pgmq.messages
    -- make it available in the past to avoid race conditions with extending deadlines
    -- which check to make sure the message is still available before extending
    SET
        available_at = now() - '1 second'::interval
    WHERE (
        queue_id = (SELECT id FROM queue_info)
        AND
        id = $2
        AND
        delivery_attempts_remaining != 0
    )
), deleted_messages AS (
    DELETE FROM pgmq.messages
    WHERE (
        queue_id = (SELECT id FROM queue_info)
        AND
        id = $2
        AND
        delivery_attempts_remaining = 0
    )
    RETURNING id, queue_id, body
), dlq_messages AS (
    SELECT
        d.id AS id,
        d.body AS body,
        dlq.queue_id,
        now()::timestamp + dlq.retention_period AS expires_at,
        dlq.max_delivery_attempts AS max_delivery_attempts
    FROM deleted_messages d
    JOIN LATERAL (
        SELECT
            pgmq.queue_links.child_id AS queue_id,
            pgmq.queues.retention_period AS retention_period,
            pgmq.queues.max_delivery_attempts AS max_delivery_attempts
        FROM pgmq.queue_links
        LEFT JOIN pgmq.queues ON (
            parent_id = d.queue_id
            AND
            link_type_id = (SELECT id FROM pgmq.queue_link_types WHERE name = 'dlq')
            AND
            pgmq.queue_links.child_id = pgmq.queues.id
        )
    ) dlq ON true
)
INSERT INTO pgmq.messages(
    queue_id,
    id,
    expires_at,
    delivery_attempts_remaining,
    available_at,
    body
)
SELECT
    queue_id,
    id,
    expires_at,
    max_delivery_attempts,
    now()::timestamp,
    body
FROM dlq_messages;
"""


async def nack_message(
    conn: PoolOrConnection,
    queue_name: str,
    message_id: UUID,
) -> None:
    await conn.execute(NACK_MESSAGE, queue_name, message_id)  # type: ignore


EXTEND_ACK_DEADLINES = """\
WITH queue_info AS (
    SELECT
        id,
        ack_deadline
    FROM pgmq.queues
    WHERE name = $1
)
UPDATE pgmq.messages
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
    -- skip any messages that already expired
    -- this avoids race conditions between
    -- extending deadlines and nacking
    available_at > now()
)
RETURNING available_at AS next_ack_deadline;
"""


async def extend_ack_deadlines(
    conn: PoolOrConnection,
    queue_name: str,
    message_ids: Sequence[UUID],
) -> Optional[datetime]:
    return await conn.fetchval(EXTEND_ACK_DEADLINES, queue_name, list(message_ids))  # type: ignore


GET_STATISTICS = """\
WITH queue_info AS (
    SELECT
        id
    FROM pgmq.queues
    WHERE name = $1
)
SELECT count(*) AS messages
FROM pgmq.messages
WHERE queue_id = (SELECT id FROM queue_info)
"""


class QueueStatisticsRecord(TypedDict):
    messages: int


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
    FROM pgmq.queues
    WHERE name = $1
)
SELECT
    id
FROM input
WHERE NOT EXISTS(
    SELECT *
    FROM pgmq.messages
    WHERE (
        pgmq.messages.id = input.id
        AND
        pgmq.messages.queue_id = (SELECT id FROM queue_info)
    )
)
"""


async def get_completed_messages(
    conn: PoolOrConnection,
    queue_name: str,
    message_ids: List[UUID],
) -> Sequence[UUID]:
    records: List[Record] = await conn.fetch(GET_COMPLETED_JOBS, queue_name, message_ids)  # type: ignore
    return [record["id"] for record in records]


CLEANUP_DEAD_MESSAGES = """\
DELETE
FROM pgmq.messages
USING pgmq.queues
WHERE (
    available_at < now()
    AND
    expires_at < now()
);
"""


async def cleanup_dead_messages(
    conn: PoolOrConnection,
) -> None:
    await conn.execute(CLEANUP_DEAD_MESSAGES)  # type: ignore
