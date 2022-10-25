from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal, Optional

import asyncpg  # type: ignore

QUEUE_NAME_REGEX = re.compile(r"^[a-zA-Z0-9_-]+$")


@dataclass(frozen=True)
class QueueOptions:
    ack_deadline: timedelta = timedelta(seconds=10)
    max_delivery_attempts: int = 10
    retention_period: timedelta = timedelta(days=7)

    def __post_init__(self) -> None:
        if self.ack_deadline < timedelta(seconds=1):
            raise ValueError("Minimum ack deadline is 1 second")
        if self.max_delivery_attempts < 1:
            raise ValueError("Minimum delivery attempts is 1")


CREATE = """\
INSERT INTO pgmq.queues(name, ack_deadline, max_delivery_attempts, retention_period)
VALUES ($1, $2, $3, $4)
ON CONFLICT DO NOTHING
RETURNING 1;
"""


async def _create(
    queue_name: str,
    conn: asyncpg.Connection,
    options: QueueOptions,
) -> bool:
    return (
        await conn.fetchval(  # type: ignore
            CREATE,
            queue_name,
            options.ack_deadline,
            options.max_delivery_attempts,
            options.retention_period,
        )
        is not None
    )


LINK = """\
WITH parent AS (
    SELECT id
    FROM pgmq.queues
    WHERE name = $1
), child AS (
    SELECT id
    FROM pgmq.queues
    WHERE name = $2
), relationship_type AS (
    SELECT id
    FROM pgmq.queue_link_types
    WHERE name = $3
)
INSERT INTO pgmq.queue_links(parent_id, child_id, link_type_id)
SELECT parent.id, child.id, relationship_type.id
FROM parent
LEFT JOIN child ON 1 = 1
LEFT JOIN relationship_type ON 1 = 1
ON CONFLICT DO NOTHING
"""


async def _link(
    conn: asyncpg.Connection,
    parent_queue_name: str,
    child_queue_name: str,
    link_type: Literal["dlq", "completed"],
) -> None:
    await conn.fetchval(  # type: ignore
        LINK,
        parent_queue_name,
        child_queue_name,
        link_type,
    )


async def create_queue(
    queue_name: str,
    pool: asyncpg.Pool,
    *,
    ack_deadline: timedelta = timedelta(seconds=10),
    max_delivery_attempts: int = 10,
    retention_period: timedelta = timedelta(days=7),
    dlq_options: Optional[QueueOptions] = QueueOptions(),
    completion_queue_options: Optional[QueueOptions] = None,
) -> bool:
    if not QUEUE_NAME_REGEX.match(queue_name):
        raise ValueError(
            "Invalid Queue Name."
            f" Queue names must conform to the pattern {QUEUE_NAME_REGEX.pattern}"
        )
    options = QueueOptions(
        ack_deadline=ack_deadline,
        max_delivery_attempts=max_delivery_attempts,
        retention_period=retention_period,
    )
    conn: asyncpg.Connection
    async with pool.acquire() as conn:  # type: ignore
        async with conn.transaction():  # type: ignore
            created = await _create(
                queue_name,
                conn,
                options,
            )
            if dlq_options is not None:
                dlq_name = f"{queue_name}_dlq"
                await _create(
                    f"{queue_name}_dlq",
                    conn,
                    dlq_options,
                )
                await _link(
                    conn,
                    parent_queue_name=queue_name,
                    child_queue_name=dlq_name,
                    link_type="dlq",
                )
            if completion_queue_options is not None:
                completion_queue_name = f"{queue_name}_completed"
                await _create(
                    completion_queue_name,
                    conn,
                    completion_queue_options,
                )
                await _link(
                    conn,
                    parent_queue_name=queue_name,
                    child_queue_name=completion_queue_name,
                    link_type="completed",
                )
        return created


DELETE = """\
DELETE
FROM pgmq.queues
WHERE name = $1
RETURNING 1
"""


async def delete_queue(
    queue_name: str,
    pool: asyncpg.Pool,
) -> bool:
    return (
        await pool.fetchval(  # type: ignore
            DELETE,
            queue_name,
        )
    ) is not None
