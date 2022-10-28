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


DEFAULT_QUEUE_OPTIONS = QueueOptions()


CREATE = """\
INSERT INTO pgmq.queues(name, ack_deadline, max_delivery_attempts, retention_period)
VALUES ($1, $2, $3, $4)
ON CONFLICT DO NOTHING
RETURNING id;
"""


async def _create(
    queue_name: str,
    conn: asyncpg.Connection,
    options: QueueOptions,
) -> bool:
    res = await conn.fetchval(  # type: ignore
        CREATE,
        queue_name,
        options.ack_deadline,
        options.max_delivery_attempts,
        options.retention_period,
    )
    return res is not None


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
SELECT
    (SELECT id FROM parent),
    (SELECT id FROM child),
    (SELECT id FROM relationship_type)
FROM parent
ON CONFLICT DO NOTHING
"""


async def _link(
    conn: asyncpg.Connection,
    parent_queue_name: str,
    child_queue_name: str,
    link_type: Literal["dlq"],
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
    ack_deadline: timedelta = DEFAULT_QUEUE_OPTIONS.ack_deadline,
    max_delivery_attempts: int = DEFAULT_QUEUE_OPTIONS.max_delivery_attempts,
    retention_period: timedelta = DEFAULT_QUEUE_OPTIONS.retention_period,
    dlq_options: Optional[QueueOptions] = QueueOptions(),
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
    res = await pool.fetchval(  # type: ignore
        DELETE,
        queue_name,
    )
    return res is not None
