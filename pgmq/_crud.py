from __future__ import annotations

import re
from datetime import timedelta

import asyncpg  # type: ignore

QUEUE_NAME_REGEX = re.compile(r"^[a-zA-Z0-9_-]+$")


async def create_queue(
    queue_name: str,
    pool: asyncpg.Pool,
    *,
    ack_deadline: timedelta = timedelta(seconds=10),
    max_delivery_attempts: int = 10,
    retention_period: timedelta = timedelta(days=7),
) -> bool:
    if not QUEUE_NAME_REGEX.match(queue_name):
        raise ValueError(
            "Invalid Queue Name."
            f" Queue names must conform to the pattern {QUEUE_NAME_REGEX.pattern}"
        )
    if ack_deadline < timedelta(seconds=1):
        raise ValueError("Minimum ack deadline is 1 second")
    return await pool.fetchval(  # type: ignore
        "SELECT pgmq.create_queue($1::text, $2, $3, $4)",
        queue_name,
        ack_deadline,
        max_delivery_attempts,
        retention_period,
    )


async def delete_queue(
    queue_name: str,
    pool: asyncpg.Pool,
) -> bool:
    return (
        await pool.fetchval(  # type: ignore
            "SELECT pgmq.delete_queue($1::text)",
            queue_name,
        )
    ) is True
