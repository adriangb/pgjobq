import itertools
import json
import random
import shutil
from collections import defaultdict
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any, AsyncIterator, Iterable, Sequence, TypeVar
from uuid import UUID

import anyio
import asyncpg  # type: ignore

from pgjobq import (
    Attribute,
    OutgoingJob,
    TelemetryHook,
    connect_to_queue,
    create_queue,
    migrate_to_latest_version,
)
from pgjobq.types import PoolOrConnection

dsn = "postgres://postgres:postgres@localhost:5433/postgres"

T = TypeVar("T")


class ExplainHook(TelemetryHook):
    def __init__(self, name: str | None = None) -> None:
        self.operations_recorded: dict[str, int] = defaultdict(int)
        self.name = name

    @asynccontextmanager
    async def on_query(
        self,
        queue_name: str,
        operation: str,
        query: str,
        conn: PoolOrConnection,
        args: Sequence[Any],
    ) -> AsyncIterator[PoolOrConnection]:
        if operation in self.operations_recorded:
            yield conn
            return
        self.operations_recorded[operation] += 1
        async with AsyncExitStack() as stack:
            if isinstance(conn, asyncpg.Pool):
                conn1: asyncpg.Connection = await stack.enter_async_context(conn.acquire())  # type: ignore
            else:
                conn1 = conn
            ps = await conn1.prepare(query)  # type: ignore
            explain = await ps.explain(*args, analyze=True)  # type: ignore
            tag = f"-{self.name}" if self.name else ""
            id = (
                ""
                if self.operations_recorded[operation] == 1
                else f"-{self.operations_recorded[operation]}"
            )
            dst = Path(__file__).parent / f"plans/{operation}{id}{tag}.json"
            with dst.open(mode="w") as f:
                f.write(json.dumps(explain))
            yield conn1


def grouper(n: int, iterable: Iterable[T]) -> Iterable[Iterable[T]]:
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


async def main() -> None:

    output = Path(__file__).parent / "plans"
    if output.exists():
        shutil.rmtree(output, ignore_errors=True)
    output.mkdir()

    async with AsyncExitStack() as stack:
        pool: asyncpg.Pool = await stack.enter_async_context(
            asyncpg.create_pool(dsn)  # type: ignore
        )
        try:
            await pool.execute("DROP SCHEMA pgjobq CASCADE")  # type: ignore
        except asyncpg.InvalidSchemaNameError:
            pass
        await migrate_to_latest_version(pool)
        await create_queue("test", pool, ack_deadline=timedelta(seconds=60))
        queue = await stack.enter_async_context(connect_to_queue("test", pool))

        # put in a baseline of jobs
        sent: set[UUID] = set()
        n = 0

        async def send_jobs(
            total: int, batch_size: int, n_deps: int, hook: ExplainHook | None = None
        ) -> None:
            nonlocal n
            for ids in grouper(batch_size, range(total)):
                jobs = [
                    OutgoingJob(
                        body=f'{{"id":{n}}}'.encode(),
                        attributes={"id": n},
                        dependencies=random.sample(list(sent), min(len(sent), 10)),
                    )
                    for n in ids
                ]
                async with queue.send(*jobs, telemetry_hook=hook) as handle:
                    sent.update(job for job in handle.jobs.keys())
                n += len(jobs)
                print(f"Published count: {n}")

        # seed some jobs
        await send_jobs(total=100_000, batch_size=100_000, n_deps=10)

        # benchmark sending a batch of jobs
        await send_jobs(
            total=1_000, batch_size=10_000, n_deps=0, hook=ExplainHook("batch-no-deps")
        )

        # benchmark sending a batch of jobs with a lot of deps
        await send_jobs(
            total=1_000,
            batch_size=10_000,
            n_deps=100,
            hook=ExplainHook("batch-with-deps"),
        )

        # benchmark sending a single job
        await send_jobs(
            total=1, batch_size=1_000, n_deps=100, hook=ExplainHook("single")
        )

        filter = Attribute("id").ge(1_000) & Attribute("id").ge(2_000)

        # receive a single job
        async with queue.receive(
            batch_size=1, telemetry_hook=ExplainHook("single")
        ) as msg_handle_rcv_stream:
            async with (await msg_handle_rcv_stream.receive()).acquire() as handle:
                sent.remove(handle.id)
        # receive a batch and process them
        async with queue.receive(
            batch_size=1_000, telemetry_hook=ExplainHook("batch")
        ) as msg_handle_rcv_stream:
            async with (await msg_handle_rcv_stream.receive()).acquire() as handle:
                sent.remove(handle.id)
        # receive and nack
        async with queue.receive(
            batch_size=1_000, telemetry_hook=ExplainHook("exception")
        ) as msg_handle_rcv_stream:
            try:
                async with (await msg_handle_rcv_stream.receive()).acquire():
                    raise Exception
            except Exception:
                pass

        # with a filter
        async with queue.receive(
            batch_size=1_000, filter=filter, telemetry_hook=ExplainHook("with-filter")
        ) as msg_handle_rcv_stream:
            async with (await msg_handle_rcv_stream.receive()).acquire() as handle:
                sent.remove(handle.id)

        # benchmark cancelling jobs
        await queue.cancel(
            *random.sample(list(sent), 100), telemetry_hook=ExplainHook("by-id")
        )

        # benchmark cancelling jobs
        await queue.cancel(filter, telemetry_hook=ExplainHook("with-filter"))


if __name__ == "__main__":
    anyio.run(main)
