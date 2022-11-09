# pgjobq

A job queue built on top of Postgres.

## Project status

Please do not use this for anything other than experimentation or inspiration.
At some point I may decide to support this long term (at which point this warning will be removed), but until then this is just a playground subject to breaking changes (including breaking schema changes).

## Purpose

Sometimes you have a Postgres database and need a queue.
You could stand up more infrastructure (SQS, Redis, etc), or you could use your existing database.
There are plenty of use cases for a persistent queue that do not require infinite scalability, snapshots or any of the other advanced features full fledged queues/event buses/job brokers have.

## Features

* Best effort at most once delivery (jobs are only delivered to one worker at a time)
* Automatic redelivery of failed jobs (even if your process crashes)
* Low latency delivery (near realtime, uses PostgreSQL's `NOTIFY` feature)
* Low latency completion tracking (using `NOTIFY`)
* Dead letter queuing
* Job attributes and attribute filtering
* Job dependencies (for processing DAG-like workflows or making jobs process FIFO)
* Persistent scheduled jobs (scheduled in the database, not the client application)
* Job cancellation (guaranteed for jobs in the queue and best effort for checked-out jobs)
* Bulk sending and polling to support large workloads
* Back pressure / bound queues
* Fully typed async Python client (using [asyncpg])
* Exponential back off for retries
* Telemetry hooks for sampling queries with EXPLAIN or integration with OpenTelemetry.

Possible features:

* Reply-to queues and response handling

## Examples

```python
from contextlib import AsyncExitStack

import anyio
import asyncpg  # type: ignore
from pgjobq import create_queue, connect_to_queue, migrate_to_latest_version

async def main() -> None:

    async with AsyncExitStack() as stack:
        pool: asyncpg.Pool = await stack.enter_async_context(
            asyncpg.create_pool(  # type: ignore
                "postgres://postgres:postgres@localhost/postgres"
            )
        )
        await migrate_to_latest_version(pool)
        await create_queue("myq", pool)
        queue = await stack.enter_async_context(
            connect_to_queue("myq", pool)
        )
        async with anyio.create_task_group() as tg:

            async def worker() -> None:
                async with queue.receive() as msg_handle_rcv_stream:
                    # receive a single job
                    async with (await msg_handle_rcv_stream.receive()).acquire():
                        print("received")
                        # do some work
                        await anyio.sleep(1)
                        print("done processing")
                        print("acked")

            tg.start_soon(worker)
            tg.start_soon(worker)

            async with queue.send(b'{"foo":"bar"}') as completion_handle:
                print("sent")
                await completion_handle.wait()
                print("completed")
                tg.cancel_scope.cancel()


if __name__ == "__main__":
    anyio.run(main)
    # prints:
    # "sent"
    # "received"
    # "done processing"
    # "acked"
    # "completed"
```

## Development

1. Clone the repo
2. Start a disposable PostgreSQL instance (e.g `docker run -it -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres`)
3. Run `make test`

[asyncpg]: https://github.com/MagicStack/asyncpg
