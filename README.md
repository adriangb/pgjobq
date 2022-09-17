# pgjobq

A job queue built on top of Postgres.

## Features

* Best effort most once delivery
* FIFO delivery for non-retried jobs
* Automatic redelivery of failed jobs
* Low latency delivery (near realtime, uses PostgreSQL's `NOTIFY` feature)
* Completion tracking (using `NOTIFY`)
* Fully typed async Python client (using [asyncpg])

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
        (send, rcv) = await stack.enter_async_context(
            connect_to_queue("myq", pool)
        )
        async with anyio.create_task_group() as tg:

            async def worker() -> None:
                async for msg_handle in rcv.poll():
                    async with msg_handle:
                        print("received")
                        # do some work
                        await anyio.sleep(1)
                        print("done processing")
                    print("acked")

            tg.start_soon(worker)
            tg.start_soon(worker)

            async with send.send(b'{"foo":"bar"}') as completion_handle:
                print("sent")
                await completion_handle()
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

[asyncpg]: https://github.com/MagicStack/asyncpg
