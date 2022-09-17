from __future__ import annotations

import sys
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)
from uuid import UUID, uuid4

import anyio
import asyncpg  # type: ignore
from anyio.abc import TaskStatus

KW: Dict[str, Any] = {}
if sys.version_info > (3, 10):  # pragma: no cover
    KW["slots"] = True


@dataclass(frozen=True, **KW)
class Message:
    id: UUID
    body: bytes


class Receive:
    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        queue_name: str,
    ) -> None:
        self._pool = pool
        self._queue_name = queue_name

    async def poll(
        self,
        batch_size: int = 1,
        poll_interval: float = 1,
    ) -> AsyncIterator[AsyncContextManager[Message]]:
        """Poll for a batch of jobs.

        Will wait until at least one and up to `batch_size` jobs are available
        by periodically checking the queue every `poll_interval` seconds.

        When a new job is put on the queue a notification is sent that will cause
        immediate polling, thus in practice the latency will be much lower than
        poll interval.
        This mechanism is however not 100% reliable so worst case latency is still
        `poll_interval`.

        Args:
            batch_size (int, optional): maximum number of messages to gether.
                Defaults to 1.
            poll_interval (float, optional): interval between polls of the queue.
                Defaults to 1.

        Returns:
            AsyncIterator[AsyncContextManager[Message]]: An iterator over a batch of messages.
            Each message is wrapped by a context manager.
            If you exit with an error the message will be nacked.
            If you exit without an error it will be acked.
            As long as you are in the context manager the visibility timeout weill be
            continually extended.
        """
        listener_channel = f"new_message_{self._queue_name}"
        async with AsyncExitStack() as stack:
            jobs_conn: "asyncpg.Connection" = await stack.enter_async_context(
                self._pool.acquire()  # type: ignore
            )

            async def get_jobs() -> Sequence[Mapping[str, Any]]:
                return await jobs_conn.fetch(  # type: ignore
                    "SELECT * FROM pgjobq.poll_for_messages($1::text, $2::smallint)",
                    self._queue_name,
                    batch_size,
                )

            jobs = await get_jobs()
            while not jobs:
                new_job = anyio.Event()
                # wait for a new job to be published or the poll interval to expire

                async def skip_forward_if_new_msg(*_: Any) -> None:
                    new_job.set()

                await jobs_conn.add_listener(  # type: ignore
                    listener_channel, skip_forward_if_new_msg
                )

                stack.push_async_callback(
                    jobs_conn.remove_listener,  # type: ignore
                    listener_channel,
                    skip_forward_if_new_msg,
                )

                async def skip_forward_if_timeout() -> None:
                    await anyio.sleep(poll_interval)
                    new_job.set()

                async with anyio.create_task_group() as tg:
                    tg.start_soon(skip_forward_if_timeout)
                    await new_job.wait()
                    tg.cancel_scope.cancel()

                jobs = await get_jobs()

        for job in jobs:

            @asynccontextmanager
            async def cm(job_record: Mapping[str, Any] = job) -> AsyncIterator[Message]:
                message = Message(id=job_record["id"], body=job_record["body"])
                next_ack_deadline: datetime = job_record["next_ack_deadline"]
                job_conn: asyncpg.Connection
                async with self._pool.acquire() as job_conn:  # type: ignore
                    try:
                        async with anyio.create_task_group() as msg_tg:

                            async def extend_ack_deadline(
                                *, task_status: TaskStatus = anyio.TASK_STATUS_IGNORED
                            ) -> None:
                                nonlocal next_ack_deadline
                                task_status.started()
                                # min ack deadline is 1 sec so 0.5 is a reasonable lower bound
                                delay = max(
                                    (
                                        datetime.now()
                                        - next_ack_deadline
                                        - timedelta(seconds=1)
                                    ).total_seconds(),
                                    0.5,
                                )
                                await anyio.sleep(delay)
                                next_ack_deadline = await job_conn.fetchval(  # type: ignore
                                    "SELECT pgjobq.extend_ack_deadline($1, $2)",
                                    self._queue_name,
                                    message.id,
                                )
                                msg_tg.start_soon(extend_ack_deadline)

                            await msg_tg.start(extend_ack_deadline)

                            yield message
                            with anyio.CancelScope(shield=True):
                                msg_tg.cancel_scope.cancel()
                                await job_conn.execute(  # type: ignore
                                    "SELECT pgjobq.ack_message($1, $2)",
                                    self._queue_name,
                                    message.id,
                                )
                    except Exception:
                        await job_conn.execute(  # type: ignore
                            "SELECT pgjobq.nack_message($1, $2)",
                            self._queue_name,
                            message.id,
                        )
                        raise

            yield cm()


WaitForDoneHandle = Callable[[], Awaitable[None]]


class Send:
    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        queue_name: str,
    ) -> None:
        self._pool = pool
        self._queue_name = queue_name

    def send(self, body: bytes) -> AsyncContextManager[WaitForDoneHandle]:
        """Put a job on the queue.

        You _must_ enter the context manager but awaiting the completion
        handle is optional.

        Args:
            body (bytes): arbitrary bytes, the body of the job.

        Returns:
            AsyncContextManager[WaitForDoneHandle]: A context manager that
        """
        done = anyio.Event()
        # create the job id application side
        # so that we can start listening before we send
        job_id = uuid4()
        done_listener_channel = f"done_{self._queue_name}_{job_id}"

        @asynccontextmanager
        async def cm() -> AsyncIterator[WaitForDoneHandle]:
            conn: asyncpg.Connection
            async with self._pool.acquire() as conn:  # type: ignore

                async def handle_done_notification(*_: Any) -> None:
                    done.set()

                await conn.add_listener(  # type: ignore
                    done_listener_channel, handle_done_notification
                )

                try:
                    res: "Optional[int]" = await conn.fetchval(  # type: ignore
                        "SELECT pgjobq.publish_message($1, $2, $3)",
                        self._queue_name,
                        job_id,
                        body,
                    )
                    if res is None:
                        raise LookupError("Queue not found, call create_queue() first")
                    yield done.wait
                finally:
                    await conn.remove_listener(  # type: ignore
                        done_listener_channel, handle_done_notification
                    )

        return cm()


@asynccontextmanager
async def connect_to_queue(
    queue_name: str,
    pool: asyncpg.Pool,
) -> AsyncIterator[Tuple[Send, Receive]]:
    send = Send(pool=pool, queue_name=queue_name)
    rcv = Receive(pool=pool, queue_name=queue_name)
    async with anyio.create_task_group() as tg:

        async def cleanup_cb() -> None:
            while True:
                await pool.execute(  # type: ignore
                    "SELECT pgjobq.cleanup_dead_messages()",
                )
                await anyio.sleep(1)

        tg.start_soon(cleanup_cb)

        yield send, rcv

        tg.cancel_scope.cancel()
