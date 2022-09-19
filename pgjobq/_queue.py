from __future__ import annotations

from collections import defaultdict
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import datetime, timedelta
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Hashable,
    Mapping,
    Optional,
    Set,
)
from uuid import UUID, uuid4

import anyio
import asyncpg  # type: ignore
from anyio.abc import TaskStatus

from pgjobq.api import CompletionHandle, Message
from pgjobq.api import Queue as AbstractQueue
from pgjobq.sql._functions import (
    ack_message,
    extend_ack_deadline,
    nack_message,
    poll_for_messages,
    publish_messages,
)


class Queue(AbstractQueue):
    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        queue_name: str,
        completion_callbacks: Dict[str, Dict[Hashable, Callable[[], Awaitable[None]]]],
        new_job_callbacks: Dict[str, Set[Callable[[], Awaitable[None]]]],
    ) -> None:
        self._pool = pool
        self._queue_name = queue_name
        self._completion_callbacks = completion_callbacks
        self._new_job_callbacks = new_job_callbacks

    async def poll(
        self,
        batch_size: int = 1,
        poll_interval: float = 1,
        fifo: bool = False,
    ) -> AsyncIterator[AsyncContextManager[Message]]:
        async with AsyncExitStack() as stack:
            jobs_conn: "asyncpg.Connection" = await stack.enter_async_context(
                self._pool.acquire()  # type: ignore
            )

            jobs = await poll_for_messages(
                jobs_conn,
                queue_name=self._queue_name,
                fifo=fifo,
                batch_size=batch_size,
            )
            while not jobs:
                new_job = anyio.Event()
                # wait for a new job to be published or the poll interval to expire

                async def set_wrapper() -> None:
                    # asyncpg calls this as a coroutine
                    # which anyio allows but raises a warning for
                    new_job.set()

                self._new_job_callbacks[self._queue_name].add(set_wrapper)

                async def skip_forward_if_timeout() -> None:
                    await anyio.sleep(poll_interval)
                    new_job.set()

                try:
                    async with anyio.create_task_group() as tg:
                        tg.start_soon(skip_forward_if_timeout)
                        await new_job.wait()
                        tg.cancel_scope.cancel()
                finally:
                    self._new_job_callbacks[self._queue_name].discard(new_job.set)

                jobs = await poll_for_messages(
                    jobs_conn,
                    queue_name=self._queue_name,
                    fifo=fifo,
                    batch_size=batch_size,
                )

        for job in jobs:

            @asynccontextmanager
            async def cm(job_record: Mapping[str, Any] = job) -> AsyncIterator[Message]:
                message = Message(id=job_record["id"], body=job_record["body"])
                next_ack_deadline: datetime = job_record["next_ack_deadline"]
                job_conn: asyncpg.Connection
                async with self._pool.acquire() as job_conn:  # type: ignore
                    try:
                        async with anyio.create_task_group() as msg_tg:

                            async def extend_ack(
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
                                next_ack_deadline = await extend_ack_deadline(
                                    job_conn,
                                    self._queue_name,
                                    message.id,
                                )
                                msg_tg.start_soon(extend_ack)

                            await msg_tg.start(extend_ack)

                            yield message
                            with anyio.CancelScope(shield=True):
                                msg_tg.cancel_scope.cancel()
                                await ack_message(
                                    job_conn,
                                    self._queue_name,
                                    message.id,
                                )
                    except Exception:
                        await nack_message(
                            job_conn,
                            self._queue_name,
                            message.id,
                        )
                        raise

            yield cm()

    def send(
        self, body: bytes, *, delay: Optional[timedelta] = None
    ) -> AsyncContextManager[CompletionHandle]:
        done = anyio.Event()
        # create the job id application side
        # so that we can start listening before we send
        job_id = uuid4()

        @asynccontextmanager
        async def cm() -> AsyncIterator[CompletionHandle]:
            conn: asyncpg.Connection
            async with self._pool.acquire() as conn:  # type: ignore

                async def handle_done_notification() -> None:
                    done.set()

                self._completion_callbacks[self._queue_name][
                    job_id
                ] = handle_done_notification

                try:
                    await publish_messages(
                        conn,
                        queue_name=self._queue_name,
                        message_id=job_id,
                        message_body=body,
                        delay=delay,
                    )
                    yield done.wait
                finally:
                    self._completion_callbacks[self._queue_name].pop(job_id)

        return cm()


@asynccontextmanager
async def connect_to_queue(
    queue_name: str,
    pool: asyncpg.Pool,
) -> AsyncIterator[AbstractQueue]:
    """Connect to an existing queue.

    This is the main way to interact with an existing Queue; they cannot
    be constructed directly.

    Args:
        queue_name (str): The name of the queue passed to `create_queue`.
        pool (asyncpg.Pool): a database connection pool.

    Returns:
        AsyncContextManager[AbstractQueue]: A context manager yielding an AbstractQueue
    """
    completion_callbacks: Dict[
        str, Dict[Hashable, Callable[[], Awaitable[None]]]
    ] = defaultdict(dict)
    new_job_callbacks: Dict[str, Set[Callable[[], Awaitable[None]]]] = defaultdict(set)

    background_conn: asyncpg.Connection
    background_conn_lock = anyio.Lock()
    async with pool.acquire() as background_conn:  # type: ignore
        async with anyio.create_task_group() as tg:

            async def cleanup_cb() -> None:
                while True:
                    async with background_conn_lock:
                        await background_conn.execute(  # type: ignore
                            "SELECT pgjobq.cleanup_dead_messages()",
                        )
                    await anyio.sleep(1)

            async def process_completion_notification(
                conn: asyncpg.Connection,
                pid: int,
                channel: str,
                payload: str,
            ) -> None:
                queue_name, message_id, *_ = payload.split(",")
                cb = completion_callbacks[queue_name].get(UUID(message_id), None)
                if cb is not None:
                    await cb()

            async def process_new_job_notification(
                conn: asyncpg.Connection,
                pid: int,
                channel: str,
                payload: str,
            ) -> None:
                queue_name, *_ = payload.split(",")
                for cb in new_job_callbacks[queue_name]:
                    await cb()

            await background_conn.add_listener(  # type: ignore
                channel="pgjobq.job_completed",
                callback=process_completion_notification,
            )

            await background_conn.add_listener(  # type: ignore
                channel="pgjobq.new_job",
                callback=process_new_job_notification,
            )

            tg.start_soon(cleanup_cb)

            queue = Queue(
                pool=pool,
                queue_name=queue_name,
                completion_callbacks=completion_callbacks,
                new_job_callbacks=new_job_callbacks,
            )

            try:
                yield queue
            finally:
                async with background_conn_lock:
                    await background_conn.remove_listener(  # type: ignore
                        channel="pgjobq.new_job",
                        callback=process_new_job_notification,
                    )
                    await background_conn.remove_listener(  # type: ignore
                        channel="pgjobq.job_completed",
                        callback=process_completion_notification,
                    )
                tg.cancel_scope.cancel()
