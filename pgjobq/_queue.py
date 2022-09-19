from __future__ import annotations

import sys
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Callable,
    Dict,
    Hashable,
    List,
    Optional,
    Set,
)
from uuid import UUID, uuid4

import anyio
import asyncpg  # type: ignore
from anyio.abc import TaskGroup

from pgjobq.api import CompletionHandle, JobHandle, JobHandleIterator, Message
from pgjobq.api import Queue as AbstractQueue
from pgjobq.sql._functions import (
    ack_message,
    extend_ack_deadline,
    nack_message,
    poll_for_messages,
    publish_messages,
)

DATACLASSES_KW: Dict[str, Any] = {}
if sys.version_info >= (3, 10):  # pragma: no cover
    DATACLASSES_KW["slots"] = True


@dataclass(**DATACLASSES_KW)
class JobManager:
    pool: asyncpg.Pool
    connection: asyncpg.Connection
    message: Message
    queue_name: str
    terminate: anyio.Event
    terminated: anyio.Event
    success: bool = False

    @asynccontextmanager
    async def wrap_worker(self) -> AsyncIterator[Message]:
        if self.terminate.is_set():
            raise RuntimeError(
                "Attempted to acquire a handle to a job that was"
                " completed, failed or is no longer available"
                " because Queue.poll() was exited"
            )
        try:
            yield self.message
            self.success = True
            self.terminate.set()
        except Exception:
            # handle exceptions after entering the message's context manager
            self.terminate.set()
            raise
        finally:
            await self.terminated.wait()

    async def manage(
        self, next_ack_deadline: datetime, completion_cb: Callable[[JobManager], None]
    ) -> None:
        try:

            async def work(deadline: datetime) -> None:
                while True:
                    # min ack deadline is 1 sec so 0.5 should be a reasonable lower bound
                    delay = max(
                        (
                            datetime.now() - deadline - timedelta(seconds=1)
                        ).total_seconds(),
                        0.5,
                    )
                    await anyio.sleep(delay)
                    try:
                        deadline = await extend_ack_deadline(
                            self.connection,
                            self.queue_name,
                            self.message.id,
                        )
                    except LookupError:
                        # message was acked while we were updating it
                        return

            async def cancel_if_terminated(tg: TaskGroup) -> None:
                await self.terminate.wait()
                tg.cancel_scope.cancel()

            async with anyio.create_task_group() as tg:
                tg.start_soon(cancel_if_terminated, tg)
                tg.start_soon(work, next_ack_deadline)
        finally:
            try:
                if self.success:
                    await ack_message(
                        self.connection,
                        self.queue_name,
                        self.message.id,
                    )
                else:
                    await nack_message(
                        self.connection,
                        self.queue_name,
                        self.message.id,
                    )
            finally:
                with anyio.CancelScope(shield=True):
                    await self.pool.release(self.connection)  # type: ignore
                completion_cb(self)
                self.terminated.set()

    # need to implement this since we override __hash__
    # but it should never be called
    def __eq__(self, *_: Any) -> bool:  # pragma: no cover
        return False

    def __hash__(self) -> int:
        return id(self)


class Queue(AbstractQueue):
    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        queue_name: str,
        completion_callbacks: Dict[str, Dict[Hashable, anyio.Event]],
        new_job_callbacks: Dict[str, Set[anyio.Event]],
    ) -> None:
        self._pool = pool
        self._queue_name = queue_name
        self._completion_callbacks = completion_callbacks
        self._new_job_callbacks = new_job_callbacks

    @asynccontextmanager
    async def poll(
        self,
        batch_size: int = 1,
        poll_interval: float = 1,
        fifo: bool = False,
    ) -> AsyncIterator[JobHandleIterator]:
        send, rcv = anyio.create_memory_object_stream(0, JobHandle)  # type: ignore

        pending_jobs: Set[JobManager] = set()

        unreleased_connections: Set[asyncpg.Connection] = set()

        async def gather_connections() -> None:
            # get a connection for each message we'll have in the batch
            # we need to do this upfront to make sure we can avoid race conditions in
            # acquiring connections to extend ack deadlines
            for _ in range(batch_size):
                unreleased_connections.add(await self._pool.acquire())  # type: ignore

        await gather_connections()

        def mark_manager_as_completed(manager: JobManager) -> None:
            pending_jobs.discard(manager)
            unreleased_connections.discard(manager.connection)

        async def gather_jobs() -> None:
            jobs = await poll_for_messages(
                self._pool,
                queue_name=self._queue_name,
                fifo=fifo,
                batch_size=batch_size,
            )
            while True:
                while not jobs:
                    continue_to_next_iter = anyio.Event()
                    # wait for a new job to be published or the poll interval to expire

                    self._new_job_callbacks[self._queue_name].add(continue_to_next_iter)

                    async def skip_forward_if_timeout() -> None:
                        await anyio.sleep(poll_interval)
                        continue_to_next_iter.set()

                    try:
                        async with anyio.create_task_group() as gather_tg:
                            gather_tg.start_soon(skip_forward_if_timeout)
                            await continue_to_next_iter.wait()
                            gather_tg.cancel_scope.cancel()
                    finally:
                        self._new_job_callbacks[self._queue_name].discard(
                            continue_to_next_iter
                        )

                    jobs = await poll_for_messages(
                        self._pool,
                        queue_name=self._queue_name,
                        fifo=fifo,
                        batch_size=batch_size,
                    )

                managers: List[JobManager] = []

                # if we didn't get batch_size jobs we'll have unused connections
                # release them now to make them available
                # since we may have been cancelled at some point between acquiring them
                # and now  this may not run, hence we also release them when exiting Queue.poll()
                for _ in range(batch_size - len(jobs)):
                    poll_tg.start_soon(self._pool.release, unreleased_connections.pop())  # type: ignore

                # start extending ack deadlines before handing control over to workers
                # so that processing each job takes > ack deadline messages aren't lost
                for job in jobs:
                    message = Message(id=job["id"], body=job["body"])
                    manager = JobManager(
                        self._pool,
                        unreleased_connections.pop(),
                        message,
                        self._queue_name,
                        anyio.Event(),
                        anyio.Event(),
                    )
                    managers.append(manager)
                    pending_jobs.add(manager)
                    # this keeps Queue.poll() open until all in-flight jobs
                    # have completed since manage_ack_deadline will return as soon
                    # as one of two things happens:
                    # - JobManager.wrap_worker() sets JobManager.terminated (it may never even run)
                    # - Queue.poll() exits and sets JobManager.terminated for all in-flight jobs
                    #   that haven't started running
                    job_tg.start_soon(
                        manager.manage,
                        job["next_ack_deadline"],
                        mark_manager_as_completed,
                    )

                for manager in managers:
                    await send.send(manager.wrap_worker())

                jobs = []
                await gather_connections()

        async with anyio.create_task_group() as job_tg:
            try:
                async with anyio.create_task_group() as poll_tg:
                    poll_tg.start_soon(gather_jobs)
                    try:
                        yield rcv
                    finally:
                        # stop polling
                        poll_tg.cancel_scope.cancel()
            finally:
                # terminate any in flight jobs that haven't started work
                # - jobs that haven't been started become unavailable
                #   and raise an exception if work is started on them
                # - jobs that were started but not finished continue
                #   to run unmolested and we wait for them to complete
                #   before exiting
                with anyio.CancelScope(shield=True):
                    for connection in unreleased_connections:
                        await self._pool.release(connection)  # type: ignore
                    for manager in pending_jobs.copy():
                        manager.terminate.set()
                        await manager.terminated.wait()

    def send(
        self, body: bytes, *bodies: bytes, delay: Optional[timedelta] = None
    ) -> AsyncContextManager[CompletionHandle]:
        # create the job id application side
        # so that we can start listening before we send
        all_bodies = [body, *bodies]
        job_ids = [uuid4() for _ in range(len(all_bodies))]
        done_events = {id: anyio.Event() for id in job_ids}
        batch_id = uuid4()

        @asynccontextmanager
        async def cm() -> AsyncIterator[CompletionHandle]:
            conn: asyncpg.Connection
            async with self._pool.acquire() as conn:  # type: ignore
                for job_id, done_event in done_events.items():
                    self._completion_callbacks[self._queue_name][job_id] = done_event
                try:
                    await publish_messages(
                        conn,
                        queue_name=self._queue_name,
                        batch_id=batch_id,
                        message_ids=job_ids,
                        message_bodies=all_bodies,
                        delay=delay,
                    )

                    async def done_handle() -> None:
                        async with anyio.create_task_group() as tg:
                            for event in done_events.values():
                                tg.start_soon(event.wait)
                        return

                    yield done_handle
                finally:
                    for job_id, done_event in done_events.items():
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
    completion_callbacks: Dict[str, Dict[Hashable, anyio.Event]] = defaultdict(dict)
    new_job_callbacks: Dict[str, Set[anyio.Event]] = defaultdict(set)

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
                queue_name, job_id = payload.split(",")
                cb = completion_callbacks[queue_name].get(UUID(job_id), None)
                if cb is not None:
                    cb.set()

            async def process_new_job_notification(
                conn: asyncpg.Connection,
                pid: int,
                channel: str,
                payload: str,
            ) -> None:
                queue_name, *_ = payload.split(",")
                for event in new_job_callbacks[queue_name]:
                    event.set()

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
