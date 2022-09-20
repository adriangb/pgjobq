from __future__ import annotations

import sys
from collections import defaultdict
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
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
    extend_ack_deadlines,
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
    message: Message
    queue_name: str
    terminate: anyio.Event
    terminated: anyio.Event
    pending_jobs: Set[JobManager]
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
            self.pending_jobs.discard(self)
            self.success = True
            self.terminate.set()
        except Exception:
            self.pending_jobs.discard(self)
            # handle exceptions after entering the message's context manager
            self.terminate.set()
            raise
        finally:
            await self.terminated.wait()

    async def shutdown(self) -> None:
        await self.terminate.wait()
        conn: asyncpg.Connection
        async with self.pool.acquire() as conn:  # type: ignore
            try:
                if self.success:
                    await ack_message(
                        conn,
                        self.queue_name,
                        self.message.id,
                    )
                else:
                    await nack_message(
                        conn,
                        self.queue_name,
                        self.message.id,
                    )
            finally:
                self.terminated.set()

    # need to implement this since we override __hash__
    # but it should never be called
    def __eq__(self, *_: Any) -> bool:  # pragma: no cover
        return False

    def __hash__(self) -> int:
        return id(self)


@dataclass(**DATACLASSES_KW)
class Queue(AbstractQueue):
    pool: asyncpg.Pool
    queue_name: str
    completion_callbacks: Dict[str, Dict[Hashable, anyio.Event]]
    new_job_callbacks: Dict[str, Set[anyio.Event]]
    in_flight_jobs: Set[JobManager]

    @asynccontextmanager
    async def poll(
        self,
        batch_size: int = 1,
        poll_interval: float = 1,
        fifo: bool = False,
    ) -> AsyncIterator[JobHandleIterator]:
        send, rcv = anyio.create_memory_object_stream(0, JobHandle)  # type: ignore

        async def gather_jobs(tg: TaskGroup) -> None:
            jobs = await poll_for_messages(
                self.pool,
                queue_name=self.queue_name,
                fifo=fifo,
                batch_size=batch_size,
            )
            while True:
                while not jobs:
                    continue_to_next_iter = anyio.Event()
                    # wait for a new job to be published or the poll interval to expire

                    self.new_job_callbacks[self.queue_name].add(continue_to_next_iter)

                    async def skip_forward_if_timeout() -> None:
                        await anyio.sleep(poll_interval)
                        continue_to_next_iter.set()

                    try:
                        async with anyio.create_task_group() as gather_tg:
                            gather_tg.start_soon(skip_forward_if_timeout)
                            await continue_to_next_iter.wait()
                            gather_tg.cancel_scope.cancel()
                    finally:
                        self.new_job_callbacks[self.queue_name].discard(
                            continue_to_next_iter
                        )

                    jobs = await poll_for_messages(
                        self.pool,
                        queue_name=self.queue_name,
                        fifo=fifo,
                        batch_size=batch_size,
                    )

                managers: List[JobManager] = []

                # start extending ack deadlines before handing control over to workers
                # so that processing each job takes > ack deadline messages aren't lost
                for job in jobs:
                    message = Message(id=job["id"], body=job["body"])
                    manager = JobManager(
                        pool=self.pool,
                        message=message,
                        queue_name=self.queue_name,
                        terminate=anyio.Event(),
                        terminated=anyio.Event(),
                        pending_jobs=self.in_flight_jobs,
                    )
                    managers.append(manager)
                    self.in_flight_jobs.add(manager)
                    # this keeps Queue.poll() open until all in-flight jobs
                    # have completed since manage_ack_deadline will return as soon
                    # as one of two things happens:
                    # - JobManager.wrap_worker() sets JobManager.terminated (it may never even run)
                    # - Queue.poll() exits and sets JobManager.terminated for all in-flight jobs
                    #   that haven't started running
                    tg.start_soon(manager.shutdown)

                for manager in managers:
                    await send.send(manager.wrap_worker())

                jobs = []

        async with anyio.create_task_group() as job_tg:
            try:
                async with anyio.create_task_group() as poll_tg:
                    poll_tg.start_soon(gather_jobs, job_tg)
                    try:
                        yield rcv
                    finally:
                        # stop polling
                        poll_tg.cancel_scope.cancel()
            finally:
                async with anyio.create_task_group() as termination_tg:
                    for manager in self.in_flight_jobs.copy():
                        manager.terminate.set()
                        termination_tg.start_soon(manager.terminated.wait)

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
            async with self.pool.acquire() as conn:  # type: ignore
                for job_id, done_event in done_events.items():
                    self.completion_callbacks[self.queue_name][job_id] = done_event
                try:
                    await publish_messages(
                        conn,
                        queue_name=self.queue_name,
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
                        self.completion_callbacks[self.queue_name].pop(job_id)

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
    in_flight_jobs: Set[JobManager] = set()

    cleanup_conn_lock = anyio.Lock()

    async def run_cleanup(conn: asyncpg.Connection) -> None:
        while True:
            async with cleanup_conn_lock:
                await conn.execute(  # type: ignore
                    "SELECT pgjobq.cleanup_dead_messages()",
                )
            await anyio.sleep(1)

    async def extend_acks(conn: asyncpg.Connection) -> None:
        while True:
            job_ids = [j.message.id for j in in_flight_jobs]
            await extend_ack_deadlines(
                conn,
                queue_name,
                job_ids,
            )
            await anyio.sleep(0.5)  # less than the min ack deadline

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

    async with AsyncExitStack() as stack:
        cleanup_conn: asyncpg.Connection = await stack.enter_async_context(pool.acquire())  # type: ignore
        ack_conn: asyncpg.Connection = await stack.enter_async_context(pool.acquire())  # type: ignore
        await cleanup_conn.add_listener(  # type: ignore
            channel="pgjobq.job_completed",
            callback=process_completion_notification,
        )
        stack.push_async_callback(
            cleanup_conn.remove_listener,  # type: ignore
            channel="pgjobq.job_completed",
            callback=process_completion_notification,
        )
        await cleanup_conn.add_listener(  # type: ignore
            channel="pgjobq.new_job",
            callback=process_new_job_notification,
        )
        stack.push_async_callback(
            cleanup_conn.remove_listener,  # type: ignore
            channel="pgjobq.new_job",
            callback=process_new_job_notification,
        )
        async with anyio.create_task_group() as tg:
            tg.start_soon(run_cleanup, cleanup_conn)
            tg.start_soon(extend_acks, ack_conn)

            queue = Queue(
                pool=pool,
                queue_name=queue_name,
                completion_callbacks=completion_callbacks,
                new_job_callbacks=new_job_callbacks,
                in_flight_jobs=in_flight_jobs,
            )
            try:
                yield queue
            finally:
                tg.cancel_scope.cancel()
