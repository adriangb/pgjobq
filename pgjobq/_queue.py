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
    Mapping,
    Optional,
    Set,
)
from uuid import UUID, uuid4

import anyio
import asyncpg  # type: ignore

from pgjobq.api import JobHandle, JobStream, Message
from pgjobq.api import Queue as AbstractQueue
from pgjobq.api import SendCompletionHandle as AbstractCompletionHandle
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


@dataclass(**DATACLASSES_KW, frozen=True)
class JobCompletionHandle:
    jobs: Mapping[UUID, anyio.Event]

    async def __call__(self) -> None:
        async with anyio.create_task_group() as tg:
            for event in self.jobs.values():
                tg.start_soon(event.wait)


@dataclass(**DATACLASSES_KW)
class JobManager:
    pool: asyncpg.Pool
    message: Message
    queue_name: str
    pending_jobs: Set[JobManager]
    terminated: bool = False
    success: bool = False

    @asynccontextmanager
    async def wrap_worker(self) -> AsyncIterator[Message]:
        if self.terminated:
            raise RuntimeError(
                "Attempted to acquire a handle to a job that was"
                " completed, failed or is no longer available"
                " because Queue.poll() was exited"
            )
        try:
            yield self.message
            self.success = True
        except Exception:
            # handle exceptions after entering the message's context manager
            raise
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        self.pending_jobs.discard(self)
        if self.terminated:
            return
        conn: asyncpg.Connection
        try:
            async with self.pool.acquire() as conn:  # type: ignore
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
            self.terminated = True

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
    in_flight_jobs: Dict[UUID, Set[JobManager]]

    @asynccontextmanager
    async def receive(
        self,
        batch_size: int = 1,
        poll_interval: float = 1,
        fifo: bool = False,
    ) -> AsyncIterator[JobStream]:
        send, rcv = anyio.create_memory_object_stream(0, JobHandle)  # type: ignore

        in_flight_jobs: Set[JobManager] = set()
        poll_id = uuid4()
        self.in_flight_jobs[poll_id] = in_flight_jobs

        async def gather_jobs() -> None:
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
                        pending_jobs=in_flight_jobs,
                    )
                    managers.append(manager)
                    in_flight_jobs.add(manager)

                for manager in managers:
                    await send.send(manager.wrap_worker())

                jobs = ()

        try:
            async with anyio.create_task_group() as poll_tg:
                poll_tg.start_soon(gather_jobs)
                try:
                    yield rcv
                finally:
                    # stop polling
                    poll_tg.cancel_scope.cancel()
        finally:
            self.in_flight_jobs.pop(poll_id)
            async with anyio.create_task_group() as termination_tg:
                for manager in in_flight_jobs.copy():
                    termination_tg.start_soon(manager.shutdown)

    def send(
        self, body: bytes, *bodies: bytes, delay: Optional[timedelta] = None
    ) -> AsyncContextManager[AbstractCompletionHandle]:
        # create the job id application side
        # so that we can start listening before we send
        all_bodies = [body, *bodies]
        job_ids = [uuid4() for _ in range(len(all_bodies))]
        done_events = {id: anyio.Event() for id in job_ids}

        @asynccontextmanager
        async def cm() -> AsyncIterator[AbstractCompletionHandle]:
            conn: asyncpg.Connection
            async with self.pool.acquire() as conn:  # type: ignore
                for job_id, done_event in done_events.items():
                    self.completion_callbacks[self.queue_name][job_id] = done_event
                await publish_messages(
                    conn,
                    queue_name=self.queue_name,
                    message_ids=job_ids,
                    message_bodies=all_bodies,
                    delay=delay,
                )
                handle = JobCompletionHandle(jobs=done_events)
                try:
                    yield handle
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
    in_flight_jobs: Dict[UUID, Set[JobManager]] = {}

    async def run_cleanup(conn: asyncpg.Connection) -> None:
        while True:
            await conn.execute(  # type: ignore
                "SELECT pgjobq.cleanup_dead_messages()",
            )
            await anyio.sleep(1)

    async def extend_acks(conn: asyncpg.Connection) -> None:
        while True:
            job_ids = [
                job.message.id for jobs in in_flight_jobs.values() for job in jobs
            ]
            if job_ids:
                print(job_ids)
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
