from __future__ import annotations

import enum
import sys
from collections import defaultdict
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
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

from pgjobq.api import JobHandle
from pgjobq.api import JobHandleStream as AbstractJobHandleStream
from pgjobq.api import Message
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


class JobState(enum.Enum):
    created = enum.auto()
    processing = enum.auto()
    succeeded = enum.auto()
    failed = enum.auto()
    out_of_scope = enum.auto()


@dataclass(**DATACLASSES_KW, eq=False)
class JobManager:
    pool: asyncpg.Pool
    message: Message
    queue_name: str
    pending_jobs: Set[JobManager]
    state: JobState = JobState.created

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[Message]:
        if self.state is JobState.out_of_scope:
            raise RuntimeError(
                "Attempted to acquire a handle to a job is no longer available,"
                " possibly because Queue.receive() went out of scope"
            )
        elif self.state in (JobState.failed, JobState.succeeded):
            raise RuntimeError(
                "Attempted to acquire a handle to a job that already completed"
            )
        elif self.state is JobState.processing:
            raise RuntimeError(
                "Attempted to acquire a handle that is already being processed"
            )
        self.state = JobState.processing
        state = JobState.succeeded
        try:
            yield self.message
        except Exception:
            # handle exceptions after entering the message's context manager
            state = JobState.failed
            raise
        finally:
            await self.shutdown(state)

    async def shutdown(self, final_state: JobState) -> None:
        self.pending_jobs.discard(self)
        if self.state not in (JobState.created, JobState.processing):
            return
        conn: asyncpg.Connection
        try:
            async with self.pool.acquire() as conn:  # type: ignore
                if final_state is JobState.succeeded:
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
            self.state = final_state


@dataclass(**DATACLASSES_KW, eq=False)
class JobHandleStream:
    get_nxt: Callable[[], Awaitable[JobHandle]]

    def __aiter__(self) -> JobHandleStream:
        return self

    def __anext__(self) -> Awaitable[JobHandle]:
        return self.get_nxt()

    receive = __anext__


@dataclass(**DATACLASSES_KW)
class Queue(AbstractQueue):
    pool: asyncpg.Pool
    queue_name: str
    completion_callbacks: Dict[str, Dict[Hashable, anyio.Event]]
    new_job_callbacks: Dict[str, Set[Callable[[], None]]]
    in_flight_jobs: Dict[UUID, Set[JobManager]]

    @asynccontextmanager
    async def receive(
        self,
        batch_size: int = 1,
        poll_interval: float = 1,
    ) -> AsyncIterator[AbstractJobHandleStream]:

        in_flight_jobs: Set[JobManager] = set()
        poll_id = uuid4()
        self.in_flight_jobs[poll_id] = in_flight_jobs

        unyielded_jobs: List[JobManager] = []

        async def get_jobs() -> None:
            conn: asyncpg.Connection
            async with self.pool.acquire() as conn:  # type: ignore
                # use a transaction so that if we get cancelled or crash
                # the messages are still available
                async with conn.transaction():  # type: ignore
                    jobs = await poll_for_messages(
                        conn,
                        queue_name=self.queue_name,
                        batch_size=batch_size,
                    )
                    managers = [
                        JobManager(
                            pool=self.pool,
                            message=Message(id=job["id"], body=job["body"]),
                            queue_name=self.queue_name,
                            pending_jobs=in_flight_jobs,
                        )
                        for job in jobs
                    ]
                    in_flight_jobs.update(managers)
                    unyielded_jobs.extend(managers)

        await get_jobs()

        async def get_next_job() -> JobHandle:
            while not unyielded_jobs:
                await get_jobs()
                if unyielded_jobs:
                    break

                # wait for a new job to be published or the poll interval to expire
                new_job = anyio.Event()
                self.new_job_callbacks[self.queue_name].add(new_job.set)  # type: ignore

                async def skip_forward_if_timeout() -> None:
                    await anyio.sleep(poll_interval)
                    new_job.set()

                try:
                    async with anyio.create_task_group() as gather_tg:
                        gather_tg.start_soon(skip_forward_if_timeout)
                        await new_job.wait()
                        gather_tg.cancel_scope.cancel()
                finally:
                    self.new_job_callbacks[self.queue_name].discard(
                        new_job.set  # type: ignore
                    )

            return unyielded_jobs.pop()

        try:
            yield JobHandleStream(get_next_job)
        finally:
            self.in_flight_jobs.pop(poll_id)
            for manager in in_flight_jobs.copy():
                await manager.shutdown(JobState.out_of_scope)

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
    new_job_callbacks: Dict[str, Set[Callable[[], None]]] = defaultdict(set)
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
            event()

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
