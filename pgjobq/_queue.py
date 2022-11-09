from __future__ import annotations

import enum
import sys
from collections import defaultdict
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from json import loads as json_loads
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Set,
    Union,
)
from uuid import UUID, uuid4

import anyio
import asyncpg  # type: ignore
from anyio.abc import TaskGroup

from pgjobq._exceptions import JobCancelledError
from pgjobq._filters import BaseClause, JobIdIn
from pgjobq._queries import (
    ack_job,
    cancel_jobs,
    cleanup_dead_jobs,
    extend_ack_deadlines,
    get_completed_jobs,
    get_statistics,
    nack_job,
    poll_for_jobs,
    publish_jobs,
)
from pgjobq.api import CompletionHandle as AbstractCompletionHandle
from pgjobq.api import Job, JobHandle
from pgjobq.api import JobHandleStream as AbstractJobHandleStream
from pgjobq.api import OutgoingJob
from pgjobq.api import Queue as AbstractQueue
from pgjobq.api import QueueStatistics

DATACLASSES_KW: Dict[str, Any] = {}
if sys.version_info >= (3, 10):  # pragma: no cover
    DATACLASSES_KW["slots"] = True


@dataclass(**DATACLASSES_KW, frozen=True)
class JobCompletionHandle:
    jobs: Mapping[UUID, anyio.Event]

    async def wait(self) -> None:
        async with anyio.create_task_group() as tg:
            for event in self.jobs.values():
                tg.start_soon(event.wait)
        return None


class JobState(enum.Enum):
    created = enum.auto()
    processing = enum.auto()
    succeeded = enum.auto()
    failed = enum.auto()
    out_of_scope = enum.auto()


@dataclass(**DATACLASSES_KW, eq=False)
class JobManager:
    pool: asyncpg.Pool
    job: Job
    queue_name: str
    pending_jobs: Set[JobManager]
    queue: Queue
    receipt_handle: int
    state: JobState = JobState.created

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[Job]:
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

        async def listen_for_cancellation(tg: TaskGroup) -> None:
            async with self.queue.wait_for_completion(self.job.id) as handle:
                await handle.wait()
                tg.cancel_scope.cancel()
                raise JobCancelledError(job=self.job.id)

        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(listen_for_cancellation, tg)
                yield self.job
                tg.cancel_scope.cancel()
            await self.shutdown(JobState.succeeded)
        except JobCancelledError as e:
            if e.job == self.job.id:
                return
            raise
        except Exception:
            # handle exceptions after entering the job's context manager
            await self.shutdown(JobState.failed)
            raise

    async def shutdown(self, final_state: JobState) -> None:
        self.pending_jobs.discard(self)
        if self.state not in (JobState.created, JobState.processing):
            return
        conn: asyncpg.Connection
        try:
            async with self.pool.acquire() as conn:  # type: ignore
                if final_state is JobState.succeeded:
                    await ack_job(
                        conn,
                        self.queue_name,
                        self.job.id,
                        self.receipt_handle,
                    )
                elif final_state is JobState.failed:
                    await nack_job(
                        conn,
                        self.queue_name,
                        self.job.id,
                        self.receipt_handle,
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
    completion_callbacks: Dict[UUID, Set[anyio.Event]]
    new_job_callbacks: Set[Callable[[], None]]
    in_flight_jobs: Dict[UUID, Set[JobManager]]
    statistics_updated: anyio.Event
    statistics: Optional[QueueStatistics]

    @asynccontextmanager
    async def receive(
        self,
        *,
        batch_size: int = 1,
        poll_interval: float = 1,
        filter: Optional[BaseClause] = None,
    ) -> AsyncIterator[AbstractJobHandleStream]:

        in_flight_jobs: Set[JobManager] = set()
        poll_id = uuid4()
        self.in_flight_jobs[poll_id] = in_flight_jobs

        unyielded_jobs: List[JobManager] = []

        async def get_jobs() -> None:
            conn: asyncpg.Connection
            async with self.pool.acquire() as conn:  # type: ignore
                # use a transaction so that if we get cancelled or crash
                # the jobs are still available
                async with conn.transaction():  # type: ignore
                    jobs = await poll_for_jobs(
                        conn,
                        queue_name=self.queue_name,
                        batch_size=batch_size,
                        filter=filter,
                    )
                    if not jobs:
                        return
                    managers = [
                        JobManager(
                            pool=self.pool,
                            job=Job(
                                id=job["id"],
                                body=job["body"],
                                attributes=(
                                    json_loads(job["attributes"])
                                    if job["attributes"] is not None
                                    else {}
                                ),
                            ),
                            queue_name=self.queue_name,
                            pending_jobs=in_flight_jobs,
                            queue=self,
                            receipt_handle=job["receipt_handle"],
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
                self.new_job_callbacks.add(new_job.set)  # type: ignore

                async def skip_forward_if_timeout() -> None:
                    await anyio.sleep(poll_interval)
                    new_job.set()

                try:
                    async with anyio.create_task_group() as gather_tg:
                        gather_tg.start_soon(skip_forward_if_timeout)
                        await new_job.wait()
                        gather_tg.cancel_scope.cancel()
                finally:
                    self.new_job_callbacks.discard(new_job.set)  # type: ignore

            return unyielded_jobs.pop()

        try:
            yield JobHandleStream(get_next_job)
        finally:
            self.in_flight_jobs.pop(poll_id)
            for manager in in_flight_jobs.copy():
                await manager.shutdown(JobState.out_of_scope)

    def send(
        self,
        job_or_body: Union[bytes, OutgoingJob],
        *jobs_or_bodies: Union[bytes, OutgoingJob],
        schedule_at: Optional[datetime] = None,
    ) -> AsyncContextManager[AbstractCompletionHandle]:
        jobs: List[OutgoingJob]
        if isinstance(job_or_body, bytes):
            jobs = [
                OutgoingJob(job_or_body),
                *(OutgoingJob(b) for b in jobs_or_bodies),  # type: ignore
            ]
        else:
            jobs = [job_or_body, *jobs_or_bodies]  # type: ignore
        ids = [uuid4() for _ in range(len(jobs))]
        publish = publish_jobs(
            conn=self.pool,
            queue_name=self.queue_name,
            ids=ids,
            jobs=jobs,
            schedule_at=schedule_at,
        )

        @asynccontextmanager
        async def cm() -> AsyncIterator[AbstractCompletionHandle]:
            # create the job id application side
            # so that we can start listening before we send
            async with self.wait_for_completion(*ids, poll_interval=None) as handle:
                await publish
                yield handle

        return cm()

    async def cancel(self, job_or_filter: Union[UUID, BaseClause], *jobs: UUID) -> None:
        filter: BaseClause
        if isinstance(job_or_filter, UUID):
            filter = JobIdIn(ids=[job_or_filter, *jobs])
        else:
            filter = job_or_filter
        await cancel_jobs(self.pool, self.queue_name, filter)

    def wait_for_completion(
        self,
        job: UUID,
        *jobs: UUID,
        poll_interval: Optional[timedelta] = timedelta(seconds=10),
    ) -> AsyncContextManager[AbstractCompletionHandle]:
        @asynccontextmanager
        async def cm() -> AsyncIterator[AbstractCompletionHandle]:
            done_events = {id: anyio.Event() for id in (job, *jobs)}
            for job_id, event in done_events.items():
                self.completion_callbacks[job_id].add(event)

            def cleanup_done_events() -> None:
                for job_id in done_events.copy().keys():
                    if done_events[job_id].is_set():
                        event = done_events.pop(job_id)
                        self.completion_callbacks[job_id].discard(event)
                        if not self.completion_callbacks[job_id]:
                            self.completion_callbacks.pop(job_id)

            async def poll_for_completion(interval: float) -> None:
                nonlocal done_events
                remaining = len(done_events)
                while True:
                    new_completion = anyio.Event()
                    # wait for a completion notification or poll interval to expire

                    async def set_new_completion(
                        event: anyio.Event, tg: TaskGroup
                    ) -> None:
                        await event.wait()
                        new_completion.set()
                        tg.cancel_scope.cancel()

                    async with anyio.create_task_group() as tg:
                        for event in done_events.values():
                            tg.start_soon(set_new_completion, event, tg)
                        await anyio.sleep(interval)
                        tg.cancel_scope.cancel()

                    if not new_completion.is_set():
                        # poll
                        completed_jobs = await get_completed_jobs(
                            self.pool,
                            self.queue_name,
                            job_ids=list(done_events.keys()),
                        )
                        if completed_jobs:
                            new_completion.set()
                        remaining -= len(completed_jobs)
                        for job in completed_jobs:
                            done_events[job].set()
                    if new_completion.is_set():
                        cleanup_done_events()
                    if remaining == 0:
                        return

            try:
                async with anyio.create_task_group() as tg:
                    if poll_interval is not None:
                        tg.start_soon(
                            poll_for_completion, poll_interval.total_seconds()
                        )
                    yield JobCompletionHandle(jobs=done_events.copy())
                    tg.cancel_scope.cancel()
            finally:
                cleanup_done_events()

        return cm()

    async def _get_statistics(self) -> QueueStatistics:
        if self.statistics is None:
            record = await get_statistics(self.pool, self.queue_name)
            self.statistics = QueueStatistics(
                jobs=record["jobs"],
                max_size=record["max_size"],
            )
        return self.statistics

    async def get_statistics(self) -> QueueStatistics:
        return await self._get_statistics()

    async def wait_if_full(self) -> None:
        stats = await self.get_statistics()
        while stats.max_size is not None and stats.jobs >= stats.max_size:
            await self.statistics_updated.wait()
            stats = await self.get_statistics()


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
    completion_callbacks: Dict[UUID, Set[anyio.Event]] = defaultdict(set)
    new_job_callbacks: Set[Callable[[], None]] = set()
    checked_out_jobs: Dict[UUID, Set[JobManager]] = {}

    queue = Queue(
        pool=pool,
        queue_name=queue_name,
        completion_callbacks=completion_callbacks,
        new_job_callbacks=new_job_callbacks,
        in_flight_jobs=checked_out_jobs,
        statistics_updated=anyio.Event(),
        statistics=None,
    )

    async def run_cleanup(conn: asyncpg.Connection) -> None:
        while True:
            await cleanup_dead_jobs(conn, queue_name)
            await anyio.sleep(1)

    async def extend_acks(conn: asyncpg.Connection) -> None:
        while True:
            job_ids = [job.job.id for jobs in checked_out_jobs.values() for job in jobs]
            receipt_handles = [
                job.receipt_handle for jobs in checked_out_jobs.values() for job in jobs
            ]
            await extend_ack_deadlines(
                conn,
                queue_name,
                job_ids,
                receipt_handles,
            )
            await anyio.sleep(0.5)  # less than the min ack deadline

    def process_completion_notification(
        conn: asyncpg.Connection,
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        if not payload:
            return
        job_ids = payload.split(",")
        if queue.statistics is not None:
            queue.statistics.jobs -= len(job_ids)
            queue.statistics_updated.set()
            queue.statistics_updated = anyio.Event()
        for job_id in job_ids:
            job_id_key = UUID(job_id)
            events = completion_callbacks.get(job_id_key, None) or ()
            for event in events:
                event.set()

    def process_new_job_notification(
        conn: asyncpg.Connection,
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        n = int(payload)
        if queue.statistics is not None:
            queue.statistics.jobs += n
            queue.statistics_updated = anyio.Event()
        for cb in new_job_callbacks:
            cb()

    async with AsyncExitStack() as stack:
        cleanup_conn: asyncpg.Connection = await stack.enter_async_context(pool.acquire())  # type: ignore
        ack_conn: asyncpg.Connection = await stack.enter_async_context(pool.acquire())  # type: ignore
        completion_channel = f"pgjobq.job_completed_{queue_name}"
        new_job_channel = f"pgjobq.new_job_{queue_name}"
        await cleanup_conn.add_listener(  # type: ignore
            channel=completion_channel,
            callback=process_completion_notification,
        )
        stack.push_async_callback(
            cleanup_conn.remove_listener,  # type: ignore
            channel=completion_channel,
            callback=process_completion_notification,
        )
        await cleanup_conn.add_listener(  # type: ignore
            channel=new_job_channel,
            callback=process_new_job_notification,
        )
        stack.push_async_callback(
            cleanup_conn.remove_listener,  # type: ignore
            channel=new_job_channel,
            callback=process_new_job_notification,
        )
        async with anyio.create_task_group() as tg:
            tg.start_soon(run_cleanup, cleanup_conn)
            tg.start_soon(extend_acks, ack_conn)

            try:
                yield queue
            finally:
                tg.cancel_scope.cancel()
