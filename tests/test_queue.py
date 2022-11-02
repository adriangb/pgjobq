from datetime import datetime, timedelta
from time import time
from typing import AsyncGenerator, List, Set
from uuid import UUID

import anyio
import asyncpg  # type: ignore
import pytest
from anyio.abc import TaskStatus

from pgjobq import (
    Attribute,
    OutgoingJob,
    Queue,
    connect_to_queue,
    create_queue,
    get_dlq_name,
)
from pgjobq.api import JobHandle, QueueStatistics


@pytest.fixture
async def queue(migrated_pool: asyncpg.Pool) -> AsyncGenerator[Queue, None]:
    await create_queue("test-queue", migrated_pool)
    async with connect_to_queue("test-queue", migrated_pool) as queue:
        yield queue


@pytest.mark.anyio
async def test_completion_handle_ignored(
    queue: Queue,
) -> None:
    async with queue.send(b'{"foo":"bar"}'):
        pass

    async with queue.receive() as job_handle_stream:
        async with (await job_handle_stream.receive()).acquire() as job:
            assert job.body == b'{"foo":"bar"}', job.body


@pytest.mark.anyio
async def test_worker_takes_longer_than_ack_interval(
    queue: Queue,
) -> None:
    async with queue.send(b'{"foo":"bar"}'):
        pass

    async with queue.receive() as job_handle_stream:
        async with (await job_handle_stream.receive()).acquire() as job:
            assert job.body == b'{"foo":"bar"}', job.body
            await anyio.sleep(1)  # default ack interval


@pytest.mark.anyio
async def test_worker_raises_exception_in_job_handle(
    queue: Queue,
) -> None:
    class MyException(Exception):
        pass

    async with queue.send(b'{"foo":"bar"}'):
        pass

    with pytest.raises(MyException):
        async with queue.receive() as job_handle_stream:
            async for job_handle in job_handle_stream:
                async with job_handle.acquire() as _:
                    raise MyException

    async with queue.receive() as job_handle_stream:
        with anyio.fail_after(0.75):  # redelivery should be immediate
            job_handle = await job_handle_stream.receive()
        async with job_handle.acquire() as job:
            assert job.body == b'{"foo":"bar"}', job.body


@pytest.mark.anyio
async def test_worker_raises_exception_before_job_handle_is_entered(
    queue: Queue,
) -> None:
    class MyException(Exception):
        pass

    async with queue.send(b'{"foo":"bar"}'):
        pass

    with pytest.raises(MyException):
        async with queue.receive() as job_handle_stream:
            async for _ in job_handle_stream:
                raise MyException

    async with queue.receive() as job_handle_stream:
        with anyio.fail_after(0.75):  # redelivery should be immediate
            job_handle = await job_handle_stream.receive()
        async with job_handle.acquire() as job:
            assert job.body == b'{"foo":"bar"}', job.body


@pytest.mark.anyio
async def test_worker_raises_exception_in_poll_with_pending_jobs(
    queue: Queue,
) -> None:
    class MyException(Exception):
        pass

    async with queue.send(b'{"foo":"bar"}'):
        pass

    with pytest.raises(MyException):
        async with queue.receive() as job_handle_stream:
            await job_handle_stream.receive()
            raise MyException

    async with queue.receive() as job_handle_stream:
        with anyio.fail_after(0.75):  # redelivery should be immediate
            job_handle = await job_handle_stream.receive()
        async with job_handle.acquire() as job:
            assert job.body == b'{"foo":"bar"}', job.body


@pytest.mark.anyio
async def test_start_job_after_poll_exited(
    queue: Queue,
) -> None:
    async with queue.send(b'{"foo":"bar"}'):
        pass

    async with queue.receive() as job_handle_stream:
        job_handle = await job_handle_stream.receive()

    with pytest.raises(RuntimeError, match="job is no longer available"):
        async with job_handle.acquire():  # type: ignore
            assert False, "should not be called"  # pragma: no cover


@pytest.mark.anyio
async def test_start_job_twice(
    queue: Queue,
) -> None:

    async with queue.send(b'{"foo":"bar"}'):
        pass

    async with queue.receive() as job_handle_stream:
        job_handle = await job_handle_stream.receive()
        async with job_handle.acquire():
            pass
            with pytest.raises(RuntimeError, match="already being processed"):
                async with job_handle.acquire():
                    pass


@pytest.mark.anyio
async def test_start_completed_job_handle(
    queue: Queue,
) -> None:

    async with queue.send(b'{"foo":"bar"}'):
        pass

    async with queue.receive() as job_handle_stream:
        job_handle = await job_handle_stream.receive()
        async with job_handle.acquire():
            pass
        with pytest.raises(RuntimeError, match="already completed"):
            async with job_handle.acquire():
                pass


@pytest.mark.anyio
async def test_execute_jobs_concurrently(
    migrated_pool: asyncpg.Pool,
) -> None:
    """We should be able to run jobs concurrently without deadlocking or other errors"""
    ack_deadline = 1
    total_jobs = 15  # larger than asyncpg's pool size
    await create_queue(
        "test-queue", migrated_pool, ack_deadline=timedelta(seconds=ack_deadline)
    )

    async def fake_job_work(job_handle: JobHandle) -> None:
        async with job_handle.acquire():
            await anyio.sleep(0.25)

    async with connect_to_queue("test-queue", migrated_pool) as queue:
        for _ in range(total_jobs):
            async with queue.send(b"{}"):
                pass

        n = total_jobs
        async with queue.receive(batch_size=total_jobs) as job_handle_stream:
            async with anyio.create_task_group() as worker_tg:
                async for job_handle in job_handle_stream:
                    worker_tg.start_soon(fake_job_work, job_handle)
                    n -= 1
                    if n == 0:
                        break


@pytest.mark.anyio
async def test_concurrent_worker_pull_atomic_delivery(
    migrated_pool: asyncpg.Pool,
) -> None:
    """Even with multiple concurrent workers each job should only be pulled once"""
    ack_deadline = 1
    await create_queue(
        "test-queue", migrated_pool, ack_deadline=timedelta(seconds=ack_deadline)
    )
    pulls: List[str] = []

    async def worker(name: str, *, task_status: TaskStatus) -> None:
        async with connect_to_queue("test-queue", migrated_pool) as queue:
            async with queue.receive() as job_handle_stream:
                task_status.started()
                async for job_handle in job_handle_stream:
                    pulls.append(name)
                    with anyio.CancelScope(shield=True):
                        async with job_handle.acquire():
                            # let other workers try to grab the job
                            await anyio.sleep(ack_deadline * 1.25)

    async with anyio.create_task_group() as tg:
        await tg.start(worker, "1")
        await tg.start(worker, "2")

        async with connect_to_queue("test-queue", migrated_pool) as queue:
            async with queue.send(b"{}") as completion_handle:
                await completion_handle.wait()
        tg.cancel_scope.cancel()

    # we check that the job was only received and processed once
    assert pulls in (["1"], ["2"])


@pytest.mark.anyio
async def test_enqueue_with_delay(
    queue: Queue,
) -> None:
    # schedule with zero delay
    scheduled_at = datetime.utcnow() + timedelta(seconds=0)
    async with queue.send(b"1", schedule_at=scheduled_at):
        pass

    async with queue.receive() as job_handle_stream:
        # job should be available
        received = False
        with anyio.fail_after(0.125):
            async with (await job_handle_stream.receive()).acquire():
                received = True
        assert received

    # schedule with a delay
    scheduled_at = datetime.utcnow() + timedelta(seconds=1)
    async with queue.send(b"2", schedule_at=scheduled_at):
        pass

    async with queue.receive() as job_handle_stream:
        # no jobs should be available
        with anyio.move_on_after(0.25) as scope:
            await job_handle_stream.receive()
        assert scope.cancel_called is True
        await anyio.sleep(0.75)
        with anyio.fail_after(0.125):
            await job_handle_stream.receive()


@pytest.mark.anyio
async def test_completion_handle_awaited(
    queue: Queue,
) -> None:
    events: List[str] = []

    async with anyio.create_task_group() as tg:

        async def worker() -> None:
            async with queue.receive() as job_handle_stream:
                async with (await job_handle_stream.receive()).acquire():
                    events.append("received")
                events.append("acked")

        tg.start_soon(worker)

        async with queue.send(b'{"foo":"bar"}') as completion_handle:
            events.append("sent")
            await completion_handle.wait()
            events.append("completed")

    assert events in (
        ["sent", "received", "completed", "acked"],
        ["sent", "received", "acked", "completed"],
    )


@pytest.mark.anyio
async def test_new_job_notification_triggers_poll(
    queue: Queue,
) -> None:
    send_times: List[float] = []
    rcv_times: List[float] = []

    async with anyio.create_task_group() as tg:

        async def producer() -> None:
            async with queue.send(b'{"foo":"bar"}') as handle:
                send_times.append(time())
                await handle.wait()

        async with queue.receive(poll_interval=60) as job_handle_stream:
            tg.start_soon(producer)
            await anyio.sleep(1)
            async with (await job_handle_stream.receive()).acquire():
                rcv_times.append(time())

    assert len(send_times) == len(rcv_times)
    # not deterministic
    # but generally we are checking that elapsed time
    # between a send and rcv << poll_interval
    assert rcv_times[0] - send_times[0] < 1


@pytest.mark.anyio
@pytest.mark.parametrize("total_jobs", [4, 5])
async def test_batched_rcv(queue: Queue, total_jobs: int) -> None:
    for _ in range(total_jobs):
        async with queue.send("{}".encode()):
            pass

    async with queue.receive(batch_size=2) as job_handle_stream:
        for _ in range(total_jobs):
            await job_handle_stream.receive()


@pytest.mark.anyio
async def test_batched_send(queue: Queue) -> None:
    events: List[str] = []

    async def worker() -> None:
        async with queue.receive() as job_handle_stream:
            async with (await job_handle_stream.receive()).acquire():
                pass
                events.append("processed")
            # make sure we're not just faster than the completion handle
            await anyio.sleep(0.05)
            async with (await job_handle_stream.receive()).acquire():
                pass
                events.append("processed")

    async with anyio.create_task_group() as tg:
        tg.start_soon(worker)
        async with queue.send(b"1", b"2") as completion_handle:
            await completion_handle.wait()
            events.append("completed")

    assert events == ["processed", "processed", "completed"]


@pytest.mark.anyio
async def test_batched_rcv_can_be_interrupted(
    queue: Queue,
) -> None:
    n = 0

    for _ in range(2):
        async with queue.send(b"{}"):
            pass

    async with queue.receive(batch_size=2) as job_handle_stream:
        async for job_handle in job_handle_stream:
            async with job_handle.acquire():
                n += 1
            break

    assert n == 1  # only one job was processed

    # we can immediately process the other job because it was nacked
    # when we exited the Queue.receive() context
    async with queue.receive() as job_handle_stream:
        with anyio.fail_after(0.75):  # redelivery should be immediate
            job_handle = await job_handle_stream.receive()
        async with job_handle.acquire() as job:
            assert job.body == b"{}", job.body


@pytest.mark.anyio
async def test_send_to_non_existent_queue_raises_exception(
    migrated_pool: asyncpg.Pool,
) -> None:
    async with connect_to_queue("test-queue", migrated_pool) as queue:
        with pytest.raises(LookupError, match="Queue not found"):
            async with queue.send(b'{"foo":"bar"}'):
                pass


@pytest.mark.anyio
async def test_receive_from_non_existent_queue_allowed(
    migrated_pool: asyncpg.Pool,
) -> None:
    # allow waiting on a non-existent queue so that workers
    # can be spun up and start listening before the queue is created
    async with connect_to_queue("test-queue", migrated_pool) as queue:
        async with queue.receive() as job_handle_stream:
            with anyio.move_on_after(0.25) as scope:  # no jobs should be available
                async for _ in job_handle_stream:
                    assert False, "should not be called"  # pragma: no cover
            assert scope.cancel_called is True


@pytest.mark.anyio
async def test_queue_statistics(
    queue: Queue,
) -> None:

    stats = await queue.get_statistics()
    expected = QueueStatistics(jobs=0)
    assert stats == expected

    async with queue.send(b"{}"):
        pass
    async with queue.send(b"{}"):
        pass

    stats = await queue.get_statistics()
    expected = QueueStatistics(jobs=2)
    assert stats == expected

    async with queue.receive() as job_handle_stream:
        async with (await job_handle_stream.receive()).acquire():
            pass

    stats = await queue.get_statistics()
    expected = QueueStatistics(jobs=1)
    assert stats == expected

    async with queue.receive() as job_handle_stream:
        async with (await job_handle_stream.receive()).acquire():
            pass

    stats = await queue.get_statistics()
    expected = QueueStatistics(jobs=0)
    assert stats == expected


@pytest.mark.anyio
async def test_get_completed_jobs_in_flight(
    migrated_pool: asyncpg.Pool,
) -> None:

    await create_queue("test-queue", migrated_pool)

    ids: Set[UUID] = set()

    async with connect_to_queue("test-queue", migrated_pool) as queue:
        async with queue.send(b"{}") as handle:
            ids.update(handle.jobs.keys())
        async with queue.send(b"{}") as handle:
            ids.update(handle.jobs.keys())

        # complete one of the two jobs
        # we won't get a notification for this one
        async with queue.receive() as job_handle_stream:
            async with (await job_handle_stream.receive()).acquire():
                pass

    async with connect_to_queue("test-queue", migrated_pool) as queue:
        async with queue.wait_for_completion(
            *ids, poll_interval=timedelta(seconds=1)
        ) as handle:
            with anyio.fail_after(5):  # fail fast during tests
                async with anyio.create_task_group() as tg:
                    tg.start_soon(handle.wait)
                    # process the other job
                    async with queue.receive() as job_handle_stream:
                        async with (await job_handle_stream.receive()).acquire():
                            pass


@pytest.mark.anyio
async def test_wait_for_completion_instant_poll(
    migrated_pool: asyncpg.Pool,
) -> None:

    await create_queue("test-queue", migrated_pool)

    ids: Set[UUID] = set()

    async with connect_to_queue("test-queue", migrated_pool) as queue:
        async with queue.send(b"{}") as handle:
            ids.update(handle.jobs.keys())
        async with queue.send(b"{}") as handle:
            ids.update(handle.jobs.keys())
        # complete both jobs
        async with queue.receive() as job_handle_stream:
            async with (await job_handle_stream.receive()).acquire():
                pass
        async with queue.receive() as job_handle_stream:
            async with (await job_handle_stream.receive()).acquire():
                pass

    async with connect_to_queue("test-queue", migrated_pool) as queue:
        async with queue.wait_for_completion(
            *ids, poll_interval=timedelta(seconds=0)
        ) as handle:
            # fail fast during tests, this should be near instant
            with anyio.fail_after(1):
                await handle.wait()


@pytest.mark.anyio
async def test_cancellation(
    queue: Queue,
) -> None:

    acquired = anyio.Event()

    async def worker() -> None:
        async with queue.receive() as job_handle_stream:
            async for job_handle in job_handle_stream:
                async with job_handle.acquire() as _:
                    acquired.set()
                    await anyio.sleep(float("inf"))
                return

    async with anyio.create_task_group() as tg:
        tg.start_soon(worker)
        async with queue.send(b'{"foo":"bar"}') as handle:
            await acquired.wait()
            await queue.cancel(*handle.jobs.keys())
            await handle.wait()


@pytest.mark.anyio
async def test_dlq_max_delivery_attempts_expired(
    migrated_pool: asyncpg.Pool,
) -> None:

    await create_queue(
        "test-queue",
        migrated_pool,
        max_delivery_attempts=1,
    )

    class MyExc(Exception):
        pass

    async with connect_to_queue("test-queue", migrated_pool) as queue:
        async with queue.send(b'{"foo":"bar"}') as handle:
            with pytest.raises(MyExc):
                async with queue.receive() as job_handle_stream:
                    async for job_handle in job_handle_stream:
                        async with job_handle.acquire() as _:
                            raise MyExc
            await handle.wait()

    received = False

    async with connect_to_queue(get_dlq_name("test-queue"), migrated_pool) as dlq:
        async with dlq.receive() as job_handle_stream:
            async for job_handle in job_handle_stream:
                async with job_handle.acquire() as _:
                    received = True
                    break

    assert received


@pytest.mark.anyio
async def test_dlq_expiration(
    migrated_pool: asyncpg.Pool,
) -> None:

    await create_queue(
        "test-queue",
        migrated_pool,
        retention_period=timedelta(seconds=1),
    )

    async with connect_to_queue("test-queue", migrated_pool) as queue:
        async with queue.send(b'{"foo":"bar"}') as handle:
            # 1 sec for expiration and 1 sec for cleanup to run
            await anyio.sleep(2)
            await handle.wait()

    received = False

    async with connect_to_queue(get_dlq_name("test-queue"), migrated_pool) as dlq:
        async with dlq.receive() as job_handle_stream:
            async for job_handle in job_handle_stream:
                async with job_handle.acquire() as _:
                    received = True
                    break

    assert received


@pytest.mark.anyio
async def test_send_with_attributes(
    queue: Queue,
) -> None:
    async with queue.send(OutgoingJob(b"", {"foo": "bar"})) as handle:
        async with queue.receive() as job_handle_stream:
            async for job_handle in job_handle_stream:
                async with job_handle.acquire() as job:
                    assert job.attributes == {"foo": "bar"}
                    break
        await handle.wait()


@pytest.mark.anyio
async def test_receive_filter_attributes(
    queue: Queue,
) -> None:
    filter = Attribute("foo").is_like("GOOD%")
    async with queue.send(OutgoingJob(b"", {"foo": "VERY BAD", "baz": None})):
        pass
    async with queue.send(OutgoingJob(b"", {"baz": 1, "foo": "..."})):
        pass

    received = 0

    async with queue.send(OutgoingJob(b"", {"foo": "VERY BAD"})):
        pass
    async with queue.receive(filter=filter) as job_handle_stream:
        with anyio.move_on_after(1):
            await job_handle_stream.receive()
            received += 1

    assert received == 0

    async with queue.send(OutgoingJob(b"", {"foo": "GOOD!"})):
        pass
    async with queue.receive(filter=filter) as job_handle_stream:
        async for job_handle in job_handle_stream:
            async with job_handle.acquire() as job:
                assert job.attributes == {"foo": "GOOD!"}
                received += 1
                break

    assert received == 1

    filter = filter | (
        Attribute("baz").does_not_exist() & Attribute("foo").is_not_null()
    )

    async with queue.receive(filter=filter) as job_handle_stream:
        async for job_handle in job_handle_stream:
            async with job_handle.acquire() as job:
                assert job.attributes == {"foo": "VERY BAD"}
                received += 1
                break

    assert received == 2


@pytest.mark.anyio
async def test_dependencies(
    queue: Queue,
) -> None:
    async with queue.send(b"1") as handle1:
        async with queue.send(
            OutgoingJob(b"2", dependencies=list(handle1.jobs.keys()))
        ) as handle2:
            async with queue.receive() as job_handle_stream:
                job_handle = await job_handle_stream.receive()
                async with job_handle.acquire() as job:
                    assert job.body == b"2"
                await handle2.wait()
                job_handle = await job_handle_stream.receive()
                async with job_handle.acquire() as job:
                    assert job.body == b"1"
                await handle1.wait()
