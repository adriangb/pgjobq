from datetime import timedelta
from time import time
from typing import AsyncContextManager, AsyncGenerator, List

import anyio
import asyncpg  # type: ignore
import pytest
from anyio.abc import TaskStatus

from pgjobq import Queue, connect_to_queue, create_queue


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

    async with queue.receive() as job_handle_iter:
        async with await job_handle_iter.receive() as job:
            assert job.body == b'{"foo":"bar"}', job.body


@pytest.mark.anyio
async def test_worker_takes_longer_than_ack_interval(
    queue: Queue,
) -> None:
    async with queue.send(b'{"foo":"bar"}'):
        pass

    async with queue.receive() as job_handle_iter:
        async with await job_handle_iter.receive() as job:
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
        async with queue.receive() as job_handle_iter:
            async for job_handle in job_handle_iter:
                async with job_handle as _:
                    raise MyException

    async with queue.receive() as job_handle_iter:
        # with anyio.fail_after(0.75):  # redelivery should be immediate
        start = time()
        job_handle = await job_handle_iter.receive()
        end = time()
        elapsed = end - start
        assert elapsed < 0.5, elapsed
        async with job_handle as job:
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
        async with queue.receive() as job_handle_iter:
            async for _ in job_handle_iter:
                raise MyException

    async with queue.receive() as job_handle_iter:
        with anyio.fail_after(0.75):  # redelivery should be immediate
            job_handle = await job_handle_iter.receive()
        async with job_handle as job:
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
        async with queue.receive() as job_handle_iter:
            await job_handle_iter.receive()
            raise MyException

    async with queue.receive() as job_handle_iter:
        with anyio.fail_after(0.75):  # redelivery should be immediate
            job_handle = await job_handle_iter.receive()
        async with job_handle as job:
            assert job.body == b'{"foo":"bar"}', job.body


@pytest.mark.anyio
async def test_start_job_after_poll_exited(
    queue: Queue,
) -> None:
    class MyException(Exception):
        pass

    async with queue.send(b'{"foo":"bar"}'):
        pass

    with pytest.raises(MyException):
        async with queue.receive() as job_handle_iter:
            job_handle = await job_handle_iter.receive()
            raise MyException

    with pytest.raises(
        RuntimeError, match="completed, failed or is no longer available"
    ):
        async with job_handle:  # type: ignore
            assert False, "should not be called"  # pragma: no cover


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

    async def fake_job_work(msg_handle: AsyncContextManager[None]) -> None:
        async with msg_handle:
            await anyio.sleep(0.25)

    async with connect_to_queue("test-queue", migrated_pool) as queue:
        for _ in range(total_jobs):
            async with queue.send(b"{}"):
                pass

        n = total_jobs
        async with queue.receive(batch_size=total_jobs) as job_handle_iter:
            async with anyio.create_task_group() as worker_tg:
                async for job_handle in job_handle_iter:
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
            async with queue.receive() as job_handler_iter:
                task_status.started()
                async for job_handler in job_handler_iter:
                    pulls.append(name)
                    with anyio.CancelScope(shield=True):
                        async with job_handler:
                            # let other workers try to grab the job
                            await anyio.sleep(ack_deadline * 1.25)

    async with anyio.create_task_group() as tg:
        await tg.start(worker, "1")
        await tg.start(worker, "2")

        async with connect_to_queue("test-queue", migrated_pool) as queue:
            async with queue.send(b"{}") as completion_handle:
                await completion_handle()
        tg.cancel_scope.cancel()

    # we check that the message was only received and processed once
    assert pulls in (["1"], ["2"])


@pytest.mark.anyio
async def test_enqueue_with_delay(
    queue: Queue,
) -> None:
    async with queue.send(b'{"foo":"bar"}', delay=timedelta(seconds=0.5)):
        pass

    async with queue.receive() as job_handler_iter:
        with anyio.move_on_after(0.25) as scope:  # no jobs should be available
            async for _ in job_handler_iter:
                assert False, "should not be called"
        assert scope.cancel_called is True

    await anyio.sleep(0.5)  # wait for the job to become available

    async with queue.receive() as job_handler_iter:
        with anyio.fail_after(0.05):  # we shouldn't have to wait anymore
            job_handler = await job_handler_iter.receive()
        async with job_handler as job:
            assert job.body == b'{"foo":"bar"}', job.body


@pytest.mark.anyio
async def test_pull_fifo(
    queue: Queue,
) -> None:
    async with queue.send(b"1"):
        pass

    async with queue.send(b"2"):
        pass

    async with queue.receive() as job_handler_iter:
        async with await job_handler_iter.receive() as job:
            assert job.body == b"1"

    async with queue.receive() as job_handler_iter:
        async with await job_handler_iter.receive() as job:
            assert job.body == b"2"


@pytest.mark.anyio
async def test_completion_handle_awaited(
    queue: Queue,
) -> None:
    events: List[str] = []

    async with anyio.create_task_group() as tg:

        async def worker() -> None:
            async with queue.receive() as job_handle_stream:
                async with await job_handle_stream.receive():
                    events.append("received")
                events.append("acked")

        tg.start_soon(worker)

        async with queue.send(b'{"foo":"bar"}') as completion_handle:
            events.append("sent")
            await completion_handle()
            events.append("completed")

    assert events in (
        ["sent", "received", "completed", "acked"],
        ["sent", "received", "acked", "completed"],
    )


@pytest.mark.anyio
async def test_new_message_notification_triggers_poll(
    queue: Queue,
) -> None:
    send_times: List[float] = []
    rcv_times: List[float] = []

    async with anyio.create_task_group() as tg:

        async def worker() -> None:
            async with queue.receive(poll_interval=60) as job_iter:
                await job_iter.receive()
                rcv_times.append(time())
            return

        tg.start_soon(worker)
        # wait for the worker to start polling
        await anyio.sleep(0.05)

        async with queue.send(b'{"foo":"bar"}'):
            send_times.append(time())
            pass

    assert len(send_times) == len(rcv_times)
    # not deterministic
    # but generally we are checking that elapsed time
    # between a send and rcv << poll_interval
    assert rcv_times[0] - send_times[0] < 0.1


@pytest.mark.anyio
@pytest.mark.parametrize("total_messages", [4, 5])
async def test_batched_rcv(queue: Queue, total_messages: int) -> None:
    for _ in range(total_messages):
        async with queue.send("{}".encode()):
            pass

    async with queue.receive(batch_size=2) as job_handle_iter:
        for _ in range(total_messages):
            await job_handle_iter.receive()


@pytest.mark.anyio
async def test_batched_send(queue: Queue) -> None:
    events: List[str] = []

    async def worker() -> None:
        async with queue.receive() as job_handle_iter:
            async with await job_handle_iter.receive():
                pass
                events.append("processed")
            # make sure we're not just faster than the completion handle
            await anyio.sleep(0.05)
            async with await job_handle_iter.receive():
                pass
                events.append("processed")

    async with anyio.create_task_group() as tg:
        tg.start_soon(worker)
        async with queue.send(b"1", b"2") as completion_handle:
            await completion_handle()
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
            async with job_handle:
                n += 1
            break

    assert n == 1  # only one job was processed

    # we can immediately process the other job because it was nacked
    # when we exited the Queue.receive() context
    async with queue.receive() as job_handle_stream:
        with anyio.fail_after(0.75):  # redelivery should be immediate
            job_handle = await job_handle_stream.receive()
        async with job_handle as job:
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
