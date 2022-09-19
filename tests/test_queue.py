from datetime import timedelta
from time import time
from typing import AsyncContextManager, AsyncGenerator, List

import anyio
import asyncpg  # type: ignore
import pytest

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

    async for msg_handle in queue.poll():
        async with msg_handle as msg:
            assert msg.body == b'{"foo":"bar"}', msg.body


@pytest.mark.anyio
async def test_worker_raises_exception(
    queue: Queue,
) -> None:
    class MyException(Exception):
        pass

    async with queue.send(b'{"foo":"bar"}'):
        pass

    with pytest.raises(MyException):
        async for msg_handle in queue.poll(batch_size=1):
            async with msg_handle as msg:
                raise MyException

    with anyio.fail_after(1):  # redelivery should be immediate
        async for msg_handle in queue.poll(batch_size=1):
            async with msg_handle as msg:
                assert msg.body == b'{"foo":"bar"}', msg.body


@pytest.mark.anyio
async def test_execute_jobs_concurrently(
    migrated_pool: asyncpg.Pool,
) -> None:
    ack_deadline = 1
    await create_queue(
        "test-queue", migrated_pool, ack_deadline=timedelta(seconds=ack_deadline)
    )

    async def fake_job_work(msg_handle: AsyncContextManager[None]) -> None:
        async with msg_handle:
            await anyio.sleep(0.25)

    async with connect_to_queue("test-queue", migrated_pool) as queue:

        for _ in range(10):
            async with queue.send(b"{}"):
                pass

        with anyio.fail_after(1):
            async with anyio.create_task_group() as worker_tg:
                async for msg_handle in queue.poll(batch_size=10):
                    worker_tg.start_soon(fake_job_work, msg_handle)


@pytest.mark.anyio
async def test_concurrent_worker_pull_atomic_delivery(
    migrated_pool: asyncpg.Pool,
) -> None:
    ack_deadline = 1
    await create_queue(
        "test-queue", migrated_pool, ack_deadline=timedelta(seconds=ack_deadline)
    )
    async with connect_to_queue("test-queue", migrated_pool) as queue:
        events: List[str] = []

        async with anyio.create_task_group() as tg:

            async def worker() -> None:
                async for msg_handle in queue.poll():
                    with anyio.CancelScope(shield=True):
                        async with msg_handle:
                            events.append("received")
                            # maybe let other worker grab job
                            await anyio.sleep(ack_deadline * 1.25)
                            events.append("done processing")
                        events.append("acked")

            tg.start_soon(worker)
            tg.start_soon(worker)

            with anyio.fail_after(ack_deadline * 5):  # just to fail fast in testing
                async with queue.send(b'{"foo":"bar"}') as completion_handle:
                    events.append("sent")
                    await completion_handle()
                    events.append("completed")
                    tg.cancel_scope.cancel()

        # we check that the message was only received and processed once
        assert events in (
            ["sent", "received", "done processing", "acked", "completed"],
            ["sent", "received", "done processing", "completed", "acked"],
        )


@pytest.mark.anyio
async def test_enqueue_with_delay(
    queue: Queue,
) -> None:
    async with queue.send(b'{"foo":"bar"}', delay=timedelta(seconds=1)):
        pass

    with anyio.move_on_after(0.5):
        async for _ in queue.poll():
            assert False, "should not be called"

    await anyio.sleep(0.5)

    with anyio.fail_after(0.1):
        async for msg_handle in queue.poll():
            async with msg_handle as msg:
                assert msg.body == b'{"foo":"bar"}', msg.body


@pytest.mark.anyio
async def test_pull_fifo(
    queue: Queue,
) -> None:
    async with queue.send(b"1"):
        pass

    async with queue.send(b"2"):
        pass

    async for msg_handle in queue.poll():
        async with msg_handle as msg:
            assert msg.body == b"1"

    async for msg_handle in queue.poll():
        async with msg_handle as msg:
            assert msg.body == b"2"


@pytest.mark.anyio
async def test_completion_handle_awaited(
    queue: Queue,
) -> None:
    events: List[str] = []

    async with anyio.create_task_group() as tg:

        async def worker() -> None:
            async for msg_handle in queue.poll():
                async with msg_handle:
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
            async for _ in queue.poll(poll_interval=60):
                rcv_times.append(time())

        tg.start_soon(worker)

        # make sure poll is sleeping
        await anyio.sleep(1)

        async with queue.send(b'{"foo":"bar"}'):
            send_times.append(time())
            pass

    assert len(send_times) == len(rcv_times)
    # not deterministic
    # but generally we are checking that elapsed time
    # between a send and rcv << poll_interval
    assert rcv_times[0] - send_times[0] < 0.1


@pytest.mark.anyio
async def test_batched_rcv(
    queue: Queue,
) -> None:
    async with anyio.create_task_group() as tg:

        async def worker() -> None:
            n = 0
            async for msg_handle in queue.poll(batch_size=2):
                async with msg_handle:
                    pass
                n += 1
            assert n == 2

        for _ in range(2):
            async with queue.send("{}".encode()):
                pass

        tg.start_soon(worker)


@pytest.mark.anyio
async def test_batched_rcv_is_interrupted(
    queue: Queue,
) -> None:
    n = 0

    async with anyio.create_task_group() as tg:

        async def worker() -> None:
            nonlocal n
            async for msg_handle in queue.poll(batch_size=2):  # pragma: no cover
                async with msg_handle:
                    pass
                n += 1
                break

        for _ in range(2):
            async with queue.send("{}".encode()):
                pass

        tg.start_soon(worker)

    assert n == 1


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
        with anyio.move_on_after(1) as scope:
            async for _ in queue.poll(poll_interval=0.05):
                assert False, "should not be called"  # pragma: no cover
        assert scope.cancel_called is True
