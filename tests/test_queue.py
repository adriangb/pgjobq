from datetime import timedelta
from time import time
from typing import AsyncGenerator, List, Tuple

import anyio
import asyncpg  # type: ignore
import pytest

from pgjobq import Receive, Send, connect_to_queue, create_queue

SendRcv = Tuple[Send, Receive]


@pytest.fixture
async def send_rcv_pair(migrated_pool: asyncpg.Pool) -> AsyncGenerator[SendRcv, None]:
    await create_queue("test-queue", migrated_pool)
    async with connect_to_queue("test-queue", migrated_pool) as (send, rcv):
        yield (send, rcv)


@pytest.mark.anyio
async def test_completion_handle_ignored(
    send_rcv_pair: SendRcv,
) -> None:
    send, rcv = send_rcv_pair
    async with send.send(b'{"foo":"bar"}'):
        pass

    async for msg_handle in rcv.poll():
        async with msg_handle as msg:
            assert msg.body == b'{"foo":"bar"}', msg.body


@pytest.mark.anyio
async def test_worker_raises_exception(
    send_rcv_pair: SendRcv,
) -> None:
    class MyException(Exception):
        pass

    send, rcv = send_rcv_pair
    async with send.send(b'{"foo":"bar"}'):
        pass

    with pytest.raises(MyException):
        async for msg_handle in rcv.poll(batch_size=1):
            async with msg_handle as msg:
                raise MyException

    with anyio.fail_after(1):  # redelivery should be immediate
        async for msg_handle in rcv.poll(batch_size=1):
            async with msg_handle as msg:
                assert msg.body == b'{"foo":"bar"}', msg.body


@pytest.mark.anyio
async def test_concurrent_worker_pull_atomic_delivery(
    migrated_pool: asyncpg.Pool,
) -> None:
    ack_deadline = 1
    await create_queue(
        "test-queue", migrated_pool, ack_deadline=timedelta(seconds=ack_deadline)
    )
    async with connect_to_queue("test-queue", migrated_pool) as (send, rcv):
        events: List[str] = []

        async with anyio.create_task_group() as tg:

            async def worker() -> None:
                async for msg_handle in rcv.poll():
                    async with msg_handle:
                        events.append("received")
                        # maybe let other worker grab job
                        await anyio.sleep(ack_deadline * 1.25)
                        events.append("done processing")
                    events.append("acked")

            tg.start_soon(worker)
            tg.start_soon(worker)

            with anyio.fail_after(ack_deadline * 5):  # just to fail fast in testing
                async with send.send(b'{"foo":"bar"}') as completion_handle:
                    events.append("sent")
                    await completion_handle()
                    events.append("completed")
                    tg.cancel_scope.cancel()

        # we check that the message was only received and processed once
        assert events == ["sent", "received", "done processing", "acked", "completed"]


@pytest.mark.anyio
async def test_completion_handle_awaited(
    send_rcv_pair: SendRcv,
) -> None:
    send, rcv = send_rcv_pair

    events: List[str] = []

    async with anyio.create_task_group() as tg:

        async def worker() -> None:
            async for msg_handle in rcv.poll():
                async with msg_handle:
                    events.append("received")
                events.append("acked")

        tg.start_soon(worker)

        async with send.send(b'{"foo":"bar"}') as completion_handle:
            events.append("sent")
            await completion_handle()
            events.append("completed")

    assert events == ["sent", "received", "acked", "completed"]


@pytest.mark.anyio
async def test_new_message_notification_triggers_poll(
    send_rcv_pair: SendRcv,
) -> None:
    send, rcv = send_rcv_pair

    send_times: List[float] = []
    rcv_times: List[float] = []

    async with anyio.create_task_group() as tg:

        async def worker() -> None:
            async for _ in rcv.poll(poll_interval=60):
                rcv_times.append(time())

        tg.start_soon(worker)

        # make sure poll is sleeping
        await anyio.sleep(1)

        async with send.send(b'{"foo":"bar"}'):
            send_times.append(time())
            pass

    assert len(send_times) == len(rcv_times)
    # not deterministic
    # but generally we are checking that elapsed time
    # between a send and rcv << poll_interval
    assert rcv_times[0] - send_times[0] < 0.1


@pytest.mark.anyio
async def test_batched_rcv(
    send_rcv_pair: SendRcv,
) -> None:
    send, rcv = send_rcv_pair

    async with anyio.create_task_group() as tg:

        async def worker() -> None:
            n = 0
            async for msg_handle in rcv.poll(batch_size=2):
                async with msg_handle:
                    pass
                n += 1
            assert n == 2

        for _ in range(2):
            async with send.send("{}".encode()):
                pass

        tg.start_soon(worker)


@pytest.mark.anyio
async def test_batched_rcv_is_interrupted(
    send_rcv_pair: SendRcv,
) -> None:
    send, rcv = send_rcv_pair

    n = 0

    async with anyio.create_task_group() as tg:

        async def worker() -> None:
            nonlocal n
            async for msg_handle in rcv.poll(batch_size=2):  # pragma: no cover
                async with msg_handle:
                    pass
                n += 1
                break

        for _ in range(2):
            async with send.send("{}".encode()):
                pass

        tg.start_soon(worker)

    assert n == 1


@pytest.mark.anyio
async def test_send_to_non_existent_queue_raises_exception(
    migrated_pool: asyncpg.Pool,
) -> None:
    async with connect_to_queue("test-queue", migrated_pool) as (send, _):
        with pytest.raises(LookupError, match="Queue not found"):
            async with send.send(b'{"foo":"bar"}'):
                pass


@pytest.mark.anyio
async def test_receive_from_non_existent_queue_allowed(
    migrated_pool: asyncpg.Pool,
) -> None:
    # allow waiting on a non-existent queue so that workers
    # can be spun up and start listening before the queue is created
    async with connect_to_queue("test-queue", migrated_pool) as (_, rcv):
        with anyio.move_on_after(1) as scope:
            async for _ in rcv.poll(poll_interval=0.05):
                assert False, "should not be called"  # pragma: no cover
        assert scope.cancel_called is True
