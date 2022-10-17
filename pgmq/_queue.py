from __future__ import annotations

import enum
import sys
from collections import defaultdict
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
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
)
from uuid import UUID, uuid4

import anyio
import asyncpg  # type: ignore
from anyio.abc import TaskGroup

from pgmq._queries import (
    ack_message,
    extend_ack_deadlines,
    get_completed_messages,
    get_statistics,
    nack_message,
    poll_for_messages,
    publish_messages_from_bytes,
)
from pgmq.api import CompletionHandle as AbstractCompletionHandle
from pgmq.api import Message, MessageHandle
from pgmq.api import MessageHandleStream as AbstractMessageHandleStream
from pgmq.api import Queue as AbstractQueue
from pgmq.api import QueueStatistics

DATACLASSES_KW: Dict[str, Any] = {}
if sys.version_info >= (3, 10):  # pragma: no cover
    DATACLASSES_KW["slots"] = True


@dataclass(**DATACLASSES_KW, frozen=True)
class MessageCompletionHandle:
    messages: Mapping[UUID, anyio.Event]

    async def wait(self) -> None:
        async with anyio.create_task_group() as tg:
            for event in self.messages.values():
                tg.start_soon(event.wait)
        return None


class MessageState(enum.Enum):
    created = enum.auto()
    processing = enum.auto()
    succeeded = enum.auto()
    failed = enum.auto()
    out_of_scope = enum.auto()


@dataclass(**DATACLASSES_KW, eq=False)
class MessageManager:
    pool: asyncpg.Pool
    message: Message
    queue_name: str
    pending_messages: Set[MessageManager]
    queue: Queue
    state: MessageState = MessageState.created

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[Message]:
        if self.state is MessageState.out_of_scope:
            raise RuntimeError(
                "Attempted to acquire a handle to a message is no longer available,"
                " possibly because Queue.receive() went out of scope"
            )
        elif self.state in (MessageState.failed, MessageState.succeeded):
            raise RuntimeError(
                "Attempted to acquire a handle to a message that already completed"
            )
        elif self.state is MessageState.processing:
            raise RuntimeError(
                "Attempted to acquire a handle that is already being processed"
            )
        self.state = MessageState.processing
        state = MessageState.succeeded

        async def listen_for_cancellation(tg: TaskGroup) -> None:
            async with self.queue.wait_for_completion(self.message.id) as handle:
                await handle.wait()
                tg.cancel_scope.cancel()

        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(listen_for_cancellation, tg)
                yield self.message
                tg.cancel_scope.cancel()
        except Exception:
            # handle exceptions after entering the message's context manager
            state = MessageState.failed
            raise
        finally:
            await self.shutdown(state)

    async def shutdown(self, final_state: MessageState) -> None:
        self.pending_messages.discard(self)
        if self.state not in (MessageState.created, MessageState.processing):
            return
        conn: asyncpg.Connection
        try:
            async with self.pool.acquire() as conn:  # type: ignore
                if final_state is MessageState.succeeded:
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
class MessageHandleStream:
    get_nxt: Callable[[], Awaitable[MessageHandle]]

    def __aiter__(self) -> MessageHandleStream:
        return self

    def __anext__(self) -> Awaitable[MessageHandle]:
        return self.get_nxt()

    receive = __anext__


@dataclass(**DATACLASSES_KW)
class Queue(AbstractQueue):
    pool: asyncpg.Pool
    queue_name: str
    completion_callbacks: Dict[UUID, Set[anyio.Event]]
    new_message_callbacks: Set[Callable[[], None]]
    in_flight_messages: Dict[UUID, Set[MessageManager]]

    @asynccontextmanager
    async def receive(
        self,
        batch_size: int = 1,
        poll_interval: float = 1,
    ) -> AsyncIterator[AbstractMessageHandleStream]:

        in_flight_messages: Set[MessageManager] = set()
        poll_id = uuid4()
        self.in_flight_messages[poll_id] = in_flight_messages

        unyielded_messages: List[MessageManager] = []

        async def get_messages() -> None:
            conn: asyncpg.Connection
            async with self.pool.acquire() as conn:  # type: ignore
                # use a transaction so that if we get cancelled or crash
                # the messages are still available
                async with conn.transaction():  # type: ignore
                    messages = await poll_for_messages(
                        conn,
                        queue_name=self.queue_name,
                        batch_size=batch_size,
                    )
                    managers = [
                        MessageManager(
                            pool=self.pool,
                            message=Message(id=message["id"], body=message["body"]),
                            queue_name=self.queue_name,
                            pending_messages=in_flight_messages,
                            queue=self,
                        )
                        for message in messages
                    ]
                    in_flight_messages.update(managers)
                    unyielded_messages.extend(managers)

        await get_messages()

        async def get_next_message() -> MessageHandle:
            while not unyielded_messages:
                await get_messages()
                if unyielded_messages:
                    break

                # wait for a new message to be published or the poll interval to expire
                new_message = anyio.Event()
                self.new_message_callbacks.add(new_message.set)  # type: ignore

                async def skip_forward_if_timeout() -> None:
                    await anyio.sleep(poll_interval)
                    new_message.set()

                try:
                    async with anyio.create_task_group() as gather_tg:
                        gather_tg.start_soon(skip_forward_if_timeout)
                        await new_message.wait()
                        gather_tg.cancel_scope.cancel()
                finally:
                    self.new_message_callbacks.discard(new_message.set)  # type: ignore

            return unyielded_messages.pop()

        try:
            yield MessageHandleStream(get_next_message)
        finally:
            self.in_flight_messages.pop(poll_id)
            for manager in in_flight_messages.copy():
                await manager.shutdown(MessageState.out_of_scope)

    def send(
        self,
        message: bytes,
        *messages: bytes,
        expire_at: Optional[datetime] = None,
        schedule_at: Optional[datetime] = None,
    ) -> AsyncContextManager[AbstractCompletionHandle]:
        bodies: List[bytes] = [message, *messages]  # type: ignore
        ids = [uuid4() for _ in range(len(bodies))]
        publish = publish_messages_from_bytes(
            conn=self.pool,
            queue_name=self.queue_name,
            ids=ids,
            bodies=bodies,
            expire_at=expire_at,
            schedule_at=schedule_at,
        )

        @asynccontextmanager
        async def cm() -> AsyncIterator[AbstractCompletionHandle]:
            # create the message id application side
            # so that we can start listening before we send
            async with self.wait_for_completion(*ids, poll_interval=None) as handle:
                await publish
                yield handle

        return cm()

    async def cancel(self, message: UUID, *messages: UUID) -> None:
        conn: asyncpg.Connection
        async with self.pool.acquire() as conn:  # type: ignore
            await ack_message(conn, self.queue_name, message, *messages)

    def wait_for_completion(
        self,
        message: UUID,
        *messages: UUID,
        poll_interval: Optional[timedelta] = timedelta(seconds=10),
    ) -> AsyncContextManager[AbstractCompletionHandle]:
        @asynccontextmanager
        async def cm() -> AsyncIterator[AbstractCompletionHandle]:
            done_events = {id: anyio.Event() for id in (message, *messages)}
            for message_id, event in done_events.items():
                self.completion_callbacks[message_id].add(event)

            def cleanup_done_events() -> None:
                for message_id in done_events.copy().keys():
                    if done_events[message_id].is_set():
                        event = done_events.pop(message_id)
                        self.completion_callbacks[message_id].discard(event)
                        if not self.completion_callbacks[message_id]:
                            self.completion_callbacks.pop(message_id)

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
                        completed_messages = await get_completed_messages(
                            self.pool,
                            self.queue_name,
                            message_ids=list(done_events.keys()),
                        )
                        if completed_messages:
                            new_completion.set()
                        remaining -= len(completed_messages)
                        for message in completed_messages:
                            done_events[message].set()
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
                    yield MessageCompletionHandle(messages=done_events.copy())
                    tg.cancel_scope.cancel()
            finally:
                cleanup_done_events()

        return cm()

    async def get_statistics(self) -> QueueStatistics:
        record = await get_statistics(self.pool, self.queue_name)
        return QueueStatistics(
            total_messages_in_queue=record["current_message_count"],
            undelivered_messages=record["undelivered_message_count"],
        )


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
    new_message_callbacks: Set[Callable[[], None]] = set()
    checked_out_messages: Dict[UUID, Set[MessageManager]] = {}

    async def run_cleanup(conn: asyncpg.Connection) -> None:
        while True:
            await conn.execute(  # type: ignore
                "SELECT pgmq.cleanup_dead_messages()",
            )
            await anyio.sleep(1)

    async def extend_acks(conn: asyncpg.Connection) -> None:
        while True:
            message_ids = [
                message.message.id
                for messages in checked_out_messages.values()
                for message in messages
            ]
            await extend_ack_deadlines(
                conn,
                queue_name,
                message_ids,
            )
            await anyio.sleep(0.5)  # less than the min ack deadline

    async def process_completion_notification(
        conn: asyncpg.Connection,
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        message_id = payload
        message_id_key = UUID(message_id)
        events = completion_callbacks.get(message_id_key, None) or ()
        for event in events:
            event.set()

    async def process_new_message_notification(
        conn: asyncpg.Connection,
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        for cb in new_message_callbacks:
            cb()

    async with AsyncExitStack() as stack:
        cleanup_conn: asyncpg.Connection = await stack.enter_async_context(pool.acquire())  # type: ignore
        ack_conn: asyncpg.Connection = await stack.enter_async_context(pool.acquire())  # type: ignore
        completion_channel = f"pgmq.message_completed_{queue_name}"
        new_message_channel = f"pgmq.new_message_{queue_name}"
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
            channel=new_message_channel,
            callback=process_new_message_notification,
        )
        stack.push_async_callback(
            cleanup_conn.remove_listener,  # type: ignore
            channel=new_message_channel,
            callback=process_new_message_notification,
        )
        async with anyio.create_task_group() as tg:
            tg.start_soon(run_cleanup, cleanup_conn)
            tg.start_soon(extend_acks, ack_conn)

            queue = Queue(
                pool=pool,
                queue_name=queue_name,
                completion_callbacks=completion_callbacks,
                new_message_callbacks=new_message_callbacks,
                in_flight_messages=checked_out_messages,
            )
            try:
                yield queue
            finally:
                tg.cancel_scope.cancel()
