from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Dict,
    Mapping,
    Optional,
)
from uuid import UUID

if sys.version_info < (3, 8):  # pragma: no cover
    from typing_extensions import Protocol
else:
    from typing import Protocol

import anyio

_DATACLASSES_KW: Dict[str, Any] = {}
if sys.version_info >= (3, 10):  # pragma: no cover
    _DATACLASSES_KW["slots"] = True


@dataclass(frozen=True, **_DATACLASSES_KW)
class Message:
    id: UUID
    body: bytes


@dataclass(frozen=True, **_DATACLASSES_KW)
class QueueStatistics:
    # total number of messages currently in the queue
    total_messages_in_queue: int
    # total number of messages that have never been delivered
    # does not include messages that have been delivered and nacked
    # or are pending re-delivery
    undelivered_messages: int


class MessageHandle(Protocol):
    def acquire(self) -> AsyncContextManager[Message]:
        ...


class CompletionHandle(Protocol):
    @property
    def messages(self) -> Mapping[UUID, anyio.Event]:
        """Completion events for each published message"""
        ...

    async def wait(self) -> None:
        """Wait for all messages to complete"""
        ...


class MessageHandleStream(Protocol):
    def __aiter__(self) -> AsyncIterator[MessageHandle]:
        ...

    def __anext__(self) -> Awaitable[MessageHandle]:
        ...

    def receive(self) -> Awaitable[MessageHandle]:
        ...


class Queue(ABC):
    @abstractmethod
    def receive(
        self,
        batch_size: int = 1,
        poll_interval: float = 1,
    ) -> AsyncContextManager[MessageHandleStream]:
        """Poll for a batch of messages.

        Will wait until at least one and up to `batch_size` messages are available
        by periodically checking the queue every `poll_interval` seconds.

        When a new message is put on the queue a notification is sent that will cause
        immediate polling, thus in practice the latency will be much lower than
        poll interval.
        This mechanism is however not 100% reliable so worst case latency is still
        `poll_interval`.

        Args:
            batch_size (int, optional): maximum number of messages to gether.
                Defaults to 1.
            poll_interval (float, optional): interval between polls of the queue.
                Defaults to 1.

        Returns:
            AsyncContextManager[MessageHandleStream]: An iterator over messages.
        """
        pass  # pragma: no cover

    @abstractmethod
    def send(
        self,
        __body: bytes,
        *bodies: bytes,
        expire_at: Optional[datetime] = ...,
        schedule_at: Optional[datetime] = ...,
    ) -> AsyncContextManager[CompletionHandle]:
        """Put messages on the queue.

        You _must_ enter the context manager but awaiting the completion
        handle is optional.

        Args:
            body (bytes): arbitrary bytes, the body of the message.

        Returns:
            AsyncContextManager[MessageCompletionHandle]
        """
        pass  # pragma: no cover

    @abstractmethod
    def wait_for_completion(
        self,
        message: UUID,
        *messages: UUID,
        poll_interval: timedelta = timedelta(seconds=10),
    ) -> AsyncContextManager[CompletionHandle]:
        """Wait for a message or group of messages to complete

        Args:
            message (UUID): message ID as returned by Queue.send()
            poll_interval (timedelta, optional): interval to poll for completion. Defaults to 10 seconds.

        Returns:
            AsyncContextManager[CompletionHandle]: A context manager that returns a completion handle.
        """
        pass  # pragma: no cover

    @abstractmethod
    def cancel(
        self,
        message: UUID,
        *messages: UUID,
    ) -> Awaitable[None]:
        """Cancel in-flight messages

        Args:
            message (UUID): message ID as returned by Queue.send()
        """
        pass  # pragma: no cover

    @abstractmethod
    async def get_statistics(self) -> QueueStatistics:
        """Gather statistics from the queue.

        Returns:
            QueueStatistics: information on the current state of the queue.
        """
        pass  # pragma: no cover
