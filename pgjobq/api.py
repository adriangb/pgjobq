import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Optional,
)
from uuid import UUID

_DATACLASSES_KW: Dict[str, Any] = {}
if sys.version_info > (3, 10):  # pragma: no cover
    _DATACLASSES_KW["slots"] = True


@dataclass(frozen=True, **_DATACLASSES_KW)
class Message:
    id: UUID
    body: bytes


CompletionHandle = Callable[[], Awaitable[None]]


class Queue(ABC):
    @abstractmethod
    def poll(
        self,
        batch_size: int = 1,
        poll_interval: float = 1,
        fifo: bool = False,
    ) -> AsyncIterator[AsyncContextManager[Message]]:
        """Poll for a batch of jobs.

        Will wait until at least one and up to `batch_size` jobs are available
        by periodically checking the queue every `poll_interval` seconds.

        When a new job is put on the queue a notification is sent that will cause
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
            AsyncIterator[AsyncContextManager[Message]]: An iterator over a batch of messages.
            Each message is wrapped by a context manager.
            If you exit with an error the message will be nacked.
            If you exit without an error it will be acked.
            As long as you are in the context manager the visibility timeout weill be
            continually extended.
        """
        pass

    @abstractmethod
    def send(
        self, body: bytes, *, delay: Optional[timedelta] = None
    ) -> AsyncContextManager[CompletionHandle]:
        """Put a job on the queue.

        You _must_ enter the context manager but awaiting the completion
        handle is optional.

        Args:
            body (bytes): arbitrary bytes, the body of the job.

        Returns:
            AsyncContextManager[WaitForDoneHandle]: A context manager that
        """
        pass
