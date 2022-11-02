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
    Sequence,
    Union,
    overload,
)
from uuid import UUID

if sys.version_info < (3, 8):  # pragma: no cover
    from typing_extensions import Protocol
else:
    from typing import Protocol

from pgjobq._filters import BaseClause

_DATACLASSES_KW: Dict[str, Any] = {}
if sys.version_info >= (3, 10):  # pragma: no cover
    _DATACLASSES_KW["slots"] = True

_ScalarValue = Union[str, int, float, bool, None]


@dataclass(frozen=True, **_DATACLASSES_KW)
class Job:
    id: UUID
    body: bytes
    attributes: Dict[str, _ScalarValue]


@dataclass(frozen=True, **_DATACLASSES_KW)
class OutgoingJob:
    body: bytes
    attributes: Optional[Dict[str, _ScalarValue]] = None
    dependencies: Sequence[UUID] = ()


@dataclass(**_DATACLASSES_KW)
class QueueStatistics:
    # total number of jobs currently in the queue
    jobs: int
    max_size: Optional[int]


class JobHandle(Protocol):
    def acquire(self) -> AsyncContextManager[Job]:
        ...


class CompletionEvent(Protocol):
    async def wait(self) -> None:
        ...


class CompletionHandle(Protocol):
    @property
    def jobs(self) -> Mapping[UUID, CompletionEvent]:
        """Completion events for each published job"""
        ...

    async def wait(self) -> None:
        """Wait for all jobs to complete"""
        ...


class JobHandleStream(Protocol):
    def __aiter__(self) -> AsyncIterator[JobHandle]:
        ...

    def __anext__(self) -> Awaitable[JobHandle]:
        ...

    def receive(self) -> Awaitable[JobHandle]:
        ...


class Queue(ABC):
    @abstractmethod
    def receive(
        self,
        *,
        batch_size: int = 1,
        poll_interval: float = 1,
        filter: Optional[BaseClause] = None,
    ) -> AsyncContextManager[JobHandleStream]:
        """Poll for a batch of jobs.

        Will wait until at least one and up to `batch_size` jobs are available
        by periodically checking the queue every `poll_interval` seconds.

        When a new job is put on the queue a notification is sent that will cause
        immediate polling, thus in practice the latency will be much lower than
        poll interval.
        This mechanism is however not 100% reliable so worst case latency is still
        `poll_interval`.

        Args:
            batch_size (int, optional): maximum number of jobs to gether.
                Defaults to 1.
            poll_interval (float, optional): interval between polls of the queue.
                Defaults to 1.

        Returns:
            AsyncContextManager[JobHandleStream]: An iterator over jobs.
        """
        pass  # pragma: no cover

    @abstractmethod
    @overload
    def send(
        self,
        __body: bytes,
        *bodies: bytes,
        schedule_at: Optional[datetime] = ...,
    ) -> AsyncContextManager[CompletionHandle]:
        ...  # pragma: no cover

    @abstractmethod
    @overload
    def send(
        self,
        __body: OutgoingJob,
        *bodies: OutgoingJob,
        schedule_at: Optional[datetime] = ...,
    ) -> AsyncContextManager[CompletionHandle]:
        ...  # pragma: no cover

    @abstractmethod
    def send(
        self,
        __body: Union[bytes, OutgoingJob],
        *bodies: Union[bytes, OutgoingJob],
        schedule_at: Optional[datetime] = ...,
    ) -> AsyncContextManager[CompletionHandle]:
        """Put jobs on the queue.

        You _must_ enter the context manager but awaiting the completion
        handle is optional.

        Args:
            body (bytes): arbitrary bytes, the body of the job.

        Returns:
            AsyncContextManager[JobCompletionHandle]
        """
        pass  # pragma: no cover

    @abstractmethod
    def wait_for_completion(
        self,
        job: UUID,
        *jobs: UUID,
        poll_interval: timedelta = timedelta(seconds=10),
    ) -> AsyncContextManager[CompletionHandle]:
        """Wait for a job or group of jobs to complete

        Args:
            job (UUID): job ID as returned by Queue.send()
            poll_interval (timedelta, optional): interval to poll for completion. Defaults to 10 seconds.

        Returns:
            AsyncContextManager[CompletionHandle]: A context manager that returns a completion handle.
        """
        pass  # pragma: no cover

    @overload
    @abstractmethod
    def cancel(
        self,
        __filter: BaseClause,
    ) -> Awaitable[None]:
        ...

    @overload
    @abstractmethod
    def cancel(
        self,
        __job: UUID,
        *jobs: UUID,
    ) -> Awaitable[None]:
        ...

    @abstractmethod
    def cancel(
        self,
        __job_or_filter: Union[UUID, BaseClause],
        *jobs: UUID,
    ) -> Awaitable[None]:
        """Cancel in-flight jobs

        Args:
            job (UUID): job ID as returned by Queue.send()
        """
        pass  # pragma: no cover

    @abstractmethod
    async def get_statistics(self) -> QueueStatistics:
        """Gather statistics from the queue.

        Returns:
            QueueStatistics: information on the current state of the queue.
        """
        pass  # pragma: no cover

    async def wait_if_full(self) -> None:
        pass  # pragma: no cover
