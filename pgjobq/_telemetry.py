from __future__ import annotations

import sys
from contextlib import asynccontextmanager
from typing import Any, AsyncContextManager, AsyncIterator, Sequence, TypeVar

if sys.version_info < (3, 8):
    from typing_extensions import Protocol
else:
    from typing import Protocol

from pgjobq.types import PoolOrConnection

PoolOrConnectionT = TypeVar("PoolOrConnectionT", bound=PoolOrConnection)


class TelemetryHook(Protocol):
    def on_query(
        self,
        queue_name: str,
        operation: str,
        query: str,
        conn: PoolOrConnectionT,
        args: Sequence[Any],
    ) -> AsyncContextManager[PoolOrConnectionT]:
        """Log, explain or otherwise manipulate the query"""
        ...


class NoOpTelemetryHook:
    @asynccontextmanager
    async def on_query(
        self,
        queue_name: str,
        operation: str,
        query: str,
        conn: PoolOrConnectionT,
        args: Sequence[Any],
    ) -> AsyncIterator[PoolOrConnectionT]:
        yield conn
