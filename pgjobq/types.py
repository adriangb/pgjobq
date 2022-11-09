from __future__ import annotations

from typing import Union

from asyncpg import Connection, Pool  # type: ignore

PoolOrConnection = Union[Pool, Connection]
