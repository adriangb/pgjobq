from __future__ import annotations

import asyncpg  # type: ignore

PoolOrConnection = asyncpg.Pool | asyncpg.Connection
