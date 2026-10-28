"""aiosqlite compatibility layer.

Uses the real aiosqlite package when available.  A small sqlite3-backed async
fallback keeps the service operational in constrained/offline environments.
The fallback mirrors only the API surface used by this project.
"""
from __future__ import annotations

try:  # pragma: no cover - exercised when dependency is installed
    from aiosqlite import *  # type: ignore  # noqa: F401,F403
    from aiosqlite import Connection, Cursor, Row, connect  # type: ignore
except ImportError:  # pragma: no cover - covered by project smoke tests here
    import asyncio
    import sqlite3
    from typing import Any, Iterable, Optional

    Row = sqlite3.Row

    class Cursor:
        def __init__(self, cursor: sqlite3.Cursor):
            self._cursor = cursor

        @property
        def rowcount(self) -> int:
            return self._cursor.rowcount

        @property
        def lastrowid(self) -> Optional[int]:
            return self._cursor.lastrowid

        async def fetchone(self):
            return await asyncio.to_thread(self._cursor.fetchone)

        async def fetchall(self):
            return await asyncio.to_thread(self._cursor.fetchall)

        async def close(self) -> None:
            await asyncio.to_thread(self._cursor.close)

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            await self.close()
            return False

    class _ExecuteContext:
        def __init__(self, connection: "Connection", sql: str, parameters: Iterable[Any] | None):
            self._connection = connection
            self._sql = sql
            self._parameters = parameters
            self._cursor: Cursor | None = None

        async def _run(self) -> Cursor:
            if self._cursor is None:
                self._cursor = await self._connection._execute(self._sql, self._parameters)
            return self._cursor

        def __await__(self):
            return self._run().__await__()

        async def __aenter__(self):
            return await self._run()

        async def __aexit__(self, exc_type, exc, tb):
            if self._cursor is not None:
                await self._cursor.close()
            return False

    class Connection:
        def __init__(self, database: str, **kwargs: Any):
            kwargs.setdefault("check_same_thread", False)
            self._conn = sqlite3.connect(database, **kwargs)
            self._lock = asyncio.Lock()

        @property
        def row_factory(self):
            return self._conn.row_factory

        @row_factory.setter
        def row_factory(self, factory):
            self._conn.row_factory = factory

        def execute(self, sql: str, parameters: Iterable[Any] | None = None) -> _ExecuteContext:
            return _ExecuteContext(self, sql, parameters)

        async def _execute(self, sql: str, parameters: Iterable[Any] | None = None) -> Cursor:
            async with self._lock:
                if parameters is None:
                    raw = await asyncio.to_thread(self._conn.execute, sql)
                else:
                    raw = await asyncio.to_thread(self._conn.execute, sql, parameters)
            return Cursor(raw)

        async def executemany(self, sql: str, seq_of_parameters) -> Cursor:
            async with self._lock:
                raw = await asyncio.to_thread(self._conn.executemany, sql, seq_of_parameters)
            return Cursor(raw)

        async def commit(self) -> None:
            async with self._lock:
                await asyncio.to_thread(self._conn.commit)

        async def rollback(self) -> None:
            async with self._lock:
                await asyncio.to_thread(self._conn.rollback)

        async def close(self) -> None:
            async with self._lock:
                await asyncio.to_thread(self._conn.close)

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            await self.close()
            return False

    class _ConnectContext:
        def __init__(self, database: str, **kwargs: Any):
            self._database = database
            self._kwargs = kwargs
            self._connection: Connection | None = None

        async def _open(self) -> Connection:
            if self._connection is None:
                self._connection = Connection(self._database, **self._kwargs)
            return self._connection

        def __await__(self):
            return self._open().__await__()

        async def __aenter__(self):
            return await self._open()

        async def __aexit__(self, exc_type, exc, tb):
            if self._connection is not None:
                await self._connection.close()
            return False

    def connect(database: str, **kwargs: Any) -> _ConnectContext:
        return _ConnectContext(database, **kwargs)
