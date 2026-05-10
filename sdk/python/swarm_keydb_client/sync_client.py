"""Synchronous wrapper around :class:`AsyncSwarmKeyDbClient`.

This module provides :class:`SwarmKeyDbClient`, a convenience wrapper that
bridges the async client into a synchronous API suitable for scripts, notebooks,
and interactive REPL sessions without any event-loop configuration.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any, Dict, List, Optional, Tuple, Union

from .client import AsyncSwarmKeyDbClient, _StreamEntry, _StreamResult


class _EventLoopThread(threading.Thread):
    """A daemon thread that owns a persistent event loop."""

    def __init__(self) -> None:
        super().__init__(daemon=True, name="swarm-keydb-sync-loop")
        self.loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        self._ready = threading.Event()

    def run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.call_soon(self._ready.set)
        self.loop.run_forever()

    def stop(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)


def _run(coro: Any, loop: asyncio.AbstractEventLoop) -> Any:
    """Submit *coro* to *loop* running on another thread and block."""
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result()


class SwarmKeyDbClient:
    """Synchronous WebSocket-first client for SwarmKeyDb.

    A thin synchronous wrapper around :class:`AsyncSwarmKeyDbClient`.  All
    methods block the calling thread until the server responds.

    Args:
        ws_url: WebSocket endpoint. Defaults to ``ws://127.0.0.1:8765/``.
        http_url: HTTP fallback base URL. Defaults to ``http://127.0.0.1:8080``.
        password: Optional AUTH password sent automatically after connect.
        reconnect: Whether to attempt automatic reconnection on disconnect.
        reconnect_base_delay: Initial reconnect backoff in seconds.
        reconnect_max_delay: Maximum reconnect backoff in seconds.
        request_timeout: Per-command timeout in seconds.
        http_fallback: Fall back to HTTP for ``get``/``set`` when WebSocket is
            unavailable.

    Example::

        from swarm_keydb_client import SwarmKeyDbClient

        client = SwarmKeyDbClient("ws://localhost:7379")
        client.set("hello", "world")
        print(client.get("hello"))  # b"world"
        client.close()
    """

    def __init__(
        self,
        ws_url: str = "ws://127.0.0.1:8765/",
        http_url: str = "http://127.0.0.1:8080",
        password: Optional[str] = None,
        *,
        reconnect: bool = True,
        reconnect_base_delay: float = 0.25,
        reconnect_max_delay: float = 5.0,
        request_timeout: float = 10.0,
        http_fallback: bool = True,
    ) -> None:
        self._thread = _EventLoopThread()
        self._thread.start()
        self._thread._ready.wait()
        self._loop = self._thread.loop
        self._async = AsyncSwarmKeyDbClient(
            ws_url=ws_url,
            http_url=http_url,
            password=password,
            reconnect=reconnect,
            reconnect_base_delay=reconnect_base_delay,
            reconnect_max_delay=reconnect_max_delay,
            request_timeout=request_timeout,
            http_fallback=http_fallback,
        )

    def _run(self, coro: Any) -> Any:
        return _run(coro, self._loop)

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Connect to SwarmKeyDb via WebSocket."""
        self._run(self._async.connect())

    def close(self) -> None:
        """Close the connection and stop the background event loop."""
        self._run(self._async.close())
        self._thread.stop()

    # ------------------------------------------------------------------
    # Core commands
    # ------------------------------------------------------------------

    def get(self, key: str) -> Optional[bytes]:
        """Return the value of *key* or ``None``."""
        return self._run(self._async.get(key))

    def set(
        self,
        key: str,
        value: Union[str, bytes, int, float],
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
        keepttl: bool = False,
    ) -> bool:
        """Set *key* to *value*."""
        return self._run(self._async.set(key, value, ex=ex, px=px, nx=nx, xx=xx, keepttl=keepttl))

    def delete(self, *keys: str) -> int:
        """Delete one or more keys."""
        return self._run(self._async.delete(*keys))

    def exists(self, *keys: str) -> int:
        """Return the number of *keys* that exist."""
        return self._run(self._async.exists(*keys))

    def expire(self, key: str, seconds: int) -> bool:
        """Set a timeout on *key* in seconds."""
        return self._run(self._async.expire(key, seconds))

    def pexpire(self, key: str, milliseconds: int) -> bool:
        """Set a timeout on *key* in milliseconds."""
        return self._run(self._async.pexpire(key, milliseconds))

    def ttl(self, key: str) -> int:
        """Return the remaining TTL of *key* in seconds."""
        return self._run(self._async.ttl(key))

    def pttl(self, key: str) -> int:
        """Return the remaining TTL of *key* in milliseconds."""
        return self._run(self._async.pttl(key))

    def keys(self, pattern: str = "*") -> List[str]:
        """Return all keys matching *pattern*."""
        return self._run(self._async.keys(pattern))

    def scan(
        self,
        cursor: int = 0,
        match: Optional[str] = None,
        count: Optional[int] = None,
    ) -> Tuple[int, List[str]]:
        """Incrementally iterate the keyspace."""
        return self._run(self._async.scan(cursor, match=match, count=count))

    def type(self, key: str) -> str:
        """Return the type of *key*."""
        return self._run(self._async.type(key))

    def rename(self, key: str, newkey: str) -> bool:
        """Rename *key* to *newkey*."""
        return self._run(self._async.rename(key, newkey))

    def persist(self, key: str) -> bool:
        """Remove the timeout from *key*."""
        return self._run(self._async.persist(key))

    # ------------------------------------------------------------------
    # String commands
    # ------------------------------------------------------------------

    def append(self, key: str, value: str) -> int:
        """Append *value* to *key*."""
        return self._run(self._async.append(key, value))

    def getrange(self, key: str, start: int, end: int) -> bytes:
        """Return a substring of *key*."""
        return self._run(self._async.getrange(key, start, end))

    def setrange(self, key: str, offset: int, value: str) -> int:
        """Overwrite part of *key* starting at *offset*."""
        return self._run(self._async.setrange(key, offset, value))

    def strlen(self, key: str) -> int:
        """Return the length of the string stored at *key*."""
        return self._run(self._async.strlen(key))

    def incr(self, key: str) -> int:
        """Increment *key* by 1."""
        return self._run(self._async.incr(key))

    def incrby(self, key: str, amount: int) -> int:
        """Increment *key* by *amount*."""
        return self._run(self._async.incrby(key, amount))

    def incrbyfloat(self, key: str, amount: float) -> float:
        """Increment *key* by *amount* (float)."""
        return self._run(self._async.incrbyfloat(key, amount))

    def decr(self, key: str) -> int:
        """Decrement *key* by 1."""
        return self._run(self._async.decr(key))

    def decrby(self, key: str, amount: int) -> int:
        """Decrement *key* by *amount*."""
        return self._run(self._async.decrby(key, amount))

    def mget(self, *keys: str) -> List[Optional[bytes]]:
        """Return the values of all *keys*."""
        return self._run(self._async.mget(*keys))

    def mset(self, mapping: Dict[str, str]) -> bool:
        """Set multiple keys at once."""
        return self._run(self._async.mset(mapping))

    def getset(self, key: str, value: str) -> Optional[bytes]:
        """Set *key* to *value* and return the old value."""
        return self._run(self._async.getset(key, value))

    # ------------------------------------------------------------------
    # Pub/Sub
    # ------------------------------------------------------------------

    def publish(self, channel: str, message: str) -> int:
        """Publish *message* to *channel*."""
        return self._run(self._async.publish(channel, message))

    # ------------------------------------------------------------------
    # Stream commands
    # ------------------------------------------------------------------

    def xadd(
        self,
        stream: str,
        fields: Dict[str, str],
        id: str = "*",
        maxlen: Optional[int] = None,
        approximate: bool = True,
    ) -> str:
        """Append a new entry to *stream*."""
        return self._run(self._async.xadd(stream, fields, id=id, maxlen=maxlen, approximate=approximate))

    def xlen(self, stream: str) -> int:
        """Return the number of entries in *stream*."""
        return self._run(self._async.xlen(stream))

    def xrange(
        self,
        stream: str,
        start: str = "-",
        end: str = "+",
        count: Optional[int] = None,
    ) -> List[_StreamEntry]:
        """Return entries in *stream* between *start* and *end*."""
        return self._run(self._async.xrange(stream, start, end, count=count))

    def xrevrange(
        self,
        stream: str,
        end: str = "+",
        start: str = "-",
        count: Optional[int] = None,
    ) -> List[_StreamEntry]:
        """Return entries in *stream* in reverse order."""
        return self._run(self._async.xrevrange(stream, end, start, count=count))

    def xread(
        self,
        streams: Dict[str, str],
        count: Optional[int] = None,
        block: Optional[int] = None,
    ) -> Optional[_StreamResult]:
        """Read entries from one or more streams."""
        return self._run(self._async.xread(streams, count=count, block=block))

    def xack(self, stream: str, group: str, *ids: str) -> int:
        """Acknowledge one or more messages in a consumer group."""
        return self._run(self._async.xack(stream, group, *ids))

    def xtrim(self, stream: str, maxlen: int, approximate: bool = True) -> int:
        """Trim *stream* to at most *maxlen* entries."""
        return self._run(self._async.xtrim(stream, maxlen, approximate=approximate))

    def xgroup_create(
        self,
        stream: str,
        group: str,
        id: str = "$",
        mkstream: bool = False,
    ) -> bool:
        """Create a consumer group."""
        return self._run(self._async.xgroup_create(stream, group, id=id, mkstream=mkstream))

    # ------------------------------------------------------------------
    # Transaction commands
    # ------------------------------------------------------------------

    def multi(self) -> bool:
        """Begin a transaction block."""
        return self._run(self._async.multi())

    def exec(self) -> Optional[List[Any]]:
        """Execute a transaction block."""
        return self._run(self._async.exec())

    def discard(self) -> bool:
        """Discard a transaction block."""
        return self._run(self._async.discard())

    def watch(self, *keys: str) -> bool:
        """Watch *keys* for modifications (optimistic locking)."""
        return self._run(self._async.watch(*keys))

    def unwatch(self) -> bool:
        """Forget all watched keys."""
        return self._run(self._async.unwatch())

    # ------------------------------------------------------------------
    # Auth / server commands
    # ------------------------------------------------------------------

    def auth(self, password: str) -> bool:
        """Authenticate with *password*."""
        return self._run(self._async.auth(password))

    def ping(self, message: Optional[str] = None) -> str:
        """Send a PING to the server."""
        return self._run(self._async.ping(message))

    def raw(self, command: str, *args: str) -> Any:
        """Execute an arbitrary command."""
        return self._run(self._async.raw(command, *args))
