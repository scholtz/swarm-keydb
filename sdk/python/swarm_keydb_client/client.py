"""Async WebSocket-first client for SwarmKeyDb."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from contextlib import asynccontextmanager
from typing import (
    Any,
    AsyncGenerator,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from .exceptions import (
    SwarmKeyDbAuthError,
    SwarmKeyDbCommandError,
    SwarmKeyDbConnectionError,
    SwarmKeyDbTimeoutError,
)

logger = logging.getLogger(__name__)

# Type aliases
_RespValue = Any
_StreamEntry = Tuple[str, Dict[str, str]]
_StreamResult = List[Tuple[str, List[_StreamEntry]]]


class _PendingRequest:
    __slots__ = ("future", "command")

    def __init__(self, future: "asyncio.Future[Any]", command: str) -> None:
        self.future = future
        self.command = command


class PubSubChannel:
    """Async context manager for a Pub/Sub subscription.

    Usage::

        async with client.subscribe("news") as ch:
            async for msg in ch.listen():
                print(msg["channel"], msg["data"])
    """

    def __init__(self, client: "AsyncSwarmKeyDbClient", channels: Sequence[str]) -> None:
        self._client = client
        self._channels = list(channels)
        self._queue: asyncio.Queue[Dict[str, str]] = asyncio.Queue()

    def _deliver(self, message: Dict[str, str]) -> None:
        self._queue.put_nowait(message)

    async def listen(self) -> AsyncGenerator[Dict[str, str], None]:
        """Yield messages from subscribed channels indefinitely."""
        while True:
            msg = await self._queue.get()
            yield msg

    async def _subscribe(self) -> None:
        for ch in self._channels:
            self._client._pubsub_handlers[ch] = self._deliver
        await self._client._send_command("SUBSCRIBE", *self._channels)

    async def _unsubscribe(self) -> None:
        for ch in self._channels:
            self._client._pubsub_handlers.pop(ch, None)
        with contextlib.suppress(Exception):
            await self._client._send_command("UNSUBSCRIBE", *self._channels)


class AsyncSwarmKeyDbClient:
    """Async WebSocket-first client for SwarmKeyDb.

    Connects via WebSocket (JSON framing) with automatic HTTP fallback for
    ``get`` / ``set`` when WebSocket is unavailable.

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

        client = AsyncSwarmKeyDbClient("ws://localhost:7379")
        await client.connect()
        await client.set("hello", "world")
        value = await client.get("hello")  # b"world"
        await client.close()
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
        self._ws_url = ws_url
        self._http_url = http_url.rstrip("/")
        self._password = password
        self._reconnect = reconnect
        self._reconnect_base_delay = reconnect_base_delay
        self._reconnect_max_delay = reconnect_max_delay
        self._request_timeout = request_timeout
        self._http_fallback = http_fallback

        self._ws: Any = None  # websockets.ClientConnection
        self._connect_lock: asyncio.Lock = asyncio.Lock()
        self._pending: asyncio.Queue[_PendingRequest] = asyncio.Queue()
        self._pending_list: List[_PendingRequest] = []
        self._reader_task: Optional[asyncio.Task[None]] = None
        self._reconnect_attempts: int = 0
        self._closed: bool = False
        self._pubsub_handlers: Dict[str, Any] = {}  # channel -> callable
        self._pattern_handlers: Dict[str, Any] = {}  # pattern -> callable
        self._in_pubsub: bool = False

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Connect to SwarmKeyDb via WebSocket.

        Raises:
            SwarmKeyDbConnectionError: If the connection cannot be established.
        """
        async with self._connect_lock:
            if self._ws is not None:
                return
            await self._open_ws()

    async def close(self) -> None:
        """Close the WebSocket connection and cancel background tasks."""
        self._closed = True
        if self._reader_task is not None:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reader_task
            self._reader_task = None
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()
            self._ws = None

    # ------------------------------------------------------------------
    # Core commands
    # ------------------------------------------------------------------

    async def get(self, key: str) -> Optional[bytes]:
        """Return the value of *key* or ``None`` if the key does not exist.

        Args:
            key: The key to retrieve.

        Returns:
            The value as ``bytes``, or ``None``.
        """
        result = await self._cmd("GET", key)
        if result is None:
            return None
        if isinstance(result, bytes):
            return result
        return str(result).encode()

    async def set(
        self,
        key: str,
        value: Union[str, bytes, int, float],
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
        keepttl: bool = False,
    ) -> bool:
        """Set *key* to *value*.

        Args:
            key: The key.
            value: The value.
            ex: Expire after *ex* seconds.
            px: Expire after *px* milliseconds.
            nx: Only set if the key does not already exist.
            xx: Only set if the key already exists.
            keepttl: Retain the existing TTL.

        Returns:
            ``True`` on success, ``False`` if the conditional (``nx``/``xx``)
            was not met.
        """
        args: List[str] = [key, str(value) if not isinstance(value, str) else value]
        if ex is not None:
            args += ["EX", str(ex)]
        elif px is not None:
            args += ["PX", str(px)]
        if nx:
            args.append("NX")
        elif xx:
            args.append("XX")
        if keepttl:
            args.append("KEEPTTL")
        result = await self._cmd("SET", *args)
        return result is not None and str(result).upper() == "OK"

    async def delete(self, *keys: str) -> int:
        """Delete one or more keys.  Returns the number of keys deleted."""
        return int(await self._cmd("DEL", *keys) or 0)

    async def exists(self, *keys: str) -> int:
        """Return the number of *keys* that exist."""
        return int(await self._cmd("EXISTS", *keys) or 0)

    async def expire(self, key: str, seconds: int) -> bool:
        """Set a timeout on *key* in seconds."""
        return bool(await self._cmd("EXPIRE", key, str(seconds)))

    async def pexpire(self, key: str, milliseconds: int) -> bool:
        """Set a timeout on *key* in milliseconds."""
        return bool(await self._cmd("PEXPIRE", key, str(milliseconds)))

    async def ttl(self, key: str) -> int:
        """Return the remaining TTL of *key* in seconds, or -1/-2."""
        return int(await self._cmd("TTL", key) or -2)

    async def pttl(self, key: str) -> int:
        """Return the remaining TTL of *key* in milliseconds, or -1/-2."""
        return int(await self._cmd("PTTL", key) or -2)

    async def keys(self, pattern: str = "*") -> List[str]:
        """Return all keys matching *pattern*."""
        result = await self._cmd("KEYS", pattern)
        return _to_str_list(result)

    async def scan(
        self,
        cursor: int = 0,
        match: Optional[str] = None,
        count: Optional[int] = None,
    ) -> Tuple[int, List[str]]:
        """Incrementally iterate the keyspace.

        Returns:
            A tuple ``(next_cursor, keys)``.  When *next_cursor* is ``0`` the
            full iteration is complete.
        """
        args = [str(cursor)]
        if match is not None:
            args += ["MATCH", match]
        if count is not None:
            args += ["COUNT", str(count)]
        result = await self._cmd("SCAN", *args)
        if isinstance(result, (list, tuple)) and len(result) == 2:
            next_cursor = int(result[0])
            ks = _to_str_list(result[1])
            return next_cursor, ks
        return 0, []

    async def type(self, key: str) -> str:
        """Return the type of *key* (string, list, set, zset, hash, stream)."""
        return str(await self._cmd("TYPE", key) or "none")

    async def rename(self, key: str, newkey: str) -> bool:
        """Rename *key* to *newkey*."""
        result = await self._cmd("RENAME", key, newkey)
        return str(result).upper() == "OK"

    async def persist(self, key: str) -> bool:
        """Remove the timeout from *key*."""
        return bool(await self._cmd("PERSIST", key))

    # ------------------------------------------------------------------
    # String commands
    # ------------------------------------------------------------------

    async def append(self, key: str, value: str) -> int:
        """Append *value* to *key* and return the new length."""
        return int(await self._cmd("APPEND", key, value) or 0)

    async def getrange(self, key: str, start: int, end: int) -> bytes:
        """Return a substring of *key*."""
        result = await self._cmd("GETRANGE", key, str(start), str(end))
        if isinstance(result, bytes):
            return result
        return str(result or "").encode()

    async def setrange(self, key: str, offset: int, value: str) -> int:
        """Overwrite part of *key* starting at *offset*."""
        return int(await self._cmd("SETRANGE", key, str(offset), value) or 0)

    async def strlen(self, key: str) -> int:
        """Return the length of the string stored at *key*."""
        return int(await self._cmd("STRLEN", key) or 0)

    async def incr(self, key: str) -> int:
        """Increment *key* by 1."""
        return int(await self._cmd("INCR", key))

    async def incrby(self, key: str, amount: int) -> int:
        """Increment *key* by *amount*."""
        return int(await self._cmd("INCRBY", key, str(amount)))

    async def incrbyfloat(self, key: str, amount: float) -> float:
        """Increment *key* by *amount* (float)."""
        return float(await self._cmd("INCRBYFLOAT", key, str(amount)))

    async def decr(self, key: str) -> int:
        """Decrement *key* by 1."""
        return int(await self._cmd("DECR", key))

    async def decrby(self, key: str, amount: int) -> int:
        """Decrement *key* by *amount*."""
        return int(await self._cmd("DECRBY", key, str(amount)))

    async def mget(self, *keys: str) -> List[Optional[bytes]]:
        """Return the values of all *keys*."""
        result = await self._cmd("MGET", *keys)
        if not isinstance(result, (list, tuple)):
            return [None] * len(keys)
        out: List[Optional[bytes]] = []
        for v in result:
            if v is None:
                out.append(None)
            elif isinstance(v, bytes):
                out.append(v)
            else:
                out.append(str(v).encode())
        return out

    async def mset(self, mapping: Dict[str, str]) -> bool:
        """Set multiple keys at once."""
        args: List[str] = []
        for k, v in mapping.items():
            args += [k, v]
        result = await self._cmd("MSET", *args)
        return str(result).upper() == "OK"

    async def getset(self, key: str, value: str) -> Optional[bytes]:
        """Set *key* to *value* and return the old value."""
        result = await self._cmd("GETSET", key, value)
        if result is None:
            return None
        return str(result).encode()

    # ------------------------------------------------------------------
    # Pub/Sub
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def subscribe(self, *channels: str) -> AsyncGenerator["PubSubChannel", None]:
        """Subscribe to one or more channels.

        Usage::

            async with client.subscribe("news", "sports") as ch:
                async for msg in ch.listen():
                    print(msg)

        Yields:
            A :class:`PubSubChannel` instance for reading messages.
        """
        channel = PubSubChannel(self, channels)
        await channel._subscribe()
        self._in_pubsub = True
        try:
            yield channel
        finally:
            await channel._unsubscribe()
            if not self._pubsub_handlers:
                self._in_pubsub = False

    async def psubscribe(self, *patterns: str, handler: Any = None) -> None:
        """Subscribe to channels matching *patterns* (glob-style).

        Args:
            *patterns: Glob patterns, e.g. ``"news.*"``.
            handler: Callable ``(pattern, channel, message) -> None``.
        """
        if handler is not None:
            for pat in patterns:
                self._pattern_handlers[pat] = handler
        await self._cmd("PSUBSCRIBE", *patterns)

    async def punsubscribe(self, *patterns: str) -> None:
        """Unsubscribe from pattern subscriptions."""
        for pat in patterns:
            self._pattern_handlers.pop(pat, None)
        await self._cmd("PUNSUBSCRIBE", *patterns)

    async def publish(self, channel: str, message: str) -> int:
        """Publish *message* to *channel*.

        Returns:
            The number of subscribers that received the message.
        """
        return int(await self._cmd("PUBLISH", channel, message) or 0)

    # ------------------------------------------------------------------
    # Stream commands
    # ------------------------------------------------------------------

    async def xadd(
        self,
        stream: str,
        fields: Dict[str, str],
        id: str = "*",
        maxlen: Optional[int] = None,
        approximate: bool = True,
    ) -> str:
        """Append a new entry to *stream*.

        Args:
            stream: Stream key.
            fields: Field/value mapping for the new entry.
            id: Entry ID. Defaults to ``"*"`` (auto-generated).
            maxlen: Trim the stream to *maxlen* entries.
            approximate: Use approximate trimming (``~``).

        Returns:
            The entry ID as a string.
        """
        args: List[str] = [stream]
        if maxlen is not None:
            args += ["MAXLEN", "~" if approximate else "", str(maxlen)]
        args.append(id)
        for k, v in fields.items():
            args += [k, v]
        result = await self._cmd("XADD", *args)
        return str(result)

    async def xlen(self, stream: str) -> int:
        """Return the number of entries in *stream*."""
        return int(await self._cmd("XLEN", stream) or 0)

    async def xrange(
        self,
        stream: str,
        start: str = "-",
        end: str = "+",
        count: Optional[int] = None,
    ) -> List[_StreamEntry]:
        """Return entries in *stream* between *start* and *end*.

        Returns:
            List of ``(id, fields_dict)`` tuples.
        """
        args = [stream, start, end]
        if count is not None:
            args += ["COUNT", str(count)]
        result = await self._cmd("XRANGE", *args)
        return _parse_stream_entries(result)

    async def xrevrange(
        self,
        stream: str,
        end: str = "+",
        start: str = "-",
        count: Optional[int] = None,
    ) -> List[_StreamEntry]:
        """Return entries in *stream* in reverse order."""
        args = [stream, end, start]
        if count is not None:
            args += ["COUNT", str(count)]
        result = await self._cmd("XREVRANGE", *args)
        return _parse_stream_entries(result)

    async def xread(
        self,
        streams: Dict[str, str],
        count: Optional[int] = None,
        block: Optional[int] = None,
    ) -> Optional[_StreamResult]:
        """Read entries from one or more streams.

        Args:
            streams: Mapping of stream name to last-seen ID.  Use ``"$"`` to
                read only new entries, ``"0"`` to read from the beginning.
            count: Maximum entries per stream.
            block: Block for *block* milliseconds (0 = forever).

        Returns:
            List of ``(stream_name, entries)`` tuples, or ``None`` on timeout.
        """
        args: List[str] = []
        if count is not None:
            args += ["COUNT", str(count)]
        if block is not None:
            args += ["BLOCK", str(block)]
        args.append("STREAMS")
        stream_names = list(streams.keys())
        args += stream_names
        args += [streams[n] for n in stream_names]
        result = await self._cmd("XREAD", *args)
        return _parse_xread_result(result)

    async def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: Dict[str, str],
        count: Optional[int] = None,
        block: Optional[int] = None,
        noack: bool = False,
    ) -> Optional[_StreamResult]:
        """Read entries from a consumer group.

        Args:
            group: Consumer group name.
            consumer: Consumer name.
            streams: Mapping of stream name to ID.  Use ``">"`` to read
                new (undelivered) entries.
            count: Maximum entries per stream.
            block: Block for *block* milliseconds (0 = forever).
            noack: Do not add entries to the PEL.

        Returns:
            List of ``(stream_name, entries)`` tuples, or ``None`` on timeout.
        """
        args: List[str] = ["GROUP", group, consumer]
        if count is not None:
            args += ["COUNT", str(count)]
        if block is not None:
            args += ["BLOCK", str(block)]
        if noack:
            args.append("NOACK")
        args.append("STREAMS")
        stream_names = list(streams.keys())
        args += stream_names
        args += [streams[n] for n in stream_names]
        result = await self._cmd("XREADGROUP", *args)
        return _parse_xread_result(result)

    async def xack(self, stream: str, group: str, *ids: str) -> int:
        """Acknowledge one or more messages in a consumer group."""
        return int(await self._cmd("XACK", stream, group, *ids) or 0)

    async def xpending(
        self,
        stream: str,
        group: str,
        start: str = "-",
        end: str = "+",
        count: int = 10,
        consumer: Optional[str] = None,
    ) -> List[Any]:
        """Return pending entries in a consumer group."""
        args = [stream, group, start, end, str(count)]
        if consumer is not None:
            args.append(consumer)
        result = await self._cmd("XPENDING", *args)
        if isinstance(result, (list, tuple)):
            return list(result)
        return []

    async def xclaim(
        self,
        stream: str,
        group: str,
        consumer: str,
        min_idle_time: int,
        *ids: str,
    ) -> List[_StreamEntry]:
        """Transfer ownership of pending entries to *consumer*."""
        result = await self._cmd(
            "XCLAIM", stream, group, consumer, str(min_idle_time), *ids
        )
        return _parse_stream_entries(result)

    async def xtrim(
        self,
        stream: str,
        maxlen: int,
        approximate: bool = True,
    ) -> int:
        """Trim *stream* to at most *maxlen* entries."""
        args = [stream, "MAXLEN"]
        if approximate:
            args.append("~")
        args.append(str(maxlen))
        return int(await self._cmd("XTRIM", *args) or 0)

    async def xgroup_create(
        self,
        stream: str,
        group: str,
        id: str = "$",
        mkstream: bool = False,
    ) -> bool:
        """Create a consumer group."""
        args = [stream, group, id]
        if mkstream:
            args.append("MKSTREAM")
        result = await self._cmd("XGROUP", "CREATE", *args)
        return str(result).upper() == "OK"

    async def xgroup_destroy(self, stream: str, group: str) -> int:
        """Destroy a consumer group."""
        return int(await self._cmd("XGROUP", "DESTROY", stream, group) or 0)

    async def xgroup_setid(self, stream: str, group: str, id: str) -> bool:
        """Set the last-delivered ID for a consumer group."""
        result = await self._cmd("XGROUP", "SETID", stream, group, id)
        return str(result).upper() == "OK"

    # ------------------------------------------------------------------
    # Transaction commands
    # ------------------------------------------------------------------

    async def multi(self) -> bool:
        """Begin a transaction block."""
        result = await self._cmd("MULTI")
        return str(result).upper() == "OK"

    async def exec(self) -> Optional[List[Any]]:
        """Execute a transaction block."""
        result = await self._cmd("EXEC")
        if result is None:
            return None
        if isinstance(result, (list, tuple)):
            return list(result)
        return [result]

    async def discard(self) -> bool:
        """Discard a transaction block."""
        result = await self._cmd("DISCARD")
        return str(result).upper() == "OK"

    async def watch(self, *keys: str) -> bool:
        """Watch *keys* for modifications (optimistic locking)."""
        result = await self._cmd("WATCH", *keys)
        return str(result).upper() == "OK"

    async def unwatch(self) -> bool:
        """Forget all watched keys."""
        result = await self._cmd("UNWATCH")
        return str(result).upper() == "OK"

    @asynccontextmanager
    async def transaction(
        self,
        *watch_keys: str,
    ) -> AsyncGenerator["AsyncSwarmKeyDbClient", None]:
        """Context manager for a Redis transaction (MULTI/EXEC).

        Optionally watches *watch_keys* for optimistic locking.  Calls
        ``EXEC`` on exit; if the context block raises an exception,
        ``DISCARD`` is called instead.

        Usage::

            async with client.transaction("counter") as tx:
                await tx.incr("counter")

        Raises:
            SwarmKeyDbCommandError: If ``EXEC`` returns ``nil`` (watch conflict).
        """
        if watch_keys:
            await self.watch(*watch_keys)
        await self.multi()
        try:
            yield self
        except Exception:
            await self.discard()
            raise
        else:
            result = await self.exec()
            if result is None:
                raise SwarmKeyDbCommandError("EXEC", "Transaction aborted (WATCH conflict)")

    # ------------------------------------------------------------------
    # Auth / server commands
    # ------------------------------------------------------------------

    async def auth(self, password: str) -> bool:
        """Authenticate with *password*."""
        result = await self._cmd("AUTH", password)
        return str(result).upper() == "OK"

    async def ping(self, message: Optional[str] = None) -> str:
        """Send a PING to the server."""
        if message is not None:
            result = await self._cmd("PING", message)
        else:
            result = await self._cmd("PING")
        return str(result or "PONG")

    async def hello(self, proto_ver: int = 3) -> Dict[str, Any]:
        """Negotiate protocol version (HELLO command)."""
        result = await self._cmd("HELLO", str(proto_ver))
        if isinstance(result, dict):
            return result
        return {}

    async def raw(self, command: str, *args: str) -> Any:
        """Execute an arbitrary command.

        Args:
            command: Command name (e.g. ``"COMMAND"``).
            *args: Command arguments.

        Returns:
            The raw server response.
        """
        return await self._cmd(command, *args)

    # ------------------------------------------------------------------
    # Internal: send a command and await response
    # ------------------------------------------------------------------

    async def _cmd(self, command: str, *args: str) -> Any:
        """Send *command* with *args* and return the parsed response."""
        if self._ws is None and not self._closed:
            await self.connect()

        if self._ws is None:
            # WebSocket unavailable — try HTTP fallback for simple ops
            if self._http_fallback and command in ("GET", "SET"):
                return await self._http_fallback_cmd(command, *args)
            raise SwarmKeyDbConnectionError(
                f"Not connected. Call connect() before sending commands. "
                f"(command: {command})"
            )

        return await self._send_command(command, *args)

    async def _send_command(self, command: str, *args: str) -> Any:
        """Send a JSON-framed command over the WebSocket."""
        loop = asyncio.get_event_loop()
        fut: asyncio.Future[Any] = loop.create_future()
        req = _PendingRequest(fut, command)
        self._pending_list.append(req)

        payload = json.dumps({"cmd": command, "args": list(args)})
        try:
            await self._ws.send(payload)
        except Exception as exc:
            self._pending_list.remove(req)
            raise SwarmKeyDbConnectionError(
                f"Failed to send command '{command}': {exc}"
            ) from exc

        try:
            result = await asyncio.wait_for(fut, timeout=self._request_timeout)
        except asyncio.TimeoutError:
            with contextlib.suppress(ValueError):
                self._pending_list.remove(req)
            raise SwarmKeyDbTimeoutError(
                f"Command '{command}' timed out after {self._request_timeout}s"
            )
        return result

    # ------------------------------------------------------------------
    # Internal: HTTP fallback
    # ------------------------------------------------------------------

    async def _http_fallback_cmd(self, command: str, *args: str) -> Any:
        """Use the HTTP REST gateway for GET/SET when WebSocket is down."""
        try:
            import httpx  # type: ignore[import]
        except ImportError as exc:
            raise SwarmKeyDbConnectionError(
                "httpx is required for HTTP fallback. Install it with: pip install httpx"
            ) from exc

        headers: Dict[str, str] = {}
        if self._password:
            headers["Authorization"] = f"Bearer {self._password}"

        async with httpx.AsyncClient() as http:
            if command == "GET":
                key = args[0]
                resp = await http.get(
                    f"{self._http_url}/get/{key}", headers=headers
                )
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                data = resp.json()
                return data.get("value")
            elif command == "SET":
                key, value = args[0], args[1]
                body: Dict[str, Any] = {"value": value}
                resp = await http.post(
                    f"{self._http_url}/set/{key}", json=body, headers=headers
                )
                resp.raise_for_status()
                return "OK"
        return None

    # ------------------------------------------------------------------
    # Internal: WebSocket management
    # ------------------------------------------------------------------

    async def _open_ws(self) -> None:
        """Open the WebSocket connection and start the reader loop."""
        try:
            import websockets  # type: ignore[import]
        except ImportError as exc:
            raise SwarmKeyDbConnectionError(
                "websockets is required. Install it with: pip install websockets"
            ) from exc

        try:
            self._ws = await websockets.connect(self._ws_url)
        except Exception as exc:
            raise SwarmKeyDbConnectionError(
                f"Cannot connect to SwarmKeyDb at {self._ws_url!r}: {exc}\n"
                "Ensure the server is running and SWARM_KEYDB_WS_PORT is set."
            ) from exc

        self._reconnect_attempts = 0
        self._reader_task = asyncio.create_task(self._reader_loop())

        if self._password:
            try:
                await self._send_command("AUTH", self._password)
            except SwarmKeyDbCommandError as exc:
                await self.close()
                raise SwarmKeyDbAuthError(
                    f"Authentication failed: {exc.server_error}"
                ) from exc

    async def _reader_loop(self) -> None:
        """Background task that reads incoming WebSocket frames."""
        try:
            async for raw in self._ws:
                try:
                    self._handle_frame(raw)
                except Exception:
                    logger.exception("Error handling WebSocket frame")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("WebSocket reader ended: %s", exc)
        finally:
            # Fail all pending requests
            self._flush_pending(
                SwarmKeyDbConnectionError("WebSocket connection closed unexpectedly")
            )
            self._ws = None
            if self._reconnect and not self._closed:
                asyncio.create_task(self._schedule_reconnect())

    def _handle_frame(self, raw: str) -> None:
        """Parse a JSON response frame and resolve the matching pending request."""
        try:
            frame = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Received non-JSON WebSocket frame: %r", raw)
            return

        # Push message (Pub/Sub or client tracking)
        if "push" in frame:
            self._dispatch_push(frame["push"])
            return

        if not self._pending_list:
            return

        req = self._pending_list.pop(0)

        if "error" in frame:
            error = str(frame["error"])
            if not req.future.done():
                req.future.set_exception(
                    SwarmKeyDbCommandError(req.command, error)
                )
            return

        data = frame.get("data")
        if not req.future.done():
            req.future.set_result(data)

    def _dispatch_push(self, push: Any) -> None:
        """Dispatch a push frame to Pub/Sub handlers."""
        if not isinstance(push, (list, tuple)) or len(push) < 1:
            return

        kind = str(push[0]).lower()
        if kind == "message" and len(push) >= 3:
            channel = str(push[1])
            message = str(push[2])
            handler = self._pubsub_handlers.get(channel)
            if handler is not None:
                handler({"type": "message", "channel": channel, "data": message})
        elif kind == "pmessage" and len(push) >= 4:
            pattern = str(push[1])
            channel = str(push[2])
            message = str(push[3])
            handler = self._pattern_handlers.get(pattern)
            if handler is not None:
                handler(
                    {
                        "type": "pmessage",
                        "pattern": pattern,
                        "channel": channel,
                        "data": message,
                    }
                )

    def _flush_pending(self, error: Exception) -> None:
        """Fail all pending futures with *error*."""
        for req in list(self._pending_list):
            if not req.future.done():
                req.future.set_exception(error)
        self._pending_list.clear()

    async def _schedule_reconnect(self) -> None:
        """Attempt to re-establish the WebSocket connection with backoff."""
        delay = min(
            self._reconnect_base_delay * (2 ** self._reconnect_attempts),
            self._reconnect_max_delay,
        )
        self._reconnect_attempts += 1
        logger.debug(
            "Reconnecting in %.1fs (attempt %d)", delay, self._reconnect_attempts
        )
        await asyncio.sleep(delay)
        if self._closed:
            return
        async with self._connect_lock:
            if self._ws is not None:
                return
            try:
                await self._open_ws()
                logger.info("Reconnected to SwarmKeyDb at %s", self._ws_url)
            except SwarmKeyDbConnectionError:
                logger.warning(
                    "Reconnect attempt %d failed; will retry", self._reconnect_attempts
                )
                asyncio.create_task(self._schedule_reconnect())


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _to_str_list(value: Any) -> List[str]:
    """Convert a raw server response to a list of strings."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(v) for v in value if v is not None]
    return [str(value)]


def _parse_stream_entries(raw: Any) -> List[_StreamEntry]:
    """Parse the standard RESP stream entry array ``[[id, [f, v, ...]], ...]``."""
    if not isinstance(raw, (list, tuple)):
        return []
    result: List[_StreamEntry] = []
    for item in raw:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        entry_id = str(item[0])
        fields_raw = item[1]
        fields: Dict[str, str] = {}
        if isinstance(fields_raw, (list, tuple)):
            it = iter(fields_raw)
            for field in it:
                value = next(it, None)
                fields[str(field)] = str(value) if value is not None else ""
        elif isinstance(fields_raw, dict):
            fields = {str(k): str(v) for k, v in fields_raw.items()}
        result.append((entry_id, fields))
    return result


def _parse_xread_result(raw: Any) -> Optional[_StreamResult]:
    """Parse the XREAD / XREADGROUP nested response."""
    if raw is None:
        return None
    if not isinstance(raw, (list, tuple)):
        return []
    result: _StreamResult = []
    for stream_data in raw:
        if not isinstance(stream_data, (list, tuple)) or len(stream_data) < 2:
            continue
        stream_name = str(stream_data[0])
        entries = _parse_stream_entries(stream_data[1])
        result.append((stream_name, entries))
    return result
