"""Unit tests for AsyncSwarmKeyDbClient using a mock WebSocket."""

from __future__ import annotations

import asyncio
import json
import pytest

from swarm_keydb_client import (
    AsyncSwarmKeyDbClient,
    SwarmKeyDbCommandError,
    SwarmKeyDbConnectionError,
    SwarmKeyDbTimeoutError,
)
from swarm_keydb_client.client import _parse_stream_entries, _parse_xread_result


# ---------------------------------------------------------------------------
# Mock WebSocket
# ---------------------------------------------------------------------------


class MockWebSocket:
    """Minimal WebSocket stub that delivers one reply per send() call.

    Each call to ``send()`` unblocks the next reply in the reply queue so that
    the reader loop always has exactly one frame available per command sent.
    This prevents the reader from racing ahead and consuming replies for
    commands that haven't been sent yet.
    """

    def __init__(self) -> None:
        self.sent: list[str] = []
        # _replies holds pre-set responses; each send() releases one slot
        self._replies: list[str] = []
        self._available: asyncio.Queue[str] = asyncio.Queue()
        self._closed = False

    def queue_reply(self, data: dict) -> None:
        """Pre-queue a reply; it will be released by the matching send()."""
        self._replies.append(json.dumps(data))

    async def send(self, data: str) -> None:
        """Record the sent frame and release the next queued reply."""
        self.sent.append(data)
        if self._replies:
            self._available.put_nowait(self._replies.pop(0))

    async def close(self) -> None:
        self._closed = True

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        if self._closed:
            raise StopAsyncIteration
        try:
            return await asyncio.wait_for(self._available.get(), timeout=5)
        except asyncio.TimeoutError:
            raise StopAsyncIteration


def _make_client(ws: MockWebSocket) -> AsyncSwarmKeyDbClient:
    """Build a client already wired to *ws* (skip actual connection)."""
    client = AsyncSwarmKeyDbClient(request_timeout=2.0, http_fallback=False)
    client._ws = ws
    client._reader_task = asyncio.get_event_loop().create_task(client._reader_loop())
    return client


# ---------------------------------------------------------------------------
# Core command tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_returns_bytes_value():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "GET", "data": "hello"})
    result = await client.get("mykey")
    assert result == b"hello"
    assert json.loads(ws.sent[0]) == {"cmd": "GET", "args": ["mykey"]}
    await client.close()


@pytest.mark.asyncio
async def test_get_returns_none_for_null():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "GET", "data": None})
    result = await client.get("missing")
    assert result is None
    await client.close()


@pytest.mark.asyncio
async def test_set_returns_true_on_ok():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "SET", "data": "OK"})
    result = await client.set("k", "v")
    assert result is True
    frame = json.loads(ws.sent[0])
    assert frame["cmd"] == "SET"
    assert frame["args"][:2] == ["k", "v"]
    await client.close()


@pytest.mark.asyncio
async def test_set_with_ex_appends_ex_args():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "SET", "data": "OK"})
    await client.set("k", "v", ex=60)
    frame = json.loads(ws.sent[0])
    assert "EX" in frame["args"]
    assert "60" in frame["args"]
    await client.close()


@pytest.mark.asyncio
async def test_set_with_nx():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "SET", "data": None})
    result = await client.set("k", "v", nx=True)
    assert result is False
    frame = json.loads(ws.sent[0])
    assert "NX" in frame["args"]
    await client.close()


@pytest.mark.asyncio
async def test_delete_returns_count():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "DEL", "data": 2})
    result = await client.delete("a", "b")
    assert result == 2
    await client.close()


@pytest.mark.asyncio
async def test_exists_returns_count():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "EXISTS", "data": 1})
    result = await client.exists("k")
    assert result == 1
    await client.close()


@pytest.mark.asyncio
async def test_expire_returns_bool():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "EXPIRE", "data": 1})
    result = await client.expire("k", 30)
    assert result is True
    await client.close()


@pytest.mark.asyncio
async def test_ttl_returns_int():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "TTL", "data": 29})
    result = await client.ttl("k")
    assert result == 29
    await client.close()


@pytest.mark.asyncio
async def test_keys_returns_list():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "KEYS", "data": ["a", "b", "c"]})
    result = await client.keys("*")
    assert result == ["a", "b", "c"]
    await client.close()


@pytest.mark.asyncio
async def test_scan_parses_cursor_and_keys():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "SCAN", "data": ["42", ["k1", "k2"]]})
    cursor, ks = await client.scan(0, match="k*", count=10)
    assert cursor == 42
    assert ks == ["k1", "k2"]
    frame = json.loads(ws.sent[0])
    assert "MATCH" in frame["args"]
    assert "COUNT" in frame["args"]
    await client.close()


# ---------------------------------------------------------------------------
# String command tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_incr():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "INCR", "data": 1})
    result = await client.incr("counter")
    assert result == 1
    await client.close()


@pytest.mark.asyncio
async def test_incrby():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "INCRBY", "data": 10})
    result = await client.incrby("counter", 10)
    assert result == 10
    frame = json.loads(ws.sent[0])
    assert frame["args"] == ["counter", "10"]
    await client.close()


@pytest.mark.asyncio
async def test_decr():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "DECR", "data": 0})
    result = await client.decr("counter")
    assert result == 0
    await client.close()


@pytest.mark.asyncio
async def test_decrby():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "DECRBY", "data": -5})
    result = await client.decrby("counter", 5)
    assert result == -5
    await client.close()


@pytest.mark.asyncio
async def test_strlen():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "STRLEN", "data": 5})
    result = await client.strlen("k")
    assert result == 5
    await client.close()


@pytest.mark.asyncio
async def test_append():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "APPEND", "data": 8})
    result = await client.append("k", "world")
    assert result == 8
    await client.close()


@pytest.mark.asyncio
async def test_mget_returns_optional_bytes_list():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "MGET", "data": ["v1", None, "v3"]})
    result = await client.mget("k1", "k2", "k3")
    assert result == [b"v1", None, b"v3"]
    await client.close()


@pytest.mark.asyncio
async def test_mset():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "MSET", "data": "OK"})
    result = await client.mset({"k1": "v1", "k2": "v2"})
    assert result is True
    frame = json.loads(ws.sent[0])
    assert frame["cmd"] == "MSET"
    assert "k1" in frame["args"]
    assert "v1" in frame["args"]
    await client.close()


# ---------------------------------------------------------------------------
# Error handling tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_server_error_raises_command_error():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "GET", "error": "WRONGTYPE Operation against a key holding the wrong kind of value"})
    with pytest.raises(SwarmKeyDbCommandError) as exc_info:
        await client.get("k")
    assert exc_info.value.command == "GET"
    assert "WRONGTYPE" in exc_info.value.server_error
    await client.close()


@pytest.mark.asyncio
async def test_not_connected_raises_connection_error():
    client = AsyncSwarmKeyDbClient(http_fallback=False)
    with pytest.raises(SwarmKeyDbConnectionError):
        await client.get("k")


@pytest.mark.asyncio
async def test_timeout_raises_timeout_error():
    ws = MockWebSocket()  # never queues a reply
    client = AsyncSwarmKeyDbClient(request_timeout=0.05, http_fallback=False)
    client._ws = ws
    client._reader_task = asyncio.get_event_loop().create_task(client._reader_loop())

    with pytest.raises(SwarmKeyDbTimeoutError):
        await client.get("k")
    await client.close()


# ---------------------------------------------------------------------------
# Pub/Sub tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_publish_sends_correct_frame():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "PUBLISH", "data": 1})
    result = await client.publish("news", "hello")
    assert result == 1
    frame = json.loads(ws.sent[0])
    assert frame == {"cmd": "PUBLISH", "args": ["news", "hello"]}
    await client.close()


@pytest.mark.asyncio
async def test_subscribe_sends_subscribe_frame():
    ws = MockWebSocket()
    client = _make_client(ws)
    # Queue the SUBSCRIBE ack
    ws.queue_reply({"cmd": "SUBSCRIBE", "data": ["subscribe", "news", 1]})
    # Queue the UNSUBSCRIBE ack (for cleanup in finally)
    ws.queue_reply({"cmd": "UNSUBSCRIBE", "data": ["unsubscribe", "news", 0]})

    async with client.subscribe("news") as ch:
        assert "news" in client._pubsub_handlers
        # Simulate a push message
        client._dispatch_push(["message", "news", "breaking!"])
        msg = await asyncio.wait_for(ch._queue.get(), timeout=1)
        assert msg == {"type": "message", "channel": "news", "data": "breaking!"}

    assert "news" not in client._pubsub_handlers
    await client.close()


@pytest.mark.asyncio
async def test_push_dispatch_pmessage():
    ws = MockWebSocket()
    client = _make_client(ws)
    received = []

    def handler(msg):
        received.append(msg)

    client._pattern_handlers["news.*"] = handler
    client._dispatch_push(["pmessage", "news.*", "news.sports", "goal!"])

    assert received == [
        {"type": "pmessage", "pattern": "news.*", "channel": "news.sports", "data": "goal!"}
    ]
    await client.close()


# ---------------------------------------------------------------------------
# Stream helper function tests
# ---------------------------------------------------------------------------


def test_parse_stream_entries_from_list():
    raw = [
        ["1-0", ["field1", "val1", "field2", "val2"]],
        ["2-0", ["f", "v"]],
    ]
    entries = _parse_stream_entries(raw)
    assert entries == [
        ("1-0", {"field1": "val1", "field2": "val2"}),
        ("2-0", {"f": "v"}),
    ]


def test_parse_stream_entries_from_dict_fields():
    raw = [["3-0", {"a": "1", "b": "2"}]]
    entries = _parse_stream_entries(raw)
    assert entries == [("3-0", {"a": "1", "b": "2"})]


def test_parse_stream_entries_empty():
    assert _parse_stream_entries(None) == []
    assert _parse_stream_entries([]) == []


def test_parse_xread_result():
    raw = [
        ["mystream", [["1-0", ["f", "v"]]]],
    ]
    result = _parse_xread_result(raw)
    assert result == [("mystream", [("1-0", {"f": "v"})])]


def test_parse_xread_result_none():
    assert _parse_xread_result(None) is None


# ---------------------------------------------------------------------------
# Stream command tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_xadd_sends_correct_frame():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "XADD", "data": "1234567890-0"})
    entry_id = await client.xadd("events", {"sensor": "temp", "value": "22"})
    assert entry_id == "1234567890-0"
    frame = json.loads(ws.sent[0])
    assert frame["cmd"] == "XADD"
    assert "events" in frame["args"]
    assert "sensor" in frame["args"]
    await client.close()


@pytest.mark.asyncio
async def test_xadd_with_maxlen():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "XADD", "data": "1-0"})
    await client.xadd("s", {"k": "v"}, maxlen=100)
    frame = json.loads(ws.sent[0])
    assert "MAXLEN" in frame["args"]
    assert "100" in frame["args"]
    await client.close()


@pytest.mark.asyncio
async def test_xlen():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "XLEN", "data": 5})
    result = await client.xlen("events")
    assert result == 5
    await client.close()


@pytest.mark.asyncio
async def test_xrange_parses_entries():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "XRANGE", "data": [["1-0", ["f", "v"]]]})
    entries = await client.xrange("events", "-", "+")
    assert entries == [("1-0", {"f": "v"})]
    await client.close()


@pytest.mark.asyncio
async def test_xrevrange_parses_entries():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "XREVRANGE", "data": [["2-0", ["x", "y"]], ["1-0", ["x", "z"]]]})
    entries = await client.xrevrange("events")
    assert entries[0][0] == "2-0"
    assert entries[1][0] == "1-0"
    await client.close()


@pytest.mark.asyncio
async def test_xread_sends_streams_args():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "XREAD", "data": None})
    result = await client.xread({"events": "0"})
    assert result is None
    frame = json.loads(ws.sent[0])
    assert "STREAMS" in frame["args"]
    assert "events" in frame["args"]
    await client.close()


@pytest.mark.asyncio
async def test_xack():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "XACK", "data": 1})
    result = await client.xack("events", "mygroup", "1-0")
    assert result == 1
    await client.close()


@pytest.mark.asyncio
async def test_xtrim():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "XTRIM", "data": 5})
    result = await client.xtrim("events", 100)
    assert result == 5
    frame = json.loads(ws.sent[0])
    assert "MAXLEN" in frame["args"]
    assert "~" in frame["args"]
    await client.close()


# ---------------------------------------------------------------------------
# Transaction tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multi_exec():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "MULTI", "data": "OK"})
    ws.queue_reply({"cmd": "SET", "data": "QUEUED"})
    ws.queue_reply({"cmd": "EXEC", "data": ["OK"]})

    await client.multi()
    await client.set("k", "v")
    result = await client.exec()
    assert result == ["OK"]
    await client.close()


@pytest.mark.asyncio
async def test_discard():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "MULTI", "data": "OK"})
    ws.queue_reply({"cmd": "DISCARD", "data": "OK"})
    await client.multi()
    result = await client.discard()
    assert result is True
    await client.close()


@pytest.mark.asyncio
async def test_watch_and_exec_conflict():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "WATCH", "data": "OK"})
    ws.queue_reply({"cmd": "MULTI", "data": "OK"})
    ws.queue_reply({"cmd": "EXEC", "data": None})  # nil = watch conflict

    await client.watch("mykey")
    await client.multi()
    result = await client.exec()
    assert result is None
    await client.close()


@pytest.mark.asyncio
async def test_transaction_context_manager_success():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "MULTI", "data": "OK"})
    ws.queue_reply({"cmd": "SET", "data": "QUEUED"})
    ws.queue_reply({"cmd": "EXEC", "data": ["OK"]})

    async with client.transaction() as tx:
        # Inside MULTI, commands return "QUEUED" — use raw() to avoid type coercion
        await tx.raw("SET", "counter", "1")
    await client.close()


@pytest.mark.asyncio
async def test_transaction_context_manager_discard_on_exception():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "MULTI", "data": "OK"})
    ws.queue_reply({"cmd": "DISCARD", "data": "OK"})

    with pytest.raises(ValueError):
        async with client.transaction() as tx:
            raise ValueError("abort!")
    await client.close()


# ---------------------------------------------------------------------------
# Auth / server command tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ping_default():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "PING", "data": "PONG"})
    result = await client.ping()
    assert result == "PONG"
    await client.close()


@pytest.mark.asyncio
async def test_ping_with_message():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "PING", "data": "hello"})
    result = await client.ping("hello")
    assert result == "hello"
    frame = json.loads(ws.sent[0])
    assert frame["args"] == ["hello"]
    await client.close()


@pytest.mark.asyncio
async def test_raw_command():
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "DBSIZE", "data": 42})
    result = await client.raw("DBSIZE")
    assert result == 42
    await client.close()


# ---------------------------------------------------------------------------
# JSON frame serialisation tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_frame_serialization_shape():
    """Ensure every command sends a valid JSON object with cmd and args fields."""
    ws = MockWebSocket()
    client = _make_client(ws)
    ws.queue_reply({"cmd": "SET", "data": "OK"})
    await client.set("mykey", "myvalue")
    raw_frame = ws.sent[0]
    frame = json.loads(raw_frame)
    assert isinstance(frame, dict)
    assert "cmd" in frame
    assert "args" in frame
    assert isinstance(frame["args"], list)
    await client.close()
