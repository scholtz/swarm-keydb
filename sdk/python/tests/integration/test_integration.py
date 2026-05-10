"""Integration tests for swarm-keydb-client against a live SwarmKeyDb instance.

These tests require a running SwarmKeyDb server with WebSocket port enabled.
By default they expect ws://127.0.0.1:8765/ and http://127.0.0.1:8080.

Set the environment variables to override:
    SWARM_KEYDB_WS_URL   — WebSocket URL
    SWARM_KEYDB_HTTP_URL — HTTP base URL
    SWARM_KEYDB_PASSWORD — AUTH password (optional)

Run only unit tests without a live server:
    pytest tests/unit/ -v

Run the full integration suite:
    pytest tests/integration/ -v -m integration
"""

from __future__ import annotations

import os
import uuid

import pytest
import pytest_asyncio

from swarm_keydb_client import AsyncSwarmKeyDbClient, SwarmKeyDbCommandError

WS_URL = os.environ.get("SWARM_KEYDB_WS_URL", "ws://127.0.0.1:8765/")
HTTP_URL = os.environ.get("SWARM_KEYDB_HTTP_URL", "http://127.0.0.1:8080")
PASSWORD = os.environ.get("SWARM_KEYDB_PASSWORD", None)

pytestmark = pytest.mark.integration


def unique_key(prefix: str = "test") -> str:
    return f"{prefix}:{uuid.uuid4().hex}"


@pytest_asyncio.fixture
async def client():
    c = AsyncSwarmKeyDbClient(WS_URL, HTTP_URL, password=PASSWORD, http_fallback=True)
    await c.connect()
    yield c
    await c.close()


# ---------------------------------------------------------------------------
# Core commands
# ---------------------------------------------------------------------------


async def test_set_get_delete(client: AsyncSwarmKeyDbClient):
    key = unique_key()
    assert await client.set(key, "hello") is True
    assert await client.get(key) == b"hello"
    assert await client.delete(key) == 1
    assert await client.get(key) is None


async def test_set_nx(client: AsyncSwarmKeyDbClient):
    key = unique_key()
    assert await client.set(key, "first", nx=True) is True
    assert await client.set(key, "second", nx=True) is False
    await client.delete(key)


async def test_exists(client: AsyncSwarmKeyDbClient):
    key = unique_key()
    assert await client.exists(key) == 0
    await client.set(key, "v")
    assert await client.exists(key) == 1
    await client.delete(key)


async def test_expire_and_ttl(client: AsyncSwarmKeyDbClient):
    key = unique_key()
    await client.set(key, "v")
    await client.expire(key, 60)
    ttl = await client.ttl(key)
    assert ttl > 0
    await client.delete(key)


async def test_keys_and_scan(client: AsyncSwarmKeyDbClient):
    prefix = f"scan:{uuid.uuid4().hex}"
    k1 = f"{prefix}:a"
    k2 = f"{prefix}:b"
    await client.set(k1, "1")
    await client.set(k2, "2")
    ks = await client.keys(f"{prefix}:*")
    assert set(ks) >= {k1, k2}
    await client.delete(k1, k2)


# ---------------------------------------------------------------------------
# String commands
# ---------------------------------------------------------------------------


async def test_incr_decr(client: AsyncSwarmKeyDbClient):
    key = unique_key()
    await client.set(key, "10")
    assert await client.incr(key) == 11
    assert await client.incrby(key, 4) == 15
    assert await client.decr(key) == 14
    assert await client.decrby(key, 4) == 10
    await client.delete(key)


async def test_append_and_strlen(client: AsyncSwarmKeyDbClient):
    key = unique_key()
    await client.set(key, "hello")
    length = await client.append(key, " world")
    assert length == 11
    assert await client.strlen(key) == 11
    await client.delete(key)


async def test_mget_mset(client: AsyncSwarmKeyDbClient):
    k1, k2 = unique_key(), unique_key()
    await client.mset({k1: "v1", k2: "v2"})
    values = await client.mget(k1, k2, unique_key())
    assert values[0] == b"v1"
    assert values[1] == b"v2"
    assert values[2] is None
    await client.delete(k1, k2)


# ---------------------------------------------------------------------------
# Pub/Sub
# ---------------------------------------------------------------------------


async def test_publish_subscribe(client: AsyncSwarmKeyDbClient):
    sub_client = AsyncSwarmKeyDbClient(WS_URL, HTTP_URL, password=PASSWORD)
    await sub_client.connect()

    received = []

    async with sub_client.subscribe("it-test-ch") as ch:
        # Publish from a different connection
        count = await client.publish("it-test-ch", "hello!")
        # count >= 1 (our subscriber)

        async for msg in ch.listen():
            received.append(msg)
            break

    await sub_client.close()
    assert len(received) == 1
    assert received[0]["data"] == "hello!"


# ---------------------------------------------------------------------------
# Streams
# ---------------------------------------------------------------------------


async def test_xadd_xrange_xlen(client: AsyncSwarmKeyDbClient):
    stream = unique_key("stream")
    entry_id = await client.xadd(stream, {"sensor": "temp", "value": "22.5"})
    assert "-" in entry_id
    length = await client.xlen(stream)
    assert length >= 1
    entries = await client.xrange(stream, "-", "+")
    assert len(entries) >= 1
    eid, fields = entries[-1]
    assert fields.get("sensor") == "temp"
    await client.delete(stream)


async def test_xrevrange(client: AsyncSwarmKeyDbClient):
    stream = unique_key("stream")
    await client.xadd(stream, {"n": "1"})
    await client.xadd(stream, {"n": "2"})
    entries = await client.xrevrange(stream)
    assert entries[0][1]["n"] == "2"
    await client.delete(stream)


async def test_xread(client: AsyncSwarmKeyDbClient):
    stream = unique_key("stream")
    await client.xadd(stream, {"x": "1"})
    result = await client.xread({stream: "0"})
    assert result is not None
    assert len(result) >= 1
    await client.delete(stream)


async def test_xgroup_create_and_xreadgroup(client: AsyncSwarmKeyDbClient):
    stream = unique_key("stream")
    group = "mygroup"
    await client.xadd(stream, {"k": "v"})
    await client.xgroup_create(stream, group, "0")
    result = await client.xreadgroup(group, "consumer1", {stream: ">"})
    assert result is not None
    stream_name, entries = result[0]
    assert len(entries) >= 1
    eid, _ = entries[0]
    acked = await client.xack(stream, group, eid)
    assert acked == 1
    await client.xgroup_destroy(stream, group)
    await client.delete(stream)


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------


async def test_multi_exec_transaction(client: AsyncSwarmKeyDbClient):
    key = unique_key()
    await client.set(key, "0")
    async with client.transaction() as tx:
        await tx.incr(key)
    value = await client.get(key)
    assert value == b"1"
    await client.delete(key)


async def test_watch_conflict_aborts_transaction(client: AsyncSwarmKeyDbClient):
    key = unique_key()
    await client.set(key, "0")

    # Second client modifies the key while first is watching
    other = AsyncSwarmKeyDbClient(WS_URL, HTTP_URL, password=PASSWORD)
    await other.connect()

    await client.watch(key)
    # Interfere from another connection
    await other.set(key, "modified")
    await client.multi()
    result = await client.exec()
    assert result is None  # transaction aborted due to watch conflict

    await other.close()
    await client.delete(key)


# ---------------------------------------------------------------------------
# Ping / raw
# ---------------------------------------------------------------------------


async def test_ping(client: AsyncSwarmKeyDbClient):
    result = await client.ping()
    assert "PONG" in result.upper()


async def test_ping_with_message(client: AsyncSwarmKeyDbClient):
    result = await client.ping("hello")
    assert "hello" in result


async def test_wrong_type_error(client: AsyncSwarmKeyDbClient):
    key = unique_key()
    await client.raw("LPUSH", key, "item")
    with pytest.raises(SwarmKeyDbCommandError) as exc_info:
        await client.get(key)
    assert "WRONGTYPE" in exc_info.value.server_error or "wrong" in exc_info.value.server_error.lower()
    await client.delete(key)
