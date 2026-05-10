from __future__ import annotations

import os
import uuid

import pytest
import pytest_asyncio

from swarm_keydb_client import AsyncSwarmKeyDbClient

WS_URL = os.environ.get("SWARM_KEYDB_WS_URL", "ws://127.0.0.1:8765/")
HTTP_URL = os.environ.get("SWARM_KEYDB_HTTP_URL", "http://127.0.0.1:8080")
PASSWORD = os.environ.get("SWARM_KEYDB_PASSWORD", None)

pytestmark = pytest.mark.integration


def unique_key(prefix: str = "sdk-smoke") -> str:
    return f"{prefix}:{uuid.uuid4().hex}"


@pytest_asyncio.fixture
async def client():
    c = AsyncSwarmKeyDbClient(WS_URL, HTTP_URL, password=PASSWORD, http_fallback=True)
    await c.connect()
    yield c
    await c.close()


async def test_set_get_roundtrip(client: AsyncSwarmKeyDbClient):
    key = unique_key("kv")
    assert await client.set(key, "hello") is True
    assert await client.get(key) == b"hello"


async def test_ping_and_hello(client: AsyncSwarmKeyDbClient):
    assert "PONG" in (await client.ping()).upper()
    hello = await client.hello(3)
    assert isinstance(hello, dict)
    assert str(hello.get("proto", "3")) == "3"


async def test_pubsub_channel_flow(client: AsyncSwarmKeyDbClient):
    sub = AsyncSwarmKeyDbClient(WS_URL, HTTP_URL, password=PASSWORD)
    await sub.connect()

    channel_name = unique_key("ch")
    async with sub.subscribe(channel_name) as channel:
        await client.publish(channel_name, "msg")
        async for message in channel.listen():
            assert message["data"] == "msg"
            break

    await sub.close()


async def test_xadd_xlen(client: AsyncSwarmKeyDbClient):
    stream = unique_key("stream")
    entry_id = await client.xadd(stream, {"f": "v"})
    assert "-" in entry_id
    assert await client.xlen(stream) >= 1
