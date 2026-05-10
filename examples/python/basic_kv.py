"""Basic key-value operations with AsyncSwarmKeyDbClient and SwarmKeyDbClient.

Run:
    python examples/python/basic_kv.py
"""

import asyncio
import os

from swarm_keydb_client import AsyncSwarmKeyDbClient, SwarmKeyDbClient

WS_URL = os.environ.get("SWARM_KEYDB_WS_URL", "ws://127.0.0.1:8765/")


# ---------------------------------------------------------------------------
# Async usage (primary API)
# ---------------------------------------------------------------------------


async def async_demo() -> None:
    client = AsyncSwarmKeyDbClient(WS_URL)
    await client.connect()

    # Basic set / get
    await client.set("greeting", "hello, world")
    value = await client.get("greeting")
    print(f"GET greeting => {value}")  # b"hello, world"

    # TTL
    await client.set("temp", "ephemeral", ex=10)
    ttl = await client.ttl("temp")
    print(f"TTL temp => {ttl}s")

    # Increment / decrement
    await client.set("counter", "0")
    await client.incr("counter")
    await client.incrby("counter", 9)
    v = await client.get("counter")
    print(f"GET counter => {v}")  # b"10"

    # Batch ops
    await client.mset({"k1": "v1", "k2": "v2", "k3": "v3"})
    values = await client.mget("k1", "k2", "k3", "missing")
    print(f"MGET => {values}")  # [b"v1", b"v2", b"v3", None]

    # Scan keys
    cursor, ks = await client.scan(0, match="k*", count=10)
    print(f"SCAN k* => cursor={cursor}, keys={ks}")

    # Clean up
    await client.delete("greeting", "temp", "counter", "k1", "k2", "k3")

    await client.close()
    print("Async demo done.")


# ---------------------------------------------------------------------------
# Sync usage (convenience wrapper)
# ---------------------------------------------------------------------------


def sync_demo() -> None:
    client = SwarmKeyDbClient(WS_URL)

    client.set("hello", "world")
    value = client.get("hello")
    print(f"Sync GET hello => {value}")  # b"world"

    client.delete("hello")
    client.close()
    print("Sync demo done.")


if __name__ == "__main__":
    print("=== Async demo ===")
    asyncio.run(async_demo())
    print()
    print("=== Sync demo ===")
    sync_demo()
