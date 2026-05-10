"""swarm-keydb-client — Python SDK for SwarmKeyDb.

WebSocket-first, async/sync Redis-compatible client for SwarmKeyDb.

Quickstart (async)::

    import asyncio
    from swarm_keydb_client import AsyncSwarmKeyDbClient

    async def main():
        client = AsyncSwarmKeyDbClient("ws://localhost:8765/")
        await client.connect()
        await client.set("hello", "world")
        value = await client.get("hello")
        print(value)  # b"world"
        await client.close()

    asyncio.run(main())

Quickstart (sync)::

    from swarm_keydb_client import SwarmKeyDbClient

    client = SwarmKeyDbClient("ws://localhost:8765/")
    client.set("hello", "world")
    print(client.get("hello"))  # b"world"
    client.close()
"""

from .client import AsyncSwarmKeyDbClient, PubSubChannel
from .exceptions import (
    SwarmKeyDbAuthError,
    SwarmKeyDbCommandError,
    SwarmKeyDbConnectionError,
    SwarmKeyDbError,
    SwarmKeyDbTimeoutError,
)
from .sync_client import SwarmKeyDbClient

__all__ = [
    "AsyncSwarmKeyDbClient",
    "SwarmKeyDbClient",
    "PubSubChannel",
    "SwarmKeyDbError",
    "SwarmKeyDbConnectionError",
    "SwarmKeyDbCommandError",
    "SwarmKeyDbTimeoutError",
    "SwarmKeyDbAuthError",
]

__version__ = "0.1.0"
