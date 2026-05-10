# swarm-keydb-client

Python SDK for [SwarmKeyDb](https://github.com/scholtz/swarm-keydb) — a
WebSocket-first, async/sync Redis-compatible client with HTTP fallback, Pub/Sub,
Streams, and Transaction support.

## Installation

```bash
pip install swarm-keydb-client
```

Optional fast RESP parser:

```bash
pip install swarm-keydb-client[hiredis]
```

## 5-line quick start

```python
import asyncio
from swarm_keydb_client import AsyncSwarmKeyDbClient

async def main():
    client = AsyncSwarmKeyDbClient("ws://localhost:8765/")
    await client.connect()
    await client.set("hello", "world")
    print(await client.get("hello"))  # b"world"
    await client.close()

asyncio.run(main())
```

## Sync interface

```python
from swarm_keydb_client import SwarmKeyDbClient

client = SwarmKeyDbClient("ws://localhost:8765/")
client.set("hello", "world")
print(client.get("hello"))  # b"world"
client.close()
```

## Pub/Sub

```python
import asyncio
from swarm_keydb_client import AsyncSwarmKeyDbClient

async def main():
    pub = AsyncSwarmKeyDbClient("ws://localhost:8765/")
    sub = AsyncSwarmKeyDbClient("ws://localhost:8765/")
    await pub.connect()
    await sub.connect()

    async with sub.subscribe("news") as ch:
        await pub.publish("news", "hello!")
        async for msg in ch.listen():
            print(msg)  # {"type": "message", "channel": "news", "data": "hello!"}
            break

    await pub.close()
    await sub.close()

asyncio.run(main())
```

## Streams

```python
import asyncio
from swarm_keydb_client import AsyncSwarmKeyDbClient

async def main():
    client = AsyncSwarmKeyDbClient("ws://localhost:8765/")
    await client.connect()

    entry_id = await client.xadd("events", {"sensor": "temp", "value": "22.5"})
    print(entry_id)

    entries = await client.xrange("events", "-", "+")
    for eid, fields in entries:
        print(eid, fields)

    await client.close()

asyncio.run(main())
```

## Transactions

```python
import asyncio
from swarm_keydb_client import AsyncSwarmKeyDbClient

async def main():
    client = AsyncSwarmKeyDbClient("ws://localhost:8765/")
    await client.connect()

    async with client.transaction("counter") as tx:
        await tx.incr("counter")

    await client.close()

asyncio.run(main())
```

## Configuration

| Parameter | Default | Description |
|---|---|---|
| `ws_url` | `ws://127.0.0.1:8765/` | WebSocket endpoint |
| `http_url` | `http://127.0.0.1:8080` | HTTP fallback base URL |
| `password` | `None` | AUTH password (sent automatically after connect) |
| `reconnect` | `True` | Auto-reconnect on disconnect |
| `reconnect_base_delay` | `0.25` | Initial reconnect backoff (seconds) |
| `reconnect_max_delay` | `5.0` | Maximum reconnect backoff (seconds) |
| `request_timeout` | `10.0` | Per-command timeout (seconds) |
| `http_fallback` | `True` | Use HTTP fallback for GET/SET when WebSocket is unavailable |

## Error types

| Exception | When raised |
|---|---|
| `SwarmKeyDbConnectionError` | Cannot connect or connection is lost |
| `SwarmKeyDbCommandError` | Server returns an error for a command |
| `SwarmKeyDbTimeoutError` | Command times out |
| `SwarmKeyDbAuthError` | Authentication fails |

## Tests

```bash
cd sdk/python
pip install -e ".[dev]"
pytest tests/unit/ -v
```

## Full documentation

See [docs/python-sdk.md](../../docs/python-sdk.md) for the complete API reference,
connection options, Pub/Sub demo, Streams walkthrough, and Docker Compose example.
