# Python SDK (`swarm-keydb-client`)

Python SDK for SwarmKeyDb — WebSocket-first, async/sync Redis-compatible client
with HTTP fallback, Pub/Sub, Streams, and Transaction support.

## Installation from package registry

```bash
pip install swarm-keydb-client
```

Optional: install `hiredis` for faster RESP parsing:

```bash
pip install swarm-keydb-client[hiredis]
```

Requires Python 3.9+.

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

For a runnable quickstart with Pub/Sub, see [`examples/python/quickstart.py`](../examples/python/quickstart.py).

## Sync interface

For scripts and interactive REPL sessions that don't want to manage an event loop:

```python
from swarm_keydb_client import SwarmKeyDbClient

client = SwarmKeyDbClient("ws://localhost:8765/")
client.set("hello", "world")
print(client.get("hello"))  # b"world"
client.close()
```

The sync client runs a background event loop on a daemon thread, so it works
with no extra configuration in any Python context.

## Connection options

Both `AsyncSwarmKeyDbClient` and `SwarmKeyDbClient` accept the same keyword
arguments:

| Parameter | Default | Description |
|---|---|---|
| `ws_url` | `ws://127.0.0.1:8765/` | WebSocket endpoint |
| `http_url` | `http://127.0.0.1:8080` | HTTP fallback base URL |
| `password` | `None` | AUTH password (sent automatically after connect) |
| `reconnect` | `True` | Auto-reconnect on disconnect |
| `reconnect_base_delay` | `0.25` | Initial reconnect backoff (seconds) |
| `reconnect_max_delay` | `5.0` | Maximum reconnect backoff (seconds) |
| `request_timeout` | `10.0` | Per-command timeout (seconds) |
| `http_fallback` | `True` | Use HTTP REST gateway for GET/SET when WebSocket is unavailable |

### With authentication

```python
client = AsyncSwarmKeyDbClient(
    "ws://localhost:8765/",
    password="mysecret",
)
await client.connect()
```

## Core commands

```python
# Key-value
await client.set("k", "v")
await client.set("k", "v", ex=60)           # expire in 60 s
await client.set("k", "v", px=60_000)       # expire in 60 000 ms
await client.set("k", "v", nx=True)         # only if not exists
await client.set("k", "v", xx=True)         # only if exists
value = await client.get("k")               # bytes | None
await client.delete("k1", "k2")
count = await client.exists("k1", "k2")

# Expiry
await client.expire("k", 30)               # TTL in seconds
await client.pexpire("k", 30_000)          # TTL in milliseconds
ttl = await client.ttl("k")               # -1 (no TTL), -2 (missing), or seconds
await client.persist("k")                 # remove TTL

# Scanning
keys = await client.keys("prefix:*")
cursor, batch = await client.scan(0, match="prefix:*", count=100)
```

## String commands

```python
await client.append("k", " world")
await client.strlen("k")
await client.incr("counter")
await client.incrby("counter", 10)
await client.incrbyfloat("ratio", 0.5)
await client.decr("counter")
await client.decrby("counter", 5)
values = await client.mget("k1", "k2", "k3")   # List[bytes | None]
await client.mset({"k1": "v1", "k2": "v2"})
await client.getrange("k", 0, 4)
await client.setrange("k", 6, "SwarmKeyDb")
old = await client.getset("k", "new")
```

## Pub/Sub

```python
import asyncio
from swarm_keydb_client import AsyncSwarmKeyDbClient

async def pubsub_demo():
    pub = AsyncSwarmKeyDbClient("ws://localhost:8765/")
    sub = AsyncSwarmKeyDbClient("ws://localhost:8765/")
    await pub.connect()
    await sub.connect()

    async with sub.subscribe("news", "sports") as ch:
        await pub.publish("news", "SwarmKeyDb is live!")
        async for msg in ch.listen():
            print(msg)
            # {"type": "message", "channel": "news", "data": "SwarmKeyDb is live!"}
            break

    await pub.close()
    await sub.close()

asyncio.run(pubsub_demo())
```

### Pattern subscriptions

```python
received = []

def on_event(msg):
    received.append(msg)

await client.psubscribe("sensor.*", handler=on_event)
await asyncio.sleep(0.5)
await client.punsubscribe("sensor.*")
```

### Publish from a different connection

```python
n = await publisher.publish("news", "breaking!")
print(f"Delivered to {n} subscriber(s)")
```

## Streams

```python
# Append entries
entry_id = await client.xadd("events", {"sensor": "temp", "value": "22.5"})

# Read entries
entries = await client.xrange("events", "-", "+")
for eid, fields in entries:
    print(eid, fields)

# Reverse order, limit 10
entries = await client.xrevrange("events", "+", "-", count=10)

# Non-blocking read from position 0
result = await client.xread({"events": "0"}, count=5)

# Blocking read (block up to 1000 ms)
result = await client.xread({"events": "$"}, block=1000)

# Stream length and trim
length = await client.xlen("events")
trimmed = await client.xtrim("events", maxlen=1000)

# Consumer groups
await client.xgroup_create("events", "mygroup", "0", mkstream=True)
result = await client.xreadgroup("mygroup", "consumer1", {"events": ">"})
if result:
    _, entries = result[0]
    for eid, _ in entries:
        await client.xack("events", "mygroup", eid)
await client.xgroup_destroy("events", "mygroup")
```

## Transactions

```python
# Explicit MULTI/EXEC
await client.multi()
await client.incr("counter")
await client.incr("counter")
results = await client.exec()
print(results)  # [1, 2]

# Context manager (recommended)
async with client.transaction() as tx:
    await tx.set("k", "v")
    await tx.incr("counter")

# Optimistic locking with WATCH
async with client.transaction("my-key") as tx:
    # If "my-key" is modified by another connection between WATCH and EXEC,
    # EXEC returns nil and SwarmKeyDbCommandError is raised.
    await tx.set("my-key", "new-value")
```

## Error handling

```python
from swarm_keydb_client import (
    SwarmKeyDbCommandError,
    SwarmKeyDbConnectionError,
    SwarmKeyDbTimeoutError,
    SwarmKeyDbAuthError,
)

try:
    await client.get("k")
except SwarmKeyDbConnectionError as e:
    print(f"Connection problem: {e}")
except SwarmKeyDbCommandError as e:
    print(f"Server error for command '{e.command}': {e.server_error}")
except SwarmKeyDbTimeoutError as e:
    print(f"Timed out: {e}")
```

All exceptions are subclasses of `SwarmKeyDbError`.

## HTTP fallback

When the WebSocket gateway is unavailable, the client falls back to the HTTP
REST gateway for `GET` and `SET` commands:

```python
client = AsyncSwarmKeyDbClient(
    ws_url="ws://localhost:8765/",
    http_url="http://localhost:8080",
    http_fallback=True,      # default
)
```

The HTTP fallback requires `httpx` (included as a dependency).

## RESP3 negotiation

```python
info = await client.hello(3)
print(info)  # {"server": "swarmkeydb", "version": "...", "proto": 3, ...}
```

## Raw commands

For any command not yet wrapped:

```python
result = await client.raw("COMMAND", "COUNT")
result = await client.raw("CONFIG", "GET", "maxmemory")
```

## Docker Compose quick start

```yaml
# docker-compose.yml
services:
  swarmkeydb:
    image: scholtz/swarm-keydb:latest
    ports:
      - "6379:6379"   # Redis TCP
      - "8765:8765"   # WebSocket gateway
      - "8080:8080"   # HTTP REST gateway
    environment:
      SWARM_KEYDB_WS_PORT: 8765
      SWARM_KEYDB_HTTP_PORT: 8080
      BACKEND: memory
```

```bash
docker compose up -d
pip install swarm-keydb-client
python examples/python/basic_kv.py
```

## Examples

| File | Description |
|---|---|
| [`examples/python/basic_kv.py`](../examples/python/basic_kv.py) | Core get/set/scan operations, async and sync |
| [`examples/python/pubsub_demo.py`](../examples/python/pubsub_demo.py) | Channel and pattern Pub/Sub |
| [`examples/python/streams_demo.py`](../examples/python/streams_demo.py) | Streams, consumer groups, XTRIM |

## Testing

```bash
cd sdk/python
pip install -e ".[dev]"

# Unit tests only (no server required)
pytest tests/unit/ -v

# CI smoke integration tests (requires a running SwarmKeyDb)
pytest tests/integration/test_ci_smoke.py -v -m integration \
  --override-ini="asyncio_mode=auto"

# Extended integration tests
pytest tests/integration/test_integration.py -v -m integration \
  --override-ini="asyncio_mode=auto"
```

Environment variables for integration tests:

| Variable | Default | Description |
|---|---|---|
| `SWARM_KEYDB_WS_URL` | `ws://127.0.0.1:8765/` | WebSocket URL |
| `SWARM_KEYDB_HTTP_URL` | `http://127.0.0.1:8080` | HTTP base URL |
| `SWARM_KEYDB_PASSWORD` | _(none)_ | AUTH password |

## API reference

### `AsyncSwarmKeyDbClient`

| Method | Signature | Description |
|---|---|---|
| `connect` | `() → None` | Connect via WebSocket |
| `close` | `() → None` | Close connection |
| `get` | `(key) → bytes\|None` | GET |
| `set` | `(key, value, ex, px, nx, xx, keepttl) → bool` | SET |
| `delete` | `(*keys) → int` | DEL |
| `exists` | `(*keys) → int` | EXISTS |
| `expire` | `(key, seconds) → bool` | EXPIRE |
| `pexpire` | `(key, ms) → bool` | PEXPIRE |
| `ttl` | `(key) → int` | TTL |
| `pttl` | `(key) → int` | PTTL |
| `keys` | `(pattern) → List[str]` | KEYS |
| `scan` | `(cursor, match, count) → (int, List[str])` | SCAN |
| `type` | `(key) → str` | TYPE |
| `rename` | `(key, newkey) → bool` | RENAME |
| `persist` | `(key) → bool` | PERSIST |
| `append` | `(key, value) → int` | APPEND |
| `strlen` | `(key) → int` | STRLEN |
| `incr` | `(key) → int` | INCR |
| `incrby` | `(key, amount) → int` | INCRBY |
| `incrbyfloat` | `(key, amount) → float` | INCRBYFLOAT |
| `decr` | `(key) → int` | DECR |
| `decrby` | `(key, amount) → int` | DECRBY |
| `mget` | `(*keys) → List[bytes\|None]` | MGET |
| `mset` | `(mapping) → bool` | MSET |
| `getset` | `(key, value) → bytes\|None` | GETSET |
| `getrange` | `(key, start, end) → bytes` | GETRANGE |
| `setrange` | `(key, offset, value) → int` | SETRANGE |
| `subscribe` | `(*channels) → AsyncContextManager[PubSubChannel]` | SUBSCRIBE |
| `psubscribe` | `(*patterns, handler) → None` | PSUBSCRIBE |
| `punsubscribe` | `(*patterns) → None` | PUNSUBSCRIBE |
| `publish` | `(channel, message) → int` | PUBLISH |
| `xadd` | `(stream, fields, id, maxlen, approximate) → str` | XADD |
| `xlen` | `(stream) → int` | XLEN |
| `xrange` | `(stream, start, end, count) → List[Entry]` | XRANGE |
| `xrevrange` | `(stream, end, start, count) → List[Entry]` | XREVRANGE |
| `xread` | `(streams, count, block) → StreamResult\|None` | XREAD |
| `xreadgroup` | `(group, consumer, streams, count, block, noack) → StreamResult\|None` | XREADGROUP |
| `xack` | `(stream, group, *ids) → int` | XACK |
| `xpending` | `(stream, group, start, end, count, consumer) → List` | XPENDING |
| `xclaim` | `(stream, group, consumer, min_idle_time, *ids) → List[Entry]` | XCLAIM |
| `xtrim` | `(stream, maxlen, approximate) → int` | XTRIM |
| `xgroup_create` | `(stream, group, id, mkstream) → bool` | XGROUP CREATE |
| `xgroup_destroy` | `(stream, group) → int` | XGROUP DESTROY |
| `xgroup_setid` | `(stream, group, id) → bool` | XGROUP SETID |
| `multi` | `() → bool` | MULTI |
| `exec` | `() → List\|None` | EXEC |
| `discard` | `() → bool` | DISCARD |
| `watch` | `(*keys) → bool` | WATCH |
| `unwatch` | `() → bool` | UNWATCH |
| `transaction` | `(*watch_keys) → AsyncContextManager` | MULTI/EXEC context |
| `auth` | `(password) → bool` | AUTH |
| `ping` | `(message) → str` | PING |
| `hello` | `(proto_ver) → dict` | HELLO |
| `raw` | `(command, *args) → Any` | Execute arbitrary command |

`SwarmKeyDbClient` exposes the same methods synchronously (excluding async-only
context managers like `subscribe` and `transaction`).
