# Python SDK Quick Reference

SwarmKeyDb provides two Python SDKs:

## `swarm-keydb-client` (WebSocket-first, recommended for new projects)

Full documentation: [`docs/python-sdk.md`](../python-sdk.md)

Install:

```bash
pip install swarm-keydb-client
```

Quick start (async):

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

Quick start (sync):

```python
from swarm_keydb_client import SwarmKeyDbClient

client = SwarmKeyDbClient("ws://localhost:8765/")
client.set("hello", "world")
print(client.get("hello"))  # b"world"
client.close()
```

Source: `sdk/python/`

## `swarm-keydb` (Redis TCP, legacy)

Install:

```bash
cd swarm-keydb-py
pip install .
```

Core API:

- `put(key, value)`, `get(key)`, `delete(key)`
- `list(pattern="*")`
- `batch_put(entries)`, `batch_get(keys)`
- `set_with_ttl(key, value, ttl_seconds)`
- `backup()`, `restore(ref, key=None)`, `rotate_key(old_key, new_key)`
- async client equivalents in `AsyncSwarmKeyDb`
- privacy options: `privacy_mode` + `privacy_key` (`PrivacyMode.NONE`, `PrivacyMode.OBLIVIOUS_HASHING`, `PrivacyMode.FULL_PSI`)

Run a complete example:

```bash
python examples/user_profile.py
```

