# swarm-keydb (Python SDK)

Python SDK for SwarmKeyDb via Redis protocol with sync and async clients.

## Quickstart

```python
from swarm_keydb import SwarmKeyDb

db = SwarmKeyDb(host="127.0.0.1", port=6379)
db.put("hello", "world")
print(db.get("hello"))
```

## API

- `get(key)`
- `put(key, value)`
- `delete(key)`
- `list(pattern="*")`
- `batch_get(keys)`
- `batch_put(entries)`
- `set_with_ttl(key, value, ttl_seconds)`
- `backup()`
- `restore(ref, key=None)`
- `rotate_key(old_key, new_key)`

## Privacy-preserving mode

```python
from swarm_keydb import PrivacyMode, SwarmKeyDb

db = SwarmKeyDb(
    host="127.0.0.1",
    port=6379,
    privacy_mode=PrivacyMode.OBLIVIOUS_HASHING,
    privacy_key="<64-char-hex-key>",
)
```

Async equivalents are available in `AsyncSwarmKeyDb`.

## Offline-first mode

```python
from swarm_keydb import OfflineMode, SwarmKeyDb

db = SwarmKeyDb(host='127.0.0.1', port=6379, offline_mode=OfflineMode.AUTO)
```

Use this alongside a server configured with `SWARM_KEYDB_OFFLINE_MODE=auto` or `always`.

## Data integrity

The SwarmKeyDb server verifies a SHA-256 integrity envelope on every read by default. If stored Swarm data has been corrupted or tampered with, `get()`/`batch_get()` raise the underlying Redis error from the server, so callers should treat failed reads as integrity-sensitive and handle them explicitly.

## Using IPFS and hybrid backends

The Python SDK calls Redis-compatible commands, so you can switch server backends without changing Python code:

```bash
BACKEND=ipfs IPFS_API_URL=http://localhost:5001/ dotnet run --project src/SwarmKeyDb.Server/SwarmKeyDb.Server.csproj
BACKEND=hybrid BEE_URL=http://localhost:1633/ BEE_POSTAGE_BATCH_ID=<id> IPFS_API_URL=http://localhost:5001/ dotnet run --project src/SwarmKeyDb.Server/SwarmKeyDb.Server.csproj
```

## Examples

- `examples/user_profile.py`
- `examples/config_management.py`
- `examples/chat_message_history.py`

## Test

```bash
python -m unittest discover -s tests -v
```
