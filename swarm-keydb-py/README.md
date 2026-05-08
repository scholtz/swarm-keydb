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

Async equivalents are available in `AsyncSwarmKeyDb`.

## Examples

- `examples/user_profile.py`
- `examples/config_management.py`
- `examples/chat_message_history.py`

## Test

```bash
python -m unittest discover -s tests -v
```
