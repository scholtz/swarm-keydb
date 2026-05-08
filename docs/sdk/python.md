# Python SDK Quick Reference

Install:

```bash
cd swarm-keydb-py
pip install .
```

Run a complete example:

```bash
python examples/user_profile.py
```

Core API:

- `put(key, value)`, `get(key)`, `delete(key)`
- `list(pattern="*")`
- `batch_put(entries)`, `batch_get(keys)`
- `set_with_ttl(key, value, ttl_seconds)`
- async client equivalents in `AsyncSwarmKeyDb`

Error handling note: invalid TTL, ACL-denied operations, and failed reads are surfaced as Redis errors.
