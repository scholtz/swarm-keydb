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
- `backup()`, `restore(ref, key=None)`, `rotate_key(old_key, new_key)`
- async client equivalents in `AsyncSwarmKeyDb`
- privacy options: `privacy_mode` + `privacy_key` (`PrivacyMode.NONE`, `PrivacyMode.OBLIVIOUS_HASHING`, `PrivacyMode.FULL_PSI`)

Error handling note: invalid TTL, ACL-denied operations, and failed reads are surfaced as Redis errors.
