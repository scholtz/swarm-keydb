# FAQ

## Do I need postage stamps?

Yes for Bee-backed storage. Set `BEE_POSTAGE_BATCH_ID` before writing keys.

## Can I run without Bee?

Yes. Default local backend works without Bee for development and tests.

## What does offline mode look like?

Set `SWARM_KEYDB_OFFLINE_MODE=auto` (or `always`) to queue Bee-bound writes locally and serve cached reads while Bee is unreachable. When connectivity returns, `OfflineSyncService` replays the journal automatically; `/health` and `/dashboard` expose the current queue depth.

## Why do I get `ERR Access denied`?

ACL is enabled and your address lacks required permissions. Run `AUTHADDR <0x-address>` and verify `SWARM_KEYDB_ACL_ENTRIES`.

## Why does `TTL` return `-1` or `-2`?

- `-1`: key exists but has no TTL.
- `-2`: key does not exist.

## Why do reads fail with an integrity error?

Integrity verification detected a hash mismatch on stored data. Re-write the key or inspect storage consistency.
