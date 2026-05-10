# A Key-Value Store on Swarm using redis protocol in C# and build to docker

## Description

Build a developer-friendly Key-Value database library on top of Swarm — a familiar `get(key)` / `put(key, value)` interface backed by decentralized, persistent storage.

Swarm has powerful primitives — content addressing, feeds, manifests — but using them today requires knowing how all the pieces fit together. Most developers don't want to think about feeds and topics and single-owner chunks. They want a simple database. This library wraps Swarm's internals into something any developer can pick up in minutes.

Under the hood, Swarm Feeds let you create a stable pointer (identified by your address + a topic string) that you can update over time. Map each "key" to a topic, and you've got a KV store. For listing keys, Swarm manifests can serve as an index.

Traditional Redis-style deployments still struggle with multi-instance data availability when each node keeps its own hot cache. Backing every instance with decentralized Swarm storage improves durability and recovery, but the product also needs cache synchronization and consistency verification so no node keeps serving stale or divergent data after writes, failovers, or reconnects.

***What is required to complete this bounty?***

- Support strings, JSON, and binary values
- Key listing and iteration
- Handle storage costs (postage stamps) transparently
- Clear docs with working examples

***What are examples of use cases you are looking to solve?***

- User profiles and settings for decentralized apps
- Config storage for dApps that need mutable state
- Chat history, bookmarks, preferences — anything an app would normally put in a database

***What are the planned features to make it a robust key-value database?***

- **Delivered foundation (implemented):** TTL/expiration, batch operations, range and prefix scans, CRDT merge strategies, encryption, ACL and DID authorization, backup/restore/key rotation, offline-first sync, consistency verification middleware, cross-instance cache invalidation + anti-entropy + partial/full resync, Redis Pub/Sub parity, browser-native WebSocket gateway (`SWARM_KEYDB_WS_PORT`) for JSON/RESP Pub/Sub and `XREAD BLOCK` subscriptions with RESP3 negotiation (`HELLO 3`/`RESET`) and client-tracking push envelopes, zero-dependency HTTP REST gateway (`SWARM_KEYDB_HTTP_PORT`) for curl/fetch JSON access plus OpenAPI/Swagger docs and RESP3 content negotiation (`Accept: application/json; resp=3`), Redis transaction semantics (`MULTI`/`EXEC`/`WATCH`), stream core + consumer-group workflows (including blocking `XREAD`/`XREADGROUP` wake/timeout behavior), Redis scripting (`EVAL`/`EVALSHA`/`SCRIPT LOAD|EXISTS|FLUSH|KILL`) with sandboxed Lua (MoonSharp), `redis.call`/`redis.pcall` dispatch, configurable timeout guard, and five Prometheus script metrics, Docker and Helm release automation, IPFS and Ethereum integrations, cross-chain sync, and SDK/connectors (JS, Python, Go, React, Node).
- **Current unresolved gaps (active roadmap):**
	- **Streams retention and resilience hardening:** completed with `XTRIM` (`MAXLEN` + `MINID`), default `XADD` retention config, retention metrics/dashboard coverage, and failure-injection tests for restart recovery, duplicate ACK idempotency, PEL re-delivery, and concurrent-group isolation.
	- **Scripting safety + replication determinism:** delivered — `EVAL`/`EVALSHA`/`SCRIPT` commands with MoonSharp sandbox (io/os/package stripped), Task-based timeout returning `BUSY`, 10 MiB output cap, stable error replies, cross-node script-cache propagation (`SCRIPT LOAD`/`EVAL`), startup script-cache resync requests, propagated `SCRIPT FLUSH`, `EVALSHA` peer-fetch fallback, and Prometheus metrics including script-replication counters.
	- **Operability hardening:** delivered — compatibility commands (`INFO`/`COMMAND`/`CLIENT`/`CONFIG`), adaptive active-expiry budgeting (`SWARM_KEYDB_EXPIRY_BUDGET_MS`), maxmemory controls (`SWARM_KEYDB_MAX_MEMORY_MB` + policies), parser conformance handling for malformed RESP, and Prometheus metrics for expiry/memory/evictions.
- **Issue-informed compatibility priorities (next roadmap):**
	- **Issue-watch follow-through:** continue weekly KeyDB/Valkey issue-watch triage and map newly discovered high-impact items to roadmap tasks and conformance tests.
- **External issue inputs used for prioritization:** KeyDB issues include Pub/Sub and cluster consistency concerns (`#853`, `#845`), `KEYS` operational hangs (`#878`), and memory/eviction instability (`#972`). Valkey issues include stream correctness (`#3429`), command edge-case parsing correctness (`#3483`), and richer command error observability (`#3636`).

***What are the UX, Privacy, other requirements?***

- The developer should never need to understand feeds, topics, or SOCs to use this library
- Data is tied to the user's Ethereum keypair — private by default

## Judging Criteria

- **Developer experience:** How easy is it to get started? Could someone use this in 5 minutes?
- **API design:** Clean, intuitive, well-documented
- **Completeness:** Supports listing, deletion, and iteration — not just get/put
- **Edge cases:** Handles missing keys and large values gracefully. *Nice to have:* a clever approach to concurrent writes (not trivial with the current protocol — impress us if you solve it)
- **Examples:** Working, runnable examples included

## Resources

**Contacts:** Swarm team at the booth + [Discord](https://discord.gg/dUS68y87U4)

**Resource Links:**

- [Swarm Feeds guide](https://docs.ethswarm.org/docs/develop/access-the-swarm/feeds)
- [Dynamic content guide](https://docs.ethswarm.org/docs/develop/dynamic-content) — practical feed examples
- [bee-js SDK](https://github.com/ethersphere/bee-js) — `FeedWriter`, `FeedReader`, `MantarayNode`
- [Swarm docs](https://docs.ethswarm.org/docs/develop/introduction/)
