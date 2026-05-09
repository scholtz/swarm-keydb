# Roadmap for SwarmKeyDb

## Multi-Instance Availability and Cache Consistency
- [x] Create a `SwarmKeyDb.SwarmConsistency` NuGet package that validates Swarm and Bee reads with content-hash verification, feed or manifest revision checks, optional quorum policies, and operator-friendly failure diagnostics before values reach callers. (100%)
- [x] Add consistency verification hooks to every cached `IKeyValueStore` path so cache hits, read-through fetches, and background refreshes evict or reject stale Swarm payloads instead of serving divergent in-memory data. (100%: `ICacheEviction` interface propagated through all decorator stores; `ConsistencyVerificationMiddleware` evicts on verification failure and re-fetches from Swarm; `OnVerificationFailure` callback in `ConsistencyOptions`; `EvictionByVerificationTotal` in `ConsistencyVerificationSnapshot`; `IBackendMetadataProvider` propagated through `EncryptingKeyValueStore`, `CompressingKeyValueStore`, `CrdtKeyValueStore`, `AclKeyValueStore`, `DidAuthKeyValueStore`, `CachingKeyValueStore`, `OfflineCapableKeyValueStore`, and `AsyncQueuedKeyValueStore`; Prometheus metrics `swarmkeydb_cache_verification_pass_total`, `swarmkeydb_cache_verification_fail_total`, `swarmkeydb_cache_eviction_by_verification_total`; `WithConsistencyVerification()` DI extension; full unit + integration test coverage)
- [x] Implement cross-instance cache synchronization with version stamps, invalidation events, and anti-entropy reconciliation so multiple SwarmKeyDb nodes converge after writes, expirations, restarts, and temporary network partitions. (100%: added version-stamped cache invalidation events, `ICacheSyncBus` with Redis and in-memory implementations, `AntiEntropyService` reconciliation loop, `SWARM_KEYDB_SYNC_*` configuration, dashboard sync status panel, and multi-instance sync/partition test coverage)
- [ ] Define partial-resync and full-resync flows for cache state recovery so an instance can cheaply catch up when version history is available and deterministically rebuild from Swarm when it is not.
- [ ] Add multi-node integration tests and production telemetry for cache drift, verification failures, synchronization lag, and forced resync counts so operators can prove consistency during failover and rolling deployments.

## Redis Pub/Sub Compatibility
- [ ] Implement Redis-compatible channel Pub/Sub commands (`SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`) with connection-scoped subscription state and RESP push reply structure matching redis-cli expectations.
- [ ] Add `PUBSUB` subcommands (`CHANNELS`, `NUMSUB`, `NUMPAT`) and ensure command results stay consistent under concurrent subscribe, unsubscribe, reconnect, and shard rebalance events.
- [ ] Build resilient fan-out delivery with bounded per-client output buffers, backpressure handling, and deterministic disconnect behavior to prevent hangs and worker failure loops reported in other Redis-compatible servers.
- [ ] Add pattern routing parity for glob semantics and duplicate-subscription edge cases, including strict tests for subscribe counts, message ordering within a connection, and unsubscribe acknowledgements.
- [ ] Provide horizontal Pub/Sub propagation across SwarmKeyDb instances via lightweight inter-node invalidation transport, then expose delivery lag and dropped-subscriber counters in `/metrics` and dashboard panels.

## Redis Transactions and Concurrency Semantics
- [ ] Implement `MULTI`, `EXEC`, and `DISCARD` with queued command responses, per-connection transactional context, and explicit error propagation semantics matching Redis behavior for queued failures.
- [ ] Add optimistic locking support with `WATCH` and `UNWATCH`, including version-stamp integration with Swarm index updates so conflicting writes reliably abort transactional execution.
- [ ] Support transactional interactions with key expiry and deletion by defining deterministic behavior for keys expiring between queueing and execution, then document compatibility differences where unavoidable.
- [ ] Create robustness tests for pipelined transaction workloads, client disconnect during `MULTI`, and replay safety under retries to avoid state corruption and orphaned queued commands.
- [ ] Add telemetry for transaction abort rates, watch conflicts, queue depth, and execution latency so operators can detect contention hotspots and tune application write patterns.

## Redis Streams and Consumer Groups
- [ ] Implement core stream commands (`XADD`, `XRANGE`, `XREVRANGE`, `XLEN`) using monotonic IDs and append-only semantics that preserve ordering guarantees expected by Redis stream clients.
- [ ] Add consumer-group workflow commands (`XGROUP`, `XREADGROUP`, `XACK`, `XPENDING`) with pending-entry tracking and restart-safe state persistence across node restarts.
- [ ] Implement blocking and non-blocking read semantics for `XREAD` and `XREADGROUP`, including timeout handling and fair wake-up behavior under multi-consumer contention.
- [ ] Define retention policies (`MAXLEN` approximate and exact trimming) that prevent unbounded memory growth while preserving practical replay windows for late or recovering consumers.
- [ ] Add failure-injection tests for crash recovery, duplicate delivery, and re-delivery from pending entries to ensure at-least-once stream processing remains predictable and observable.

## Scripting, Functions, and Runtime Safety
- [ ] Implement script execution compatibility (`EVAL`, `EVALSHA`, `SCRIPT LOAD`, `SCRIPT EXISTS`, `SCRIPT FLUSH`) with deterministic command sandboxing and explicit resource limits.
- [ ] Introduce script runtime guards for maximum CPU time, recursion depth, and output size so long-running or malicious scripts cannot starve command processing threads.
- [ ] Add deterministic script replication and cache invalidation behavior for multi-instance deployments so script SHA resolution remains consistent after rolling updates and failovers.
- [ ] Implement secure defaults that disable unsafe host integration primitives and return stable protocol-level errors instead of leaking runtime or framework exception internals.
- [ ] Add regression tests covering known script-engine CVE patterns from Redis-compatible ecosystems, including denial-of-service vectors, stack exhaustion, and remote code execution preconditions.

## Compatibility, Expiry, and Operability Hardening
- [ ] Expand Redis command coverage with compatibility-focused priorities (`INFO`, `COMMAND`, `CLIENT`, `CONFIG GET`) to improve ecosystem tooling support and reduce unknown-command operational surprises.
- [ ] Rework active expiry scheduling with adaptive scan budgeting to keep latency predictable under heavy TTL churn, avoiding keyspace cleanup stalls seen in high-write Redis-compatible deployments.
- [ ] Introduce memory-pressure controls (`maxmemory`-style limits and documented eviction policies) with deterministic behavior, clear metrics, and safety valves before process-level OOM conditions.
- [ ] Add protocol conformance tests for parser edge cases, malformed RESP frames, and command argument validation, including integer overflow and boundary-value handling across all Redis-visible commands.
- [ ] Add issue-watch automation that tracks selected KeyDB and Valkey issues, then maps each relevant finding to SwarmKeyDb tests or roadmap tasks with explicit status in release notes.
