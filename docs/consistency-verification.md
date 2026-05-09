# Consistency Verification

SwarmKeyDb includes an optional **consistency verification layer** via the
`SwarmKeyDb.SwarmConsistency` NuGet package. When enabled, every cached read path
(cache hit, read-through, and background refresh) is gated by a verifier before values
reach callers — so stale, tampered, or divergent Swarm payloads are evicted and
re-fetched instead of being silently served.

---

## Quick Start (5 minutes)

**1. Install the package**

```bash
dotnet add package SwarmKeyDb.SwarmConsistency
```

**2. Register with a single line**

```csharp
// In Program.cs or wherever you configure services
builder.Services
    .AddSwarmKeyDbStore(swarmClient, keyIndex)
    .WithConsistencyVerification(new Uri("http://localhost:1633/"));
```

That's it. Every `GetAsync` call now verifies the returned value against the live Bee
node before returning it to callers.

**3. Or register with full control**

```csharp
builder.Services
    .AddSwarmKeyDbStore(swarmClient, keyIndex)
    .AddSwarmConsistency(
        beeNodeUrls: [new Uri("http://bee-1:1633/"), new Uri("http://bee-2:1633/")],
        configure: options =>
        {
            options.FailureMode   = ConsistencyFailureMode.Warn;
            options.QuorumThreshold = 2;                    // both nodes must agree
            options.OnVerificationFailure = (key, result) =>
                Console.Error.WriteLine($"Verification failed: {key} — {result.FailureReason}");
        });
```

---

## How It Works

The `ConsistencyVerificationMiddleware` decorator sits on top of the full store chain
(including `CachingKeyValueStore`).  For every `GetAsync`:

1. The inner store is called; a cached value is returned if available.
2. The middleware resolves the Swarm backend reference for the key (propagated through
   every decorator via `IBackendMetadataProvider`).
3. `ISwarmConsistencyVerifier.VerifyContentHashAsync` is called with the Swarm reference
   and the SHA-256 hash of the in-memory value.
4. **On pass** — the value is returned as-is.
5. **On fail**:
   - The key is **evicted from the in-memory cache** via `ICacheEviction` so the next
     read fetches fresh from Swarm.
   - The operator's `OnVerificationFailure` callback (if any) is invoked.
   - The middleware re-fetches the value from the backend and returns it.
   - In `Strict` mode, `ConsistencyViolationException` is thrown instead.

---

## Configuration

### `ConsistencyOptions`

| Property | Type | Default | Description |
|---|---|---|---|
| `Enabled` | `bool` | `true` | Master switch. Set to `false` to pass all reads through without verification. |
| `FailureMode` | `ConsistencyFailureMode` | `Strict` | `Strict` throws `ConsistencyViolationException`; `Warn` logs and evicts. |
| `QuorumThreshold` | `int` | `1` | Number of Bee nodes that must agree before a value is admitted. Requires multiple `beeNodeUrls`. |
| `FeedOwner` | `string` | all-zero address | Default Bee feed owner used for feed-revision checks. |
| `ExpectedFeedRevisionResolver` | `Func<string, ulong?>?` | `null` | Per-key resolver for expected feed revision. Return `null` to skip feed checks. |
| `ExpectedManifestLineageResolver` | `Func<string, (string, IReadOnlyList<string>)?>?` | `null` | Per-key resolver for expected manifest ancestors. |
| `OnVerificationFailure` | `Action<string, VerificationResult>?` | `null` | Callback invoked in `Warn` mode after a verification failure and cache eviction. Receives the key name and the full `VerificationResult`. |

### Environment variables (server)

| Variable | Default | Purpose |
|---|---|---|
| `SWARM_KEYDB_CONSISTENCY_ENABLED` | `false` | Enables verification middleware. |
| `SWARM_KEYDB_CONSISTENCY_FAILURE_MODE` | `Strict` | `Strict` or `Warn`. |
| `SWARM_KEYDB_CONSISTENCY_QUORUM_THRESHOLD` | `1` | Quorum threshold. |
| `SWARM_KEYDB_CONSISTENCY_FEED_OWNER` | all-zero address | Default feed owner. |
| `SWARM_KEYDB_CONSISTENCY_BEE_NODES` | unset | Comma-separated Bee URLs. Falls back to `BEE_URL`. |

---

## Prometheus Metrics

The following counters are exposed at `/metrics`:

| Metric | Type | Description |
|---|---|---|
| `swarmkeydb_cache_verification_pass_total` | counter | Cache reads that passed verification. |
| `swarmkeydb_cache_verification_fail_total` | counter | Cache reads that failed verification. |
| `swarmkeydb_cache_eviction_by_verification_total` | counter | Cache evictions triggered by a verification failure. |
| `swarmkeydb_consistency_verification_total` | counter | Total verification checks performed. |
| `swarmkeydb_consistency_violations_total` | counter | Total violations detected. |
| `swarmkeydb_consistency_success_rate` | gauge | Fraction of verifications that passed (0–1). |
| `swarmkeydb_consistency_worst_latency_ms` | gauge | Highest single-verification latency in ms. |

The `/dashboard` and `/health` endpoints also display `totalVerifications`,
`violationCount`, `successRate`, and `worstLatencyMs`.

---

## FAQ

**What happens when verification fails?**

In `Warn` mode (availability-biased), the stale cache entry is evicted and the
middleware re-fetches the value from Swarm.  The caller receives the freshly fetched
value.  In `Strict` mode (consistency-biased), `ConsistencyViolationException` is thrown
immediately with the key name, expected hash, actual hash, and failure reason.

**Does verification add latency to every read?**

Only when a backend reference is available for the key (i.e. the key was stored via
`SwarmKeyValueStore`).  Reads that return no reference (e.g. from a store that does not
implement `IBackendMetadataProvider`) are passed through without verification.

**Can I choose which keys to verify?**

Yes. Use `ExpectedFeedRevisionResolver` and `ExpectedManifestLineageResolver` to enable
per-key feed/manifest checks, or simply set `Enabled = false` for keys you want to
bypass.

**What is `ICacheEviction`?**

`ICacheEviction` is a lightweight interface implemented by `CachingKeyValueStore` and
propagated through every outer decorator (`OfflineCapableKeyValueStore`,
`AsyncQueuedKeyValueStore`).  It allows the verification middleware to remove a single
entry from the in-memory cache without touching the Swarm backend — so the next read
goes directly to Swarm for a fresh authoritative value.

**Can I use quorum verification?**

Yes. Provide multiple Bee URLs in `AddSwarmConsistency`/`WithConsistencyVerification`
and set `QuorumThreshold` to the required agreement count.  A `QuorumNotMetException` is
raised (and handled internally) when fewer than `QuorumThreshold` nodes agree.
