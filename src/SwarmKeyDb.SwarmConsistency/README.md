# SwarmKeyDb.SwarmConsistency

`SwarmKeyDb.SwarmConsistency` verifies Swarm/Bee reads before values are returned to callers.

## Features

- Bee feed revision checks (`/feeds/{owner}/{topic}`)
- Byte content hash verification (`/bytes/{reference}`)
- Manifest lineage checks (`/bzz/{reference}`)
- Quorum verifier across multiple Bee nodes
- `IKeyValueStore` middleware with strict/warn failure modes
- Structured diagnostics and status metrics for operators

## Basic usage

```csharp
services.AddSwarmKeyDbStore(new BeeSwarmClient(new Uri("http://localhost:1633/"), batchId), new InMemoryKeyIndex());
services.AddSwarmConsistency(
    new[] { new Uri("http://localhost:1633/") },
    options =>
    {
        options.FailureMode = ConsistencyFailureMode.Strict;
        options.FeedOwner = "00112233445566778899aabbccddeeff00112233";
    });
```
