# swarm-keydb-go

Go SDK for SwarmKeyDb via Redis protocol.

## Quickstart

```go
ctx := context.Background()
client := swarmkeydb.New(swarmkeydb.Options{Host: "127.0.0.1", Port: 6379})
_ = client.Put(ctx, "hello", "world")
v, _ := client.Get(ctx, "hello")
fmt.Println(v)
```

## API

- `Get(ctx, key)`
- `Put(ctx, key, value)`
- `Delete(ctx, key)`
- `List(ctx, pattern)`
- `BatchGet(ctx, keys)`
- `BatchPut(ctx, entries)`
- `SetWithTTL(ctx, key, value, ttlSeconds)`
- `Backup(ctx)`
- `Restore(ctx, ref, key)`
- `RotateKey(ctx, oldKey, newKey)`
- `PutJSON` / `GetJSON` helpers

## Privacy-preserving mode

```go
client := swarmkeydb.New(swarmkeydb.Options{
  Host:          "127.0.0.1",
  Port:          6379,
  PrivacyMode:   swarmkeydb.PrivacyModeObliviousHashing,
  PrivacyKeyHex: "<64-char-hex-key>",
})
```

## Offline-first mode

```go
client := swarmkeydb.New(swarmkeydb.Options{
  Host:        "127.0.0.1",
  Port:        6379,
  OfflineMode: swarmkeydb.OfflineModeAuto,
})
```

Use this when the backing SwarmKeyDb server has offline-first support enabled.

## Data integrity

The SwarmKeyDb server verifies a SHA-256 integrity envelope on every read by default. If stored Swarm data has been corrupted or tampered with, `Get`/`BatchGet` return an error from Redis; callers should handle that error path explicitly.

## Using IPFS and hybrid backends

Backend selection is server-side, so Go client code stays the same:

```bash
BACKEND=ipfs IPFS_API_URL=http://localhost:5001/ dotnet run --project src/SwarmKeyDb.Server/SwarmKeyDb.Server.csproj
BACKEND=hybrid BEE_URL=http://localhost:1633/ BEE_POSTAGE_BATCH_ID=<id> IPFS_API_URL=http://localhost:5001/ dotnet run --project src/SwarmKeyDb.Server/SwarmKeyDb.Server.csproj
```

## Examples

- `examples/user-profile/main.go`
- `examples/config-management/main.go`
- `examples/chat-history/main.go`

## Test

```bash
go test ./...
```
