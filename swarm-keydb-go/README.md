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
- `PutJSON` / `GetJSON` helpers

## Examples

- `examples/user-profile/main.go`
- `examples/config-management/main.go`
- `examples/chat-history/main.go`

## Test

```bash
go test ./...
```
