# SwarmKeyDb Go SDK

A first-class Go client for [SwarmKeyDb](https://github.com/scholtz/swarm-keydb) — a Redis-compatible key-value store backed by Swarm decentralised storage. The SDK communicates via WebSocket (primary) with automatic HTTP fallback, and exposes a go-redis–style API.

## Requirements

- Go 1.21+
- SwarmKeyDb server with WebSocket gateway on port 8765 and/or HTTP REST on port 8080

## Installation

```sh
go get github.com/scholtz/swarm-keydb/sdk/go
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    swarmkeydb "github.com/scholtz/swarm-keydb/sdk/go"
)

func main() {
    client := swarmkeydb.NewClient(&swarmkeydb.Options{
        Addr:     "ws://localhost:8765",
        HTTPAddr: "http://localhost:8080",
        Password: "optional-password",
    })
    defer client.Close()

    ctx := context.Background()

    if err := client.Set(ctx, "hello", "world", 60*time.Second).Err(); err != nil {
        log.Fatal(err)
    }

    val, err := client.Get(ctx, "hello").Result()
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(val) // world
}
```

## Configuration

```go
client := swarmkeydb.NewClient(&swarmkeydb.Options{
    // WebSocket gateway address (primary transport).
    Addr: "ws://localhost:8765",           // default: ws://127.0.0.1:8765

    // HTTP REST address (fallback for GET/SET when WebSocket is unavailable).
    HTTPAddr: "http://localhost:8080",     // default: http://127.0.0.1:8080

    // Password sent with AUTH after each new connection.
    Password: "",

    // Number of pooled WebSocket connections.
    PoolSize: 10,                          // default: 10

    // Timeout for dialing a new WebSocket connection.
    DialTimeout: 5 * time.Second,

    // Per-command response deadline.
    ReadTimeout: 10 * time.Second,

    // Maximum retries on transient network errors.
    MaxRetries: 3,

    // Retry back-off range.
    MinRetryBackoff: 8 * time.Millisecond,
    MaxRetryBackoff: 512 * time.Millisecond,

    // Enable HTTP fallback for GET/SET when WebSocket fails.
    HTTPFallback: true,
})
```

## Command Reference

### Core / Key Commands

```go
ctx := context.Background()

// Ping
pong := client.Ping(ctx).Val() // "PONG"

// Get / Set
client.Set(ctx, "key", "value", 0)                    // no TTL
client.Set(ctx, "key", "value", 60*time.Second)        // with TTL
client.Set(ctx, "key", "value", 500*time.Millisecond)  // ms precision (PX)
val, err := client.Get(ctx, "key").Result()

// SetNX / SetXX
ok := client.SetNX(ctx, "key", "val", 0).Val()  // true if key did not exist
ok  = client.SetXX(ctx, "key", "val", 0).Val()  // true if key existed

// GetSet
old, err := client.GetSet(ctx, "key", "newvalue").Result()

// Delete / Exists
n, err := client.Del(ctx, "key1", "key2").Result()
n, err  = client.Exists(ctx, "key1", "key2").Result()

// TTL
ttl, err  := client.TTL(ctx, "key").Result()   // time.Duration; -1 no TTL, -2 missing
pttl, err := client.PTTL(ctx, "key").Result()

// Expire / PExpire / Persist
client.Expire(ctx, "key", 30*time.Second)
client.PExpire(ctx, "key", 500*time.Millisecond)
client.Persist(ctx, "key")

// Type / Rename
t := client.Type(ctx, "key").Val()
client.Rename(ctx, "old", "new")
client.RenameNX(ctx, "old", "new")

// Scan / Keys
cursor, keys, err := client.Scan(ctx, 0, "prefix:*", 100).Result()
keys, err := client.Keys(ctx, "prefix:*").Result()

// FlushDB
client.FlushDB(ctx)
```

### String Commands

```go
// Append / StrLen
newLen, err := client.Append(ctx, "key", "suffix").Result()
length, err  := client.StrLen(ctx, "key").Result()

// Range
sub, err := client.GetRange(ctx, "key", 0, 4).Result()
newLen, err := client.SetRange(ctx, "key", 6, "replacement").Result()

// Numeric operations
n, err   := client.Incr(ctx, "counter").Result()
n, err    = client.IncrBy(ctx, "counter", 10).Result()
f, err   := client.IncrByFloat(ctx, "float-key", 1.5).Result()
n, err    = client.Decr(ctx, "counter").Result()
n, err    = client.DecrBy(ctx, "counter", 5).Result()

// Multi-key
vals, err := client.MGet(ctx, "k1", "k2", "k3").Result()
client.MSet(ctx, "k1", "v1", "k2", "v2")
```

### Pub/Sub

```go
// Subscribe to channels
ps, err := client.Subscribe(ctx, "channel1", "channel2")
if err != nil {
    log.Fatal(err)
}
defer ps.Close()

// Receive messages
ch := ps.Channel(swarmkeydb.WithChannelSize(512))
for msg := range ch {
    fmt.Printf("[%s] %s\n", msg.Channel, msg.Payload)
}

// Pattern subscribe
pps, err := client.PSubscribe(ctx, "prefix.*")
defer pps.Close()

// Publish
client.Publish(ctx, "channel1", "hello")

// Dynamic subscribe/unsubscribe
ps.Subscribe(ctx, "new-channel")
ps.Unsubscribe(ctx, "channel1")
ps.PSubscribe(ctx, "other.*")
ps.PUnsubscribe(ctx, "prefix.*")
```

### Streams

```go
// XADD
id, err := client.XAdd(ctx, &swarmkeydb.XAddArgs{
    Stream: "mystream",
    Values: []interface{}{"field1", "value1", "field2", "value2"},
}).Result()

// XADD with MAXLEN trimming
id, err = client.XAdd(ctx, &swarmkeydb.XAddArgs{
    Stream: "mystream",
    MaxLen: 1000,
    Approx: true, // MAXLEN ~
    Values: []interface{}{"field", "value"},
}).Result()

// XLen
length, err := client.XLen(ctx, "mystream").Result()

// XRange / XRangeN
msgs, err := client.XRange(ctx, "mystream", "-", "+").Result()
msgs, err  = client.XRangeN(ctx, "mystream", "-", "+", 10).Result()

// XRevRange / XRevRangeN
msgs, err = client.XRevRange(ctx, "mystream", "+", "-").Result()

// XRead
streams, err := client.XRead(ctx, &swarmkeydb.XReadArgs{
    Streams: []string{"mystream", "0"},
    Count:   10,
    Block:   0, // non-blocking
}).Result()

// Consumer groups
client.XGroupCreate(ctx, "mystream", "mygroup", "0")
client.XGroupCreateMkStream(ctx, "mystream", "mygroup", "$")

streams, err = client.XReadGroup(ctx, &swarmkeydb.XReadGroupArgs{
    Group:    "mygroup",
    Consumer: "consumer-1",
    Streams:  []string{"mystream", ">"},
    Count:    10,
}).Result()

// Acknowledge messages
for _, s := range streams {
    for _, m := range s.Messages {
        client.XAck(ctx, "mystream", "mygroup", m.ID)
    }
}

// Trim
client.XTrim(ctx, "mystream", 1000)
client.XTrimApprox(ctx, "mystream", 1000)

// Pending / Claim
pending, err := client.XPending(ctx, "mystream", "mygroup").Result()
fmt.Printf("Pending: %d\n", pending.Count)

msgs, err = client.XClaim(ctx, &swarmkeydb.XClaimArgs{
    Stream:   "mystream",
    Group:    "mygroup",
    Consumer: "consumer-2",
    MinIdle:  60000, // ms
    Messages: []string{id},
}).Result()
```

### Transactions (MULTI/EXEC)

```go
// Simple TxPipelined
results, err := client.TxPipelined(ctx, func(pipe swarmkeydb.Pipeliner) error {
    pipe.Do(ctx, "SET", "key1", "value1")
    pipe.Do(ctx, "SET", "key2", "value2")
    pipe.Do(ctx, "GET", "key1")
    return nil
})
if err != nil {
    log.Fatal(err)
}
for i, r := range results {
    v, _ := r.(*swarmkeydb.Cmd).Result()
    fmt.Printf("[%d] %v\n", i, v)
}

// Optimistic locking with Watch
err = client.Watch(ctx, func(tx *swarmkeydb.Tx) error {
    // Read current value inside the watch scope
    // Then atomically update
    _, err := tx.TxPipelined(ctx, func(pipe swarmkeydb.Pipeliner) error {
        pipe.Do(ctx, "INCR", "counter")
        return nil
    })
    return err
}, "counter")
if err != nil {
    log.Fatal(err)
}
```

### Arbitrary Commands

```go
result, err := client.Do(ctx, "COMMAND", "COUNT").Result()
```

## Error Handling

```go
val, err := client.Get(ctx, "missing-key").Result()
if err == swarmkeydb.Nil {
    fmt.Println("key does not exist")
}

err = client.Set(ctx, "k", "v", 0).Err()
var cmdErr *swarmkeydb.CommandError
if errors.As(err, &cmdErr) {
    fmt.Printf("server rejected command %q: %s\n", cmdErr.Command, cmdErr.Msg)
}

var connErr *swarmkeydb.ConnectionError
if errors.As(err, &connErr) {
    fmt.Printf("connection failed: %s\n", connErr.Msg)
}

var authErr *swarmkeydb.AuthError
if errors.As(err, &authErr) {
    fmt.Println("authentication failed:", authErr.Msg)
}

// Watch conflict
if errors.Is(err, swarmkeydb.WatchConflictError) {
    fmt.Println("transaction aborted: watched key was modified")
}
```

## Connection Pool

The client maintains a pool of WebSocket connections (default size: 10). Connections are borrowed for each command and returned after completion. If a connection is closed by the server, it is discarded and a new one is dialed transparently.

```go
client := swarmkeydb.NewClient(&swarmkeydb.Options{
    Addr:        "ws://localhost:8765",
    PoolSize:    20,             // increase for high-concurrency workloads
    DialTimeout: 5 * time.Second,
    ReadTimeout: 10 * time.Second,
    MaxRetries:  3,
})
```

## HTTP Fallback

When `HTTPFallback: true` (the default), GET and SET commands fall back to the HTTP REST API (`POST /cmd`) after all WebSocket retries are exhausted. This is useful for environments where WebSocket connections are restricted.

```go
client := swarmkeydb.NewClient(&swarmkeydb.Options{
    Addr:         "ws://localhost:8765",
    HTTPAddr:     "http://localhost:8080",
    HTTPFallback: true,
})
```

## Running Examples

```sh
# Start SwarmKeyDb server first, then:
go run ./examples/basic_kv/
go run ./examples/pubsub_demo/
go run ./examples/streams_demo/
go run ./examples/transactions_demo/
```

## Running Tests

```sh
# Unit tests (no server required — uses mock WS server)
go test ./tests/unit/ -v -timeout 30s
```

## License

Apache 2.0 — see [LICENSE](../../LICENSE).
