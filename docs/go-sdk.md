# Go SDK API Reference

Module path: `github.com/scholtz/swarm-keydb/sdk/go`  
Package: `swarmkeydb`

## Installation from package registry

```bash
go get github.com/scholtz/swarm-keydb/sdk/go@latest
```

## Quick start

```go
package main

import (
    "context"
    "fmt"

    swarmkeydb "github.com/scholtz/swarm-keydb/sdk/go"
)

func main() {
    client := swarmkeydb.NewClient(&swarmkeydb.Options{Addr: "ws://127.0.0.1:8765"})
    defer client.Close()
    _ = client.Set(context.Background(), "hello", "world", 0).Err()
    value, _ := client.Get(context.Background(), "hello").Result()
    fmt.Println(value)
}
```

Runnable example: [`examples/go/main.go`](../examples/go/main.go).

## Overview

The Go SDK provides a goroutine-safe client for SwarmKeyDb using a WebSocket connection pool as the primary transport, with an optional HTTP fallback. The API surface mirrors [go-redis](https://github.com/redis/go-redis) conventions so teams familiar with go-redis can adopt this SDK with minimal ramp-up.

---

## Types

### Client

```go
type Client struct { /* unexported */ }

func NewClient(opts *Options) *Client
func (c *Client) Close() error
```

`NewClient` initialises the pool lazily — connections are opened on first use. `Close` drains and closes every pooled connection.

---

### Options

| Field | Type | Default | Description |
|---|---|---|---|
| `Addr` | `string` | `ws://127.0.0.1:8765` | WebSocket gateway address |
| `HTTPAddr` | `string` | `http://127.0.0.1:8080` | HTTP REST address (fallback) |
| `Password` | `string` | `""` | AUTH password |
| `PoolSize` | `int` | `10` | Connection pool size |
| `DialTimeout` | `time.Duration` | `5s` | New connection timeout |
| `ReadTimeout` | `time.Duration` | `10s` | Per-command timeout |
| `WriteTimeout` | `time.Duration` | `10s` | Write timeout (reserved) |
| `MaxRetries` | `int` | `3` | Max retries on transient error |
| `MinRetryBackoff` | `time.Duration` | `8ms` | Min retry delay |
| `MaxRetryBackoff` | `time.Duration` | `512ms` | Max retry delay |
| `HTTPFallback` | `bool` | `true` | Enable HTTP fallback |

---

### Result Types

All commands return a typed result struct that carries both the value and an optional error. This avoids multiple return values at the call site while keeping error checks explicit.

| Type | `Val()` return | `Result()` return | Used by |
|---|---|---|---|
| `Cmd` | `interface{}` | `(interface{}, error)` | `Do` |
| `StringCmd` | `string` | `(string, error)` | `Get`, `GetRange`, … |
| `StatusCmd` | `string` | `(string, error)` | `Set`, `Ping`, `Type`, … |
| `IntCmd` | `int64` | `(int64, error)` | `Del`, `Incr`, `XLen`, … |
| `BoolCmd` | `bool` | `(bool, error)` | `SetNX`, `Expire`, … |
| `FloatCmd` | `float64` | `(float64, error)` | `IncrByFloat` |
| `SliceCmd` | `[]interface{}` | `([]interface{}, error)` | `MGet` |
| `StringSliceCmd` | `[]string` | `([]string, error)` | `Keys` |
| `DurationCmd` | `time.Duration` | `(time.Duration, error)` | `TTL`, `PTTL` |
| `ScanCmd` | `(uint64, []string)` | `(uint64, []string, error)` | `Scan` |
| `XMessageSliceCmd` | `[]XMessage` | `([]XMessage, error)` | `XRange`, `XClaim`, … |
| `XStreamSliceCmd` | `[]XStream` | `([]XStream, error)` | `XRead`, `XReadGroup` |
| `XPendingCmd` | `*XPending` | `(*XPending, error)` | `XPending` |

All result types implement the `Cmder` interface:

```go
type Cmder interface {
    Err() error
}
```

---

### Sentinel Errors

```go
var Nil = errors.New("swarmkeydb: nil")
var WatchConflictError = errors.New("swarmkeydb: watch conflict: transaction aborted")
```

### Error Types

```go
type ConnectionError struct {
    Msg   string
    Cause error  // underlying net/dial error
}

type CommandError struct {
    Command string
    Msg     string  // server-returned error text
}

type TimeoutError struct {
    Command string
}

type AuthError struct {
    Msg string
}
```

All error types implement `error`. `ConnectionError` additionally implements `Unwrap() error` for use with `errors.Is` / `errors.As`.

---

## Command API

### Core / Key Commands

```go
Ping(ctx context.Context) *StatusCmd
Get(ctx context.Context, key string) *StringCmd
Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *StatusCmd
SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *BoolCmd
SetXX(ctx context.Context, key string, value interface{}, expiration time.Duration) *BoolCmd
GetSet(ctx context.Context, key string, value interface{}) *StringCmd
Del(ctx context.Context, keys ...string) *IntCmd
Exists(ctx context.Context, keys ...string) *IntCmd
Expire(ctx context.Context, key string, expiration time.Duration) *BoolCmd
PExpire(ctx context.Context, key string, expiration time.Duration) *BoolCmd
TTL(ctx context.Context, key string) *DurationCmd
PTTL(ctx context.Context, key string) *DurationCmd
Persist(ctx context.Context, key string) *BoolCmd
Keys(ctx context.Context, pattern string) *StringSliceCmd
Scan(ctx context.Context, cursor uint64, match string, count int64) *ScanCmd
Type(ctx context.Context, key string) *StatusCmd
Rename(ctx context.Context, key, newkey string) *StatusCmd
RenameNX(ctx context.Context, key, newkey string) *BoolCmd
FlushDB(ctx context.Context) *StatusCmd
```

**TTL semantics:** `DurationCmd.Val()` returns `time.Duration(-1)` when the key has no TTL and `time.Duration(-2)` when the key does not exist, matching Redis conventions.

---

### String Commands

```go
Append(ctx context.Context, key, value string) *IntCmd
GetRange(ctx context.Context, key string, start, end int64) *StringCmd
SetRange(ctx context.Context, key string, offset int64, value string) *IntCmd
StrLen(ctx context.Context, key string) *IntCmd
Incr(ctx context.Context, key string) *IntCmd
IncrBy(ctx context.Context, key string, value int64) *IntCmd
IncrByFloat(ctx context.Context, key string, value float64) *FloatCmd
Decr(ctx context.Context, key string) *IntCmd
DecrBy(ctx context.Context, key string, value int64) *IntCmd
MGet(ctx context.Context, keys ...string) *SliceCmd
MSet(ctx context.Context, keysvalues ...interface{}) *StatusCmd
```

---

### Pub/Sub

#### Client methods

```go
Subscribe(ctx context.Context, channels ...string) (*PubSub, error)
PSubscribe(ctx context.Context, patterns ...string) (*PubSub, error)
Publish(ctx context.Context, channel, message string) *IntCmd
```

Each `Subscribe` / `PSubscribe` call opens a **dedicated** WebSocket connection so pub/sub operations are isolated from command traffic.

#### PubSub type

```go
type PubSub struct { /* unexported */ }

// Channel returns a buffered channel that delivers incoming messages.
func (ps *PubSub) Channel(opts ...ChannelOption) <-chan *Message

// Subscribe adds channels to the subscription.
func (ps *PubSub) Subscribe(ctx context.Context, channels ...string) error

// Unsubscribe removes channels from the subscription.
func (ps *PubSub) Unsubscribe(ctx context.Context, channels ...string) error

// PSubscribe adds pattern subscriptions.
func (ps *PubSub) PSubscribe(ctx context.Context, patterns ...string) error

// PUnsubscribe removes pattern subscriptions.
func (ps *PubSub) PUnsubscribe(ctx context.Context, patterns ...string) error

// Close closes the pub/sub connection.
func (ps *PubSub) Close() error
```

#### ChannelOption

```go
func WithChannelSize(n int) ChannelOption  // set buffer size; default 256
```

#### Message

```go
type Message struct {
    Type    string  // "message" or "pmessage"
    Channel string
    Pattern string  // non-empty for pmessage
    Payload string
}
```

---

### Streams

#### Arg types

```go
type XAddArgs struct {
    Stream string
    MaxLen int64   // 0 = no trimming
    Approx bool   // use MAXLEN ~
    MinID  string // MINID trimming
    ID     string // "" or "*" for auto-generate
    Values []interface{}
}

type XReadArgs struct {
    Streams []string  // alternating stream names and IDs
    Count   int64
    Block   int64     // milliseconds; 0 non-blocking
}

type XReadGroupArgs struct {
    Group    string
    Consumer string
    Streams  []string
    Count    int64
    Block    int64
    NoAck    bool
}

type XClaimArgs struct {
    Stream   string
    Group    string
    Consumer string
    MinIdle  int64    // milliseconds
    Messages []string // IDs to claim
}
```

#### Result types

```go
type XMessage struct {
    ID     string
    Values map[string]interface{}
}

type XStream struct {
    Stream   string
    Messages []XMessage
}

type XPending struct {
    Count     int64
    Lower     string
    Higher    string
    Consumers map[string]int64
}
```

#### Stream commands

```go
XAdd(ctx context.Context, args *XAddArgs) *StringCmd
XLen(ctx context.Context, stream string) *IntCmd
XRange(ctx context.Context, stream, start, stop string) *XMessageSliceCmd
XRangeN(ctx context.Context, stream, start, stop string, count int64) *XMessageSliceCmd
XRevRange(ctx context.Context, stream, stop, start string) *XMessageSliceCmd
XRevRangeN(ctx context.Context, stream, stop, start string, count int64) *XMessageSliceCmd
XRead(ctx context.Context, args *XReadArgs) *XStreamSliceCmd
XReadGroup(ctx context.Context, args *XReadGroupArgs) *XStreamSliceCmd
XAck(ctx context.Context, stream, group string, ids ...string) *IntCmd
XTrim(ctx context.Context, key string, maxLen int64) *IntCmd
XTrimApprox(ctx context.Context, key string, maxLen int64) *IntCmd
XGroupCreate(ctx context.Context, stream, group, start string) *StatusCmd
XGroupCreateMkStream(ctx context.Context, stream, group, start string) *StatusCmd
XGroupSetID(ctx context.Context, stream, group, start string) *StatusCmd
XGroupDestroy(ctx context.Context, stream, group string) *IntCmd
XPending(ctx context.Context, stream, group string) *XPendingCmd
XClaim(ctx context.Context, args *XClaimArgs) *XMessageSliceCmd
```

---

### Transactions

#### TxPipelined

Executes a function that queues commands and then flushes them atomically as a `MULTI` / `EXEC` block. Returns the results of all queued commands.

```go
func (c *Client) TxPipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error)
```

#### Watch

Opens a dedicated connection, issues `WATCH` for the given keys, then calls `fn` inside a `Tx`. If the transaction is aborted due to a watch conflict (`WatchConflictError`), `fn` is retried up to `MaxRetries` times.

```go
func (c *Client) Watch(ctx context.Context, fn func(*Tx) error, keys ...string) error
```

Inside `fn`, use `tx.TxPipelined` to queue and execute commands:

```go
err := client.Watch(ctx, func(tx *swarmkeydb.Tx) error {
    _, err := tx.TxPipelined(ctx, func(pipe swarmkeydb.Pipeliner) error {
        pipe.Do(ctx, "SET", "key", "value")
        return nil
    })
    return err
}, "key")
```

#### Pipeliner interface

```go
type Pipeliner interface {
    Do(ctx context.Context, args ...interface{}) *Cmd
    Exec(ctx context.Context) ([]Cmder, error)
}
```

---

### Arbitrary Commands

```go
func (c *Client) Do(ctx context.Context, args ...interface{}) *Cmd
```

The first argument must be a `string` (the command name). All other arguments are formatted with `fmt.Sprintf("%v", ...)`.

---

## Wire Protocol

Commands are encoded as JSON frames:

```json
{"cmd":"SET","args":["mykey","myvalue","EX","60"]}
```

Responses:

```json
{"result": "OK"}
{"error": "ERR wrong number of arguments"}
```

Pub/Sub push frames:

```json
{"type":"message","channel":"ch","data":"payload"}
{"type":"pmessage","channel":"ch","pattern":"ch*","data":"payload"}
```

---

## Internal Transport

The `internal/transport` package is not part of the public API and may change without notice. It contains:

- `Conn` — a gorilla/websocket connection with FIFO request-response matching and a dedicated push channel for pub/sub frames.
- `HTTPTransport` — a thin `net/http` wrapper for the REST fallback.

---

## Thread Safety

`Client` is goroutine-safe. `PubSub` is goroutine-safe for subscribe/unsubscribe operations. Individual `Conn` objects inside the pool are not shared across concurrent commands — each command borrows a connection for the duration of its round-trip.

---

## Versioning

The module follows [semver](https://semver.org/). Breaking API changes will increment the major version.
