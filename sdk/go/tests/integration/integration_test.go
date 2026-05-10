package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	swarmkeydb "github.com/scholtz/swarm-keydb/sdk/go"
)

func wsURL() string {
	if v := os.Getenv("SWARM_KEYDB_WS_URL"); v != "" {
		return v
	}
	return "ws://127.0.0.1:8765"
}

func httpURL() string {
	if v := os.Getenv("SWARM_KEYDB_HTTP_URL"); v != "" {
		return v
	}
	return "http://127.0.0.1:8080"
}

func newClient() *swarmkeydb.Client {
	return swarmkeydb.NewClient(&swarmkeydb.Options{
		Addr:            wsURL(),
		HTTPAddr:        httpURL(),
		PoolSize:        2,
		DialTimeout:     2 * time.Second,
		ReadTimeout:     5 * time.Second,
		MaxRetries:      2,
		MinRetryBackoff: 50 * time.Millisecond,
		MaxRetryBackoff: 100 * time.Millisecond,
		HTTPFallback:    true,
	})
}

func unique(prefix string) string {
	return fmt.Sprintf("%s:%d", prefix, time.Now().UnixNano())
}

func requireReady(t *testing.T, c *swarmkeydb.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for {
		if err := c.Ping(ctx).Err(); err == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("server not ready: %v", ctx.Err())
		default:
			time.Sleep(250 * time.Millisecond)
		}
	}
}

func TestConnectSetGetDelExists(t *testing.T) {
	c := newClient()
	defer c.Close()
	requireReady(t, c)

	ctx := context.Background()
	key := unique("go:int")

	if err := c.Set(ctx, key, "value", 0).Err(); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	value, err := c.Get(ctx, key).Result()
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if value != "value" {
		t.Fatalf("expected value, got %q", value)
	}

	exists, err := c.Exists(ctx, key).Result()
	if err != nil {
		t.Fatalf("exists failed: %v", err)
	}
	if exists != 1 {
		t.Fatalf("expected exists=1, got %d", exists)
	}

	deleted, err := c.Del(ctx, key).Result()
	if err != nil {
		t.Fatalf("del failed: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected deleted=1, got %d", deleted)
	}
}

func TestExpireTTLAndMissingKey(t *testing.T) {
	c := newClient()
	defer c.Close()
	requireReady(t, c)

	ctx := context.Background()
	key := unique("go:ttl")
	if err := c.Set(ctx, key, "v", 0).Err(); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if !c.Expire(ctx, key, 5*time.Second).Val() {
		t.Fatalf("expire failed")
	}
	ttl, err := c.TTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("ttl failed: %v", err)
	}
	if ttl <= 0 {
		t.Fatalf("expected ttl > 0, got %v", ttl)
	}

	missing, err := c.Get(ctx, unique("go:missing")).Result()
	if !errors.Is(err, swarmkeydb.Nil) {
		t.Fatalf("expected nil error for missing key, got value=%q err=%v", missing, err)
	}
}

func TestPublishSubscribe(t *testing.T) {
	publisher := newClient()
	defer publisher.Close()
	subscriber := newClient()
	defer subscriber.Close()
	requireReady(t, publisher)
	requireReady(t, subscriber)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	channel := unique("go:ch")
	ps, err := subscriber.Subscribe(ctx, channel)
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	defer ps.Close()

	msgCh := ps.Channel()
	time.Sleep(200 * time.Millisecond)
	if _, err := publisher.Publish(ctx, channel, "hello-go").Result(); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case msg := <-msgCh:
		if msg == nil || msg.Payload != "hello-go" {
			t.Fatalf("unexpected message: %#v", msg)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for pubsub message")
	}
}

func TestXAddXReadAndWrongTypeError(t *testing.T) {
	c := newClient()
	defer c.Close()
	requireReady(t, c)
	ctx := context.Background()

	stream := unique("go:stream")
	xreadDone := make(chan struct{})
	go func() {
		defer close(xreadDone)
		_, _ = c.XRead(ctx, &swarmkeydb.XReadArgs{
			Streams: []string{stream, "$"},
			Block:   2000,
			Count:   1,
		}).Result()
	}()

	time.Sleep(100 * time.Millisecond)
	if _, err := c.XAdd(ctx, &swarmkeydb.XAddArgs{Stream: stream, ID: "*", Values: []interface{}{"f", "v"}}).Result(); err != nil {
		t.Fatalf("xadd failed: %v", err)
	}
	select {
	case <-xreadDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("xread block did not return")
	}

	wrongType := unique("go:wrongtype")
	if err := c.Do(ctx, "LPUSH", wrongType, "item").Err(); err != nil {
		t.Fatalf("seed list for wrongtype failed: %v", err)
	}
	if err := c.Get(ctx, wrongType).Err(); err == nil {
		t.Fatalf("expected wrong type error from get on list key")
	}
}

func TestReconnectBackoffOnUnavailableServer(t *testing.T) {
	c := swarmkeydb.NewClient(&swarmkeydb.Options{
		Addr:            "ws://127.0.0.1:1",
		HTTPFallback:    false,
		DialTimeout:     20 * time.Millisecond,
		ReadTimeout:     20 * time.Millisecond,
		MaxRetries:      2,
		MinRetryBackoff: 50 * time.Millisecond,
		MaxRetryBackoff: 50 * time.Millisecond,
	})
	defer c.Close()

	start := time.Now()
	err := c.Get(context.Background(), "k").Err()
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected connection error")
	}
	if elapsed < 90*time.Millisecond {
		t.Fatalf("expected retry backoff delay, elapsed only %v", elapsed)
	}
}
