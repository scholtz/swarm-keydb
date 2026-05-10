package unit_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	swarmkeydb "github.com/scholtz/swarm-keydb/sdk/go"
)

// ---------------------------------------------------------------------------
// Mock WebSocket server helpers
// ---------------------------------------------------------------------------

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type wsFrame struct {
	Cmd  string   `json:"cmd"`
	Args []string `json:"args"`
}

type wsResp struct {
	Result  interface{} `json:"result,omitempty"`
	Error   string      `json:"error,omitempty"`
	Type    string      `json:"type,omitempty"`
	Channel string      `json:"channel,omitempty"`
	Data    string      `json:"data,omitempty"`
}

// mockWS responds to WebSocket frames with a handler function.
func mockWS(t *testing.T, handler func(cmd string, args []string) wsResp) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("upgrade error: %v", err)
			return
		}
		defer conn.Close()
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var f wsFrame
			if err := json.Unmarshal(msg, &f); err != nil {
				return
			}
			resp := handler(f.Cmd, f.Args)
			data, _ := json.Marshal(resp)
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
		}
	}))
	return srv
}

func wsURL(srv *httptest.Server) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func newTestClient(t *testing.T, srv *httptest.Server) *swarmkeydb.Client {
	t.Helper()
	return swarmkeydb.NewClient(&swarmkeydb.Options{
		Addr:         wsURL(srv),
		PoolSize:     2,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		MaxRetries:   1,
		HTTPFallback: false,
	})
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestPing(t *testing.T) {
	srv := mockWS(t, func(cmd string, args []string) wsResp {
		if cmd == "PING" {
			return wsResp{Result: "PONG"}
		}
		return wsResp{Error: "unexpected command " + cmd}
	})
	defer srv.Close()
	c := newTestClient(t, srv)
	defer c.Close()

	res := c.Ping(context.Background())
	if res.Err() != nil {
		t.Fatalf("Ping error: %v", res.Err())
	}
	if res.Val() != "PONG" {
		t.Errorf("expected PONG, got %q", res.Val())
	}
}

func TestPingError(t *testing.T) {
	srv := mockWS(t, func(cmd string, args []string) wsResp {
		return wsResp{Error: "ERR server busy"}
	})
	defer srv.Close()
	c := newTestClient(t, srv)
	defer c.Close()

	res := c.Ping(context.Background())
	if res.Err() == nil {
		t.Fatal("expected error from Ping, got nil")
	}
}

func TestSetGet(t *testing.T) {
	store := make(map[string]string)
	srv := mockWS(t, func(cmd string, args []string) wsResp {
		switch cmd {
		case "SET":
			store[args[0]] = args[1]
			return wsResp{Result: "OK"}
		case "GET":
			v, ok := store[args[0]]
			if !ok {
				return wsResp{Result: nil}
			}
			return wsResp{Result: v}
		}
		return wsResp{Error: "unknown"}
	})
	defer srv.Close()
	c := newTestClient(t, srv)
	defer c.Close()

	ctx := context.Background()
	if err := c.Set(ctx, "hello", "world", 0).Err(); err != nil {
		t.Fatalf("Set error: %v", err)
	}
	val, err := c.Get(ctx, "hello").Result()
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if val != "world" {
		t.Errorf("expected world, got %q", val)
	}
}

func TestSetWithTTL(t *testing.T) {
	var capturedArgs []string
	srv := mockWS(t, func(cmd string, args []string) wsResp {
		if cmd == "SET" {
			capturedArgs = args
			return wsResp{Result: "OK"}
		}
		return wsResp{Error: "unknown"}
	})
	defer srv.Close()
	c := newTestClient(t, srv)
	defer c.Close()

	ctx := context.Background()

	// Test EX for >= 1s TTL
	if err := c.Set(ctx, "k", "v", 2*time.Second).Err(); err != nil {
		t.Fatalf("Set with EX error: %v", err)
	}
	if len(capturedArgs) < 4 || capturedArgs[2] != "EX" || capturedArgs[3] != "2" {
		t.Errorf("expected EX 2, got args %v", capturedArgs)
	}

	// Test PX for sub-second TTL
	if err := c.Set(ctx, "k", "v", 500*time.Millisecond).Err(); err != nil {
		t.Fatalf("Set with PX error: %v", err)
	}
	if len(capturedArgs) < 4 || capturedArgs[2] != "PX" || capturedArgs[3] != "500" {
		t.Errorf("expected PX 500, got args %v", capturedArgs)
	}
}

func TestGetMissing(t *testing.T) {
	srv := mockWS(t, func(cmd string, args []string) wsResp {
		return wsResp{Result: nil}
	})
	defer srv.Close()
	c := newTestClient(t, srv)
	defer c.Close()

	cmd := c.Get(context.Background(), "missing")
	if cmd.Err() != swarmkeydb.Nil {
		t.Errorf("expected Nil error, got %v", cmd.Err())
	}
}

func TestAuth(t *testing.T) {
	var authed bool
	srv := mockWS(t, func(cmd string, args []string) wsResp {
		if cmd == "AUTH" {
			if len(args) > 0 && args[0] == "secret" {
				authed = true
				return wsResp{Result: "OK"}
			}
			return wsResp{Error: "WRONGPASS"}
		}
		if !authed {
			return wsResp{Error: "NOAUTH"}
		}
		if cmd == "PING" {
			return wsResp{Result: "PONG"}
		}
		return wsResp{Error: "unknown"}
	})
	defer srv.Close()

	c := swarmkeydb.NewClient(&swarmkeydb.Options{
		Addr:         wsURL(srv),
		Password:     "secret",
		PoolSize:     1,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		MaxRetries:   0,
		HTTPFallback: false,
	})
	defer c.Close()

	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("Ping after auth error: %v", err)
	}
	if !authed {
		t.Error("AUTH was not sent")
	}
}

func TestAuthFail(t *testing.T) {
	srv := mockWS(t, func(cmd string, args []string) wsResp {
		if cmd == "AUTH" {
			return wsResp{Error: "WRONGPASS invalid username-password pair"}
		}
		return wsResp{Error: "NOAUTH"}
	})
	defer srv.Close()

	c := swarmkeydb.NewClient(&swarmkeydb.Options{
		Addr:         wsURL(srv),
		Password:     "wrong",
		PoolSize:     1,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		MaxRetries:   0,
		HTTPFallback: false,
	})
	defer c.Close()

	err := c.Ping(context.Background()).Err()
	if err == nil {
		t.Fatal("expected auth error, got nil")
	}
}

func TestCommandError(t *testing.T) {
	srv := mockWS(t, func(cmd string, args []string) wsResp {
		return wsResp{Error: "ERR some error"}
	})
	defer srv.Close()
	c := newTestClient(t, srv)
	defer c.Close()

	err := c.Get(context.Background(), "k").Err()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *swarmkeydb.CommandError
	if !isCommandError(err, &cmdErr) {
		t.Fatalf("expected CommandError, got %T: %v", err, err)
	}
}

// isCommandError is a simple type assertion helper since errors.As is stdlib.
func isCommandError(err error, target **swarmkeydb.CommandError) bool {
	if ce, ok := err.(*swarmkeydb.CommandError); ok {
		*target = ce
		return true
	}
	return false
}

func TestDel(t *testing.T) {
	srv := mockWS(t, func(cmd string, args []string) wsResp {
		if cmd == "DEL" {
			return wsResp{Result: float64(len(args))}
		}
		return wsResp{Error: "unknown"}
	})
	defer srv.Close()
	c := newTestClient(t, srv)
	defer c.Close()

	n, err := c.Del(context.Background(), "a", "b", "c").Result()
	if err != nil {
		t.Fatalf("Del error: %v", err)
	}
	if n != 3 {
		t.Errorf("expected 3, got %d", n)
	}
}

func TestIncr(t *testing.T) {
	var counter int64 = 0
	srv := mockWS(t, func(cmd string, args []string) wsResp {
		if cmd == "INCR" {
			atomic.AddInt64(&counter, 1)
			return wsResp{Result: float64(atomic.LoadInt64(&counter))}
		}
		return wsResp{Error: "unknown"}
	})
	defer srv.Close()
	c := newTestClient(t, srv)
	defer c.Close()

	val, err := c.Incr(context.Background(), "counter").Result()
	if err != nil {
		t.Fatalf("Incr error: %v", err)
	}
	if val != 1 {
		t.Errorf("expected 1, got %d", val)
	}
}

func TestPubSubDelivery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var f wsFrame
			if err := json.Unmarshal(msg, &f); err != nil {
				return
			}
			if f.Cmd == "SUBSCRIBE" {
				// send subscribe confirmation
				conf, _ := json.Marshal(wsResp{
					Type:    "subscribe",
					Channel: f.Args[0],
					Result:  float64(1),
				})
				_ = conn.WriteMessage(websocket.TextMessage, conf)
				// push a message
				push, _ := json.Marshal(wsResp{
					Type:    "message",
					Channel: f.Args[0],
					Data:    "hello pubsub",
				})
				_ = conn.WriteMessage(websocket.TextMessage, push)
			}
		}
	}))
	defer srv.Close()

	c := swarmkeydb.NewClient(&swarmkeydb.Options{
		Addr:         wsURL(srv),
		PoolSize:     2,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		MaxRetries:   0,
		HTTPFallback: false,
	})
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ps, err := c.Subscribe(ctx, "test-channel")
	if err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}
	defer ps.Close()

	ch := ps.Channel()
	select {
	case msg := <-ch:
		if msg.Payload != "hello pubsub" {
			t.Errorf("expected 'hello pubsub', got %q", msg.Payload)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for pub/sub message")
	}
}

func TestContextCancellation(t *testing.T) {
	// Server that never responds
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		// just read and ignore
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	c := swarmkeydb.NewClient(&swarmkeydb.Options{
		Addr:         wsURL(srv),
		PoolSize:     1,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  5 * time.Second,
		MaxRetries:   0,
		HTTPFallback: false,
	})
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := c.Get(ctx, "key").Err()
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestHTTPFallback(t *testing.T) {
	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cmd" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"result":"fallback-value"}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer httpSrv.Close()

	c := swarmkeydb.NewClient(&swarmkeydb.Options{
		Addr:         "ws://127.0.0.1:19999", // unreachable
		HTTPAddr:     httpSrv.URL,
		PoolSize:     1,
		DialTimeout:  100 * time.Millisecond,
		ReadTimeout:  500 * time.Millisecond,
		MaxRetries:   0,
		HTTPFallback: true,
	})
	defer c.Close()

	val, err := c.Get(context.Background(), "key").Result()
	if err != nil {
		t.Fatalf("HTTP fallback Get error: %v", err)
	}
	if val != "fallback-value" {
		t.Errorf("expected 'fallback-value', got %q", val)
	}
}

func TestConnectionPool(t *testing.T) {
	var conns atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		conns.Add(1)
		defer func() {
			conns.Add(-1)
			conn.Close()
		}()
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var f wsFrame
			if err := json.Unmarshal(msg, &f); err != nil {
				return
			}
			resp, _ := json.Marshal(wsResp{Result: "OK"})
			if err := conn.WriteMessage(websocket.TextMessage, resp); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	c := swarmkeydb.NewClient(&swarmkeydb.Options{
		Addr:         wsURL(srv),
		PoolSize:     3,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		MaxRetries:   0,
		HTTPFallback: false,
	})
	defer c.Close()

	ctx := context.Background()
	// Run several sequential commands - pool should reuse connections
	for i := 0; i < 5; i++ {
		if err := c.Ping(ctx).Err(); err != nil {
			t.Fatalf("Ping %d error: %v", i, err)
		}
	}
	// Pool should have at most PoolSize connections
	if n := conns.Load(); n > 3 {
		t.Errorf("expected <= 3 connections, got %d", n)
	}
}

func TestCommandSerialization(t *testing.T) {
	var lastCmd wsFrame
	srv := mockWS(t, func(cmd string, args []string) wsResp {
		lastCmd = wsFrame{Cmd: cmd, Args: args}
		return wsResp{Result: "OK"}
	})
	defer srv.Close()
	c := newTestClient(t, srv)
	defer c.Close()

	ctx := context.Background()
	_ = c.Set(ctx, "mykey", "myvalue", 60*time.Second)

	if lastCmd.Cmd != "SET" {
		t.Errorf("expected SET, got %q", lastCmd.Cmd)
	}
	if len(lastCmd.Args) < 4 {
		t.Fatalf("expected at least 4 args, got %d: %v", len(lastCmd.Args), lastCmd.Args)
	}
	if lastCmd.Args[0] != "mykey" {
		t.Errorf("expected mykey, got %q", lastCmd.Args[0])
	}
	if lastCmd.Args[1] != "myvalue" {
		t.Errorf("expected myvalue, got %q", lastCmd.Args[1])
	}
	if lastCmd.Args[2] != "EX" {
		t.Errorf("expected EX, got %q", lastCmd.Args[2])
	}
	if lastCmd.Args[3] != "60" {
		t.Errorf("expected 60, got %q", lastCmd.Args[3])
	}
}
