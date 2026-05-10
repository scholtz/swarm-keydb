// Package swarmkeydb provides a Go client for SwarmKeyDb.
//
// SwarmKeyDb is a Redis-compatible key-value store with WebSocket and HTTP
// gateways. This client uses WebSocket as the primary transport and falls back
// to HTTP for simple GET/SET operations when WebSocket is unavailable.
//
// # Quick start
//
//	client := swarmkeydb.NewClient(&swarmkeydb.Options{
//	    Addr:     "ws://localhost:8765",
//	    Password: "secret",
//	})
//	defer client.Close()
//
//	ctx := context.Background()
//	if err := client.Set(ctx, "key", "value", 0).Err(); err != nil {
//	    log.Fatal(err)
//	}
//	val, err := client.Get(ctx, "key").Result()
package swarmkeydb

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/scholtz/swarm-keydb/sdk/go/internal/transport"
)

// maxBackoffShift caps the bit-shift in retryBackoff to prevent overflow
// when MaxRetries is configured to a very large value.
const maxBackoffShift = 30

// Options configures a SwarmKeyDb client.
type Options struct {
	// Addr is the WebSocket address, e.g. "ws://localhost:8765".
	// Defaults to "ws://127.0.0.1:8765".
	Addr string

	// HTTPAddr is the HTTP fallback address, e.g. "http://localhost:8080".
	// Defaults to "http://127.0.0.1:8080".
	HTTPAddr string

	// Password is sent with AUTH after each connection.
	Password string

	// PoolSize is the number of WebSocket connections to maintain.
	// Defaults to 10.
	PoolSize int

	// DialTimeout is the timeout for establishing a new connection.
	// Defaults to 5 seconds.
	DialTimeout time.Duration

	// ReadTimeout is the per-command response timeout.
	// Defaults to 10 seconds.
	ReadTimeout time.Duration

	// WriteTimeout is the per-command write timeout.
	// Defaults to 10 seconds.
	WriteTimeout time.Duration

	// MaxRetries is the maximum number of retries on transient errors.
	// Defaults to 3.
	MaxRetries int

	// MinRetryBackoff is the minimum delay between retries.
	// Defaults to 8ms.
	MinRetryBackoff time.Duration

	// MaxRetryBackoff is the maximum delay between retries.
	// Defaults to 512ms.
	MaxRetryBackoff time.Duration

	// HTTPFallback enables HTTP fallback when WebSocket is unavailable.
	// Defaults to true.
	HTTPFallback bool
}

func (o *Options) defaults() {
	if o.Addr == "" {
		o.Addr = "ws://127.0.0.1:8765"
	}
	if o.HTTPAddr == "" {
		o.HTTPAddr = "http://127.0.0.1:8080"
	}
	if o.PoolSize <= 0 {
		o.PoolSize = 10
	}
	if o.DialTimeout <= 0 {
		o.DialTimeout = 5 * time.Second
	}
	if o.ReadTimeout <= 0 {
		o.ReadTimeout = 10 * time.Second
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = 10 * time.Second
	}
	if o.MaxRetries <= 0 {
		o.MaxRetries = 3
	}
	if o.MinRetryBackoff <= 0 {
		o.MinRetryBackoff = 8 * time.Millisecond
	}
	if o.MaxRetryBackoff <= 0 {
		o.MaxRetryBackoff = 512 * time.Millisecond
	}
}

// poolConn wraps a transport connection held in the pool.
type poolConn struct {
	conn *transport.Conn
}

// Client is a goroutine-safe SwarmKeyDb client backed by a connection pool.
type Client struct {
	opts     Options
	pool     chan *poolConn
	mu       sync.Mutex
	http     *transport.HTTPTransport
	closed   bool
	closedCh chan struct{}
	rng      *rand.Rand // per-client PRNG for jitter; guarded by mu
}

// NewClient returns a new Client. Call Close when done.
func NewClient(opts *Options) *Client {
	o := Options{}
	if opts != nil {
		o = *opts
	}
	o.defaults()

	c := &Client{
		opts:     o,
		pool:     make(chan *poolConn, o.PoolSize),
		closedCh: make(chan struct{}),
		// In Go 1.20+ the global rand source is auto-seeded; use it via rand.New
		// with a random source seeded from the global pool for per-client isolation.
		rng: rand.New(rand.NewSource(rand.Int63())),
	}
	if o.HTTPFallback {
		c.http = transport.NewHTTPTransport(o.HTTPAddr, o.Password, o.ReadTimeout)
	}
	return c
}

// Close closes all pooled connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	close(c.closedCh)
	for {
		select {
		case pc := <-c.pool:
			_ = pc.conn.Close()
		default:
			return nil
		}
	}
}

// acquire borrows a connection from the pool, dialing a new one if needed.
func (c *Client) acquire(ctx context.Context) (*poolConn, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, &ConnectionError{Msg: "client is closed"}
	}
	c.mu.Unlock()

	// Try to get an idle healthy connection
	for {
		select {
		case pc := <-c.pool:
			if !pc.conn.IsClosed() {
				return pc, nil
			}
		default:
			goto dial
		}
	}
dial:
	dialCtx, cancel := context.WithTimeout(ctx, c.opts.DialTimeout)
	defer cancel()
	conn, err := transport.Dial(dialCtx, transport.DialOptions{
		Addr:        c.opts.Addr,
		DialTimeout: c.opts.DialTimeout,
		ReadTimeout: c.opts.ReadTimeout,
	})
	if err != nil {
		return nil, &ConnectionError{Msg: "dial failed", Cause: err}
	}
	// Authenticate if password set
	if c.opts.Password != "" {
		resp, err := conn.Send(ctx, "AUTH", []string{c.opts.Password})
		if err != nil {
			_ = conn.Close()
			return nil, &ConnectionError{Msg: "auth send failed", Cause: err}
		}
		if resp.Error != "" {
			_ = conn.Close()
			return nil, &AuthError{Msg: resp.Error}
		}
	}
	return &poolConn{conn: conn}, nil
}

// release returns a connection to the pool.
func (c *Client) release(pc *poolConn) {
	if pc.conn.IsClosed() {
		return
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		_ = pc.conn.Close()
		return
	}
	select {
	case c.pool <- pc:
	default:
		_ = pc.conn.Close()
	}
}

// do executes cmd+args with retries and context cancellation.
func (c *Client) do(ctx context.Context, cmd string, args []string) (*transport.Response, error) {
	var lastErr error
	for attempt := 0; attempt <= c.opts.MaxRetries; attempt++ {
		if attempt > 0 {
			backoff := c.retryBackoff(attempt)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
		pc, err := c.acquire(ctx)
		if err != nil {
			lastErr = err
			continue
		}
		cmdCtx, cancel := context.WithTimeout(ctx, c.opts.ReadTimeout)
		resp, err := pc.conn.Send(cmdCtx, cmd, args)
		cancel()
		if err != nil {
			_ = pc.conn.Close()
			lastErr = err
			continue
		}
		c.release(pc)
		if resp.Error != "" {
			return resp, &CommandError{Command: cmd, Msg: resp.Error}
		}
		return resp, nil
	}
	// Try HTTP fallback for GET/SET
	if c.http != nil && (cmd == "GET" || cmd == "SET") {
		resp, err := c.http.Do(ctx, cmd, args)
		if err == nil {
			return resp, nil
		}
	}
	return nil, lastErr
}

func (c *Client) retryBackoff(attempt int) time.Duration {
	base := float64(c.opts.MinRetryBackoff)
	max := float64(c.opts.MaxRetryBackoff)
	shift := attempt - 1
	if shift > maxBackoffShift {
		shift = maxBackoffShift
	}
	d := base * float64(uint(1)<<uint(shift))
	if d > max {
		d = max
	}
	// add jitter ±25% using per-client PRNG (guarded by mu)
	jitter := d * 0.25
	c.mu.Lock()
	r := c.rng.Float64()
	c.mu.Unlock()
	d = d - jitter + r*2*jitter
	return time.Duration(d)
}

// Do executes an arbitrary command.
func (c *Client) Do(ctx context.Context, args ...interface{}) *Cmd {
	if len(args) == 0 {
		return newErrCmd(fmt.Errorf("swarmkeydb: Do requires at least one argument"))
	}
	cmd, ok := args[0].(string)
	if !ok {
		return newErrCmd(fmt.Errorf("swarmkeydb: first Do argument must be a string"))
	}
	strArgs := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		strArgs = append(strArgs, fmt.Sprintf("%v", a))
	}
	resp, err := c.do(ctx, cmd, strArgs)
	if err != nil {
		return newErrCmd(err)
	}
	return &Cmd{val: resp.Result}
}

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

// Cmder is the common interface for all command result types.
type Cmder interface {
	Err() error
}

// Cmd is a generic command result.
type Cmd struct {
	val interface{}
	err error
}

func newErrCmd(err error) *Cmd { return &Cmd{err: err} }

// Err returns any error from the command.
func (c *Cmd) Err() error { return c.err }

// Result returns the raw value and error.
func (c *Cmd) Result() (interface{}, error) { return c.val, c.err }

// StringCmd is a command that returns a string value.
type StringCmd struct {
	val string
	err error
}

// Err returns any error from the command.
func (c *StringCmd) Err() error { return c.err }

// Result returns the string value and error.
func (c *StringCmd) Result() (string, error) { return c.val, c.err }

// Val returns the string value, empty string on error.
func (c *StringCmd) Val() string { return c.val }

// StatusCmd is a command that returns a status string (e.g. "OK").
type StatusCmd struct {
	val string
	err error
}

// Err returns any error from the command.
func (c *StatusCmd) Err() error { return c.err }

// Result returns the status string and error.
func (c *StatusCmd) Result() (string, error) { return c.val, c.err }

// Val returns the status value, empty string on error.
func (c *StatusCmd) Val() string { return c.val }

// IntCmd is a command that returns an integer value.
type IntCmd struct {
	val int64
	err error
}

// Err returns any error from the command.
func (c *IntCmd) Err() error { return c.err }

// Result returns the int64 value and error.
func (c *IntCmd) Result() (int64, error) { return c.val, c.err }

// Val returns the int64 value, 0 on error.
func (c *IntCmd) Val() int64 { return c.val }

// BoolCmd is a command that returns a bool value.
type BoolCmd struct {
	val bool
	err error
}

// Err returns any error from the command.
func (c *BoolCmd) Err() error { return c.err }

// Result returns the bool value and error.
func (c *BoolCmd) Result() (bool, error) { return c.val, c.err }

// Val returns the bool value, false on error.
func (c *BoolCmd) Val() bool { return c.val }

// FloatCmd is a command that returns a float64 value.
type FloatCmd struct {
	val float64
	err error
}

// Err returns any error from the command.
func (c *FloatCmd) Err() error { return c.err }

// Result returns the float64 value and error.
func (c *FloatCmd) Result() (float64, error) { return c.val, c.err }

// Val returns the float64 value, 0 on error.
func (c *FloatCmd) Val() float64 { return c.val }

// SliceCmd is a command that returns a []interface{} value.
type SliceCmd struct {
	val []interface{}
	err error
}

// Err returns any error from the command.
func (c *SliceCmd) Err() error { return c.err }

// Result returns the slice and error.
func (c *SliceCmd) Result() ([]interface{}, error) { return c.val, c.err }

// Val returns the slice, nil on error.
func (c *SliceCmd) Val() []interface{} { return c.val }

// StringSliceCmd is a command that returns a []string value.
type StringSliceCmd struct {
	val []string
	err error
}

// Err returns any error from the command.
func (c *StringSliceCmd) Err() error { return c.err }

// Result returns the string slice and error.
func (c *StringSliceCmd) Result() ([]string, error) { return c.val, c.err }

// Val returns the string slice, nil on error.
func (c *StringSliceCmd) Val() []string { return c.val }

// DurationCmd is a command that returns a time.Duration (TTL).
type DurationCmd struct {
	val time.Duration
	err error
}

// Err returns any error from the command.
func (c *DurationCmd) Err() error { return c.err }

// Result returns the duration and error.
func (c *DurationCmd) Result() (time.Duration, error) { return c.val, c.err }

// Val returns the duration, 0 on error.
func (c *DurationCmd) Val() time.Duration { return c.val }

// ScanCmd holds the result of a SCAN command.
type ScanCmd struct {
	cursor uint64
	keys   []string
	err    error
}

// Err returns any error from the command.
func (c *ScanCmd) Err() error { return c.err }

// Result returns cursor, keys, and error.
func (c *ScanCmd) Result() (uint64, []string, error) { return c.cursor, c.keys, c.err }

// Val returns cursor and keys.
func (c *ScanCmd) Val() (uint64, []string) { return c.cursor, c.keys }

// ---------------------------------------------------------------------------
// Helper converters
// ---------------------------------------------------------------------------

func toStringCmd(resp *transport.Response, err error) *StringCmd {
	if err != nil {
		return &StringCmd{err: err}
	}
	if resp.Result == nil {
		return &StringCmd{err: Nil}
	}
	return &StringCmd{val: fmt.Sprintf("%v", resp.Result)}
}

func toStatusCmd(resp *transport.Response, err error) *StatusCmd {
	if err != nil {
		return &StatusCmd{err: err}
	}
	if resp.Result == nil {
		return &StatusCmd{val: ""}
	}
	return &StatusCmd{val: fmt.Sprintf("%v", resp.Result)}
}

func toIntCmd(resp *transport.Response, err error) *IntCmd {
	if err != nil {
		return &IntCmd{err: err}
	}
	return &IntCmd{val: toInt64(resp.Result)}
}

func toBoolCmd(resp *transport.Response, err error) *BoolCmd {
	if err != nil {
		return &BoolCmd{err: err}
	}
	switch v := resp.Result.(type) {
	case bool:
		return &BoolCmd{val: v}
	case float64:
		return &BoolCmd{val: v != 0}
	case string:
		return &BoolCmd{val: v == "1" || v == "OK" || v == "true"}
	case nil:
		return &BoolCmd{val: false}
	default:
		return &BoolCmd{val: toInt64(v) != 0}
	}
}

func toFloatCmd(resp *transport.Response, err error) *FloatCmd {
	if err != nil {
		return &FloatCmd{err: err}
	}
	switch v := resp.Result.(type) {
	case float64:
		return &FloatCmd{val: v}
	case string:
		var f float64
		fmt.Sscanf(v, "%f", &f)
		return &FloatCmd{val: f}
	default:
		return &FloatCmd{val: float64(toInt64(v))}
	}
}

func toStringSliceCmd(resp *transport.Response, err error) *StringSliceCmd {
	if err != nil {
		return &StringSliceCmd{err: err}
	}
	arr, ok := resp.Result.([]interface{})
	if !ok {
		return &StringSliceCmd{err: fmt.Errorf("swarmkeydb: unexpected type %T", resp.Result)}
	}
	ss := make([]string, len(arr))
	for i, v := range arr {
		ss[i] = fmt.Sprintf("%v", v)
	}
	return &StringSliceCmd{val: ss}
}

func toSliceCmd(resp *transport.Response, err error) *SliceCmd {
	if err != nil {
		return &SliceCmd{err: err}
	}
	if resp.Result == nil {
		return &SliceCmd{val: nil}
	}
	arr, ok := resp.Result.([]interface{})
	if !ok {
		return &SliceCmd{err: fmt.Errorf("swarmkeydb: unexpected type %T", resp.Result)}
	}
	return &SliceCmd{val: arr}
}

func toInt64(v interface{}) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	case string:
		var n int64
		fmt.Sscanf(x, "%d", &n)
		return n
	case bool:
		if x {
			return 1
		}
		return 0
	default:
		return 0
	}
}
