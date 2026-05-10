package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// isPubSubType reports whether the response type represents a pub/sub push frame.
func isPubSubType(t string) bool {
	switch t {
	case "message", "pmessage", "subscribe", "unsubscribe", "psubscribe", "punsubscribe":
		return true
	}
	return false
}

type Frame struct {
	Cmd  string   `json:"cmd"`
	Args []string `json:"args"`
}

// Response is the JSON wire format for a SwarmKeyDb response.
type Response struct {
	Result interface{} `json:"result"`
	Error  string      `json:"error,omitempty"`
	// PubSub push fields
	Type    string `json:"type,omitempty"`
	Channel string `json:"channel,omitempty"`
	Pattern string `json:"pattern,omitempty"`
	Data    string `json:"data,omitempty"`
}

// PushMessage represents a pub/sub push frame from the server.
type PushMessage struct {
	Type    string
	Channel string
	Pattern string
	Data    string
}

// pending holds an in-flight request correlation.
type pending struct {
	resp chan *Response
}

// Conn is a single gorilla websocket connection with request-response matching.
type Conn struct {
	ws     *websocket.Conn
	mu     sync.Mutex // protects ws.WriteMessage
	once   sync.Once
	closed atomic.Bool
	pushCh chan *PushMessage
	pendMu sync.Mutex
	pendQ  []*pending // FIFO queue of in-flight requests
}

// DialOptions configures a websocket dial.
type DialOptions struct {
	Addr        string
	DialTimeout time.Duration
	ReadTimeout time.Duration
}

// Dial opens a websocket connection to addr.
func Dial(ctx context.Context, opts DialOptions) (*Conn, error) {
	dialer := websocket.Dialer{
		HandshakeTimeout: opts.DialTimeout,
		NetDialContext:   nil,
	}
	header := http.Header{}
	ws, _, err := dialer.DialContext(ctx, opts.Addr, header)
	if err != nil {
		return nil, err
	}
	c := &Conn{
		ws:     ws,
		pushCh: make(chan *PushMessage, 256),
	}
	go c.readLoop()
	return c, nil
}

func (c *Conn) readLoop() {
	defer func() {
		c.closed.Store(true)
		// drain all pending with an error response
		c.pendMu.Lock()
		pq := c.pendQ
		c.pendQ = nil
		c.pendMu.Unlock()
		errResp := &Response{Error: "connection closed"}
		for _, p := range pq {
			select {
			case p.resp <- errResp:
			default:
			}
		}
		close(c.pushCh)
	}()
	for {
		_, msg, err := c.ws.ReadMessage()
		if err != nil {
			return
		}
		var decoded interface{}
		if err := json.Unmarshal(msg, &decoded); err != nil {
			continue
		}

		resp, pm, isPush := parseIncomingFrame(decoded)
		if isPush && pm != nil {
			select {
			case c.pushCh <- pm:
			default:
			}
			continue
		}
		if resp == nil {
			continue
		}

		// dequeue the next pending request
		c.pendMu.Lock()
		if len(c.pendQ) == 0 {
			c.pendMu.Unlock()
			continue
		}
		p := c.pendQ[0]
		c.pendQ = c.pendQ[1:]
		c.pendMu.Unlock()
		select {
		case p.resp <- resp:
		default:
		}
	}
}

func parseIncomingFrame(decoded interface{}) (*Response, *PushMessage, bool) {
	switch frame := decoded.(type) {
	case map[string]interface{}:
		if t, ok := frame["type"].(string); ok {
			if t == "push" {
				return nil, parsePushFrame(frame["data"]), true
			}
			if isPubSubType(t) {
				return nil, &PushMessage{
					Type:    t,
					Channel: fmt.Sprintf("%v", frame["channel"]),
					Pattern: fmt.Sprintf("%v", frame["pattern"]),
					Data:    fmt.Sprintf("%v", frame["data"]),
				}, true
			}
		}

		if errText, ok := frame["error"].(string); ok && errText != "" {
			return &Response{Error: errText}, nil, false
		}

		if result, ok := frame["result"]; ok {
			return &Response{Result: result}, nil, false
		}
		if data, ok := frame["data"]; ok {
			return &Response{Result: data}, nil, false
		}
		if len(frame) == 0 {
			return &Response{Result: nil}, nil, false
		}
		return &Response{Result: frame}, nil, false
	case []interface{}:
		if pm := parsePushFrame(frame); pm != nil {
			return nil, pm, true
		}
		return &Response{Result: frame}, nil, false
	default:
		return &Response{Result: frame}, nil, false
	}
}

func parsePushFrame(payload interface{}) *PushMessage {
	arr, ok := payload.([]interface{})
	if !ok || len(arr) == 0 {
		return nil
	}
	typ := fmt.Sprintf("%v", arr[0])
	if !isPubSubType(typ) {
		return nil
	}

	pm := &PushMessage{Type: typ}
	if len(arr) >= 2 {
		pm.Channel = fmt.Sprintf("%v", arr[1])
	}
	if len(arr) >= 3 {
		pm.Data = fmt.Sprintf("%v", arr[2])
	}
	if typ == "pmessage" {
		if len(arr) >= 2 {
			pm.Pattern = fmt.Sprintf("%v", arr[1])
		}
		if len(arr) >= 3 {
			pm.Channel = fmt.Sprintf("%v", arr[2])
		}
		if len(arr) >= 4 {
			pm.Data = fmt.Sprintf("%v", arr[3])
		}
	}
	return pm
}

// Send sends cmd+args and returns the server response.
func (c *Conn) Send(ctx context.Context, cmd string, args []string) (*Response, error) {
	if c.closed.Load() {
		return nil, fmt.Errorf("connection closed")
	}
	if args == nil {
		args = []string{}
	}
	f := Frame{Cmd: cmd, Args: args}
	data, err := json.Marshal(f)
	if err != nil {
		return nil, err
	}
	p := &pending{resp: make(chan *Response, 1)}
	c.pendMu.Lock()
	c.pendQ = append(c.pendQ, p)
	c.pendMu.Unlock()

	c.mu.Lock()
	err = c.ws.WriteMessage(websocket.TextMessage, data)
	c.mu.Unlock()
	if err != nil {
		// remove from queue
		c.pendMu.Lock()
		for i, pp := range c.pendQ {
			if pp == p {
				c.pendQ = append(c.pendQ[:i], c.pendQ[i+1:]...)
				break
			}
		}
		c.pendMu.Unlock()
		return nil, err
	}
	select {
	case <-ctx.Done():
		// remove from queue
		c.pendMu.Lock()
		for i, pp := range c.pendQ {
			if pp == p {
				c.pendQ = append(c.pendQ[:i], c.pendQ[i+1:]...)
				break
			}
		}
		c.pendMu.Unlock()
		return nil, ctx.Err()
	case r := <-p.resp:
		return r, nil
	}
}

// PushMessages returns the channel for incoming pub/sub push frames.
func (c *Conn) PushMessages() <-chan *PushMessage {
	return c.pushCh
}

// Write sends cmd+args without waiting for a response (fire-and-forget).
// Used for pub/sub commands where confirmations arrive as push messages.
func (c *Conn) Write(cmd string, args []string) error {
	if c.closed.Load() {
		return fmt.Errorf("connection closed")
	}
	if args == nil {
		args = []string{}
	}
	f := Frame{Cmd: cmd, Args: args}
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	c.mu.Lock()
	err = c.ws.WriteMessage(websocket.TextMessage, data)
	c.mu.Unlock()
	return err
}

// Close closes the underlying websocket.
func (c *Conn) Close() error {
	var err error
	c.once.Do(func() {
		err = c.ws.Close()
	})
	return err
}

// IsClosed reports whether the connection is closed.
func (c *Conn) IsClosed() bool {
	return c.closed.Load()
}
