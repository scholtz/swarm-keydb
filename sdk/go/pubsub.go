package swarmkeydb

import (
	"context"
	"sync"

	"github.com/scholtz/swarm-keydb/sdk/go/internal/transport"
)

// Message is a pub/sub message received from SwarmKeyDb.
type Message struct {
	// Type is "message" or "pmessage".
	Type string
	// Channel is the channel the message was published to.
	Channel string
	// Pattern is the pattern that matched (for pmessage).
	Pattern string
	// Payload is the message payload.
	Payload string
}

// ChannelOption configures the PubSub Channel method.
type ChannelOption func(*channelOptions)

type channelOptions struct {
	bufSize int
}

// WithChannelSize sets the message channel buffer size.
func WithChannelSize(n int) ChannelOption {
	return func(o *channelOptions) { o.bufSize = n }
}

// PubSub manages a pub/sub subscription on its own dedicated WebSocket connection.
type PubSub struct {
	client   *Client
	conn     *transport.Conn
	mu       sync.Mutex
	channels map[string]struct{}
	patterns map[string]struct{}
	msgCh    chan *Message
	quit     chan struct{}
	once     sync.Once
}

func newPubSub(client *Client, conn *transport.Conn) *PubSub {
	ps := &PubSub{
		client:   client,
		conn:     conn,
		channels: make(map[string]struct{}),
		patterns: make(map[string]struct{}),
		msgCh:    make(chan *Message, 256),
		quit:     make(chan struct{}),
	}
	go ps.readLoop()
	return ps
}

func (ps *PubSub) readLoop() {
	pushCh := ps.conn.PushMessages()
	for {
		select {
		case <-ps.quit:
			return
		case pm, ok := <-pushCh:
			if !ok {
				return
			}
			if pm.Type != "message" && pm.Type != "pmessage" {
				continue
			}
			msg := &Message{
				Type:    pm.Type,
				Channel: pm.Channel,
				Pattern: pm.Pattern,
				Payload: pm.Data,
			}
			select {
			case ps.msgCh <- msg:
			default:
			}
		}
	}
}

// Channel returns a Go channel that receives pub/sub messages.
func (ps *PubSub) Channel(opts ...ChannelOption) <-chan *Message {
	o := &channelOptions{bufSize: 256}
	for _, opt := range opts {
		opt(o)
	}
	ch := make(chan *Message, o.bufSize)
	go func() {
		defer close(ch)
		for {
			select {
			case <-ps.quit:
				return
			case msg, ok := <-ps.msgCh:
				if !ok {
					return
				}
				select {
				case ch <- msg:
				case <-ps.quit:
					return
				}
			}
		}
	}()
	return ch
}

// Subscribe subscribes to additional channels.
func (ps *PubSub) Subscribe(ctx context.Context, channels ...string) error {
	ps.mu.Lock()
	for _, ch := range channels {
		ps.channels[ch] = struct{}{}
	}
	ps.mu.Unlock()
	return ps.conn.Write("SUBSCRIBE", channels)
}

// Unsubscribe unsubscribes from channels.
func (ps *PubSub) Unsubscribe(ctx context.Context, channels ...string) error {
	ps.mu.Lock()
	for _, ch := range channels {
		delete(ps.channels, ch)
	}
	ps.mu.Unlock()
	return ps.conn.Write("UNSUBSCRIBE", channels)
}

// PSubscribe subscribes to patterns.
func (ps *PubSub) PSubscribe(ctx context.Context, patterns ...string) error {
	ps.mu.Lock()
	for _, p := range patterns {
		ps.patterns[p] = struct{}{}
	}
	ps.mu.Unlock()
	return ps.conn.Write("PSUBSCRIBE", patterns)
}

// PUnsubscribe unsubscribes from patterns.
func (ps *PubSub) PUnsubscribe(ctx context.Context, patterns ...string) error {
	ps.mu.Lock()
	for _, p := range patterns {
		delete(ps.patterns, p)
	}
	ps.mu.Unlock()
	return ps.conn.Write("PUNSUBSCRIBE", patterns)
}

// Close closes the pub/sub subscription.
func (ps *PubSub) Close() error {
	var err error
	ps.once.Do(func() {
		close(ps.quit)
		err = ps.conn.Close()
	})
	return err
}

// Subscribe creates a new PubSub subscription for the given channels.
func (c *Client) Subscribe(ctx context.Context, channels ...string) (*PubSub, error) {
	conn, err := transport.Dial(ctx, transport.DialOptions{
		Addr:        c.opts.Addr,
		DialTimeout: c.opts.DialTimeout,
		ReadTimeout: c.opts.ReadTimeout,
	})
	if err != nil {
		return nil, &ConnectionError{Msg: "subscribe dial failed", Cause: err}
	}
	if c.opts.Password != "" {
		resp, err := conn.Send(ctx, "AUTH", []string{c.opts.Password})
		if err != nil || resp.Error != "" {
			_ = conn.Close()
			if resp != nil && resp.Error != "" {
				return nil, &AuthError{Msg: resp.Error}
			}
			return nil, &ConnectionError{Msg: "subscribe auth failed", Cause: err}
		}
	}
	ps := newPubSub(c, conn)
	if err := ps.Subscribe(ctx, channels...); err != nil {
		_ = ps.Close()
		return nil, err
	}
	return ps, nil
}

// PSubscribe creates a new PubSub subscription for the given patterns.
func (c *Client) PSubscribe(ctx context.Context, patterns ...string) (*PubSub, error) {
	conn, err := transport.Dial(ctx, transport.DialOptions{
		Addr:        c.opts.Addr,
		DialTimeout: c.opts.DialTimeout,
		ReadTimeout: c.opts.ReadTimeout,
	})
	if err != nil {
		return nil, &ConnectionError{Msg: "psubscribe dial failed", Cause: err}
	}
	if c.opts.Password != "" {
		resp, err := conn.Send(ctx, "AUTH", []string{c.opts.Password})
		if err != nil || resp.Error != "" {
			_ = conn.Close()
			if resp != nil && resp.Error != "" {
				return nil, &AuthError{Msg: resp.Error}
			}
			return nil, &ConnectionError{Msg: "psubscribe auth failed", Cause: err}
		}
	}
	ps := newPubSub(c, conn)
	if err := ps.PSubscribe(ctx, patterns...); err != nil {
		_ = ps.Close()
		return nil, err
	}
	return ps, nil
}
