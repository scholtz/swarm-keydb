package swarmkeydb

import (
	"context"
	"fmt"
	"sync"

	"github.com/scholtz/swarm-keydb/sdk/go/internal/transport"
)

// Pipeliner queues commands and flushes them in a single round-trip.
type Pipeliner interface {
	// Do queues an arbitrary command.
	Do(ctx context.Context, args ...interface{}) *Cmd
	// Exec flushes all queued commands.
	Exec(ctx context.Context) ([]Cmder, error)
}

type pipelineCmd struct {
	cmd  string
	args []string
	res  *Cmd
}

// Pipeline accumulates commands and executes them as a MULTI/EXEC transaction.
type Pipeline struct {
	client *Client
	mu     sync.Mutex
	cmds   []*pipelineCmd
}

// newPipeline creates a new Pipeline for client.
func newPipeline(c *Client) *Pipeline {
	return &Pipeline{client: c}
}

// Do queues an arbitrary command.
func (p *Pipeline) Do(ctx context.Context, args ...interface{}) *Cmd {
	if len(args) == 0 {
		cmd := newErrCmd(fmt.Errorf("swarmkeydb: Do requires at least one argument"))
		return cmd
	}
	name, ok := args[0].(string)
	if !ok {
		cmd := newErrCmd(fmt.Errorf("swarmkeydb: first Do argument must be a string"))
		return cmd
	}
	strArgs := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		strArgs = append(strArgs, fmt.Sprintf("%v", a))
	}
	c := &Cmd{}
	p.mu.Lock()
	p.cmds = append(p.cmds, &pipelineCmd{cmd: name, args: strArgs, res: c})
	p.mu.Unlock()
	return c
}

// Exec flushes all queued commands using MULTI/EXEC.
func (p *Pipeline) Exec(ctx context.Context) ([]Cmder, error) {
	p.mu.Lock()
	cmds := p.cmds
	p.cmds = nil
	p.mu.Unlock()

	if len(cmds) == 0 {
		return nil, nil
	}

	pc, err := p.client.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer p.client.release(pc)

	// MULTI
	if _, err := pc.conn.Send(ctx, "MULTI", nil); err != nil {
		return nil, err
	}
	// Queue commands
	for _, c := range cmds {
		if _, err := pc.conn.Send(ctx, c.cmd, c.args); err != nil {
			// DISCARD on error
			_, _ = pc.conn.Send(ctx, "DISCARD", nil)
			return nil, err
		}
	}
	// EXEC
	execResp, err := pc.conn.Send(ctx, "EXEC", nil)
	if err != nil {
		return nil, err
	}
	if execResp.Error != "" {
		return nil, &CommandError{Command: "EXEC", Msg: execResp.Error}
	}
	if execResp.Result == nil {
		return nil, WatchConflictError
	}
	arr, ok := execResp.Result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("swarmkeydb: unexpected EXEC result type %T", execResp.Result)
	}

	result := make([]Cmder, len(cmds))
	for i, c := range cmds {
		if i < len(arr) {
			if arr[i] == nil {
				c.res.val = nil
			} else {
				c.res.val = arr[i]
			}
		} else {
			c.res.err = fmt.Errorf("swarmkeydb: missing EXEC result for command %d", i)
		}
		result[i] = c.res
	}
	return result, nil
}

// TxPipelined executes fn inside a MULTI/EXEC transaction and returns results.
func (c *Client) TxPipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	p := newPipeline(c)
	if err := fn(p); err != nil {
		return nil, err
	}
	return p.Exec(ctx)
}

// Tx represents an active transaction (for Watch).
type Tx struct {
	client  *Client
	conn    *transport.Conn
	watched []string
}

// TxPipelined executes fn inside the transaction.
func (tx *Tx) TxPipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	p := &txPipeline{tx: tx}
	if err := fn(p); err != nil {
		return nil, err
	}
	return p.exec(ctx)
}

// txPipeline is a Pipeliner backed by a Tx connection.
type txPipeline struct {
	tx   *Tx
	mu   sync.Mutex
	cmds []*pipelineCmd
}

func (p *txPipeline) Do(ctx context.Context, args ...interface{}) *Cmd {
	if len(args) == 0 {
		return newErrCmd(fmt.Errorf("swarmkeydb: Do requires at least one argument"))
	}
	name, ok := args[0].(string)
	if !ok {
		return newErrCmd(fmt.Errorf("swarmkeydb: first argument must be a string"))
	}
	strArgs := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		strArgs = append(strArgs, fmt.Sprintf("%v", a))
	}
	c := &Cmd{}
	p.mu.Lock()
	p.cmds = append(p.cmds, &pipelineCmd{cmd: name, args: strArgs, res: c})
	p.mu.Unlock()
	return c
}

func (p *txPipeline) Exec(ctx context.Context) ([]Cmder, error) {
	return p.exec(ctx)
}

func (p *txPipeline) exec(ctx context.Context) ([]Cmder, error) {
	p.mu.Lock()
	cmds := p.cmds
	p.cmds = nil
	p.mu.Unlock()

	conn := p.tx.conn

	if _, err := conn.Send(ctx, "MULTI", nil); err != nil {
		return nil, err
	}
	for _, c := range cmds {
		if _, err := conn.Send(ctx, c.cmd, c.args); err != nil {
			_, _ = conn.Send(ctx, "DISCARD", nil)
			return nil, err
		}
	}
	execResp, err := conn.Send(ctx, "EXEC", nil)
	if err != nil {
		return nil, err
	}
	if execResp.Error != "" {
		return nil, &CommandError{Command: "EXEC", Msg: execResp.Error}
	}
	if execResp.Result == nil {
		return nil, WatchConflictError
	}
	arr, _ := execResp.Result.([]interface{})
	result := make([]Cmder, len(cmds))
	for i, c := range cmds {
		if i < len(arr) {
			c.res.val = arr[i]
		}
		result[i] = c.res
	}
	return result, nil
}

// Watch executes fn with an optimistic lock on keys.
// If any of the watched keys are modified before EXEC, fn is retried (up to MaxRetries).
func (c *Client) Watch(ctx context.Context, fn func(*Tx) error, keys ...string) error {
	for attempt := 0; attempt <= c.opts.MaxRetries; attempt++ {
		conn, err := transport.Dial(ctx, transport.DialOptions{
			Addr:        c.opts.Addr,
			DialTimeout: c.opts.DialTimeout,
			ReadTimeout: c.opts.ReadTimeout,
		})
		if err != nil {
			return &ConnectionError{Msg: "watch dial failed", Cause: err}
		}
		if c.opts.Password != "" {
			resp, err := conn.Send(ctx, "AUTH", []string{c.opts.Password})
			if err != nil || (resp != nil && resp.Error != "") {
				_ = conn.Close()
				if resp != nil && resp.Error != "" {
					return &AuthError{Msg: resp.Error}
				}
				return err
			}
		}
		if len(keys) > 0 {
			if _, err := conn.Send(ctx, "WATCH", keys); err != nil {
				_ = conn.Close()
				return err
			}
		}
		tx := &Tx{client: c, conn: conn, watched: keys}
		err = fn(tx)
		_ = conn.Close()
		if err == WatchConflictError {
			continue
		}
		return err
	}
	return WatchConflictError
}
