package swarmkeydb

import (
	"context"
	"fmt"
	"strconv"
)

// XMessage represents a single stream entry.
type XMessage struct {
	// ID is the stream entry ID (e.g. "1526919030474-55").
	ID string
	// Values are the entry fields.
	Values map[string]interface{}
}

// XStream represents a stream returned by XREAD / XREADGROUP.
type XStream struct {
	Stream   string
	Messages []XMessage
}

// XPending holds summary pending information for a consumer group.
type XPending struct {
	Count     int64
	Lower     string
	Higher    string
	Consumers map[string]int64
}

// XAddArgs configures an XADD command.
type XAddArgs struct {
	Stream string
	MaxLen int64  // MAXLEN trimming; 0 means no trim
	Approx bool   // use ~ approximate trimming
	MinID  string // MINID trimming
	ID     string // entry ID; "" or "*" for auto-generate
	Values []interface{}
}

// XReadArgs configures an XREAD command.
type XReadArgs struct {
	Streams []string // alternating: stream1, stream2, id1, id2
	Count   int64
	Block   int64 // milliseconds; 0 means non-blocking
}

// XReadGroupArgs configures an XREADGROUP command.
type XReadGroupArgs struct {
	Group    string
	Consumer string
	Streams  []string
	Count    int64
	Block    int64
	NoAck    bool
}

// XClaimArgs configures an XCLAIM command.
type XClaimArgs struct {
	Stream   string
	Group    string
	Consumer string
	MinIdle  int64 // milliseconds
	Messages []string
}

// XMessageSliceCmd holds the result of stream range commands.
type XMessageSliceCmd struct {
	val []XMessage
	err error
}

// Err returns any error.
func (c *XMessageSliceCmd) Err() error { return c.err }

// Result returns the messages and error.
func (c *XMessageSliceCmd) Result() ([]XMessage, error) { return c.val, c.err }

// Val returns the messages, nil on error.
func (c *XMessageSliceCmd) Val() []XMessage { return c.val }

// XStreamSliceCmd holds the result of XREAD / XREADGROUP.
type XStreamSliceCmd struct {
	val []XStream
	err error
}

// Err returns any error.
func (c *XStreamSliceCmd) Err() error { return c.err }

// Result returns the streams and error.
func (c *XStreamSliceCmd) Result() ([]XStream, error) { return c.val, c.err }

// Val returns the streams, nil on error.
func (c *XStreamSliceCmd) Val() []XStream { return c.val }

// XPendingCmd holds the result of XPENDING.
type XPendingCmd struct {
	val *XPending
	err error
}

// Err returns any error.
func (c *XPendingCmd) Err() error { return c.err }

// Result returns the pending summary and error.
func (c *XPendingCmd) Result() (*XPending, error) { return c.val, c.err }

// Val returns the pending summary, nil on error.
func (c *XPendingCmd) Val() *XPending { return c.val }

// ---------------------------------------------------------------------------
// Stream commands
// ---------------------------------------------------------------------------

// XAdd appends a new entry to a stream and returns the entry ID.
func (c *Client) XAdd(ctx context.Context, args *XAddArgs) *StringCmd {
	a := []string{args.Stream}
	if args.MaxLen > 0 {
		a = append(a, "MAXLEN")
		if args.Approx {
			a = append(a, "~")
		}
		a = append(a, strconv.FormatInt(args.MaxLen, 10))
	} else if args.MinID != "" {
		a = append(a, "MINID")
		if args.Approx {
			a = append(a, "~")
		}
		a = append(a, args.MinID)
	}
	id := args.ID
	if id == "" {
		id = "*"
	}
	a = append(a, id)
	for _, v := range args.Values {
		a = append(a, fmt.Sprintf("%v", v))
	}
	return toStringCmd(c.do(ctx, "XADD", a))
}

// XLen returns the number of entries in a stream.
func (c *Client) XLen(ctx context.Context, stream string) *IntCmd {
	return toIntCmd(c.do(ctx, "XLEN", []string{stream}))
}

// XRange returns a range of entries from a stream.
func (c *Client) XRange(ctx context.Context, stream, start, stop string) *XMessageSliceCmd {
	return c.xRange(ctx, stream, start, stop, 0)
}

// XRangeN returns up to count entries from a stream range.
func (c *Client) XRangeN(ctx context.Context, stream, start, stop string, count int64) *XMessageSliceCmd {
	return c.xRange(ctx, stream, start, stop, count)
}

func (c *Client) xRange(ctx context.Context, stream, start, stop string, count int64) *XMessageSliceCmd {
	args := []string{stream, start, stop}
	if count > 0 {
		args = append(args, "COUNT", strconv.FormatInt(count, 10))
	}
	resp, err := c.do(ctx, "XRANGE", args)
	if err != nil {
		return &XMessageSliceCmd{err: err}
	}
	msgs, parseErr := parseXMessages(resp.Result)
	return &XMessageSliceCmd{val: msgs, err: parseErr}
}

// XRevRange returns entries from a stream in reverse order.
func (c *Client) XRevRange(ctx context.Context, stream, stop, start string) *XMessageSliceCmd {
	return c.xRevRange(ctx, stream, stop, start, 0)
}

// XRevRangeN returns up to count entries from a stream in reverse order.
func (c *Client) XRevRangeN(ctx context.Context, stream, stop, start string, count int64) *XMessageSliceCmd {
	return c.xRevRange(ctx, stream, stop, start, count)
}

func (c *Client) xRevRange(ctx context.Context, stream, stop, start string, count int64) *XMessageSliceCmd {
	args := []string{stream, stop, start}
	if count > 0 {
		args = append(args, "COUNT", strconv.FormatInt(count, 10))
	}
	resp, err := c.do(ctx, "XREVRANGE", args)
	if err != nil {
		return &XMessageSliceCmd{err: err}
	}
	msgs, parseErr := parseXMessages(resp.Result)
	return &XMessageSliceCmd{val: msgs, err: parseErr}
}

// XRead reads from one or more streams.
func (c *Client) XRead(ctx context.Context, args *XReadArgs) *XStreamSliceCmd {
	a := []string{}
	if args.Count > 0 {
		a = append(a, "COUNT", strconv.FormatInt(args.Count, 10))
	}
	if args.Block >= 0 {
		a = append(a, "BLOCK", strconv.FormatInt(args.Block, 10))
	}
	a = append(a, "STREAMS")
	a = append(a, args.Streams...)
	resp, err := c.do(ctx, "XREAD", a)
	if err != nil {
		return &XStreamSliceCmd{err: err}
	}
	streams, parseErr := parseXStreams(resp.Result)
	return &XStreamSliceCmd{val: streams, err: parseErr}
}

// XReadGroup reads from a consumer group.
func (c *Client) XReadGroup(ctx context.Context, args *XReadGroupArgs) *XStreamSliceCmd {
	a := []string{"GROUP", args.Group, args.Consumer}
	if args.Count > 0 {
		a = append(a, "COUNT", strconv.FormatInt(args.Count, 10))
	}
	if args.Block >= 0 {
		a = append(a, "BLOCK", strconv.FormatInt(args.Block, 10))
	}
	if args.NoAck {
		a = append(a, "NOACK")
	}
	a = append(a, "STREAMS")
	a = append(a, args.Streams...)
	resp, err := c.do(ctx, "XREADGROUP", a)
	if err != nil {
		return &XStreamSliceCmd{err: err}
	}
	streams, parseErr := parseXStreams(resp.Result)
	return &XStreamSliceCmd{val: streams, err: parseErr}
}

// XAck acknowledges one or more messages in a consumer group.
func (c *Client) XAck(ctx context.Context, stream, group string, ids ...string) *IntCmd {
	args := append([]string{stream, group}, ids...)
	return toIntCmd(c.do(ctx, "XACK", args))
}

// XTrim trims a stream to a maximum length.
func (c *Client) XTrim(ctx context.Context, key string, maxLen int64) *IntCmd {
	return toIntCmd(c.do(ctx, "XTRIM", []string{key, "MAXLEN", strconv.FormatInt(maxLen, 10)}))
}

// XTrimApprox trims a stream approximately.
func (c *Client) XTrimApprox(ctx context.Context, key string, maxLen int64) *IntCmd {
	return toIntCmd(c.do(ctx, "XTRIM", []string{key, "MAXLEN", "~", strconv.FormatInt(maxLen, 10)}))
}

// XGroupCreate creates a consumer group.
func (c *Client) XGroupCreate(ctx context.Context, stream, group, start string) *StatusCmd {
	return toStatusCmd(c.do(ctx, "XGROUP", []string{"CREATE", stream, group, start}))
}

// XGroupCreateMkStream creates a consumer group, creating the stream if needed.
func (c *Client) XGroupCreateMkStream(ctx context.Context, stream, group, start string) *StatusCmd {
	return toStatusCmd(c.do(ctx, "XGROUP", []string{"CREATE", stream, group, start, "MKSTREAM"}))
}

// XGroupSetID sets the last delivered ID for a consumer group.
func (c *Client) XGroupSetID(ctx context.Context, stream, group, start string) *StatusCmd {
	return toStatusCmd(c.do(ctx, "XGROUP", []string{"SETID", stream, group, start}))
}

// XGroupDestroy deletes a consumer group.
func (c *Client) XGroupDestroy(ctx context.Context, stream, group string) *IntCmd {
	return toIntCmd(c.do(ctx, "XGROUP", []string{"DESTROY", stream, group}))
}

// XPending returns pending message summary for a consumer group.
func (c *Client) XPending(ctx context.Context, stream, group string) *XPendingCmd {
	resp, err := c.do(ctx, "XPENDING", []string{stream, group})
	if err != nil {
		return &XPendingCmd{err: err}
	}
	arr, ok := resp.Result.([]interface{})
	if !ok || len(arr) < 4 {
		return &XPendingCmd{err: fmt.Errorf("swarmkeydb: unexpected XPENDING result")}
	}
	p := &XPending{
		Count:     toInt64(arr[0]),
		Lower:     fmt.Sprintf("%v", arr[1]),
		Higher:    fmt.Sprintf("%v", arr[2]),
		Consumers: make(map[string]int64),
	}
	if consumers, ok := arr[3].([]interface{}); ok {
		for _, c := range consumers {
			if pair, ok := c.([]interface{}); ok && len(pair) >= 2 {
				p.Consumers[fmt.Sprintf("%v", pair[0])] = toInt64(pair[1])
			}
		}
	}
	return &XPendingCmd{val: p}
}

// XClaim claims ownership of pending messages.
func (c *Client) XClaim(ctx context.Context, args *XClaimArgs) *XMessageSliceCmd {
	a := []string{args.Stream, args.Group, args.Consumer,
		strconv.FormatInt(args.MinIdle, 10)}
	a = append(a, args.Messages...)
	resp, err := c.do(ctx, "XCLAIM", a)
	if err != nil {
		return &XMessageSliceCmd{err: err}
	}
	msgs, parseErr := parseXMessages(resp.Result)
	return &XMessageSliceCmd{val: msgs, err: parseErr}
}

// ---------------------------------------------------------------------------
// Parse helpers
// ---------------------------------------------------------------------------

func parseXMessages(v interface{}) ([]XMessage, error) {
	arr, ok := v.([]interface{})
	if !ok {
		if v == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("swarmkeydb: unexpected stream entry type %T", v)
	}
	msgs := make([]XMessage, 0, len(arr))
	for _, entry := range arr {
		pair, ok := entry.([]interface{})
		if !ok || len(pair) < 2 {
			continue
		}
		id := fmt.Sprintf("%v", pair[0])
		fields, _ := pair[1].([]interface{})
		vals := make(map[string]interface{}, len(fields)/2)
		for i := 0; i+1 < len(fields); i += 2 {
			k := fmt.Sprintf("%v", fields[i])
			vals[k] = fields[i+1]
		}
		msgs = append(msgs, XMessage{ID: id, Values: vals})
	}
	return msgs, nil
}

func parseXStreams(v interface{}) ([]XStream, error) {
	arr, ok := v.([]interface{})
	if !ok {
		if v == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("swarmkeydb: unexpected streams type %T", v)
	}
	streams := make([]XStream, 0, len(arr))
	for _, s := range arr {
		pair, ok := s.([]interface{})
		if !ok || len(pair) < 2 {
			continue
		}
		name := fmt.Sprintf("%v", pair[0])
		msgs, err := parseXMessages(pair[1])
		if err != nil {
			return nil, err
		}
		streams = append(streams, XStream{Stream: name, Messages: msgs})
	}
	return streams, nil
}
