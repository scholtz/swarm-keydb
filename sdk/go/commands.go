package swarmkeydb

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// ---------------------------------------------------------------------------
// Core / Key commands
// ---------------------------------------------------------------------------

// Ping sends a PING command and returns PONG.
func (c *Client) Ping(ctx context.Context) *StatusCmd {
	return toStatusCmd(c.do(ctx, "PING", nil))
}

// Get returns the value of key.
func (c *Client) Get(ctx context.Context, key string) *StringCmd {
	return toStringCmd(c.do(ctx, "GET", []string{key}))
}

// Set sets key to value. expiration 0 means no expiry.
func (c *Client) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *StatusCmd {
	args := []string{key, fmt.Sprintf("%v", value)}
	if expiration > 0 {
		if expiration < time.Second {
			args = append(args, "PX", strconv.FormatInt(expiration.Milliseconds(), 10))
		} else {
			args = append(args, "EX", strconv.FormatInt(int64(expiration.Seconds()), 10))
		}
	}
	return toStatusCmd(c.do(ctx, "SET", args))
}

// SetNX sets key to value only if the key does not exist.
func (c *Client) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *BoolCmd {
	args := []string{key, fmt.Sprintf("%v", value), "NX"}
	if expiration > 0 {
		if expiration < time.Second {
			args = append(args, "PX", strconv.FormatInt(expiration.Milliseconds(), 10))
		} else {
			args = append(args, "EX", strconv.FormatInt(int64(expiration.Seconds()), 10))
		}
	}
	resp, err := c.do(ctx, "SET", args)
	if err != nil {
		return &BoolCmd{err: err}
	}
	return &BoolCmd{val: resp.Result != nil}
}

// SetXX sets key to value only if the key already exists.
func (c *Client) SetXX(ctx context.Context, key string, value interface{}, expiration time.Duration) *BoolCmd {
	args := []string{key, fmt.Sprintf("%v", value), "XX"}
	if expiration > 0 {
		if expiration < time.Second {
			args = append(args, "PX", strconv.FormatInt(expiration.Milliseconds(), 10))
		} else {
			args = append(args, "EX", strconv.FormatInt(int64(expiration.Seconds()), 10))
		}
	}
	resp, err := c.do(ctx, "SET", args)
	if err != nil {
		return &BoolCmd{err: err}
	}
	return &BoolCmd{val: resp.Result != nil}
}

// GetSet atomically sets key to value and returns the old value.
func (c *Client) GetSet(ctx context.Context, key string, value interface{}) *StringCmd {
	return toStringCmd(c.do(ctx, "GETSET", []string{key, fmt.Sprintf("%v", value)}))
}

// Del deletes one or more keys and returns the number of keys deleted.
func (c *Client) Del(ctx context.Context, keys ...string) *IntCmd {
	return toIntCmd(c.do(ctx, "DEL", keys))
}

// Exists returns the number of keys that exist.
func (c *Client) Exists(ctx context.Context, keys ...string) *IntCmd {
	return toIntCmd(c.do(ctx, "EXISTS", keys))
}

// Expire sets a timeout on key in seconds.
func (c *Client) Expire(ctx context.Context, key string, expiration time.Duration) *BoolCmd {
	return toBoolCmd(c.do(ctx, "EXPIRE", []string{key, strconv.FormatInt(int64(expiration.Seconds()), 10)}))
}

// PExpire sets a timeout on key in milliseconds.
func (c *Client) PExpire(ctx context.Context, key string, expiration time.Duration) *BoolCmd {
	return toBoolCmd(c.do(ctx, "PEXPIRE", []string{key, strconv.FormatInt(expiration.Milliseconds(), 10)}))
}

// TTL returns the remaining TTL of key in seconds (-1 no TTL, -2 not found).
func (c *Client) TTL(ctx context.Context, key string) *DurationCmd {
	resp, err := c.do(ctx, "TTL", []string{key})
	if err != nil {
		return &DurationCmd{err: err}
	}
	n := toInt64(resp.Result)
	if n < 0 {
		return &DurationCmd{val: time.Duration(n)}
	}
	return &DurationCmd{val: time.Duration(n) * time.Second}
}

// PTTL returns the remaining TTL of key in milliseconds.
func (c *Client) PTTL(ctx context.Context, key string) *DurationCmd {
	resp, err := c.do(ctx, "PTTL", []string{key})
	if err != nil {
		return &DurationCmd{err: err}
	}
	n := toInt64(resp.Result)
	if n < 0 {
		return &DurationCmd{val: time.Duration(n)}
	}
	return &DurationCmd{val: time.Duration(n) * time.Millisecond}
}

// Persist removes the TTL from key.
func (c *Client) Persist(ctx context.Context, key string) *BoolCmd {
	return toBoolCmd(c.do(ctx, "PERSIST", []string{key}))
}

// Keys returns all keys matching pattern.
func (c *Client) Keys(ctx context.Context, pattern string) *StringSliceCmd {
	return toStringSliceCmd(c.do(ctx, "KEYS", []string{pattern}))
}

// Scan incrementally iterates over keys.
func (c *Client) Scan(ctx context.Context, cursor uint64, match string, count int64) *ScanCmd {
	args := []string{strconv.FormatUint(cursor, 10)}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	if count > 0 {
		args = append(args, "COUNT", strconv.FormatInt(count, 10))
	}
	resp, err := c.do(ctx, "SCAN", args)
	if err != nil {
		return &ScanCmd{err: err}
	}
	arr, ok := resp.Result.([]interface{})
	if !ok || len(arr) < 2 {
		return &ScanCmd{err: fmt.Errorf("swarmkeydb: unexpected SCAN result")}
	}
	cur, _ := strconv.ParseUint(fmt.Sprintf("%v", arr[0]), 10, 64)
	keysArr, _ := arr[1].([]interface{})
	keys := make([]string, len(keysArr))
	for i, k := range keysArr {
		keys[i] = fmt.Sprintf("%v", k)
	}
	return &ScanCmd{cursor: cur, keys: keys}
}

// Type returns the type of the value stored at key.
func (c *Client) Type(ctx context.Context, key string) *StatusCmd {
	return toStatusCmd(c.do(ctx, "TYPE", []string{key}))
}

// Rename renames key to newkey.
func (c *Client) Rename(ctx context.Context, key, newkey string) *StatusCmd {
	return toStatusCmd(c.do(ctx, "RENAME", []string{key, newkey}))
}

// RenameNX renames key to newkey only if newkey does not exist.
func (c *Client) RenameNX(ctx context.Context, key, newkey string) *BoolCmd {
	return toBoolCmd(c.do(ctx, "RENAMENX", []string{key, newkey}))
}

// FlushDB deletes all keys in the current database.
func (c *Client) FlushDB(ctx context.Context) *StatusCmd {
	return toStatusCmd(c.do(ctx, "FLUSHDB", nil))
}

// ---------------------------------------------------------------------------
// String commands
// ---------------------------------------------------------------------------

// Append appends value to the string stored at key and returns the new length.
func (c *Client) Append(ctx context.Context, key, value string) *IntCmd {
	return toIntCmd(c.do(ctx, "APPEND", []string{key, value}))
}

// GetRange returns a substring of the string at key.
func (c *Client) GetRange(ctx context.Context, key string, start, end int64) *StringCmd {
	return toStringCmd(c.do(ctx, "GETRANGE", []string{key,
		strconv.FormatInt(start, 10), strconv.FormatInt(end, 10)}))
}

// SetRange overwrites part of the string at key starting at offset.
func (c *Client) SetRange(ctx context.Context, key string, offset int64, value string) *IntCmd {
	return toIntCmd(c.do(ctx, "SETRANGE", []string{key, strconv.FormatInt(offset, 10), value}))
}

// Incr increments the integer value of key by 1.
func (c *Client) Incr(ctx context.Context, key string) *IntCmd {
	return toIntCmd(c.do(ctx, "INCR", []string{key}))
}

// IncrBy increments the integer value of key by value.
func (c *Client) IncrBy(ctx context.Context, key string, value int64) *IntCmd {
	return toIntCmd(c.do(ctx, "INCRBY", []string{key, strconv.FormatInt(value, 10)}))
}

// IncrByFloat increments the float value of key by value.
func (c *Client) IncrByFloat(ctx context.Context, key string, value float64) *FloatCmd {
	return toFloatCmd(c.do(ctx, "INCRBYFLOAT", []string{key, strconv.FormatFloat(value, 'f', -1, 64)}))
}

// Decr decrements the integer value of key by 1.
func (c *Client) Decr(ctx context.Context, key string) *IntCmd {
	return toIntCmd(c.do(ctx, "DECR", []string{key}))
}

// DecrBy decrements the integer value of key by value.
func (c *Client) DecrBy(ctx context.Context, key string, value int64) *IntCmd {
	return toIntCmd(c.do(ctx, "DECRBY", []string{key, strconv.FormatInt(value, 10)}))
}

// StrLen returns the length of the string value stored at key.
func (c *Client) StrLen(ctx context.Context, key string) *IntCmd {
	return toIntCmd(c.do(ctx, "STRLEN", []string{key}))
}

// MGet returns the values of all specified keys.
func (c *Client) MGet(ctx context.Context, keys ...string) *SliceCmd {
	return toSliceCmd(c.do(ctx, "MGET", keys))
}

// MSet sets multiple key-value pairs. keysvalues must be alternating keys and values.
func (c *Client) MSet(ctx context.Context, keysvalues ...interface{}) *StatusCmd {
	args := make([]string, len(keysvalues))
	for i, kv := range keysvalues {
		args[i] = fmt.Sprintf("%v", kv)
	}
	return toStatusCmd(c.do(ctx, "MSET", args))
}

// Publish publishes a message to a channel and returns the number of subscribers that received it.
func (c *Client) Publish(ctx context.Context, channel, message string) *IntCmd {
	return toIntCmd(c.do(ctx, "PUBLISH", []string{channel, message}))
}
