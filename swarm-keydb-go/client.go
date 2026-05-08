package swarmkeydb

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	SetEx(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Keys(ctx context.Context, pattern string) *redis.StringSliceCmd
	MGet(ctx context.Context, keys ...string) *redis.SliceCmd
	MSet(ctx context.Context, values ...interface{}) *redis.StatusCmd
	Do(ctx context.Context, args ...interface{}) *redis.Cmd
}

type Options struct {
	Host          string
	Port          int
	Password      string
	PrivacyMode   PrivacyMode
	PrivacyKeyHex string
	// DidMode sets the DID authentication mode. Defaults to DidAuthModeNone.
	DidMode   DidAuthMode
	// DidRpcUrl is the Ethereum JSON-RPC endpoint used for on-chain DID controller lookups.
	DidRpcUrl string
	// DidMethod is the DID method string, e.g. "ethr" (default).
	DidMethod string
}

// DidAuthMode controls whether DID authentication is required for store operations.
type DidAuthMode string

const (
	// DidAuthModeNone disables DID authentication (default).
	DidAuthModeNone   DidAuthMode = "none"
	// DidAuthModeEthrDid requires callers to authenticate with a did:ethr identity.
	DidAuthModeEthrDid DidAuthMode = "ethr_did"
)

type Client struct {
	redis            RedisClient
	privacyMode      PrivacyMode
	privacyKey       []byte
	tokenToPlain     map[string]string
	privacyConfigErr error
	didMode          DidAuthMode
	didRpcUrl        string
	currentDid       string
}

type PrivacyMode string

const (
	PrivacyModeNone             PrivacyMode = "none"
	PrivacyModeObliviousHashing PrivacyMode = "oblivious_hashing"
	PrivacyModeFullPSI          PrivacyMode = "full_psi"
)

func New(opts Options) *Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", opts.Host, opts.Port),
		Password: opts.Password,
	})

	return newClientWithOptions(&redisAdapter{inner: rdb}, opts)
}

func NewWithRedisClient(r RedisClient) *Client {
	return newClientWithOptions(r, Options{})
}

func NewWithRedisClientAndOptions(r RedisClient, opts Options) *Client {
	return newClientWithOptions(r, opts)
}

func newClientWithOptions(r RedisClient, opts Options) *Client {
	mode := opts.PrivacyMode
	if mode == "" {
		mode = PrivacyModeNone
	}
	var privacyKey []byte
	var privacyConfigErr error
	if mode != PrivacyModeNone {
		privacyKey, privacyConfigErr = hex.DecodeString(opts.PrivacyKeyHex)
	}
	didMode := opts.DidMode
	if didMode == "" {
		didMode = DidAuthModeNone
	}
	return &Client{
		redis:            r,
		privacyMode:      mode,
		privacyKey:       privacyKey,
		tokenToPlain:     map[string]string{},
		privacyConfigErr: privacyConfigErr,
		didMode:          didMode,
		didRpcUrl:        opts.DidRpcUrl,
	}
}

// SetDid registers a decentralized identity (DID) for the current connection.
// When proofMessage and proofSignature are non-empty the server verifies the
// Ethereum personal-sign proof immediately via the AUTHDID command.
// All subsequent operations on this connection are performed under the given DID.
func (c *Client) SetDid(ctx context.Context, did, proofMessage, proofSignature string) error {
	c.currentDid = did
	args := []interface{}{"AUTHDID", did}
	if proofMessage != "" && proofSignature != "" {
		args = append(args, proofMessage, proofSignature)
	}
	return c.redis.Do(ctx, args...).Err()
}

// ClearDid clears the DID context from this client instance.
// Note: the server-side DID binding persists until the connection is closed or
// overwritten with a new AUTHDID call.
func (c *Client) ClearDid() {
	c.currentDid = ""
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("key must be non-empty")
	}
	token, err := c.tokenizeKey(key)
	if err != nil {
		return "", err
	}

	v, err := c.redis.Get(ctx, token).Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrKeyNotFound
	}
	if err != nil {
		return "", fmt.Errorf("get %q: %w", key, err)
	}

	return v, nil
}

func (c *Client) Put(ctx context.Context, key string, value string) error {
	if key == "" {
		return fmt.Errorf("key must be non-empty")
	}
	token, err := c.tokenizeKey(key)
	if err != nil {
		return err
	}

	if err := c.redis.Set(ctx, token, value, 0).Err(); err != nil {
		return fmt.Errorf("put %q: %w", key, err)
	}
	c.rememberKey(token, key)
	return nil
}

func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	if key == "" {
		return false, fmt.Errorf("key must be non-empty")
	}
	token, err := c.tokenizeKey(key)
	if err != nil {
		return false, err
	}

	n, err := c.redis.Del(ctx, token).Result()
	if err != nil {
		return false, fmt.Errorf("delete %q: %w", key, err)
	}
	if n > 0 {
		delete(c.tokenToPlain, token)
	}
	return n > 0, nil
}

func (c *Client) List(ctx context.Context, pattern string) ([]string, error) {
	if pattern == "" {
		pattern = "*"
	}
	if c.privacyMode != PrivacyModeNone {
		keys := make([]string, 0, len(c.tokenToPlain))
		for _, key := range c.tokenToPlain {
			if matchesPattern(key, pattern) {
				keys = append(keys, key)
			}
		}
		return keys, nil
	}

	v, err := c.redis.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("list %q: %w", pattern, err)
	}
	return v, nil
}

func (c *Client) BatchGet(ctx context.Context, keys []string) ([]*string, error) {
	if len(keys) == 0 {
		return []*string{}, nil
	}

	args := make([]string, len(keys))
	for i, key := range keys {
		token, tokenErr := c.tokenizeKey(key)
		if tokenErr != nil {
			return nil, tokenErr
		}
		args[i] = token
	}
	v, err := c.redis.MGet(ctx, args...).Result()
	if err != nil {
		return nil, fmt.Errorf("batchGet: %w", err)
	}

	out := make([]*string, len(v))
	for i, item := range v {
		if item == nil {
			continue
		}
		s := fmt.Sprint(item)
		out[i] = &s
	}
	return out, nil
}

func (c *Client) BatchPut(ctx context.Context, entries map[string]string) error {
	if len(entries) == 0 {
		return nil
	}

	args := make([]interface{}, 0, len(entries)*2)
	for k, v := range entries {
		if k == "" {
			return fmt.Errorf("key must be non-empty")
		}
		token, err := c.tokenizeKey(k)
		if err != nil {
			return err
		}
		args = append(args, token, v)
		c.rememberKey(token, k)
	}

	if err := c.redis.MSet(ctx, args...).Err(); err != nil {
		return fmt.Errorf("batchPut: %w", err)
	}
	return nil
}

func (c *Client) SetWithTTL(ctx context.Context, key string, value string, ttlSeconds int) error {
	if key == "" {
		return fmt.Errorf("key must be non-empty")
	}
	if ttlSeconds <= 0 {
		return fmt.Errorf("ttlSeconds must be greater than zero")
	}
	token, err := c.tokenizeKey(key)
	if err != nil {
		return err
	}

	if err := c.redis.SetEx(ctx, token, value, time.Duration(ttlSeconds)*time.Second).Err(); err != nil {
		return fmt.Errorf("setWithTTL %q: %w", key, err)
	}
	c.rememberKey(token, key)
	return nil
}

func (c *Client) Backup(ctx context.Context) (string, error) {
	v, err := c.redis.Do(ctx, "BACKUP").Text()
	if err != nil {
		return "", fmt.Errorf("backup: %w", err)
	}
	return v, nil
}

func (c *Client) Restore(ctx context.Context, ref string, key string) (int64, error) {
	if ref == "" {
		return 0, fmt.Errorf("ref must be non-empty")
	}

	args := []interface{}{"RESTOREDB", ref}
	if key != "" {
		args = append(args, key)
	}

	v, err := c.redis.Do(ctx, args...).Int64()
	if err != nil {
		return 0, fmt.Errorf("restore %q: %w", ref, err)
	}
	return v, nil
}

func (c *Client) RotateKey(ctx context.Context, oldKey string, newKey string) (string, error) {
	if oldKey == "" {
		return "", fmt.Errorf("oldKey must be non-empty")
	}
	if newKey == "" {
		return "", fmt.Errorf("newKey must be non-empty")
	}

	v, err := c.redis.Do(ctx, "ROTATEKEY", oldKey, newKey).Text()
	if err != nil {
		return "", fmt.Errorf("rotateKey: %w", err)
	}
	return v, nil
}

func (c *Client) PutJSON(ctx context.Context, key string, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal %q: %w", key, err)
	}
	return c.Put(ctx, key, string(b))
}

func (c *Client) GetJSON(ctx context.Context, key string, out any) error {
	v, err := c.Get(ctx, key)
	if err != nil {
		return err
	}

	if err := json.Unmarshal([]byte(v), out); err != nil {
		return fmt.Errorf("unmarshal %q: %w", key, err)
	}
	return nil
}

type redisAdapter struct {
	inner *redis.Client
}

func (r *redisAdapter) Get(ctx context.Context, key string) *redis.StringCmd {
	return r.inner.Get(ctx, key)
}

func (r *redisAdapter) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return r.inner.Set(ctx, key, value, expiration)
}

func (r *redisAdapter) SetEx(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return r.inner.SetEx(ctx, key, value, expiration)
}

func (r *redisAdapter) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	return r.inner.Del(ctx, keys...)
}

func (r *redisAdapter) Keys(ctx context.Context, pattern string) *redis.StringSliceCmd {
	return r.inner.Keys(ctx, pattern)
}

func (r *redisAdapter) MGet(ctx context.Context, keys ...string) *redis.SliceCmd {
	return r.inner.MGet(ctx, keys...)
}

func (r *redisAdapter) MSet(ctx context.Context, values ...interface{}) *redis.StatusCmd {
	return r.inner.MSet(ctx, values...)
}

func (r *redisAdapter) Do(ctx context.Context, args ...interface{}) *redis.Cmd {
	return r.inner.Do(ctx, args...)
}

func (c *Client) tokenizeKey(key string) (string, error) {
	if c.privacyMode == PrivacyModeNone {
		return key, nil
	}
	if c.privacyConfigErr != nil {
		return "", fmt.Errorf("privacy configuration error: %w", c.privacyConfigErr)
	}
	if len(c.privacyKey) == 0 {
		return "", fmt.Errorf("privacy key must be set when privacy mode is enabled")
	}
	mac := hmac.New(sha256.New, c.privacyKey)
	_, _ = mac.Write([]byte(key))
	return hex.EncodeToString(mac.Sum(nil)), nil
}

func (c *Client) rememberKey(token string, key string) {
	if c.privacyMode != PrivacyModeNone {
		c.tokenToPlain[token] = key
	}
}

func matchesPattern(key string, pattern string) bool {
	if pattern == "" || pattern == "*" {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return key == pattern
	}
	prefix := strings.SplitN(pattern, "*", 2)[0]
	return strings.HasPrefix(key, prefix)
}
