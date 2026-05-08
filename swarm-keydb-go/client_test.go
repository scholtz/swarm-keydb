package swarmkeydb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type mockRedis struct {
	store      map[string]string
	err        error
	doCommands [][]interface{}
}

func newMockRedis() *mockRedis {
	return &mockRedis{store: map[string]string{}}
}

func (m *mockRedis) Get(ctx context.Context, key string) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx)
	if m.err != nil {
		cmd.SetErr(m.err)
		return cmd
	}
	v, ok := m.store[key]
	if !ok {
		cmd.SetErr(redis.Nil)
		return cmd
	}
	cmd.SetVal(v)
	return cmd
}

func (m *mockRedis) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	cmd := redis.NewStatusCmd(ctx)
	if m.err != nil {
		cmd.SetErr(m.err)
		return cmd
	}
	m.store[key] = value.(string)
	cmd.SetVal("OK")
	return cmd
}

func (m *mockRedis) SetEx(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return m.Set(ctx, key, value, expiration)
}

func (m *mockRedis) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx)
	if m.err != nil {
		cmd.SetErr(m.err)
		return cmd
	}
	n := int64(0)
	for _, key := range keys {
		if _, ok := m.store[key]; ok {
			n++
			delete(m.store, key)
		}
	}
	cmd.SetVal(n)
	return cmd
}

func (m *mockRedis) Keys(ctx context.Context, pattern string) *redis.StringSliceCmd {
	cmd := redis.NewStringSliceCmd(ctx)
	if m.err != nil {
		cmd.SetErr(m.err)
		return cmd
	}
	keys := make([]string, 0, len(m.store))
	for k := range m.store {
		keys = append(keys, k)
	}
	cmd.SetVal(keys)
	return cmd
}

func (m *mockRedis) MGet(ctx context.Context, keys ...string) *redis.SliceCmd {
	cmd := redis.NewSliceCmd(ctx)
	if m.err != nil {
		cmd.SetErr(m.err)
		return cmd
	}
	values := make([]interface{}, len(keys))
	for i, key := range keys {
		if value, ok := m.store[key]; ok {
			values[i] = value
		}
	}
	cmd.SetVal(values)
	return cmd
}

func (m *mockRedis) MSet(ctx context.Context, values ...interface{}) *redis.StatusCmd {
	cmd := redis.NewStatusCmd(ctx)
	if m.err != nil {
		cmd.SetErr(m.err)
		return cmd
	}
	for i := 0; i < len(values); i += 2 {
		m.store[values[i].(string)] = values[i+1].(string)
	}
	cmd.SetVal("OK")
	return cmd
}

func (m *mockRedis) Do(ctx context.Context, args ...interface{}) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	if m.err != nil {
		cmd.SetErr(m.err)
		return cmd
	}

	if len(args) > 0 {
		m.doCommands = append(m.doCommands, args)
	}

	switch args[0] {
	case "BACKUP":
		cmd.SetVal("swarm://backup-ref")
	case "RESTOREDB":
		cmd.SetVal(int64(2))
	case "ROTATEKEY":
		cmd.SetVal("swarm://rotation-ref")
	case "AUTHDID":
		cmd.SetVal("OK")
	default:
		cmd.SetErr(errors.New("unexpected command"))
	}

	return cmd
}

func TestPutGetDeleteList(t *testing.T) {
	ctx := context.Background()
	client := NewWithRedisClient(newMockRedis())

	if err := client.Put(ctx, "a", "1"); err != nil {
		t.Fatal(err)
	}
	v, err := client.Get(ctx, "a")
	if err != nil || v != "1" {
		t.Fatalf("got (%q,%v)", v, err)
	}
	deleted, err := client.Delete(ctx, "a")
	if err != nil || !deleted {
		t.Fatalf("delete failed: %v", err)
	}
	keys, err := client.List(ctx, "*")
	if err != nil || len(keys) != 0 {
		t.Fatalf("list failed: %v keys=%v", err, keys)
	}
}

func TestBatchGetBatchPutAndTTL(t *testing.T) {
	ctx := context.Background()
	client := NewWithRedisClient(newMockRedis())

	if err := client.BatchPut(ctx, map[string]string{"k1": "v1", "k2": "v2"}); err != nil {
		t.Fatal(err)
	}
	values, err := client.BatchGet(ctx, []string{"k1", "k2", "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if values[0] == nil || *values[0] != "v1" || values[2] != nil {
		t.Fatalf("unexpected values: %#v", values)
	}
	if err := client.SetWithTTL(ctx, "temp", "x", 5); err != nil {
		t.Fatal(err)
	}
}

func TestErrors(t *testing.T) {
	ctx := context.Background()
	client := NewWithRedisClient(newMockRedis())

	if _, err := client.Get(ctx, "missing"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
	if err := client.Put(ctx, "", "x"); err == nil {
		t.Fatal("expected validation error")
	}
	if err := client.SetWithTTL(ctx, "k", "v", 0); err == nil {
		t.Fatal("expected ttl validation error")
	}
	if err := client.SetWithTTL(ctx, "k", "v", -1); err == nil {
		t.Fatal("expected ttl validation error")
	}

	failing := newMockRedis()
	failing.err = errors.New("connection refused")
	client = NewWithRedisClient(failing)
	if err := client.Put(ctx, "k", "v"); err == nil {
		t.Fatal("expected wrapped error")
	}
}

type userProfile struct {
	Name string `json:"name"`
}

func TestJSONHelpers(t *testing.T) {
	ctx := context.Background()
	client := NewWithRedisClient(newMockRedis())

	if err := client.PutJSON(ctx, "user:1", userProfile{Name: "Ada"}); err != nil {
		t.Fatal(err)
	}

	var profile userProfile
	if err := client.GetJSON(ctx, "user:1", &profile); err != nil {
		t.Fatal(err)
	}
	if profile.Name != "Ada" {
		t.Fatalf("unexpected profile: %#v", profile)
	}
}

func TestManagementHelpers(t *testing.T) {
	ctx := context.Background()
	client := NewWithRedisClient(newMockRedis())

	backupRef, err := client.Backup(ctx)
	if err != nil || backupRef != "swarm://backup-ref" {
		t.Fatalf("backup failed: %v ref=%q", err, backupRef)
	}

	restored, err := client.Restore(ctx, "swarm://backup-ref", "old-key")
	if err != nil || restored != 2 {
		t.Fatalf("restore failed: %v restored=%d", err, restored)
	}

	rotationRef, err := client.RotateKey(ctx, "old-key", "new-key")
	if err != nil || rotationRef != "swarm://rotation-ref" {
		t.Fatalf("rotate failed: %v ref=%q", err, rotationRef)
	}
}

func TestPrivacyModeTokenizesKeys(t *testing.T) {
	ctx := context.Background()
	backend := newMockRedis()
	client := NewWithRedisClientAndOptions(backend, Options{
		PrivacyMode:   PrivacyModeObliviousHashing,
		PrivacyKeyHex: "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
	})

	if err := client.Put(ctx, "secret:key", "1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := backend.store["secret:key"]; ok {
		t.Fatal("expected plaintext key to be hidden in backend")
	}
	keys, err := client.List(ctx, "secret:*")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "secret:key" {
		t.Fatalf("unexpected listed keys: %#v", keys)
	}
}

func TestDidAuthModeOption(t *testing.T) {
	backend := newMockRedis()
	client := NewWithRedisClientAndOptions(backend, Options{
		DidMode:   DidAuthModeEthrDid,
		DidRpcUrl: "http://localhost:8545",
	})
	if client.didMode != DidAuthModeEthrDid {
		t.Fatalf("expected didMode=%s, got %s", DidAuthModeEthrDid, client.didMode)
	}
	if client.didRpcUrl != "http://localhost:8545" {
		t.Fatalf("expected didRpcUrl=http://localhost:8545, got %s", client.didRpcUrl)
	}
}

func TestSetDidWithoutProof(t *testing.T) {
	ctx := context.Background()
	backend := newMockRedis()
	client := NewWithRedisClient(backend)

	did := "did:ethr:0x1111111111111111111111111111111111111111"
	if err := client.SetDid(ctx, did, "", ""); err != nil {
		t.Fatal(err)
	}
	if client.currentDid != did {
		t.Fatalf("expected currentDid=%s, got %s", did, client.currentDid)
	}
	if len(backend.doCommands) != 1 {
		t.Fatalf("expected 1 Do command, got %d", len(backend.doCommands))
	}
	if backend.doCommands[0][0] != "AUTHDID" || backend.doCommands[0][1] != did {
		t.Fatalf("unexpected AUTHDID command: %#v", backend.doCommands[0])
	}
}

func TestSetDidWithProof(t *testing.T) {
	ctx := context.Background()
	backend := newMockRedis()
	client := NewWithRedisClient(backend)

	did := "did:ethr:0x1234"
	if err := client.SetDid(ctx, did, "message", "0xsig"); err != nil {
		t.Fatal(err)
	}
	if len(backend.doCommands) != 1 {
		t.Fatalf("expected 1 Do command, got %d", len(backend.doCommands))
	}
	cmd := backend.doCommands[0]
	if len(cmd) != 4 || cmd[2] != "message" || cmd[3] != "0xsig" {
		t.Fatalf("expected AUTHDID with proof args, got %#v", cmd)
	}
}

func TestClearDid(t *testing.T) {
	backend := newMockRedis()
	client := NewWithRedisClient(backend)
	client.currentDid = "did:ethr:0x1111111111111111111111111111111111111111"
	client.ClearDid()
	if client.currentDid != "" {
		t.Fatalf("expected empty currentDid after ClearDid, got %s", client.currentDid)
	}
}
