using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SwarmKeyDb;
using SwarmKeyDb.Cli;

var tests = new (string Name, Func<Task> Test)[]
{
    ("client stores strings json binary and lists keys", ClientStoresSupportedValuesAsync),
    ("bee client parses upload references", BeeClientParsesUploadReferenceAsync),
    ("redis protocol supports set get exists delete", RedisProtocolRoundTripAsync),
    ("redis protocol supports keys and scan", RedisProtocolKeyIterationAsync),
    ("prefix scan returns matching keys", PrefixScanReturnsMatchingKeysAsync),
    ("range scan supports boundaries and reverse order", RangeScanSupportsBoundariesAndReverseOrderAsync),
    ("range scan rejects invalid bounds", RangeScanRejectsInvalidBoundsAsync),
    ("scan async returns paginated opaque cursor", ScanAsyncReturnsPaginatedOpaqueCursorAsync),
    ("query async applies key and value predicates", QueryAsyncAppliesKeyAndValuePredicatesAsync),
    ("persistent file index supports restart querying", PersistentFileIndexSupportsRestartQueryingAsync),
    ("cli supports put get delete list scan and stats", CliSupportsDataCommandsAsync),
    ("cli config set and get persists settings", CliConfigSetAndGetPersistsSettingsAsync),
    ("cli put validates value source arguments", CliPutValidatesValueSourceArgumentsAsync),
    ("cli uses environment variable overrides", CliUsesEnvironmentVariableOverridesAsync),
    ("mget returns nulls for missing keys", MGetReturnsNullsForMissingKeysAsync),
    ("mset sets multiple keys atomically", MSetSetsMultipleKeysAtomicallyAsync),
    ("setex stores value with ttl", SetExStoresValueWithTtlAsync),
    ("expire evicts key after delay", ExpireEvictsKeyAfterDelayAsync),
    ("persist removes ttl", PersistRemovesTtlAsync),
    ("ttl returns negative two for missing key", TtlReturnsNegativeTwoForMissingKeyAsync),
    ("set with ex option sets expiry", SetWithExOptionSetsExpiryAsync),
    ("batch operations resp format", BatchOperationsRespFormatAsync),
    ("async batch get and put round trip", AsyncBatchGetAndPutRoundTripAsync),
    ("async flush waits for queued fire and forget writes", AsyncFlushWaitsForQueuedFireAndForgetWritesAsync),
    ("async fire and forget captures and logs errors", AsyncFireAndForgetCapturesAndLogsErrorsAsync),
    ("async write queue respects configured max concurrency", AsyncWriteQueueRespectsConfiguredMaxConcurrencyAsync),
    ("async batch throughput is at least 2x sequential baseline", AsyncBatchThroughputIsAtLeastTwoXSequentialBaselineAsync),
    ("setex rejects non positive ttl", SetExRejectsNonPositiveTtlAsync),
    ("msetnx does not partially write when blocked", MSetNxDoesNotPartiallyWriteWhenBlockedAsync),
    ("pexpire and pttl round trip", PExpireAndPttlRoundTripAsync),
    ("expireat in past removes key", ExpireAtInPastRemovesKeyAsync),
    ("set with exat option sets expiry", SetWithExAtOptionSetsExpiryAsync),
    ("setex rejects overflow ttl", SetExRejectsOverflowTtlAsync),
    ("vector clock increment compare and merge", VectorClockIncrementCompareAndMergeAsync),
    ("lww register tie break is deterministic", LwwRegisterTieBreakIsDeterministicAsync),
    ("or set add remove and concurrent merge", OrSetAddRemoveAndConcurrentMergeAsync),
    ("pn counter increment decrement merge", PnCounterIncrementDecrementMergeAsync),
    ("crdt merge method uses default lww register", CrdtMergeMethodUsesDefaultLwwRegisterAsync),
    ("custom merge strategy can be configured per key", CustomMergeStrategyCanBeConfiguredPerKeyAsync),
    ("two instances merge concurrent writes deterministically", TwoInstancesMergeConcurrentWritesDeterministicallyAsync),
    ("caching store get returns cached value after put", CachingKeyValueStoreGetReturnsCachedValueAfterPutAsync),
    ("caching store put invalidates cache", CachingKeyValueStorePutInvalidatesCacheAsync),
    ("caching store delete invalidates cache", CachingKeyValueStoreDeleteInvalidatesCacheAsync),
    ("caching store respects key ttl", CachingKeyValueStoreRespectsKeyTtlAsync),
    ("caching store max entries evicts lru", CachingKeyValueStoreMaxEntriesEvictsLruAsync),
    ("compressing store put stores compressed value", CompressingKeyValueStorePutStoresCompressedValueAsync),
    ("compressing store get returns decompressed value", CompressingKeyValueStoreGetReturnsDecompressedValueAsync),
    ("compressing store skips compression below min size", CompressingKeyValueStoreSkipsCompressionBelowMinSizeAsync),
    ("compressing store handles legacy uncompressed data", CompressingKeyValueStoreHandlesLegacyUncompressedDataAsync),
    ("compressing store brotli compress and decompress", CompressingKeyValueStoreBrotliCompressAndDecompressAsync),
    ("compressing store delete and ttl pass through", CompressingKeyValueStoreDeleteAndTtlPassThroughAsync),
    ("encrypting store put stores encrypted value", EncryptingKeyValueStorePutStoresEncryptedValueAsync),
    ("encrypting store get returns decrypted value", EncryptingKeyValueStoreGetReturnsDecryptedValueAsync),
    ("encrypting store nonce is random same value different ciphertext", EncryptingKeyValueStoreNonceIsRandomSameValueDifferentCiphertextAsync),
    ("encrypting store legacy unencrypted data returned unchanged", EncryptingKeyValueStoreLegacyUnencryptedDataReturnedUnchangedAsync),
    ("encrypting store tampered ciphertext throws cryptographic exception", EncryptingKeyValueStoreTamperedCiphertextThrowsCryptographicExceptionAsync),
    ("encrypting store delete and ttl pass through", EncryptingKeyValueStoreDeleteAndTtlPassThroughAsync),
    ("encrypting store ethereum key derivation produces consistent key", EncryptingKeyValueStoreEthereumKeyDerivationProducesConsistentKeyAsync),
    ("encrypting store startup fails when enabled with no key", EncryptingKeyValueStoreStartupFailsWhenEnabledWithNoKeyAsync),
    ("acl allowlist read address can get", AclAllowlistReadAddressCanGetAsync),
    ("acl allowlist write address can put and delete", AclAllowlistWriteAddressCanPutAndDeleteAsync),
    ("acl allowlist unlisted address is denied on get", AclAllowlistUnlistedAddressIsDeniedOnGetAsync),
    ("acl allowlist unlisted address is denied on put", AclAllowlistUnlistedAddressIsDeniedOnPutAsync),
    ("acl allowlist admin grants read and write", AclAllowlistAdminGrantsReadAndWriteAsync),
    ("acl denylist blocked address is denied and non blocked address is allowed", AclDenylistBlockedAddressIsDeniedAndNonBlockedAddressIsAllowedAsync),
    ("acl disabled passes all operations through", AclDisabledPassesAllOperationsThroughAsync),
    ("acl startup fails when enabled with empty entries", AclStartupFailsWhenEnabledWithEmptyEntriesAsync),
    ("service collection places acl between swarm and encryption", ServiceCollectionPlacesAclBetweenSwarmAndEncryptionAsync),
    ("cached read still requires acl permission", CachedReadStillRequiresAclPermissionAsync),
    ("redis protocol returns access denied error for unauthorized address", RedisProtocolReturnsAccessDeniedErrorForUnauthorizedAddressAsync),
    ("composite key constructs key from segments", CompositeKeyConstructsKeyFromSegmentsAsync),
    ("composite key rejects segment containing separator", CompositeKeyRejectsSegmentContainingSeparatorAsync),
    ("composite key rejects empty segment", CompositeKeyRejectsEmptySegmentAsync),
    ("composite key supports custom separator", CompositeKeySupportsCustomSeparatorAsync),
    ("namespaced store scopes put and get to prefix", NamespacedStoreScopesPutAndGetToPrefixAsync),
    ("namespaced store list keys strips prefix", NamespacedStoreListKeysStripsPrefixAsync),
    ("namespaced store delete removes prefixed key", NamespacedStoreDeleteRemovesPrefixedKeyAsync),
    ("namespaced store isolates two namespaces", NamespacedStoreIsolatesTwoNamespacesAsync),
    ("delete namespace removes all keys under prefix", DeleteNamespaceRemovesAllKeysUnderPrefixAsync),
    ("with namespace scopes client operations", WithNamespaceScopesClientOperationsAsync),
    ("cli delete namespace removes prefixed keys", CliDeleteNamespaceRemovesPrefixedKeysAsync),
    ("btree index lookup insert delete", BTreeIndexLookupInsertDeleteAsync),
    ("btree index range scan returns ordered subset", BTreeIndexRangeScanReturnsOrderedSubsetAsync),
    ("btree index prefix scan uses efficient range", BTreeIndexPrefixScanUsesEfficientRangeAsync),
    ("btree index expiry evicts keys on access", BTreeIndexExpiryEvictsKeysOnAccessAsync),
    ("btree index rebuild purges expired entries", BTreeIndexRebuildPurgesExpiredEntriesAsync),
    ("btree index range scan open bounds", BTreeIndexRangeScanOpenBoundsAsync),
    ("swarm store with btree index range scan", SwarmStoreWithBTreeIndexRangeScanAsync),
    ("swarm store with btree index prefix scan", SwarmStoreWithBTreeIndexPrefixScanAsync),
};

foreach (var (name, test) in tests)
{
    await test();
    Console.WriteLine($"PASS {name}");
}

static async Task ClientStoresSupportedValuesAsync()
{
    var client = new SwarmKeyDbClient(new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex()));

    await client.PutStringAsync("profile:name", "Ada");
    await client.PutJsonAsync("profile:settings", new Settings(true, 3));
    await client.PutBytesAsync("profile:avatar", new byte[] { 0, 1, 2, 255 });

    AssertEqual("Ada", await client.GetStringAsync("profile:name"));
    var settings = await client.GetJsonAsync<Settings>("profile:settings");
    Assert(settings is { Enabled: true, Count: 3 }, "JSON value should round-trip.");
    AssertSequenceEqual(new byte[] { 0, 1, 2, 255 }, await client.GetBytesAsync("profile:avatar") ?? Array.Empty<byte>());
    AssertSequenceEqual(new[] { "profile:avatar", "profile:name", "profile:settings" }, await client.KeysAsync());
    Assert(await client.DeleteAsync("profile:name"), "Delete should report existing key.");
    AssertEqual(null, await client.GetStringAsync("profile:name"));
}

static async Task BeeClientParsesUploadReferenceAsync()
{
    var handler = new StubHttpMessageHandler(request =>
    {
        AssertEqual("POST", request.Method.Method);
        Assert(request.Headers.Contains("swarm-postage-batch-id"), "Upload should include postage batch header.");
        return new HttpResponseMessage(System.Net.HttpStatusCode.OK)
        {
            Content = new StringContent("{\"reference\":\"abc123\"}")
        };
    });
    using var httpClient = new HttpClient(handler) { BaseAddress = new Uri("http://localhost:1633/") };
    var client = new BeeSwarmClient(httpClient, "batch-id");

    AssertEqual("abc123", await client.UploadAsync(new byte[] { 1, 2, 3 }));
}

static async Task RedisProtocolRoundTripAsync()
{
    var processor = CreateProcessor();
    var response = await ExecuteAsync(processor,
        RespCommand("SET", "hello", "world") +
        RespCommand("GET", "hello") +
        RespCommand("EXISTS", "hello", "missing") +
        RespCommand("DEL", "hello") +
        RespCommand("GET", "hello"));

    AssertEqual("+OK\r\n$5\r\nworld\r\n:1\r\n:1\r\n$-1\r\n", response);
}

static async Task RedisProtocolKeyIterationAsync()
{
    var processor = CreateProcessor();
    var response = await ExecuteAsync(processor,
        RespCommand("SET", "app:a", "1") +
        RespCommand("SET", "app:b", "2") +
        RespCommand("SET", "other", "3") +
        RespCommand("KEYS", "app:*") +
        RespCommand("SCAN", "0", "MATCH", "app:*", "COUNT", "1"));

    Assert(response.Contains("*2\r\n$5\r\napp:a\r\n$5\r\napp:b\r\n", StringComparison.Ordinal), "KEYS should return matching app keys.");
    Assert(response.EndsWith("*2\r\n$1\r\n1\r\n*1\r\n$5\r\napp:a\r\n", StringComparison.Ordinal), "SCAN should return next cursor and one key.");
}

static async Task PrefixScanReturnsMatchingKeysAsync()
{
    var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
    await store.PutAsync("user:alice:profile", Encoding.UTF8.GetBytes("1"));
    await store.PutAsync("user:alice:settings", Encoding.UTF8.GetBytes("2"));
    await store.PutAsync("user:bob:profile", Encoding.UTF8.GetBytes("3"));

    var keys = await store.GetKeysWithPrefixAsync("user:alice:");
    AssertSequenceEqual(new[] { "user:alice:profile", "user:alice:settings" }, keys);
}

static async Task RangeScanSupportsBoundariesAndReverseOrderAsync()
{
    var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
    await store.PutAsync("k:1", Encoding.UTF8.GetBytes("one"));
    await store.PutAsync("k:2", Encoding.UTF8.GetBytes("two"));
    await store.PutAsync("k:3", Encoding.UTF8.GetBytes("three"));

    var descending = await store.GetKeyRangeAsync("k:1", "k:3", new RangeScanOptions
    {
        IncludeStart = false,
        IncludeEnd = true,
        Descending = true,
        IncludeValues = true
    });

    AssertSequenceEqual(new[] { "k:3", "k:2" }, descending.Select(static item => item.Key));
    AssertEqual("three", Encoding.UTF8.GetString(descending[0].Value!));
    AssertEqual("two", Encoding.UTF8.GetString(descending[1].Value!));
}

static async Task RangeScanRejectsInvalidBoundsAsync()
{
    var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
    await store.PutAsync("a", Encoding.UTF8.GetBytes("1"));

    var threw = false;
    try
    {
        _ = await store.GetKeyRangeAsync("z", "a");
    }
    catch (ArgumentException ex) when (ex.Message.Contains("startKey must be lexicographically ≤ endKey.", StringComparison.Ordinal))
    {
        threw = true;
    }

    Assert(threw, "Expected a clear error when startKey is greater than endKey.");
}

static async Task ScanAsyncReturnsPaginatedOpaqueCursorAsync()
{
    var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
    await store.PutAsync("page:1", Encoding.UTF8.GetBytes("1"));
    await store.PutAsync("page:2", Encoding.UTF8.GetBytes("2"));
    await store.PutAsync("page:3", Encoding.UTF8.GetBytes("3"));

    var first = await store.ScanAsync(null, 2);
    AssertSequenceEqual(new[] { "page:1", "page:2" }, first.Keys);
    Assert(!string.IsNullOrEmpty(first.NextCursor), "Expected non-empty cursor for additional pages.");
    Assert(!first.NextCursor.Contains("page:2", StringComparison.Ordinal), "Cursor should be opaque, not a raw key.");

    var second = await store.ScanAsync(first.NextCursor, 2);
    AssertSequenceEqual(new[] { "page:3" }, second.Keys);
    AssertEqual(string.Empty, second.NextCursor);
}

static async Task QueryAsyncAppliesKeyAndValuePredicatesAsync()
{
    var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
    await store.PutAsync("msg:1", Encoding.UTF8.GetBytes("hello"));
    await store.PutAsync("msg:2", Encoding.UTF8.GetBytes("bye"));
    await store.PutAsync("cfg:1", Encoding.UTF8.GetBytes("hello"));

    var matches = new List<string>();
    await foreach (var item in store.QueryAsync(
                       key => key.StartsWith("msg:", StringComparison.Ordinal),
                       value => Encoding.UTF8.GetString(value).Contains("hello", StringComparison.Ordinal)))
    {
        matches.Add(item.Key);
    }

    AssertSequenceEqual(new[] { "msg:1" }, matches);
}

static async Task PersistentFileIndexSupportsRestartQueryingAsync()
{
    var root = Path.Combine(Path.GetTempPath(), "swarm-keydb-tests", Guid.NewGuid().ToString("N"));
    var indexPath = Path.Combine(root, "index.json");
    Directory.CreateDirectory(root);
    try
    {
        var swarm = new InMemorySwarmClient();
        var store1 = new SwarmKeyValueStore(swarm, new FileKeyIndex(indexPath));
        await store1.PutAsync("orders:001", Encoding.UTF8.GetBytes("first"));
        await store1.PutAsync("orders:002", Encoding.UTF8.GetBytes("second"));
        await store1.PutAsync("profile:001", Encoding.UTF8.GetBytes("other"));

        var store2 = new SwarmKeyValueStore(swarm, new FileKeyIndex(indexPath));
        var prefix = await store2.GetKeysWithPrefixAsync("orders:");
        var range = await store2.GetKeyRangeAsync("orders:001", "orders:999");

        AssertSequenceEqual(new[] { "orders:001", "orders:002" }, prefix);
        AssertSequenceEqual(new[] { "orders:001", "orders:002" }, range.Select(static item => item.Key));
    }
    finally
    {
        if (Directory.Exists(root))
        {
            Directory.Delete(root, recursive: true);
        }
    }
}

static async Task CliSupportsDataCommandsAsync()
{
    var swarm = new InMemorySwarmClient();
    var index = new InMemoryKeyIndex();
    var options = new CliExecutionOptions
    {
        SwarmClientFactory = _ => swarm,
        KeyIndexFactory = _ => index,
        EnvironmentFactory = static () => new EnvironmentSnapshot
        {
            Home = Path.Combine(Path.GetTempPath(), "swarm-keydb-cli-tests", Guid.NewGuid().ToString("N")),
            BeeUrl = "http://localhost:1633/",
            BatchId = "batch-id"
        }
    };

    var putResult = await RunCliAsync(new[] { "put", "user:alice", "{\"name\":\"Alice\"}" }, options);
    AssertEqual(0, putResult.ExitCode);
    AssertEqual("OK user:alice", putResult.Stdout.Trim());

    var getResult = await RunCliAsync(new[] { "get", "user:alice", "--output", "json" }, options);
    AssertEqual(0, getResult.ExitCode);
    var getPayload = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(getResult.Stdout.Trim());
    Assert(getPayload is not null, "Expected get JSON payload.");
    Assert(getPayload!["found"].GetBoolean(), "Expected key to exist.");
    AssertEqual("{\"name\":\"Alice\"}", getPayload["value"].GetString());

    await RunCliAsync(new[] { "put", "user:bob", "2" }, options);
    await RunCliAsync(new[] { "put", "profile:charlie", "3" }, options);

    var listResult = await RunCliAsync(new[] { "list", "--prefix", "user:" }, options);
    AssertEqual(0, listResult.ExitCode);
    Assert(listResult.Stdout.Contains("user:alice", StringComparison.Ordinal), "List should include prefixed key.");
    Assert(listResult.Stdout.Contains("user:bob", StringComparison.Ordinal), "List should include second prefixed key.");
    Assert(!listResult.Stdout.Contains("profile:charlie", StringComparison.Ordinal), "List should filter non-prefixed key.");

    var scanResult = await RunCliAsync(new[] { "scan", "--from", "user:a", "--to", "user:z" }, options);
    AssertEqual(0, scanResult.ExitCode);
    Assert(scanResult.Stdout.Contains("user:alice", StringComparison.Ordinal), "Scan should include user:alice.");
    Assert(scanResult.Stdout.Contains("user:bob", StringComparison.Ordinal), "Scan should include user:bob.");

    var statsResult = await RunCliAsync(new[] { "stats", "--output", "json" }, options);
    AssertEqual(0, statsResult.ExitCode);
    var statsPayload = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(statsResult.Stdout.Trim());
    Assert(statsPayload is not null, "Expected stats JSON payload.");
    Assert(statsPayload!["keyCount"].GetInt32() >= 3, "Stats should report all inserted keys.");
    Assert(statsPayload["storageBytes"].GetInt64() > 0, "Stats should include storage usage.");

    var deleteResult = await RunCliAsync(new[] { "delete", "user:alice" }, options);
    AssertEqual(0, deleteResult.ExitCode);
    AssertEqual("1", deleteResult.Stdout.Trim());

    var missingResult = await RunCliAsync(new[] { "get", "user:alice" }, options);
    AssertEqual(1, missingResult.ExitCode);
    Assert(missingResult.Stderr.Contains("Key not found: user:alice", StringComparison.Ordinal), "Expected explicit missing key error message.");
}

static async Task CliConfigSetAndGetPersistsSettingsAsync()
{
    var home = Path.Combine(Path.GetTempPath(), "swarm-keydb-cli-tests", Guid.NewGuid().ToString("N"));
    var options = new CliExecutionOptions
    {
        EnvironmentFactory = () => new EnvironmentSnapshot
        {
            Home = home
        }
    };

    var setResult = await RunCliAsync(new[] { "config", "set", "--bee-url", "http://localhost:1733/", "--batch-id", "batch-123", "--output", "table" }, options);
    AssertEqual(0, setResult.ExitCode);
    var configPath = Path.Combine(home, ".swarmkeydb", "config.json");
    Assert(File.Exists(configPath), "Config file should be created.");

    var getResult = await RunCliAsync(new[] { "config", "get", "--output", "json" }, options);
    AssertEqual(0, getResult.ExitCode);
    var payload = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(getResult.Stdout.Trim());
    Assert(payload is not null, "Expected config JSON payload.");
    AssertEqual("http://localhost:1733/", payload!["BeeUrl"].GetString());
    AssertEqual("batch-123", payload["BatchId"].GetString());
}

static async Task CliPutValidatesValueSourceArgumentsAsync()
{
    var options = CreateCliTestOptions();

    var noValueResult = await RunCliAsync(new[] { "put", "k" }, options);
    AssertEqual(1, noValueResult.ExitCode);
    Assert(noValueResult.Stderr.Contains("put requires <value> or --file <path>.", StringComparison.Ordinal), "Expected validation message for missing value.");

    var bothSourcesResult = await RunCliAsync(new[] { "put", "k", "v", "--file", "x" }, options);
    AssertEqual(1, bothSourcesResult.ExitCode);
    Assert(bothSourcesResult.Stderr.Contains("Use either inline <value> or --file, not both.", StringComparison.Ordinal), "Expected validation message for conflicting value sources.");
}

static async Task CliUsesEnvironmentVariableOverridesAsync()
{
    var options = new CliExecutionOptions
    {
        EnvironmentFactory = static () => new EnvironmentSnapshot
        {
            Home = Path.Combine(Path.GetTempPath(), "swarm-keydb-cli-env"),
            BeeUrl = "http://127.0.0.1:1/",
            BatchId = "env-batch"
        }
    };

    var result = await RunCliAsync(new[] { "put", "env:test", "1" }, options);
    AssertEqual(1, result.ExitCode);
    Assert(result.Stderr.Contains("Bee node unreachable", StringComparison.Ordinal), "Expected Bee connectivity error.");
}

static async Task MGetReturnsNullsForMissingKeysAsync()
{
    var processor = CreateProcessor();
    var response = await ExecuteAsync(processor,
        RespCommand("SET", "a", "1") +
        RespCommand("MGET", "a", "missing", "b"));

    AssertEqual("+OK\r\n*3\r\n$1\r\n1\r\n$-1\r\n$-1\r\n", response);
}

static async Task MSetSetsMultipleKeysAtomicallyAsync()
{
    var processor = CreateProcessor();
    var response = await ExecuteAsync(processor,
        RespCommand("MSET", "a", "1", "b", "2", "c", "3") +
        RespCommand("MGET", "a", "b", "c"));

    AssertEqual("+OK\r\n*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n", response);
}

static async Task SetExStoresValueWithTtlAsync()
{
    var processor = CreateProcessor();
    var setExResponse = await ExecuteAsync(processor, RespCommand("SETEX", "session:token", "300", "abc123"));
    AssertEqual("+OK\r\n", setExResponse);

    var ttlResponse = await ExecuteAsync(processor, RespCommand("TTL", "session:token"));
    var ttl = ParseIntegerResponse(ttlResponse);
    Assert(ttl is > 0 and <= 300, "TTL should be within expected range.");
}

static async Task ExpireEvictsKeyAfterDelayAsync()
{
    const int ttlSeconds = 1;
    const int expiryDelayMs = 1100;
    var processor = CreateProcessor();
    var response = await ExecuteAsync(processor,
        RespCommand("SET", "short", "1") +
        RespCommand("EXPIRE", "short", ttlSeconds.ToString()));
    AssertEqual("+OK\r\n:1\r\n", response);

    await Task.Delay(expiryDelayMs);
    AssertEqual("$-1\r\n", await ExecuteAsync(processor, RespCommand("GET", "short")));
}

static async Task PersistRemovesTtlAsync()
{
    var processor = CreateProcessor();
    var response = await ExecuteAsync(processor,
        RespCommand("SET", "persist:key", "value") +
        RespCommand("EXPIRE", "persist:key", "30") +
        RespCommand("PERSIST", "persist:key") +
        RespCommand("TTL", "persist:key"));

    AssertEqual("+OK\r\n:1\r\n:1\r\n:-1\r\n", response);
}

static async Task TtlReturnsNegativeTwoForMissingKeyAsync()
{
    var processor = CreateProcessor();
    AssertEqual(":-2\r\n", await ExecuteAsync(processor, RespCommand("TTL", "missing:key")));
}

static async Task SetWithExOptionSetsExpiryAsync()
{
    var processor = CreateProcessor();
    var setResponse = await ExecuteAsync(processor, RespCommand("SET", "option:key", "v", "EX", "10"));
    AssertEqual("+OK\r\n", setResponse);

    var ttl = ParseIntegerResponse(await ExecuteAsync(processor, RespCommand("TTL", "option:key")));
    Assert(ttl is > 0 and <= 10, "SET EX should apply TTL.");
}

static async Task BatchOperationsRespFormatAsync()
{
    var processor = CreateProcessor();
    var response = await ExecuteAsync(processor,
        RespCommand("MSET", "a", "1", "b", "2") +
        RespCommand("MGET", "a", "missing", "b") +
        RespCommand("MSETNX", "a", "9", "c", "3") +
        RespCommand("MSETNX", "x", "7", "y", "8") +
        RespCommand("MDEL", "a", "x", "missing"));

    AssertEqual("+OK\r\n*3\r\n$1\r\n1\r\n$-1\r\n$1\r\n2\r\n:0\r\n:1\r\n:2\r\n", response);
}

static async Task AsyncBatchGetAndPutRoundTripAsync()
{
    var client = new SwarmKeyDbClient(CreateAsyncQueuedStore(new CountingKeyValueStore(), maxConcurrentWrites: 4));
    await client.BatchPutAsync(new[]
    {
        new KeyValuePair<string, ReadOnlyMemory<byte>>("batch:a", Encoding.UTF8.GetBytes("1")),
        new KeyValuePair<string, ReadOnlyMemory<byte>>("batch:b", Encoding.UTF8.GetBytes("2")),
        new KeyValuePair<string, ReadOnlyMemory<byte>>("batch:c", Encoding.UTF8.GetBytes("3"))
    });

    var values = await client.BatchGetAsync(new[] { "batch:a", "missing", "batch:c" });
    AssertEqual("1", Encoding.UTF8.GetString(values[0]!));
    AssertEqual(null, values[1]);
    AssertEqual("3", Encoding.UTF8.GetString(values[2]!));
}

static async Task AsyncFlushWaitsForQueuedFireAndForgetWritesAsync()
{
    var inner = new DelayedWriteKeyValueStore(writeDelayMs: 30);
    var logger = new TestLogger<AsyncQueuedKeyValueStore>();
    var client = new SwarmKeyDbClient(CreateAsyncQueuedStore(inner, maxConcurrentWrites: 4, batchSize: 50, flushIntervalMs: 10, logger: logger));

    for (var i = 0; i < 30; i++)
    {
        var key = $"flush:{i:D2}";
        client.FireAndForget(() => client.PutStringAsync(key, "value"), operationName: $"put-{i}");
    }

    await client.FlushAsync();
    var keys = await client.GetKeysWithPrefixAsync("flush:");
    AssertEqual(30, keys.Count);
    foreach (var key in keys)
    {
        AssertEqual("value", await client.GetStringAsync(key));
    }
    AssertEqual(0, logger.Messages.Count);
}

static async Task AsyncFireAndForgetCapturesAndLogsErrorsAsync()
{
    var logger = new TestLogger<AsyncQueuedKeyValueStore>();
    var store = new AsyncQueuedKeyValueStore(new CountingKeyValueStore(), new AsyncProcessingOptions(), logger);

    store.FireAndForget(() => throw new InvalidOperationException("boom"), "exploding-async-task");
    store.FireAndForget(() => throw new InvalidOperationException("boom-action"), "exploding-sync-action");

    var captured = await WaitUntilValueAsync(
        action: () => Task.FromResult(logger.Messages.Count),
        predicate: count => count >= 2,
        timeout: TimeSpan.FromSeconds(2),
        pollInterval: TimeSpan.FromMilliseconds(25));

    Assert(captured >= 2, "Expected fire-and-forget logger to capture both async and action overload failures.");
    Assert(logger.Messages.Any(message => message.Contains("exploding-async-task", StringComparison.Ordinal)), "Expected operation name in structured log message.");
    Assert(logger.Messages.Any(message => message.Contains("exploding-sync-action", StringComparison.Ordinal)), "Expected action overload operation name in structured log message.");
}

static async Task AsyncWriteQueueRespectsConfiguredMaxConcurrencyAsync()
{
    var inner = new DelayedWriteKeyValueStore(writeDelayMs: 40);
    var client = new SwarmKeyDbClient(CreateAsyncQueuedStore(inner, maxConcurrentWrites: 2, batchSize: 100, flushIntervalMs: 5));

    var entries = Enumerable.Range(0, 20)
        .Select(i => new KeyValuePair<string, ReadOnlyMemory<byte>>($"concurrency:{i}", Encoding.UTF8.GetBytes("v")))
        .ToArray();

    await client.BatchPutAsync(entries);
    await client.FlushAsync();

    Assert(inner.MaxObservedConcurrentWrites <= 2, "Write queue should never exceed max concurrent writes.");
    Assert(inner.MaxObservedConcurrentWrites >= 2, "Write queue should process at least two writes in parallel when configured.");
}

static async Task AsyncBatchThroughputIsAtLeastTwoXSequentialBaselineAsync()
{
    const int operationCount = 120;
    const int writeDelayMs = 20;

    var baselineStore = new DelayedWriteKeyValueStore(writeDelayMs);
    var baselineWatch = System.Diagnostics.Stopwatch.StartNew();
    for (var i = 0; i < operationCount; i++)
    {
        await baselineStore.PutAsync($"baseline:{i}", Encoding.UTF8.GetBytes("v"));
    }
    baselineWatch.Stop();

    var asyncClient = new SwarmKeyDbClient(CreateAsyncQueuedStore(
        new DelayedWriteKeyValueStore(writeDelayMs),
        maxConcurrentWrites: 8,
        batchSize: operationCount,
        flushIntervalMs: 1));
    var payload = Enumerable.Range(0, operationCount)
        .Select(i => new KeyValuePair<string, ReadOnlyMemory<byte>>($"async:{i}", Encoding.UTF8.GetBytes("v")))
        .ToArray();

    var asyncWatch = System.Diagnostics.Stopwatch.StartNew();
    await asyncClient.BatchPutAsync(payload);
    await asyncClient.FlushAsync();
    asyncWatch.Stop();

    var improvedAtLeastTwoX = baselineWatch.Elapsed.TotalMilliseconds >= 2 * asyncWatch.Elapsed.TotalMilliseconds;
    Assert(improvedAtLeastTwoX,
        $"Expected >=2x throughput improvement. Baseline: {baselineWatch.Elapsed.TotalMilliseconds:F2} ms, Async: {asyncWatch.Elapsed.TotalMilliseconds:F2} ms.");
}

static async Task SetExRejectsNonPositiveTtlAsync()
{
    var processor = CreateProcessor();
    AssertEqual("-ERR invalid expire time in 'setex' command\r\n", await ExecuteAsync(processor, RespCommand("SETEX", "bad", "0", "v")));
    AssertEqual("-ERR invalid expire time in 'psetex' command\r\n", await ExecuteAsync(processor, RespCommand("PSETEX", "bad", "-1", "v")));
}

static async Task MSetNxDoesNotPartiallyWriteWhenBlockedAsync()
{
    var processor = CreateProcessor();
    var response = await ExecuteAsync(processor,
        RespCommand("SET", "a", "existing") +
        RespCommand("MSETNX", "a", "new", "b", "new-b") +
        RespCommand("MGET", "a", "b"));

    AssertEqual("+OK\r\n:0\r\n*2\r\n$8\r\nexisting\r\n$-1\r\n", response);
}

static async Task PExpireAndPttlRoundTripAsync()
{
    var processor = CreateProcessor();
    var setResponse = await ExecuteAsync(processor,
        RespCommand("SET", "ms:key", "v") +
        RespCommand("PEXPIRE", "ms:key", "500") +
        RespCommand("PTTL", "ms:key"));

    Assert(setResponse.StartsWith("+OK\r\n:1\r\n:", StringComparison.Ordinal), "PEXPIRE should apply and PTTL should return integer.");
    var pttl = ParseIntegerResponse(setResponse[(setResponse.LastIndexOf(':'))..]);
    Assert(pttl is > 0 and <= 500, "PTTL should be positive and not exceed requested TTL.");
}

static async Task ExpireAtInPastRemovesKeyAsync()
{
    var processor = CreateProcessor();
    var past = DateTimeOffset.UtcNow.AddSeconds(-1).ToUnixTimeSeconds().ToString();
    var response = await ExecuteAsync(processor,
        RespCommand("SET", "past:key", "v") +
        RespCommand("EXPIREAT", "past:key", past) +
        RespCommand("GET", "past:key"));

    AssertEqual("+OK\r\n:1\r\n$-1\r\n", response);
}

static async Task SetWithExAtOptionSetsExpiryAsync()
{
    var processor = CreateProcessor();
    var exat = DateTimeOffset.UtcNow.AddSeconds(10).ToUnixTimeSeconds().ToString();
    var response = await ExecuteAsync(processor,
        RespCommand("SET", "exat:key", "v", "EXAT", exat) +
        RespCommand("TTL", "exat:key"));

    Assert(response.StartsWith("+OK\r\n:", StringComparison.Ordinal), "SET EXAT should return OK then TTL integer.");
    var ttl = ParseIntegerResponse(response[(response.LastIndexOf(':'))..]);
    Assert(ttl is > 0 and <= 10, "EXAT should set a near-future TTL.");
}

static async Task SetExRejectsOverflowTtlAsync()
{
    var processor = CreateProcessor();
    AssertEqual("-ERR value is not an integer or out of range\r\n", await ExecuteAsync(processor, RespCommand("SETEX", "bad", "9223372036854775807", "v")));
    AssertEqual("-ERR value is not an integer or out of range\r\n", await ExecuteAsync(processor, RespCommand("SET", "bad", "v", "PX", "9223372036854775807")));
}

static Task VectorClockIncrementCompareAndMergeAsync()
{
    var left = VectorClock.Empty.Increment("node-a").Increment("node-a");
    var right = VectorClock.Empty.Increment("node-b");

    AssertEqual(VectorClockComparison.Concurrent, left.Compare(right));
    AssertEqual(VectorClockComparison.Before, right.Compare(left.Merge(right)));
    AssertEqual(VectorClockComparison.After, left.Merge(right).Compare(right));
    AssertEqual(2L, left.Merge(right).Entries["node-a"]);
    AssertEqual(1L, left.Merge(right).Entries["node-b"]);
    return Task.CompletedTask;
}

static Task LwwRegisterTieBreakIsDeterministicAsync()
{
    var strategy = LwwRegisterMergeStrategy.Instance;
    var timestamp = DateTimeOffset.UtcNow;
    var existing = new CrdtValue(
        Encoding.UTF8.GetBytes("left"),
        new VectorClock(new Dictionary<string, long>(StringComparer.Ordinal) { ["a"] = 1 }),
        timestamp,
        "node-a");
    var incoming = new CrdtValue(
        Encoding.UTF8.GetBytes("right"),
        new VectorClock(new Dictionary<string, long>(StringComparer.Ordinal) { ["b"] = 1 }),
        timestamp,
        "node-b");

    var merged = strategy.Merge("k", existing, incoming);
    AssertEqual("right", Encoding.UTF8.GetString(merged.Value));
    AssertEqual(VectorClockComparison.Equal, merged.VectorClock.Compare(new VectorClock(new Dictionary<string, long>(StringComparer.Ordinal) { ["a"] = 1, ["b"] = 1 })));
    return Task.CompletedTask;
}

static Task OrSetAddRemoveAndConcurrentMergeAsync()
{
    var left = OrSetValue.Empty.Add("alpha", "node-a:1");
    var right = OrSetValue.Empty.Remove("alpha").Add("beta", "node-b:1");
    AssertSequenceEqual(new[] { "beta" }, right.Elements);
    var merged = left.Merge(right);
    AssertSequenceEqual(new[] { "alpha", "beta" }, merged.Elements);

    var removed = merged.Remove("alpha");
    AssertSequenceEqual(new[] { "beta" }, removed.Elements);
    return Task.CompletedTask;
}

static Task PnCounterIncrementDecrementMergeAsync()
{
    var left = PnCounterValue.Zero.Increment("node-a", 3).Decrement("node-a", 1);
    var right = PnCounterValue.Zero.Increment("node-b", 2).Decrement("node-b", 1);
    var merged = left.Merge(right);

    AssertEqual(3L, merged.Value);
    return Task.CompletedTask;
}

static async Task CrdtMergeMethodUsesDefaultLwwRegisterAsync()
{
    var store = new CrdtKeyValueStore(new CountingKeyValueStore(), nodeId: "node-a");
    await store.PutAsync("doc", Encoding.UTF8.GetBytes("v1"));
    await store.MergeAsync("doc", Encoding.UTF8.GetBytes("v2"));

    AssertEqual("v2", Encoding.UTF8.GetString((await store.GetAsync("doc"))!));
}

static async Task CustomMergeStrategyCanBeConfiguredPerKeyAsync()
{
    var store = new CrdtKeyValueStore(new CountingKeyValueStore(), nodeId: "node-a");
    await store.SetKeyOptionsAsync("set:key", new KeyOptions { MergeStrategy = OrSetMergeStrategy.Instance });

    await store.PutAsync("set:key", OrSetValue.Empty.Add("one", "node-a:1").ToByteArray());
    await store.MergeAsync("set:key", OrSetValue.Empty.Add("two", "node-b:1").ToByteArray());

    var merged = OrSetValue.FromByteArray((await store.GetAsync("set:key"))!);
    AssertSequenceEqual(new[] { "one", "two" }, merged.Elements);
}

static async Task TwoInstancesMergeConcurrentWritesDeterministicallyAsync()
{
    var swarm = new InMemorySwarmClient();
    var index = new InMemoryKeyIndex();
    var storeA = new CrdtKeyValueStore(new SwarmKeyValueStore(swarm, index), nodeId: "node-a");
    var storeB = new CrdtKeyValueStore(new SwarmKeyValueStore(swarm, index), nodeId: "node-b");

    await storeA.SetKeyOptionsAsync("shared:set", new KeyOptions { MergeStrategy = OrSetMergeStrategy.Instance });
    await storeB.SetKeyOptionsAsync("shared:set", new KeyOptions { MergeStrategy = OrSetMergeStrategy.Instance });

    await storeA.PutAsync("shared:set", OrSetValue.Empty.Add("alice", "node-a:1").ToByteArray());
    await storeB.MergeAsync("shared:set", OrSetValue.Empty.Add("bob", "node-b:1").ToByteArray());

    var fromA = OrSetValue.FromByteArray((await storeA.GetAsync("shared:set"))!);
    var fromB = OrSetValue.FromByteArray((await storeB.GetAsync("shared:set"))!);
    AssertSequenceEqual(new[] { "alice", "bob" }, fromA.Elements);
    AssertSequenceEqual(fromA.Elements, fromB.Elements);
}

static async Task CachingKeyValueStoreGetReturnsCachedValueAfterPutAsync()
{
    var inner = new CountingKeyValueStore();
    var store = CreateCachingStore(inner, maxEntries: 8);
    await store.PutAsync("hot:key", Encoding.UTF8.GetBytes("v1"));

    AssertEqual("v1", Encoding.UTF8.GetString((await store.GetAsync("hot:key"))!));
    AssertEqual("v1", Encoding.UTF8.GetString((await store.GetAsync("hot:key"))!));
    AssertEqual(1, inner.GetCallCount("hot:key"));
}

static async Task CachingKeyValueStorePutInvalidatesCacheAsync()
{
    var inner = new CountingKeyValueStore();
    var store = CreateCachingStore(inner, maxEntries: 8);
    await store.PutAsync("hot:key", Encoding.UTF8.GetBytes("v1"));
    _ = await store.GetAsync("hot:key");

    await store.PutAsync("hot:key", Encoding.UTF8.GetBytes("v2"));
    var reloaded = await store.GetAsync("hot:key");

    AssertEqual("v2", Encoding.UTF8.GetString(reloaded!));
    AssertEqual(2, inner.GetCallCount("hot:key"));
}

static async Task CachingKeyValueStoreDeleteInvalidatesCacheAsync()
{
    var inner = new CountingKeyValueStore();
    var store = CreateCachingStore(inner, maxEntries: 8);
    await store.PutAsync("hot:key", Encoding.UTF8.GetBytes("v1"));
    _ = await store.GetAsync("hot:key");
    Assert(await store.DeleteAsync("hot:key"), "Delete should return true for existing key.");

    var afterDelete = await store.GetAsync("hot:key");

    AssertEqual(null, afterDelete);
    AssertEqual(2, inner.GetCallCount("hot:key"));
}

static async Task CachingKeyValueStoreRespectsKeyTtlAsync()
{
    var inner = new CountingKeyValueStore();
    var store = CreateCachingStore(inner, maxEntries: 8, defaultEntryTtl: TimeSpan.FromMinutes(1));
    await store.PutAsync("ttl:key", Encoding.UTF8.GetBytes("v1"));
    Assert(await store.SetTtlAsync("ttl:key", TimeSpan.FromSeconds(1)), "SetTtlAsync should succeed.");
    _ = await store.GetAsync("ttl:key");

    var afterExpiry = await WaitUntilValueAsync(
        action: () => store.GetAsync("ttl:key"),
        predicate: value => value is null,
        timeout: TimeSpan.FromSeconds(2),
        pollInterval: TimeSpan.FromMilliseconds(50));

    AssertEqual(null, afterExpiry);
    Assert(inner.GetCallCount("ttl:key") >= 2, "Expected at least one re-read after TTL expiry.");
}

static async Task CachingKeyValueStoreMaxEntriesEvictsLruAsync()
{
    var inner = new CountingKeyValueStore();
    var store = CreateCachingStore(inner, maxEntries: 1);
    await store.PutAsync("a", Encoding.UTF8.GetBytes("A"));
    await store.PutAsync("b", Encoding.UTF8.GetBytes("B"));

    _ = await store.GetAsync("a");
    _ = await store.GetAsync("b");
    _ = await store.GetAsync("a");

    AssertEqual(2, inner.GetCallCount("a"));
    Assert(store.Evictions > 0, "Expected at least one cache eviction.");
}

static RedisCommandProcessor CreateProcessor() =>
    new(new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex()));

static CachingKeyValueStore CreateCachingStore(CountingKeyValueStore inner, int maxEntries, TimeSpan? defaultEntryTtl = null)
{
    var options = Options.Create(new CacheOptions
    {
        Enabled = true,
        MaxEntries = maxEntries,
        DefaultEntryTtl = defaultEntryTtl
    });
    var cache = new MemoryCache(new MemoryCacheOptions());
    return new CachingKeyValueStore(inner, cache, options, NullLogger<CachingKeyValueStore>.Instance);
}

static AsyncQueuedKeyValueStore CreateAsyncQueuedStore(
    IKeyValueStore inner,
    int maxConcurrentWrites,
    int batchSize = 64,
    int flushIntervalMs = 5,
    Microsoft.Extensions.Logging.ILogger<AsyncQueuedKeyValueStore>? logger = null) =>
    new(
        inner,
        new AsyncProcessingOptions
        {
            Enabled = true,
            MaxConcurrentWrites = maxConcurrentWrites,
            WriteBatchSize = batchSize,
            BatchFlushIntervalMs = flushIntervalMs
        },
        logger ?? NullLogger<AsyncQueuedKeyValueStore>.Instance);

static CompressingKeyValueStore CreateCompressingStore(IKeyValueStore inner, bool enabled = true, CompressionAlgorithm algorithm = CompressionAlgorithm.GZip, int minSizeBytes = 0)
{
    var options = Options.Create(new CompressionOptions
    {
        Enabled = enabled,
        Algorithm = algorithm,
        MinSizeBytes = minSizeBytes
    });
    return new CompressingKeyValueStore(inner, options, NullLogger<CompressingKeyValueStore>.Instance);
}

static async Task CompressingKeyValueStorePutStoresCompressedValueAsync()
{
    var inner = new CountingKeyValueStore();
    var store = CreateCompressingStore(inner, minSizeBytes: 0);
    var original = Encoding.UTF8.GetBytes(new string('x', 200));

    await store.PutAsync("compressed:key", original);

    var stored = await inner.GetAsync("compressed:key")
        ?? throw new InvalidOperationException("Inner store should have data.");
    Assert(!stored.SequenceEqual(original), "Stored bytes should be compressed (different from original).");
    // GZip magic
    Assert(stored[0] == 0x1F && stored[1] == 0x8B, "Stored data should start with GZip magic bytes.");
}

static async Task CompressingKeyValueStoreGetReturnsDecompressedValueAsync()
{
    var inner = new CountingKeyValueStore();
    var store = CreateCompressingStore(inner, minSizeBytes: 0);
    var original = Encoding.UTF8.GetBytes(new string('y', 200));

    await store.PutAsync("roundtrip:key", original);
    var retrieved = await store.GetAsync("roundtrip:key");

    Assert(retrieved is not null, "Retrieved value should not be null.");
    Assert(retrieved!.SequenceEqual(original), "Retrieved value should equal the original.");
}

static async Task CompressingKeyValueStoreSkipsCompressionBelowMinSizeAsync()
{
    var inner = new CountingKeyValueStore();
    var store = CreateCompressingStore(inner, minSizeBytes: 100);
    var small = Encoding.UTF8.GetBytes("tiny");

    await store.PutAsync("small:key", small);

    var stored = await inner.GetAsync("small:key");
    Assert(stored is not null, "Inner store should have data.");
    Assert(stored!.SequenceEqual(small), "Small value should be stored uncompressed.");
}

static async Task CompressingKeyValueStoreHandlesLegacyUncompressedDataAsync()
{
    var inner = new CountingKeyValueStore();
    var legacy = Encoding.UTF8.GetBytes("legacy-plain-text-value");
    // Directly put uncompressed data into the inner store (simulates old data)
    await inner.PutAsync("legacy:key", legacy);

    var store = CreateCompressingStore(inner, minSizeBytes: 0);
    var retrieved = await store.GetAsync("legacy:key");

    Assert(retrieved is not null, "Legacy value should be readable.");
    Assert(retrieved!.SequenceEqual(legacy), "Legacy uncompressed value should be returned as-is.");
}

static async Task CompressingKeyValueStoreBrotliCompressAndDecompressAsync()
{
    var inner = new CountingKeyValueStore();
    var store = CreateCompressingStore(inner, algorithm: CompressionAlgorithm.Brotli, minSizeBytes: 0);
    var original = Encoding.UTF8.GetBytes(new string('z', 200));

    await store.PutAsync("brotli:key", original);
    var stored = await inner.GetAsync("brotli:key")
        ?? throw new InvalidOperationException("Inner store should have data.");

    Assert(!stored.SequenceEqual(original), "Stored bytes should be compressed.");
    // Custom magic prefix 0xCE 0xB8 (SwarmKeyDb-specific Brotli wrapper)
    Assert(stored[0] == 0xCE && stored[1] == 0xB8, "Stored data should start with custom Brotli magic bytes.");

    var retrieved = await store.GetAsync("brotli:key");
    Assert(retrieved is not null, "Retrieved value should not be null.");
    Assert(retrieved!.SequenceEqual(original), "Brotli roundtrip should return original value.");
}

static async Task CompressingKeyValueStoreDeleteAndTtlPassThroughAsync()
{
    var inner = new CountingKeyValueStore();
    var store = CreateCompressingStore(inner, minSizeBytes: 0);
    var value = Encoding.UTF8.GetBytes("hello");

    await store.PutAsync("pass:key", value);
    Assert(await store.SetTtlAsync("pass:key", TimeSpan.FromMinutes(1)), "SetTtlAsync should pass through.");

    var (exists, ttl) = await store.GetTtlAsync("pass:key");
    Assert(exists, "GetTtlAsync should show key exists.");
    Assert(ttl is not null, "GetTtlAsync should return a TTL.");

    Assert(await store.RemoveTtlAsync("pass:key"), "RemoveTtlAsync should pass through.");
    Assert(await store.DeleteAsync("pass:key"), "DeleteAsync should pass through.");

    var keys = await store.ListKeysAsync();
    Assert(!keys.Contains("pass:key"), "ListKeysAsync should show key deleted.");
}

static EncryptingKeyValueStore CreateEncryptingStore(IKeyValueStore inner, string? keyHex = null, string? ethPrivKeyHex = null, bool enabled = true)
{
    var options = Options.Create(new EncryptionOptions
    {
        Enabled = enabled,
        KeyHex = keyHex,
        EthPrivateKeyHex = ethPrivKeyHex
    });
    return new EncryptingKeyValueStore(inner, options, NullLogger<EncryptingKeyValueStore>.Instance);
}

static AclKeyValueStore CreateAclStore(IKeyValueStore inner, IEthAddressAccessor? accessor = null, bool enabled = true, AclMode mode = AclMode.Allowlist, params AclEntry[] entries)
{
    var options = Options.Create(new AclOptions
    {
        Enabled = enabled,
        Mode = mode,
        Entries = entries.ToList()
    });
    return new AclKeyValueStore(inner, options, accessor ?? new AsyncLocalEthAddressAccessor());
}

static string MakeKeyHex() => Convert.ToHexString(System.Security.Cryptography.RandomNumberGenerator.GetBytes(32));

const string AllowedAddress = "0x1111111111111111111111111111111111111111";
const string OtherAddress = "0x2222222222222222222222222222222222222222";
const string BlockedAddress = "0x3333333333333333333333333333333333333333";

static async Task EncryptingKeyValueStorePutStoresEncryptedValueAsync()
{
    var inner = new CountingKeyValueStore();
    var keyHex = MakeKeyHex();
    var store = CreateEncryptingStore(inner, keyHex: keyHex);
    var original = System.Text.Encoding.UTF8.GetBytes("secret value");

    await store.PutAsync("enc:key", original);

    var stored = await inner.GetAsync("enc:key")
        ?? throw new InvalidOperationException("Inner store should have data.");
    Assert(!stored.SequenceEqual(original), "Stored bytes should be encrypted (different from original).");
    // Magic bytes 0xAE 0x73
    Assert(stored[0] == 0xAE && stored[1] == 0x73, "Stored data should start with encryption magic bytes 0xAE 0x73.");
}

static async Task EncryptingKeyValueStoreGetReturnsDecryptedValueAsync()
{
    var inner = new CountingKeyValueStore();
    var keyHex = MakeKeyHex();
    var store = CreateEncryptingStore(inner, keyHex: keyHex);
    var original = System.Text.Encoding.UTF8.GetBytes("another secret");

    await store.PutAsync("dec:key", original);
    var retrieved = await store.GetAsync("dec:key");

    Assert(retrieved is not null, "Retrieved value should not be null.");
    Assert(retrieved!.SequenceEqual(original), "Decrypted value should equal the original plaintext.");
}

static async Task EncryptingKeyValueStoreNonceIsRandomSameValueDifferentCiphertextAsync()
{
    var inner = new CountingKeyValueStore();
    var keyHex = MakeKeyHex();
    var store = CreateEncryptingStore(inner, keyHex: keyHex);
    var original = System.Text.Encoding.UTF8.GetBytes("determinism test");

    await store.PutAsync("nonce:key", original);
    var first = (await inner.GetAsync("nonce:key"))!.ToArray();

    await store.PutAsync("nonce:key", original);
    var second = (await inner.GetAsync("nonce:key"))!.ToArray();

    Assert(!first.SequenceEqual(second), "Two encryptions of the same value must produce different ciphertext (random nonce).");
}

static async Task EncryptingKeyValueStoreLegacyUnencryptedDataReturnedUnchangedAsync()
{
    var inner = new CountingKeyValueStore();
    var keyHex = MakeKeyHex();
    var legacy = System.Text.Encoding.UTF8.GetBytes("old plaintext value");
    // Store raw bytes directly in the inner store (no magic bytes).
    await inner.PutAsync("legacy:key", legacy);

    var store = CreateEncryptingStore(inner, keyHex: keyHex);
    var retrieved = await store.GetAsync("legacy:key");

    Assert(retrieved is not null, "Legacy value should be readable.");
    Assert(retrieved!.SequenceEqual(legacy), "Legacy unencrypted value should be returned as-is.");
}

static async Task EncryptingKeyValueStoreTamperedCiphertextThrowsCryptographicExceptionAsync()
{
    var inner = new CountingKeyValueStore();
    var keyHex = MakeKeyHex();
    var store = CreateEncryptingStore(inner, keyHex: keyHex);
    var original = System.Text.Encoding.UTF8.GetBytes("tamper test");

    await store.PutAsync("tamper:key", original);

    // Flip a bit in the stored ciphertext (after the 30-byte header).
    var stored = (await inner.GetAsync("tamper:key"))!;
    stored[stored.Length - 1] ^= 0xFF;
    await inner.PutAsync("tamper:key", stored);

    var threw = false;
    try
    {
        await store.GetAsync("tamper:key");
    }
    catch (System.Security.Cryptography.CryptographicException)
    {
        threw = true;
    }

    Assert(threw, "Tampered ciphertext must throw CryptographicException (GCM tag verification failure).");
}

static async Task EncryptingKeyValueStoreDeleteAndTtlPassThroughAsync()
{
    var inner = new CountingKeyValueStore();
    var keyHex = MakeKeyHex();
    var store = CreateEncryptingStore(inner, keyHex: keyHex);
    var value = System.Text.Encoding.UTF8.GetBytes("ttl test");

    await store.PutAsync("pass:key", value);
    Assert(await store.SetTtlAsync("pass:key", TimeSpan.FromMinutes(1)), "SetTtlAsync should pass through.");

    var (exists, ttl) = await store.GetTtlAsync("pass:key");
    Assert(exists, "GetTtlAsync should show key exists.");
    Assert(ttl is not null, "GetTtlAsync should return a TTL.");

    Assert(await store.RemoveTtlAsync("pass:key"), "RemoveTtlAsync should pass through.");
    Assert(await store.DeleteAsync("pass:key"), "DeleteAsync should pass through.");

    var keys = await store.ListKeysAsync();
    Assert(!keys.Contains("pass:key"), "ListKeysAsync should show key deleted.");
}

static Task EncryptingKeyValueStoreEthereumKeyDerivationProducesConsistentKeyAsync()
{
    // A well-known Ethereum private key (test vector, NOT for production use).
    var ethPrivKeyHex = "4c0883a69102937d6231471b5dbb6e538eba2ef45d64b07157e4bcef88d9bdba";

    var key1 = EncryptingKeyValueStore.DeriveKeyFromEthPrivateKey(ethPrivKeyHex);
    var key2 = EncryptingKeyValueStore.DeriveKeyFromEthPrivateKey(ethPrivKeyHex);

    Assert(key1.Length == 32, "Derived key should be 32 bytes.");
    Assert(key1.SequenceEqual(key2), "Same Ethereum private key must always derive the same AES key.");

    // Different Ethereum key must produce a different AES key.
    var differentEthKey = "1c0883a69102937d6231471b5dbb6e538eba2ef45d64b07157e4bcef88d9bdba";
    var key3 = EncryptingKeyValueStore.DeriveKeyFromEthPrivateKey(differentEthKey);
    Assert(!key1.SequenceEqual(key3), "Different Ethereum keys must derive different AES keys.");

    return Task.CompletedTask;
}

static Task EncryptingKeyValueStoreStartupFailsWhenEnabledWithNoKeyAsync()
{
    var inner = new CountingKeyValueStore();
    var options = Options.Create(new EncryptionOptions { Enabled = true, KeyHex = null, EthPrivateKeyHex = null });

    var threw = false;
    try
    {
        _ = new EncryptingKeyValueStore(inner, options, NullLogger<EncryptingKeyValueStore>.Instance);
    }
    catch (InvalidOperationException ex) when (ex.Message.Contains("no key is configured"))
    {
        threw = true;
    }

    Assert(threw, "Constructor must throw InvalidOperationException when encryption is enabled but no key is configured.");
    return Task.CompletedTask;
}

static async Task AclAllowlistReadAddressCanGetAsync()
{
    var inner = new CountingKeyValueStore();
    await inner.PutAsync("shared:key", Encoding.UTF8.GetBytes("value"));
    var accessor = new AsyncLocalEthAddressAccessor { CurrentAddress = AllowedAddress };
    var store = CreateAclStore(inner, accessor, true, AclMode.Allowlist,
        new AclEntry { EthAddress = AllowedAddress, Permission = AclPermission.Read });

    var value = await store.GetAsync("shared:key");

    AssertEqual("value", Encoding.UTF8.GetString(value!));
}

static async Task AclAllowlistWriteAddressCanPutAndDeleteAsync()
{
    var inner = new CountingKeyValueStore();
    var accessor = new AsyncLocalEthAddressAccessor { CurrentAddress = AllowedAddress };
    var store = CreateAclStore(inner, accessor, true, AclMode.Allowlist,
        new AclEntry { EthAddress = AllowedAddress, Permission = AclPermission.Write });

    await store.PutAsync("shared:key", Encoding.UTF8.GetBytes("value"));
    Assert(await store.DeleteAsync("shared:key"), "Write permission should allow delete.");
}

static async Task AclAllowlistUnlistedAddressIsDeniedOnGetAsync()
{
    var inner = new CountingKeyValueStore();
    await inner.PutAsync("shared:key", Encoding.UTF8.GetBytes("value"));
    var accessor = new AsyncLocalEthAddressAccessor { CurrentAddress = OtherAddress };
    var store = CreateAclStore(inner, accessor, true, AclMode.Allowlist,
        new AclEntry { EthAddress = AllowedAddress, Permission = AclPermission.Read });

    await AssertAccessDeniedAsync(
        async () => _ = await store.GetAsync("shared:key"),
        $"Access denied: address {EthereumAddress.Normalize(OtherAddress)} does not have read permission.");
}

static async Task AclAllowlistUnlistedAddressIsDeniedOnPutAsync()
{
    var inner = new CountingKeyValueStore();
    var accessor = new AsyncLocalEthAddressAccessor { CurrentAddress = OtherAddress };
    var store = CreateAclStore(inner, accessor, true, AclMode.Allowlist,
        new AclEntry { EthAddress = AllowedAddress, Permission = AclPermission.Write });

    await AssertAccessDeniedAsync(
        () => store.PutAsync("shared:key", Encoding.UTF8.GetBytes("value")),
        $"Access denied: address {EthereumAddress.Normalize(OtherAddress)} does not have write permission.");
}

static async Task AclAllowlistAdminGrantsReadAndWriteAsync()
{
    var inner = new CountingKeyValueStore();
    var accessor = new AsyncLocalEthAddressAccessor { CurrentAddress = AllowedAddress };
    var store = CreateAclStore(inner, accessor, true, AclMode.Allowlist,
        new AclEntry { EthAddress = AllowedAddress, Permission = AclPermission.Admin });

    await store.PutAsync("shared:key", Encoding.UTF8.GetBytes("value"));
    var value = await store.GetAsync("shared:key");
    AssertEqual("value", Encoding.UTF8.GetString(value!));
    Assert(await store.DeleteAsync("shared:key"), "Admin permission should allow delete.");
}

static async Task AclDenylistBlockedAddressIsDeniedAndNonBlockedAddressIsAllowedAsync()
{
    var inner = new CountingKeyValueStore();
    await inner.PutAsync("shared:key", Encoding.UTF8.GetBytes("value"));
    var accessor = new AsyncLocalEthAddressAccessor { CurrentAddress = BlockedAddress };
    var store = CreateAclStore(inner, accessor, true, AclMode.Denylist,
        new AclEntry { EthAddress = BlockedAddress, Permission = AclPermission.Admin });

    await AssertAccessDeniedAsync(
        async () => _ = await store.GetAsync("shared:key"),
        $"Access denied: address {EthereumAddress.Normalize(BlockedAddress)} does not have read permission.");

    accessor.CurrentAddress = AllowedAddress;
    var value = await store.GetAsync("shared:key");
    AssertEqual("value", Encoding.UTF8.GetString(value!));
}

static async Task AclDisabledPassesAllOperationsThroughAsync()
{
    var inner = new CountingKeyValueStore();
    var store = CreateAclStore(inner, enabled: false);

    await store.PutAsync("open:key", Encoding.UTF8.GetBytes("value"));
    AssertEqual("value", Encoding.UTF8.GetString((await store.GetAsync("open:key"))!));
    Assert(await store.SetTtlAsync("open:key", TimeSpan.FromMinutes(1)), "Disabled ACL should not block TTL updates.");
    Assert(await store.DeleteAsync("open:key"), "Disabled ACL should not block deletes.");
}

static Task AclStartupFailsWhenEnabledWithEmptyEntriesAsync()
{
    var inner = new CountingKeyValueStore();
    var options = Options.Create(new AclOptions { Enabled = true, Mode = AclMode.Allowlist, Entries = [] });

    var threw = false;
    try
    {
        _ = new AclKeyValueStore(inner, options, new AsyncLocalEthAddressAccessor());
    }
    catch (InvalidOperationException ex) when (ex.Message.Contains("SWARM_KEYDB_ACL_ENTRIES", StringComparison.Ordinal))
    {
        threw = true;
    }

    Assert(threw, "Constructor must throw InvalidOperationException when ACL is enabled but no entries are configured.");
    return Task.CompletedTask;
}

static async Task ServiceCollectionPlacesAclBetweenSwarmAndEncryptionAsync()
{
    var services = new ServiceCollection();
    services.AddOptions();
    services.AddSingleton<IOptions<CacheOptions>>(Options.Create(new CacheOptions { Enabled = true, MaxEntries = 8 }));
    services.AddSingleton<IOptions<CompressionOptions>>(Options.Create(new CompressionOptions { Enabled = true, MinSizeBytes = 0 }));
    services.AddSingleton<IOptions<EncryptionOptions>>(Options.Create(new EncryptionOptions { Enabled = true, KeyHex = MakeKeyHex() }));
    services.AddSingleton<IOptions<AclOptions>>(Options.Create(new AclOptions
    {
        Enabled = true,
        Mode = AclMode.Allowlist,
        Entries = [new AclEntry { EthAddress = AllowedAddress, Permission = AclPermission.Admin }]
    }));
    services.AddSingleton<IEthAddressAccessor>(new AsyncLocalEthAddressAccessor { CurrentAddress = AllowedAddress });
    services.AddSingleton<Microsoft.Extensions.Caching.Memory.IMemoryCache>(new MemoryCache(new MemoryCacheOptions()));
    services.AddSingleton<Microsoft.Extensions.Logging.ILogger<CachingKeyValueStore>>(NullLogger<CachingKeyValueStore>.Instance);
    services.AddSingleton<Microsoft.Extensions.Logging.ILogger<CompressingKeyValueStore>>(NullLogger<CompressingKeyValueStore>.Instance);
    services.AddSingleton<Microsoft.Extensions.Logging.ILogger<EncryptingKeyValueStore>>(NullLogger<EncryptingKeyValueStore>.Instance);
    services.AddSwarmKeyDbStore(new InMemorySwarmClient(), new InMemoryKeyIndex());

    using var provider = services.BuildServiceProvider();
    var store = provider.GetRequiredService<IKeyValueStore>();

    AssertEqual(typeof(CachingKeyValueStore), store.GetType());
    AssertEqual(typeof(CrdtKeyValueStore), GetInnerStoreAtDepth(store, depth: 1).GetType());
    AssertEqual(typeof(CompressingKeyValueStore), GetInnerStoreAtDepth(store, depth: 2).GetType());
    AssertEqual(typeof(EncryptingKeyValueStore), GetInnerStoreAtDepth(store, depth: 3).GetType());
    AssertEqual(typeof(AclKeyValueStore), GetInnerStoreAtDepth(store, depth: 4).GetType());
    AssertEqual(typeof(SwarmKeyValueStore), GetInnerStoreAtDepth(store, depth: 5).GetType());

    await store.PutAsync("pipeline:key", Encoding.UTF8.GetBytes("value"));
    AssertEqual("value", Encoding.UTF8.GetString((await store.GetAsync("pipeline:key"))!));
}

static async Task CachedReadStillRequiresAclPermissionAsync()
{
    var services = new ServiceCollection();
    services.AddOptions();
    services.AddSingleton<IOptions<CacheOptions>>(Options.Create(new CacheOptions { Enabled = true, MaxEntries = 8 }));
    services.AddSingleton<IOptions<CompressionOptions>>(Options.Create(new CompressionOptions { Enabled = true, MinSizeBytes = 0 }));
    services.AddSingleton<IOptions<EncryptionOptions>>(Options.Create(new EncryptionOptions { Enabled = true, KeyHex = MakeKeyHex() }));
    services.AddSingleton<IOptions<AclOptions>>(Options.Create(new AclOptions
    {
        Enabled = true,
        Mode = AclMode.Allowlist,
        Entries = [new AclEntry { EthAddress = AllowedAddress, Permission = AclPermission.Admin }]
    }));
    var accessor = new AsyncLocalEthAddressAccessor();
    services.AddSingleton<IEthAddressAccessor>(accessor);
    services.AddSingleton<Microsoft.Extensions.Caching.Memory.IMemoryCache>(new MemoryCache(new MemoryCacheOptions()));
    services.AddSingleton<Microsoft.Extensions.Logging.ILogger<CachingKeyValueStore>>(NullLogger<CachingKeyValueStore>.Instance);
    services.AddSingleton<Microsoft.Extensions.Logging.ILogger<CompressingKeyValueStore>>(NullLogger<CompressingKeyValueStore>.Instance);
    services.AddSingleton<Microsoft.Extensions.Logging.ILogger<EncryptingKeyValueStore>>(NullLogger<EncryptingKeyValueStore>.Instance);
    services.AddSwarmKeyDbStore(new InMemorySwarmClient(), new InMemoryKeyIndex());

    using var provider = services.BuildServiceProvider();
    var processor = new RedisCommandProcessor(provider.GetRequiredService<IKeyValueStore>(), accessor);

    var allowed = await ExecuteAsync(processor,
        RespCommand("AUTHADDR", AllowedAddress) +
        RespCommand("SET", "shared:key", "value") +
        RespCommand("GET", "shared:key"));
    AssertEqual("+OK\r\n+OK\r\n$5\r\nvalue\r\n", allowed);

    var denied = await ExecuteAsync(processor,
        RespCommand("AUTHADDR", OtherAddress) +
        RespCommand("GET", "shared:key"));
    AssertEqual(
        $"+OK\r\n-ERR Access denied: address {EthereumAddress.Normalize(OtherAddress)} does not have read permission.\r\n",
        denied);
}

static async Task RedisProtocolReturnsAccessDeniedErrorForUnauthorizedAddressAsync()
{
    var accessor = new AsyncLocalEthAddressAccessor();
    var store = CreateAclStore(new CountingKeyValueStore(), accessor, true, AclMode.Allowlist,
        new AclEntry { EthAddress = AllowedAddress, Permission = AclPermission.Read });
    var processor = new RedisCommandProcessor(store, accessor);

    var response = await ExecuteAsync(processor,
        RespCommand("AUTHADDR", OtherAddress) +
        RespCommand("GET", "shared:key"));

    AssertEqual(
        $"+OK\r\n-ERR Access denied: address {EthereumAddress.Normalize(OtherAddress)} does not have read permission.\r\n",
        response);
}

static Task CompositeKeyConstructsKeyFromSegmentsAsync()
{
    AssertEqual("a:b:c", CompositeKey.Of("a", "b", "c"));
    AssertEqual("users:alice:profile", CompositeKey.Of("users", "alice", "profile"));
    AssertEqual("single", CompositeKey.Of("single"));
    return Task.CompletedTask;
}

static Task CompositeKeyRejectsSegmentContainingSeparatorAsync()
{
    var threw = false;
    try
    {
        _ = CompositeKey.Of("a", "b:c");
    }
    catch (ArgumentException ex) when (ex.Message.Contains("separator", StringComparison.Ordinal))
    {
        threw = true;
    }

    Assert(threw, "Expected ArgumentException when a segment contains the separator character.");
    return Task.CompletedTask;
}

static Task CompositeKeyRejectsEmptySegmentAsync()
{
    var threw = false;
    try
    {
        _ = CompositeKey.Of("a", "", "c");
    }
    catch (ArgumentException)
    {
        threw = true;
    }

    Assert(threw, "Expected ArgumentException when a segment is empty.");

    // Also verify null segment throws
    threw = false;
    try
    {
        _ = CompositeKey.Of("a", null!, "c");
    }
    catch (ArgumentException)
    {
        threw = true;
    }

    Assert(threw, "Expected ArgumentException when a segment is null.");
    return Task.CompletedTask;
}

static Task CompositeKeySupportsCustomSeparatorAsync()
{
    AssertEqual("users/alice/profile", CompositeKey.Of('/', "users", "alice", "profile"));

    var threw = false;
    try
    {
        _ = CompositeKey.Of('/', "users/alice", "profile");
    }
    catch (ArgumentException ex) when (ex.Message.Contains("separator", StringComparison.Ordinal))
    {
        threw = true;
    }

    Assert(threw, "Expected ArgumentException when segment contains custom separator.");
    return Task.CompletedTask;
}

static async Task NamespacedStoreScopesPutAndGetToPrefixAsync()
{
    var inner = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
    var ns = new NamespacedKeyValueStore(inner, "users:alice:");

    await ns.PutAsync("profile", Encoding.UTF8.GetBytes("Alice"));
    var value = await ns.GetAsync("profile");
    AssertEqual("Alice", Encoding.UTF8.GetString(value!));

    // Verify underlying store has prefixed key
    var rawValue = await inner.GetAsync("users:alice:profile");
    AssertEqual("Alice", Encoding.UTF8.GetString(rawValue!));

    // Key without prefix should not be visible through namespaced view
    var notFound = await ns.GetAsync("users:alice:profile");
    Assert(notFound is null, "Namespaced store should not find a key that includes the prefix in its name.");
}

static async Task NamespacedStoreListKeysStripsPrefixAsync()
{
    var inner = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
    await inner.PutAsync("users:alice:profile", Encoding.UTF8.GetBytes("1"));
    await inner.PutAsync("users:alice:settings", Encoding.UTF8.GetBytes("2"));
    await inner.PutAsync("users:bob:profile", Encoding.UTF8.GetBytes("3"));

    var ns = new NamespacedKeyValueStore(inner, "users:alice:");
    var keys = await ns.ListKeysAsync();

    AssertSequenceEqual(new[] { "profile", "settings" }, keys);
}

static async Task NamespacedStoreDeleteRemovesPrefixedKeyAsync()
{
    var inner = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
    var ns = new NamespacedKeyValueStore(inner, "ns:");

    await ns.PutAsync("key1", Encoding.UTF8.GetBytes("v1"));
    Assert(await ns.DeleteAsync("key1"), "Delete should return true for existing key.");
    Assert(await ns.GetAsync("key1") is null, "Key should be gone after delete.");

    // Underlying store should also have no prefixed key
    Assert(await inner.GetAsync("ns:key1") is null, "Underlying prefixed key should also be gone.");
}

static async Task NamespacedStoreIsolatesTwoNamespacesAsync()
{
    var inner = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
    var ns1 = new NamespacedKeyValueStore(inner, "ns1:");
    var ns2 = new NamespacedKeyValueStore(inner, "ns2:");

    await ns1.PutAsync("shared", Encoding.UTF8.GetBytes("from-ns1"));
    await ns2.PutAsync("shared", Encoding.UTF8.GetBytes("from-ns2"));

    AssertEqual("from-ns1", Encoding.UTF8.GetString((await ns1.GetAsync("shared"))!));
    AssertEqual("from-ns2", Encoding.UTF8.GetString((await ns2.GetAsync("shared"))!));

    var keys1 = await ns1.ListKeysAsync();
    var keys2 = await ns2.ListKeysAsync();
    AssertSequenceEqual(new[] { "shared" }, keys1);
    AssertSequenceEqual(new[] { "shared" }, keys2);
}

static async Task DeleteNamespaceRemovesAllKeysUnderPrefixAsync()
{
    IKeyValueStore store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
    await store.PutAsync("users:alice:profile", Encoding.UTF8.GetBytes("1"));
    await store.PutAsync("users:alice:settings", Encoding.UTF8.GetBytes("2"));
    await store.PutAsync("users:bob:profile", Encoding.UTF8.GetBytes("3"));

    var deleted = await store.DeleteNamespaceAsync("users:alice:");
    AssertEqual(2, deleted);

    var remaining = await store.ListKeysAsync();
    AssertSequenceEqual(new[] { "users:bob:profile" }, remaining);
}

static async Task WithNamespaceScopesClientOperationsAsync()
{
    var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
    var client = new SwarmKeyDbClient(store);

    var aliceDb = client.WithNamespace("users:alice:");
    await aliceDb.PutStringAsync("profile", "{\"name\":\"Alice\"}");
    await aliceDb.PutStringAsync("settings", "{\"theme\":\"dark\"}");

    var bobDb = client.WithNamespace("users:bob:");
    await bobDb.PutStringAsync("profile", "{\"name\":\"Bob\"}");

    // Scoped list returns only local keys
    var aliceKeys = await aliceDb.KeysAsync();
    AssertSequenceEqual(new[] { "profile", "settings" }, aliceKeys);

    var bobKeys = await bobDb.KeysAsync();
    AssertSequenceEqual(new[] { "profile" }, bobKeys);

    // Scoped get returns correct value
    AssertEqual("{\"name\":\"Alice\"}", await aliceDb.GetStringAsync("profile"));
    AssertEqual("{\"name\":\"Bob\"}", await bobDb.GetStringAsync("profile"));

    // Delete namespace via client
    var deletedCount = await client.DeleteNamespaceAsync("users:alice:");
    AssertEqual(2, deletedCount);

    var afterDelete = await aliceDb.KeysAsync();
    AssertSequenceEqual(Array.Empty<string>(), afterDelete);

    // Bob's data unaffected
    AssertEqual("{\"name\":\"Bob\"}", await bobDb.GetStringAsync("profile"));
}

static async Task CliDeleteNamespaceRemovesPrefixedKeysAsync()
{
    var options = CreateCliTestOptions();

    await RunCliAsync(new[] { "put", "users:alice:profile", "Alice" }, options);
    await RunCliAsync(new[] { "put", "users:alice:settings", "dark" }, options);
    await RunCliAsync(new[] { "put", "users:bob:profile", "Bob" }, options);

    var deleteResult = await RunCliAsync(new[] { "delete-namespace", "users:alice:" }, options);
    AssertEqual(0, deleteResult.ExitCode);
    AssertEqual("2", deleteResult.Stdout.Trim());

    var listResult = await RunCliAsync(new[] { "list" }, options);
    Assert(!listResult.Stdout.Contains("users:alice:", StringComparison.Ordinal), "Alice's keys should be deleted.");
    Assert(listResult.Stdout.Contains("users:bob:profile", StringComparison.Ordinal), "Bob's key should remain.");
}

static CliExecutionOptions CreateCliTestOptions()
{
    var swarm = new InMemorySwarmClient();
    var index = new InMemoryKeyIndex();
    return new CliExecutionOptions
    {
        SwarmClientFactory = _ => swarm,
        KeyIndexFactory = _ => index,
        EnvironmentFactory = static () => new EnvironmentSnapshot
        {
            Home = Path.Combine(Path.GetTempPath(), "swarm-keydb-cli-tests", Guid.NewGuid().ToString("N")),
            BeeUrl = "http://localhost:1633/",
            BatchId = "batch-id"
        }
    };
}

static async Task<(int ExitCode, string Stdout, string Stderr)> RunCliAsync(string[] args, CliExecutionOptions options)
{
    var stdout = new StringWriter();
    var stderr = new StringWriter();
    var code = await SwarmKeyDbCliApp.RunAsync(args, stdout, stderr, options);
    return (code, stdout.ToString(), stderr.ToString());
}

static async Task<string> ExecuteAsync(RedisCommandProcessor processor, string commands)
{
    await using var input = new MemoryStream(Encoding.UTF8.GetBytes(commands));
    await using var output = new MemoryStream();
    await processor.ProcessAsync(input, output);
    return Encoding.UTF8.GetString(output.ToArray());
}

static string RespCommand(params string[] parts)
{
    var builder = new StringBuilder();
    builder.Append('*').Append(parts.Length).Append("\r\n");
    foreach (var part in parts)
    {
        var bytes = Encoding.UTF8.GetBytes(part);
        builder.Append('$').Append(bytes.Length).Append("\r\n").Append(part).Append("\r\n");
    }

    return builder.ToString();
}

static async Task AssertAccessDeniedAsync(Func<Task> action, string expectedMessage)
{
    try
    {
        await action();
        throw new InvalidOperationException("Expected AccessDeniedException to be thrown.");
    }
    catch (AccessDeniedException ex)
    {
        AssertEqual(expectedMessage, ex.Message);
        AssertEqual(403, ex.StatusCode);
    }
}

static void Assert(bool condition, string message)
{
    if (!condition)
    {
        throw new InvalidOperationException(message);
    }
}

static void AssertEqual<T>(T expected, T actual)
{
    if (!EqualityComparer<T>.Default.Equals(expected, actual))
    {
        throw new InvalidOperationException($"Expected '{expected}', got '{actual}'.");
    }
}

static void AssertSequenceEqual<T>(IEnumerable<T> expected, IEnumerable<T> actual)
{
    var expectedArray = expected.ToArray();
    var actualArray = actual.ToArray();
    if (!expectedArray.SequenceEqual(actualArray))
    {
        throw new InvalidOperationException($"Expected [{string.Join(", ", expectedArray)}], got [{string.Join(", ", actualArray)}].");
    }
}

static long ParseIntegerResponse(string response)
{
    Assert(response.StartsWith(':') && response.EndsWith("\r\n", StringComparison.Ordinal), "Expected RESP integer response.");
    return long.Parse(response[1..^2]);
}

static async Task<T> WaitUntilValueAsync<T>(Func<Task<T>> action, Func<T, bool> predicate, TimeSpan timeout, TimeSpan pollInterval)
{
    var deadline = DateTimeOffset.UtcNow.Add(timeout);
    T lastValue = await action();
    while (DateTimeOffset.UtcNow < deadline)
    {
        if (predicate(lastValue))
        {
            return lastValue;
        }

        await Task.Delay(pollInterval);
        lastValue = await action();
    }

    return lastValue;
}

static IKeyValueStore GetInnerStore(object store)
{
    var field = store.GetType().GetField("_inner", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
    Assert(field is not null, $"Expected {store.GetType().Name} to expose an _inner field.");
    return (IKeyValueStore)field!.GetValue(store)!;
}

static IKeyValueStore GetInnerStoreAtDepth(IKeyValueStore store, int depth)
{
    var current = store;
    for (var i = 0; i < depth; i++)
    {
        current = GetInnerStore(current);
    }

    return current;
}

// ── BTreeKeyIndex tests ──────────────────────────────────────────────────────

static async Task BTreeIndexLookupInsertDeleteAsync()
{
    var index = new BTreeKeyIndex();

    // Insert and lookup
    await index.SetReferenceAsync("b", "ref-b");
    await index.SetReferenceAsync("a", "ref-a");
    await index.SetReferenceAsync("c", "ref-c");

    AssertEqual("ref-a", await index.GetReferenceAsync("a"));
    AssertEqual("ref-b", await index.GetReferenceAsync("b"));
    AssertEqual("ref-c", await index.GetReferenceAsync("c"));
    AssertEqual(null, await index.GetReferenceAsync("missing"));

    // Keys are returned in sorted order
    AssertSequenceEqual(new[] { "a", "b", "c" }, await index.ListKeysAsync());

    // Delete
    Assert(await index.RemoveAsync("b"), "Delete should return true for existing key.");
    Assert(!await index.RemoveAsync("b"), "Delete should return false for missing key.");
    AssertEqual(null, await index.GetReferenceAsync("b"));
    AssertSequenceEqual(new[] { "a", "c" }, await index.ListKeysAsync());
}

static async Task BTreeIndexRangeScanReturnsOrderedSubsetAsync()
{
    var index = new BTreeKeyIndex();
    foreach (var k in new[] { "k:1", "k:2", "k:3", "k:4", "k:5" })
    {
        await index.SetReferenceAsync(k, "ref");
    }

    // Inclusive bounds
    var range = await index.GetKeysInRangeAsync("k:2", "k:4", includeStart: true, includeEnd: true);
    AssertSequenceEqual(new[] { "k:2", "k:3", "k:4" }, range);

    // Exclusive start
    var excStart = await index.GetKeysInRangeAsync("k:2", "k:4", includeStart: false, includeEnd: true);
    AssertSequenceEqual(new[] { "k:3", "k:4" }, excStart);

    // Exclusive end
    var excEnd = await index.GetKeysInRangeAsync("k:2", "k:4", includeStart: true, includeEnd: false);
    AssertSequenceEqual(new[] { "k:2", "k:3" }, excEnd);

    // Open lower bound
    var openLow = await index.GetKeysInRangeAsync(null, "k:2", includeEnd: true);
    AssertSequenceEqual(new[] { "k:1", "k:2" }, openLow);

    // Open upper bound
    var openHigh = await index.GetKeysInRangeAsync("k:4", null, includeStart: true);
    AssertSequenceEqual(new[] { "k:4", "k:5" }, openHigh);
}

static async Task BTreeIndexPrefixScanUsesEfficientRangeAsync()
{
    var index = new BTreeKeyIndex();
    await index.SetReferenceAsync("user:alice:profile", "r1");
    await index.SetReferenceAsync("user:alice:settings", "r2");
    await index.SetReferenceAsync("user:bob:profile", "r3");
    await index.SetReferenceAsync("zzz:other", "r4");

    // Prefix range: ["user:alice:", "user:alice;") — ';' is the char after ':'
    var aliceKeys = await index.GetKeysInRangeAsync("user:alice:", "user:alice;", includeStart: true, includeEnd: false);
    AssertSequenceEqual(new[] { "user:alice:profile", "user:alice:settings" }, aliceKeys);
}

static async Task BTreeIndexExpiryEvictsKeysOnAccessAsync()
{
    var index = new BTreeKeyIndex();
    var past = DateTimeOffset.UtcNow.AddSeconds(-1);
    await index.SetReferenceAsync("expired", "ref", past);
    await index.SetReferenceAsync("alive", "ref-alive");

    AssertEqual(null, await index.GetReferenceAsync("expired"));
    AssertEqual("ref-alive", await index.GetReferenceAsync("alive"));
    AssertSequenceEqual(new[] { "alive" }, await index.ListKeysAsync());
}

static async Task BTreeIndexRebuildPurgesExpiredEntriesAsync()
{
    var index = new BTreeKeyIndex();
    var past = DateTimeOffset.UtcNow.AddSeconds(-1);
    await index.SetReferenceAsync("gone", "ref", past);
    await index.SetReferenceAsync("keep", "ref2");

    await index.RebuildIndexAsync();

    AssertSequenceEqual(new[] { "keep" }, await index.ListKeysAsync());
}

static async Task BTreeIndexRangeScanOpenBoundsAsync()
{
    var index = new BTreeKeyIndex();
    foreach (var k in new[] { "aaa", "bbb", "ccc" })
    {
        await index.SetReferenceAsync(k, "r");
    }

    // Fully open range returns all keys
    var all = await index.GetKeysInRangeAsync(null, null);
    AssertSequenceEqual(new[] { "aaa", "bbb", "ccc" }, all);
}

static async Task SwarmStoreWithBTreeIndexRangeScanAsync()
{
    var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new BTreeKeyIndex());
    await store.PutAsync("order:001", Encoding.UTF8.GetBytes("first"));
    await store.PutAsync("order:002", Encoding.UTF8.GetBytes("second"));
    await store.PutAsync("order:003", Encoding.UTF8.GetBytes("third"));
    await store.PutAsync("other:x", Encoding.UTF8.GetBytes("outside"));

    var range = await store.GetKeyRangeAsync("order:001", "order:002", new RangeScanOptions
    {
        IncludeStart = true,
        IncludeEnd = true,
        IncludeValues = true
    });

    AssertSequenceEqual(new[] { "order:001", "order:002" }, range.Select(e => e.Key));
    AssertEqual("first", Encoding.UTF8.GetString(range[0].Value!));
    AssertEqual("second", Encoding.UTF8.GetString(range[1].Value!));
}

static async Task SwarmStoreWithBTreeIndexPrefixScanAsync()
{
    var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new BTreeKeyIndex());
    await store.PutAsync("tag:alpha", Encoding.UTF8.GetBytes("1"));
    await store.PutAsync("tag:beta", Encoding.UTF8.GetBytes("2"));
    await store.PutAsync("other:gamma", Encoding.UTF8.GetBytes("3"));

    var keys = await store.GetKeysWithPrefixAsync("tag:");
    AssertSequenceEqual(new[] { "tag:alpha", "tag:beta" }, keys);
}

internal sealed record Settings(bool Enabled, int Count);

internal sealed class StubHttpMessageHandler : HttpMessageHandler
{
    private readonly Func<HttpRequestMessage, HttpResponseMessage> _handler;

    public StubHttpMessageHandler(Func<HttpRequestMessage, HttpResponseMessage> handler)
    {
        _handler = handler;
    }

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
        Task.FromResult(_handler(request));
}

internal sealed class CountingKeyValueStore : IKeyValueStore
{
    private readonly Dictionary<string, byte[]> _values = new(StringComparer.Ordinal);
    private readonly Dictionary<string, DateTimeOffset> _expiries = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _getCalls = new(StringComparer.Ordinal);

    public Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        _values[key] = value.ToArray();
        _expiries.Remove(key);
        return Task.CompletedTask;
    }

    public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        _getCalls[key] = GetCallCount(key) + 1;
        if (_expiries.TryGetValue(key, out var expiresAt) && expiresAt <= DateTimeOffset.UtcNow)
        {
            _values.Remove(key);
            _expiries.Remove(key);
            return Task.FromResult<byte[]?>(null);
        }

        return Task.FromResult(_values.TryGetValue(key, out var value) ? value.ToArray() : null);
    }

    public Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        var removed = _values.Remove(key);
        _expiries.Remove(key);
        return Task.FromResult(removed);
    }

    public Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyList<string>>(_values.Keys.OrderBy(static key => key, StringComparer.Ordinal).ToArray());

    public Task<bool> SetTtlAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        if (!_values.ContainsKey(key))
        {
            return Task.FromResult(false);
        }

        _expiries[key] = DateTimeOffset.UtcNow.Add(ttl);
        return Task.FromResult(true);
    }

    public Task<(bool Exists, TimeSpan? Ttl)> GetTtlAsync(string key, CancellationToken cancellationToken = default)
    {
        if (!_values.ContainsKey(key))
        {
            return Task.FromResult((false, (TimeSpan?)null));
        }

        if (!_expiries.TryGetValue(key, out var expiresAt))
        {
            return Task.FromResult((true, (TimeSpan?)null));
        }

        return Task.FromResult((true, (TimeSpan?)(expiresAt - DateTimeOffset.UtcNow)));
    }

    public Task<bool> RemoveTtlAsync(string key, CancellationToken cancellationToken = default) =>
        Task.FromResult(_expiries.Remove(key));

    public int GetCallCount(string key) => _getCalls.TryGetValue(key, out var count) ? count : 0;
}

internal sealed class DelayedWriteKeyValueStore : IKeyValueStore
{
    private readonly int _writeDelayMs;
    private readonly Dictionary<string, byte[]> _values = new(StringComparer.Ordinal);
    private readonly object _sync = new();
    private int _activeWrites;
    private int _maxObservedConcurrentWrites;

    public DelayedWriteKeyValueStore(int writeDelayMs)
    {
        _writeDelayMs = writeDelayMs;
    }

    public int MaxObservedConcurrentWrites => Volatile.Read(ref _maxObservedConcurrentWrites);

    public async Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        var active = Interlocked.Increment(ref _activeWrites);
        while (true)
        {
            var currentMax = Volatile.Read(ref _maxObservedConcurrentWrites);
            if (active <= currentMax)
            {
                break;
            }

            if (Interlocked.CompareExchange(ref _maxObservedConcurrentWrites, active, currentMax) == currentMax)
            {
                break;
            }
        }

        try
        {
            await Task.Delay(_writeDelayMs, cancellationToken);
            lock (_sync)
            {
                _values[key] = value.ToArray();
            }
        }
        finally
        {
            Interlocked.Decrement(ref _activeWrites);
        }
    }

    public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default) =>
        Task.FromResult(GetValueCopy(key));

    public Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default) =>
        Task.FromResult(DeleteKey(key));

    public Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyList<string>>(ListKeysSnapshot());

    private byte[]? GetValueCopy(string key)
    {
        lock (_sync)
        {
            return _values.TryGetValue(key, out var value) ? value.ToArray() : null;
        }
    }

    private bool DeleteKey(string key)
    {
        lock (_sync)
        {
            return _values.Remove(key);
        }
    }

    private IReadOnlyList<string> ListKeysSnapshot()
    {
        lock (_sync)
        {
            return _values.Keys.OrderBy(static key => key, StringComparer.Ordinal).ToArray();
        }
    }
}

internal sealed class TestLogger<T> : Microsoft.Extensions.Logging.ILogger<T>
{
    public List<string> Messages { get; } = [];

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

    public void Log<TState>(
        Microsoft.Extensions.Logging.LogLevel logLevel,
        Microsoft.Extensions.Logging.EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        Messages.Add(formatter(state, exception));
    }
}
