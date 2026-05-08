using System.Text;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SwarmKeyDb;

var tests = new (string Name, Func<Task> Test)[]
{
    ("client stores strings json binary and lists keys", ClientStoresSupportedValuesAsync),
    ("bee client parses upload references", BeeClientParsesUploadReferenceAsync),
    ("redis protocol supports set get exists delete", RedisProtocolRoundTripAsync),
    ("redis protocol supports keys and scan", RedisProtocolKeyIterationAsync),
    ("mget returns nulls for missing keys", MGetReturnsNullsForMissingKeysAsync),
    ("mset sets multiple keys atomically", MSetSetsMultipleKeysAtomicallyAsync),
    ("setex stores value with ttl", SetExStoresValueWithTtlAsync),
    ("expire evicts key after delay", ExpireEvictsKeyAfterDelayAsync),
    ("persist removes ttl", PersistRemovesTtlAsync),
    ("ttl returns negative two for missing key", TtlReturnsNegativeTwoForMissingKeyAsync),
    ("set with ex option sets expiry", SetWithExOptionSetsExpiryAsync),
    ("batch operations resp format", BatchOperationsRespFormatAsync),
    ("setex rejects non positive ttl", SetExRejectsNonPositiveTtlAsync),
    ("msetnx does not partially write when blocked", MSetNxDoesNotPartiallyWriteWhenBlockedAsync),
    ("pexpire and pttl round trip", PExpireAndPttlRoundTripAsync),
    ("expireat in past removes key", ExpireAtInPastRemovesKeyAsync),
    ("set with exat option sets expiry", SetWithExAtOptionSetsExpiryAsync),
    ("setex rejects overflow ttl", SetExRejectsOverflowTtlAsync),
    ("caching store get returns cached value after put", CachingKeyValueStoreGetReturnsCachedValueAfterPutAsync),
    ("caching store put invalidates cache", CachingKeyValueStorePutInvalidatesCacheAsync),
    ("caching store delete invalidates cache", CachingKeyValueStoreDeleteInvalidatesCacheAsync),
    ("caching store respects key ttl", CachingKeyValueStoreRespectsKeyTtlAsync),
    ("caching store max entries evicts lru", CachingKeyValueStoreMaxEntriesEvictsLruAsync)
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
