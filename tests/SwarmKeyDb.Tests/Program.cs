using System.Text;
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
    ("setex rejects non positive ttl", SetExRejectsNonPositiveTtlAsync)
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
    var processor = CreateProcessor();
    var response = await ExecuteAsync(processor,
        RespCommand("SET", "short", "1") +
        RespCommand("EXPIRE", "short", "1"));
    AssertEqual("+OK\r\n:1\r\n", response);

    await Task.Delay(1100);
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

static RedisCommandProcessor CreateProcessor() =>
    new(new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex()));

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
