using System.Text;
using SwarmKeyDb;

var tests = new (string Name, Func<Task> Test)[]
{
    ("client stores strings json binary and lists keys", ClientStoresSupportedValuesAsync),
    ("redis protocol supports set get exists delete", RedisProtocolRoundTripAsync),
    ("redis protocol supports keys and scan", RedisProtocolKeyIterationAsync)
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

internal sealed record Settings(bool Enabled, int Count);
