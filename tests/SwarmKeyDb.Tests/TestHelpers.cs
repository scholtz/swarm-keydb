using System.Text;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SwarmKeyDb;
using SwarmKeyDb.Cli;
using SwarmKeyDb.Server;

namespace SwarmKeyDb.Tests;

internal static class TestHelpers
{
    internal const string AllowedAddress = "0x1111111111111111111111111111111111111111";
    internal const string OtherAddress = "0x2222222222222222222222222222222222222222";
    internal const string BlockedAddress = "0x3333333333333333333333333333333333333333";

    internal static RedisCommandProcessor CreateProcessor() =>
        new(new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex()));

    internal static (RedisCommandProcessor Processor, ScriptReplicationManager ReplicationManager) CreateScriptReplicationProcessor(
        ICacheSyncBus syncBus,
        string nodeId,
        params string[] peers)
    {
        var cache = new ScriptCache();
        var replicationManager = new ScriptReplicationManager(
            cache,
            syncBus,
            new CacheSyncOptions
            {
                Enabled = true,
                NodeId = nodeId,
                Peers = peers,
                SyncIntervalSeconds = 1
            },
            NullLogger<ScriptReplicationManager>.Instance);

        var processor = new RedisCommandProcessor(
            new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex()),
            scriptCache: cache,
            scriptReplicationManager: replicationManager);
        return (processor, replicationManager);
    }

    internal static CachingKeyValueStore CreateCachingStore(
        CountingKeyValueStore inner,
        int maxEntries,
        TimeSpan? defaultEntryTtl = null,
        ICacheSyncBus? syncBus = null,
        IOptions<CacheSyncOptions>? syncOptions = null)
    {
        var options = Options.Create(new CacheOptions
        {
            Enabled = true,
            MaxEntries = maxEntries,
            DefaultEntryTtl = defaultEntryTtl
        });
        var cache = new MemoryCache(new MemoryCacheOptions());
        return new CachingKeyValueStore(
            inner,
            cache,
            options,
            NullLogger<CachingKeyValueStore>.Instance,
            syncBus,
            syncOptions);
    }

    internal static AsyncQueuedKeyValueStore CreateAsyncQueuedStore(
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

    internal static OfflineCapableKeyValueStore CreateOfflineStore(
        CountingKeyValueStore remote,
        ToggleConnectivityProbe probe,
        SwarmKeyDbOptions? options = null)
    {
        IOfflineJournal journal = options?.OfflineJournal == OfflineJournalType.Sqlite
            ? new SqliteOfflineJournal(Path.Combine(Path.GetTempPath(), $"swarm-keydb-offline-{Guid.NewGuid():N}.sqlite"))
            : new InMemoryOfflineJournal();

        return
        new(
            new ConnectivityBoundKeyValueStore(remote, probe),
            journal,
            probe,
            options ?? new SwarmKeyDbOptions
            {
                OfflineMode = OfflineMode.Auto,
                OfflineJournal = OfflineJournalType.Memory
            },
            NullLogger<OfflineCapableKeyValueStore>.Instance);
    }

    internal static CompressingKeyValueStore CreateCompressingStore(IKeyValueStore inner, bool enabled = true, CompressionAlgorithm algorithm = CompressionAlgorithm.GZip, int minSizeBytes = 0)
    {
        var options = Options.Create(new CompressionOptions
        {
            Enabled = enabled,
            Algorithm = algorithm,
            MinSizeBytes = minSizeBytes
        });
        return new CompressingKeyValueStore(inner, options, NullLogger<CompressingKeyValueStore>.Instance);
    }

    internal static EncryptingKeyValueStore CreateEncryptingStore(IKeyValueStore inner, string? keyHex = null, string? ethPrivKeyHex = null, bool enabled = true)
    {
        var options = Options.Create(new EncryptionOptions
        {
            Enabled = enabled,
            KeyHex = keyHex,
            EthPrivateKeyHex = ethPrivKeyHex
        });
        return new EncryptingKeyValueStore(inner, options, NullLogger<EncryptingKeyValueStore>.Instance);
    }

    internal static AclKeyValueStore CreateAclStore(IKeyValueStore inner, IEthAddressAccessor? accessor = null, bool enabled = true, AclMode mode = AclMode.Allowlist, params AclEntry[] entries)
    {
        var options = Options.Create(new AclOptions
        {
            Enabled = enabled,
            Mode = mode,
            Entries = entries.ToList()
        });
        return new AclKeyValueStore(inner, options, accessor ?? new AsyncLocalEthAddressAccessor());
    }

    internal static string MakeKeyHex() => Convert.ToHexString(System.Security.Cryptography.RandomNumberGenerator.GetBytes(32));

    internal static CliExecutionOptions CreateCliTestOptions()
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

    internal static async Task<(int ExitCode, string Stdout, string Stderr)> RunCliAsync(string[] args, CliExecutionOptions options)
    {
        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var code = await SwarmKeyDbCliApp.RunAsync(args, stdout, stderr, options);
        return (code, stdout.ToString(), stderr.ToString());
    }

    internal static async Task<string> ExecuteAsync(RedisCommandProcessor processor, string commands)
    {
        await using var input = new MemoryStream(Encoding.UTF8.GetBytes(commands));
        await using var output = new MemoryStream();
        await processor.ProcessAsync(input, output);
        return Encoding.UTF8.GetString(output.ToArray());
    }

    internal static IKeyValueStore GetInnerStore(object store)
    {
        var field = store.GetType().GetField("_inner", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        Assert(field is not null, $"Expected {store.GetType().Name} to expose an _inner field.");
        return (IKeyValueStore)field!.GetValue(store)!;
    }

    internal static IKeyValueStore GetInnerStoreAtDepth(IKeyValueStore store, int depth)
    {
        var current = store;
        for (var i = 0; i < depth; i++)
        {
            current = GetInnerStore(current);
        }

        return current;
    }

    internal static string RespCommand(params string[] parts)
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

    internal static async Task AssertAccessDeniedAsync(Func<Task> action, string expectedMessage)
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

    internal static void Assert(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException(message);
        }
    }

    internal static void AssertEqual<T>(T expected, T actual)
    {
        if (!EqualityComparer<T>.Default.Equals(expected, actual))
        {
            throw new InvalidOperationException($"Expected '{expected}', got '{actual}'.");
        }
    }

    internal static void AssertSequenceEqual<T>(IEnumerable<T> expected, IEnumerable<T> actual)
    {
        var expectedArray = expected.ToArray();
        var actualArray = actual.ToArray();
        if (!expectedArray.SequenceEqual(actualArray))
        {
            throw new InvalidOperationException($"Expected [{string.Join(", ", expectedArray)}], got [{string.Join(", ", actualArray)}].");
        }
    }

    internal static long ParseIntegerResponse(string response)
    {
        Assert(response.StartsWith(':') && response.EndsWith("\r\n", StringComparison.Ordinal), "Expected RESP integer response.");
        return long.Parse(response[1..^2]);
    }

    internal static async Task<T> WaitUntilValueAsync<T>(Func<Task<T>> action, Func<T, bool> predicate, TimeSpan timeout, TimeSpan pollInterval)
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

    internal static RedisCommandProcessor CreatePubSubProcessor(PubSubManager? manager = null)
    {
        manager ??= new PubSubManager();
        return new RedisCommandProcessor(
            new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex()),
            pubSubManager: manager);
    }

    internal static (Stream Input, MemoryStream Output) CreatePipe()
    {
        var output = new MemoryStream();
        var input = new BlockingStream();
        return (input, output);
    }

    internal static async Task WriteRespCommandAsync(Stream stream, params string[] parts)
    {
        var builder = new StringBuilder();
        builder.Append('*').Append(parts.Length).Append("\r\n");
        foreach (var part in parts)
        {
            var bytes = Encoding.UTF8.GetBytes(part);
            builder.Append('$').Append(bytes.Length).Append("\r\n").Append(part).Append("\r\n");
        }

        var bytes2 = Encoding.UTF8.GetBytes(builder.ToString());
        await stream.WriteAsync(bytes2);
        await stream.FlushAsync();
    }

    internal static RespValue BuildRespCommand(params string[] parts)
    {
        var items = parts.Select(static p => RespValue.BulkString(p)).ToArray();
        return RespValue.Array(items);
    }

    internal static string ReadAllBytes(MemoryStream stream)
    {
        var pos = stream.Position;
        stream.Position = 0;
        var text = Encoding.UTF8.GetString(stream.ToArray());
        stream.Position = pos;
        return text;
    }

    internal static async Task<string> WaitForOutputGrowthAsync(MemoryStream stream, int previousLength, int timeoutMs, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (DateTime.UtcNow < deadline)
        {
            var text = ReadAllBytes(stream);
            if (text.Length > previousLength)
            {
                return text;
            }

            await Task.Delay(20, cancellationToken);
        }

        return ReadAllBytes(stream);
    }
}
