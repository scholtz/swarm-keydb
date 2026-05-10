using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using SwarmKeyDb;
using SwarmKeyDb.Cli;
using SwarmKeyDb.Migrate;
using SwarmKeyDb.SwarmConsistency;
using SwarmKeyDb.Server;
using static SwarmKeyDb.Tests.TestHelpers;

namespace SwarmKeyDb.Tests;

[TestFixture]
public class BatchShardTests
{
    [Test]
    public async Task BatchOperationsRespFormatAsync()
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

    [Test]
    public Task ConsistentHashRingDistributesKeysWithLowImbalanceAsync()
    {
        var shardA = new ShardStore("a", new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex()));
        var shardB = new ShardStore("b", new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex()));
        var shardC = new ShardStore("c", new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex()));
        var router = new ShardingRouter([shardA, shardB, shardC], shardCount: 3, virtualNodesPerNode: 128);

        var counts = new Dictionary<string, int>(StringComparer.Ordinal)
        {
            ["a"] = 0,
            ["b"] = 0,
            ["c"] = 0
        };

        for (var i = 0; i < 10_000; i++)
        {
            var shard = router.ResolveShardName($"dist:{i}");
            counts[shard]++;
        }

        const double average = 10_000 / 3.0;
        foreach (var count in counts.Values)
        {
            var imbalance = Math.Abs(count - average) / average;
            Assert(imbalance < 0.20, $"Expected shard imbalance <20%, got {imbalance:P2}.");
        }

        return Task.CompletedTask;
    }

    [Test]
    public Task ShardingRouterRoutesDeterministicallyAndMinimizesRedistributionAsync()
    {
        var shardA = new ShardStore("a", new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex()));
        var shardB = new ShardStore("b", new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex()));
        var shardC = new ShardStore("c", new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex()));
        var routerA = new ShardingRouter([shardA, shardB, shardC], shardCount: 3, virtualNodesPerNode: 128);
        var routerB = new ShardingRouter([shardA, shardB, shardC], shardCount: 3, virtualNodesPerNode: 128);
        var routerWithoutC = new ShardingRouter([shardA, shardB], shardCount: 3, virtualNodesPerNode: 128);

        var moved = 0;
        for (var i = 0; i < 10_000; i++)
        {
            var key = $"stable:{i}";
            var first = routerA.ResolveShardName(key);
            var second = routerB.ResolveShardName(key);
            AssertEqual(first, second);

            if (!string.Equals(first, routerWithoutC.ResolveShardName(key), StringComparison.Ordinal))
            {
                moved++;
            }
        }

        var movedRatio = moved / 10_000d;
        Assert(movedRatio is > 0.15 and < 0.50, $"Expected partial redistribution (~1/N), got {movedRatio:P2}.");
        return Task.CompletedTask;
    }

    [Test]
    public async Task ShardingRouterRoutesKeyOperationsToResolvedShardAsync()
    {
        var storeA = new CountingKeyValueStore();
        var storeB = new CountingKeyValueStore();
        var storeC = new CountingKeyValueStore();
        var stores = new Dictionary<string, CountingKeyValueStore>(StringComparer.Ordinal)
        {
            ["a"] = storeA,
            ["b"] = storeB,
            ["c"] = storeC
        };

        var router = new ShardingRouter(
        [
            new ShardStore("a", storeA),
            new ShardStore("b", storeB),
            new ShardStore("c", storeC)
        ],
            shardCount: 3,
            virtualNodesPerNode: 128);

        const string key = "route:key";
        var expectedShard = router.ResolveShardName(key);
        await router.PutAsync(key, Encoding.UTF8.GetBytes("value"));
        AssertEqual("value", Encoding.UTF8.GetString((await router.GetAsync(key))!));

        foreach (var entry in stores)
        {
            var hasKey = (await entry.Value.GetAsync(key)) is not null;
            AssertEqual(entry.Key == expectedShard, hasKey);
        }

        Assert(await router.DeleteAsync(key), "Delete should route to the same shard and remove the key.");
        Assert(await router.GetAsync(key) is null, "Deleted key should not be retrievable.");
    }

    [Test]
    public async Task ShardingRouterScanAggregatesKeysFromAllShardsAsync()
    {
        var router = new ShardingRouter(
        [
            new ShardStore("a", new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex())),
            new ShardStore("b", new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex())),
            new ShardStore("c", new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex()))
        ],
            shardCount: 3,
            virtualNodesPerNode: 128);

        var client = new SwarmKeyDbClient(router);
        var values = Enumerable.Range(0, 1_000)
            .Select(i => new KeyValuePair<string, ReadOnlyMemory<byte>>($"item:{i:D4}", Encoding.UTF8.GetBytes($"v-{i}")))
            .ToArray();
        await client.BatchPutAsync(values);

        var reads = Enumerable.Range(0, 1_000)
            .Select(async i => (Index: i, Value: await client.GetStringAsync($"item:{i:D4}")))
            .ToArray();
        var readResults = await Task.WhenAll(reads);
        foreach (var result in readResults)
        {
            AssertEqual($"v-{result.Index}", result.Value);
        }

        var keys = await client.KeysAsync();
        AssertEqual(1_000, keys.Count);
        var uniqueKeys = new HashSet<string>(keys, StringComparer.Ordinal);
        AssertEqual(1_000, uniqueKeys.Count);

        var scanned = new HashSet<string>(StringComparer.Ordinal);
        var cursor = string.Empty;
        do
        {
            var page = await client.ScanAsync(cursor.Length == 0 ? null : cursor, 111);
            foreach (var key in page.Keys)
            {
                scanned.Add(key);
            }

            cursor = page.NextCursor;
        } while (!string.IsNullOrEmpty(cursor));

        AssertEqual(1_000, scanned.Count);
    }

    [Test]
    public async Task AsyncBatchGetAndPutRoundTripAsync()
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

    [Test]
    public async Task AsyncFlushWaitsForQueuedFireAndForgetWritesAsync()
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

    [Test]
    public async Task AsyncFireAndForgetCapturesAndLogsErrorsAsync()
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

    [Test]
    public async Task AsyncWriteQueueRespectsConfiguredMaxConcurrencyAsync()
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

    [Test]
    public async Task AsyncBatchThroughputIsAtLeastTwoXSequentialBaselineAsync()
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

}
