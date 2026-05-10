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
public class OfflineCrdtTests
{
    [Test]
    public async Task OfflineSyncQueuesWritesAndReplaysOnReconnectAsync()
    {
        var probe = new ToggleConnectivityProbe(initiallyConnected: false);
        var remote = new CountingKeyValueStore();
        var store = CreateOfflineStore(remote, probe);

        var write = await store.PutWithResultAsync("offline:queued", Encoding.UTF8.GetBytes("Ada"));
        Assert(write.Queued, "Offline write should be queued while connectivity is unavailable.");
        AssertEqual(1L, store.QueueDepth);

        var cached = await store.GetWithResultAsync("offline:queued");
        Assert(cached.FromCache, "Queued write should be immediately readable from the local offline cache.");
        AssertEqual("Ada", Encoding.UTF8.GetString(cached.Value!));

        probe.SetConnected(true);
        var replayed = await store.SyncPendingOperationsAsync();
        AssertEqual(1, replayed);
        AssertEqual(0L, store.QueueDepth);
        AssertEqual("Ada", Encoding.UTF8.GetString((await remote.GetAsync("offline:queued"))!));
    }

    [Test]
    public async Task OfflineGetReturnsCachedValueMetadataAsync()
    {
        var probe = new ToggleConnectivityProbe(initiallyConnected: true);
        var remote = new CountingKeyValueStore();
        var store = CreateOfflineStore(remote, probe);

        var write = await store.PutWithResultAsync("offline:cached", Encoding.UTF8.GetBytes("warm"));
        Assert(!write.Queued, "Online write should reach the backend immediately.");

        probe.SetConnected(false);
        var read = await store.GetWithResultAsync("offline:cached");
        Assert(read.FromCache, "Expected cached read metadata when the backend becomes unreachable.");
        Assert(read.CachedAt is not null, "Cached read should expose the cache timestamp.");
        AssertEqual("warm", Encoding.UTF8.GetString(read.Value!));
    }

    [Test]
    public async Task OfflineSyncInvokesConflictResolverAsync()
    {
        var probe = new ToggleConnectivityProbe(initiallyConnected: false);
        var remote = new CountingKeyValueStore();
        await remote.PutAsync("offline:conflict", Encoding.UTF8.GetBytes("remote"));
        OfflineConflictContext? capturedConflict = null;
        var store = CreateOfflineStore(
            remote,
            probe,
            options: new SwarmKeyDbOptions
            {
                OfflineMode = OfflineMode.Auto,
                OfflineJournal = OfflineJournalType.Memory,
                OnConflict = context =>
                {
                    capturedConflict = context;
                    return Encoding.UTF8.GetBytes("resolved");
                }
            });

        var queued = await store.PutWithResultAsync("offline:conflict", Encoding.UTF8.GetBytes("local"));
        Assert(queued.Queued, "Conflicting offline write should be queued while disconnected.");

        probe.SetConnected(true);
        await store.SyncPendingOperationsAsync();

        Assert(capturedConflict is not null, "Expected custom conflict resolver to be invoked.");
        AssertEqual("local", Encoding.UTF8.GetString(capturedConflict!.LocalValue!));
        AssertEqual("remote", Encoding.UTF8.GetString(capturedConflict.RemoteValue!));
        AssertEqual("resolved", Encoding.UTF8.GetString((await remote.GetAsync("offline:conflict"))!));
    }

    [Test]
    public async Task SqliteOfflineJournalPersistsEntriesAcrossRestartAsync()
    {
        var path = Path.Combine(Path.GetTempPath(), $"swarm-keydb-offline-{Guid.NewGuid():N}.sqlite");
        try
        {
            var journal = new SqliteOfflineJournal(path);
            await journal.AppendAsync(OfflineOperationType.Put, "offline:sqlite", Encoding.UTF8.GetBytes("persisted"));

            var restarted = new SqliteOfflineJournal(path);
            var entries = await restarted.ReadBatchAsync(10);
            AssertEqual(1, entries.Count);
            AssertEqual("offline:sqlite", entries[0].Key);
            AssertEqual("persisted", Encoding.UTF8.GetString(entries[0].Value!));
        }
        finally
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
    }

    [Test]
    public Task VectorClockIncrementCompareAndMergeAsync()
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

    [Test]
    public Task LwwRegisterTieBreakIsDeterministicAsync()
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

    [Test]
    public Task OrSetAddRemoveAndConcurrentMergeAsync()
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

    [Test]
    public Task PnCounterIncrementDecrementMergeAsync()
    {
        var left = PnCounterValue.Zero.Increment("node-a", 3).Decrement("node-a", 1);
        var right = PnCounterValue.Zero.Increment("node-b", 2).Decrement("node-b", 1);
        var merged = left.Merge(right);

        AssertEqual(3L, merged.Value);
        return Task.CompletedTask;
    }

    [Test]
    public async Task CrdtMergeMethodUsesDefaultLwwRegisterAsync()
    {
        var store = new CrdtKeyValueStore(new CountingKeyValueStore(), nodeId: "node-a");
        await store.PutAsync("doc", Encoding.UTF8.GetBytes("v1"));
        await store.MergeAsync("doc", Encoding.UTF8.GetBytes("v2"));

        AssertEqual("v2", Encoding.UTF8.GetString((await store.GetAsync("doc"))!));
    }

    [Test]
    public async Task CustomMergeStrategyCanBeConfiguredPerKeyAsync()
    {
        var store = new CrdtKeyValueStore(new CountingKeyValueStore(), nodeId: "node-a");
        await store.SetKeyOptionsAsync("set:key", new KeyOptions { MergeStrategy = OrSetMergeStrategy.Instance });

        await store.PutAsync("set:key", OrSetValue.Empty.Add("one", "node-a:1").ToByteArray());
        await store.MergeAsync("set:key", OrSetValue.Empty.Add("two", "node-b:1").ToByteArray());

        var merged = OrSetValue.FromByteArray((await store.GetAsync("set:key"))!);
        AssertSequenceEqual(new[] { "one", "two" }, merged.Elements);
    }

    [Test]
    public async Task TwoInstancesMergeConcurrentWritesDeterministicallyAsync()
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

}
