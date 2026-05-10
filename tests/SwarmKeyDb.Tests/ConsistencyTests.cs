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
public class ConsistencyTests
{
    [Test]
    public async Task CachingKeyValueStoreGetReturnsCachedValueAfterPutAsync()
    {
        var inner = new CountingKeyValueStore();
        var store = CreateCachingStore(inner, maxEntries: 8);
        await store.PutAsync("hot:key", Encoding.UTF8.GetBytes("v1"));

        AssertEqual("v1", Encoding.UTF8.GetString((await store.GetAsync("hot:key"))!));
        AssertEqual("v1", Encoding.UTF8.GetString((await store.GetAsync("hot:key"))!));
        AssertEqual(1, inner.GetCallCount("hot:key"));
    }

    [Test]
    public async Task CachingKeyValueStorePutInvalidatesCacheAsync()
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

    [Test]
    public async Task CachingKeyValueStoreDeleteInvalidatesCacheAsync()
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

    [Test]
    public async Task CachingKeyValueStoreRespectsKeyTtlAsync()
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

    [Test]
    public async Task CachingKeyValueStoreMaxEntriesEvictsLruAsync()
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

    [Test]
    public async Task CacheSyncInvalidationPropagatesAcrossInstancesAsync()
    {
        var remote = new CountingKeyValueStore();
        var bus = new InMemoryCacheSyncBus();
        var syncA = Options.Create(new CacheSyncOptions { Enabled = true, NodeId = "node-a", Peers = ["node-b"], SyncIntervalSeconds = 1 });
        var syncB = Options.Create(new CacheSyncOptions { Enabled = true, NodeId = "node-b", Peers = ["node-a"], SyncIntervalSeconds = 1 });
        var storeA = CreateCachingStore(remote, maxEntries: 8, syncBus: bus, syncOptions: syncA);
        var storeB = CreateCachingStore(remote, maxEntries: 8, syncBus: bus, syncOptions: syncB);

        await storeA.PutAsync("sync:key", Encoding.UTF8.GetBytes("v1"));
        _ = await storeB.GetAsync("sync:key");
        AssertEqual("v1", Encoding.UTF8.GetString((await storeB.GetAsync("sync:key"))!));

        await storeA.PutAsync("sync:key", Encoding.UTF8.GetBytes("v2"));
        var refreshed = await WaitUntilValueAsync(
            action: () => storeB.GetAsync("sync:key"),
            predicate: value => value is not null && Encoding.UTF8.GetString(value) == "v2",
            timeout: TimeSpan.FromSeconds(2),
            pollInterval: TimeSpan.FromMilliseconds(50));

        AssertEqual("v2", Encoding.UTF8.GetString(refreshed!));
    }

    [Test]
    public async Task CacheSyncAntiEntropyReconcilesAfterPartitionAsync()
    {
        var remote = new CountingKeyValueStore();
        var bus = new InMemoryCacheSyncBus();
        var syncA = new CacheSyncOptions { Enabled = true, NodeId = "node-a", Peers = ["node-b"], SyncIntervalSeconds = 1 };
        var syncB = new CacheSyncOptions { Enabled = true, NodeId = "node-b", Peers = ["node-a"], SyncIntervalSeconds = 1 };
        var storeA = CreateCachingStore(remote, maxEntries: 8, syncBus: bus, syncOptions: Options.Create(syncA));
        var storeB = CreateCachingStore(remote, maxEntries: 8, syncBus: bus, syncOptions: Options.Create(syncB));
        var serviceA = new AntiEntropyService(storeA, bus, syncA, NullLogger<AntiEntropyService>.Instance);
        var serviceB = new AntiEntropyService(storeB, bus, syncB, NullLogger<AntiEntropyService>.Instance);

        await storeA.PutAsync("partition:key", Encoding.UTF8.GetBytes("v1"));
        _ = await storeB.GetAsync("partition:key");
        AssertEqual("v1", Encoding.UTF8.GetString((await storeB.GetAsync("partition:key"))!));

        bus.SetNodeConnected("node-b", false);
        await storeA.PutAsync("partition:key", Encoding.UTF8.GetBytes("v2"));
        AssertEqual("v1", Encoding.UTF8.GetString((await storeB.GetAsync("partition:key"))!));

        bus.SetNodeConnected("node-b", true);
        await serviceA.TriggerReconciliationAsync();
        await serviceB.TriggerReconciliationAsync();

        var converged = await WaitUntilValueAsync(
            action: () => storeB.GetAsync("partition:key"),
            predicate: value => value is not null && Encoding.UTF8.GetString(value) == "v2",
            timeout: TimeSpan.FromSeconds(2),
            pollInterval: TimeSpan.FromMilliseconds(50));

        AssertEqual("v2", Encoding.UTF8.GetString(converged!));
    }

    [Test]
    public async Task MultiNode_Failover_PrimaryKilled_SecondaryConvergesAsync()
    {
        var remote = new CountingKeyValueStore();
        var bus = new InMemoryCacheSyncBus();
        var syncA = new CacheSyncOptions { Enabled = true, NodeId = "node-a", Peers = ["node-b", "node-c"], SyncIntervalSeconds = 1 };
        var syncB = new CacheSyncOptions { Enabled = true, NodeId = "node-b", Peers = ["node-a", "node-c"], SyncIntervalSeconds = 1 };
        var syncC = new CacheSyncOptions { Enabled = true, NodeId = "node-c", Peers = ["node-a", "node-b"], SyncIntervalSeconds = 1 };

        var storeA = CreateCachingStore(remote, maxEntries: 8, syncBus: bus, syncOptions: Options.Create(syncA));
        var storeB = CreateCachingStore(remote, maxEntries: 8, syncBus: bus, syncOptions: Options.Create(syncB));
        var storeC = CreateCachingStore(remote, maxEntries: 8, syncBus: bus, syncOptions: Options.Create(syncC));
        var serviceA = new AntiEntropyService(storeA, bus, syncA, NullLogger<AntiEntropyService>.Instance);
        var serviceB = new AntiEntropyService(storeB, bus, syncB, NullLogger<AntiEntropyService>.Instance);
        var serviceC = new AntiEntropyService(storeC, bus, syncC, NullLogger<AntiEntropyService>.Instance);

        await storeA.PutAsync("failover:key", Encoding.UTF8.GetBytes("v1"));
        _ = await storeB.GetAsync("failover:key");
        _ = await storeC.GetAsync("failover:key");

        bus.SetNodeConnected("node-a", false);
        await storeB.PutAsync("failover:key", Encoding.UTF8.GetBytes("v2"));
        var secondaryConverged = await WaitUntilValueAsync(
            action: () => storeC.GetAsync("failover:key"),
            predicate: value => value is not null && Encoding.UTF8.GetString(value) == "v2",
            timeout: TimeSpan.FromSeconds(2),
            pollInterval: TimeSpan.FromMilliseconds(50));
        AssertEqual("v2", Encoding.UTF8.GetString(secondaryConverged!));

        bus.SetNodeConnected("node-a", true);
        await serviceA.TriggerReconciliationAsync();
        await serviceB.TriggerReconciliationAsync();
        await serviceC.TriggerReconciliationAsync();
        var recoveredPrimary = await WaitUntilValueAsync(
            action: () => storeA.GetAsync("failover:key"),
            predicate: value => value is not null && Encoding.UTF8.GetString(value) == "v2",
            timeout: TimeSpan.FromSeconds(2),
            pollInterval: TimeSpan.FromMilliseconds(50));
        AssertEqual("v2", Encoding.UTF8.GetString(recoveredPrimary!));
    }

    [Test]
    public async Task MultiNode_RollingUpdate_NodeByNodeRestart_NoStaleReadsAsync()
    {
        var remote = new CountingKeyValueStore();
        var bus = new InMemoryCacheSyncBus();
        var syncA = new CacheSyncOptions { Enabled = true, NodeId = "node-a", Peers = ["node-b", "node-c"], SyncIntervalSeconds = 1 };
        var syncB = new CacheSyncOptions { Enabled = true, NodeId = "node-b", Peers = ["node-a", "node-c"], SyncIntervalSeconds = 1 };
        var syncC = new CacheSyncOptions { Enabled = true, NodeId = "node-c", Peers = ["node-a", "node-b"], SyncIntervalSeconds = 1 };

        var storeA = CreateCachingStore(remote, maxEntries: 8, syncBus: bus, syncOptions: Options.Create(syncA));
        await storeA.PutAsync("rolling:key", Encoding.UTF8.GetBytes("v1"));

        var storeB = CreateCachingStore(remote, maxEntries: 8, syncBus: bus, syncOptions: Options.Create(syncB));
        AssertEqual("v1", Encoding.UTF8.GetString((await storeB.GetAsync("rolling:key"))!));

        await storeA.PutAsync("rolling:key", Encoding.UTF8.GetBytes("v2"));
        var restartedNodeB = CreateCachingStore(remote, maxEntries: 8, syncBus: bus, syncOptions: Options.Create(syncB));
        var nodeBRead = await WaitUntilValueAsync(
            action: () => restartedNodeB.GetAsync("rolling:key"),
            predicate: value => value is not null && Encoding.UTF8.GetString(value) == "v2",
            timeout: TimeSpan.FromSeconds(2),
            pollInterval: TimeSpan.FromMilliseconds(50));
        AssertEqual("v2", Encoding.UTF8.GetString(nodeBRead!));

        await storeA.PutAsync("rolling:key", Encoding.UTF8.GetBytes("v3"));
        var restartedNodeC = CreateCachingStore(remote, maxEntries: 8, syncBus: bus, syncOptions: Options.Create(syncC));
        var nodeCRead = await WaitUntilValueAsync(
            action: () => restartedNodeC.GetAsync("rolling:key"),
            predicate: value => value is not null && Encoding.UTF8.GetString(value) == "v3",
            timeout: TimeSpan.FromSeconds(2),
            pollInterval: TimeSpan.FromMilliseconds(50));
        AssertEqual("v3", Encoding.UTF8.GetString(nodeCRead!));
    }

    [Test]
    public async Task MultiNode_NetworkPartition_Reconnect_ConvergesWithAntiEntropyAsync()
    {
        var remote = new CountingKeyValueStore();
        var bus = new InMemoryCacheSyncBus();
        var syncA = new CacheSyncOptions { Enabled = true, NodeId = "node-a", Peers = ["node-b", "node-c"], SyncIntervalSeconds = 1 };
        var syncB = new CacheSyncOptions { Enabled = true, NodeId = "node-b", Peers = ["node-a", "node-c"], SyncIntervalSeconds = 1 };
        var syncC = new CacheSyncOptions { Enabled = true, NodeId = "node-c", Peers = ["node-a", "node-b"], SyncIntervalSeconds = 1 };

        var storeA = CreateCachingStore(remote, maxEntries: 8, syncBus: bus, syncOptions: Options.Create(syncA));
        var storeB = CreateCachingStore(remote, maxEntries: 8, syncBus: bus, syncOptions: Options.Create(syncB));
        var storeC = CreateCachingStore(remote, maxEntries: 8, syncBus: bus, syncOptions: Options.Create(syncC));
        var serviceA = new AntiEntropyService(storeA, bus, syncA, NullLogger<AntiEntropyService>.Instance);
        var serviceC = new AntiEntropyService(storeC, bus, syncC, NullLogger<AntiEntropyService>.Instance);

        await storeA.PutAsync("partition:multi", Encoding.UTF8.GetBytes("v1"));
        _ = await storeB.GetAsync("partition:multi");
        _ = await storeC.GetAsync("partition:multi");

        bus.SetNodeConnected("node-c", false);
        await storeA.PutAsync("partition:multi", Encoding.UTF8.GetBytes("v2"));
        AssertEqual("v1", Encoding.UTF8.GetString((await storeC.GetAsync("partition:multi"))!));

        bus.SetNodeConnected("node-c", true);
        await serviceA.TriggerReconciliationAsync();
        await serviceC.TriggerReconciliationAsync();
        var converged = await WaitUntilValueAsync(
            action: () => storeC.GetAsync("partition:multi"),
            predicate: value => value is not null && Encoding.UTF8.GetString(value) == "v2",
            timeout: TimeSpan.FromSeconds(2),
            pollInterval: TimeSpan.FromMilliseconds(50));
        AssertEqual("v2", Encoding.UTF8.GetString(converged!));
    }

    [Test]
    public async Task ResyncCoordinatorChoosesModeByVersionGapAsync()
    {
        var remote = new CountingKeyValueStore();
        var bus = new InMemoryCacheSyncBus();
        var syncA = new CacheSyncOptions { Enabled = true, NodeId = "node-a", Peers = ["node-b"], SyncIntervalSeconds = 1 };
        var syncB = new CacheSyncOptions { Enabled = true, NodeId = "node-b", Peers = ["node-a"], SyncIntervalSeconds = 1 };
        var storeA = CreateCachingStore(remote, maxEntries: 8, syncBus: bus, syncOptions: Options.Create(syncA));
        var storeB = CreateCachingStore(remote, maxEntries: 8, syncBus: bus, syncOptions: Options.Create(syncB));

        await storeA.PutAsync("resync:key", Encoding.UTF8.GetBytes("v1"));
        _ = await storeB.GetAsync("resync:key");

        var coordinator = new ResyncCoordinator(
            storeB,
            storeB,
            bus,
            syncB,
            new ResyncOptions
            {
                Mode = ResyncMode.Auto,
                MaxVersionGapForPartialResync = 4,
                FullResyncBatchSize = 4,
                ResyncTimeoutSeconds = 10
            });

        bus.SetNodeConnected("node-b", false);
        for (var i = 0; i < 3; i++)
        {
            await storeA.PutAsync("resync:key", Encoding.UTF8.GetBytes($"v{i + 2}"));
        }
        bus.SetNodeConnected("node-b", true);

        var partial = await coordinator.TriggerResyncAsync(ResyncMode.Auto);
        AssertEqual(ResyncMode.Partial, partial.Mode);
        AssertEqual(1, partial.KeysReplayed);

        bus.SetNodeConnected("node-b", false);
        for (var i = 0; i < 8; i++)
        {
            await storeA.PutAsync("resync:key", Encoding.UTF8.GetBytes($"v{i + 10}"));
        }
        bus.SetNodeConnected("node-b", true);

        var full = await coordinator.TriggerResyncAsync(ResyncMode.Auto);
        AssertEqual(ResyncMode.Full, full.Mode);
        Assert(full.KeysReplayed >= 1, "Full resync should replay at least one key.");
    }

    [Test]
    public async Task ResyncCoordinatorFullModeRebuildsCacheAsync()
    {
        var remote = new CountingKeyValueStore();
        await remote.PutAsync("restore:key", Encoding.UTF8.GetBytes("v1"));
        var sync = new CacheSyncOptions { Enabled = true, NodeId = "node-full" };
        var store = CreateCachingStore(remote, maxEntries: 8, syncBus: NoOpCacheSyncBus.Instance, syncOptions: Options.Create(sync));
        AssertEqual("v1", Encoding.UTF8.GetString((await store.GetAsync("restore:key"))!));

        await remote.PutAsync("restore:key", Encoding.UTF8.GetBytes("v2"));

        var coordinator = new ResyncCoordinator(
            store,
            store,
            NoOpCacheSyncBus.Instance,
            sync,
            new ResyncOptions
            {
                Mode = ResyncMode.Full,
                FullResyncBatchSize = 2,
                ResyncTimeoutSeconds = 10
            });

        var result = await coordinator.TriggerResyncAsync(ResyncMode.Full);
        AssertEqual(ResyncMode.Full, result.Mode);
        AssertEqual(1, result.KeysReplayed);
        AssertEqual("v2", Encoding.UTF8.GetString((await store.GetAsync("restore:key"))!));
    }

}
