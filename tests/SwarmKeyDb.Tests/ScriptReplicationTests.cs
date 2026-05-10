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
public class ScriptReplicationTests
{
    [Test]
    public async Task ScriptReplicationPropagatesEvalShaAcrossNodesAsync()
    {
        var bus = new InMemoryCacheSyncBus();
        var (nodeA, replicationA) = CreateScriptReplicationProcessor(bus, "node-a", "node-b");
        var (nodeB, replicationB) = CreateScriptReplicationProcessor(bus, "node-b", "node-a");
        try
        {
            var script = "return 'replicated'";
            var loadResp = await ExecuteAsync(nodeA, RespCommand("SCRIPT", "LOAD", script));
            var sha1 = loadResp.Split("\r\n", StringSplitOptions.RemoveEmptyEntries)[1];

            var evalShaResp = await WaitUntilValueAsync(
                action: () => ExecuteAsync(nodeB, RespCommand("EVALSHA", sha1, "0")),
                predicate: value => value == "$10\r\nreplicated\r\n",
                timeout: TimeSpan.FromSeconds(1),
                pollInterval: TimeSpan.FromMilliseconds(25));

            AssertEqual("$10\r\nreplicated\r\n", evalShaResp);
        }
        finally
        {
            replicationA.Dispose();
            replicationB.Dispose();
        }
    }

    [Test]
    public async Task ScriptFlushPropagatesAcrossNodesAsync()
    {
        var bus = new InMemoryCacheSyncBus();
        var (nodeA, replicationA) = CreateScriptReplicationProcessor(bus, "node-a", "node-b");
        var (nodeB, replicationB) = CreateScriptReplicationProcessor(bus, "node-b", "node-a");
        try
        {
            var script = "return 'flush-propagation'";
            var loadResp = await ExecuteAsync(nodeA, RespCommand("SCRIPT", "LOAD", script));
            var sha1 = loadResp.Split("\r\n", StringSplitOptions.RemoveEmptyEntries)[1];
            _ = await WaitUntilValueAsync(
                action: () => ExecuteAsync(nodeB, RespCommand("SCRIPT", "EXISTS", sha1)),
                predicate: value => value.Contains(":1\r\n", StringComparison.Ordinal),
                timeout: TimeSpan.FromSeconds(1),
                pollInterval: TimeSpan.FromMilliseconds(25));

            AssertEqual("+OK\r\n", await ExecuteAsync(nodeA, RespCommand("SCRIPT", "FLUSH")));
            var afterFlush = await WaitUntilValueAsync(
                action: () => ExecuteAsync(nodeB, RespCommand("SCRIPT", "EXISTS", sha1)),
                predicate: value => value.Contains(":0\r\n", StringComparison.Ordinal),
                timeout: TimeSpan.FromSeconds(1),
                pollInterval: TimeSpan.FromMilliseconds(25));

            Assert(afterFlush.Contains(":0\r\n", StringComparison.Ordinal), $"Expected propagated flush on peer, got: {afterFlush}");
        }
        finally
        {
            replicationA.Dispose();
            replicationB.Dispose();
        }
    }

    [Test]
    public async Task EvalShaFallbackFetchRecoversMissingScriptFromPeerAsync()
    {
        var bus = new InMemoryCacheSyncBus();
        var (nodeA, replicationA) = CreateScriptReplicationProcessor(bus, "node-a", "node-b");
        var (nodeB, replicationB) = CreateScriptReplicationProcessor(bus, "node-b", "node-a");
        try
        {
            bus.SetNodeConnected("node-b", false);
            var script = "return 'fetch-recovered'";
            var loadResp = await ExecuteAsync(nodeA, RespCommand("SCRIPT", "LOAD", script));
            var sha1 = loadResp.Split("\r\n", StringSplitOptions.RemoveEmptyEntries)[1];

            bus.SetNodeConnected("node-b", true);
            var evalShaResp = await ExecuteAsync(nodeB, RespCommand("EVALSHA", sha1, "0"));
            AssertEqual("$15\r\nfetch-recovered\r\n", evalShaResp);
        }
        finally
        {
            replicationA.Dispose();
            replicationB.Dispose();
        }
    }

    [Test]
    public async Task ScriptStartupResyncHydratesRestartedNodeCacheAsync()
    {
        var bus = new InMemoryCacheSyncBus();
        var (nodeA, replicationA) = CreateScriptReplicationProcessor(bus, "node-a", "node-b");
        var (_, replicationB) = CreateScriptReplicationProcessor(bus, "node-b", "node-a");
        try
        {
            var script = "return 'startup-sync'";
            var loadResp = await ExecuteAsync(nodeA, RespCommand("SCRIPT", "LOAD", script));
            var sha1 = loadResp.Split("\r\n", StringSplitOptions.RemoveEmptyEntries)[1];

            replicationB.Dispose();
            var (restartedNodeB, restartedReplicationB) = CreateScriptReplicationProcessor(bus, "node-b", "node-a");
            try
            {
                await restartedReplicationB.RequestStartupResyncAsync();

                var evalShaResp = await WaitUntilValueAsync(
                    action: () => ExecuteAsync(restartedNodeB, RespCommand("EVALSHA", sha1, "0")),
                    predicate: value => value == "$12\r\nstartup-sync\r\n",
                    timeout: TimeSpan.FromSeconds(1),
                    pollInterval: TimeSpan.FromMilliseconds(25));
                AssertEqual("$12\r\nstartup-sync\r\n", evalShaResp);
            }
            finally
            {
                restartedReplicationB.Dispose();
            }
        }
        finally
        {
            replicationB.Dispose();
            replicationA.Dispose();
        }
    }

    [Test]
    public async Task PrometheusMetricsExposeScriptTelemetryAsync()
    {
        var bus = new InMemoryCacheSyncBus();
        var (nodeA, replicationA) = CreateScriptReplicationProcessor(bus, "node-a", "node-b");
        var (nodeB, replicationB) = CreateScriptReplicationProcessor(bus, "node-b", "node-a");
        try
        {
            // Run two EVAL and one EVALSHA (after SCRIPT LOAD) on node B
            await ExecuteAsync(nodeB, RespCommand("EVAL", "return 1", "0"));
            await ExecuteAsync(nodeB, RespCommand("EVAL", "return 2", "0"));
            var sha1 = ScriptCache.ComputeSha1("return 3");
            await ExecuteAsync(nodeB, RespCommand("SCRIPT", "LOAD", "return 3"));
            await ExecuteAsync(nodeB, RespCommand("EVALSHA", sha1, "0"));
            // Trigger an error
            await ExecuteAsync(nodeB, RespCommand("EVAL", "return redis.call('BADCMD')", "0"));

            // Force a cross-node EVALSHA miss recovery.
            bus.SetNodeConnected("node-b", false);
            var remoteLoad = await ExecuteAsync(nodeA, RespCommand("SCRIPT", "LOAD", "return 'replication-metric'"));
            var replicatedSha = remoteLoad.Split("\r\n", StringSplitOptions.RemoveEmptyEntries)[1];
            bus.SetNodeConnected("node-b", true);
            await ExecuteAsync(nodeB, RespCommand("EVALSHA", replicatedSha, "0"));

            // Emit script flush propagation metric from this node.
            await ExecuteAsync(nodeB, RespCommand("SCRIPT", "FLUSH"));

            var metrics = new MonitoringMetrics(
                () => NoOpCacheStats.Instance,
                scriptMetricsAccessor: () => nodeB.GetScriptMetrics());

            var port = TestNetHelpers.GetFreePort();
            var server = new MonitoringHttpServer(
                IPAddress.Loopback,
                port,
                metrics,
                new StaticReadinessProbe(ready: true, message: "ready"),
                metricsEnabled: true,
                dashboardEnabled: false,
                NullLogger<MonitoringHttpServer>.Instance);
            using var cts = new CancellationTokenSource();
            var runTask = server.RunAsync(cts.Token);
            using var client = new HttpClient();

            var payload = await client.GetStringAsync($"http://127.0.0.1:{port}/metrics");

            Assert(payload.Contains("swarmkeydb_script_eval_total{privacy_mode=\"none\"} 3", StringComparison.Ordinal),
                $"Expected eval_total=3, got: {payload}");
            Assert(payload.Contains("swarmkeydb_script_evalsha_total{privacy_mode=\"none\"} 2", StringComparison.Ordinal),
                $"Expected evalsha_total=2, got: {payload}");
            Assert(payload.Contains("swarmkeydb_script_error_total", StringComparison.Ordinal),
                "Expected error_total metric to be present.");
            Assert(payload.Contains("swarmkeydb_script_timeout_total", StringComparison.Ordinal),
                "Expected timeout_total metric to be present.");
            Assert(payload.Contains("swarmkeydb_script_replication_sent_total", StringComparison.Ordinal),
                "Expected script replication sent metric.");
            Assert(payload.Contains("swarmkeydb_script_replication_received_total", StringComparison.Ordinal),
                "Expected script replication received metric.");
            Assert(payload.Contains("swarmkeydb_script_cache_miss_recovered_total{privacy_mode=\"none\"} 1", StringComparison.Ordinal),
                "Expected one recovered script cache miss.");
            Assert(payload.Contains("swarmkeydb_script_flush_propagated_total{privacy_mode=\"none\"} 1", StringComparison.Ordinal),
                "Expected one propagated script flush.");
            Assert(payload.Contains("swarmkeydb_script_cache_size", StringComparison.Ordinal),
                "Expected script cache size gauge.");
            Assert(payload.Contains("swarmkeydb_script_exec_duration_seconds_bucket", StringComparison.Ordinal),
                "Expected exec_duration histogram buckets.");
            Assert(payload.Contains("swarmkeydb_script_exec_duration_seconds_count", StringComparison.Ordinal),
                "Expected exec_duration histogram count.");

            cts.Cancel();
            await runTask;
            server.Dispose();
        }
        finally
        {
            replicationA.Dispose();
            replicationB.Dispose();
        }
    }

}
