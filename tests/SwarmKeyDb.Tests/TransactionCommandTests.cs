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
public class TransactionCommandTests
{
    [Test]
    public async Task RedisMultiExecQueuesAndExecutesAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("SET", "tx:a", "before") +
            RespCommand("MULTI") +
            RespCommand("SET", "tx:a", "after") +
            RespCommand("GET", "tx:a") +
            RespCommand("EXEC"));

        // +OK for initial SET, +OK for MULTI, +QUEUED x2, then array of [+OK, $5\r\nafter]
        Assert(response.Contains("+OK\r\n+OK\r\n+QUEUED\r\n+QUEUED\r\n", StringComparison.Ordinal), "Expected OK, MULTI OK, two QUEUEDs.");
        Assert(response.Contains("*2\r\n+OK\r\n$5\r\nafter\r\n", StringComparison.Ordinal), "Expected EXEC array with SET OK and GET result.");
    }

    [Test]
    public async Task RedisMultiExecReturnsQueuedAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("MULTI") +
            RespCommand("SET", "k", "v") +
            RespCommand("GET", "k") +
            RespCommand("DEL", "k") +
            RespCommand("EXEC"));

        Assert(response.Contains("+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n", StringComparison.Ordinal), "Each queued command must return +QUEUED.");
        Assert(response.Contains("*3\r\n", StringComparison.Ordinal), "EXEC must return a 3-element array.");
    }

    [Test]
    public async Task RedisExecWithoutMultiReturnsErrorAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor, RespCommand("EXEC"));
        Assert(response.StartsWith("-ERR EXEC without MULTI", StringComparison.Ordinal), "EXEC without MULTI should return error.");
    }

    [Test]
    public async Task RedisDiscardWithoutMultiReturnsErrorAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor, RespCommand("DISCARD"));
        Assert(response.StartsWith("-ERR DISCARD without MULTI", StringComparison.Ordinal), "DISCARD without MULTI should return error.");
    }

    [Test]
    public async Task RedisMultiNestedReturnsErrorAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("MULTI") +
            RespCommand("MULTI") +
            RespCommand("DISCARD"));

        Assert(response.Contains("-ERR MULTI calls can not be nested", StringComparison.Ordinal), "Nested MULTI must return an error.");
        // Transaction should still be valid — DISCARD clears it
        Assert(response.EndsWith("+OK\r\n", StringComparison.Ordinal), "DISCARD should succeed after nested MULTI error.");
    }

    [Test]
    public async Task RedisDiscardClearsQueuedCommandsAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("SET", "disc:k", "original") +
            RespCommand("MULTI") +
            RespCommand("SET", "disc:k", "overwritten") +
            RespCommand("DISCARD") +
            RespCommand("GET", "disc:k"));

        Assert(response.Contains("+OK\r\n", StringComparison.Ordinal), "Expected OK responses.");
        Assert(response.EndsWith("$8\r\noriginal\r\n", StringComparison.Ordinal), "DISCARD should have discarded the queued SET; GET should return original value.");
    }

    [Test]
    public async Task RedisPipelinedDiscardExitsTransactionStateAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("SET", "disc:pipe", "before") +
            RespCommand("MULTI") +
            RespCommand("SET", "disc:pipe", "after") +
            RespCommand("DISCARD") +
            RespCommand("EXEC") +
            RespCommand("GET", "disc:pipe"));

        Assert(response.Contains("+OK\r\n+OK\r\n+QUEUED\r\n+OK\r\n", StringComparison.Ordinal), "Expected pipelined MULTI queue and DISCARD acknowledgements.");
        Assert(response.Contains("-ERR EXEC without MULTI", StringComparison.Ordinal), "EXEC after DISCARD in a pipelined batch must fail.");
        Assert(response.EndsWith("$6\r\nbefore\r\n", StringComparison.Ordinal), "DISCARD should prevent queued mutations from being applied.");
    }

    [Test]
    public async Task RedisExecWithEmptyQueueReturnsEmptyArrayAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("MULTI") +
            RespCommand("EXEC"));

        Assert(response.Contains("*0\r\n", StringComparison.Ordinal), "EXEC with empty queue should return an empty array.");
    }

    [Test]
    public async Task RedisMultiUnknownCommandMarksQueueErrorAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("MULTI") +
            RespCommand("UNKNOWNCMD", "arg") +
            RespCommand("SET", "k", "v") +
            RespCommand("EXEC"));

        // The unknown command should return an error during queuing
        Assert(response.Contains("-ERR unknown command `UNKNOWNCMD`", StringComparison.Ordinal), "Unknown command during MULTI must return error.");
        // EXEC must return EXECABORT since there was a queue error
        Assert(response.Contains("-EXECABORT", StringComparison.Ordinal), "EXEC must return EXECABORT when a queue error was set.");
    }

    [Test]
    public async Task RedisTransactionQueuedCommandsAreCleanedUpOnDisconnectAsync()
    {
        var processor = CreateProcessor();

        var (sessionInput, sessionOutput) = CreatePipe();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var sessionTask = processor.ProcessAsync(sessionInput, sessionOutput, cts.Token);

        await WriteRespCommandAsync(sessionInput, "MULTI");
        await WriteRespCommandAsync(sessionInput, "SET", "disconnect:k", "queued-value");
        await Task.Delay(100, cts.Token);

        await sessionInput.DisposeAsync();
        await sessionTask;

        var replayResponse = await ExecuteAsync(processor, RespCommand("EXEC"));
        Assert(replayResponse.StartsWith("-ERR EXEC without MULTI", StringComparison.Ordinal), "Disconnected transaction state must be cleaned and not leak to future sessions.");
        var getResponse = await ExecuteAsync(processor, RespCommand("GET", "disconnect:k"));
        Assert(getResponse.EndsWith("$-1\r\n", StringComparison.Ordinal), "Queued write from disconnected MULTI session must not be applied.");
    }

    [Test]
    public async Task RedisTransactionReplayAfterDisconnectDoesNotDoubleExecuteAsync()
    {
        var processor = CreateProcessor();
        await ExecuteAsync(processor, RespCommand("SET", "replay:k", "initial"));

        var (sessionInput, sessionOutput) = CreatePipe();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var sessionTask = processor.ProcessAsync(sessionInput, sessionOutput, cts.Token);

        await WriteRespCommandAsync(sessionInput, "MULTI");
        await WriteRespCommandAsync(sessionInput, "SET", "replay:k", "queued");
        await Task.Delay(100, cts.Token);
        await sessionInput.DisposeAsync();
        await sessionTask;

        var retryResponse = await ExecuteAsync(processor, RespCommand("EXEC") + RespCommand("GET", "replay:k"));
        Assert(retryResponse.Contains("-ERR EXEC without MULTI", StringComparison.Ordinal), "Retried EXEC on a new connection must not re-use the old queue.");
        Assert(retryResponse.EndsWith("$7\r\ninitial\r\n", StringComparison.Ordinal), "Retried connection must not double-apply the disconnected transaction.");
    }

    [Test]
    public async Task RedisTransactionUsesCurrentStateAfterExpiryAsync()
    {
        var processor = CreateProcessor();
        await ExecuteAsync(processor, RespCommand("SETEX", "tx:exp", "1", "v"));

        var (sessionInput, sessionOutput) = CreatePipe();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var sessionTask = processor.ProcessAsync(sessionInput, sessionOutput, cts.Token);

        await WriteRespCommandAsync(sessionInput, "MULTI");
        await WriteRespCommandAsync(sessionInput, "GET", "tx:exp");
        await Task.Delay(1200, cts.Token);
        await WriteRespCommandAsync(sessionInput, "EXEC");
        await Task.Delay(100, cts.Token);

        cts.Cancel();
        try { await sessionTask; } catch (OperationCanceledException) { }

        var output = ReadAllBytes(sessionOutput);
        Assert(output.Contains("*1\r\n$-1\r\n", StringComparison.Ordinal), $"Expired key should return nil in EXEC slot. Output: {output}");
    }

    [Test]
    public async Task RedisTransactionUsesCurrentStateAfterDeletionAsync()
    {
        var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
        var processor = new RedisCommandProcessor(store);
        await ExecuteAsync(processor, RespCommand("SET", "tx:del", "v1"));

        var (sessionInput, sessionOutput) = CreatePipe();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var sessionTask = processor.ProcessAsync(sessionInput, sessionOutput, cts.Token);

        await WriteRespCommandAsync(sessionInput, "MULTI");
        await WriteRespCommandAsync(sessionInput, "GET", "tx:del");
        await Task.Delay(50, cts.Token);

        await ExecuteAsync(processor, RespCommand("DEL", "tx:del"));

        await WriteRespCommandAsync(sessionInput, "EXEC");
        await Task.Delay(100, cts.Token);

        cts.Cancel();
        try { await sessionTask; } catch (OperationCanceledException) { }

        var output = ReadAllBytes(sessionOutput);
        Assert(output.Contains("*1\r\n$-1\r\n", StringComparison.Ordinal), $"Deleted key should return nil when EXEC runs. Output: {output}");
    }

    [Test]
    public async Task RedisWatchExecAbortsOnConflictAsync()
    {
        var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
        var processor = new RedisCommandProcessor(store);

        // Pre-set the key via a separate session (connection A)
        await ExecuteAsync(processor, RespCommand("SET", "watch:k", "original"));

        // Session A: WATCH the key, then MULTI + EXEC — runs concurrently via a pipe
        var (sessionAInput, sessionAOutput) = CreatePipe();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var sessionATask = processor.ProcessAsync(sessionAInput, sessionAOutput, cts.Token);

        // Send WATCH to session A, wait for it to be processed
        await WriteRespCommandAsync(sessionAInput, "WATCH", "watch:k");
        await Task.Delay(100, cts.Token);

        // Session B: Modify the watched key (simulates another concurrent connection)
        await ExecuteAsync(processor, RespCommand("SET", "watch:k", "modified-by-b"));

        // Session A continues: MULTI + EXEC — should abort due to WATCH conflict
        await WriteRespCommandAsync(sessionAInput, "MULTI");
        await WriteRespCommandAsync(sessionAInput, "GET", "watch:k");
        await WriteRespCommandAsync(sessionAInput, "EXEC");
        await Task.Delay(100, cts.Token);

        cts.Cancel();
        try { await sessionATask; } catch (OperationCanceledException) { }

        var output = ReadAllBytes(sessionAOutput);
        Assert(output.Contains("*-1\r\n", StringComparison.Ordinal),
            $"EXEC must return null array when WATCH conflict detected. Output: {output}");
    }

    [Test]
    public async Task RedisWatchExecSucceedsWhenKeyNotModifiedAsync()
    {
        var processor = CreateProcessor();
        await ExecuteAsync(processor, RespCommand("SET", "watch:clean", "hello"));

        // Session A: WATCH + MULTI + EXEC — no concurrent modification
        var (sessionAInput, sessionAOutput) = CreatePipe();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var sessionATask = processor.ProcessAsync(sessionAInput, sessionAOutput, cts.Token);

        await WriteRespCommandAsync(sessionAInput, "WATCH", "watch:clean");
        await Task.Delay(50, cts.Token);

        // No modification by another connection — EXEC should succeed
        await WriteRespCommandAsync(sessionAInput, "MULTI");
        await WriteRespCommandAsync(sessionAInput, "GET", "watch:clean");
        await WriteRespCommandAsync(sessionAInput, "EXEC");
        await Task.Delay(100, cts.Token);

        cts.Cancel();
        try { await sessionATask; } catch (OperationCanceledException) { }

        var output = ReadAllBytes(sessionAOutput);
        Assert(output.Contains("+OK\r\n", StringComparison.Ordinal), "Expected OK responses.");
        Assert(!output.Contains("*-1\r\n", StringComparison.Ordinal), $"Should NOT return null array when no conflict. Output: {output}");
        Assert(output.Contains("$5\r\nhello\r\n", StringComparison.Ordinal), $"EXEC should succeed; GET should return 'hello'. Output: {output}");
    }

    [Test]
    public async Task RedisUnwatchClearsWatchRegistrationsAsync()
    {
        var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
        var processor = new RedisCommandProcessor(store);

        await ExecuteAsync(processor, RespCommand("SET", "unwatch:k", "v1"));

        // Session A: WATCH, then UNWATCH, then MULTI + EXEC
        var (sessionAInput, sessionAOutput) = CreatePipe();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var sessionATask = processor.ProcessAsync(sessionAInput, sessionAOutput, cts.Token);

        // Watch and then unwatch
        await WriteRespCommandAsync(sessionAInput, "WATCH", "unwatch:k");
        await Task.Delay(50, cts.Token);
        await WriteRespCommandAsync(sessionAInput, "UNWATCH");
        await Task.Delay(50, cts.Token);

        // Modify the key — after UNWATCH this should NOT trigger abort
        await ExecuteAsync(processor, RespCommand("SET", "unwatch:k", "v2"));

        // MULTI + EXEC — should succeed because UNWATCH was called
        await WriteRespCommandAsync(sessionAInput, "MULTI");
        await WriteRespCommandAsync(sessionAInput, "GET", "unwatch:k");
        await WriteRespCommandAsync(sessionAInput, "EXEC");
        await Task.Delay(100, cts.Token);

        cts.Cancel();
        try { await sessionATask; } catch (OperationCanceledException) { }

        var output = ReadAllBytes(sessionAOutput);
        Assert(!output.Contains("*-1\r\n", StringComparison.Ordinal), $"UNWATCH should have cleared watch; EXEC must not abort. Output: {output}");
        Assert(output.Contains("$2\r\nv2\r\n", StringComparison.Ordinal), $"GET should return 'v2' after successful EXEC. Output: {output}");
    }

    [Test]
    public async Task RedisWatchInsideMultiReturnsErrorAsync()
    {
        var (sessionAInput, sessionAOutput) = CreatePipe();
        var processor = CreateProcessor();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var sessionATask = processor.ProcessAsync(sessionAInput, sessionAOutput, cts.Token);

        await WriteRespCommandAsync(sessionAInput, "MULTI");
        await Task.Delay(50, cts.Token);
        await WriteRespCommandAsync(sessionAInput, "WATCH", "k");
        await WriteRespCommandAsync(sessionAInput, "DISCARD");
        await Task.Delay(100, cts.Token);

        cts.Cancel();
        try { await sessionATask; } catch (OperationCanceledException) { }

        var output = ReadAllBytes(sessionAOutput);
        Assert(output.Contains("-ERR WATCH inside MULTI is not allowed", StringComparison.Ordinal),
            $"WATCH inside MULTI must return error. Output: {output}");
    }

    [Test]
    public async Task RedisRuntimeErrorInExecReturnedInSlotAsync()
    {
        var processor = CreateProcessor();

        // INCR is not implemented, so it will produce an error at execution time
        var response = await ExecuteAsync(processor,
            RespCommand("SET", "rt:k", "value") +
            RespCommand("MULTI") +
            RespCommand("SET", "rt:k", "ok") +
            RespCommand("GET", "rt:k") +
            RespCommand("EXEC"));

        // EXEC should return an array; SET should succeed, GET should return "ok"
        Assert(response.Contains("*2\r\n+OK\r\n$2\r\nok\r\n", StringComparison.Ordinal), "SET and GET in EXEC should succeed.");
    }

    [Test]
    public async Task RedisTransactionMetricsAreTrackedAsync()
    {
        var processor = CreateProcessor();

        // Successful transaction
        await ExecuteAsync(processor,
            RespCommand("MULTI") +
            RespCommand("SET", "m:k", "v") +
            RespCommand("EXEC"));

        var metrics = processor.GetTransactionMetrics();
        Assert(metrics.StartedTotal == 1, $"Expected StartedTotal=1, got {metrics.StartedTotal}.");
        Assert(metrics.CommittedTotal == 1, $"Expected CommittedTotal=1, got {metrics.CommittedTotal}.");
        Assert(metrics.AbortedTotal == 0, $"Expected AbortedTotal=0, got {metrics.AbortedTotal}.");
        Assert(metrics.WatchConflictTotal == 0, $"Expected WatchConflictTotal=0, got {metrics.WatchConflictTotal}.");
        Assert(metrics.QueueDepth.Count == 1, $"Expected one queue depth observation, got {metrics.QueueDepth.Count}.");
        Assert(metrics.ExecDuration.Count == 1, $"Expected one exec duration observation, got {metrics.ExecDuration.Count}.");

        // Trigger a WATCH conflict abort using a concurrent session
        var store2 = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
        var processor2 = new RedisCommandProcessor(store2);
        await ExecuteAsync(processor2, RespCommand("SET", "conflict:k", "v"));

        var (sessionInput, sessionOutput) = CreatePipe();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var sessionTask = processor2.ProcessAsync(sessionInput, sessionOutput, cts.Token);

        await WriteRespCommandAsync(sessionInput, "WATCH", "conflict:k");
        await Task.Delay(100, cts.Token);

        // Another session modifies the key
        await ExecuteAsync(processor2, RespCommand("SET", "conflict:k", "modified"));

        await WriteRespCommandAsync(sessionInput, "MULTI");
        await WriteRespCommandAsync(sessionInput, "EXEC");
        await Task.Delay(100, cts.Token);

        cts.Cancel();
        try { await sessionTask; } catch (OperationCanceledException) { }

        var metrics2 = processor2.GetTransactionMetrics();
        Assert(metrics2.AbortedTotal == 1, $"Expected AbortedTotal=1, got {metrics2.AbortedTotal}.");
        Assert(metrics2.WatchConflictTotal == 1, $"Expected WatchConflictTotal=1, got {metrics2.WatchConflictTotal}.");
    }

    [Test]
    public async Task PrometheusMetricsExposesTransactionTelemetryAsync()
    {
        var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
        var processorForMetrics = new RedisCommandProcessor(store);

        // Execute a transaction to populate counters
        await ExecuteAsync(processorForMetrics,
            RespCommand("MULTI") +
            RespCommand("SET", "pm:k", "v") +
            RespCommand("EXEC"));

        var metrics = new MonitoringMetrics(
            () => NoOpCacheStats.Instance,
            transactionMetricsAccessor: () => processorForMetrics.GetTransactionMetrics());

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
        Assert(payload.Contains("# HELP swarmkeydb_transaction_started_total", StringComparison.Ordinal), "Expected transaction_started HELP.");
        Assert(payload.Contains("# TYPE swarmkeydb_transaction_started_total counter", StringComparison.Ordinal), "Expected transaction_started TYPE.");
        Assert(payload.Contains("swarmkeydb_transaction_started_total{privacy_mode=\"none\"} 1", StringComparison.Ordinal), "Expected started counter = 1.");
        Assert(payload.Contains("# HELP swarmkeydb_transaction_committed_total", StringComparison.Ordinal), "Expected transaction_committed HELP.");
        Assert(payload.Contains("swarmkeydb_transaction_committed_total{privacy_mode=\"none\"} 1", StringComparison.Ordinal), "Expected committed counter = 1.");
        Assert(payload.Contains("# HELP swarmkeydb_transaction_aborted_total", StringComparison.Ordinal), "Expected transaction_aborted HELP.");
        Assert(payload.Contains("# HELP swarmkeydb_transaction_watch_conflict_total", StringComparison.Ordinal), "Expected watch_conflict HELP.");
        Assert(payload.Contains("# HELP swarmkeydb_transaction_queue_depth", StringComparison.Ordinal), "Expected queue depth histogram HELP.");
        Assert(payload.Contains("swarmkeydb_transaction_queue_depth_bucket{le=\"1\",privacy_mode=\"none\"}", StringComparison.Ordinal), "Expected queue depth histogram buckets.");
        Assert(payload.Contains("# HELP swarmkeydb_transaction_exec_duration_seconds", StringComparison.Ordinal), "Expected exec duration histogram HELP.");
        Assert(payload.Contains("swarmkeydb_transaction_exec_duration_seconds_count{privacy_mode=\"none\"} 1", StringComparison.Ordinal), "Expected exec duration count.");

        cts.Cancel();
        await runTask;
        server.Dispose();
    }

}
