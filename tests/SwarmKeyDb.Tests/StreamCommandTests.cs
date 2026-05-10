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
public class StreamCommandTests
{
    [Test]
    public async Task RedisStreamRoundTripAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("XADD", "events", "1-0", "type", "created", "user", "alice") +
            RespCommand("XADD", "events", "1-*", "type", "updated", "user", "bob") +
            RespCommand("XRANGE", "events", "-", "+") +
            RespCommand("XRANGE", "events", "-", "+", "COUNT", "1") +
            RespCommand("XREVRANGE", "events", "+", "-", "COUNT", "1") +
            RespCommand("XLEN", "events") +
            RespCommand("XLEN", "missing:events"));

        Assert(response.Contains("$3\r\n1-0\r\n", StringComparison.Ordinal), "XADD with explicit ID should echo ID.");
        Assert(response.Contains("$3\r\n1-1\r\n", StringComparison.Ordinal), "XADD with partial ID should auto-generate a monotonic ID.");
        Assert(response.Contains("*2\r\n*2\r\n$3\r\n1-0\r\n*4\r\n$4\r\ntype\r\n$7\r\ncreated\r\n$4\r\nuser\r\n$5\r\nalice\r\n", StringComparison.Ordinal),
            "XRANGE should return ascending stream entries in Redis nested-array format.");
        Assert(response.Contains("*1\r\n*2\r\n$3\r\n1-0\r\n", StringComparison.Ordinal), "XRANGE COUNT 1 should return at most one entry.");
        Assert(response.Contains("*1\r\n*2\r\n$3\r\n1-1\r\n", StringComparison.Ordinal), "XREVRANGE COUNT 1 should return the most recent entry first.");
        Assert(response.EndsWith(":2\r\n:0\r\n", StringComparison.Ordinal), "XLEN should return stream length and 0 for missing key.");
    }

    [Test]
    public async Task RedisStreamIdValidationAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("XADD", "logs", "2-1", "f", "v") +
            RespCommand("XADD", "logs", "2-1", "f", "dup") +
            RespCommand("XADD", "logs", "1-9", "f", "old") +
            RespCommand("XADD", "logs", "0-0", "f", "zero") +
            RespCommand("XADD", "logs", "bad-id", "f", "bad") +
            RespCommand("XRANGE", "logs", "zzz", "+"));

        Assert(response.Contains("$3\r\n2-1\r\n", StringComparison.Ordinal), "Initial XADD should succeed.");
        Assert(response.Contains("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n", StringComparison.Ordinal),
            "XADD must reject duplicate/out-of-order IDs.");
        Assert(response.Contains("-ERR The ID specified in XADD must be greater than 0-0\r\n", StringComparison.Ordinal),
            "XADD must reject the reserved 0-0 ID.");
        Assert(response.Contains("-ERR Invalid stream ID specified as stream command argument\r\n", StringComparison.Ordinal),
            "XADD/XRANGE must reject malformed stream IDs.");
    }

    [Test]
    public async Task RedisStreamMaxLenTrimmingAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("XADD", "trim:events", "MAXLEN", "~", "2", "1-0", "f", "v1") +
            RespCommand("XADD", "trim:events", "MAXLEN", "~", "2", "2-0", "f", "v2") +
            RespCommand("XADD", "trim:events", "MAXLEN", "~", "2", "3-0", "f", "v3") +
            RespCommand("XRANGE", "trim:events", "-", "+") +
            RespCommand("XLEN", "trim:events"));

        Assert(response.Contains("*2\r\n*2\r\n$3\r\n2-0\r\n*2\r\n$1\r\nf\r\n$2\r\nv2\r\n*2\r\n$3\r\n3-0\r\n*2\r\n$1\r\nf\r\n$2\r\nv3\r\n", StringComparison.Ordinal),
            "MAXLEN trimming should retain only the newest entries.");
        Assert(response.EndsWith(":2\r\n", StringComparison.Ordinal), "XLEN should reflect trimmed stream size.");
    }

    [Test]
    public async Task RedisStreamXTrimMaxLenAndMinIdAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("XADD", "xtrim:events", "1-0", "f", "v1") +
            RespCommand("XADD", "xtrim:events", "2-0", "f", "v2") +
            RespCommand("XADD", "xtrim:events", "3-0", "f", "v3") +
            RespCommand("XADD", "xtrim:events", "4-0", "f", "v4") +
            RespCommand("XTRIM", "xtrim:events", "MAXLEN", "2") +
            RespCommand("XRANGE", "xtrim:events", "-", "+") +
            RespCommand("XADD", "xtrim:events", "5-0", "f", "v5") +
            RespCommand("XTRIM", "xtrim:events", "MINID", "4-0") +
            RespCommand("XRANGE", "xtrim:events", "-", "+"));

        Assert(response.Contains(":2\r\n*2\r\n*2\r\n$3\r\n3-0\r\n", StringComparison.Ordinal),
            "XTRIM MAXLEN should delete and retain only newest IDs.");
        Assert(response.Contains(":1\r\n*2\r\n*2\r\n$3\r\n4-0\r\n", StringComparison.Ordinal),
            "XTRIM MINID should remove entries older than the threshold ID.");
    }

    [Test]
    public async Task RedisStreamXTrimValidationAndEmptyStreamAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("XTRIM", "missing:events", "MAXLEN", "10") +
            RespCommand("XADD", "xtrim:validate", "1-0", "f", "v1") +
            RespCommand("XTRIM", "xtrim:validate", "MAXLEN", "-1") +
            RespCommand("XTRIM", "xtrim:validate", "MINID", "bad-id"));

        Assert(response.StartsWith(":0\r\n", StringComparison.Ordinal), "XTRIM on a missing stream should return 0.");
        Assert(response.Contains("-ERR invalid arguments\r\n", StringComparison.Ordinal), "Invalid MAXLEN/MINID should return ERR invalid arguments.");
    }

    [Test]
    public async Task RedisStreamDefaultRetentionPolicyAsync()
    {
        var processor = new RedisCommandProcessor(
            new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex()),
            streamTrimOptions: new StreamTrimOptions
            {
                DefaultMaxLen = 2,
                DefaultMaxLenApproximate = true
            });

        var response = await ExecuteAsync(processor,
            RespCommand("XADD", "default-trim:events", "1-0", "f", "v1") +
            RespCommand("XADD", "default-trim:events", "2-0", "f", "v2") +
            RespCommand("XADD", "default-trim:events", "3-0", "f", "v3") +
            RespCommand("XLEN", "default-trim:events") +
            RespCommand("XRANGE", "default-trim:events", "-", "+"));

        Assert(response.Contains(":2\r\n", StringComparison.Ordinal), "Default stream retention should cap stream size when XADD omits MAXLEN.");
        Assert(response.Contains("*2\r\n*2\r\n$3\r\n2-0\r\n", StringComparison.Ordinal), "Default retention should keep newest entries.");
    }

    [Test]
    public async Task RedisStreamWrongTypeErrorsAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("SET", "plain:key", "value") +
            RespCommand("XADD", "plain:key", "*", "f", "v") +
            RespCommand("XRANGE", "plain:key", "-", "+") +
            RespCommand("XREVRANGE", "plain:key", "+", "-") +
            RespCommand("XLEN", "plain:key"));

        Assert(response.Contains("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", StringComparison.Ordinal),
            "Stream commands must return WRONGTYPE for non-stream keys.");
    }

    [Test]
    public async Task RedisStreamConsumerGroupWorkflowAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("XADD", "cg:events", "1-0", "f", "v1") +
            RespCommand("XADD", "cg:events", "2-0", "f", "v2") +
            RespCommand("XGROUP", "CREATE", "cg:events", "workers", "0-0") +
            RespCommand("XREADGROUP", "GROUP", "workers", "c1", "COUNT", "2", "STREAMS", "cg:events", ">") +
            RespCommand("XPENDING", "cg:events", "workers") +
            RespCommand("XPENDING", "cg:events", "workers", "-", "+", "10") +
            RespCommand("XACK", "cg:events", "workers", "1-0") +
            RespCommand("XPENDING", "cg:events", "workers"));

        Assert(response.Contains("+OK\r\n", StringComparison.Ordinal), "XGROUP CREATE should return +OK.");
        Assert(response.Contains("*1\r\n*2\r\n$9\r\ncg:events\r\n*2\r\n*2\r\n$3\r\n1-0\r\n", StringComparison.Ordinal),
            "XREADGROUP should return stream entries in RESP2 nested format.");
        Assert(response.Contains("*4\r\n:2\r\n$3\r\n1-0\r\n$3\r\n2-0\r\n*1\r\n*2\r\n$2\r\nc1\r\n:2\r\n", StringComparison.Ordinal),
            "XPENDING summary should expose count, min/max IDs, and per-consumer counts.");
        Assert(response.Contains("*2\r\n*4\r\n$3\r\n1-0\r\n$2\r\nc1\r\n", StringComparison.Ordinal),
            "XPENDING range form should expose pending entries with consumer ownership.");
        Assert(response.Contains(":1\r\n*4\r\n:1\r\n$3\r\n2-0\r\n$3\r\n2-0\r\n*1\r\n*2\r\n$2\r\nc1\r\n:1\r\n", StringComparison.Ordinal),
            "XACK should remove acknowledged IDs from the PEL.");
    }

    [Test]
    public async Task RedisStreamConsumerGroupClaimAndPendingAsync()
    {
        var processor = CreateProcessor();
        await ExecuteAsync(processor,
            RespCommand("XADD", "claim:events", "1-0", "f", "v1") +
            RespCommand("XGROUP", "CREATE", "claim:events", "workers", "0-0") +
            RespCommand("XREADGROUP", "GROUP", "workers", "c1", "STREAMS", "claim:events", ">"));

        await Task.Delay(20);
        var response = await ExecuteAsync(processor,
            RespCommand("XCLAIM", "claim:events", "workers", "c2", "1", "1-0") +
            RespCommand("XPENDING", "claim:events", "workers", "-", "+", "10") +
            RespCommand("XAUTOCLAIM", "claim:events", "workers", "c3", "1", "0-0", "COUNT", "10") +
            RespCommand("XPENDING", "claim:events", "workers", "-", "+", "10") +
            RespCommand("XGROUP", "DELCONSUMER", "claim:events", "workers", "c3") +
            RespCommand("XPENDING", "claim:events", "workers"));

        Assert(response.Contains("*1\r\n*2\r\n$3\r\n1-0\r\n*2\r\n$1\r\nf\r\n$2\r\nv1\r\n", StringComparison.Ordinal),
            "XCLAIM should return claimed entries.");
        Assert(response.Contains("$2\r\nc2\r\n", StringComparison.Ordinal) && response.Contains(":2\r\n", StringComparison.Ordinal),
            "XCLAIM should reassign ownership and increment delivery count.");
        Assert(response.Contains("*3\r\n$3\r\n1-0\r\n*1\r\n*2\r\n$3\r\n1-0\r\n*2\r\n$1\r\nf\r\n$2\r\nv1\r\n*0\r\n", StringComparison.Ordinal),
            "XAUTOCLAIM should return [next-id, entries, deleted-ids].");
        Assert(response.Contains("$2\r\nc3\r\n", StringComparison.Ordinal) && response.Contains(":3\r\n", StringComparison.Ordinal),
            "XAUTOCLAIM should transfer ownership and increase delivery count.");
        Assert(response.Contains(":1\r\n*4\r\n:0\r\n$-1\r\n$-1\r\n*0\r\n", StringComparison.Ordinal),
            "XGROUP DELCONSUMER should remove pending entries owned by the consumer.");
    }

    [Test]
    public async Task RedisStreamConsumerGroupPersistenceAcrossRestartAsync()
    {
        var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
        var processor1 = new RedisCommandProcessor(store);
        await ExecuteAsync(processor1,
            RespCommand("XADD", "persist:events", "1-0", "f", "v1") +
            RespCommand("XGROUP", "CREATE", "persist:events", "workers", "0-0") +
            RespCommand("XREADGROUP", "GROUP", "workers", "c1", "STREAMS", "persist:events", ">"));

        var processor2 = new RedisCommandProcessor(store);
        var response = await ExecuteAsync(processor2, RespCommand("XPENDING", "persist:events", "workers"));
        Assert(response.Contains("*4\r\n:1\r\n$3\r\n1-0\r\n$3\r\n1-0\r\n*1\r\n*2\r\n$2\r\nc1\r\n:1\r\n", StringComparison.Ordinal),
            "Pending entries should survive processor restart when backed by persisted stream state.");
    }

    [Test]
    public async Task RedisStreamDuplicateAckIsIdempotentAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("XADD", "dup-ack:events", "1-0", "f", "v1") +
            RespCommand("XGROUP", "CREATE", "dup-ack:events", "workers", "0-0") +
            RespCommand("XREADGROUP", "GROUP", "workers", "c1", "STREAMS", "dup-ack:events", ">") +
            RespCommand("XACK", "dup-ack:events", "workers", "1-0") +
            RespCommand("XACK", "dup-ack:events", "workers", "1-0") +
            RespCommand("XPENDING", "dup-ack:events", "workers"));

        Assert(response.Contains(":1\r\n:0\r\n*4\r\n:0\r\n$-1\r\n$-1\r\n*0\r\n", StringComparison.Ordinal),
            "XACK should be idempotent and keep XPENDING at zero after a duplicate ACK.");
    }

    [Test]
    public async Task RedisStreamPendingRedeliveryAfterConsumerCrashAsync()
    {
        var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
        var processor1 = new RedisCommandProcessor(store);
        await ExecuteAsync(processor1,
            RespCommand("XADD", "crash:events", "1-0", "f", "v1") +
            RespCommand("XGROUP", "CREATE", "crash:events", "workers", "0-0") +
            RespCommand("XREADGROUP", "GROUP", "workers", "c1", "STREAMS", "crash:events", ">"));

        await Task.Delay(20);
        var processor2 = new RedisCommandProcessor(store);
        var response = await ExecuteAsync(processor2,
            RespCommand("XAUTOCLAIM", "crash:events", "workers", "c2", "1", "0-0", "COUNT", "10") +
            RespCommand("XACK", "crash:events", "workers", "1-0") +
            RespCommand("XPENDING", "crash:events", "workers"));

        Assert(response.Contains("*3\r\n$3\r\n1-0\r\n*1\r\n*2\r\n$3\r\n1-0\r\n*2\r\n$1\r\nf\r\n$2\r\nv1\r\n*0\r\n", StringComparison.Ordinal),
            "XAUTOCLAIM should re-deliver pending entries after consumer restart.");
        Assert(response.Contains(":1\r\n*4\r\n:0\r\n$-1\r\n$-1\r\n*0\r\n", StringComparison.Ordinal),
            "Re-processed claimed entry should ACK cleanly and clear pending list.");
    }

    [Test]
    public async Task RedisStreamConcurrentGroupsIsolationOnRestartAsync()
    {
        var store = new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex());
        var processor1 = new RedisCommandProcessor(store);
        await ExecuteAsync(processor1,
            RespCommand("XADD", "iso:events", "1-0", "f", "v1") +
            RespCommand("XGROUP", "CREATE", "iso:events", "g1", "0-0") +
            RespCommand("XGROUP", "CREATE", "iso:events", "g2", "0-0") +
            RespCommand("XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "iso:events", ">") +
            RespCommand("XREADGROUP", "GROUP", "g2", "c2", "STREAMS", "iso:events", ">") +
            RespCommand("XACK", "iso:events", "g1", "1-0"));

        var processor2 = new RedisCommandProcessor(store);
        var response = await ExecuteAsync(processor2,
            RespCommand("XPENDING", "iso:events", "g1") +
            RespCommand("XPENDING", "iso:events", "g2"));

        Assert(response.Contains("*4\r\n:0\r\n$-1\r\n$-1\r\n*0\r\n", StringComparison.Ordinal),
            "ACK in group g1 should clear only g1 pending entries.");
        Assert(response.Contains("*4\r\n:1\r\n$3\r\n1-0\r\n$3\r\n1-0\r\n*1\r\n*2\r\n$2\r\nc2\r\n:1\r\n", StringComparison.Ordinal),
            "Group g2 pending entries should remain intact across restart.");
    }

    [Test]
    public async Task RedisStreamConsumerGroupAdminCommandsAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("XGROUP", "CREATE", "admin:events", "workers", "0-0", "MKSTREAM") +
            RespCommand("XGROUP", "CREATE", "admin:events", "workers", "0-0") +
            RespCommand("XADD", "admin:events", "1-0", "f", "v1") +
            RespCommand("XGROUP", "SETID", "admin:events", "workers", "1-0") +
            RespCommand("XGROUP", "DESTROY", "admin:events", "workers") +
            RespCommand("XREADGROUP", "GROUP", "workers", "c1", "STREAMS", "admin:events", ">"));

        Assert(response.StartsWith("+OK\r\n", StringComparison.Ordinal), "XGROUP CREATE MKSTREAM should create stream and group.");
        Assert(response.Contains("-BUSYGROUP Consumer Group name already exists\r\n", StringComparison.Ordinal),
            "Duplicate XGROUP CREATE should return BUSYGROUP.");
        Assert(response.Contains("+OK\r\n:1\r\n", StringComparison.Ordinal), "XGROUP SETID and DESTROY should succeed.");
        Assert(response.Contains("-NOGROUP No such key 'admin:events' or consumer group 'workers' in XREADGROUP with GROUP option\r\n", StringComparison.Ordinal),
            "After XGROUP DESTROY, XREADGROUP should return NOGROUP.");
    }

    [Test]
    public async Task RedisStreamXReadAndGroupValidateOptionsAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("XREAD", "COUNT", "0", "STREAMS", "opts:events", "0-0") +
            RespCommand("XREAD", "BLOCK", "-1", "STREAMS", "opts:events", "0-0") +
            RespCommand("XREAD", "BLOCK", "10", "STREAMS", "opts:events") +
            RespCommand("XGROUP", "CREATE", "opts:events", "workers", "0-0", "MKSTREAM") +
            RespCommand("XREADGROUP", "GROUP", "workers", "c1", "COUNT", "0", "STREAMS", "opts:events", ">") +
            RespCommand("XREADGROUP", "GROUP", "workers", "c1", "BLOCK", "-5", "STREAMS", "opts:events", ">") +
            RespCommand("XREADGROUP", "GROUP", "workers", "c1", "UNKNOWN", "STREAMS", "opts:events", ">"));

        var valueErrorCount = response.Split("-ERR value is not an integer or out of range\r\n", StringSplitOptions.None).Length - 1;
        AssertEqual(4, valueErrorCount);
        Assert(response.Contains("-ERR syntax error\r\n", StringComparison.Ordinal),
            "XREAD/XREADGROUP should return syntax errors for malformed option layouts.");
    }

    [Test]
    public async Task RedisStreamXReadGroupNoAckDoesNotCreatePendingAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("XADD", "noack:events", "1-0", "f", "v1") +
            RespCommand("XGROUP", "CREATE", "noack:events", "workers", "0-0") +
            RespCommand("XREADGROUP", "GROUP", "workers", "c1", "NOACK", "STREAMS", "noack:events", ">") +
            RespCommand("XPENDING", "noack:events", "workers"));

        Assert(response.Contains("*1\r\n*2\r\n$12\r\nnoack:events\r\n*1\r\n*2\r\n$3\r\n1-0\r\n", StringComparison.Ordinal),
            "NOACK reads should still return stream entries.");
        Assert(response.Contains("*4\r\n:0\r\n$-1\r\n$-1\r\n*0\r\n", StringComparison.Ordinal),
            "NOACK reads should not create pending entries.");

        var metrics = processor.GetStreamMetrics();
        AssertEqual(0L, metrics.PendingEntriesTotal);
    }

    [Test]
    public async Task RedisStreamXReadBlockingWakeAndTimeoutAsync()
    {
        var processor = CreateProcessor();
        var (sessionInput, sessionOutput) = CreatePipe();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var sessionTask = processor.ProcessAsync(sessionInput, sessionOutput, cts.Token);

        await WriteRespCommandAsync(sessionInput, "XREAD", "BLOCK", "0", "STREAMS", "block:events", "$");
        await Task.Delay(100, cts.Token);
        var blockedMetrics = processor.GetStreamMetrics();
        AssertEqual(1L, blockedMetrics.BlockedReaders);
        Assert(blockedMetrics.BlockedReadersByStream.TryGetValue("block:events", out var blockedByStream) && blockedByStream == 1,
            "Blocked reader count per stream should be tracked while waiting.");

        await ExecuteAsync(processor, RespCommand("XADD", "block:events", "1-0", "f", "v1"));
        await Task.Delay(150, cts.Token);

        var wakeOutput = ReadAllBytes(sessionOutput);
        Assert(wakeOutput.Contains("*1\r\n*2\r\n$12\r\nblock:events\r\n*1\r\n*2\r\n$3\r\n1-0\r\n*2\r\n$1\r\nf\r\n$2\r\nv1\r\n", StringComparison.Ordinal),
            $"Blocking XREAD should wake and return the newly appended entry. Output: {wakeOutput}");

        var wakeOutputLength = wakeOutput.Length;

        await WriteRespCommandAsync(sessionInput, "XREAD", "BLOCK", "200", "STREAMS", "block:events", "$");
        var timeoutOutput = await WaitForOutputGrowthAsync(sessionOutput, wakeOutputLength, 1500, cts.Token);
        Assert(timeoutOutput.Length > wakeOutputLength, $"Timed blocking XREAD should append a timeout response. Output: {timeoutOutput}");
        var timeoutDelta = timeoutOutput[wakeOutputLength..];
        Assert(timeoutDelta.Contains("*-1\r\n", StringComparison.Ordinal), $"Timed blocking XREAD should return null array on timeout. Output: {timeoutOutput}");

        cts.Cancel();
        try { await sessionTask; } catch (OperationCanceledException) { }
    }

    [Test]
    public async Task RedisStreamXReadGroupBlockingFairWakeAsync()
    {
        var processor = CreateProcessor();
        await ExecuteAsync(processor,
            RespCommand("XADD", "fair:events", "0-1", "f", "seed") +
            RespCommand("XGROUP", "CREATE", "fair:events", "workers", "0-1"));

        var (input1, output1) = CreatePipe();
        var (input2, output2) = CreatePipe();
        var (input3, output3) = CreatePipe();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        var task1 = processor.ProcessAsync(input1, output1, cts.Token);
        var task2 = processor.ProcessAsync(input2, output2, cts.Token);
        var task3 = processor.ProcessAsync(input3, output3, cts.Token);

        await WriteRespCommandAsync(input1, "XREADGROUP", "GROUP", "workers", "c1", "BLOCK", "0", "COUNT", "1", "STREAMS", "fair:events", ">");
        await WriteRespCommandAsync(input2, "XREADGROUP", "GROUP", "workers", "c2", "BLOCK", "0", "COUNT", "1", "STREAMS", "fair:events", ">");
        await WriteRespCommandAsync(input3, "XREADGROUP", "GROUP", "workers", "c3", "BLOCK", "0", "COUNT", "1", "STREAMS", "fair:events", ">");
        await Task.Delay(150, cts.Token);

        var blockedMetrics = processor.GetStreamMetrics();
        AssertEqual(3L, blockedMetrics.BlockedReaders);
        Assert(blockedMetrics.BlockedReadersByStream.TryGetValue("fair:events", out var blockedByStream) && blockedByStream == 3,
            "All group consumers should be counted as blocked on the stream.");

        await ExecuteAsync(processor,
            RespCommand("XADD", "fair:events", "1-0", "f", "v1") +
            RespCommand("XADD", "fair:events", "2-0", "f", "v2") +
            RespCommand("XADD", "fair:events", "3-0", "f", "v3"));
        await Task.Delay(250, cts.Token);

        var out1 = ReadAllBytes(output1);
        var out2 = ReadAllBytes(output2);
        var out3 = ReadAllBytes(output3);
        var matches1 = (out1.Contains("1-0", StringComparison.Ordinal) ? 1 : 0) + (out1.Contains("2-0", StringComparison.Ordinal) ? 1 : 0) + (out1.Contains("3-0", StringComparison.Ordinal) ? 1 : 0);
        var matches2 = (out2.Contains("1-0", StringComparison.Ordinal) ? 1 : 0) + (out2.Contains("2-0", StringComparison.Ordinal) ? 1 : 0) + (out2.Contains("3-0", StringComparison.Ordinal) ? 1 : 0);
        var matches3 = (out3.Contains("1-0", StringComparison.Ordinal) ? 1 : 0) + (out3.Contains("2-0", StringComparison.Ordinal) ? 1 : 0) + (out3.Contains("3-0", StringComparison.Ordinal) ? 1 : 0);
        Assert(matches1 == 1 && matches2 == 1 && matches3 == 1, $"Each blocked consumer should receive exactly one new entry. Outputs: {out1} || {out2} || {out3}");

        var pending = await ExecuteAsync(processor, RespCommand("XPENDING", "fair:events", "workers"));
        Assert(pending.Contains("*4\r\n:3\r\n$3\r\n1-0\r\n$3\r\n3-0\r\n*3\r\n", StringComparison.Ordinal), $"Expected 3 pending entries after fair wake-up distribution. Output: {pending}");
        Assert(pending.Contains("$2\r\nc1\r\n:1\r\n", StringComparison.Ordinal), "Consumer c1 should own one pending entry.");
        Assert(pending.Contains("$2\r\nc2\r\n:1\r\n", StringComparison.Ordinal), "Consumer c2 should own one pending entry.");
        Assert(pending.Contains("$2\r\nc3\r\n:1\r\n", StringComparison.Ordinal), "Consumer c3 should own one pending entry.");

        cts.Cancel();
        try { await task1; } catch (OperationCanceledException) { }
        try { await task2; } catch (OperationCanceledException) { }
        try { await task3; } catch (OperationCanceledException) { }
    }

    [Test]
    public async Task RedisStreamBlockingReadCancellationCleansUpWaitersAsync()
    {
        var processor = CreateProcessor();
        var (sessionInput, sessionOutput) = CreatePipe();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var sessionTask = processor.ProcessAsync(sessionInput, sessionOutput, cts.Token);

        await WriteRespCommandAsync(sessionInput, "XREAD", "BLOCK", "0", "STREAMS", "cancel:events", "$");
        await Task.Delay(120);
        var blocked = processor.GetStreamMetrics();
        AssertEqual(1L, blocked.BlockedReaders);

        await sessionInput.DisposeAsync();
        try { await sessionTask; } catch (OperationCanceledException) { }
        await Task.Delay(120);

        var after = processor.GetStreamMetrics();
        AssertEqual(0L, after.BlockedReaders);
        Assert(!after.BlockedReadersByStream.ContainsKey("cancel:events"), "Per-stream blocked reader counts should be removed after disconnect cleanup.");
        _ = sessionOutput;
    }

}
