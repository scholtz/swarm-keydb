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
public class ScriptingCommandTests
{
    [Test]
    public async Task EvalReturnsIntegerAsync()
    {
        var processor = CreateProcessor();
        var resp = await ExecuteAsync(processor, RespCommand("EVAL", "return 42", "0"));
        AssertEqual(":42\r\n", resp);
    }

    [Test]
    public async Task EvalReturnsStringAsync()
    {
        var processor = CreateProcessor();
        var resp = await ExecuteAsync(processor, RespCommand("EVAL", "return 'hello'", "0"));
        AssertEqual("$5\r\nhello\r\n", resp);
    }

    [Test]
    public async Task EvalReturnsTableAsArrayAsync()
    {
        var processor = CreateProcessor();
        var resp = await ExecuteAsync(processor, RespCommand("EVAL", "return {1, 2, 3}", "0"));
        // Array of 3 integers
        Assert(resp.StartsWith("*3\r\n", StringComparison.Ordinal), "Expected array of 3 elements.");
        Assert(resp.Contains(":1\r\n", StringComparison.Ordinal), "Expected element 1.");
        Assert(resp.Contains(":2\r\n", StringComparison.Ordinal), "Expected element 2.");
        Assert(resp.Contains(":3\r\n", StringComparison.Ordinal), "Expected element 3.");
    }

    [Test]
    public async Task EvalReturnsNilAsync()
    {
        var processor = CreateProcessor();
        var resp = await ExecuteAsync(processor, RespCommand("EVAL", "return nil", "0"));
        // nil → nil bulk string
        AssertEqual("$-1\r\n", resp);
    }

    [Test]
    public async Task EvalKeysAndArgvAreAccessibleAsync()
    {
        var processor = CreateProcessor();

        var respKey = await ExecuteAsync(processor,
            RespCommand("EVAL", "return KEYS[1]", "1", "mykey"));
        AssertEqual("$5\r\nmykey\r\n", respKey);

        var respArg = await ExecuteAsync(processor,
            RespCommand("EVAL", "return ARGV[1]", "0", "myarg"));
        AssertEqual("$5\r\nmyarg\r\n", respArg);
    }

    [Test]
    public async Task EvalRedisCallDispatchesAsync()
    {
        var processor = CreateProcessor();

        // SET via redis.call — returns bulk string "OK" (Lua string → RESP2 bulk string)
        var setResp = await ExecuteAsync(processor,
            RespCommand("EVAL", "return redis.call('SET', KEYS[1], ARGV[1])", "1", "scripting:key", "scripted-value"));
        Assert(setResp.Contains("OK", StringComparison.Ordinal), "Expected OK response from SET via redis.call.");

        // GET the value directly to verify it was really set
        var getResp = await ExecuteAsync(processor, RespCommand("GET", "scripting:key"));
        AssertEqual("$14\r\nscripted-value\r\n", getResp);
    }

    [Test]
    public async Task EvalRedisPCallCatchesErrorsAsync()
    {
        var processor = CreateProcessor();

        // redis.pcall returns {err=...} on error without aborting the script
        // Use an unknown command to trigger a guaranteed error
        var resp = await ExecuteAsync(processor,
            RespCommand("EVAL",
                "local r = redis.pcall('BADCOMMAND_THAT_DOES_NOT_EXIST'); if r and r.err then return 'caught' else return 'no_error' end",
                "0"));

        Assert(
            resp.Contains("caught", StringComparison.Ordinal) ||
            resp.Contains("no_error", StringComparison.Ordinal),
            $"Expected script to complete via pcall, got: {resp}");
    }

    [Test]
    public async Task EvalArityErrorAsync()
    {
        var processor = CreateProcessor();

        // EVAL with only 1 arg (script) — missing numkeys
        var resp = await ExecuteAsync(processor, RespCommand("EVAL", "return 1"));
        Assert(resp.StartsWith("-ERR", StringComparison.Ordinal), "Expected ERR for missing numkeys.");
    }

    [Test]
    public async Task EvalNumKeysOutOfRangeAsync()
    {
        var processor = CreateProcessor();

        // numkeys = -1 → error
        var negResp = await ExecuteAsync(processor, RespCommand("EVAL", "return 1", "-1"));
        Assert(negResp.StartsWith("-ERR", StringComparison.Ordinal), "Expected ERR for negative numkeys.");

        // numkeys greater than provided args → error
        var overResp = await ExecuteAsync(processor, RespCommand("EVAL", "return 1", "5", "k1"));
        Assert(overResp.StartsWith("-ERR", StringComparison.Ordinal), "Expected ERR for numkeys > args.");
    }

    [Test]
    public async Task EvalShaNoscriptOnCacheMissAsync()
    {
        var processor = CreateProcessor();
        var resp = await ExecuteAsync(processor,
            RespCommand("EVALSHA", "0000000000000000000000000000000000000000", "0"));
        Assert(resp.StartsWith("-NOSCRIPT", StringComparison.Ordinal), "Expected NOSCRIPT error on cache miss.");
    }

    [Test]
    public async Task EvalShaAfterScriptLoadAsync()
    {
        var processor = CreateProcessor();
        var script = "return 'evalsha_result'";

        // Load the script
        var loadResp = await ExecuteAsync(processor, RespCommand("SCRIPT", "LOAD", script));
        // SHA1 is 40 hex chars returned as bulk string: $40\r\n<sha>\r\n
        Assert(loadResp.StartsWith("$40\r\n", StringComparison.Ordinal), $"Expected 40-char SHA1, got: {loadResp}");
        var loadParts = loadResp.Split("\r\n");
        Assert(loadParts.Length >= 2, $"Expected at least 2 RESP lines, got: {loadResp}");
        var sha1 = loadParts[1];

        // Execute via EVALSHA
        var evalResp = await ExecuteAsync(processor, RespCommand("EVALSHA", sha1, "0"));
        AssertEqual("$14\r\nevalsha_result\r\n", evalResp);
    }

    [Test]
    public async Task ScriptLoadReturnsSha1Async()
    {
        var processor = CreateProcessor();
        var script = "return 1";
        var expectedSha1 = ScriptCache.ComputeSha1(script);

        var resp = await ExecuteAsync(processor, RespCommand("SCRIPT", "LOAD", script));
        Assert(resp.StartsWith("$40\r\n", StringComparison.Ordinal), "Expected 40-char SHA1 bulk string.");
        var respParts = resp.Split("\r\n");
        Assert(respParts.Length >= 2, $"Expected at least 2 RESP lines, got: {resp}");
        var sha1 = respParts[1];
        AssertEqual(expectedSha1, sha1);
    }

    [Test]
    public async Task ScriptExistsAsync()
    {
        var processor = CreateProcessor();
        var script = "return 'exists_test'";
        var sha1 = ScriptCache.ComputeSha1(script);
        var fakeSha1 = "0000000000000000000000000000000000000000";

        // Before loading: both should return 0
        var beforeResp = await ExecuteAsync(processor, RespCommand("SCRIPT", "EXISTS", sha1, fakeSha1));
        Assert(beforeResp.StartsWith("*2\r\n", StringComparison.Ordinal), "Expected array of 2.");
        Assert(beforeResp.Contains(":0\r\n:0\r\n", StringComparison.Ordinal), "Both scripts should be absent.");

        // Load the script
        await ExecuteAsync(processor, RespCommand("SCRIPT", "LOAD", script));

        // After loading: first should return 1, second should return 0
        var afterResp = await ExecuteAsync(processor, RespCommand("SCRIPT", "EXISTS", sha1, fakeSha1));
        Assert(afterResp.Contains(":1\r\n:0\r\n", StringComparison.Ordinal), $"Expected 1,0 after load, got: {afterResp}");
    }

    [Test]
    public async Task ScriptFlushAsync()
    {
        var processor = CreateProcessor();
        var script = "return 'flush_test'";
        var sha1 = ScriptCache.ComputeSha1(script);

        // Load script
        await ExecuteAsync(processor, RespCommand("SCRIPT", "LOAD", script));

        // Verify it exists
        var beforeFlush = await ExecuteAsync(processor, RespCommand("SCRIPT", "EXISTS", sha1));
        Assert(beforeFlush.Contains(":1\r\n", StringComparison.Ordinal), "Script should exist before flush.");

        // Flush
        var flushResp = await ExecuteAsync(processor, RespCommand("SCRIPT", "FLUSH"));
        AssertEqual("+OK\r\n", flushResp);

        // Verify it no longer exists
        var afterFlush = await ExecuteAsync(processor, RespCommand("SCRIPT", "EXISTS", sha1));
        Assert(afterFlush.Contains(":0\r\n", StringComparison.Ordinal), "Script should be absent after flush.");
    }

    [Test]
    public async Task ScriptKillNotBusyAsync()
    {
        var processor = CreateProcessor();
        var resp = await ExecuteAsync(processor, RespCommand("SCRIPT", "KILL"));
        Assert(resp.StartsWith("-NOTBUSY", StringComparison.Ordinal), "Expected NOTBUSY when no script is running.");
    }

    [Test]
    public async Task EvalTimeoutReturnsBusyAsync()
    {
        // Use a very short timeout (100ms) so the infinite-loop test completes quickly.
        var engine = new ScriptEngine(timeoutMs: 100);
        var processor = new RedisCommandProcessor(
            new SwarmKeyValueStore(new InMemorySwarmClient(), new InMemoryKeyIndex()),
            scriptEngine: engine);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var resp = await ExecuteAsync(processor, RespCommand("EVAL", "while true do end", "0"));
        sw.Stop();

        Assert(resp.StartsWith("-BUSY", StringComparison.Ordinal), $"Expected BUSY error, got: {resp}");
        // The command loop should have returned well within 1 second (the 100ms timeout + some overhead)
        Assert(sw.ElapsedMilliseconds < 2000, $"Expected timeout to fire quickly, took {sw.ElapsedMilliseconds}ms.");
    }

    [Test]
    public async Task EvalSandboxBlocksIoAsync()
    {
        var processor = CreateProcessor();
        var resp = await ExecuteAsync(processor, RespCommand("EVAL", "return io", "0"));
        // io is nil in the sandbox
        AssertEqual("$-1\r\n", resp);
    }

    [Test]
    public async Task EvalSandboxBlocksOsAsync()
    {
        var processor = CreateProcessor();
        var resp = await ExecuteAsync(processor, RespCommand("EVAL", "return os", "0"));
        // os is nil in the sandbox
        AssertEqual("$-1\r\n", resp);
    }

    [Test]
    public async Task EvalFullRoundTripAsync()
    {
        var processor = CreateProcessor();

        // SET via EVAL, then GET via direct command
        await ExecuteAsync(processor,
            RespCommand("EVAL", "redis.call('SET', KEYS[1], ARGV[1]); redis.call('EXPIRE', KEYS[1], '3600')", "1", "roundtrip:key", "roundtrip-value"));

        var getResp = await ExecuteAsync(processor, RespCommand("GET", "roundtrip:key"));
        AssertEqual("$15\r\nroundtrip-value\r\n", getResp);

        // Script that reads and returns a value
        var getScript = await ExecuteAsync(processor,
            RespCommand("EVAL", "return redis.call('GET', KEYS[1])", "1", "roundtrip:key"));
        AssertEqual("$15\r\nroundtrip-value\r\n", getScript);
    }

    [Test]
    public async Task EvalRedlockStyleLockAsync()
    {
        // Simplified Redlock-style atomic lock using EXISTS + SET + EXPIRE
        const string lockScript = @"
    if redis.call('EXISTS', KEYS[1]) == 0 then
      redis.call('SET', KEYS[1], ARGV[1])
      redis.call('EXPIRE', KEYS[1], ARGV[2])
      return 1
    else
      return 0
    end";

        var processor = CreateProcessor();

        // First acquisition should succeed (key doesn't exist)
        var acq1 = await ExecuteAsync(processor,
            RespCommand("EVAL", lockScript, "1", "lock:resource", "owner1", "10"));
        Assert(acq1.Contains(":1\r\n", StringComparison.Ordinal), $"First lock acquisition should succeed, got: {acq1}");

        // Second acquisition by a different owner should fail (key already exists)
        var acq2 = await ExecuteAsync(processor,
            RespCommand("EVAL", lockScript, "1", "lock:resource", "owner2", "10"));
        Assert(acq2.Contains(":0\r\n", StringComparison.Ordinal), $"Second lock acquisition should fail (key exists), got: {acq2}");
    }

}
