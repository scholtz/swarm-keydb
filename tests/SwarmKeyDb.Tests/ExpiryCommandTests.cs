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
public class ExpiryCommandTests
{
    [Test]
    public async Task ExpireEvictsKeyAfterDelayAsync()
    {
        const int ttlSeconds = 1;
        const int expiryDelayMs = 1100;
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("SET", "short", "1") +
            RespCommand("EXPIRE", "short", ttlSeconds.ToString()));
        AssertEqual("+OK\r\n:1\r\n", response);

        await Task.Delay(expiryDelayMs);
        AssertEqual("$-1\r\n", await ExecuteAsync(processor, RespCommand("GET", "short")));
    }

    [Test]
    public async Task PersistRemovesTtlAsync()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("SET", "persist:key", "value") +
            RespCommand("EXPIRE", "persist:key", "30") +
            RespCommand("PERSIST", "persist:key") +
            RespCommand("TTL", "persist:key"));

        AssertEqual("+OK\r\n:1\r\n:1\r\n:-1\r\n", response);
    }

    [Test]
    public async Task TtlReturnsNegativeTwoForMissingKeyAsync()
    {
        var processor = CreateProcessor();
        AssertEqual(":-2\r\n", await ExecuteAsync(processor, RespCommand("TTL", "missing:key")));
    }

    [Test]
    public async Task SetWithExOptionSetsExpiryAsync()
    {
        var processor = CreateProcessor();
        var setResponse = await ExecuteAsync(processor, RespCommand("SET", "option:key", "v", "EX", "10"));
        AssertEqual("+OK\r\n", setResponse);

        var ttl = ParseIntegerResponse(await ExecuteAsync(processor, RespCommand("TTL", "option:key")));
        Assert(ttl is > 0 and <= 10, "SET EX should apply TTL.");
    }

    [Test]
    public async Task SetExRejectsNonPositiveTtlAsync()
    {
        var processor = CreateProcessor();
        AssertEqual("-ERR invalid expire time in 'setex' command\r\n", await ExecuteAsync(processor, RespCommand("SETEX", "bad", "0", "v")));
        AssertEqual("-ERR invalid expire time in 'psetex' command\r\n", await ExecuteAsync(processor, RespCommand("PSETEX", "bad", "-1", "v")));
    }

    [Test]
    public async Task PExpireAndPttlRoundTripAsync()
    {
        var processor = CreateProcessor();
        var setResponse = await ExecuteAsync(processor,
            RespCommand("SET", "ms:key", "v") +
            RespCommand("PEXPIRE", "ms:key", "500") +
            RespCommand("PTTL", "ms:key"));

        Assert(setResponse.StartsWith("+OK\r\n:1\r\n:", StringComparison.Ordinal), "PEXPIRE should apply and PTTL should return integer.");
        var pttl = ParseIntegerResponse(setResponse[(setResponse.LastIndexOf(':'))..]);
        Assert(pttl is > 0 and <= 500, "PTTL should be positive and not exceed requested TTL.");
    }

    [Test]
    public async Task ExpireAtInPastRemovesKeyAsync()
    {
        var processor = CreateProcessor();
        var past = DateTimeOffset.UtcNow.AddSeconds(-1).ToUnixTimeSeconds().ToString();
        var response = await ExecuteAsync(processor,
            RespCommand("SET", "past:key", "v") +
            RespCommand("EXPIREAT", "past:key", past) +
            RespCommand("GET", "past:key"));

        AssertEqual("+OK\r\n:1\r\n$-1\r\n", response);
    }

    [Test]
    public async Task SetWithExAtOptionSetsExpiryAsync()
    {
        var processor = CreateProcessor();
        var exat = DateTimeOffset.UtcNow.AddSeconds(10).ToUnixTimeSeconds().ToString();
        var response = await ExecuteAsync(processor,
            RespCommand("SET", "exat:key", "v", "EXAT", exat) +
            RespCommand("TTL", "exat:key"));

        Assert(response.StartsWith("+OK\r\n:", StringComparison.Ordinal), "SET EXAT should return OK then TTL integer.");
        var ttl = ParseIntegerResponse(response[(response.LastIndexOf(':'))..]);
        Assert(ttl is > 0 and <= 10, "EXAT should set a near-future TTL.");
    }

    [Test]
    public async Task SetExRejectsOverflowTtlAsync()
    {
        var processor = CreateProcessor();
        AssertEqual("-ERR value is not an integer or out of range\r\n", await ExecuteAsync(processor, RespCommand("SETEX", "bad", "9223372036854775807", "v")));
        AssertEqual("-ERR value is not an integer or out of range\r\n", await ExecuteAsync(processor, RespCommand("SET", "bad", "v", "PX", "9223372036854775807")));
    }

}
