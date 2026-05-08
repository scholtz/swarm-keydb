using System.Text.RegularExpressions;

namespace SwarmKeyDb;

public sealed class RedisCommandProcessor : IDisposable
{
    private readonly IKeyValueStore _store;
    private readonly IEthAddressAccessor? _ethAddressAccessor;
    private readonly SemaphoreSlim _mutationGate = new(1, 1);

    public RedisCommandProcessor(IKeyValueStore store, IEthAddressAccessor? ethAddressAccessor = null)
    {
        _store = store;
        _ethAddressAccessor = ethAddressAccessor;
    }

    public async Task ProcessAsync(Stream input, Stream output, CancellationToken cancellationToken = default)
    {
        var reader = new RespReader(input);
        var writer = new RespWriter(output);
        string? currentAddress = null;

        if (_ethAddressAccessor is not null)
        {
            _ethAddressAccessor.CurrentAddress = null;
        }
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                RespValue? request;
                try
                {
                    request = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (EndOfStreamException)
                {
                    break;
                }

                if (request is null)
                {
                    break;
                }

                if (_ethAddressAccessor is not null)
                {
                    _ethAddressAccessor.CurrentAddress = currentAddress;
                }

                var response = await ExecuteAsync(request, cancellationToken).ConfigureAwait(false);
                if (TryGetAuthorizedAddress(request, response, out var authorizedAddress))
                {
                    currentAddress = authorizedAddress;
                }
                await writer.WriteAsync(response, cancellationToken).ConfigureAwait(false);
                if (IsQuit(request))
                {
                    break;
                }
            }
        }
        finally
        {
            if (_ethAddressAccessor is not null)
            {
                _ethAddressAccessor.CurrentAddress = null;
            }
        }
    }

    public async Task<RespValue> ExecuteAsync(RespValue request, CancellationToken cancellationToken = default)
    {
        if (request.Type != RespType.Array || request.Items is null || request.Items.Count == 0)
        {
            return RespValue.Error("ERR expected command array");
        }

        var args = request.Items;
        var command = args[0].AsString().ToUpperInvariant();
        try
        {
            return command switch
            {
                "PING" => args.Count > 1 ? RespValue.BulkString(args[1].Bytes) : RespValue.SimpleString("PONG"),
                "ECHO" => RequireArity(args, 2) ?? RespValue.BulkString(args[1].Bytes),
                "AUTHADDR" => SetCallerAddress(args),
                "SET" => await SetAsync(args, cancellationToken).ConfigureAwait(false),
                "SETEX" => await SetExAsync(args, milliseconds: false, cancellationToken).ConfigureAwait(false),
                "PSETEX" => await SetExAsync(args, milliseconds: true, cancellationToken).ConfigureAwait(false),
                "GET" => await GetAsync(args, cancellationToken).ConfigureAwait(false),
                "DEL" => await DelAsync(args, cancellationToken).ConfigureAwait(false),
                "MDEL" => await MDelAsync(args, cancellationToken).ConfigureAwait(false),
                "MGET" => await MGetAsync(args, cancellationToken).ConfigureAwait(false),
                "MSET" => await MSetAsync(args, cancellationToken).ConfigureAwait(false),
                "MSETNX" => await MSetNxAsync(args, cancellationToken).ConfigureAwait(false),
                "EXISTS" => await ExistsAsync(args, cancellationToken).ConfigureAwait(false),
                "EXPIRE" => await ExpireAsync(args, milliseconds: false, absolute: false, cancellationToken).ConfigureAwait(false),
                "PEXPIRE" => await ExpireAsync(args, milliseconds: true, absolute: false, cancellationToken).ConfigureAwait(false),
                "EXPIREAT" => await ExpireAsync(args, milliseconds: false, absolute: true, cancellationToken).ConfigureAwait(false),
                "TTL" => await TtlAsync(args, milliseconds: false, cancellationToken).ConfigureAwait(false),
                "PTTL" => await TtlAsync(args, milliseconds: true, cancellationToken).ConfigureAwait(false),
                "PERSIST" => await PersistAsync(args, cancellationToken).ConfigureAwait(false),
                "KEYS" => await KeysAsync(args, cancellationToken).ConfigureAwait(false),
                "SCAN" => await ScanAsync(args, cancellationToken).ConfigureAwait(false),
                "TYPE" => await TypeAsync(args, cancellationToken).ConfigureAwait(false),
                "QUIT" => RespValue.SimpleString("OK"),
                _ => RespValue.Error($"ERR unknown command '{command}'")
            };
        }
        catch (AccessDeniedException ex)
        {
            return RespValue.Error("ERR " + ex.Message);
        }
        catch (ArgumentException ex)
        {
            return RespValue.Error("ERR " + ex.Message);
        }
        catch (InvalidOperationException ex)
        {
            return RespValue.Error("ERR " + ex.Message);
        }
        catch (OverflowException ex)
        {
            return RespValue.Error("ERR " + ex.Message);
        }
    }

    private RespValue SetCallerAddress(IReadOnlyList<RespValue> args)
    {
        var arityError = RequireArity(args, 2);
        if (arityError is not null)
        {
            return arityError;
        }

        if (_ethAddressAccessor is null)
        {
            return RespValue.Error("ERR AUTHADDR is not available.");
        }

        _ethAddressAccessor.CurrentAddress = EthereumAddress.Normalize(args[1].AsString());
        return RespValue.SimpleString("OK");
    }

    private async Task<RespValue> SetAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count != 3 && args.Count != 5)
        {
            return RespValue.Error("ERR wrong number of arguments for 'SET'");
        }

        var ttl = TryParseSetExpiryOption(args, out var setError);
        if (setError is not null)
        {
            return RespValue.Error(setError);
        }

        await _store.PutAsync(args[1].AsString(), args[2].Bytes ?? Array.Empty<byte>(), cancellationToken).ConfigureAwait(false);
        if (ttl is { } expiry)
        {
            await _store.SetTtlAsync(args[1].AsString(), expiry, cancellationToken).ConfigureAwait(false);
        }

        return RespValue.SimpleString("OK");
    }

    private async Task<RespValue> SetExAsync(IReadOnlyList<RespValue> args, bool milliseconds, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 4);
        if (arityError is not null)
        {
            return arityError;
        }

        if (!long.TryParse(args[2].AsString(), out var ttlValue) || ttlValue <= 0)
        {
            return RespValue.Error($"ERR invalid expire time in '{args[0].AsString().ToLowerInvariant()}' command");
        }

        if (!TryParseRelativeTtl(ttlValue, milliseconds, out var ttl))
        {
            return RespValue.Error("ERR value is not an integer or out of range");
        }

        await _store.PutAsync(args[1].AsString(), args[3].Bytes ?? Array.Empty<byte>(), cancellationToken).ConfigureAwait(false);
        await _store.SetTtlAsync(args[1].AsString(), ttl, cancellationToken).ConfigureAwait(false);
        return RespValue.SimpleString("OK");
    }

    private async Task<RespValue> GetAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 2);
        if (arityError is not null)
        {
            return arityError;
        }

        return RespValue.BulkString(await _store.GetAsync(args[1].AsString(), cancellationToken).ConfigureAwait(false));
    }

    private async Task<RespValue> DelAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        return await DeleteManyAsync(args, cancellationToken).ConfigureAwait(false);
    }

    private async Task<RespValue> MDelAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        return await DeleteManyAsync(args, cancellationToken).ConfigureAwait(false);
    }

    private async Task<RespValue> DeleteManyAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 2)
        {
            return RespValue.Error($"ERR wrong number of arguments for '{args[0].AsString()}'");
        }

        var deleted = 0;
        for (var i = 1; i < args.Count; i++)
        {
            if (await _store.DeleteAsync(args[i].AsString(), cancellationToken).ConfigureAwait(false))
            {
                deleted++;
            }
        }

        return RespValue.IntegerValue(deleted);
    }

    private async Task<RespValue> MGetAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 2)
        {
            return RespValue.Error("ERR wrong number of arguments for 'MGET'");
        }

        var values = new RespValue[args.Count - 1];
        for (var i = 1; i < args.Count; i++)
        {
            values[i - 1] = RespValue.BulkString(await _store.GetAsync(args[i].AsString(), cancellationToken).ConfigureAwait(false));
        }

        return RespValue.Array(values);
    }

    private async Task<RespValue> MSetAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 3 || args.Count % 2 == 0)
        {
            return RespValue.Error("ERR wrong number of arguments for 'MSET'");
        }

        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            for (var i = 1; i < args.Count; i += 2)
            {
                await _store.PutAsync(args[i].AsString(), args[i + 1].Bytes ?? Array.Empty<byte>(), cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _mutationGate.Release();
        }

        return RespValue.SimpleString("OK");
    }

    private async Task<RespValue> MSetNxAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 3 || args.Count % 2 == 0)
        {
            return RespValue.Error("ERR wrong number of arguments for 'MSETNX'");
        }

        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            for (var i = 1; i < args.Count; i += 2)
            {
                if (await _store.GetAsync(args[i].AsString(), cancellationToken).ConfigureAwait(false) is not null)
                {
                    return RespValue.IntegerValue(0);
                }
            }

            for (var i = 1; i < args.Count; i += 2)
            {
                await _store.PutAsync(args[i].AsString(), args[i + 1].Bytes ?? Array.Empty<byte>(), cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _mutationGate.Release();
        }

        return RespValue.IntegerValue(1);
    }

    private async Task<RespValue> ExistsAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 2)
        {
            return RespValue.Error("ERR wrong number of arguments for 'EXISTS'");
        }

        var found = 0;
        for (var i = 1; i < args.Count; i++)
        {
            if (await _store.GetAsync(args[i].AsString(), cancellationToken).ConfigureAwait(false) is not null)
            {
                found++;
            }
        }

        return RespValue.IntegerValue(found);
    }

    private async Task<RespValue> ExpireAsync(IReadOnlyList<RespValue> args, bool milliseconds, bool absolute, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 3);
        if (arityError is not null)
        {
            return arityError;
        }

        if (!long.TryParse(args[2].AsString(), out var ttlValue))
        {
            return RespValue.Error("ERR value is not an integer or out of range");
        }

        if (absolute)
        {
            DateTimeOffset expiresAt;
            try
            {
                expiresAt = DateTimeOffset.FromUnixTimeSeconds(ttlValue);
            }
            catch (ArgumentOutOfRangeException)
            {
                return RespValue.Error("ERR value is not an integer or out of range");
            }

            var ttl = expiresAt - DateTimeOffset.UtcNow;
            if (ttl <= TimeSpan.Zero)
            {
                return RespValue.IntegerValue(await _store.DeleteAsync(args[1].AsString(), cancellationToken).ConfigureAwait(false) ? 1 : 0);
            }

            return RespValue.IntegerValue(await _store.SetTtlAsync(args[1].AsString(), ttl, cancellationToken).ConfigureAwait(false) ? 1 : 0);
        }

        if (ttlValue <= 0)
        {
            return RespValue.IntegerValue(await _store.DeleteAsync(args[1].AsString(), cancellationToken).ConfigureAwait(false) ? 1 : 0);
        }

        if (!TryParseRelativeTtl(ttlValue, milliseconds, out var relativeTtl))
        {
            return RespValue.Error("ERR value is not an integer or out of range");
        }

        return RespValue.IntegerValue(await _store.SetTtlAsync(args[1].AsString(), relativeTtl, cancellationToken).ConfigureAwait(false) ? 1 : 0);
    }

    private async Task<RespValue> TtlAsync(IReadOnlyList<RespValue> args, bool milliseconds, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 2);
        if (arityError is not null)
        {
            return arityError;
        }

        var (exists, ttl) = await _store.GetTtlAsync(args[1].AsString(), cancellationToken).ConfigureAwait(false);
        if (!exists)
        {
            return RespValue.IntegerValue(-2);
        }

        if (ttl is null)
        {
            return RespValue.IntegerValue(-1);
        }

        var value = milliseconds
            ? (long)Math.Floor(ttl.Value.TotalMilliseconds)
            : (long)Math.Floor(ttl.Value.TotalSeconds);
        return RespValue.IntegerValue(Math.Max(0, value));
    }

    private async Task<RespValue> PersistAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 2);
        if (arityError is not null)
        {
            return arityError;
        }

        return RespValue.IntegerValue(await _store.RemoveTtlAsync(args[1].AsString(), cancellationToken).ConfigureAwait(false) ? 1 : 0);
    }

    private async Task<RespValue> KeysAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 2);
        if (arityError is not null)
        {
            return arityError;
        }

        var keys = await _store.ListKeysAsync(cancellationToken).ConfigureAwait(false);
        var regex = GlobToRegex(args[1].AsString());
        return RespValue.Array(keys.Where(key => regex.IsMatch(key)).Select(RespValue.BulkString).ToArray());
    }

    private async Task<RespValue> ScanAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 2 || !int.TryParse(args[1].AsString(), out var cursor) || cursor < 0)
        {
            return RespValue.Error("ERR invalid cursor");
        }

        var pattern = "*";
        var count = 10;
        for (var i = 2; i < args.Count; i += 2)
        {
            if (i + 1 >= args.Count)
            {
                return RespValue.Error("ERR syntax error");
            }

            var option = args[i].AsString().ToUpperInvariant();
            if (option == "MATCH")
            {
                pattern = args[i + 1].AsString();
            }
            else if (option == "COUNT" && int.TryParse(args[i + 1].AsString(), out var parsedCount) && parsedCount > 0)
            {
                count = parsedCount;
            }
            else
            {
                return RespValue.Error("ERR syntax error");
            }
        }

        var regex = GlobToRegex(pattern);
        var keys = (await _store.ListKeysAsync(cancellationToken).ConfigureAwait(false))
            .Where(key => regex.IsMatch(key))
            .ToArray();
        var batch = keys.Skip(cursor).Take(count).Select(RespValue.BulkString).ToArray();
        var nextCursor = cursor + batch.Length >= keys.Length ? 0 : cursor + batch.Length;
        return RespValue.Array(new[]
        {
            RespValue.BulkString(nextCursor.ToString()),
            RespValue.Array(batch)
        });
    }

    private async Task<RespValue> TypeAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 2);
        if (arityError is not null)
        {
            return arityError;
        }

        return RespValue.SimpleString(await _store.GetAsync(args[1].AsString(), cancellationToken).ConfigureAwait(false) is null ? "none" : "string");
    }

    private static RespValue? RequireArity(IReadOnlyList<RespValue> args, int expected) =>
        args.Count == expected ? null : RespValue.Error($"ERR wrong number of arguments for '{args[0].AsString()}'");

    private static bool IsQuit(RespValue request) =>
        request.Type == RespType.Array && request.Items is { Count: > 0 } && request.Items[0].AsString().Equals("QUIT", StringComparison.OrdinalIgnoreCase);

    private static bool TryGetAuthorizedAddress(RespValue request, RespValue response, out string? address)
    {
        address = null;
        if (response.Type == RespType.Error ||
            request.Type != RespType.Array ||
            request.Items is not { Count: 2 } items ||
            !items[0].AsString().Equals("AUTHADDR", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        address = EthereumAddress.Normalize(items[1].AsString());
        return true;
    }

    private static TimeSpan? TryParseSetExpiryOption(IReadOnlyList<RespValue> args, out string? error)
    {
        error = null;
        if (args.Count == 3)
        {
            return null;
        }

        var option = args[3].AsString().ToUpperInvariant();
        if (!long.TryParse(args[4].AsString(), out var value))
        {
            error = "ERR value is not an integer or out of range";
            return null;
        }

        TimeSpan ttl;
        switch (option)
        {
            case "EX":
                if (!TryParseRelativeTtl(value, milliseconds: false, out ttl))
                {
                    error = "ERR value is not an integer or out of range";
                    return null;
                }
                break;
            case "PX":
                if (!TryParseRelativeTtl(value, milliseconds: true, out ttl))
                {
                    error = "ERR value is not an integer or out of range";
                    return null;
                }
                break;
            case "EXAT":
                try
                {
                    ttl = DateTimeOffset.FromUnixTimeSeconds(value) - DateTimeOffset.UtcNow;
                }
                catch (ArgumentOutOfRangeException)
                {
                    error = "ERR value is not an integer or out of range";
                    return null;
                }

                break;
            default:
                error = "ERR syntax error";
                return null;
        }

        if (ttl <= TimeSpan.Zero)
        {
            error = "ERR invalid expire time in 'set' command";
            return null;
        }

        return ttl;
    }

    private static bool TryParseRelativeTtl(long value, bool milliseconds, out TimeSpan ttl)
    {
        try
        {
            ttl = milliseconds ? TimeSpan.FromMilliseconds(value) : TimeSpan.FromSeconds(value);
            return true;
        }
        catch (OverflowException)
        {
            ttl = default;
            return false;
        }
    }

    private static Regex GlobToRegex(string pattern)
    {
        var escaped = Regex.Escape(pattern).Replace("\\*", ".*").Replace("\\?", ".");
        return new Regex("^" + escaped + "$", RegexOptions.CultureInvariant);
    }

    public void Dispose()
    {
        _mutationGate.Dispose();
    }
}
