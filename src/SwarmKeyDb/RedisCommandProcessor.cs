using System.Text.RegularExpressions;

namespace SwarmKeyDb;

public sealed class RedisCommandProcessor
{
    private readonly IKeyValueStore _store;

    public RedisCommandProcessor(IKeyValueStore store)
    {
        _store = store;
    }

    public async Task ProcessAsync(Stream input, Stream output, CancellationToken cancellationToken = default)
    {
        var reader = new RespReader(input);
        var writer = new RespWriter(output);

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

            var response = await ExecuteAsync(request, cancellationToken).ConfigureAwait(false);
            await writer.WriteAsync(response, cancellationToken).ConfigureAwait(false);
            if (IsQuit(request))
            {
                break;
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
                "SET" => await SetAsync(args, cancellationToken).ConfigureAwait(false),
                "GET" => await GetAsync(args, cancellationToken).ConfigureAwait(false),
                "DEL" => await DelAsync(args, cancellationToken).ConfigureAwait(false),
                "EXISTS" => await ExistsAsync(args, cancellationToken).ConfigureAwait(false),
                "KEYS" => await KeysAsync(args, cancellationToken).ConfigureAwait(false),
                "SCAN" => await ScanAsync(args, cancellationToken).ConfigureAwait(false),
                "TYPE" => await TypeAsync(args, cancellationToken).ConfigureAwait(false),
                "QUIT" => RespValue.SimpleString("OK"),
                _ => RespValue.Error($"ERR unknown command '{command}'")
            };
        }
        catch (ArgumentException ex)
        {
            return RespValue.Error("ERR " + ex.Message);
        }
        catch (InvalidOperationException ex)
        {
            return RespValue.Error("ERR " + ex.Message);
        }
    }

    private async Task<RespValue> SetAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 3);
        if (arityError is not null)
        {
            return arityError;
        }

        if (args.Count > 3)
        {
            return RespValue.Error("ERR SET options are not supported");
        }

        await _store.PutAsync(args[1].AsString(), args[2].Bytes ?? Array.Empty<byte>(), cancellationToken).ConfigureAwait(false);
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
        if (args.Count < 2)
        {
            return RespValue.Error("ERR wrong number of arguments for 'DEL'");
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

    private static Regex GlobToRegex(string pattern)
    {
        var escaped = Regex.Escape(pattern).Replace("\\*", ".*").Replace("\\?", ".");
        return new Regex("^" + escaped + "$", RegexOptions.CultureInvariant);
    }
}
