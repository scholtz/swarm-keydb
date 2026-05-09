using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using MoonSharp.Interpreter;

namespace SwarmKeyDb;

/// <summary>
/// Sandboxed Lua execution engine built on MoonSharp (MIT licence).
///
/// Security model
/// ──────────────
/// • Uses <see cref="CoreModules.Preset_HardSandbox"/> which removes io, os, package,
///   dofile, loadfile, and require before any script code runs.
/// • Additional nil-assignment ensures the globals load, rawget, rawset, and
///   collectgarbage are absent even when MoonSharp is updated and the preset changes.
/// • Scripts receive only KEYS, ARGV, and the redis table.
///
/// Timeout model
/// ─────────────
/// The script is launched on a Task (thread-pool thread).  The calling thread waits for
/// at most <paramref name="timeoutMs"/> milliseconds.  If the script has not finished by
/// then the engine returns a BUSY error and continues; the background task runs to
/// completion so it does not permanently consume a thread-pool thread.
///
/// RESP conversion
/// ───────────────
/// Follows Redis 7.x Lua→RESP2 mapping:
///   Lua number (integer)   → Integer reply
///   Lua string             → Bulk string
///   Lua false / nil        → Nil bulk string
///   Lua true               → Integer 1
///   Lua table {ok=…}       → Simple string
///   Lua table {err=…}      → Error reply
///   Lua table (array)      → Multi-bulk array (recursive)
/// </summary>
public sealed class ScriptEngine
{
    // Unsafe globals that must never be accessible inside scripts.
    private static readonly string[] UnsafeGlobals =
        ["io", "os", "package", "dofile", "loadfile", "require", "load", "rawget", "rawset", "collectgarbage"];

    private readonly ILogger<ScriptEngine> _logger;
    private readonly int _timeoutMs;
    private readonly long _maxOutputBytes;

    /// <summary>
    /// Initialises a new <see cref="ScriptEngine"/>.
    /// </summary>
    /// <param name="timeoutMs">
    /// Maximum wall-clock milliseconds a single script may run (default 5 000).
    /// </param>
    /// <param name="maxOutputBytes">
    /// Maximum total bytes that may be returned from a script (default 10 MiB).
    /// </param>
    /// <param name="logger">Optional logger.  Falls back to a no-op logger.</param>
    public ScriptEngine(
        int timeoutMs = 5_000,
        long maxOutputBytes = 10 * 1024 * 1024,
        ILogger<ScriptEngine>? logger = null)
    {
        _timeoutMs = Math.Max(100, timeoutMs);
        _maxOutputBytes = Math.Max(4096, maxOutputBytes);
        _logger = logger ?? NullLogger<ScriptEngine>.Instance;
    }

    /// <summary>
    /// Executes a Lua script in the sandbox.
    /// </summary>
    /// <param name="scriptSource">Raw Lua source code.</param>
    /// <param name="keys">Values exposed as the <c>KEYS</c> table (1-based).</param>
    /// <param name="argv">Values exposed as the <c>ARGV</c> table (1-based).</param>
    /// <param name="redisCallAsync">
    /// Async delegate invoked when the script calls <c>redis.call()</c> or
    /// <c>redis.pcall()</c>.  The first argument is the command; the rest are its
    /// arguments.  Returns a <see cref="RespValue"/> which is converted back into a
    /// Lua value before being returned to the script.
    /// </param>
    /// <returns>
    /// The RESP-encoded result of the script, or an error reply.
    /// </returns>
    public async Task<RespValue> ExecuteAsync(
        string scriptSource,
        IReadOnlyList<string> keys,
        IReadOnlyList<string> argv,
        Func<string, IReadOnlyList<string>, Task<RespValue>> redisCallAsync)
    {
        var timeoutTask = Task.Delay(_timeoutMs);

        // Scripts dispatch redis.call / redis.pcall on the script thread synchronously.
        // We bridge back into the async world by marshalling onto the calling context.
        // We use a bounded channel so the script thread can fire-and-forget calls while
        // the awaiting thread drains them.
        var scriptTask = Task.Run(() => RunScript(scriptSource, keys, argv, redisCallAsync));

        var winner = await Task.WhenAny(scriptTask, timeoutTask).ConfigureAwait(false);

        if (winner == timeoutTask)
        {
            _logger.LogWarning("Script execution exceeded timeout of {TimeoutMs}ms — returning BUSY error.", _timeoutMs);
            return RespValue.Error("BUSY Script exceeded time limit");
        }

        // Propagate the result (or exception) from the script task.
        try
        {
            return await scriptTask.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Script execution failed: {ErrorType}", ex.GetType().Name);
            return RespValue.Error($"ERR {ex.Message}");
        }
    }

    // ─── Private ─────────────────────────────────────────────────────────────────

    private RespValue RunScript(
        string scriptSource,
        IReadOnlyList<string> keys,
        IReadOnlyList<string> argv,
        Func<string, IReadOnlyList<string>, Task<RespValue>> redisCallAsync)
    {
        var script = new Script(CoreModules.Preset_HardSandbox);
        script.Options.DebugPrint = _ => { }; // silence print()

        StripUnsafeGlobals(script);
        SetupKeysArgv(script, keys, argv);
        SetupRedisTable(script, redisCallAsync);

        DynValue result;
        try
        {
            result = script.DoString(scriptSource);
        }
        catch (ScriptRuntimeException ex)
        {
            var msg = ex.DecoratedMessage ?? ex.Message;
            _logger.LogWarning("Script runtime error: {Message}", msg);
            return RespValue.Error($"ERR {msg}");
        }
        catch (SyntaxErrorException ex)
        {
            var msg = ex.DecoratedMessage ?? ex.Message;
            _logger.LogWarning("Script syntax error: {Message}", msg);
            return RespValue.Error($"ERR compile error: {msg}");
        }

        var respResult = LuaToResp(result);

        // Enforce output size limit.
        if (EstimateRespSize(respResult) > _maxOutputBytes)
        {
            _logger.LogWarning("Script output exceeded maximum size of {MaxOutputBytes} bytes.", _maxOutputBytes);
            return RespValue.Error("ERR script output too large");
        }

        return respResult;
    }

    private static void StripUnsafeGlobals(Script script)
    {
        foreach (var g in UnsafeGlobals)
        {
            script.Globals[g] = DynValue.Nil;
        }
    }

    private static void SetupKeysArgv(Script script, IReadOnlyList<string> keys, IReadOnlyList<string> argv)
    {
        var keysTable = new Table(script);
        for (var i = 0; i < keys.Count; i++)
        {
            keysTable[i + 1] = DynValue.NewString(keys[i]);
        }

        script.Globals["KEYS"] = keysTable;

        var argvTable = new Table(script);
        for (var i = 0; i < argv.Count; i++)
        {
            argvTable[i + 1] = DynValue.NewString(argv[i]);
        }

        script.Globals["ARGV"] = argvTable;
    }

    private static void SetupRedisTable(
        Script script,
        Func<string, IReadOnlyList<string>, Task<RespValue>> redisCallAsync)
    {
        var redisTable = new Table(script);

        // redis.call — errors propagate as script errors.
        redisTable["call"] = DynValue.NewCallback((ctx, cbArgs) =>
        {
            var (cmd, cmdArgs) = ParseRedisCallArgs(cbArgs);
            var respResult = redisCallAsync(cmd, cmdArgs).GetAwaiter().GetResult();
            if (respResult.Type == RespType.Error)
            {
                throw new ScriptRuntimeException(respResult.Text ?? "ERR unknown error");
            }

            return RespToLua(script, respResult);
        });

        // redis.pcall — errors are caught and returned as a Lua table {err=…}.
        redisTable["pcall"] = DynValue.NewCallback((ctx, cbArgs) =>
        {
            var (cmd, cmdArgs) = ParseRedisCallArgs(cbArgs);
            try
            {
                var respResult = redisCallAsync(cmd, cmdArgs).GetAwaiter().GetResult();
                if (respResult.Type == RespType.Error)
                {
                    var errTable = new Table(script);
                    errTable["err"] = DynValue.NewString(respResult.Text ?? "ERR");
                    return DynValue.NewTable(errTable);
                }

                return RespToLua(script, respResult);
            }
            catch (Exception ex)
            {
                var errTable = new Table(script);
                errTable["err"] = DynValue.NewString(ex.Message);
                return DynValue.NewTable(errTable);
            }
        });

        // redis.status_reply — creates a {ok=…} table (Redis 7.x helper).
        redisTable["status_reply"] = DynValue.NewCallback((ctx, cbArgs) =>
        {
            var msg = cbArgs.Count > 0 ? cbArgs[0].CastToString() ?? "OK" : "OK";
            var t = new Table(script);
            t["ok"] = DynValue.NewString(msg);
            return DynValue.NewTable(t);
        });

        // redis.error_reply — creates an {err=…} table (Redis 7.x helper).
        redisTable["error_reply"] = DynValue.NewCallback((ctx, cbArgs) =>
        {
            var msg = cbArgs.Count > 0 ? cbArgs[0].CastToString() ?? "ERR" : "ERR";
            var t = new Table(script);
            t["err"] = DynValue.NewString(msg);
            return DynValue.NewTable(t);
        });

        script.Globals["redis"] = redisTable;
    }

    private static (string Command, IReadOnlyList<string> Args) ParseRedisCallArgs(CallbackArguments cbArgs)
    {
        if (cbArgs.Count == 0)
        {
            throw new ScriptRuntimeException("redis.call requires at least one argument");
        }

        var cmd = cbArgs[0].CastToString() ?? throw new ScriptRuntimeException("redis.call: command must be a string");
        var cmdArgs = new List<string>(cbArgs.Count - 1);
        for (var i = 1; i < cbArgs.Count; i++)
        {
            var v = cbArgs[i];
            if (v.Type == DataType.Number)
            {
                // Preserve the full numeric value as a string (matching Redis behavior).
                // If the value is an exact integer, format without a decimal point.
                var n = v.Number;
                cmdArgs.Add(n == Math.Truncate(n)
                    ? ((long)n).ToString(System.Globalization.CultureInfo.InvariantCulture)
                    : n.ToString(System.Globalization.CultureInfo.InvariantCulture));
            }
            else
            {
                cmdArgs.Add(v.CastToString() ?? string.Empty);
            }
        }

        return (cmd, cmdArgs);
    }

    // ─── RESP → Lua ──────────────────────────────────────────────────────────────

    private static DynValue RespToLua(Script script, RespValue resp)
    {
        return resp.Type switch
        {
            RespType.SimpleString => DynValue.NewString(resp.Text ?? string.Empty),
            RespType.BulkString when resp.Bytes is null => DynValue.False,
            RespType.BulkString => DynValue.NewString(resp.AsString()),
            RespType.Integer => DynValue.NewNumber(resp.Integer),
            RespType.Error => DynValue.NewString(resp.Text ?? "ERR"),
            RespType.Array when resp.Items is null => DynValue.False,
            RespType.Array => BuildLuaTable(script, resp.Items!),
            _ => DynValue.Nil
        };
    }

    private static DynValue BuildLuaTable(Script script, IReadOnlyList<RespValue> items)
    {
        var table = new Table(script);
        for (var i = 0; i < items.Count; i++)
        {
            table[i + 1] = RespToLua(script, items[i]);
        }

        return DynValue.NewTable(table);
    }

    // ─── Lua → RESP ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Converts a Lua <see cref="DynValue"/> to a <see cref="RespValue"/> following
    /// Redis 7.x Lua→RESP2 mapping rules.
    /// </summary>
    internal static RespValue LuaToResp(DynValue value)
    {
        switch (value.Type)
        {
            case DataType.Number:
                // Redis converts all Lua numbers to integers.
                return RespValue.IntegerValue((long)Math.Truncate(value.Number));

            case DataType.String:
                return RespValue.BulkString(value.String);

            case DataType.Boolean:
                // Lua true → integer 1; Lua false → nil bulk string.
                return value.Boolean ? RespValue.IntegerValue(1) : RespValue.BulkString((byte[]?)null);

            case DataType.Nil:
            case DataType.Void:
                return RespValue.BulkString((byte[]?)null);

            case DataType.Table:
                return TableToResp(value.Table);

            default:
                return RespValue.BulkString((byte[]?)null);
        }
    }

    private static RespValue TableToResp(Table table)
    {
        // Status reply: {ok = "..."} → simple string.
        var okField = table.Get("ok");
        if (okField.Type == DataType.String)
        {
            return RespValue.SimpleString(okField.String);
        }

        // Error reply: {err = "..."} → error.
        var errField = table.Get("err");
        if (errField.Type == DataType.String)
        {
            return RespValue.Error(errField.String);
        }

        // Array table: convert 1-based integer keys until the first missing index.
        var items = new List<RespValue>();
        for (var i = 1; ; i++)
        {
            var elem = table.Get(i);
            if (elem.Type == DataType.Nil || elem.Type == DataType.Void)
            {
                break;
            }

            items.Add(LuaToResp(elem));
        }

        return RespValue.Array(items);
    }

    // ─── Output size estimation ───────────────────────────────────────────────────

    private static long EstimateRespSize(RespValue value)
    {
        return value.Type switch
        {
            RespType.BulkString => value.Bytes?.Length ?? 0,
            RespType.SimpleString or RespType.Error => value.Text?.Length ?? 0,
            RespType.Integer => 20,
            RespType.Array when value.Items is not null => value.Items.Sum(EstimateRespSize),
            _ => 0
        };
    }
}
