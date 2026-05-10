using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using SwarmKeyDb;

namespace SwarmKeyDb.Server;

public sealed class WebSocketGateway : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly HttpListener _listener = new();
    private readonly RedisCommandProcessor _processor;
    private readonly PubSubManager? _pubSubManager;
    private readonly MonitoringMetrics _metrics;
    private readonly string? _requiredPassword;
    private readonly HashSet<string> _allowedOrigins;
    private readonly bool _allowAnyOrigin;
    private readonly ILogger<WebSocketGateway> _logger;

    public WebSocketGateway(
        IPAddress address,
        int port,
        RedisCommandProcessor processor,
        MonitoringMetrics metrics,
        PubSubManager? pubSubManager = null,
        string? requiredPassword = null,
        IEnumerable<string>? allowedOrigins = null,
        ILogger<WebSocketGateway>? logger = null)
    {
        _processor = processor;
        _metrics = metrics;
        _pubSubManager = pubSubManager;
        _requiredPassword = string.IsNullOrWhiteSpace(requiredPassword) ? null : requiredPassword;
        _logger = logger ?? NullLogger<WebSocketGateway>.Instance;
        _allowedOrigins = new HashSet<string>(
            (allowedOrigins ?? ["*"])
                .Where(static origin => !string.IsNullOrWhiteSpace(origin))
                .Select(static origin => origin.Trim()),
            StringComparer.OrdinalIgnoreCase);
        _allowAnyOrigin = _allowedOrigins.Count == 0 || _allowedOrigins.Contains("*");
        _listener.Prefixes.Add($"http://{(address.Equals(IPAddress.Any) ? "+" : address.ToString())}:{port}/");
    }

    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        _listener.Start();
        _logger.LogInformation("SwarmKeyDb WebSocket gateway listening on {Prefix}", _listener.Prefixes.FirstOrDefault());
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                HttpListenerContext context;
                try
                {
                    context = await _listener.GetContextAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (HttpListenerException)
                {
                    break;
                }

                _ = Task.Run(() => HandleContextAsync(context, cancellationToken), cancellationToken);
            }
        }
        finally
        {
            _listener.Stop();
        }
    }

    public void Dispose()
    {
        _listener.Close();
    }

    private async Task HandleContextAsync(HttpListenerContext context, CancellationToken cancellationToken)
    {
        if (!context.Request.IsWebSocketRequest)
        {
            context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
            context.Response.Close();
            return;
        }

        if (!IsOriginAllowed(context.Request.Headers["Origin"]))
        {
            _metrics.OnWebSocketError();
            context.Response.StatusCode = (int)HttpStatusCode.Forbidden;
            context.Response.Close();
            return;
        }

        HttpListenerWebSocketContext webSocketContext;
        try
        {
            webSocketContext = await context.AcceptWebSocketAsync(subProtocol: null).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _metrics.OnWebSocketError();
            _logger.LogWarning(ex, "WebSocket upgrade failed.");
            return;
        }

        _metrics.OnWebSocketConnectionOpened();
        try
        {
            await HandleSocketAsync(webSocketContext.WebSocket, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _metrics.OnWebSocketError();
            _logger.LogWarning(ex, "WebSocket connection closed with error.");
        }
        finally
        {
            _metrics.OnWebSocketConnectionClosed();
            try
            {
                if (webSocketContext.WebSocket.State is WebSocketState.Open or WebSocketState.CloseReceived)
                {
                    await webSocketContext.WebSocket.CloseAsync(
                        WebSocketCloseStatus.NormalClosure,
                        "closing",
                        CancellationToken.None).ConfigureAwait(false);
                }
            }
            catch
            {
            }

            webSocketContext.WebSocket.Dispose();
        }
    }

    private async Task HandleSocketAsync(WebSocket socket, CancellationToken cancellationToken)
    {
        var connectionId = Guid.NewGuid().ToString("N");
        var channelSubs = new HashSet<string>(StringComparer.Ordinal);
        var patternSubs = new HashSet<string>(StringComparer.Ordinal);
        var pushChannel = Channel.CreateBounded<RespValue>(new BoundedChannelOptions(256)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true
        });
        var sendLock = new SemaphoreSlim(1, 1);
        WebSocketResponseMode outputMode = WebSocketResponseMode.Json;
        var isAuthenticated = _requiredPassword is null;
        var pushTask = PumpPushAsync(socket, pushChannel.Reader, () => outputMode, sendLock, cancellationToken);

        try
        {
            while (!cancellationToken.IsCancellationRequested && socket.State == WebSocketState.Open)
            {
                var message = await ReceiveMessageAsync(socket, cancellationToken).ConfigureAwait(false);
                if (message is null)
                {
                    break;
                }

                _metrics.OnWebSocketMessageReceived();
                if (!TryParseCommand(message, out var request, out var command, out var inputMode, out var parseError))
                {
                    outputMode = inputMode;
                    await SendErrorAsync(socket, outputMode, parseError ?? "ERR invalid command frame", null, sendLock, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                outputMode = inputMode;
                if (!isAuthenticated && command is not "AUTH" and not "PING" and not "QUIT")
                {
                    await SendErrorAsync(socket, outputMode, "NOAUTH Authentication required.", command, sendLock, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                if (command == "AUTH")
                {
                    var authResponse = Authenticate(request);
                    if (authResponse.Type != RespType.Error)
                    {
                        isAuthenticated = true;
                    }

                    await SendResponseAsync(socket, outputMode, authResponse, command, sendLock, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                if (_pubSubManager is not null && command is "SUBSCRIBE" or "UNSUBSCRIBE" or "PSUBSCRIBE" or "PUNSUBSCRIBE")
                {
                    await HandlePubSubCommandAsync(
                        request,
                        command,
                        connectionId,
                        pushChannel.Writer,
                        channelSubs,
                        patternSubs,
                        socket,
                        outputMode,
                        sendLock,
                        cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var isSubscribed = channelSubs.Count + patternSubs.Count > 0;
                if (isSubscribed && command is not ("PING" or "RESET" or "QUIT"))
                {
                    await SendErrorAsync(
                        socket,
                        outputMode,
                        $"ERR Can't call '{command.ToLowerInvariant()}' in subscribe mode",
                        command,
                        sendLock,
                        cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var response = await _processor.ExecuteAsync(request, cancellationToken).ConfigureAwait(false);
                await SendResponseAsync(socket, outputMode, response, command, sendLock, cancellationToken).ConfigureAwait(false);
                if (command == "QUIT")
                {
                    break;
                }
            }
        }
        finally
        {
            if (_pubSubManager is not null)
            {
                _pubSubManager.RemoveConnection(connectionId);
            }

            pushChannel.Writer.TryComplete();
            try
            {
                await pushTask.ConfigureAwait(false);
            }
            catch
            {
            }

            sendLock.Dispose();
        }
    }

    private async Task HandlePubSubCommandAsync(
        RespValue request,
        string command,
        string connectionId,
        ChannelWriter<RespValue> pushWriter,
        HashSet<string> channelSubs,
        HashSet<string> patternSubs,
        WebSocket socket,
        WebSocketResponseMode outputMode,
        SemaphoreSlim sendLock,
        CancellationToken cancellationToken)
    {
        var args = request.Items!;
        var isPattern = command is "PSUBSCRIBE" or "PUNSUBSCRIBE";
        var isSubscribe = command is "SUBSCRIBE" or "PSUBSCRIBE";
        var replyType = isPattern
            ? (isSubscribe ? "psubscribe" : "punsubscribe")
            : (isSubscribe ? "subscribe" : "unsubscribe");

        if (isSubscribe)
        {
            if (args.Count < 2)
            {
                await SendErrorAsync(
                    socket,
                    outputMode,
                    $"ERR wrong number of arguments for '{command.ToLowerInvariant()}'",
                    command,
                    sendLock,
                    cancellationToken).ConfigureAwait(false);
                return;
            }

            for (var i = 1; i < args.Count; i++)
            {
                var target = args[i].AsString();
                int total;
                if (isPattern)
                {
                    total = _pubSubManager!.PSubscribe(connectionId, target, pushWriter);
                    patternSubs.Add(target);
                }
                else
                {
                    total = _pubSubManager!.Subscribe(connectionId, target, pushWriter);
                    channelSubs.Add(target);
                }

                await SendResponseAsync(
                    socket,
                    outputMode,
                    RespValue.Array(
                    [
                        RespValue.BulkString(replyType),
                        RespValue.BulkString(target),
                        RespValue.IntegerValue(total)
                    ]),
                    command,
                    sendLock,
                    cancellationToken).ConfigureAwait(false);
            }

            return;
        }

        IReadOnlyList<string> targets;
        if (args.Count < 2)
        {
            targets = isPattern ? patternSubs.ToArray() : channelSubs.ToArray();
        }
        else
        {
            targets = args.Skip(1).Select(static item => item.AsString()).ToArray();
        }

        if (targets.Count == 0)
        {
            await SendResponseAsync(
                socket,
                outputMode,
                RespValue.Array(
                [
                    RespValue.BulkString(replyType),
                    RespValue.BulkString((string?)null),
                    RespValue.IntegerValue(0)
                ]),
                command,
                sendLock,
                cancellationToken).ConfigureAwait(false);
            return;
        }

        foreach (var target in targets)
        {
            int remaining;
            if (isPattern)
            {
                remaining = _pubSubManager!.PUnsubscribe(connectionId, target);
                patternSubs.Remove(target);
            }
            else
            {
                remaining = _pubSubManager!.Unsubscribe(connectionId, target);
                channelSubs.Remove(target);
            }

            await SendResponseAsync(
                socket,
                outputMode,
                RespValue.Array(
                [
                    RespValue.BulkString(replyType),
                    RespValue.BulkString(target),
                    RespValue.IntegerValue(remaining)
                ]),
                command,
                sendLock,
                cancellationToken).ConfigureAwait(false);
        }
    }

    private RespValue Authenticate(RespValue request)
    {
        if (_requiredPassword is null)
        {
            return RespValue.SimpleString("OK");
        }

        if (request.Items is not { Count: 2 })
        {
            return RespValue.Error("ERR wrong number of arguments for 'AUTH' command");
        }

        var provided = request.Items[1].AsString();
        return provided == _requiredPassword
            ? RespValue.SimpleString("OK")
            : RespValue.Error("WRONGPASS invalid username-password pair or user is disabled.");
    }

    private async Task PumpPushAsync(
        WebSocket socket,
        ChannelReader<RespValue> reader,
        Func<WebSocketResponseMode> outputMode,
        SemaphoreSlim sendLock,
        CancellationToken cancellationToken)
    {
        while (await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            while (reader.TryRead(out var push))
            {
                await SendFrameAsync(socket, outputMode(), push, null, isPush: true, sendLock, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static async Task<string?> ReceiveMessageAsync(WebSocket socket, CancellationToken cancellationToken)
    {
        var buffer = new byte[4096];
        using var stream = new MemoryStream();
        while (true)
        {
            var result = await socket.ReceiveAsync(buffer, cancellationToken).ConfigureAwait(false);
            if (result.MessageType == WebSocketMessageType.Close)
            {
                return null;
            }

            if (result.Count > 0)
            {
                stream.Write(buffer, 0, result.Count);
            }

            if (result.EndOfMessage)
            {
                break;
            }
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private bool IsOriginAllowed(string? origin)
    {
        if (_allowAnyOrigin)
        {
            return true;
        }

        if (string.IsNullOrWhiteSpace(origin))
        {
            return false;
        }

        return _allowedOrigins.Contains(origin.Trim());
    }

    private bool TryParseCommand(
        string frame,
        out RespValue request,
        out string command,
        out WebSocketResponseMode mode,
        out string? error)
    {
        request = RespValue.Array(Array.Empty<RespValue>());
        command = string.Empty;
        mode = LooksLikeJson(frame) ? WebSocketResponseMode.Json : WebSocketResponseMode.Resp;
        error = null;

        if (mode == WebSocketResponseMode.Json)
        {
            try
            {
                using var document = JsonDocument.Parse(frame);
                if (!document.RootElement.TryGetProperty("cmd", out var cmdElement) || cmdElement.ValueKind != JsonValueKind.String)
                {
                    error = "ERR JSON command must include string 'cmd'.";
                    return false;
                }

                var cmd = cmdElement.GetString();
                if (string.IsNullOrWhiteSpace(cmd))
                {
                    error = "ERR JSON command must include string 'cmd'.";
                    return false;
                }

                var items = new List<RespValue> { RespValue.BulkString(cmd) };
                if (document.RootElement.TryGetProperty("args", out var argsElement))
                {
                    if (argsElement.ValueKind != JsonValueKind.Array)
                    {
                        error = "ERR JSON field 'args' must be an array.";
                        return false;
                    }

                    foreach (var arg in argsElement.EnumerateArray())
                    {
                        if (arg.ValueKind != JsonValueKind.String)
                        {
                            error = "ERR JSON command args must be strings.";
                            return false;
                        }

                        items.Add(RespValue.BulkString(arg.GetString()));
                    }
                }

                request = RespValue.Array(items);
                command = cmd.ToUpperInvariant();
                return true;
            }
            catch (JsonException)
            {
                error = "ERR malformed JSON command.";
                return false;
            }
        }

        try
        {
            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(frame));
            var parsed = new RespReader(stream).ReadAsync(CancellationToken.None).GetAwaiter().GetResult();
            if (parsed is null || parsed.Type != RespType.Array || parsed.Items is null || parsed.Items.Count == 0)
            {
                error = "ERR Protocol error: invalid RESP frame";
                return false;
            }

            request = parsed;
            command = request.Items[0].AsString().ToUpperInvariant();
            return true;
        }
        catch (Exception ex) when (ex is InvalidDataException or EndOfStreamException or FormatException or OverflowException)
        {
            error = "ERR Protocol error: invalid RESP frame";
            return false;
        }
    }

    private async Task SendResponseAsync(
        WebSocket socket,
        WebSocketResponseMode mode,
        RespValue value,
        string command,
        SemaphoreSlim sendLock,
        CancellationToken cancellationToken)
    {
        await SendFrameAsync(socket, mode, value, command, isPush: false, sendLock, cancellationToken).ConfigureAwait(false);
    }

    private async Task SendErrorAsync(
        WebSocket socket,
        WebSocketResponseMode mode,
        string error,
        string? command,
        SemaphoreSlim sendLock,
        CancellationToken cancellationToken)
    {
        _metrics.OnWebSocketError();
        await SendFrameAsync(socket, mode, RespValue.Error(error), command, isPush: false, sendLock, cancellationToken).ConfigureAwait(false);
    }

    private async Task SendFrameAsync(
        WebSocket socket,
        WebSocketResponseMode mode,
        RespValue value,
        string? command,
        bool isPush,
        SemaphoreSlim sendLock,
        CancellationToken cancellationToken)
    {
        if (socket.State != WebSocketState.Open)
        {
            return;
        }

        string text;
        if (mode == WebSocketResponseMode.Resp)
        {
            using var stream = new MemoryStream();
            var writer = new RespWriter(stream);
            await writer.WriteAsync(value, cancellationToken).ConfigureAwait(false);
            text = Encoding.UTF8.GetString(stream.ToArray());
        }
        else
        {
            object payload = value.Type == RespType.Error
                ? new { error = value.Text, cmd = command }
                : isPush
                    ? new { push = ToJson(value) }
                    : new { cmd = command, data = ToJson(value) };
            text = JsonSerializer.Serialize(payload, JsonOptions);
        }

        await sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var bytes = Encoding.UTF8.GetBytes(text);
            await socket.SendAsync(bytes, WebSocketMessageType.Text, endOfMessage: true, cancellationToken).ConfigureAwait(false);
            _metrics.OnWebSocketMessageSent();
        }
        finally
        {
            sendLock.Release();
        }
    }

    private static bool LooksLikeJson(string payload)
    {
        foreach (var ch in payload)
        {
            if (char.IsWhiteSpace(ch))
            {
                continue;
            }

            return ch == '{';
        }

        return false;
    }

    private static object? ToJson(RespValue value)
    {
        return value.Type switch
        {
            RespType.SimpleString => value.Text,
            RespType.Error => new Dictionary<string, object?> { ["error"] = value.Text },
            RespType.Integer => value.Integer,
            RespType.BulkString => value.Bytes is null ? null : Encoding.UTF8.GetString(value.Bytes),
            RespType.Array => value.Items is null ? null : value.Items.Select(ToJson).ToArray(),
            _ => null
        };
    }

    private enum WebSocketResponseMode
    {
        Json,
        Resp
    }
}
