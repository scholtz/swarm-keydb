using System.Net;
using System.Net.WebSockets;
using System.IO.Pipelines;
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
        var sendLock = new SemaphoreSlim(1, 1);
        WebSocketResponseMode outputMode = WebSocketResponseMode.Json;
        var protocolVersion = 2;
        var isTrackingEnabled = false;
        var isAuthenticated = _requiredPassword is null;
        var pendingCommands = Channel.CreateUnbounded<PendingCommand>();
        var inputPipe = new Pipe();
        var outputPipe = new Pipe();
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        await using var processorInputReaderStream = inputPipe.Reader.AsStream();
        await using var processorInputWriterStream = inputPipe.Writer.AsStream();
        await using var processorOutputReaderStream = outputPipe.Reader.AsStream();
        await using var processorOutputWriterStream = outputPipe.Writer.AsStream();
        var processorTask = _processor.ProcessAsync(processorInputReaderStream, processorOutputWriterStream, linkedCts.Token);
        var processorWriter = new RespWriter(processorInputWriterStream);
        var responseTask = PumpResponsesAsync(
            socket,
            new RespReader(processorOutputReaderStream),
            pendingCommands.Reader,
            () => outputMode,
            () => protocolVersion,
            updateOutcome: (pending, response) =>
            {
                ApplyCommandOutcome(pending, response, ref protocolVersion, ref isTrackingEnabled);
            },
            sendLock,
            linkedCts.Token);

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
                if (!TryParseCommand(message, out var request, out var command, out var args, out var inputMode, out var parseError))
                {
                    outputMode = inputMode;
                    await SendErrorAsync(socket, outputMode, parseError ?? "ERR invalid command frame", null, sendLock, protocolVersion, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                outputMode = inputMode;
                if (!isAuthenticated && command is not "AUTH" and not "PING" and not "QUIT")
                {
                    await SendErrorAsync(socket, outputMode, "NOAUTH Authentication required.", command, sendLock, protocolVersion, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                if (command == "AUTH")
                {
                    var authResponse = Authenticate(request);
                    if (authResponse.Type != RespType.Error)
                    {
                        isAuthenticated = true;
                    }

                    await SendResponseAsync(socket, outputMode, authResponse, command, sendLock, protocolVersion, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                await processorWriter.WriteAsync(request, cancellationToken).ConfigureAwait(false);
                pendingCommands.Writer.TryWrite(new PendingCommand(command, args, outputMode));
                if (command == "QUIT")
                {
                    break;
                }
            }
        }
        finally
        {
            if (isTrackingEnabled)
            {
                _metrics.OnWebSocketClientTrackingDisabled();
            }

            pendingCommands.Writer.TryComplete();
            try
            {
                await processorInputWriterStream.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
            }

            linkedCts.Cancel();

            try
            {
                await responseTask.ConfigureAwait(false);
            }
            catch
            {
            }

            try
            {
                await processorTask.ConfigureAwait(false);
            }
            catch
            {
            }

            sendLock.Dispose();
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

    private async Task PumpResponsesAsync(
        WebSocket socket,
        RespReader reader,
        ChannelReader<PendingCommand> pending,
        Func<WebSocketResponseMode> outputMode,
        Func<int> protocolVersion,
        Action<PendingCommand, RespValue> updateOutcome,
        SemaphoreSlim sendLock,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested && socket.State == WebSocketState.Open)
        {
            RespValue value;
            try
            {
                value = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (EndOfStreamException)
            {
                break;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }

            if (IsPushValue(value))
            {
                await SendFrameAsync(socket, outputMode(), value, null, isPush: true, sendLock, protocolVersion(), cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (!await pending.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                await SendFrameAsync(socket, outputMode(), value, null, isPush: false, sendLock, protocolVersion(), cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (!pending.TryRead(out var command))
            {
                continue;
            }

            await SendFrameAsync(socket, command.Mode, value, command.Command, isPush: false, sendLock, protocolVersion(), cancellationToken).ConfigureAwait(false);
            updateOutcome(command, value);
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
        out string[] args,
        out WebSocketResponseMode mode,
        out string? error)
    {
        request = RespValue.Array(Array.Empty<RespValue>());
        command = string.Empty;
        args = [];
        mode = LooksLikeJson(frame) ? WebSocketResponseMode.Json : WebSocketResponseMode.Resp;
        error = null;

        if (mode == WebSocketResponseMode.Json)
        {
            try
            {
                using var document = JsonDocument.Parse(frame);
                if (document.RootElement.ValueKind == JsonValueKind.Array)
                {
                    if (document.RootElement.GetArrayLength() == 0 || document.RootElement[0].ValueKind != JsonValueKind.String)
                    {
                        error = "ERR JSON array command must start with a string command name.";
                        return false;
                    }

                    var jsonArrayCommand = document.RootElement[0].GetString();
                    if (string.IsNullOrWhiteSpace(jsonArrayCommand))
                    {
                        error = "ERR JSON array command must start with a string command name.";
                        return false;
                    }

                    var jsonArrayItems = new List<RespValue> { RespValue.BulkString(jsonArrayCommand) };
                    var jsonArrayArgs = new List<string>();
                    for (var i = 1; i < document.RootElement.GetArrayLength(); i++)
                    {
                        var arg = document.RootElement[i];
                        if (arg.ValueKind != JsonValueKind.String)
                        {
                            error = "ERR JSON command args must be strings.";
                            return false;
                        }

                        var argText = arg.GetString() ?? string.Empty;
                        jsonArrayItems.Add(RespValue.BulkString(argText));
                        jsonArrayArgs.Add(argText);
                    }

                    request = RespValue.Array(jsonArrayItems);
                    command = jsonArrayCommand.ToUpperInvariant();
                    args = jsonArrayArgs.ToArray();
                    return true;
                }

                if (document.RootElement.ValueKind != JsonValueKind.Object ||
                    !document.RootElement.TryGetProperty("cmd", out var cmdElement) ||
                    cmdElement.ValueKind != JsonValueKind.String)
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
                var parsedArgs = new List<string>();
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
                        parsedArgs.Add(arg.GetString() ?? string.Empty);
                    }
                }

                request = RespValue.Array(items);
                command = cmd.ToUpperInvariant();
                args = parsedArgs.ToArray();
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
            args = request.Items.Skip(1).Select(static item => item.AsString()).ToArray();
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
        int protocolVersion,
        CancellationToken cancellationToken)
    {
        await SendFrameAsync(socket, mode, value, command, isPush: false, sendLock, protocolVersion, cancellationToken).ConfigureAwait(false);
    }

    private async Task SendErrorAsync(
        WebSocket socket,
        WebSocketResponseMode mode,
        string error,
        string? command,
        SemaphoreSlim sendLock,
        int protocolVersion,
        CancellationToken cancellationToken)
    {
        _metrics.OnWebSocketError();
        await SendFrameAsync(socket, mode, RespValue.Error(error), command, isPush: false, sendLock, protocolVersion, cancellationToken).ConfigureAwait(false);
    }

    private async Task SendFrameAsync(
        WebSocket socket,
        WebSocketResponseMode mode,
        RespValue value,
        string? command,
        bool isPush,
        SemaphoreSlim sendLock,
        int protocolVersion,
        CancellationToken cancellationToken)
    {
        if (socket.State != WebSocketState.Open)
        {
            return;
        }

        byte[] payloadBytes;
        WebSocketMessageType messageType;
        if (mode == WebSocketResponseMode.Resp)
        {
            using var stream = new MemoryStream();
            var writer = new RespWriter(stream)
            {
                ProtocolVersion = protocolVersion
            };
            await writer.WriteAsync(value, cancellationToken).ConfigureAwait(false);
            payloadBytes = stream.ToArray();
            messageType = WebSocketMessageType.Binary;
        }
        else
        {
            object payload = value.Type == RespType.Error
                ? new { error = value.Text, cmd = command }
                : isPush
                    ? new { type = "push", data = ToJson(value, protocolVersion) }
                    : ToJson(value, protocolVersion) ?? new object();
            payloadBytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(payload, JsonOptions));
            messageType = WebSocketMessageType.Text;
        }

        await sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await socket.SendAsync(payloadBytes, messageType, endOfMessage: true, cancellationToken).ConfigureAwait(false);
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

            return ch is '{' or '[';
        }

        return false;
    }

    private static object? ToJson(RespValue value, int protocolVersion)
    {
        return value.Type switch
        {
            RespType.SimpleString => value.Text,
            RespType.Error => new Dictionary<string, object?> { ["error"] = value.Text },
            RespType.Integer => value.Integer,
            RespType.BulkString => value.Bytes is null ? null : Encoding.UTF8.GetString(value.Bytes),
            RespType.Array => value.Items is null ? null : value.Items.Select(item => ToJson(item, protocolVersion)).ToArray(),
            RespType.Map => MapToJson(value.Items, protocolVersion),
            RespType.Set => value.Items is null ? null : value.Items.Select(item => ToJson(item, protocolVersion)).ToArray(),
            RespType.Double => protocolVersion >= 3 ? value.DoubleValue : value.AsDoubleString(),
            RespType.Boolean => protocolVersion >= 3 ? value.BoolValue : (value.BoolValue ? 1 : 0),
            RespType.BigNumber => value.Text,
            RespType.VerbatimString => value.Text,
            RespType.Null => null,
            RespType.BlobError => value.Text,
            RespType.Push => value.Items is null ? null : value.Items.Select(item => ToJson(item, protocolVersion)).ToArray(),
            _ => null
        };
    }

    private static object? MapToJson(IReadOnlyList<RespValue>? items, int protocolVersion)
    {
        if (items is null)
        {
            return null;
        }

        if (protocolVersion < 3)
        {
            return items.Select(item => ToJson(item, protocolVersion)).ToArray();
        }

        if (items.Count % 2 != 0)
        {
            return items.Select(item => ToJson(item, protocolVersion)).ToArray();
        }

        var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
        for (var i = 0; i < items.Count; i += 2)
        {
            dict[ToJsonKey(items[i])] = ToJson(items[i + 1], protocolVersion);
        }

        return dict;
    }

    private static string ToJsonKey(RespValue value) =>
        value.Type switch
        {
            RespType.SimpleString => value.Text ?? string.Empty,
            RespType.BulkString => value.Bytes is null ? string.Empty : Encoding.UTF8.GetString(value.Bytes),
            RespType.Integer => value.Integer.ToString(),
            _ => ToJson(value, 3)?.ToString() ?? string.Empty
        };

    private static bool IsPushValue(RespValue value)
    {
        if (value.Type == RespType.Push)
        {
            return true;
        }

        if (value.Type == RespType.Array && value.Items is { Count: > 0 })
        {
            var first = value.Items[0].AsString();
            return first is "message" or "pmessage" or "invalidate";
        }

        return false;
    }

    private void ApplyCommandOutcome(PendingCommand pending, RespValue response, ref int protocolVersion, ref bool trackingEnabled)
    {
        if (response.Type == RespType.Error)
        {
            return;
        }

        if (pending.Command == "HELLO" && pending.Arguments.Length >= 1)
        {
            if (pending.Arguments[0] == "3")
            {
                if (protocolVersion != 3)
                {
                    protocolVersion = 3;
                    _metrics.OnWebSocketResp3Negotiated();
                }
            }
            else if (pending.Arguments[0] == "2")
            {
                protocolVersion = 2;
            }
        }
        else if (pending.Command == "CLIENT" && pending.Arguments.Length >= 2 &&
                 string.Equals(pending.Arguments[0], "TRACKING", StringComparison.OrdinalIgnoreCase))
        {
            if (string.Equals(pending.Arguments[1], "ON", StringComparison.OrdinalIgnoreCase) && !trackingEnabled)
            {
                trackingEnabled = true;
                _metrics.OnWebSocketClientTrackingEnabled();
            }
            else if (string.Equals(pending.Arguments[1], "OFF", StringComparison.OrdinalIgnoreCase) && trackingEnabled)
            {
                trackingEnabled = false;
                _metrics.OnWebSocketClientTrackingDisabled();
            }
        }
        else if (pending.Command == "RESET")
        {
            protocolVersion = 2;
            if (trackingEnabled)
            {
                trackingEnabled = false;
                _metrics.OnWebSocketClientTrackingDisabled();
            }
        }
    }

    private readonly record struct PendingCommand(string Command, string[] Arguments, WebSocketResponseMode Mode);

    private enum WebSocketResponseMode
    {
        Json,
        Resp
    }
}
