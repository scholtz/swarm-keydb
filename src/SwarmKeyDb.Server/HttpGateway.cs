using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using SwarmKeyDb;

namespace SwarmKeyDb.Server;

public sealed class HttpGateway : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly HttpListener _listener = new();
    private readonly RedisCommandProcessor _processor;
    private readonly MonitoringMetrics _metrics;
    private readonly string? _requiredPassword;
    private readonly HashSet<string> _allowedOrigins;
    private readonly bool _allowAnyOrigin;
    private readonly ILogger<HttpGateway> _logger;

    public HttpGateway(
        IPAddress address,
        int port,
        RedisCommandProcessor processor,
        MonitoringMetrics metrics,
        string? requiredPassword = null,
        IEnumerable<string>? allowedOrigins = null,
        ILogger<HttpGateway>? logger = null)
    {
        _processor = processor;
        _metrics = metrics;
        _requiredPassword = string.IsNullOrWhiteSpace(requiredPassword) ? null : requiredPassword;
        _logger = logger ?? NullLogger<HttpGateway>.Instance;
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
        _logger.LogInformation("SwarmKeyDb HTTP gateway listening on {Prefix}", _listener.Prefixes.FirstOrDefault());
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

    public void Dispose() => _listener.Close();

    private async Task HandleContextAsync(HttpListenerContext context, CancellationToken cancellationToken)
    {
        var request = context.Request;
        var response = context.Response;
        var method = request.HttpMethod ?? "GET";
        var route = "unknown";
        var statusCode = (int)HttpStatusCode.InternalServerError;
        var startedAt = Stopwatch.StartNew();

        try
        {
            var origin = request.Headers["Origin"];
            if (!IsOriginAllowed(origin))
            {
                statusCode = (int)HttpStatusCode.Forbidden;
                await WriteErrorAsync(response, HttpStatusCode.Forbidden, "ERR CORS origin forbidden", origin, cancellationToken).ConfigureAwait(false);
                return;
            }

            AddCorsHeaders(response, origin);
            if (HttpMethodsMatch(method, "OPTIONS"))
            {
                route = "OPTIONS";
                statusCode = (int)HttpStatusCode.NoContent;
                response.StatusCode = statusCode;
                response.Close();
                return;
            }

            var path = request.Url?.AbsolutePath ?? "/";
            var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
            var resp3Requested = WantsResp3Json(request.Headers["Accept"]);

            if (path.Equals("/openapi.json", StringComparison.OrdinalIgnoreCase))
            {
                route = "/openapi.json";
                statusCode = (int)HttpStatusCode.OK;
                await WriteJsonAsync(response, HttpStatusCode.OK, BuildOpenApiSpec(), origin, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (path.Equals("/docs", StringComparison.OrdinalIgnoreCase))
            {
                route = "/docs";
                statusCode = (int)HttpStatusCode.OK;
                await WriteTextAsync(response, HttpStatusCode.OK, SwaggerUiHtml, "text/html; charset=utf-8", origin, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (_requiredPassword is not null)
            {
                var authResult = Authenticate(request);
                if (!authResult.Success)
                {
                    route = "AUTH";
                    statusCode = (int)HttpStatusCode.Unauthorized;
                    await WriteErrorAsync(response, HttpStatusCode.Unauthorized, authResult.ErrorMessage!, origin, cancellationToken).ConfigureAwait(false);
                    return;
                }
            }

            if (HttpMethodsMatch(method, "GET") && segments.Length == 2 && segments[0].Equals("get", StringComparison.OrdinalIgnoreCase))
            {
                route = "/get/{key}";
                statusCode = await ExecuteRouteAsync(
                    response,
                    origin,
                    resp3Requested,
                    cancellationToken,
                    "GET",
                    DecodeSegment(segments[1])).ConfigureAwait(false);
                return;
            }

            if (HttpMethodsMatch(method, "POST") && segments.Length == 2 && segments[0].Equals("set", StringComparison.OrdinalIgnoreCase))
            {
                route = "/set/{key}";
                var key = DecodeSegment(segments[1]);
                using var body = await ParseBodyAsync(request, cancellationToken).ConfigureAwait(false);
                if (body is null)
                {
                    statusCode = (int)HttpStatusCode.BadRequest;
                    await WriteErrorAsync(response, HttpStatusCode.BadRequest, "ERR request body must be valid JSON.", origin, cancellationToken).ConfigureAwait(false);
                    return;
                }

                if (!body.RootElement.TryGetProperty("value", out var valueElement) || valueElement.ValueKind != JsonValueKind.String)
                {
                    statusCode = (int)HttpStatusCode.BadRequest;
                    await WriteErrorAsync(response, HttpStatusCode.BadRequest, "ERR 'value' field is required and must be a string.", origin, cancellationToken).ConfigureAwait(false);
                    return;
                }

                var args = new List<string> { key, valueElement.GetString() ?? string.Empty };
                if (body.RootElement.TryGetProperty("ex", out var exElement))
                {
                    if (exElement.ValueKind != JsonValueKind.Number || !exElement.TryGetInt64(out var exSeconds) || exSeconds <= 0)
                    {
                        statusCode = (int)HttpStatusCode.BadRequest;
                        await WriteErrorAsync(response, HttpStatusCode.BadRequest, "ERR 'ex' must be a positive integer.", origin, cancellationToken).ConfigureAwait(false);
                        return;
                    }

                    args.Add("EX");
                    args.Add(exSeconds.ToString());
                }

                statusCode = await ExecuteRouteAsync(response, origin, resp3Requested, cancellationToken, "SET", args.ToArray()).ConfigureAwait(false);
                return;
            }

            if (HttpMethodsMatch(method, "DELETE") && segments.Length == 2 && segments[0].Equals("del", StringComparison.OrdinalIgnoreCase))
            {
                route = "/del/{key}";
                statusCode = await ExecuteRouteAsync(
                    response,
                    origin,
                    resp3Requested,
                    cancellationToken,
                    "DEL",
                    DecodeSegment(segments[1])).ConfigureAwait(false);
                return;
            }

            if (HttpMethodsMatch(method, "GET") && segments.Length == 2 && segments[0].Equals("exists", StringComparison.OrdinalIgnoreCase))
            {
                route = "/exists/{key}";
                statusCode = await ExecuteRouteAsync(
                    response,
                    origin,
                    resp3Requested,
                    cancellationToken,
                    "EXISTS",
                    DecodeSegment(segments[1])).ConfigureAwait(false);
                return;
            }

            if (HttpMethodsMatch(method, "POST") && segments.Length == 2 && segments[0].Equals("expire", StringComparison.OrdinalIgnoreCase))
            {
                route = "/expire/{key}";
                using var body = await ParseBodyAsync(request, cancellationToken).ConfigureAwait(false);
                if (body is null ||
                    !body.RootElement.TryGetProperty("seconds", out var secondsElement) ||
                    secondsElement.ValueKind != JsonValueKind.Number ||
                    !secondsElement.TryGetInt64(out var seconds))
                {
                    statusCode = (int)HttpStatusCode.BadRequest;
                    await WriteErrorAsync(response, HttpStatusCode.BadRequest, "ERR 'seconds' field is required and must be an integer.", origin, cancellationToken).ConfigureAwait(false);
                    return;
                }

                statusCode = await ExecuteRouteAsync(
                    response,
                    origin,
                    resp3Requested,
                    cancellationToken,
                    "EXPIRE",
                    DecodeSegment(segments[1]),
                    seconds.ToString()).ConfigureAwait(false);
                return;
            }

            if (HttpMethodsMatch(method, "GET") && segments.Length == 2 && segments[0].Equals("ttl", StringComparison.OrdinalIgnoreCase))
            {
                route = "/ttl/{key}";
                statusCode = await ExecuteRouteAsync(
                    response,
                    origin,
                    resp3Requested,
                    cancellationToken,
                    "TTL",
                    DecodeSegment(segments[1])).ConfigureAwait(false);
                return;
            }

            if (HttpMethodsMatch(method, "GET") && segments.Length == 2 && segments[0].Equals("keys", StringComparison.OrdinalIgnoreCase))
            {
                route = "/keys/{pattern}";
                statusCode = await ExecuteRouteAsync(
                    response,
                    origin,
                    resp3Requested,
                    cancellationToken,
                    "KEYS",
                    DecodeSegment(segments[1])).ConfigureAwait(false);
                return;
            }

            if (HttpMethodsMatch(method, "POST") && segments.Length == 2 && segments[0].Equals("publish", StringComparison.OrdinalIgnoreCase))
            {
                route = "/publish/{channel}";
                using var body = await ParseBodyAsync(request, cancellationToken).ConfigureAwait(false);
                if (body is null || !body.RootElement.TryGetProperty("message", out var messageElement) || messageElement.ValueKind != JsonValueKind.String)
                {
                    statusCode = (int)HttpStatusCode.BadRequest;
                    await WriteErrorAsync(response, HttpStatusCode.BadRequest, "ERR 'message' field is required and must be a string.", origin, cancellationToken).ConfigureAwait(false);
                    return;
                }

                statusCode = await ExecuteRouteAsync(
                    response,
                    origin,
                    resp3Requested,
                    cancellationToken,
                    "PUBLISH",
                    DecodeSegment(segments[1]),
                    messageElement.GetString() ?? string.Empty).ConfigureAwait(false);
                return;
            }

            if (HttpMethodsMatch(method, "POST") && segments.Length == 2 && segments[0].Equals("xadd", StringComparison.OrdinalIgnoreCase))
            {
                route = "/xadd/{stream}";
                using var body = await ParseBodyAsync(request, cancellationToken).ConfigureAwait(false);
                if (body is null ||
                    !body.RootElement.TryGetProperty("fields", out var fieldsElement) ||
                    fieldsElement.ValueKind != JsonValueKind.Object)
                {
                    statusCode = (int)HttpStatusCode.BadRequest;
                    await WriteErrorAsync(response, HttpStatusCode.BadRequest, "ERR 'fields' object is required.", origin, cancellationToken).ConfigureAwait(false);
                    return;
                }

                var args = new List<string> { DecodeSegment(segments[1]), "*" };
                foreach (var property in fieldsElement.EnumerateObject())
                {
                    if (property.Value.ValueKind != JsonValueKind.String)
                    {
                        statusCode = (int)HttpStatusCode.BadRequest;
                        await WriteErrorAsync(response, HttpStatusCode.BadRequest, "ERR 'fields' values must be strings.", origin, cancellationToken).ConfigureAwait(false);
                        return;
                    }

                    args.Add(property.Name);
                    args.Add(property.Value.GetString() ?? string.Empty);
                }

                if (args.Count <= 2)
                {
                    statusCode = (int)HttpStatusCode.BadRequest;
                    await WriteErrorAsync(response, HttpStatusCode.BadRequest, "ERR 'fields' object must include at least one field.", origin, cancellationToken).ConfigureAwait(false);
                    return;
                }

                statusCode = await ExecuteRouteAsync(response, origin, resp3Requested, cancellationToken, "XADD", args.ToArray()).ConfigureAwait(false);
                return;
            }

            if (HttpMethodsMatch(method, "POST") && segments.Length == 1 && segments[0].Equals("cmd", StringComparison.OrdinalIgnoreCase))
            {
                route = "/cmd";
                using var body = await ParseBodyAsync(request, cancellationToken).ConfigureAwait(false);
                if (body is null)
                {
                    statusCode = (int)HttpStatusCode.BadRequest;
                    await WriteErrorAsync(response, HttpStatusCode.BadRequest, "ERR request body must be valid JSON.", origin, cancellationToken).ConfigureAwait(false);
                    return;
                }

                if (!body.RootElement.TryGetProperty("cmd", out var cmdElement) || cmdElement.ValueKind != JsonValueKind.String || string.IsNullOrWhiteSpace(cmdElement.GetString()))
                {
                    statusCode = (int)HttpStatusCode.BadRequest;
                    await WriteErrorAsync(response, HttpStatusCode.BadRequest, "ERR 'cmd' field is required and must be a string.", origin, cancellationToken).ConfigureAwait(false);
                    return;
                }

                var command = cmdElement.GetString()!;
                var args = new List<string>();
                if (body.RootElement.TryGetProperty("args", out var argsElement))
                {
                    if (argsElement.ValueKind != JsonValueKind.Array)
                    {
                        statusCode = (int)HttpStatusCode.BadRequest;
                        await WriteErrorAsync(response, HttpStatusCode.BadRequest, "ERR 'args' field must be an array of strings.", origin, cancellationToken).ConfigureAwait(false);
                        return;
                    }

                    foreach (var arg in argsElement.EnumerateArray())
                    {
                        if (arg.ValueKind != JsonValueKind.String)
                        {
                            statusCode = (int)HttpStatusCode.BadRequest;
                            await WriteErrorAsync(response, HttpStatusCode.BadRequest, "ERR 'args' field must be an array of strings.", origin, cancellationToken).ConfigureAwait(false);
                            return;
                        }

                        args.Add(arg.GetString() ?? string.Empty);
                    }
                }

                statusCode = await ExecuteRouteAsync(response, origin, resp3Requested, cancellationToken, command, args.ToArray()).ConfigureAwait(false);
                return;
            }

            route = "/not-found";
            statusCode = (int)HttpStatusCode.NotFound;
            await WriteErrorAsync(response, HttpStatusCode.NotFound, "ERR endpoint not found.", origin, cancellationToken).ConfigureAwait(false);
        }
        catch (JsonException)
        {
            statusCode = (int)HttpStatusCode.BadRequest;
            await WriteErrorAsync(response, HttpStatusCode.BadRequest, "ERR malformed JSON request body.", request.Headers["Origin"], cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "HTTP gateway request failed.");
            statusCode = (int)HttpStatusCode.InternalServerError;
            await WriteErrorAsync(response, HttpStatusCode.InternalServerError, "ERR internal server error.", request.Headers["Origin"], cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _metrics.OnHttpRequestCompleted(method, route, statusCode, startedAt.Elapsed);
        }
    }

    private async Task<int> ExecuteRouteAsync(
        HttpListenerResponse response,
        string? origin,
        bool resp3Requested,
        CancellationToken cancellationToken,
        string command,
        params string[] args)
    {
        if (resp3Requested)
        {
            _metrics.OnHttpResp3Request();
        }

        var requestItems = new List<RespValue> { RespValue.BulkString(command) };
        requestItems.AddRange(args.Select(RespValue.BulkString));
        var result = await _processor.ExecuteAsync(RespValue.Array(requestItems), cancellationToken).ConfigureAwait(false);
        if (resp3Requested)
        {
            result = NormalizeResp3JsonResult(command, args, result);
        }
        if (result.Type == RespType.Error)
        {
            await WriteErrorAsync(response, HttpStatusCode.BadRequest, result.Text ?? "ERR command failed.", origin, cancellationToken).ConfigureAwait(false);
            return (int)HttpStatusCode.BadRequest;
        }

        await WriteJsonAsync(
            response,
            HttpStatusCode.OK,
            new { result = ToJsonCompatibleResult(result, resp3Requested ? 3 : 2) },
            origin,
            cancellationToken).ConfigureAwait(false);
        return (int)HttpStatusCode.OK;
    }

    private (bool Success, string? ErrorMessage) Authenticate(HttpListenerRequest request)
    {
        var queryPassword = request.QueryString["auth"];
        if (!string.IsNullOrEmpty(queryPassword))
        {
            return queryPassword == _requiredPassword
                ? (true, null)
                : (false, "WRONGPASS invalid username-password pair or user is disabled.");
        }

        var authorization = request.Headers["Authorization"];
        if (string.IsNullOrWhiteSpace(authorization))
        {
            return (false, "NOAUTH Authentication required");
        }

        const string bearerPrefix = "Bearer ";
        if (!authorization.StartsWith(bearerPrefix, StringComparison.OrdinalIgnoreCase))
        {
            return (false, "NOAUTH Authentication required");
        }

        var token = authorization[bearerPrefix.Length..].Trim();
        if (string.IsNullOrEmpty(token))
        {
            return (false, "NOAUTH Authentication required");
        }

        return token == _requiredPassword
            ? (true, null)
            : (false, "WRONGPASS invalid username-password pair or user is disabled.");
    }

    private bool IsOriginAllowed(string? origin)
    {
        if (_allowAnyOrigin || string.IsNullOrWhiteSpace(origin))
        {
            return true;
        }

        return _allowedOrigins.Contains(origin.Trim());
    }

    private void AddCorsHeaders(HttpListenerResponse response, string? origin)
    {
        response.Headers["Access-Control-Allow-Methods"] = "GET,POST,DELETE,OPTIONS";
        response.Headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization";
        if (_allowAnyOrigin)
        {
            response.Headers["Access-Control-Allow-Origin"] = "*";
            return;
        }

        if (!string.IsNullOrWhiteSpace(origin) && _allowedOrigins.Contains(origin.Trim()))
        {
            response.Headers["Access-Control-Allow-Origin"] = origin.Trim();
            response.Headers["Vary"] = "Origin";
        }
    }

    private static async Task<JsonDocument?> ParseBodyAsync(HttpListenerRequest request, CancellationToken cancellationToken)
    {
        if (!request.HasEntityBody)
        {
            return null;
        }

        using var stream = request.InputStream;
        using var reader = new StreamReader(stream, request.ContentEncoding ?? Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
        var body = await reader.ReadToEndAsync(cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(body))
        {
            return null;
        }

        return JsonDocument.Parse(body);
    }

    private static async Task WriteErrorAsync(
        HttpListenerResponse response,
        HttpStatusCode status,
        string error,
        string? origin,
        CancellationToken cancellationToken)
    {
        await WriteJsonAsync(response, status, new { error }, origin, cancellationToken).ConfigureAwait(false);
    }

    private static async Task WriteJsonAsync(
        HttpListenerResponse response,
        HttpStatusCode status,
        object payload,
        string? origin,
        CancellationToken cancellationToken)
    {
        response.StatusCode = (int)status;
        response.ContentType = "application/json";
        var json = JsonSerializer.Serialize(payload, JsonOptions);
        await WriteBytesAsync(response, Encoding.UTF8.GetBytes(json), cancellationToken).ConfigureAwait(false);
    }

    private static async Task WriteTextAsync(
        HttpListenerResponse response,
        HttpStatusCode status,
        string payload,
        string contentType,
        string? origin,
        CancellationToken cancellationToken)
    {
        response.StatusCode = (int)status;
        response.ContentType = contentType;
        await WriteBytesAsync(response, Encoding.UTF8.GetBytes(payload), cancellationToken).ConfigureAwait(false);
    }

    private static async Task WriteBytesAsync(HttpListenerResponse response, byte[] bytes, CancellationToken cancellationToken)
    {
        response.ContentLength64 = bytes.Length;
        await response.OutputStream.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
        response.Close();
    }

    private static object? ToJsonCompatibleResult(RespValue value, int protocolVersion) =>
        value.Type switch
        {
            RespType.SimpleString => value.Text,
            RespType.Integer => value.Integer,
            RespType.BulkString => value.Bytes is null ? null : Encoding.UTF8.GetString(value.Bytes),
            RespType.Array => value.Items is null ? null : value.Items.Select(item => ToJsonCompatibleResult(item, protocolVersion)).ToArray(),
            RespType.Map => MapToJson(value.Items, protocolVersion),
            RespType.Set => value.Items is null ? null : value.Items.Select(item => ToJsonCompatibleResult(item, protocolVersion)).ToArray(),
            RespType.Double => protocolVersion >= 3 ? value.DoubleValue : value.AsDoubleString(),
            RespType.Boolean => protocolVersion >= 3 ? value.BoolValue : (value.BoolValue ? 1 : 0),
            RespType.BigNumber => value.Text,
            RespType.VerbatimString => value.Text,
            RespType.Null => null,
            RespType.Push => value.Items is null ? null : value.Items.Select(item => ToJsonCompatibleResult(item, protocolVersion)).ToArray(),
            RespType.Error => value.Text,
            _ => null
        };

    private static object? MapToJson(IReadOnlyList<RespValue>? items, int protocolVersion)
    {
        if (items is null)
        {
            return null;
        }

        if (protocolVersion < 3 || items.Count % 2 != 0)
        {
            return items.Select(item => ToJsonCompatibleResult(item, protocolVersion)).ToArray();
        }

        var result = new Dictionary<string, object?>(StringComparer.Ordinal);
        for (var i = 0; i < items.Count; i += 2)
        {
            result[ToJsonKey(items[i])] = ToJsonCompatibleResult(items[i + 1], protocolVersion);
        }

        return result;
    }

    private static string ToJsonKey(RespValue value) =>
        value.Type switch
        {
            RespType.SimpleString => value.Text ?? string.Empty,
            RespType.BulkString => value.Bytes is null ? string.Empty : Encoding.UTF8.GetString(value.Bytes),
            RespType.Integer => value.Integer.ToString(),
            _ => ToJsonCompatibleResult(value, 3)?.ToString() ?? string.Empty
        };

    private static bool WantsResp3Json(string? acceptHeader) =>
        !string.IsNullOrWhiteSpace(acceptHeader) &&
        acceptHeader.Contains("application/json", StringComparison.OrdinalIgnoreCase) &&
        acceptHeader.Contains("resp=3", StringComparison.OrdinalIgnoreCase);

    private static RespValue NormalizeResp3JsonResult(string command, IReadOnlyList<string> args, RespValue result)
    {
        if (command.Equals("EXISTS", StringComparison.OrdinalIgnoreCase) && result.Type == RespType.Integer)
        {
            return RespValue.Boolean(result.Integer != 0);
        }

        if (command.Equals("ZSCORE", StringComparison.OrdinalIgnoreCase) &&
            result.Type == RespType.BulkString &&
            result.Bytes is not null &&
            double.TryParse(Encoding.UTF8.GetString(result.Bytes), NumberStyles.Float, CultureInfo.InvariantCulture, out var score))
        {
            return RespValue.Double(score);
        }

        if ((command.Equals("HGETALL", StringComparison.OrdinalIgnoreCase) ||
             (command.Equals("CONFIG", StringComparison.OrdinalIgnoreCase) &&
              args.Count >= 1 &&
              args[0].Equals("GET", StringComparison.OrdinalIgnoreCase))) &&
            result.Type == RespType.Array &&
            result.Items is { Count: > 0 } items &&
            items.Count % 2 == 0)
        {
            return RespValue.Map(items);
        }

        return result;
    }

    private static string DecodeSegment(string value) => WebUtility.UrlDecode(value);

    private static bool HttpMethodsMatch(string? value, string expected) =>
        string.Equals(value, expected, StringComparison.OrdinalIgnoreCase);

    private static object BuildOpenApiSpec() => new
    {
        openapi = "3.0.3",
        info = new
        {
            title = "SwarmKeyDb HTTP REST Gateway",
            version = "1.0.0"
        },
        xResp3 = new
        {
            accept = "application/json; resp=3",
            variants = new
            {
                map = new { resp2 = new[] { "field", "value" }, resp3 = new { field = "value" } },
                number = new { resp2 = "1.5", resp3 = 1.5 },
                boolean = new { resp2 = 1, resp3 = true }
            }
        },
        paths = new Dictionary<string, object>(StringComparer.Ordinal)
        {
            ["/get/{key}"] = new
            {
                get = new
                {
                    summary = "GET key",
                    parameters = new[] { new { name = "key", @in = "path", required = true, schema = new { type = "string" } } }
                }
            },
            ["/set/{key}"] = new
            {
                post = new
                {
                    summary = "SET key",
                    parameters = new[] { new { name = "key", @in = "path", required = true, schema = new { type = "string" } } }
                }
            },
            ["/del/{key}"] = new { delete = new { summary = "DEL key" } },
            ["/exists/{key}"] = new { get = new { summary = "EXISTS key" } },
            ["/expire/{key}"] = new { post = new { summary = "EXPIRE key" } },
            ["/ttl/{key}"] = new { get = new { summary = "TTL key" } },
            ["/keys/{pattern}"] = new { get = new { summary = "KEYS pattern" } },
            ["/publish/{channel}"] = new { post = new { summary = "PUBLISH channel message" } },
            ["/xadd/{stream}"] = new { post = new { summary = "XADD stream" } },
            ["/cmd"] = new { post = new { summary = "Execute generic Redis command" } }
        }
    };

    private const string SwaggerUiHtml = """
                                         <!doctype html>
                                         <html lang="en">
                                         <head>
                                           <meta charset="utf-8">
                                           <title>SwarmKeyDb HTTP REST API</title>
                                           <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
                                         </head>
                                         <body>
                                           <div id="swagger-ui"></div>
                                           <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
                                           <script>
                                             SwaggerUIBundle({
                                               url: "/openapi.json",
                                               dom_id: "#swagger-ui"
                                             });
                                           </script>
                                         </body>
                                         </html>
                                         """;
}
