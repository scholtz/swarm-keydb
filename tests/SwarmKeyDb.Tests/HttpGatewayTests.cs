using System.Net;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;
using SwarmKeyDb;
using SwarmKeyDb.Server;

namespace SwarmKeyDb.Tests;

[TestFixture]
[Category("Integration")]
public class HttpGatewayTests
{
    [Test]
    public async Task HttpGatewaySetGetDelAndCmdRoundTripAsync()
    {
        var (gateway, metrics, cts, runTask, port) = StartGateway();
        using var client = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{port}") };

        var setResponse = await client.PostAsJsonAsync("/set/hello", new { value = "world" }, cts.Token);
        var setJson = await ReadJsonAsync(setResponse, cts.Token);
        Assert.That(setResponse.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        Assert.That(setJson.RootElement.GetProperty("result").GetString(), Is.EqualTo("OK"));

        var getResponse = await client.GetAsync("/get/hello", cts.Token);
        var getJson = await ReadJsonAsync(getResponse, cts.Token);
        Assert.That(getResponse.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        Assert.That(getJson.RootElement.GetProperty("result").GetString(), Is.EqualTo("world"));

        var delResponse = await client.DeleteAsync("/del/hello", cts.Token);
        var delJson = await ReadJsonAsync(delResponse, cts.Token);
        Assert.That(delResponse.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        Assert.That(delJson.RootElement.GetProperty("result").GetInt32(), Is.EqualTo(1));

        var cmdResponse = await client.PostAsJsonAsync("/cmd", new { cmd = "EXISTS", args = new[] { "hello" } }, cts.Token);
        var cmdJson = await ReadJsonAsync(cmdResponse, cts.Token);
        Assert.That(cmdResponse.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        Assert.That(cmdJson.RootElement.GetProperty("result").GetInt32(), Is.EqualTo(0));

        var metricsPayload = metrics.CollectPrometheus();
        Assert.That(metricsPayload.Contains("swarmkeydb_http_requests_total", StringComparison.Ordinal), Is.True, "Expected HTTP request counter metric.");
        Assert.That(metricsPayload.Contains("swarmkeydb_http_request_duration_seconds", StringComparison.Ordinal), Is.True, "Expected HTTP latency metric.");
        Assert.That(metricsPayload.Contains("swarmkeydb_http_resp3_requests_total", StringComparison.Ordinal), Is.True, "Expected HTTP RESP3 request metric.");

        await StopGatewayAsync(gateway, cts, runTask);
    }

    [Test]
    public async Task HttpGatewayResp3AcceptHeaderReturnsTypedShapesAndKeepsResp2DefaultAsync()
    {
        var (gateway, _, cts, runTask, port) = StartGateway();
        using var client = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{port}") };

        var defaultConfig = await client.PostAsJsonAsync("/cmd", new { cmd = "CONFIG", args = new[] { "GET", "*" } }, cts.Token);
        var defaultConfigJson = await ReadJsonAsync(defaultConfig, cts.Token);
        Assert.That(defaultConfig.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        Assert.That(defaultConfigJson.RootElement.GetProperty("result").ValueKind, Is.EqualTo(JsonValueKind.Array));

        var resp3ConfigRequest = new HttpRequestMessage(HttpMethod.Post, "/cmd")
        {
            Content = JsonContent.Create(new { cmd = "CONFIG", args = new[] { "GET", "*" } })
        };
        resp3ConfigRequest.Headers.Accept.ParseAdd("application/json; resp=3");
        var resp3Config = await client.SendAsync(resp3ConfigRequest, cts.Token);
        var resp3ConfigJson = await ReadJsonAsync(resp3Config, cts.Token);
        Assert.That(resp3Config.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        Assert.That(resp3ConfigJson.RootElement.GetProperty("result").ValueKind, Is.EqualTo(JsonValueKind.Object));

        var resp3ExistsRequest = new HttpRequestMessage(HttpMethod.Get, "/exists/no-such-key");
        resp3ExistsRequest.Headers.Accept.ParseAdd("application/json; resp=3");
        var resp3Exists = await client.SendAsync(resp3ExistsRequest, cts.Token);
        var resp3ExistsJson = await ReadJsonAsync(resp3Exists, cts.Token);
        Assert.That(resp3Exists.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        Assert.That(resp3ExistsJson.RootElement.GetProperty("result").ValueKind, Is.EqualTo(JsonValueKind.False));

        var defaultExists = await client.GetAsync("/exists/no-such-key", cts.Token);
        var defaultExistsJson = await ReadJsonAsync(defaultExists, cts.Token);
        Assert.That(defaultExists.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        Assert.That(defaultExistsJson.RootElement.GetProperty("result").ValueKind, Is.EqualTo(JsonValueKind.Number));

        await StopGatewayAsync(gateway, cts, runTask);
    }

    [Test]
    public async Task HttpGatewayRequiresBearerOrQueryAuthWhenRequirePassSetAsync()
    {
        var (gateway, _, cts, runTask, port) = StartGateway(requirePass: "secret");
        using var client = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{port}") };

        var missingAuth = await client.GetAsync("/get/secure-key", cts.Token);
        var missingAuthJson = await ReadJsonAsync(missingAuth, cts.Token);
        Assert.That(missingAuth.StatusCode, Is.EqualTo(HttpStatusCode.Unauthorized));
        Assert.That(missingAuthJson.RootElement.GetProperty("error").GetString(), Does.Contain("NOAUTH"));

        var wrongAuthRequest = new HttpRequestMessage(HttpMethod.Get, "/get/secure-key");
        wrongAuthRequest.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", "wrong");
        var wrongAuth = await client.SendAsync(wrongAuthRequest, cts.Token);
        var wrongAuthJson = await ReadJsonAsync(wrongAuth, cts.Token);
        Assert.That(wrongAuth.StatusCode, Is.EqualTo(HttpStatusCode.Unauthorized));
        Assert.That(wrongAuthJson.RootElement.GetProperty("error").GetString(), Does.Contain("WRONGPASS"));

        var queryAuth = await client.GetAsync("/get/secure-key?auth=secret", cts.Token);
        var queryAuthJson = await ReadJsonAsync(queryAuth, cts.Token);
        Assert.That(queryAuth.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        Assert.That(queryAuthJson.RootElement.TryGetProperty("result", out _), Is.True, "Expected successful response shape for authenticated request.");

        await StopGatewayAsync(gateway, cts, runTask);
    }

    [Test]
    public async Task HttpGatewayRejectsDisallowedCorsOriginsAsync()
    {
        var (gateway, _, cts, runTask, port) = StartGateway(allowedOrigins: ["https://allowed.example"]);
        using var client = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{port}") };

        var blocked = new HttpRequestMessage(HttpMethod.Get, "/get/key");
        blocked.Headers.Add("Origin", "https://blocked.example");
        var blockedResponse = await client.SendAsync(blocked, cts.Token);
        var blockedJson = await ReadJsonAsync(blockedResponse, cts.Token);
        Assert.That(blockedResponse.StatusCode, Is.EqualTo(HttpStatusCode.Forbidden));
        Assert.That(blockedJson.RootElement.GetProperty("error").GetString(), Does.Contain("CORS"));

        var allowed = new HttpRequestMessage(HttpMethod.Get, "/get/key");
        allowed.Headers.Add("Origin", "https://allowed.example");
        var allowedResponse = await client.SendAsync(allowed, cts.Token);
        Assert.That(allowedResponse.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        Assert.That(allowedResponse.Headers.GetValues("Access-Control-Allow-Origin").Single(), Is.EqualTo("https://allowed.example"));

        await StopGatewayAsync(gateway, cts, runTask);
    }

    [Test]
    public async Task HttpGatewayValidatesJsonBodiesAsync()
    {
        var (gateway, _, cts, runTask, port) = StartGateway();
        using var client = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{port}") };

        var malformed = await client.PostAsync("/set/a", new StringContent("{", Encoding.UTF8, "application/json"), cts.Token);
        var malformedJson = await ReadJsonAsync(malformed, cts.Token);
        Assert.That(malformed.StatusCode, Is.EqualTo(HttpStatusCode.BadRequest));
        Assert.That(malformedJson.RootElement.GetProperty("error").GetString(), Does.Contain("malformed JSON"));

        var missingValue = await client.PostAsJsonAsync("/set/a", new { ex = 10 }, cts.Token);
        var missingValueJson = await ReadJsonAsync(missingValue, cts.Token);
        Assert.That(missingValue.StatusCode, Is.EqualTo(HttpStatusCode.BadRequest));
        Assert.That(missingValueJson.RootElement.GetProperty("error").GetString(), Does.Contain("'value'"));

        var extraFields = await client.PostAsJsonAsync("/set/a", new { value = "ok", ex = 5, ignored = "field" }, cts.Token);
        var extraFieldsJson = await ReadJsonAsync(extraFields, cts.Token);
        Assert.That(extraFields.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        Assert.That(extraFieldsJson.RootElement.GetProperty("result").GetString(), Is.EqualTo("OK"));

        await StopGatewayAsync(gateway, cts, runTask);
    }

    [Test]
    public async Task HttpGatewayCmdAndXAddEndpointsSupportStreamWritesAsync()
    {
        var (gateway, _, cts, runTask, port) = StartGateway();
        using var client = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{port}") };

        var xaddResponse = await client.PostAsJsonAsync("/xadd/events", new { fields = new { field = "value" } }, cts.Token);
        var xaddJson = await ReadJsonAsync(xaddResponse, cts.Token);
        Assert.That(xaddResponse.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        Assert.That(xaddJson.RootElement.GetProperty("result").GetString(), Does.Contain("-"));

        var cmdResponse = await client.PostAsJsonAsync("/cmd", new { cmd = "XRANGE", args = new[] { "events", "-", "+" } }, cts.Token);
        var cmdJson = await ReadJsonAsync(cmdResponse, cts.Token);
        Assert.That(cmdResponse.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        Assert.That(cmdJson.RootElement.GetProperty("result").ValueKind, Is.EqualTo(JsonValueKind.Array));

        var unknownCmd = await client.PostAsJsonAsync("/cmd", new { cmd = "NO_SUCH_COMMAND" }, cts.Token);
        var unknownCmdJson = await ReadJsonAsync(unknownCmd, cts.Token);
        Assert.That(unknownCmd.StatusCode, Is.EqualTo(HttpStatusCode.BadRequest));
        Assert.That(unknownCmdJson.RootElement.GetProperty("error").GetString(), Does.Contain("ERR"));

        await StopGatewayAsync(gateway, cts, runTask);
    }

    [Test]
    public async Task HttpGatewayServesOpenApiAndSwaggerUiAsync()
    {
        var (gateway, _, cts, runTask, port) = StartGateway();
        using var client = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{port}") };

        var openApi = await client.GetAsync("/openapi.json", cts.Token);
        var openApiJson = await ReadJsonAsync(openApi, cts.Token);
        Assert.That(openApi.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        Assert.That(openApiJson.RootElement.GetProperty("openapi").GetString(), Is.EqualTo("3.0.3"));
        Assert.That(openApiJson.RootElement.GetProperty("paths").TryGetProperty("/get/{key}", out _), Is.True);

        var docs = await client.GetStringAsync("/docs", cts.Token);
        Assert.That(docs.Contains("SwaggerUIBundle", StringComparison.Ordinal), Is.True, "Expected Swagger UI payload.");

        await StopGatewayAsync(gateway, cts, runTask);
    }

    private static (HttpGateway Gateway, MonitoringMetrics Metrics, CancellationTokenSource Cts, Task RunTask, int Port) StartGateway(
        string? requirePass = null,
        IReadOnlyList<string>? allowedOrigins = null)
    {
        var metrics = new MonitoringMetrics(() => NoOpCacheStats.Instance);
        var processor = TestHelpers.CreatePubSubProcessor();
        var port = GetFreePort();
        var gateway = new HttpGateway(
            IPAddress.Loopback,
            port,
            processor,
            metrics,
            requiredPassword: requirePass,
            allowedOrigins: allowedOrigins ?? ["*"],
            logger: NullLogger<HttpGateway>.Instance);
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var runTask = gateway.RunAsync(cts.Token);
        Task.Delay(80, cts.Token).GetAwaiter().GetResult();
        return (gateway, metrics, cts, runTask, port);
    }

    private static async Task StopGatewayAsync(HttpGateway gateway, CancellationTokenSource cts, Task runTask)
    {
        cts.Cancel();
        try
        {
            await runTask;
        }
        catch (OperationCanceledException)
        {
        }
        catch (HttpListenerException)
        {
        }
        finally
        {
            gateway.Dispose();
            cts.Dispose();
        }
    }

    private static async Task<JsonDocument> ReadJsonAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        var payload = await response.Content.ReadAsStringAsync(cancellationToken);
        return JsonDocument.Parse(payload);
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }
}
