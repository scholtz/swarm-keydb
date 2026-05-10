using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;
using SwarmKeyDb;
using SwarmKeyDb.Server;
using static SwarmKeyDb.Tests.TestHelpers;

namespace SwarmKeyDb.Tests;

[TestFixture]
[Category("Integration")]
public class WebSocketGatewayTests
{
    [Test]
    public async Task WebSocketGatewayJsonPubSubSubscribePublishAsync()
    {
        var (gateway, _, _, cts, runTask, port) = StartGateway();
        using var subscriber = new ClientWebSocket();
        using var publisher = new ClientWebSocket();
        await subscriber.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/"), cts.Token);
        await publisher.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/"), cts.Token);

        await SendTextAsync(subscriber, """{"cmd":"SUBSCRIBE","args":["ws:news"]}""", cts.Token);
        var subscribeAck = await ReceiveTextAsync(subscriber, cts.Token);
        Assert(subscribeAck.Contains("subscribe", StringComparison.Ordinal), $"Expected subscribe acknowledgement. Payload: {subscribeAck}");

        await SendTextAsync(publisher, """{"cmd":"PUBLISH","args":["ws:news","hello-ws"]}""", cts.Token);
        _ = await ReceiveTextAsync(publisher, cts.Token);
        var push = await ReceiveTextAsync(subscriber, cts.Token);
        Assert(push.Contains("hello-ws", StringComparison.Ordinal), $"Expected pushed pub/sub payload. Payload: {push}");

        await StopGatewayAsync(gateway, cts, runTask);
    }

    [Test]
    public async Task WebSocketGatewaySupportsXReadBlockAsync()
    {
        var (gateway, _, _, cts, runTask, port) = StartGateway();
        using var reader = new ClientWebSocket();
        using var writer = new ClientWebSocket();
        await reader.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/"), cts.Token);
        await writer.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/"), cts.Token);

        await SendTextAsync(reader, """{"cmd":"XREAD","args":["BLOCK","0","STREAMS","ws:stream","$"]}""", cts.Token);
        await Task.Delay(120, cts.Token);
        await SendTextAsync(writer, """{"cmd":"XADD","args":["ws:stream","1-0","f","v1"]}""", cts.Token);
        _ = await ReceiveTextAsync(writer, cts.Token);

        var readPayload = await ReceiveTextAsync(reader, cts.Token);
        Assert(readPayload.Contains("ws:stream", StringComparison.Ordinal), $"XREAD response should include stream key. Payload: {readPayload}");
        Assert(readPayload.Contains("v1", StringComparison.Ordinal), $"XREAD response should include appended value. Payload: {readPayload}");

        await StopGatewayAsync(gateway, cts, runTask);
    }

    [Test]
    public async Task WebSocketGatewayEnforcesAuthWhenRequirePassConfiguredAsync()
    {
        var (gateway, _, _, cts, runTask, port) = StartGateway(requirePass: "secret");
        using var client = new ClientWebSocket();
        await client.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/"), cts.Token);

        await SendTextAsync(client, """{"cmd":"GET","args":["k"]}""", cts.Token);
        var noAuth = await ReceiveTextAsync(client, cts.Token);
        Assert(noAuth.Contains("NOAUTH", StringComparison.Ordinal), $"Expected NOAUTH error. Payload: {noAuth}");

        await SendTextAsync(client, """{"cmd":"AUTH","args":["bad"]}""", cts.Token);
        var wrongPass = await ReceiveTextAsync(client, cts.Token);
        Assert(wrongPass.Contains("WRONGPASS", StringComparison.Ordinal), $"Expected WRONGPASS error. Payload: {wrongPass}");

        await SendTextAsync(client, """{"cmd":"AUTH","args":["secret"]}""", cts.Token);
        var okAuth = await ReceiveTextAsync(client, cts.Token);
        Assert(okAuth.Contains("OK", StringComparison.Ordinal), $"Expected AUTH success payload. Payload: {okAuth}");

        await SendTextAsync(client, """{"cmd":"PING"}""", cts.Token);
        var ping = await ReceiveTextAsync(client, cts.Token);
        Assert(ping.Contains("PONG", StringComparison.Ordinal), $"Expected authenticated command to succeed. Payload: {ping}");

        await StopGatewayAsync(gateway, cts, runTask);
    }

    [Test]
    public async Task WebSocketGatewayRejectsDisallowedOriginsAsync()
    {
        var (gateway, _, _, cts, runTask, port) = StartGateway(allowedOrigins: ["https://allowed.example"]);
        using var disallowed = new ClientWebSocket();
        disallowed.Options.SetRequestHeader("Origin", "https://blocked.example");
        try
        {
            await disallowed.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/"), cts.Token);
            throw new InvalidOperationException("Expected disallowed origin handshake to fail.");
        }
        catch (WebSocketException)
        {
        }

        using var allowed = new ClientWebSocket();
        allowed.Options.SetRequestHeader("Origin", "https://allowed.example");
        await allowed.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/"), cts.Token);
        await SendTextAsync(allowed, """{"cmd":"PING"}""", cts.Token);
        var pong = await ReceiveTextAsync(allowed, cts.Token);
        Assert(pong.Contains("PONG", StringComparison.Ordinal), $"Expected allowed origin to connect. Payload: {pong}");

        await StopGatewayAsync(gateway, cts, runTask);
    }

    [Test]
    public async Task WebSocketGatewayRespModeReturnsProtocolErrorsForMalformedFramesAsync()
    {
        var (gateway, _, _, cts, runTask, port) = StartGateway();
        using var client = new ClientWebSocket();
        await client.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/"), cts.Token);

        await SendTextAsync(client, "*2\r\n$3\r\nGET\r\n$3\r", cts.Token);
        var error = await ReceiveTextAsync(client, cts.Token);
        Assert(error.Contains("ERR Protocol error: invalid RESP frame", StringComparison.Ordinal), $"Expected RESP protocol error response. Payload: {error}");

        await StopGatewayAsync(gateway, cts, runTask);
    }

    [Test]
    public async Task WebSocketGatewayPrometheusCountersAreExposedAsync()
    {
        var (gateway, metrics, _, cts, runTask, port) = StartGateway();
        using var client = new ClientWebSocket();
        await client.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/"), cts.Token);
        await SendTextAsync(client, """{"cmd":"PING"}""", cts.Token);
        _ = await ReceiveTextAsync(client, cts.Token);
        await client.CloseAsync(WebSocketCloseStatus.NormalClosure, "done", cts.Token);
        await Task.Delay(100, cts.Token);

        var payload = metrics.CollectPrometheus();
        Assert(payload.Contains("swarmkeydb_ws_connections_total{privacy_mode=\"none\"}", StringComparison.Ordinal), "WS connections metric should be exposed.");
        Assert(payload.Contains("swarmkeydb_ws_messages_received_total{privacy_mode=\"none\"}", StringComparison.Ordinal), "WS received metric should be exposed.");
        Assert(payload.Contains("swarmkeydb_ws_messages_sent_total{privacy_mode=\"none\"}", StringComparison.Ordinal), "WS sent metric should be exposed.");
        Assert(payload.Contains("swarmkeydb_ws_errors_total{privacy_mode=\"none\"}", StringComparison.Ordinal), "WS error metric should be exposed.");
        Assert(payload.Contains("swarmkeydb_ws_resp3_connections_total{privacy_mode=\"none\"}", StringComparison.Ordinal), "WS RESP3 metric should be exposed.");
        Assert(payload.Contains("swarmkeydb_ws_client_tracking_connections{privacy_mode=\"none\"}", StringComparison.Ordinal), "WS tracking metric should be exposed.");

        await StopGatewayAsync(gateway, cts, runTask);
    }

    [Test]
    public async Task WebSocketGatewayJsonArrayHello3NegotiatesResp3AndReturnsMapAsync()
    {
        var (gateway, metrics, _, cts, runTask, port) = StartGateway();
        using var client = new ClientWebSocket();
        await client.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/"), cts.Token);

        await SendTextAsync(client, """["HELLO","3"]""", cts.Token);
        var helloPayload = await ReceiveTextAsync(client, cts.Token);
        using var helloJson = JsonDocument.Parse(helloPayload);
        Assert(helloJson.RootElement.ValueKind == JsonValueKind.Object, $"Expected HELLO 3 map payload. Payload: {helloPayload}");
        AssertEqual(3, helloJson.RootElement.GetProperty("proto").GetInt32());

        var metricsPayload = metrics.CollectPrometheus();
        Assert(metricsPayload.Contains("swarmkeydb_ws_resp3_connections_total", StringComparison.Ordinal), "Expected ws RESP3 metric.");

        await StopGatewayAsync(gateway, cts, runTask);
    }

    [Test]
    public async Task WebSocketGatewayClientTrackingPushAndResetRoundTripAsync()
    {
        var (gateway, metrics, _, cts, runTask, port) = StartGateway();
        using var tracked = new ClientWebSocket();
        using var writer = new ClientWebSocket();
        await tracked.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/"), cts.Token);
        await writer.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/"), cts.Token);

        await SendTextAsync(tracked, """["HELLO","3"]""", cts.Token);
        _ = await ReceiveTextAsync(tracked, cts.Token);
        await SendTextAsync(tracked, """["CLIENT","TRACKING","ON"]""", cts.Token);
        _ = await ReceiveTextAsync(tracked, cts.Token);
        await SendTextAsync(tracked, """["GET","tracked:key"]""", cts.Token);
        _ = await ReceiveTextAsync(tracked, cts.Token);

        await SendTextAsync(writer, """{"cmd":"SET","args":["tracked:key","v1"]}""", cts.Token);
        _ = await ReceiveTextAsync(writer, cts.Token);
        var push = await ReceiveTextAsync(tracked, cts.Token);
        using var pushJson = JsonDocument.Parse(push);
        AssertEqual("push", pushJson.RootElement.GetProperty("type").GetString());
        AssertEqual("invalidate", pushJson.RootElement.GetProperty("data")[0].GetString());

        await SendTextAsync(tracked, """["RESET"]""", cts.Token);
        var reset = await ReceiveTextAsync(tracked, cts.Token);
        AssertEqual("\"RESET\"", reset);

        var metricsPayload = metrics.CollectPrometheus();
        Assert(metricsPayload.Contains("swarmkeydb_ws_client_tracking_connections", StringComparison.Ordinal), "Expected ws tracking metric.");

        await StopGatewayAsync(gateway, cts, runTask);
    }

    private static (WebSocketGateway Gateway, MonitoringMetrics Metrics, RedisCommandProcessor Processor, CancellationTokenSource Cts, Task RunTask, int Port) StartGateway(
        string? requirePass = null,
        IReadOnlyList<string>? allowedOrigins = null)
    {
        var manager = new PubSubManager();
        var metrics = new MonitoringMetrics(() => NoOpCacheStats.Instance);
        var processor = CreatePubSubProcessor(manager);
        var port = GetFreePort();
        var gateway = new WebSocketGateway(
            IPAddress.Loopback,
            port,
            processor,
            metrics,
            manager,
            requiredPassword: requirePass,
            allowedOrigins: allowedOrigins ?? ["*"],
            logger: NullLogger<WebSocketGateway>.Instance);
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var runTask = gateway.RunAsync(cts.Token);
        Task.Delay(80, cts.Token).GetAwaiter().GetResult();
        return (gateway, metrics, processor, cts, runTask, port);
    }

    private static async Task StopGatewayAsync(WebSocketGateway gateway, CancellationTokenSource cts, Task runTask)
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

    private static async Task SendTextAsync(ClientWebSocket socket, string text, CancellationToken cancellationToken)
    {
        var bytes = Encoding.UTF8.GetBytes(text);
        await socket.SendAsync(bytes, WebSocketMessageType.Text, endOfMessage: true, cancellationToken);
    }

    private static async Task<string> ReceiveTextAsync(ClientWebSocket socket, CancellationToken cancellationToken)
    {
        var buffer = new byte[4096];
        using var stream = new MemoryStream();
        while (true)
        {
            var result = await socket.ReceiveAsync(buffer, cancellationToken);
            if (result.MessageType == WebSocketMessageType.Close)
            {
                return string.Empty;
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
