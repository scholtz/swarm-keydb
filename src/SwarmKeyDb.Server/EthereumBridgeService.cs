using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using SwarmKeyDb;

namespace SwarmKeyDb.Server;

/// <summary>Current operational status of the Ethereum bridge.</summary>
public enum EthereumBridgeStatus
{
    Disabled,
    Connecting,
    Connected,
    Retrying,
    Error
}

/// <summary>
/// Snapshot of the Ethereum bridge state, exposed via the <c>/ethereum/bridge</c> endpoint.
/// </summary>
public sealed class EthereumBridgeState
{
    public EthereumBridgeStatus Status { get; set; } = EthereumBridgeStatus.Disabled;
    public long? LastProcessedBlock { get; set; }
    public long EventCount { get; set; }
    public DateTimeOffset? ConnectedSince { get; set; }
    public string? LastError { get; set; }
    public string? ContractAddress { get; set; }
    public string? RpcUrl { get; set; }
}

/// <summary>
/// Background service that bridges Ethereum smart contract events to SwarmKeyDb operations.
///
/// Supported modes:
/// - WebSocket (ws:// or wss://): uses eth_subscribe for real-time event streaming.
/// - HTTP (http:// or https://): polls eth_getLogs on a configurable interval.
///
/// Handled events (from ISwarmKeyDb.sol):
/// - DataWriteRequested(address indexed user, string key, bytes value) →
///       writes (key, value) to the SwarmKeyDb store.
/// - DataReadRequested(address indexed user, string key) →
///       logs the read request (oracle response pattern; extend as needed).
/// </summary>
public sealed class EthereumBridgeService : IAsyncDisposable
{
    // keccak256("DataWriteRequested(address,string,bytes)")
    // keccak256("DataReadRequested(address,string)")
    // These are computed lazily on first use via ComputeTopics().
    private static string? _writeRequestedTopic;
    private static string? _readRequestedTopic;

    private readonly IKeyValueStore _store;
    private readonly EthereumBridgeOptions _options;
    private readonly ILogger _logger;
    private readonly EthereumBridgeState _state = new();
    private long _eventCount;
    private CancellationTokenSource? _cts;
    private Task? _runTask;

    public EthereumBridgeService(
        IKeyValueStore store,
        EthereumBridgeOptions options,
        ILogger<EthereumBridgeService> logger)
    {
        _store = store;
        _options = options;
        _logger = logger;
        _state.Status = options.Enabled ? EthereumBridgeStatus.Connecting : EthereumBridgeStatus.Disabled;
        _state.ContractAddress = options.ContractAddress;
        _state.RpcUrl = options.RpcUrl;
    }

    /// <summary>Gets a snapshot of the current bridge state for the monitoring endpoint.</summary>
    public EthereumBridgeState GetState() => new()
    {
        Status = _state.Status,
        LastProcessedBlock = _state.LastProcessedBlock,
        EventCount = Interlocked.Read(ref _eventCount),
        ConnectedSince = _state.ConnectedSince,
        LastError = _state.LastError,
        ContractAddress = _state.ContractAddress,
        RpcUrl = _state.RpcUrl
    };

    /// <summary>Starts the bridge. Returns immediately; the bridge runs in the background.</summary>
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (!_options.Enabled)
        {
            return Task.CompletedTask;
        }

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _runTask = RunAsync(_cts.Token);
        return Task.CompletedTask;
    }

    /// <summary>Awaits the internal run loop to finish (after cancellation).</summary>
    public async Task WaitForShutdownAsync()
    {
        if (_runTask is not null)
        {
            await _runTask.ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_cts is not null)
        {
            await _cts.CancelAsync().ConfigureAwait(false);
            _cts.Dispose();
        }

        if (_runTask is not null)
        {
            try
            {
                await _runTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // expected on shutdown
            }
        }
    }

    private async Task RunAsync(CancellationToken ct)
    {
        ComputeTopics();

        var rpcUrl = _options.RpcUrl;
        if (string.IsNullOrWhiteSpace(rpcUrl))
        {
            _state.Status = EthereumBridgeStatus.Error;
            _state.LastError = "ETH_RPC_URL is not configured.";
            _logger.LogError("Ethereum bridge is enabled but ETH_RPC_URL is not set. Bridge will not start.");
            return;
        }

        if (string.IsNullOrWhiteSpace(_options.ContractAddress))
        {
            _state.Status = EthereumBridgeStatus.Error;
            _state.LastError = "ETH_CONTRACT_ADDRESS is not configured.";
            _logger.LogError("Ethereum bridge is enabled but ETH_CONTRACT_ADDRESS is not set. Bridge will not start.");
            return;
        }

        var isWebSocket = rpcUrl.StartsWith("ws://", StringComparison.OrdinalIgnoreCase)
                       || rpcUrl.StartsWith("wss://", StringComparison.OrdinalIgnoreCase);

        _logger.LogInformation(
            "Ethereum bridge starting. Mode={Mode} Contract={Contract}",
            isWebSocket ? "WebSocket" : "HTTP-polling",
            _options.ContractAddress);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                _state.Status = EthereumBridgeStatus.Connecting;
                _state.LastError = null;

                if (isWebSocket)
                {
                    await RunWebSocketLoopAsync(rpcUrl, ct).ConfigureAwait(false);
                }
                else
                {
                    await RunHttpPollingLoopAsync(rpcUrl, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                var delay = TimeSpan.FromSeconds(Math.Max(1, _options.ReconnectDelaySeconds));
                _state.Status = EthereumBridgeStatus.Retrying;
                _state.LastError = ex.Message;
                _state.ConnectedSince = null;
                _logger.LogWarning(ex,
                    "Ethereum bridge disconnected. Reconnecting in {Delay}s.", delay.TotalSeconds);

                await Task.Delay(delay, ct).ConfigureAwait(false);
            }
        }

        _state.Status = EthereumBridgeStatus.Disabled;
        _logger.LogInformation("Ethereum bridge stopped.");
    }

    // ── WebSocket mode (eth_subscribe) ────────────────────────────────────────

    private async Task RunWebSocketLoopAsync(string rpcUrl, CancellationToken ct)
    {
        using var ws = new ClientWebSocket();
        await ws.ConnectAsync(new Uri(rpcUrl), ct).ConfigureAwait(false);

        _logger.LogInformation("Ethereum bridge WebSocket connected to {Url}.", rpcUrl);

        // Send eth_subscribe request for logs matching our contract address
        var subscribeRequest = BuildSubscribeRequest();
        var requestBytes = Encoding.UTF8.GetBytes(subscribeRequest);
        await ws.SendAsync(requestBytes, WebSocketMessageType.Text, endOfMessage: true, ct)
            .ConfigureAwait(false);

        var subscriptionId = await ReadSubscriptionIdAsync(ws, ct).ConfigureAwait(false);
        if (string.IsNullOrEmpty(subscriptionId))
        {
            throw new InvalidOperationException("Failed to obtain WebSocket subscription ID from Ethereum node.");
        }

        _state.Status = EthereumBridgeStatus.Connected;
        _state.ConnectedSince = DateTimeOffset.UtcNow;
        _logger.LogInformation("Ethereum bridge subscribed. SubscriptionId={Id}", subscriptionId);

        var buffer = new byte[65536];
        var messageBuilder = new StringBuilder();

        while (!ct.IsCancellationRequested)
        {
            messageBuilder.Clear();
            WebSocketReceiveResult result;
            do
            {
                result = await ws.ReceiveAsync(buffer, ct).ConfigureAwait(false);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    throw new InvalidOperationException("WebSocket connection closed by remote.");
                }

                messageBuilder.Append(Encoding.UTF8.GetString(buffer, 0, result.Count));
            }
            while (!result.EndOfMessage);

            var json = messageBuilder.ToString();
            ProcessWebSocketMessage(json);
        }
    }

    private string BuildSubscribeRequest()
    {
        var topics = new List<string?>();

        // Include both event topics as a filter using the Ethereum OR syntax.
        // We rely on the two separate entries so either write OR read events match.
        topics.Add(_writeRequestedTopic);

        return JsonSerializer.Serialize(new
        {
            jsonrpc = "2.0",
            method = "eth_subscribe",
            @params = new object[]
            {
                "logs",
                new
                {
                    address = _options.ContractAddress,
                    // topics[0] filter: accept either DataWriteRequested or DataReadRequested
                    topics = new object[]
                    {
                        new string?[] { _writeRequestedTopic, _readRequestedTopic }
                    }
                }
            },
            id = 1
        });
    }

    private static async Task<string?> ReadSubscriptionIdAsync(ClientWebSocket ws, CancellationToken ct)
    {
        var buffer = new byte[4096];
        var result = await ws.ReceiveAsync(buffer, ct).ConfigureAwait(false);
        var json = Encoding.UTF8.GetString(buffer, 0, result.Count);
        using var doc = JsonDocument.Parse(json);
        if (doc.RootElement.TryGetProperty("result", out var resultEl))
        {
            return resultEl.GetString();
        }

        return null;
    }

    private void ProcessWebSocketMessage(string json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            // eth_subscription notifications have method "eth_subscription"
            if (!root.TryGetProperty("method", out var methodEl)
                || methodEl.GetString() != "eth_subscription")
            {
                return;
            }

            if (!root.TryGetProperty("params", out var paramsEl)
                || !paramsEl.TryGetProperty("result", out var logEl))
            {
                return;
            }

            HandleLog(logEl);
        }
        catch (JsonException ex)
        {
            _logger.LogDebug(ex, "Ethereum bridge: malformed WebSocket message skipped.");
        }
    }

    // ── HTTP polling mode (eth_getLogs) ───────────────────────────────────────

    private async Task RunHttpPollingLoopAsync(string rpcUrl, CancellationToken ct)
    {
        using var http = new HttpClient();
        var pollInterval = TimeSpan.FromSeconds(Math.Max(1, _options.PollIntervalSeconds));

        _state.Status = EthereumBridgeStatus.Connected;
        _state.ConnectedSince = DateTimeOffset.UtcNow;
        _logger.LogInformation("Ethereum bridge HTTP polling started. Interval={Interval}s", pollInterval.TotalSeconds);

        while (!ct.IsCancellationRequested)
        {
            await Task.Delay(pollInterval, ct).ConfigureAwait(false);

            // Get the latest block number
            long latestBlock;
            try
            {
                latestBlock = await GetBlockNumberAsync(http, rpcUrl, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Ethereum bridge: failed to fetch latest block number.");
                _state.Status = EthereumBridgeStatus.Retrying;
                throw;
            }

            var fromBlock = _state.LastProcessedBlock.HasValue
                ? _state.LastProcessedBlock.Value + 1
                : latestBlock;

            if (fromBlock > latestBlock)
            {
                continue;
            }

            _state.Status = EthereumBridgeStatus.Connected;

            var logs = await GetLogsAsync(http, rpcUrl, fromBlock, latestBlock, ct).ConfigureAwait(false);
            foreach (var log in logs)
            {
                HandleLog(log);
            }

            _state.LastProcessedBlock = latestBlock;
        }
    }

    private async Task<long> GetBlockNumberAsync(HttpClient http, string rpcUrl, CancellationToken ct)
    {
        var request = new { jsonrpc = "2.0", method = "eth_blockNumber", @params = Array.Empty<object>(), id = 1 };
        var response = await PostRpcAsync(http, rpcUrl, request, ct).ConfigureAwait(false);
        using var doc = JsonDocument.Parse(response);
        var hexBlock = doc.RootElement.GetProperty("result").GetString()
            ?? throw new InvalidOperationException("eth_blockNumber returned null result.");
        return Convert.ToInt64(hexBlock, 16);
    }

    private async Task<IEnumerable<JsonElement>> GetLogsAsync(
        HttpClient http,
        string rpcUrl,
        long fromBlock,
        long toBlock,
        CancellationToken ct)
    {
        var request = new
        {
            jsonrpc = "2.0",
            method = "eth_getLogs",
            @params = new object[]
            {
                new
                {
                    address = _options.ContractAddress,
                    fromBlock = "0x" + fromBlock.ToString("X"),
                    toBlock = "0x" + toBlock.ToString("X"),
                    topics = new object[]
                    {
                        new string?[] { _writeRequestedTopic, _readRequestedTopic }
                    }
                }
            },
            id = 2
        };

        var response = await PostRpcAsync(http, rpcUrl, request, ct).ConfigureAwait(false);
        using var doc = JsonDocument.Parse(response);
        var result = doc.RootElement.GetProperty("result");
        return result.EnumerateArray().Select(e => e.Clone()).ToList();
    }

    private static async Task<string> PostRpcAsync(HttpClient http, string rpcUrl, object request, CancellationToken ct)
    {
        var json = JsonSerializer.Serialize(request);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        using var response = await http.PostAsync(rpcUrl, content, ct).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
    }

    // ── Event handling ────────────────────────────────────────────────────────

    private void HandleLog(JsonElement logEl)
    {
        try
        {
            if (!logEl.TryGetProperty("topics", out var topicsEl))
            {
                return;
            }

            var topics = topicsEl.EnumerateArray().Select(t => t.GetString()).ToArray();
            if (topics.Length == 0 || topics[0] is null)
            {
                return;
            }

            var selector = topics[0]!;

            if (string.Equals(selector, _writeRequestedTopic, StringComparison.OrdinalIgnoreCase))
            {
                HandleDataWriteRequested(logEl, topics);
            }
            else if (string.Equals(selector, _readRequestedTopic, StringComparison.OrdinalIgnoreCase))
            {
                HandleDataReadRequested(logEl, topics);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Ethereum bridge: failed to process log entry. Skipping.");
        }
    }

    private void HandleDataWriteRequested(JsonElement logEl, string?[] topics)
    {
        // topics[1] = indexed user address (32 bytes, left-padded)
        var userAddress = topics.Length > 1 && topics[1] is not null
            ? "0x" + topics[1]![26..] // rightmost 20 bytes = 40 hex chars
            : "unknown";

        // data = ABI-encoded (string key, bytes value)
        if (!logEl.TryGetProperty("data", out var dataEl))
        {
            return;
        }

        var hexData = dataEl.GetString();
        if (string.IsNullOrEmpty(hexData) || hexData == "0x")
        {
            return;
        }

        var (key, value) = DecodeStringBytesAbi(hexData);
        if (string.IsNullOrEmpty(key))
        {
            _logger.LogWarning("Ethereum bridge: DataWriteRequested event has empty key. Skipping.");
            return;
        }

        // Update block tracking from log blockNumber
        if (logEl.TryGetProperty("blockNumber", out var blockEl) && blockEl.GetString() is { } blockHex)
        {
            _state.LastProcessedBlock = Convert.ToInt64(blockHex, 16);
        }

        _logger.LogInformation(
            "Ethereum bridge: DataWriteRequested key={Key} valueLen={Len} user={User}",
            key, value.Length, userAddress);

        // Fire-and-forget write into SwarmKeyDb (avoid blocking the receive loop)
        _ = Task.Run(async () =>
        {
            try
            {
                await _store.PutAsync(key, value).ConfigureAwait(false);
                Interlocked.Increment(ref _eventCount);
                _logger.LogDebug("Ethereum bridge: wrote key={Key}", key);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Ethereum bridge: failed to write key={Key}", key);
            }
        });
    }

    private void HandleDataReadRequested(JsonElement logEl, string?[] topics)
    {
        var userAddress = topics.Length > 1 && topics[1] is not null
            ? "0x" + topics[1]![26..]
            : "unknown";

        if (!logEl.TryGetProperty("data", out var dataEl))
        {
            return;
        }

        var hexData = dataEl.GetString();
        if (string.IsNullOrEmpty(hexData) || hexData == "0x")
        {
            return;
        }

        // data = ABI-encoded (string key)
        var key = DecodeStringAbi(hexData);
        if (string.IsNullOrEmpty(key))
        {
            return;
        }

        if (logEl.TryGetProperty("blockNumber", out var blockEl) && blockEl.GetString() is { } blockHex)
        {
            _state.LastProcessedBlock = Convert.ToInt64(blockHex, 16);
        }

        Interlocked.Increment(ref _eventCount);
        _logger.LogInformation(
            "Ethereum bridge: DataReadRequested key={Key} user={User}",
            key, userAddress);

        // The oracle pattern: off-chain agent reads the value and posts it back on-chain.
        // Extend this method to call a write-back transaction if _options.PrivateKeyHex is set.
        _ = Task.Run(async () =>
        {
            try
            {
                var value = await _store.GetAsync(key).ConfigureAwait(false);
                _logger.LogDebug(
                    "Ethereum bridge: DataReadRequested resolved key={Key} found={Found}",
                    key, value is not null);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Ethereum bridge: failed to resolve DataReadRequested key={Key}", key);
            }
        });
    }

    // ── ABI helpers ───────────────────────────────────────────────────────────

    /// <summary>
    /// Decodes ABI-encoded <c>(string key, bytes value)</c> from a hex data field.
    /// </summary>
    public static (string key, byte[] value) DecodeStringBytesAbi(string hexData)
    {
        var data = FromHex(hexData);

        // Word 0: offset to key (from start of data)
        // Word 1: offset to value (from start of data)
        var keyOffset = (int)ReadBigEndianUInt256Lower64(data, 0);
        var valueOffset = (int)ReadBigEndianUInt256Lower64(data, 32);

        // At keyOffset: length of key, then key bytes
        var keyLength = (int)ReadBigEndianUInt256Lower64(data, keyOffset);
        var keyBytes = data.AsSpan(keyOffset + 32, keyLength).ToArray();
        var key = Encoding.UTF8.GetString(keyBytes);

        // At valueOffset: length of value, then value bytes
        var valueLength = (int)ReadBigEndianUInt256Lower64(data, valueOffset);
        var value = data.AsSpan(valueOffset + 32, valueLength).ToArray();

        return (key, value);
    }

    /// <summary>
    /// Decodes ABI-encoded <c>(string key)</c> from a hex data field.
    /// </summary>
    public static string DecodeStringAbi(string hexData)
    {
        var data = FromHex(hexData);

        // Word 0: offset to key (should be 0x20 = 32)
        var keyOffset = (int)ReadBigEndianUInt256Lower64(data, 0);
        var keyLength = (int)ReadBigEndianUInt256Lower64(data, keyOffset);
        var keyBytes = data.AsSpan(keyOffset + 32, keyLength).ToArray();
        return Encoding.UTF8.GetString(keyBytes);
    }

    /// <summary>
    /// Reads the lower 8 bytes of a big-endian 256-bit integer (sufficient for lengths and offsets
    /// we'll ever encounter in practice).
    /// </summary>
    public static ulong ReadBigEndianUInt256Lower64(byte[] data, int offset)
    {
        // The 256-bit (32-byte) big-endian integer; return last 8 bytes as ulong.
        return ((ulong)data[offset + 24] << 56)
             | ((ulong)data[offset + 25] << 48)
             | ((ulong)data[offset + 26] << 40)
             | ((ulong)data[offset + 27] << 32)
             | ((ulong)data[offset + 28] << 24)
             | ((ulong)data[offset + 29] << 16)
             | ((ulong)data[offset + 30] <<  8)
             | ((ulong)data[offset + 31]);
    }

    private static byte[] FromHex(string hex)
    {
        var s = hex.StartsWith("0x", StringComparison.OrdinalIgnoreCase) ? hex[2..] : hex;
        if (s.Length % 2 != 0)
        {
            s = "0" + s;
        }

        return Convert.FromHexString(s);
    }

    // ── Topic computation ─────────────────────────────────────────────────────

    private static void ComputeTopics()
    {
        if (_writeRequestedTopic is not null)
        {
            return;
        }

        _writeRequestedTopic = "0x" + KeccakHash.ComputeHex("DataWriteRequested(address,string,bytes)");
        _readRequestedTopic = "0x" + KeccakHash.ComputeHex("DataReadRequested(address,string)");
    }
}
