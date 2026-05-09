using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace SwarmKeyDb.Server;

public sealed class MonitoringHttpServer : IDisposable
{
    private readonly HttpListener _listener = new();
    private readonly MonitoringMetrics _metrics;
    private readonly IReadinessProbe _readinessProbe;
    private readonly bool _metricsEnabled;
    private readonly bool _dashboardEnabled;
    private readonly ILogger<MonitoringHttpServer> _logger;
    private readonly IShardHealthProvider? _shardHealthProvider;
    private readonly IBackendStatusProvider? _backendStatusProvider;
    private readonly EthereumBridgeService? _ethereumBridge;
    private readonly CrossChainSyncService? _crossChainSyncService;
    private readonly IOfflineStatusProvider _offlineStatusProvider;
    private readonly IConsistencyVerificationStatusProvider _consistencyStatusProvider;
    private readonly string _privacyMode;
    private readonly string _didMode;

    public MonitoringHttpServer(
        IPAddress address,
        int port,
        MonitoringMetrics metrics,
        IReadinessProbe readinessProbe,
        bool metricsEnabled,
        bool dashboardEnabled,
        ILogger<MonitoringHttpServer> logger,
        IShardHealthProvider? shardHealthProvider = null,
        IBackendStatusProvider? backendStatusProvider = null,
        EthereumBridgeService? ethereumBridge = null,
        CrossChainSyncService? crossChainSyncService = null,
        IOfflineStatusProvider? offlineStatusProvider = null,
        IConsistencyVerificationStatusProvider? consistencyStatusProvider = null,
        PrivacyMode privacyMode = PrivacyMode.None,
        DidAuthMode didMode = DidAuthMode.None)
    {
        _metrics = metrics;
        _readinessProbe = readinessProbe;
        _metricsEnabled = metricsEnabled;
        _dashboardEnabled = dashboardEnabled;
        _logger = logger;
        _shardHealthProvider = shardHealthProvider;
        _backendStatusProvider = backendStatusProvider;
        _ethereumBridge = ethereumBridge;
        _crossChainSyncService = crossChainSyncService;
        _offlineStatusProvider = offlineStatusProvider ?? NoOpOfflineStatusProvider.Instance;
        _consistencyStatusProvider = consistencyStatusProvider ?? NoOpConsistencyVerificationStatusProvider.Instance;
        _privacyMode = privacyMode.ToString().ToLowerInvariant();
        _didMode = didMode.ToString().ToLowerInvariant();
        _listener.Prefixes.Add($"http://{(address.Equals(IPAddress.Any) ? "+" : address.ToString())}:{port}/");
    }

    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        _listener.Start();
        _logger.LogInformation("Monitoring HTTP server listening on {Prefix}", _listener.Prefixes.FirstOrDefault());
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

                _ = Task.Run(() => HandleAsync(context, cancellationToken), cancellationToken);
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

    private async Task HandleAsync(HttpListenerContext context, CancellationToken cancellationToken)
    {
        var path = context.Request.Url?.AbsolutePath ?? "/";
        if (path.Equals("/health", StringComparison.OrdinalIgnoreCase))
        {
            var shardHealth = _shardHealthProvider is null
                ? null
                : await _shardHealthProvider.GetShardHealthAsync(cancellationToken).ConfigureAwait(false);
            var degraded = shardHealth?.Any(static shard => !shard.Ready) == true;
            var consistencySnapshot = _consistencyStatusProvider.GetSnapshot();
            await WriteJsonAsync(
                context.Response,
                degraded ? HttpStatusCode.ServiceUnavailable : HttpStatusCode.OK,
                new
                {
                    status = degraded ? "degraded" : "healthy",
                    offline_queue_depth = _offlineStatusProvider.QueueDepth,
                    last_successful_sync_utc = _offlineStatusProvider.LastSuccessfulSyncUtc,
                    consistencyVerification = new
                    {
                        lastVerificationUtc = consistencySnapshot.LastVerificationUtc,
                        totalVerifications = consistencySnapshot.TotalVerifications,
                        successRate = consistencySnapshot.SuccessRate,
                        violationCount = consistencySnapshot.ViolationCount,
                        worstLatencyMs = consistencySnapshot.WorstLatencyMs,
                        evictionByVerificationTotal = consistencySnapshot.EvictionByVerificationTotal
                    },
                    shards = shardHealth?.Select(static shard => new
                    {
                        shard = shard.Shard,
                        status = shard.Ready ? "healthy" : "unreachable",
                        message = shard.Message,
                        keyCount = shard.KeyCount
                    })
                },
                cancellationToken).ConfigureAwait(false);
            return;
        }

        if (path.Equals("/ready", StringComparison.OrdinalIgnoreCase))
        {
            var (ready, message) = await _readinessProbe.CheckAsync(cancellationToken).ConfigureAwait(false);
            var shardHealth = _shardHealthProvider is null
                ? null
                : await _shardHealthProvider.GetShardHealthAsync(cancellationToken).ConfigureAwait(false);
            await WriteJsonAsync(
                context.Response,
                ready ? HttpStatusCode.OK : HttpStatusCode.ServiceUnavailable,
                new
                {
                    status = ready ? "ready" : "not_ready",
                    message,
                    offline_queue_depth = _offlineStatusProvider.QueueDepth,
                    last_successful_sync_utc = _offlineStatusProvider.LastSuccessfulSyncUtc,
                    shards = shardHealth?.Select(static shard => new
                    {
                        shard = shard.Shard,
                        status = shard.Ready ? "ready" : "not_ready",
                        message = shard.Message,
                        keyCount = shard.KeyCount
                    })
                },
                cancellationToken).ConfigureAwait(false);
            return;
        }

        if (_metricsEnabled && path.Equals("/metrics", StringComparison.OrdinalIgnoreCase))
        {
            var payload = _metrics.CollectPrometheus();
            if (_shardHealthProvider is not null)
            {
                var shardHealth = await _shardHealthProvider.GetShardHealthAsync(cancellationToken).ConfigureAwait(false);
                payload = $"{payload}{BuildShardPrometheusMetrics(shardHealth, _privacyMode)}";
            }

            await WriteTextAsync(context.Response, HttpStatusCode.OK, payload, "text/plain; version=0.0.4", cancellationToken).ConfigureAwait(false);
            return;
        }

        if (_dashboardEnabled && path.Equals("/logs", StringComparison.OrdinalIgnoreCase))
        {
            var count = int.TryParse(context.Request.QueryString["count"], out var parsedCount) ? parsedCount : 100;
            await WriteJsonAsync(context.Response, HttpStatusCode.OK, _metrics.GetRecentLogs(count), cancellationToken).ConfigureAwait(false);
            return;
        }

        if (path.Equals("/backend", StringComparison.OrdinalIgnoreCase))
        {
            var statuses = _backendStatusProvider is null
                ? []
                : await _backendStatusProvider.GetStatusAsync(cancellationToken).ConfigureAwait(false);
            var degraded = statuses.Any(static status => !status.Ready);
            await WriteJsonAsync(
                context.Response,
                degraded ? HttpStatusCode.ServiceUnavailable : HttpStatusCode.OK,
                new
                {
                    status = degraded ? "degraded" : "healthy",
                    backends = statuses.Select(static backend => new
                    {
                        backend = backend.Backend,
                        status = backend.Ready ? "healthy" : "unreachable",
                        message = backend.Message
                    })
                },
                cancellationToken).ConfigureAwait(false);
            return;
        }

        if (path.Equals("/ethereum/bridge", StringComparison.OrdinalIgnoreCase))
        {
            var bridgeState = _ethereumBridge?.GetState() ?? new EthereumBridgeState();
            // Return 200 when bridge is disabled (intentional) or connected.
            // Return 503 only when the bridge is enabled but not currently operational
            // (connecting / retrying / error) to signal a transient failure.
            var statusCode = bridgeState.Status is EthereumBridgeStatus.Connected
                                                 or EthereumBridgeStatus.Disabled
                ? HttpStatusCode.OK
                : HttpStatusCode.ServiceUnavailable;
            await WriteJsonAsync(
                context.Response,
                statusCode,
                new
                {
                    status = bridgeState.Status.ToString().ToLowerInvariant(),
                    lastProcessedBlock = bridgeState.LastProcessedBlock,
                    eventCount = bridgeState.EventCount,
                    connectedSince = bridgeState.ConnectedSince,
                    lastError = bridgeState.LastError,
                    contractAddress = bridgeState.ContractAddress,
                    rpcUrl = bridgeState.RpcUrl
                },
                cancellationToken).ConfigureAwait(false);
            return;
        }

        if (path.Equals("/sync", StringComparison.OrdinalIgnoreCase))
        {
            var summary = _crossChainSyncService is null
                ? Array.Empty<ChainSyncSummary>()
                : await _crossChainSyncService.GetSummaryAsync(cancellationToken).ConfigureAwait(false);
            await WriteJsonAsync(context.Response, HttpStatusCode.OK, new { chains = summary }, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (path.StartsWith("/sync/", StringComparison.OrdinalIgnoreCase))
        {
            var key = WebUtility.UrlDecode(path["/sync/".Length..]);
            if (string.IsNullOrWhiteSpace(key))
            {
                await WriteJsonAsync(context.Response, HttpStatusCode.BadRequest, new { error = "Missing key." }, cancellationToken).ConfigureAwait(false);
                return;
            }

            var status = _crossChainSyncService is null
                ? null
                : await _crossChainSyncService.GetStatusAsync(key, cancellationToken).ConfigureAwait(false);
            object payload = status is null ? new { key, chains = Array.Empty<object>() } : status;
            await WriteJsonAsync(
                context.Response,
                status is null ? HttpStatusCode.NotFound : HttpStatusCode.OK,
                payload,
                cancellationToken).ConfigureAwait(false);
            return;
        }

        if (_dashboardEnabled && (path.Equals("/dashboard", StringComparison.OrdinalIgnoreCase) || path.Equals("/", StringComparison.OrdinalIgnoreCase)))
        {
            await WriteTextAsync(context.Response, HttpStatusCode.OK, BuildDashboardHtml(_privacyMode, _didMode), "text/html; charset=utf-8", cancellationToken).ConfigureAwait(false);
            return;
        }

        await WriteTextAsync(context.Response, HttpStatusCode.NotFound, "Not Found", "text/plain", cancellationToken).ConfigureAwait(false);
    }

    private static async Task WriteJsonAsync(HttpListenerResponse response, HttpStatusCode statusCode, object payload, CancellationToken cancellationToken)
    {
        response.StatusCode = (int)statusCode;
        response.ContentType = "application/json";
        var json = JsonSerializer.Serialize(payload);
        await WriteBytesAsync(response, Encoding.UTF8.GetBytes(json), cancellationToken).ConfigureAwait(false);
    }

    private static async Task WriteTextAsync(HttpListenerResponse response, HttpStatusCode statusCode, string payload, string contentType, CancellationToken cancellationToken)
    {
        response.StatusCode = (int)statusCode;
        response.ContentType = contentType;
        await WriteBytesAsync(response, Encoding.UTF8.GetBytes(payload), cancellationToken).ConfigureAwait(false);
    }

    private static async Task WriteBytesAsync(HttpListenerResponse response, byte[] bytes, CancellationToken cancellationToken)
    {
        response.ContentLength64 = bytes.Length;
        await response.OutputStream.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
        response.Close();
    }

    private static string BuildShardPrometheusMetrics(IReadOnlyList<ShardHealthStatus> shardHealth, string privacyMode)
    {
        var builder = new StringBuilder();
        builder.AppendLine("# HELP swarmkeydb_shard_up Shard health status (1=healthy, 0=unreachable).");
        builder.AppendLine("# TYPE swarmkeydb_shard_up gauge");
        builder.AppendLine("# HELP swarmkeydb_shard_key_count Approximate key count by shard.");
        builder.AppendLine("# TYPE swarmkeydb_shard_key_count gauge");
        foreach (var shard in shardHealth)
        {
            builder.AppendLine($"swarmkeydb_shard_up{{shard=\"{EscapeMetricLabel(shard.Shard)}\",privacy_mode=\"{EscapeMetricLabel(privacyMode)}\"}} {(shard.Ready ? 1 : 0)}");
            if (shard.KeyCount is { } keyCount)
            {
                builder.AppendLine($"swarmkeydb_shard_key_count{{shard=\"{EscapeMetricLabel(shard.Shard)}\",privacy_mode=\"{EscapeMetricLabel(privacyMode)}\"}} {keyCount}");
            }
        }

        return builder.ToString();
    }

    private static string EscapeMetricLabel(string value) =>
        value
            .Replace("\\", "\\\\", StringComparison.Ordinal)
            .Replace("\"", "\\\"", StringComparison.Ordinal)
            .Replace("\n", "\\n", StringComparison.Ordinal)
            .Replace("\r", "\\r", StringComparison.Ordinal);

    private static string BuildDashboardHtml(string privacyMode, string didMode) =>
        DashboardHtmlTemplate
            .Replace("__PRIVACY_MODE__", WebUtility.HtmlEncode(privacyMode), StringComparison.Ordinal)
            .Replace("__DID_MODE__", WebUtility.HtmlEncode(didMode), StringComparison.Ordinal);

    private const string DashboardHtmlTemplate = """
                                         <!doctype html>
                                         <html lang="en">
                                         <head>
                                           <meta charset="utf-8">
                                           <title>SwarmKeyDb Dashboard</title>
                                            <style>
                                              body { font-family: sans-serif; margin: 1.5rem; }
                                              .ok { color: #1c7c1c; }
                                              .warn { color: #a76a00; }
                                              .bad { color: #b22; }
                                              table { border-collapse: collapse; width: 100%; margin-top: 1rem; }
                                              th, td { border: 1px solid #ddd; padding: 0.5rem; text-align: left; }
                                              code { background: #f5f5f5; padding: 0.1rem 0.25rem; }
                                            </style>
                                          </head>
                                          <body>
                                            <h1>SwarmKeyDb Dashboard</h1>
                                             <p>Privacy Mode: <strong>__PRIVACY_MODE__</strong></p>
                                              <p>DID Auth Mode: <strong>__DID_MODE__</strong></p>
                                             <p>Readiness: <span id="ready-status">loading...</span></p>
                                             <p>Offline Queue: <strong id="offline-queue-depth">loading...</strong></p>
                                             <p>Last Sync: <strong id="offline-last-sync">loading...</strong></p>
                                             <p>Consistency Success Rate: <strong id="consistency-success-rate">loading...</strong></p>
                                             <p>Consistency Violations: <strong id="consistency-violation-count">loading...</strong></p>
                                             <p>Consistency Worst Latency: <strong id="consistency-worst-latency">loading...</strong></p>
                                             <p>Cache Evictions by Verification: <strong id="consistency-eviction-count">loading...</strong></p>
                                              <h2>Cross-chain replication health</h2>
                                            <table>
                                              <thead>
                                                <tr><th>Chain</th><th>Pending</th><th>Synced</th><th>Failed</th><th>Health</th></tr>
                                              </thead>
                                              <tbody id="sync-summary"></tbody>
                                            </table>
                                            <label for="sync-key">Sync status key</label>
                                            <input id="sync-key" value="profile:name" />
                                            <button id="sync-refresh" type="button">Refresh sync status</button>
                                            <pre id="sync-status">loading...</pre>
                                            <h2>Operation counters</h2>
                                            <pre id="metrics">loading...</pre>
                                            <h2>Recent logs</h2>
                                           <table>
                                             <thead>
                                               <tr><th>Time</th><th>Level</th><th>Correlation ID</th><th>Command</th><th>Message</th></tr>
                                             </thead>
                                             <tbody id="logs"></tbody>
                                            </table>
                                            <script>
                                              const readyStatus = document.getElementById('ready-status');
                                              const syncSummaryEl = document.getElementById('sync-summary');
                                              const syncStatusEl = document.getElementById('sync-status');
                                              const syncKeyInput = document.getElementById('sync-key');
                                              const syncRefreshButton = document.getElementById('sync-refresh');
                                              const metricsEl = document.getElementById('metrics');
                                              const logsEl = document.getElementById('logs');
                                              const offlineQueueDepthEl = document.getElementById('offline-queue-depth');
                                              const offlineLastSyncEl = document.getElementById('offline-last-sync');
                                              const consistencySuccessRateEl = document.getElementById('consistency-success-rate');
                                              const consistencyViolationCountEl = document.getElementById('consistency-violation-count');
                                              const consistencyWorstLatencyEl = document.getElementById('consistency-worst-latency');
                                              const consistencyEvictionCountEl = document.getElementById('consistency-eviction-count');
                                              function parseCounters(metricsText) {
                                               const wanted = [
                                                 'swarmkeydb_operations_total{operation="get",status="success"}',
                                                 'swarmkeydb_operations_total{operation="put",status="success"}',
                                                 'swarmkeydb_operations_total{operation="delete",status="success"}',
                                                 'swarmkeydb_operations_total{operation="list",status="success"}',
                                                 'swarmkeydb_operations_total{operation="batch",status="success"}',
                                                  'swarmkeydb_cache_hit_ratio',
                                                  'swarmkeydb_active_connections',
                                                  'swarmkeydb_swarm_reads_total',
                                                  'swarmkeydb_swarm_writes_total',
                                                  'swarmkeydb_consistency_success_rate',
                                                  'swarmkeydb_consistency_violations_total',
                                                  'swarmkeydb_consistency_worst_latency_ms',
                                                  'swarmkeydb_cache_verification_pass_total',
                                                  'swarmkeydb_cache_verification_fail_total',
                                                  'swarmkeydb_cache_eviction_by_verification_total'
                                                ];
                                               return metricsText.split('\n').filter(line => wanted.some(prefix => line.startsWith(prefix))).join('\n');
                                             }
                                             async function refreshReady() {
                                                const response = await fetch('/ready');
                                                const data = await response.json();
                                                readyStatus.textContent = data.status + ' (' + data.message + ')';
                                                readyStatus.className = response.ok ? 'ok' : 'bad';
                                                 offlineQueueDepthEl.textContent = String(data.offline_queue_depth ?? 0);
                                                 offlineLastSyncEl.textContent = data.last_successful_sync_utc
                                                   ? new Date(data.last_successful_sync_utc).toLocaleString()
                                                   : 'never';
                                                 const consistency = data.consistencyVerification || {};
                                                 consistencySuccessRateEl.textContent = `${Math.round((consistency.successRate ?? 1) * 10000) / 100}%`;
                                                 consistencyViolationCountEl.textContent = String(consistency.violationCount ?? 0);
                                                 consistencyWorstLatencyEl.textContent = `${Math.round(consistency.worstLatencyMs ?? 0)} ms`;
                                                 consistencyEvictionCountEl.textContent = String(consistency.evictionByVerificationTotal ?? 0);
                                               }
                                              async function refreshMetrics() {
                                                const response = await fetch('/metrics');
                                                const text = await response.text();
                                                metricsEl.textContent = parseCounters(text);
                                              }
                                              async function refreshSyncSummary() {
                                                const response = await fetch('/sync');
                                                const payload = await response.json();
                                                syncSummaryEl.innerHTML = '';
                                                payload.chains.forEach(chain => {
                                                  const row = document.createElement('tr');
                                                  const healthClass = chain.health === 'green' ? 'ok' : chain.health === 'yellow' ? 'warn' : 'bad';
                                                  const cells = [
                                                    `${chain.chainName} (${chain.chainId})`,
                                                    String(chain.pendingCount),
                                                    String(chain.syncedCount),
                                                    String(chain.failedCount)
                                                  ];
                                                  cells.forEach(value => {
                                                    const cell = document.createElement('td');
                                                    cell.textContent = value;
                                                    row.appendChild(cell);
                                                  });
                                                  const healthCell = document.createElement('td');
                                                  const badge = document.createElement('span');
                                                  badge.className = healthClass;
                                                  badge.textContent = chain.health;
                                                  healthCell.appendChild(badge);
                                                  row.appendChild(healthCell);
                                                  syncSummaryEl.appendChild(row);
                                                });
                                                if (!payload.chains.length) {
                                                  syncSummaryEl.innerHTML = '<tr><td colspan="5">Cross-chain sync disabled or no tracked keys yet.</td></tr>';
                                                }
                                              }
                                              async function refreshSyncStatus() {
                                                const key = syncKeyInput.value.trim();
                                                if (!key) {
                                                  syncStatusEl.textContent = 'Enter a key to inspect sync state.';
                                                  return;
                                                }
                                                const response = await fetch('/sync/' + encodeURIComponent(key));
                                                const payload = await response.json();
                                                syncStatusEl.textContent = JSON.stringify(payload, null, 2);
                                              }
                                              async function refreshLogs() {
                                                const response = await fetch('/logs?count=15');
                                                const logs = await response.json();
                                               logsEl.innerHTML = '';
                                               logs.forEach(log => {
                                                 const row = document.createElement('tr');
                                                 row.innerHTML = `<td>${new Date(log.timestamp).toLocaleTimeString()}</td><td>${log.level}</td><td><code>${log.correlationId}</code></td><td>${log.command}</td><td>${log.message}</td>`;
                                                 logsEl.appendChild(row);
                                                });
                                              }
                                              async function refreshAll() {
                                                await Promise.all([refreshReady(), refreshMetrics(), refreshLogs(), refreshSyncSummary(), refreshSyncStatus()]);
                                              }
                                              syncRefreshButton.addEventListener('click', refreshSyncStatus);
                                              refreshAll();
                                              setInterval(refreshAll, 3000);
                                            </script>
                                         </body>
                                         </html>
                                         """;
}
