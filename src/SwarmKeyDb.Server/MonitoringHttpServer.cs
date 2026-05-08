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

    public MonitoringHttpServer(
        IPAddress address,
        int port,
        MonitoringMetrics metrics,
        IReadinessProbe readinessProbe,
        bool metricsEnabled,
        bool dashboardEnabled,
        ILogger<MonitoringHttpServer> logger)
    {
        _metrics = metrics;
        _readinessProbe = readinessProbe;
        _metricsEnabled = metricsEnabled;
        _dashboardEnabled = dashboardEnabled;
        _logger = logger;
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
            await WriteJsonAsync(context.Response, HttpStatusCode.OK, new { status = "healthy" }, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (path.Equals("/ready", StringComparison.OrdinalIgnoreCase))
        {
            var (ready, message) = await _readinessProbe.CheckAsync(cancellationToken).ConfigureAwait(false);
            await WriteJsonAsync(
                context.Response,
                ready ? HttpStatusCode.OK : HttpStatusCode.ServiceUnavailable,
                new { status = ready ? "ready" : "not_ready", message },
                cancellationToken).ConfigureAwait(false);
            return;
        }

        if (_metricsEnabled && path.Equals("/metrics", StringComparison.OrdinalIgnoreCase))
        {
            await WriteTextAsync(context.Response, HttpStatusCode.OK, _metrics.CollectPrometheus(), "text/plain; version=0.0.4", cancellationToken).ConfigureAwait(false);
            return;
        }

        if (_dashboardEnabled && path.Equals("/logs", StringComparison.OrdinalIgnoreCase))
        {
            var count = int.TryParse(context.Request.QueryString["count"], out var parsedCount) ? parsedCount : 100;
            await WriteJsonAsync(context.Response, HttpStatusCode.OK, _metrics.GetRecentLogs(count), cancellationToken).ConfigureAwait(false);
            return;
        }

        if (_dashboardEnabled && (path.Equals("/dashboard", StringComparison.OrdinalIgnoreCase) || path.Equals("/", StringComparison.OrdinalIgnoreCase)))
        {
            await WriteTextAsync(context.Response, HttpStatusCode.OK, DashboardHtml, "text/html; charset=utf-8", cancellationToken).ConfigureAwait(false);
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

    private const string DashboardHtml = """
                                         <!doctype html>
                                         <html lang="en">
                                         <head>
                                           <meta charset="utf-8">
                                           <title>SwarmKeyDb Dashboard</title>
                                           <style>
                                             body { font-family: sans-serif; margin: 1.5rem; }
                                             .ok { color: #1c7c1c; }
                                             .bad { color: #b22; }
                                             table { border-collapse: collapse; width: 100%; margin-top: 1rem; }
                                             th, td { border: 1px solid #ddd; padding: 0.5rem; text-align: left; }
                                             code { background: #f5f5f5; padding: 0.1rem 0.25rem; }
                                           </style>
                                         </head>
                                         <body>
                                           <h1>SwarmKeyDb Dashboard</h1>
                                           <p>Readiness: <span id="ready-status">loading...</span></p>
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
                                             const metricsEl = document.getElementById('metrics');
                                             const logsEl = document.getElementById('logs');
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
                                                 'swarmkeydb_swarm_writes_total'
                                               ];
                                               return metricsText.split('\n').filter(line => wanted.some(prefix => line.startsWith(prefix))).join('\n');
                                             }
                                             async function refreshReady() {
                                               const response = await fetch('/ready');
                                               const data = await response.json();
                                               readyStatus.textContent = data.status + ' (' + data.message + ')';
                                               readyStatus.className = response.ok ? 'ok' : 'bad';
                                             }
                                             async function refreshMetrics() {
                                               const response = await fetch('/metrics');
                                               const text = await response.text();
                                               metricsEl.textContent = parseCounters(text);
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
                                               await Promise.all([refreshReady(), refreshMetrics(), refreshLogs()]);
                                             }
                                             refreshAll();
                                             setInterval(refreshAll, 3000);
                                           </script>
                                         </body>
                                         </html>
                                         """;
}
