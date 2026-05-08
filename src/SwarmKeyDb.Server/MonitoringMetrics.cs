using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using SwarmKeyDb;

namespace SwarmKeyDb.Server;

public sealed class MonitoringMetrics : IRedisCommandObserver
{
    private static readonly double[] LatencyBuckets = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];
    private readonly ConcurrentDictionary<string, OperationStats> _operations = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, long> _errors = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentQueue<MonitoringLogEntry> _logs = new();
    private readonly int _maxLogEntries;
    private readonly Func<ICacheStats> _cacheStatsAccessor;
    private readonly string _privacyMode;
    private long _activeConnections;
    private long _swarmReads;
    private long _swarmWrites;

    public MonitoringMetrics(Func<ICacheStats> cacheStatsAccessor, int maxLogEntries = 200, PrivacyMode privacyMode = PrivacyMode.None)
    {
        _cacheStatsAccessor = cacheStatsAccessor;
        _maxLogEntries = Math.Max(10, maxLogEntries);
        _privacyMode = privacyMode.ToString().ToLowerInvariant();
    }

    public void OnConnectionOpened() => Interlocked.Increment(ref _activeConnections);

    public void OnConnectionClosed() => Interlocked.Decrement(ref _activeConnections);

    public void OnSwarmRead() => Interlocked.Increment(ref _swarmReads);

    public void OnSwarmWrite() => Interlocked.Increment(ref _swarmWrites);

    public IReadOnlyList<MonitoringLogEntry> GetRecentLogs(int count)
    {
        var requested = Math.Max(1, count);
        return _logs.ToArray().TakeLast(requested).ToArray();
    }

    public void OnCommandCompleted(
        string command,
        string operation,
        bool succeeded,
        string? errorType,
        TimeSpan elapsed,
        string correlationId)
    {
        var stats = _operations.GetOrAdd(operation, _ => new OperationStats(LatencyBuckets));
        stats.Increment(succeeded, elapsed.TotalSeconds);
        if (!succeeded && !string.IsNullOrWhiteSpace(errorType))
        {
            _errors.AddOrUpdate(errorType, 1, (_, current) => current + 1);
        }

        var level = succeeded ? "INFO" : "ERROR";
        var message = succeeded
            ? $"Redis command {command} completed successfully."
            : $"Redis command {command} failed with error type '{errorType ?? "unknown"}'.";
        _logs.Enqueue(new MonitoringLogEntry(DateTimeOffset.UtcNow, level, correlationId, command, message));
        while (_logs.Count > _maxLogEntries && _logs.TryDequeue(out _))
        {
        }
    }

    public string CollectPrometheus()
    {
        var builder = new StringBuilder();
        builder.AppendLine("# HELP swarmkeydb_operations_total Total Redis operations by operation and status.");
        builder.AppendLine("# TYPE swarmkeydb_operations_total counter");
        builder.AppendLine("# HELP swarmkeydb_operation_latency_seconds Redis operation latency histogram.");
        builder.AppendLine("# TYPE swarmkeydb_operation_latency_seconds histogram");
        builder.AppendLine("# HELP swarmkeydb_operation_latency_quantile_seconds Redis operation latency quantiles.");
        builder.AppendLine("# TYPE swarmkeydb_operation_latency_quantile_seconds gauge");
        builder.AppendLine("# HELP swarmkeydb_operation_errors_total Total Redis operation errors by type.");
        builder.AppendLine("# TYPE swarmkeydb_operation_errors_total counter");
        builder.AppendLine("# HELP swarmkeydb_cache_hits_total Cache hits.");
        builder.AppendLine("# TYPE swarmkeydb_cache_hits_total counter");
        builder.AppendLine("# HELP swarmkeydb_cache_misses_total Cache misses.");
        builder.AppendLine("# TYPE swarmkeydb_cache_misses_total counter");
        builder.AppendLine("# HELP swarmkeydb_cache_hit_ratio Cache hit ratio.");
        builder.AppendLine("# TYPE swarmkeydb_cache_hit_ratio gauge");
        builder.AppendLine("# HELP swarmkeydb_active_connections Active Redis TCP connections.");
        builder.AppendLine("# TYPE swarmkeydb_active_connections gauge");
        builder.AppendLine("# HELP swarmkeydb_swarm_reads_total Total Swarm reads.");
        builder.AppendLine("# TYPE swarmkeydb_swarm_reads_total counter");
        builder.AppendLine("# HELP swarmkeydb_swarm_writes_total Total Swarm writes.");
        builder.AppendLine("# TYPE swarmkeydb_swarm_writes_total counter");

        foreach (var entry in _operations.OrderBy(static item => item.Key, StringComparer.Ordinal))
        {
            var operation = entry.Key.ToLowerInvariant();
            var stats = entry.Value.Snapshot();
            builder.AppendLine(FormatMetric("swarmkeydb_operations_total", stats.SuccessCount, ("operation", operation), ("status", "success")));
            builder.AppendLine(FormatMetric("swarmkeydb_operations_total", stats.ErrorCount, ("operation", operation), ("status", "error")));

            var cumulative = 0L;
            for (var i = 0; i < LatencyBuckets.Length; i++)
            {
                cumulative += stats.BucketCounts[i];
                builder.AppendLine(FormatMetric("swarmkeydb_operation_latency_seconds_bucket", cumulative, ("operation", operation), ("le", LatencyBuckets[i].ToString(CultureInfo.InvariantCulture))));
            }

            builder.AppendLine(FormatMetric("swarmkeydb_operation_latency_seconds_bucket", stats.Count, ("operation", operation), ("le", "+Inf")));
            builder.AppendLine(FormatMetric("swarmkeydb_operation_latency_seconds_sum", stats.SumSeconds, ("operation", operation)));
            builder.AppendLine(FormatMetric("swarmkeydb_operation_latency_seconds_count", stats.Count, ("operation", operation)));
            builder.AppendLine(FormatMetric("swarmkeydb_operation_latency_quantile_seconds", stats.P50Seconds, ("operation", operation), ("quantile", "0.50")));
            builder.AppendLine(FormatMetric("swarmkeydb_operation_latency_quantile_seconds", stats.P95Seconds, ("operation", operation), ("quantile", "0.95")));
            builder.AppendLine(FormatMetric("swarmkeydb_operation_latency_quantile_seconds", stats.P99Seconds, ("operation", operation), ("quantile", "0.99")));
        }

        foreach (var error in _errors.OrderBy(static item => item.Key, StringComparer.Ordinal))
        {
            builder.AppendLine(FormatMetric("swarmkeydb_operation_errors_total", error.Value, ("error_type", error.Key.ToLowerInvariant())));
        }

        var cacheStats = _cacheStatsAccessor();
        var cacheHits = cacheStats.Hits;
        var cacheMisses = cacheStats.Misses;
        var totalLookups = cacheHits + cacheMisses;
        var hitRatio = totalLookups == 0 ? 0D : cacheHits / (double)totalLookups;

        builder.AppendLine(FormatMetric("swarmkeydb_cache_hits_total", cacheHits));
        builder.AppendLine(FormatMetric("swarmkeydb_cache_misses_total", cacheMisses));
        builder.AppendLine(FormatMetric("swarmkeydb_cache_hit_ratio", hitRatio));
        builder.AppendLine(FormatMetric("swarmkeydb_active_connections", Interlocked.Read(ref _activeConnections)));
        builder.AppendLine(FormatMetric("swarmkeydb_swarm_reads_total", Interlocked.Read(ref _swarmReads)));
        builder.AppendLine(FormatMetric("swarmkeydb_swarm_writes_total", Interlocked.Read(ref _swarmWrites)));

        return builder.ToString();
    }

    private string FormatMetric(string name, long value, params (string Label, string Value)[] labels) =>
        FormatMetric(name, value.ToString(CultureInfo.InvariantCulture), labels);

    private string FormatMetric(string name, double value, params (string Label, string Value)[] labels) =>
        FormatMetric(name, value.ToString(CultureInfo.InvariantCulture), labels);

    private string FormatMetric(string name, string value, params (string Label, string Value)[] labels)
    {
        var allLabels = labels.Concat([(Label: "privacy_mode", Value: _privacyMode)]).ToArray();
        if (allLabels.Length == 0)
        {
            return $"{name} {value}";
        }

        var renderedLabels = string.Join(",", allLabels.Select(label => $"{label.Label}=\"{EscapeLabel(label.Value)}\""));
        return $"{name}{{{renderedLabels}}} {value}";
    }

    private static string EscapeLabel(string value) =>
        value.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("\"", "\\\"", StringComparison.Ordinal);

    private sealed class OperationStats
    {
        private readonly object _gate = new();
        private readonly double[] _latencyBuckets;
        private readonly long[] _bucketCounts;
        private readonly Queue<double> _recentDurations = new();
        private readonly int _windowSize;
        private long _successCount;
        private long _errorCount;
        private long _count;
        private double _sumSeconds;

        public OperationStats(double[] latencyBuckets, int windowSize = 1024)
        {
            _latencyBuckets = latencyBuckets;
            _bucketCounts = new long[latencyBuckets.Length];
            _windowSize = windowSize;
        }

        public void Increment(bool succeeded, double elapsedSeconds)
        {
            lock (_gate)
            {
                if (succeeded)
                {
                    _successCount++;
                }
                else
                {
                    _errorCount++;
                }

                _count++;
                _sumSeconds += elapsedSeconds;
                for (var i = 0; i < _latencyBuckets.Length; i++)
                {
                    if (elapsedSeconds <= _latencyBuckets[i])
                    {
                        _bucketCounts[i]++;
                        break;
                    }
                }

                _recentDurations.Enqueue(elapsedSeconds);
                while (_recentDurations.Count > _windowSize)
                {
                    _recentDurations.Dequeue();
                }
            }
        }

        public OperationStatsSnapshot Snapshot()
        {
            lock (_gate)
            {
                var durations = _recentDurations.ToArray();
                Array.Sort(durations);
                return new OperationStatsSnapshot(
                    SuccessCount: _successCount,
                    ErrorCount: _errorCount,
                    Count: _count,
                    SumSeconds: _sumSeconds,
                    BucketCounts: _bucketCounts.ToArray(),
                    P50Seconds: Percentile(durations, 0.50),
                    P95Seconds: Percentile(durations, 0.95),
                    P99Seconds: Percentile(durations, 0.99));
            }
        }

        private static double Percentile(IReadOnlyList<double> sortedValues, double percentile)
        {
            if (sortedValues.Count == 0)
            {
                return 0;
            }

            var position = percentile * (sortedValues.Count - 1);
            var lowerIndex = (int)Math.Floor(position);
            var upperIndex = (int)Math.Ceiling(position);
            if (lowerIndex == upperIndex)
            {
                return sortedValues[lowerIndex];
            }

            var weight = position - lowerIndex;
            return sortedValues[lowerIndex] + (sortedValues[upperIndex] - sortedValues[lowerIndex]) * weight;
        }
    }

    private sealed record OperationStatsSnapshot(
        long SuccessCount,
        long ErrorCount,
        long Count,
        double SumSeconds,
        long[] BucketCounts,
        double P50Seconds,
        double P95Seconds,
        double P99Seconds);
}

public sealed record MonitoringLogEntry(
    DateTimeOffset Timestamp,
    string Level,
    string CorrelationId,
    string Command,
    string Message);

public sealed class NoOpCacheStats : ICacheStats
{
    public static readonly NoOpCacheStats Instance = new();
    public long Hits => 0;
    public long Misses => 0;
    public long Evictions => 0;
}
