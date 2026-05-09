using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using SwarmKeyDb;

namespace SwarmKeyDb.Server;

public sealed class MonitoringMetrics : IRedisCommandObserver, IResyncMetricsReporter
{
    private static readonly double[] LatencyBuckets = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];
    private readonly ConcurrentDictionary<string, OperationStats> _operations = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, long> _errors = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentQueue<MonitoringLogEntry> _logs = new();
    private readonly int _maxLogEntries;
    private readonly Func<ICacheStats> _cacheStatsAccessor;
    private readonly Func<IOfflineStatusProvider> _offlineStatusAccessor;
    private readonly Func<IConsistencyVerificationStatusProvider> _consistencyStatusAccessor;
    private readonly Func<ICacheSyncStatusProvider> _cacheSyncStatusAccessor;
    private readonly Func<PubSubManager?> _pubSubManagerAccessor;
    private readonly Func<TransactionMetricsSnapshot> _transactionMetricsAccessor;
    private readonly string _privacyMode;
    private long _activeConnections;
    private long _swarmReads;
    private long _swarmWrites;
    private long _resyncPartialTotal;
    private long _resyncFullTotal;
    private long _resyncKeysReplayedTotal;
    private long _resyncDurationMilliseconds;
    private long _cacheDriftTotal;
    private long _lastObservedCacheSyncUnixMs;

    public MonitoringMetrics(
        Func<ICacheStats> cacheStatsAccessor,
        Func<IOfflineStatusProvider>? offlineStatusAccessor = null,
        Func<IConsistencyVerificationStatusProvider>? consistencyStatusAccessor = null,
        Func<ICacheSyncStatusProvider>? cacheSyncStatusAccessor = null,
        int maxLogEntries = 200,
        PrivacyMode privacyMode = PrivacyMode.None,
        Func<PubSubManager?>? pubSubManagerAccessor = null,
        Func<TransactionMetricsSnapshot>? transactionMetricsAccessor = null)
    {
        _cacheStatsAccessor = cacheStatsAccessor;
        _offlineStatusAccessor = offlineStatusAccessor ?? (() => NoOpOfflineStatusProvider.Instance);
        _consistencyStatusAccessor = consistencyStatusAccessor ?? (() => NoOpConsistencyVerificationStatusProvider.Instance);
        _cacheSyncStatusAccessor = cacheSyncStatusAccessor ?? (() => NoOpCacheSyncStatusProvider.Instance);
        _pubSubManagerAccessor = pubSubManagerAccessor ?? (() => null);
        _transactionMetricsAccessor = transactionMetricsAccessor ?? (() => new TransactionMetricsSnapshot(0, 0, 0));
        _maxLogEntries = Math.Max(10, maxLogEntries);
        _privacyMode = privacyMode.ToString().ToLowerInvariant();
    }

    public void OnConnectionOpened() => Interlocked.Increment(ref _activeConnections);

    public void OnConnectionClosed() => Interlocked.Decrement(ref _activeConnections);

    public void OnSwarmRead() => Interlocked.Increment(ref _swarmReads);

    public void OnSwarmWrite() => Interlocked.Increment(ref _swarmWrites);

    public void RecordResync(ResyncMode mode, TimeSpan duration, int keysReplayed)
    {
        if (mode == ResyncMode.Partial)
        {
            Interlocked.Increment(ref _resyncPartialTotal);
        }
        else if (mode == ResyncMode.Full)
        {
            Interlocked.Increment(ref _resyncFullTotal);
        }

        Interlocked.Add(ref _resyncKeysReplayedTotal, Math.Max(0, keysReplayed));
        Interlocked.Exchange(ref _resyncDurationMilliseconds, Math.Max(0, (long)duration.TotalMilliseconds));
    }

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
        builder.AppendLine("# HELP swarmkeydb_offline_queue_depth Pending offline journal entries.");
        builder.AppendLine("# TYPE swarmkeydb_offline_queue_depth gauge");
        builder.AppendLine("# HELP swarmkeydb_offline_last_sync_unix_time Last successful offline sync time.");
        builder.AppendLine("# TYPE swarmkeydb_offline_last_sync_unix_time gauge");
        builder.AppendLine("# HELP swarmkeydb_consistency_verification_total Total consistency verification checks.");
        builder.AppendLine("# TYPE swarmkeydb_consistency_verification_total counter");
        builder.AppendLine("# HELP swarmkeydb_consistency_violations_total Total consistency verification violations.");
        builder.AppendLine("# TYPE swarmkeydb_consistency_violations_total counter");
        builder.AppendLine("# HELP swarmkeydb_consistency_success_rate Consistency verification success rate.");
        builder.AppendLine("# TYPE swarmkeydb_consistency_success_rate gauge");
        builder.AppendLine("# HELP swarmkeydb_consistency_worst_latency_ms Worst consistency verification latency in milliseconds.");
        builder.AppendLine("# TYPE swarmkeydb_consistency_worst_latency_ms gauge");
        builder.AppendLine("# HELP swarmkeydb_consistency_last_verification_unix_time Last consistency verification timestamp.");
        builder.AppendLine("# TYPE swarmkeydb_consistency_last_verification_unix_time gauge");
        builder.AppendLine("# HELP swarmkeydb_cache_verification_pass_total Total cache reads that passed consistency verification.");
        builder.AppendLine("# TYPE swarmkeydb_cache_verification_pass_total counter");
        builder.AppendLine("# HELP swarmkeydb_cache_verification_fail_total Total cache reads that failed consistency verification.");
        builder.AppendLine("# TYPE swarmkeydb_cache_verification_fail_total counter");
        builder.AppendLine("# HELP swarmkeydb_cache_eviction_by_verification_total Total cache evictions triggered by a consistency verification failure.");
        builder.AppendLine("# TYPE swarmkeydb_cache_eviction_by_verification_total counter");
        builder.AppendLine("# HELP swarmkeydb_resync_partial_total Total completed partial resync operations.");
        builder.AppendLine("# TYPE swarmkeydb_resync_partial_total counter");
        builder.AppendLine("# HELP swarmkeydb_resync_full_total Total completed full resync operations.");
        builder.AppendLine("# TYPE swarmkeydb_resync_full_total counter");
        builder.AppendLine("# HELP swarmkeydb_resync_duration_seconds Duration in seconds for the last completed resync.");
        builder.AppendLine("# TYPE swarmkeydb_resync_duration_seconds gauge");
        builder.AppendLine("# HELP swarmkeydb_resync_keys_replayed_total Total keys replayed by resync operations.");
        builder.AppendLine("# TYPE swarmkeydb_resync_keys_replayed_total counter");
        builder.AppendLine("# HELP swarmkeydb_cache_drift_total Total drifted keys reconciled by anti-entropy cycles.");
        builder.AppendLine("# TYPE swarmkeydb_cache_drift_total counter");
        builder.AppendLine("# HELP swarmkeydb_sync_lag_keys Current key reconciliation lag (pending reconciliations).");
        builder.AppendLine("# TYPE swarmkeydb_sync_lag_keys gauge");
        builder.AppendLine("# HELP swarmkeydb_pubsub_subscribers_total Current number of unique Pub/Sub subscriber connections.");
        builder.AppendLine("# TYPE swarmkeydb_pubsub_subscribers_total gauge");
        builder.AppendLine("# HELP swarmkeydb_pubsub_messages_published_total Total messages published via PUBLISH command.");
        builder.AppendLine("# TYPE swarmkeydb_pubsub_messages_published_total counter");
        builder.AppendLine("# HELP swarmkeydb_pubsub_messages_dropped_total Total messages dropped due to slow or full subscriber buffers.");
        builder.AppendLine("# TYPE swarmkeydb_pubsub_messages_dropped_total counter");
        builder.AppendLine("# HELP swarmkeydb_transaction_exec_total Total EXEC commands executed (including aborted transactions).");
        builder.AppendLine("# TYPE swarmkeydb_transaction_exec_total counter");
        builder.AppendLine("# HELP swarmkeydb_transaction_abort_total Total transactions aborted (EXECABORT or WATCH conflict).");
        builder.AppendLine("# TYPE swarmkeydb_transaction_abort_total counter");
        builder.AppendLine("# HELP swarmkeydb_transaction_watch_conflict_total Total EXEC aborts caused by a WATCH key conflict.");
        builder.AppendLine("# TYPE swarmkeydb_transaction_watch_conflict_total counter");

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
        var offlineStatus = _offlineStatusAccessor();
        builder.AppendLine(FormatMetric("swarmkeydb_offline_queue_depth", offlineStatus.QueueDepth));
        builder.AppendLine(FormatMetric(
            "swarmkeydb_offline_last_sync_unix_time",
            offlineStatus.LastSuccessfulSyncUtc?.ToUnixTimeSeconds() ?? 0));
        var consistency = _consistencyStatusAccessor().GetSnapshot();
        builder.AppendLine(FormatMetric("swarmkeydb_consistency_verification_total", consistency.TotalVerifications));
        builder.AppendLine(FormatMetric("swarmkeydb_consistency_violations_total", consistency.ViolationCount));
        builder.AppendLine(FormatMetric("swarmkeydb_consistency_success_rate", consistency.SuccessRate));
        builder.AppendLine(FormatMetric("swarmkeydb_consistency_worst_latency_ms", consistency.WorstLatencyMs));
        builder.AppendLine(FormatMetric(
            "swarmkeydb_consistency_last_verification_unix_time",
            consistency.LastVerificationUtc?.ToUnixTimeSeconds() ?? 0));
        var passTotal = Math.Max(0, consistency.TotalVerifications - consistency.ViolationCount);
        builder.AppendLine(FormatMetric("swarmkeydb_cache_verification_pass_total", passTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_cache_verification_fail_total", consistency.ViolationCount));
        builder.AppendLine(FormatMetric("swarmkeydb_cache_eviction_by_verification_total", consistency.EvictionByVerificationTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_resync_partial_total", Interlocked.Read(ref _resyncPartialTotal)));
        builder.AppendLine(FormatMetric("swarmkeydb_resync_full_total", Interlocked.Read(ref _resyncFullTotal)));
        builder.AppendLine(FormatMetric("swarmkeydb_resync_duration_seconds", Interlocked.Read(ref _resyncDurationMilliseconds) / 1000D));
        builder.AppendLine(FormatMetric("swarmkeydb_resync_keys_replayed_total", Interlocked.Read(ref _resyncKeysReplayedTotal)));
        var cacheSync = _cacheSyncStatusAccessor().GetSnapshot();
        UpdateCacheDriftTotal(cacheSync);
        builder.AppendLine(FormatMetric("swarmkeydb_cache_drift_total", Interlocked.Read(ref _cacheDriftTotal)));
        builder.AppendLine(FormatMetric("swarmkeydb_sync_lag_keys", Math.Max(0, cacheSync.PendingReconciliations)));
        var pubSub = _pubSubManagerAccessor();
        builder.AppendLine(FormatMetric("swarmkeydb_pubsub_subscribers_total", pubSub?.GetSubscribersTotal() ?? 0L));
        builder.AppendLine(FormatMetric("swarmkeydb_pubsub_messages_published_total", pubSub?.MessagesPublishedTotal ?? 0L));
        builder.AppendLine(FormatMetric("swarmkeydb_pubsub_messages_dropped_total", pubSub?.MessagesDroppedTotal ?? 0L));
        var txMetrics = _transactionMetricsAccessor();
        builder.AppendLine(FormatMetric("swarmkeydb_transaction_exec_total", txMetrics.ExecTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_transaction_abort_total", txMetrics.AbortTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_transaction_watch_conflict_total", txMetrics.WatchConflictTotal));

        return builder.ToString();
    }

    private void UpdateCacheDriftTotal(CacheSyncSnapshot snapshot)
    {
        if (!snapshot.LastSuccessfulSyncUtc.HasValue)
        {
            return;
        }

        var observedUnixMs = snapshot.LastSuccessfulSyncUtc.Value.ToUnixTimeMilliseconds();
        while (true)
        {
            var lastSeenUnixMs = Interlocked.Read(ref _lastObservedCacheSyncUnixMs);
            if (observedUnixMs <= lastSeenUnixMs)
            {
                return;
            }

            if (Interlocked.CompareExchange(ref _lastObservedCacheSyncUnixMs, observedUnixMs, lastSeenUnixMs) == lastSeenUnixMs)
            {
                Interlocked.Add(ref _cacheDriftTotal, Math.Max(0, snapshot.ReconciledKeysLastCycle));
                return;
            }
        }
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
