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
    private readonly Func<StreamMetricsSnapshot> _streamMetricsAccessor;
    private readonly Func<ScriptMetricsSnapshot> _scriptMetricsAccessor;
    private readonly Func<CompatibilityMetricsSnapshot> _compatibilityMetricsAccessor;
    private readonly string _privacyMode;
    private long _activeConnections;
    private long _activeWebSocketConnections;
    private long _webSocketConnectionsTotal;
    private long _webSocketMessagesReceivedTotal;
    private long _webSocketMessagesSentTotal;
    private long _webSocketErrorsTotal;
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
        Func<TransactionMetricsSnapshot>? transactionMetricsAccessor = null,
        Func<StreamMetricsSnapshot>? streamMetricsAccessor = null,
        Func<ScriptMetricsSnapshot>? scriptMetricsAccessor = null,
        Func<CompatibilityMetricsSnapshot>? compatibilityMetricsAccessor = null)
    {
        _cacheStatsAccessor = cacheStatsAccessor;
        _offlineStatusAccessor = offlineStatusAccessor ?? (() => NoOpOfflineStatusProvider.Instance);
        _consistencyStatusAccessor = consistencyStatusAccessor ?? (() => NoOpConsistencyVerificationStatusProvider.Instance);
        _cacheSyncStatusAccessor = cacheSyncStatusAccessor ?? (() => NoOpCacheSyncStatusProvider.Instance);
        _pubSubManagerAccessor = pubSubManagerAccessor ?? (() => null);
        _transactionMetricsAccessor = transactionMetricsAccessor ?? (() => EmptyTransactionMetricsSnapshot);
        _streamMetricsAccessor = streamMetricsAccessor ?? (() => EmptyStreamMetricsSnapshot);
        _scriptMetricsAccessor = scriptMetricsAccessor ?? (() => EmptyScriptMetricsSnapshot);
        _compatibilityMetricsAccessor = compatibilityMetricsAccessor ?? (() => new CompatibilityMetricsSnapshot(0, 0, 0, 0, 0, 0));
        _maxLogEntries = Math.Max(10, maxLogEntries);
        _privacyMode = privacyMode.ToString().ToLowerInvariant();
    }

    public void OnConnectionOpened() => Interlocked.Increment(ref _activeConnections);

    public void OnConnectionClosed() => Interlocked.Decrement(ref _activeConnections);

    public void OnWebSocketConnectionOpened()
    {
        Interlocked.Increment(ref _activeWebSocketConnections);
        Interlocked.Increment(ref _webSocketConnectionsTotal);
    }

    public void OnWebSocketConnectionClosed() => Interlocked.Decrement(ref _activeWebSocketConnections);

    public void OnWebSocketMessageReceived() => Interlocked.Increment(ref _webSocketMessagesReceivedTotal);

    public void OnWebSocketMessageSent() => Interlocked.Increment(ref _webSocketMessagesSentTotal);

    public void OnWebSocketError() => Interlocked.Increment(ref _webSocketErrorsTotal);

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
        builder.AppendLine("# HELP swarmkeydb_ws_active_connections Active WebSocket gateway connections.");
        builder.AppendLine("# TYPE swarmkeydb_ws_active_connections gauge");
        builder.AppendLine("# HELP swarmkeydb_ws_connections_total Total accepted WebSocket gateway connections.");
        builder.AppendLine("# TYPE swarmkeydb_ws_connections_total counter");
        builder.AppendLine("# HELP swarmkeydb_ws_messages_received_total Total WebSocket frames received by the gateway.");
        builder.AppendLine("# TYPE swarmkeydb_ws_messages_received_total counter");
        builder.AppendLine("# HELP swarmkeydb_ws_messages_sent_total Total WebSocket frames sent by the gateway.");
        builder.AppendLine("# TYPE swarmkeydb_ws_messages_sent_total counter");
        builder.AppendLine("# HELP swarmkeydb_ws_errors_total Total WebSocket gateway errors.");
        builder.AppendLine("# TYPE swarmkeydb_ws_errors_total counter");
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
        builder.AppendLine("# HELP swarmkeydb_transaction_started_total Total MULTI invocations.");
        builder.AppendLine("# TYPE swarmkeydb_transaction_started_total counter");
        builder.AppendLine("# HELP swarmkeydb_transaction_committed_total Total successful EXEC completions.");
        builder.AppendLine("# TYPE swarmkeydb_transaction_committed_total counter");
        builder.AppendLine("# HELP swarmkeydb_transaction_aborted_total Total transactions aborted by DISCARD or WATCH conflict.");
        builder.AppendLine("# TYPE swarmkeydb_transaction_aborted_total counter");
        builder.AppendLine("# HELP swarmkeydb_transaction_watch_conflict_total Total EXEC aborts caused by a WATCH key conflict.");
        builder.AppendLine("# TYPE swarmkeydb_transaction_watch_conflict_total counter");
        builder.AppendLine("# HELP swarmkeydb_transaction_queue_depth Transaction queue depth histogram.");
        builder.AppendLine("# TYPE swarmkeydb_transaction_queue_depth histogram");
        builder.AppendLine("# HELP swarmkeydb_transaction_exec_duration_seconds EXEC duration histogram.");
        builder.AppendLine("# TYPE swarmkeydb_transaction_exec_duration_seconds histogram");
        builder.AppendLine("# HELP swarmkeydb_stream_pending_entries_total Current pending stream entries.");
        builder.AppendLine("# TYPE swarmkeydb_stream_pending_entries_total gauge");
        builder.AppendLine("# HELP swarmkeydb_stream_xack_total Total acknowledged stream pending entries.");
        builder.AppendLine("# TYPE swarmkeydb_stream_xack_total counter");
        builder.AppendLine("# HELP swarmkeydb_stream_xclaim_total Total claimed stream pending entries.");
        builder.AppendLine("# TYPE swarmkeydb_stream_xclaim_total counter");
        builder.AppendLine("# HELP swarmkeydb_stream_group_count Total stream consumer groups.");
        builder.AppendLine("# TYPE swarmkeydb_stream_group_count gauge");
        builder.AppendLine("# HELP swarmkeydb_stream_idle_consumer_count Total idle stream consumers.");
        builder.AppendLine("# TYPE swarmkeydb_stream_idle_consumer_count gauge");
        builder.AppendLine("# HELP swarmkeydb_stream_blocked_readers Current blocked stream readers.");
        builder.AppendLine("# TYPE swarmkeydb_stream_blocked_readers gauge");
        builder.AppendLine("# HELP swarmkeydb_stream_blocked_readers_by_stream Current blocked stream readers per stream key.");
        builder.AppendLine("# TYPE swarmkeydb_stream_blocked_readers_by_stream gauge");
        builder.AppendLine("# HELP swarmkeydb_stream_xread_wakeup_total Total stream read wakeups.");
        builder.AppendLine("# TYPE swarmkeydb_stream_xread_wakeup_total counter");
        builder.AppendLine("# HELP swarmkeydb_stream_trimmed_total Total stream entries trimmed by retention policies.");
        builder.AppendLine("# TYPE swarmkeydb_stream_trimmed_total counter");
        builder.AppendLine("# HELP swarmkeydb_stream_length_bytes Current serialized stream payload size in bytes.");
        builder.AppendLine("# TYPE swarmkeydb_stream_length_bytes gauge");
        builder.AppendLine("# HELP swarmkeydb_script_eval_total Total EVAL invocations.");
        builder.AppendLine("# TYPE swarmkeydb_script_eval_total counter");
        builder.AppendLine("# HELP swarmkeydb_script_evalsha_total Total EVALSHA invocations.");
        builder.AppendLine("# TYPE swarmkeydb_script_evalsha_total counter");
        builder.AppendLine("# HELP swarmkeydb_script_error_total Total script errors (runtime + timeout).");
        builder.AppendLine("# TYPE swarmkeydb_script_error_total counter");
        builder.AppendLine("# HELP swarmkeydb_script_timeout_total Total scripts terminated by the CPU-time guard.");
        builder.AppendLine("# TYPE swarmkeydb_script_timeout_total counter");
        builder.AppendLine("# HELP swarmkeydb_script_replication_sent_total Total script replication events published.");
        builder.AppendLine("# TYPE swarmkeydb_script_replication_sent_total counter");
        builder.AppendLine("# HELP swarmkeydb_script_replication_received_total Total script replication events received from peer nodes.");
        builder.AppendLine("# TYPE swarmkeydb_script_replication_received_total counter");
        builder.AppendLine("# HELP swarmkeydb_script_cache_miss_recovered_total Total EVALSHA cache misses recovered from peer script replication.");
        builder.AppendLine("# TYPE swarmkeydb_script_cache_miss_recovered_total counter");
        builder.AppendLine("# HELP swarmkeydb_script_flush_propagated_total Total SCRIPT FLUSH events propagated to peer nodes.");
        builder.AppendLine("# TYPE swarmkeydb_script_flush_propagated_total counter");
        builder.AppendLine("# HELP swarmkeydb_script_cache_size Current number of cached scripts on this node.");
        builder.AppendLine("# TYPE swarmkeydb_script_cache_size gauge");
        builder.AppendLine("# HELP swarmkeydb_script_exec_duration_seconds Script execution duration histogram.");
        builder.AppendLine("# TYPE swarmkeydb_script_exec_duration_seconds histogram");
        builder.AppendLine("# HELP swarmkeydb_expiry_scan_duration_seconds Average adaptive expiry scan duration in seconds.");
        builder.AppendLine("# TYPE swarmkeydb_expiry_scan_duration_seconds gauge");
        builder.AppendLine("# HELP swarmkeydb_expiry_keys_deleted_total Total keys deleted by adaptive expiry scans.");
        builder.AppendLine("# TYPE swarmkeydb_expiry_keys_deleted_total counter");
        builder.AppendLine("# HELP swarmkeydb_expiry_budget_exceeded_total Total expiry scans that hit the cycle budget cap.");
        builder.AppendLine("# TYPE swarmkeydb_expiry_budget_exceeded_total counter");
        builder.AppendLine("# HELP swarmkeydb_memory_used_bytes Estimated memory used by key/value payloads.");
        builder.AppendLine("# TYPE swarmkeydb_memory_used_bytes gauge");
        builder.AppendLine("# HELP swarmkeydb_memory_limit_bytes Configured max memory limit in bytes (0 means unlimited).");
        builder.AppendLine("# TYPE swarmkeydb_memory_limit_bytes gauge");
        builder.AppendLine("# HELP swarmkeydb_eviction_total Total key evictions caused by maxmemory enforcement.");
        builder.AppendLine("# TYPE swarmkeydb_eviction_total counter");

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

        var compatibility = _compatibilityMetricsAccessor();
        builder.AppendLine(FormatMetric("swarmkeydb_expiry_scan_duration_seconds", compatibility.ExpiryScanDurationSeconds));
        builder.AppendLine(FormatMetric("swarmkeydb_expiry_keys_deleted_total", compatibility.ExpiryKeysDeletedTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_expiry_budget_exceeded_total", compatibility.ExpiryBudgetExceededTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_memory_used_bytes", compatibility.MemoryUsedBytes));
        builder.AppendLine(FormatMetric("swarmkeydb_memory_limit_bytes", compatibility.MemoryLimitBytes));
        builder.AppendLine(FormatMetric("swarmkeydb_eviction_total", compatibility.EvictionTotal));

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
        builder.AppendLine(FormatMetric("swarmkeydb_ws_active_connections", Interlocked.Read(ref _activeWebSocketConnections)));
        builder.AppendLine(FormatMetric("swarmkeydb_ws_connections_total", Interlocked.Read(ref _webSocketConnectionsTotal)));
        builder.AppendLine(FormatMetric("swarmkeydb_ws_messages_received_total", Interlocked.Read(ref _webSocketMessagesReceivedTotal)));
        builder.AppendLine(FormatMetric("swarmkeydb_ws_messages_sent_total", Interlocked.Read(ref _webSocketMessagesSentTotal)));
        builder.AppendLine(FormatMetric("swarmkeydb_ws_errors_total", Interlocked.Read(ref _webSocketErrorsTotal)));
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
        builder.AppendLine(FormatMetric("swarmkeydb_transaction_started_total", txMetrics.StartedTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_transaction_committed_total", txMetrics.CommittedTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_transaction_aborted_total", txMetrics.AbortedTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_transaction_watch_conflict_total", txMetrics.WatchConflictTotal));
        AppendHistogramMetrics(builder, "swarmkeydb_transaction_queue_depth", txMetrics.QueueDepth);
        AppendHistogramMetrics(builder, "swarmkeydb_transaction_exec_duration_seconds", txMetrics.ExecDuration);
        var streamMetrics = _streamMetricsAccessor();
        builder.AppendLine(FormatMetric("swarmkeydb_stream_pending_entries_total", streamMetrics.PendingEntriesTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_stream_xack_total", streamMetrics.XAckTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_stream_xclaim_total", streamMetrics.XClaimTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_stream_group_count", streamMetrics.GroupCount));
        builder.AppendLine(FormatMetric("swarmkeydb_stream_idle_consumer_count", streamMetrics.IdleConsumerCount));
        builder.AppendLine(FormatMetric("swarmkeydb_stream_blocked_readers", streamMetrics.BlockedReaders));
        builder.AppendLine(FormatMetric("swarmkeydb_stream_xread_wakeup_total", streamMetrics.XReadWakeupTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_stream_trimmed_total", streamMetrics.TrimmedTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_stream_length_bytes", streamMetrics.StreamLengthBytesTotal));
        foreach (var entry in streamMetrics.BlockedReadersByStream.OrderBy(static pair => pair.Key, StringComparer.Ordinal))
        {
            builder.AppendLine(FormatMetric("swarmkeydb_stream_blocked_readers_by_stream", entry.Value, ("stream", entry.Key)));
        }

        foreach (var entry in streamMetrics.StreamLengthBytesByStream.OrderBy(static pair => pair.Key, StringComparer.Ordinal))
        {
            builder.AppendLine(FormatMetric("swarmkeydb_stream_length_bytes", entry.Value, ("stream", entry.Key)));
        }

        var scriptMetrics = _scriptMetricsAccessor();
        builder.AppendLine(FormatMetric("swarmkeydb_script_eval_total", scriptMetrics.EvalTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_script_evalsha_total", scriptMetrics.EvalShaTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_script_error_total", scriptMetrics.ErrorTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_script_timeout_total", scriptMetrics.TimeoutTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_script_replication_sent_total", scriptMetrics.ReplicationSentTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_script_replication_received_total", scriptMetrics.ReplicationReceivedTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_script_cache_miss_recovered_total", scriptMetrics.CacheMissRecoveredTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_script_flush_propagated_total", scriptMetrics.FlushPropagatedTotal));
        builder.AppendLine(FormatMetric("swarmkeydb_script_cache_size", scriptMetrics.CacheSize));
        AppendScriptDurationHistogram(builder, "swarmkeydb_script_exec_duration_seconds", scriptMetrics.ExecDuration);

        return builder.ToString();
    }

    private static readonly TransactionMetricsSnapshot EmptyTransactionMetricsSnapshot = new(
        0,
        0,
        0,
        0,
        new TransactionHistogramSnapshot(
            RedisCommandProcessor.TransactionQueueDepthBucketUpperBounds,
            new long[RedisCommandProcessor.TransactionQueueDepthBucketUpperBounds.Length],
            0,
            0),
        new TransactionHistogramSnapshot(
            RedisCommandProcessor.TransactionExecDurationBucketUpperBounds,
            new long[RedisCommandProcessor.TransactionExecDurationBucketUpperBounds.Length],
            0,
            0));
    private static readonly StreamMetricsSnapshot EmptyStreamMetricsSnapshot = new(0, 0, 0, 0, 0, 0, 0, new Dictionary<string, long>(StringComparer.Ordinal), 0, 0, new Dictionary<string, long>(StringComparer.Ordinal));
    private static readonly ScriptMetricsSnapshot EmptyScriptMetricsSnapshot = new(
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        new ScriptDurationHistogramSnapshot(
            RedisCommandProcessor.ScriptExecDurationBucketUpperBounds,
            new long[RedisCommandProcessor.ScriptExecDurationBucketUpperBounds.Length],
            0,
            0));

    private void AppendHistogramMetrics(StringBuilder builder, string metricName, TransactionHistogramSnapshot histogram)
    {
        for (var i = 0; i < histogram.BucketUpperBounds.Count && i < histogram.BucketCounts.Count; i++)
        {
            builder.AppendLine(FormatMetric(
                $"{metricName}_bucket",
                histogram.BucketCounts[i],
                ("le", histogram.BucketUpperBounds[i].ToString(CultureInfo.InvariantCulture))));
        }

        builder.AppendLine(FormatMetric($"{metricName}_bucket", histogram.Count, ("le", "+Inf")));
        builder.AppendLine(FormatMetric($"{metricName}_sum", histogram.Sum));
        builder.AppendLine(FormatMetric($"{metricName}_count", histogram.Count));
    }

    private void AppendScriptDurationHistogram(StringBuilder builder, string metricName, ScriptDurationHistogramSnapshot histogram)
    {
        for (var i = 0; i < histogram.BucketUpperBounds.Count && i < histogram.BucketCounts.Count; i++)
        {
            builder.AppendLine(FormatMetric(
                $"{metricName}_bucket",
                histogram.BucketCounts[i],
                ("le", histogram.BucketUpperBounds[i].ToString(CultureInfo.InvariantCulture))));
        }

        builder.AppendLine(FormatMetric($"{metricName}_bucket", histogram.Count, ("le", "+Inf")));
        builder.AppendLine(FormatMetric($"{metricName}_sum", histogram.Sum));
        builder.AppendLine(FormatMetric($"{metricName}_count", histogram.Count));
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
