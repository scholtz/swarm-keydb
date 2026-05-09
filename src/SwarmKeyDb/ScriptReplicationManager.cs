using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace SwarmKeyDb;

public sealed class ScriptReplicationManager : IDisposable
{
    private const string EventPrefix = "__script__:";
    private const string ReplicatePrefix = EventPrefix + "replicate:";
    private const string FlushReason = EventPrefix + "flush";
    private const string FetchPrefix = EventPrefix + "fetch:";
    private const string ResyncPrefix = EventPrefix + "resync:";
    private static readonly TimeSpan DefaultRecoveryTimeout = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan RecoveryPollInterval = TimeSpan.FromMilliseconds(25);

    private readonly ScriptCache _scriptCache;
    private readonly ICacheSyncBus? _syncBus;
    private readonly ILogger<ScriptReplicationManager> _logger;
    private readonly string _nodeId;
    private readonly bool _enabled;
    private readonly IDisposable? _subscription;
    private long _sentTotal;
    private long _receivedTotal;
    private long _cacheMissRecoveredTotal;
    private long _flushPropagatedTotal;

    public ScriptReplicationManager(
        ScriptCache scriptCache,
        ICacheSyncBus? syncBus,
        CacheSyncOptions? syncOptions = null,
        ILogger<ScriptReplicationManager>? logger = null)
    {
        _scriptCache = scriptCache;
        _syncBus = syncBus;
        _logger = logger ?? NullLogger<ScriptReplicationManager>.Instance;
        _nodeId = syncOptions?.NodeId ?? $"{Environment.MachineName}:{Environment.ProcessId}";
        _enabled = syncOptions?.Enabled == true && syncBus is not null && !ReferenceEquals(syncBus, NoOpCacheSyncBus.Instance);
        if (_enabled)
        {
            _subscription = _syncBus! is ICacheSyncBusWithNodeSubscriptions withNodeSubscriptions
                ? withNodeSubscriptions.SubscribeInvalidations(_nodeId, HandleInvalidationAsync)
                : _syncBus.SubscribeInvalidations(HandleInvalidationAsync);
        }
    }

    public bool Enabled => _enabled;

    public Task PublishLoadedScriptAsync(string sha1, string script, CancellationToken cancellationToken = default) =>
        PublishScriptReplicationEventAsync(sha1, script, cancellationToken);

    public async Task PublishFlushAsync(CancellationToken cancellationToken = default)
    {
        if (!_enabled)
        {
            return;
        }

        Interlocked.Increment(ref _flushPropagatedTotal);
        await PublishEventAsync("*", FlushReason, cancellationToken).ConfigureAwait(false);
    }

    public async Task RequestStartupResyncAsync(CancellationToken cancellationToken = default)
    {
        if (!_enabled)
        {
            return;
        }

        await PublishEventAsync("*", ResyncPrefix + _nodeId, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> TryRecoverMissingScriptAsync(string sha1, CancellationToken cancellationToken = default)
    {
        if (!_enabled)
        {
            return false;
        }

        await PublishEventAsync(sha1, FetchPrefix + _nodeId, cancellationToken).ConfigureAwait(false);
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.Elapsed < DefaultRecoveryTimeout)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_scriptCache.Exists(sha1))
            {
                Interlocked.Increment(ref _cacheMissRecoveredTotal);
                return true;
            }

            await Task.Delay(RecoveryPollInterval, cancellationToken).ConfigureAwait(false);
        }

        return _scriptCache.Exists(sha1);
    }

    public ScriptReplicationMetricsSnapshot GetMetricsSnapshot() =>
        new(
            SentTotal: Interlocked.Read(ref _sentTotal),
            ReceivedTotal: Interlocked.Read(ref _receivedTotal),
            CacheMissRecoveredTotal: Interlocked.Read(ref _cacheMissRecoveredTotal),
            FlushPropagatedTotal: Interlocked.Read(ref _flushPropagatedTotal),
            CacheSize: _scriptCache.Count);

    public void Dispose()
    {
        _subscription?.Dispose();
    }

    private async Task PublishScriptReplicationEventAsync(string sha1, string script, CancellationToken cancellationToken)
    {
        if (!_enabled)
        {
            return;
        }

        var scriptBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(script));
        await PublishEventAsync(sha1, ReplicatePrefix + scriptBase64, cancellationToken).ConfigureAwait(false);
    }

    private async Task PublishEventAsync(string key, string reason, CancellationToken cancellationToken)
    {
        if (!_enabled || _syncBus is null)
        {
            return;
        }

        try
        {
            Interlocked.Increment(ref _sentTotal);
            await _syncBus.PublishInvalidationAsync(
                new CacheInvalidationEvent(
                    SourceNodeId: _nodeId,
                    Key: key,
                    VersionStamp: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    TimestampUtc: DateTimeOffset.UtcNow,
                    Reason: reason),
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Script replication publish failed for key '{Key}'.", key);
        }
    }

    private async Task HandleInvalidationAsync(CacheInvalidationEvent invalidation)
    {
        if (!_enabled || string.Equals(invalidation.SourceNodeId, _nodeId, StringComparison.Ordinal))
        {
            return;
        }

        var reason = invalidation.Reason;
        if (reason.StartsWith(ReplicatePrefix, StringComparison.Ordinal))
        {
            await HandleReplicatedScriptAsync(invalidation).ConfigureAwait(false);
            return;
        }

        if (string.Equals(reason, FlushReason, StringComparison.Ordinal))
        {
            _scriptCache.Flush();
            Interlocked.Increment(ref _receivedTotal);
            return;
        }

        if (reason.StartsWith(FetchPrefix, StringComparison.Ordinal))
        {
            await HandleFetchRequestAsync(invalidation).ConfigureAwait(false);
            return;
        }

        if (reason.StartsWith(ResyncPrefix, StringComparison.Ordinal))
        {
            await HandleResyncRequestAsync(invalidation).ConfigureAwait(false);
        }
    }

    private Task HandleReplicatedScriptAsync(CacheInvalidationEvent invalidation)
    {
        var base64 = invalidation.Reason[ReplicatePrefix.Length..];
        string script;
        try
        {
            script = Encoding.UTF8.GetString(Convert.FromBase64String(base64));
        }
        catch (FormatException)
        {
            _logger.LogWarning("Received malformed script replication payload from node {NodeId}.", invalidation.SourceNodeId);
            return Task.CompletedTask;
        }

        if (!_scriptCache.TryStoreReplicated(invalidation.Key, script))
        {
            _logger.LogWarning("Rejected script replication payload due to SHA mismatch for key '{Sha1}'.", invalidation.Key);
            return Task.CompletedTask;
        }

        Interlocked.Increment(ref _receivedTotal);
        return Task.CompletedTask;
    }

    private Task HandleFetchRequestAsync(CacheInvalidationEvent invalidation)
    {
        var requesterNodeId = invalidation.Reason[FetchPrefix.Length..];
        if (string.Equals(requesterNodeId, _nodeId, StringComparison.Ordinal))
        {
            return Task.CompletedTask;
        }

        var script = _scriptCache.Get(invalidation.Key);
        return script is null
            ? Task.CompletedTask
            : PublishScriptReplicationEventAsync(invalidation.Key, script, CancellationToken.None);
    }

    private async Task HandleResyncRequestAsync(CacheInvalidationEvent invalidation)
    {
        var requesterNodeId = invalidation.Reason[ResyncPrefix.Length..];
        if (string.Equals(requesterNodeId, _nodeId, StringComparison.Ordinal))
        {
            return;
        }

        foreach (var entry in _scriptCache.Snapshot())
        {
            await PublishScriptReplicationEventAsync(entry.Key, entry.Value, CancellationToken.None).ConfigureAwait(false);
        }
    }
}

public readonly record struct ScriptReplicationMetricsSnapshot(
    long SentTotal,
    long ReceivedTotal,
    long CacheMissRecoveredTotal,
    long FlushPropagatedTotal,
    int CacheSize);
