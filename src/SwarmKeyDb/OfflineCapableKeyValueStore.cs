using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace SwarmKeyDb;

public sealed class OfflineCapableKeyValueStore : IOfflineKeyValueStore, ICacheStats, IBackendMetadataProvider, ICacheEviction, ICacheSyncParticipant
{
    private readonly IKeyValueStore _inner;
    private readonly IOfflineJournal _journal;
    private readonly IConnectivityProbe _connectivityProbe;
    private readonly SwarmKeyDbOptions _options;
    private readonly ILogger<OfflineCapableKeyValueStore> _logger;
    private readonly SemaphoreSlim _syncLock = new(1, 1);
    private readonly Dictionary<string, LocalCacheEntry> _cache = new(StringComparer.Ordinal);
    private readonly object _cacheLock = new();
    private long _queueDepth;
    private bool _isOffline;
    private DateTimeOffset? _lastSuccessfulSyncUtc;

    public OfflineCapableKeyValueStore(
        IKeyValueStore inner,
        IOfflineJournal journal,
        IConnectivityProbe connectivityProbe,
        SwarmKeyDbOptions options,
        ILogger<OfflineCapableKeyValueStore>? logger = null)
    {
        _inner = inner;
        _journal = journal;
        _connectivityProbe = connectivityProbe;
        _options = options;
        _logger = logger ?? NullLogger<OfflineCapableKeyValueStore>.Instance;
        _queueDepth = _journal.CountAsync().GetAwaiter().GetResult();
    }

    public long QueueDepth => Interlocked.Read(ref _queueDepth);
    public DateTimeOffset? LastSuccessfulSyncUtc => _lastSuccessfulSyncUtc;
    public bool IsOffline => Volatile.Read(ref _isOffline);
    public OfflineMode Mode => _options.OfflineMode;
    public long Hits => (_inner as ICacheStats)?.Hits ?? 0;
    public long Misses => (_inner as ICacheStats)?.Misses ?? 0;
    public long Evictions => (_inner as ICacheStats)?.Evictions ?? 0;
    public long PendingReconciliations => (_inner as ICacheSyncParticipant)?.PendingReconciliations ?? 0;

    public Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        PutWithResultAsync(key, value, cancellationToken);

    public async Task PutWithStrategyAsync(string key, ReadOnlyMemory<byte> value, IMergeStrategy mergeStrategy, CancellationToken cancellationToken = default)
    {
        if (ShouldPreferOfflineWrites())
        {
            await QueueAsync(OfflineOperationType.Put, key, value.ToArray(), pending: true, cancellationToken).ConfigureAwait(false);
            return;
        }

        try
        {
            await _inner.PutWithStrategyAsync(key, value, mergeStrategy, cancellationToken).ConfigureAwait(false);
            SetCache(key, value.ToArray(), pending: false);
            MarkOffline(false);
        }
        catch (Exception ex) when (ShouldQueueAfterFailure(ex, cancellationToken))
        {
            _logger.LogWarning(ex, "Queuing PUTWITHSTRATEGY for key '{Key}' because the backend is unreachable.", key);
            await QueueAsync(OfflineOperationType.Put, key, value.ToArray(), pending: true, cancellationToken).ConfigureAwait(false);
        }
    }

    public Task MergeAsync(string key, ReadOnlyMemory<byte> incomingValue, CancellationToken cancellationToken = default) =>
        PutAsync(key, incomingValue, cancellationToken);

    public Task SetKeyOptionsAsync(string key, KeyOptions options, CancellationToken cancellationToken = default) =>
        _inner.SetKeyOptionsAsync(key, options, cancellationToken);

    public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        var result = await GetWithResultAsync(key, cancellationToken).ConfigureAwait(false);
        return result.Value;
    }

    public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        if (ShouldPreferOfflineWrites())
        {
            await QueueAsync(OfflineOperationType.Delete, key, value: null, pending: true, cancellationToken).ConfigureAwait(false);
            return true;
        }

        try
        {
            var deleted = await _inner.DeleteAsync(key, cancellationToken).ConfigureAwait(false);
            SetCache(key, value: null, pending: false, isDeleted: true);
            MarkOffline(false);
            return deleted;
        }
        catch (Exception ex) when (ShouldQueueAfterFailure(ex, cancellationToken))
        {
            _logger.LogWarning(ex, "Queuing DELETE for key '{Key}' because the backend is unreachable.", key);
            await QueueAsync(OfflineOperationType.Delete, key, value: null, pending: true, cancellationToken).ConfigureAwait(false);
            return true;
        }
    }

    public async Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default)
    {
        IReadOnlyList<string> innerKeys;
        try
        {
            innerKeys = await _inner.ListKeysAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (IsConnectivityFailure(ex, cancellationToken))
        {
            MarkOffline(true);
            innerKeys = Array.Empty<string>();
        }

        lock (_cacheLock)
        {
            return innerKeys
                .Concat(_cache.Where(static pair => !pair.Value.IsDeleted).Select(static pair => pair.Key))
                .Distinct(StringComparer.Ordinal)
                .OrderBy(static key => key, StringComparer.Ordinal)
                .ToArray();
        }
    }

    public Task<bool> SetTtlAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default) =>
        _inner.SetTtlAsync(key, ttl, cancellationToken);

    public Task<(bool Exists, TimeSpan? Ttl)> GetTtlAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.GetTtlAsync(key, cancellationToken);

    public Task<bool> RemoveTtlAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.RemoveTtlAsync(key, cancellationToken);

    public async Task<OfflineWriteResult> PutWithResultAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        if (ShouldPreferOfflineWrites())
        {
            return await QueueAsync(OfflineOperationType.Put, key, value.ToArray(), pending: true, cancellationToken).ConfigureAwait(false);
        }

        try
        {
            await _inner.PutAsync(key, value, cancellationToken).ConfigureAwait(false);
            SetCache(key, value.ToArray(), pending: false);
            MarkOffline(false);
            return new OfflineWriteResult(false, QueueDepth);
        }
        catch (Exception ex) when (ShouldQueueAfterFailure(ex, cancellationToken))
        {
            _logger.LogWarning(ex, "Queuing PUT for key '{Key}' because the backend is unreachable.", key);
            return await QueueAsync(OfflineOperationType.Put, key, value.ToArray(), pending: true, cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task<OfflineWriteResult> DeleteWithResultAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        if (ShouldPreferOfflineWrites())
        {
            return await QueueAsync(OfflineOperationType.Delete, key, value: null, pending: true, cancellationToken).ConfigureAwait(false);
        }

        try
        {
            await _inner.DeleteAsync(key, cancellationToken).ConfigureAwait(false);
            SetCache(key, value: null, pending: false, isDeleted: true);
            MarkOffline(false);
            return new OfflineWriteResult(false, QueueDepth);
        }
        catch (Exception ex) when (ShouldQueueAfterFailure(ex, cancellationToken))
        {
            _logger.LogWarning(ex, "Queuing DELETE for key '{Key}' because the backend is unreachable.", key);
            return await QueueAsync(OfflineOperationType.Delete, key, value: null, pending: true, cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task<OfflineReadResult> GetWithResultAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        if (TryGetPendingLocalValue(key, out var local))
        {
            return local;
        }

        if (_options.OfflineMode == OfflineMode.Always)
        {
            return TryGetCachedResult(key, out var cachedAlways)
                ? cachedAlways
                : new OfflineReadResult(null, true, null);
        }

        try
        {
            var value = await _inner.GetAsync(key, cancellationToken).ConfigureAwait(false);
            if (value is null)
            {
                SetCache(key, value: null, pending: false, isDeleted: true);
                return new OfflineReadResult(null, false, null);
            }

            SetCache(key, value, pending: false);
            MarkOffline(false);
            return new OfflineReadResult(value, false, null);
        }
        catch (Exception ex) when (IsConnectivityFailure(ex, cancellationToken))
        {
            _logger.LogWarning(ex, "Serving cached GET for key '{Key}' because the backend is unreachable.", key);
            MarkOffline(true);
            if (TryGetCachedResult(key, out var cached))
            {
                return cached;
            }

            throw;
        }
    }

    public async Task<int> SyncPendingOperationsAsync(CancellationToken cancellationToken = default)
    {
        if (_options.OfflineMode == OfflineMode.Never)
        {
            return 0;
        }

        if (!await _connectivityProbe.IsConnectedAsync(cancellationToken).ConfigureAwait(false))
        {
            MarkOffline(true);
            return 0;
        }

        await _syncLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var processed = 0;
            while (true)
            {
                var entries = await _journal.ReadBatchAsync(limit: 64, cancellationToken).ConfigureAwait(false);
                if (entries.Count == 0)
                {
                    _lastSuccessfulSyncUtc = DateTimeOffset.UtcNow;
                    MarkOffline(false);
                    return processed;
                }

                foreach (var entry in entries)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    try
                    {
                        switch (entry.OperationType)
                        {
                            case OfflineOperationType.Put:
                            {
                                var localValue = entry.Value ?? Array.Empty<byte>();
                                var remoteValue = await _inner.GetAsync(entry.Key, cancellationToken).ConfigureAwait(false);
                                var resolved = ResolveConflict(entry.Key, localValue, remoteValue);
                                await _inner.PutAsync(entry.Key, resolved, cancellationToken).ConfigureAwait(false);
                                SetCache(entry.Key, resolved, pending: false);
                                break;
                            }
                            case OfflineOperationType.Delete:
                                await _inner.DeleteAsync(entry.Key, cancellationToken).ConfigureAwait(false);
                                SetCache(entry.Key, value: null, pending: false, isDeleted: true);
                                break;
                        }
                    }
                    catch (Exception ex) when (IsConnectivityFailure(ex, cancellationToken))
                    {
                        MarkOffline(true);
                        _logger.LogWarning(ex, "Stopping offline sync because the backend became unreachable again.");
                        return processed;
                    }

                    await _journal.RemoveThroughAsync(entry.Sequence, cancellationToken).ConfigureAwait(false);
                    Interlocked.Decrement(ref _queueDepth);
                    processed++;
                }
            }
        }
        finally
        {
            _syncLock.Release();
        }
    }

    private bool ShouldPreferOfflineWrites() => _options.OfflineMode switch
    {
        OfflineMode.Always => true,
        OfflineMode.Auto => !_connectivityProbe.IsConnectedAsync().GetAwaiter().GetResult(),
        _ => false
    };

    private bool ShouldQueueAfterFailure(Exception exception, CancellationToken cancellationToken) =>
        _options.OfflineMode != OfflineMode.Never && IsConnectivityFailure(exception, cancellationToken);

    private static bool IsConnectivityFailure(Exception exception, CancellationToken cancellationToken)
    {
        if (exception is OperationCanceledException && cancellationToken.IsCancellationRequested)
        {
            return false;
        }

        return exception is HttpRequestException
            or TaskCanceledException
            or IOException;
    }

    private async Task<OfflineWriteResult> QueueAsync(
        OfflineOperationType operationType,
        string key,
        byte[]? value,
        bool pending,
        CancellationToken cancellationToken)
    {
        await _journal.AppendAsync(operationType, key, value, cancellationToken).ConfigureAwait(false);
        var queueDepth = Interlocked.Increment(ref _queueDepth);
        SetCache(key, value, pending, isDeleted: operationType == OfflineOperationType.Delete);
        MarkOffline(true);
        return new OfflineWriteResult(true, queueDepth);
    }

    private byte[] ResolveConflict(string key, byte[] localValue, byte[]? remoteValue)
    {
        if (remoteValue is null || remoteValue.SequenceEqual(localValue))
        {
            return localValue;
        }

        if (_options.OnConflict is null)
        {
            return localValue;
        }

        return _options.OnConflict(new OfflineConflictContext(key, localValue, remoteValue)) ?? localValue;
    }

    private void SetCache(string key, byte[]? value, bool pending, bool isDeleted = false)
    {
        lock (_cacheLock)
        {
            _cache[key] = new LocalCacheEntry(value?.ToArray(), DateTimeOffset.UtcNow, pending, isDeleted);
        }
    }

    private LocalCacheEntry? GetCachedEntry(string key)
    {
        lock (_cacheLock)
        {
            return _cache.TryGetValue(key, out var entry) ? entry : null;
        }
    }

    private bool TryGetPendingLocalValue(string key, out OfflineReadResult result)
    {
        var entry = GetCachedEntry(key);
        if (entry is { PendingWrite: true })
        {
            result = new OfflineReadResult(entry.IsDeleted ? null : entry.Value?.ToArray(), true, entry.CachedAtUtc);
            return true;
        }

        result = default!;
        return false;
    }

    private bool TryGetCachedResult(string key, out OfflineReadResult result)
    {
        var entry = GetCachedEntry(key);
        if (entry is null)
        {
            result = default!;
            return false;
        }

        result = new OfflineReadResult(entry.IsDeleted ? null : entry.Value?.ToArray(), true, entry.CachedAtUtc);
        return true;
    }

    private void MarkOffline(bool isOffline) => Volatile.Write(ref _isOffline, isOffline);

    public Task<string?> GetBackendMetadataAsync(string key, CancellationToken cancellationToken = default) =>
        (_inner as IBackendMetadataProvider)?.GetBackendMetadataAsync(key, cancellationToken) ?? Task.FromResult<string?>(null);

    public Task<IReadOnlyDictionary<string, long>> GetVersionStampsAsync(CancellationToken cancellationToken = default) =>
        (_inner as ICacheSyncParticipant)?.GetVersionStampsAsync(cancellationToken)
        ?? Task.FromResult<IReadOnlyDictionary<string, long>>(new Dictionary<string, long>(StringComparer.Ordinal));

    public Task ReconcileKeyAsync(string key, long versionStamp, CancellationToken cancellationToken = default) =>
        (_inner as ICacheSyncParticipant)?.ReconcileKeyAsync(key, versionStamp, cancellationToken)
        ?? Task.CompletedTask;

    public void EvictFromCache(string key) => (_inner as ICacheEviction)?.EvictFromCache(key);

    private sealed record LocalCacheEntry(byte[]? Value, DateTimeOffset CachedAtUtc, bool PendingWrite, bool IsDeleted);
}
