using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace SwarmKeyDb;

public sealed class CachingKeyValueStore : IKeyValueStore, ICacheStats, ICacheEviction, IBackendMetadataProvider
    , ICacheSyncParticipant
{
    private readonly IKeyValueStore _inner;
    private readonly IMemoryCache _cache;
    private readonly CacheOptions _options;
    private readonly ILogger<CachingKeyValueStore> _logger;
    private readonly ICacheSyncBus _syncBus;
    private readonly CacheSyncOptions _syncOptions;
    private readonly object _lruLock = new();
    private readonly object _versionLock = new();
    private readonly LinkedList<string> _lruKeys = new();
    private readonly Dictionary<string, LinkedListNode<string>> _lruNodes = new(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _versionStamps = new(StringComparer.Ordinal);
    private readonly IDisposable? _syncSubscription;
    private readonly IDisposable? _syncVersionRegistration;
    private long _hits;
    private long _misses;
    private long _evictions;
    private long _versionCounter;
    private long _pendingReconciliations;

    public CachingKeyValueStore(
        IKeyValueStore inner,
        IMemoryCache cache,
        IOptions<CacheOptions> options,
        ILogger<CachingKeyValueStore> logger,
        ICacheSyncBus? syncBus = null,
        IOptions<CacheSyncOptions>? syncOptions = null)
    {
        _inner = inner;
        _cache = cache;
        _options = options.Value;
        _logger = logger;
        _syncBus = syncBus ?? NoOpCacheSyncBus.Instance;
        _syncOptions = syncOptions?.Value ?? new CacheSyncOptions();
        if (_syncOptions.Enabled)
        {
            _syncSubscription = _syncBus is ICacheSyncBusWithNodeSubscriptions nodeSubscriptions
                ? nodeSubscriptions.SubscribeInvalidations(_syncOptions.NodeId, HandleInvalidationAsync)
                : _syncBus.SubscribeInvalidations(HandleInvalidationAsync);
            _syncVersionRegistration = (_syncBus as ICacheSyncPeerStateBus)?
                .RegisterVersionProvider(_syncOptions.NodeId, GetVersionStampsAsync);
        }
    }

    public long Hits => Interlocked.Read(ref _hits);
    public long Misses => Interlocked.Read(ref _misses);
    public long Evictions => Interlocked.Read(ref _evictions);
    public long PendingReconciliations => Interlocked.Read(ref _pendingReconciliations);

    public async Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        await _inner.PutAsync(key, value, cancellationToken).ConfigureAwait(false);
        EvictFromCacheCore(key, "write");
        await PublishInvalidationAsync(key, reason: "write", cancellationToken).ConfigureAwait(false);
    }

    public async Task PutWithStrategyAsync(
        string key,
        ReadOnlyMemory<byte> value,
        IMergeStrategy mergeStrategy,
        CancellationToken cancellationToken = default)
    {
        await _inner.PutWithStrategyAsync(key, value, mergeStrategy, cancellationToken).ConfigureAwait(false);
        EvictFromCacheCore(key, "write");
        await PublishInvalidationAsync(key, reason: "write", cancellationToken).ConfigureAwait(false);
    }

    public async Task MergeAsync(string key, ReadOnlyMemory<byte> incomingValue, CancellationToken cancellationToken = default)
    {
        await _inner.MergeAsync(key, incomingValue, cancellationToken).ConfigureAwait(false);
        EvictFromCacheCore(key, "write");
        await PublishInvalidationAsync(key, reason: "write", cancellationToken).ConfigureAwait(false);
    }

    public Task SetKeyOptionsAsync(string key, KeyOptions options, CancellationToken cancellationToken = default) =>
        _inner.SetKeyOptionsAsync(key, options, cancellationToken);

    public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        if (!_options.Enabled)
        {
            return await _inner.GetAsync(key, cancellationToken).ConfigureAwait(false);
        }

        if (_cache.TryGetValue(key, out byte[]? cached) && cached is not null)
        {
            if (_inner is IAccessControlVerifier verifier)
            {
                verifier.EnsureReadAccess();
            }

            Interlocked.Increment(ref _hits);
            TouchLru(key);
            _logger.LogDebug("Cache hit for key '{Key}'.", key);
            return cached.ToArray();
        }

        Interlocked.Increment(ref _misses);
        _logger.LogDebug("Cache miss for key '{Key}'.", key);

        var value = await _inner.GetAsync(key, cancellationToken).ConfigureAwait(false);
        if (value is null)
        {
            return null;
        }

        EnsureVersionStampExists(key);

        var ttl = await ResolveCacheEntryTtlAsync(key, cancellationToken).ConfigureAwait(false);
        if (ttl <= TimeSpan.Zero)
        {
            return value;
        }

        var evictedKey = AddOrRefreshLru(key);
        if (evictedKey is not null)
        {
            Interlocked.Increment(ref _evictions);
            _cache.Remove(evictedKey);
            _logger.LogDebug("Cache capacity eviction for key '{Key}'.", evictedKey);
        }

        using var entry = _cache.CreateEntry(key);
        entry.Value = value.ToArray();
        if (ttl is not null)
        {
            entry.AbsoluteExpirationRelativeToNow = ttl;
        }

        entry.RegisterPostEvictionCallback((evicted, _, reason, _) =>
        {
            if (reason is EvictionReason.Expired)
            {
                Interlocked.Increment(ref _evictions);
            }

            if (evicted is string evictedKey)
            {
                RemoveLru(evictedKey);
            }

            _logger.LogDebug("Cache eviction for reason {Reason}.", reason);
        });

        return value;
    }

    public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        var deleted = await _inner.DeleteAsync(key, cancellationToken).ConfigureAwait(false);
        EvictFromCacheCore(key, "delete");
        await PublishInvalidationAsync(key, reason: "delete", cancellationToken).ConfigureAwait(false);
        return deleted;
    }

    public Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default) =>
        _inner.ListKeysAsync(cancellationToken);

    public async Task<bool> SetTtlAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        var updated = await _inner.SetTtlAsync(key, ttl, cancellationToken).ConfigureAwait(false);
        EvictFromCacheCore(key, "ttl");
        if (updated)
        {
            await PublishInvalidationAsync(key, reason: "ttl", cancellationToken).ConfigureAwait(false);
        }

        return updated;
    }

    public Task<(bool Exists, TimeSpan? Ttl)> GetTtlAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.GetTtlAsync(key, cancellationToken);

    public async Task<bool> RemoveTtlAsync(string key, CancellationToken cancellationToken = default)
    {
        var updated = await _inner.RemoveTtlAsync(key, cancellationToken).ConfigureAwait(false);
        EvictFromCacheCore(key, "ttl");
        if (updated)
        {
            await PublishInvalidationAsync(key, reason: "ttl", cancellationToken).ConfigureAwait(false);
        }

        return updated;
    }

    public void EvictFromCache(string key)
    {
        EvictFromCacheCore(key, "consistency");
    }

    public Task<string?> GetBackendMetadataAsync(string key, CancellationToken cancellationToken = default) =>
        (_inner as IBackendMetadataProvider)?.GetBackendMetadataAsync(key, cancellationToken) ?? Task.FromResult<string?>(null);

    public Task<IReadOnlyDictionary<string, long>> GetVersionStampsAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_versionLock)
        {
            return Task.FromResult<IReadOnlyDictionary<string, long>>(
                _versionStamps.ToDictionary(static pair => pair.Key, static pair => pair.Value, StringComparer.Ordinal));
        }
    }

    public async Task ReconcileKeyAsync(string key, long versionStamp, CancellationToken cancellationToken = default)
    {
        if (versionStamp <= GetVersionStamp(key))
        {
            return;
        }

        Interlocked.Increment(ref _pendingReconciliations);
        try
        {
            EvictFromCacheCore(key, "anti-entropy");
            _ = await _inner.GetAsync(key, cancellationToken).ConfigureAwait(false);
            SetVersionStamp(key, versionStamp);
        }
        finally
        {
            Interlocked.Decrement(ref _pendingReconciliations);
        }
    }

    private async Task<TimeSpan?> ResolveCacheEntryTtlAsync(string key, CancellationToken cancellationToken)
    {
        var (_, keyTtl) = await _inner.GetTtlAsync(key, cancellationToken).ConfigureAwait(false);
        return (keyTtl, _options.DefaultEntryTtl) switch
        {
            (null, null) => null,
            ({ } ttl, null) => ttl,
            (null, { } configured) => configured,
            ({ } ttl, { } configured) => ttl < configured ? ttl : configured
        };
    }

    private string? AddOrRefreshLru(string key)
    {
        lock (_lruLock)
        {
            if (_lruNodes.TryGetValue(key, out var existing))
            {
                _lruKeys.Remove(existing);
                _lruKeys.AddLast(existing);
                return null;
            }

            string? evictedKey = null;
            var limit = Math.Max(CacheOptions.MinimumMaxEntries, _options.MaxEntries);
            if (_lruKeys.Count >= limit && _lruKeys.First is { } oldest)
            {
                evictedKey = oldest.Value;
                _lruKeys.RemoveFirst();
                _lruNodes.Remove(evictedKey);
            }

            var node = _lruKeys.AddLast(key);
            _lruNodes[key] = node;
            return evictedKey;
        }
    }

    private void TouchLru(string key)
    {
        lock (_lruLock)
        {
            if (!_lruNodes.TryGetValue(key, out var node))
            {
                return;
            }

            _lruKeys.Remove(node);
            _lruKeys.AddLast(node);
        }
    }

    private void RemoveLru(string key)
    {
        lock (_lruLock)
        {
            if (!_lruNodes.TryGetValue(key, out var node))
            {
                return;
            }

            _lruNodes.Remove(key);
            _lruKeys.Remove(node);
        }
    }

    private async Task PublishInvalidationAsync(string key, string reason, CancellationToken cancellationToken)
    {
        if (!_syncOptions.Enabled)
        {
            return;
        }

        var stamp = NextVersionStamp(key);
        try
        {
            await _syncBus.PublishInvalidationAsync(
                new CacheInvalidationEvent(
                    _syncOptions.NodeId,
                    key,
                    stamp,
                    DateTimeOffset.UtcNow,
                    reason),
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Cache invalidation publish failed for key '{Key}'. Continuing with local cache only.", key);
        }
    }

    private Task HandleInvalidationAsync(CacheInvalidationEvent invalidation)
    {
        if (!_syncOptions.Enabled || string.Equals(invalidation.SourceNodeId, _syncOptions.NodeId, StringComparison.Ordinal))
        {
            return Task.CompletedTask;
        }

        if (invalidation.VersionStamp <= GetVersionStamp(invalidation.Key))
        {
            return Task.CompletedTask;
        }

        SetVersionStamp(invalidation.Key, invalidation.VersionStamp);
        EvictFromCacheCore(invalidation.Key, "peer");
        return Task.CompletedTask;
    }

    private void EvictFromCacheCore(string key, string reason)
    {
        _cache.Remove(key);
        RemoveLru(key);
        Interlocked.Increment(ref _evictions);
        _logger.LogDebug("Cache eviction ({Reason}) for key '{Key}'.", reason, key);
    }

    private long NextVersionStamp(string key)
    {
        var stamp = Interlocked.Increment(ref _versionCounter);
        lock (_versionLock)
        {
            _versionStamps[key] = stamp;
        }

        return stamp;
    }

    private long GetVersionStamp(string key)
    {
        lock (_versionLock)
        {
            return _versionStamps.TryGetValue(key, out var stamp) ? stamp : 0;
        }
    }

    private void EnsureVersionStampExists(string key)
    {
        lock (_versionLock)
        {
            _versionStamps.TryAdd(key, 0);
        }
    }

    private void SetVersionStamp(string key, long stamp)
    {
        lock (_versionLock)
        {
            _versionStamps[key] = stamp;
        }

        long current;
        while ((current = Interlocked.Read(ref _versionCounter)) < stamp)
        {
            if (Interlocked.CompareExchange(ref _versionCounter, stamp, current) == current)
            {
                break;
            }
        }
    }
}
