using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace SwarmKeyDb;

public sealed class CachingKeyValueStore : IKeyValueStore, ICacheStats, ICacheEviction, IBackendMetadataProvider
{
    private readonly IKeyValueStore _inner;
    private readonly IMemoryCache _cache;
    private readonly CacheOptions _options;
    private readonly ILogger<CachingKeyValueStore> _logger;
    private readonly object _lruLock = new();
    private readonly LinkedList<string> _lruKeys = new();
    private readonly Dictionary<string, LinkedListNode<string>> _lruNodes = new(StringComparer.Ordinal);
    private long _hits;
    private long _misses;
    private long _evictions;

    public CachingKeyValueStore(
        IKeyValueStore inner,
        IMemoryCache cache,
        IOptions<CacheOptions> options,
        ILogger<CachingKeyValueStore> logger)
    {
        _inner = inner;
        _cache = cache;
        _options = options.Value;
        _logger = logger;
    }

    public long Hits => Interlocked.Read(ref _hits);
    public long Misses => Interlocked.Read(ref _misses);
    public long Evictions => Interlocked.Read(ref _evictions);

    public async Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        await _inner.PutAsync(key, value, cancellationToken).ConfigureAwait(false);
        _cache.Remove(key);
        RemoveLru(key);
    }

    public async Task PutWithStrategyAsync(
        string key,
        ReadOnlyMemory<byte> value,
        IMergeStrategy mergeStrategy,
        CancellationToken cancellationToken = default)
    {
        await _inner.PutWithStrategyAsync(key, value, mergeStrategy, cancellationToken).ConfigureAwait(false);
        _cache.Remove(key);
        RemoveLru(key);
    }

    public async Task MergeAsync(string key, ReadOnlyMemory<byte> incomingValue, CancellationToken cancellationToken = default)
    {
        await _inner.MergeAsync(key, incomingValue, cancellationToken).ConfigureAwait(false);
        _cache.Remove(key);
        RemoveLru(key);
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
        _cache.Remove(key);
        RemoveLru(key);
        return deleted;
    }

    public Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default) =>
        _inner.ListKeysAsync(cancellationToken);

    public async Task<bool> SetTtlAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        var updated = await _inner.SetTtlAsync(key, ttl, cancellationToken).ConfigureAwait(false);
        _cache.Remove(key);
        RemoveLru(key);
        return updated;
    }

    public Task<(bool Exists, TimeSpan? Ttl)> GetTtlAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.GetTtlAsync(key, cancellationToken);

    public async Task<bool> RemoveTtlAsync(string key, CancellationToken cancellationToken = default)
    {
        var updated = await _inner.RemoveTtlAsync(key, cancellationToken).ConfigureAwait(false);
        _cache.Remove(key);
        RemoveLru(key);
        return updated;
    }

    public void EvictFromCache(string key)
    {
        _cache.Remove(key);
        RemoveLru(key);
        Interlocked.Increment(ref _evictions);
        _logger.LogDebug("Cache eviction by consistency verification for key '{Key}'.", key);
    }

    public Task<string?> GetBackendMetadataAsync(string key, CancellationToken cancellationToken = default) =>
        (_inner as IBackendMetadataProvider)?.GetBackendMetadataAsync(key, cancellationToken) ?? Task.FromResult<string?>(null);

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
}
