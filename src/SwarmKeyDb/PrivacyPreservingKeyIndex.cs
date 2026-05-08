namespace SwarmKeyDb;

public sealed class PrivacyPreservingKeyIndex : IKeyIndex
{
    private readonly IKeyIndex _inner;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly Dictionary<string, ManifestEntry> _manifest = new(StringComparer.Ordinal);
    private IKeyPrivacyStrategy _strategy;

    public PrivacyPreservingKeyIndex(IKeyIndex inner, IKeyPrivacyStrategy strategy)
    {
        _inner = inner;
        _strategy = strategy;
    }

    public PrivacyMode PrivacyMode => _strategy.Mode;

    public async Task<string?> GetReferenceAsync(string key, CancellationToken cancellationToken = default)
    {
        var token = _strategy.DeriveToken(key);
        return await _inner.GetReferenceAsync(token, cancellationToken).ConfigureAwait(false);
    }

    public async Task SetReferenceAsync(string key, string reference, DateTimeOffset? expiresAt = null, CancellationToken cancellationToken = default)
    {
        var token = _strategy.DeriveToken(key);
        await _inner.SetReferenceAsync(token, reference, expiresAt, cancellationToken).ConfigureAwait(false);

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _manifest[key] = new ManifestEntry(token, expiresAt);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<bool> SetExpiryAsync(string key, DateTimeOffset expiresAt, CancellationToken cancellationToken = default)
    {
        var token = _strategy.DeriveToken(key);
        var updated = await _inner.SetExpiryAsync(token, expiresAt, cancellationToken).ConfigureAwait(false);
        if (!updated)
        {
            return false;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_manifest.TryGetValue(key, out var entry))
            {
                _manifest[key] = entry with { ExpiresAt = expiresAt };
            }
        }
        finally
        {
            _gate.Release();
        }

        return true;
    }

    public async Task<(bool Exists, DateTimeOffset? ExpiresAt)> GetExpiryAsync(string key, CancellationToken cancellationToken = default)
    {
        var token = _strategy.DeriveToken(key);
        return await _inner.GetExpiryAsync(token, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> RemoveExpiryAsync(string key, CancellationToken cancellationToken = default)
    {
        var token = _strategy.DeriveToken(key);
        var updated = await _inner.RemoveExpiryAsync(token, cancellationToken).ConfigureAwait(false);
        if (!updated)
        {
            return false;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_manifest.TryGetValue(key, out var entry))
            {
                _manifest[key] = entry with { ExpiresAt = null };
            }
        }
        finally
        {
            _gate.Release();
        }

        return true;
    }

    public async Task<bool> RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        var token = _strategy.DeriveToken(key);
        var removed = await _inner.RemoveAsync(token, cancellationToken).ConfigureAwait(false);

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _manifest.Remove(key);
        }
        finally
        {
            _gate.Release();
        }

        return removed;
    }

    public async Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            PruneExpiredUnsafe(DateTimeOffset.UtcNow);
            if (_strategy.Mode == PrivacyMode.None)
            {
                return _manifest.Keys.Order(StringComparer.Ordinal).ToArray();
            }

            if (_manifest.Count == 0)
            {
                var tokenKeys = await _inner.ListKeysAsync(cancellationToken).ConfigureAwait(false);
                if (tokenKeys.Count > 0)
                {
                    throw new PrivacyModeException(
                        "Privacy-preserving key scans require a local key manifest. Rebuild or migrate the local privacy index before running KEYS/SCAN/range queries.");
                }
            }

            return _manifest.Keys.Order(StringComparer.Ordinal).ToArray();
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task RebuildIndexAsync(CancellationToken cancellationToken = default)
    {
        await _inner.RebuildIndexAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<string>> GetKeysInRangeAsync(
        string? startKey,
        string? endKey,
        bool includeStart = true,
        bool includeEnd = true,
        CancellationToken cancellationToken = default)
    {
        var keys = await ListKeysAsync(cancellationToken).ConfigureAwait(false);
        return keys
            .Where(k => QueryScanHelpers.MatchesLowerBound(k, startKey, includeStart)
                     && QueryScanHelpers.MatchesUpperBound(k, endKey, includeEnd))
            .ToArray();
    }

    public async Task<int> RotateStrategyAsync(
        IKeyPrivacyStrategy newStrategy,
        bool dryRun = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(newStrategy);
        var migrated = 0;

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            PruneExpiredUnsafe(DateTimeOffset.UtcNow);
            foreach (var (key, entry) in _manifest.ToArray())
            {
                cancellationToken.ThrowIfCancellationRequested();
                var newToken = newStrategy.DeriveToken(key);
                if (string.Equals(newToken, entry.Token, StringComparison.Ordinal))
                {
                    continue;
                }

                var reference = await _inner.GetReferenceAsync(entry.Token, cancellationToken).ConfigureAwait(false);
                if (reference is null)
                {
                    continue;
                }

                migrated++;
                if (dryRun)
                {
                    continue;
                }

                await _inner.SetReferenceAsync(newToken, reference, entry.ExpiresAt, cancellationToken).ConfigureAwait(false);
                await _inner.RemoveAsync(entry.Token, cancellationToken).ConfigureAwait(false);
                _manifest[key] = entry with { Token = newToken };
            }

            if (!dryRun)
            {
                _strategy = newStrategy;
            }
        }
        finally
        {
            _gate.Release();
        }

        return migrated;
    }

    private void PruneExpiredUnsafe(DateTimeOffset now)
    {
        foreach (var key in _manifest
                     .Where(kvp => kvp.Value.ExpiresAt is { } expiresAt && expiresAt <= now)
                     .Select(kvp => kvp.Key)
                     .ToArray())
        {
            _manifest.Remove(key);
        }
    }

    private sealed record ManifestEntry(string Token, DateTimeOffset? ExpiresAt);
}
