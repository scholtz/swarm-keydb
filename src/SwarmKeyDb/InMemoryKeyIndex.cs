namespace SwarmKeyDb;

public sealed class InMemoryKeyIndex : IKeyIndex
{
    private readonly Dictionary<string, KeyIndexEntry> _references = new(StringComparer.Ordinal);

    public Task<string?> GetReferenceAsync(string key, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(TryGetActiveEntry(key, DateTimeOffset.UtcNow, out var entry) ? entry.Reference : null);
    }

    public Task SetReferenceAsync(string key, string reference, DateTimeOffset? expiresAt = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _references[key] = new KeyIndexEntry(reference, expiresAt);
        return Task.CompletedTask;
    }

    public Task<bool> SetExpiryAsync(string key, DateTimeOffset expiresAt, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!TryGetActiveEntry(key, DateTimeOffset.UtcNow, out var entry))
        {
            return Task.FromResult(false);
        }

        _references[key] = entry with { ExpiresAt = expiresAt };
        return Task.FromResult(true);
    }

    public Task<(bool Exists, DateTimeOffset? ExpiresAt)> GetExpiryAsync(string key, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!TryGetActiveEntry(key, DateTimeOffset.UtcNow, out var entry))
        {
            return Task.FromResult((false, (DateTimeOffset?)null));
        }

        return Task.FromResult((true, entry.ExpiresAt));
    }

    public Task<bool> RemoveExpiryAsync(string key, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!TryGetActiveEntry(key, DateTimeOffset.UtcNow, out var entry) || entry.ExpiresAt is null)
        {
            return Task.FromResult(false);
        }

        _references[key] = entry with { ExpiresAt = null };
        return Task.FromResult(true);
    }

    public Task<bool> RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_references.Remove(key));
    }

    public Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        RemoveExpiredEntries(DateTimeOffset.UtcNow);
        return Task.FromResult<IReadOnlyList<string>>(_references.Keys.Order(StringComparer.Ordinal).ToArray());
    }

    private bool TryGetActiveEntry(string key, DateTimeOffset now, out KeyIndexEntry entry)
    {
        if (!_references.TryGetValue(key, out var value))
        {
            entry = default;
            return false;
        }

        entry = value;
        if (entry.ExpiresAt is { } expiresAt && expiresAt <= now)
        {
            _references.Remove(key);
            return false;
        }

        return true;
    }

    private void RemoveExpiredEntries(DateTimeOffset now)
    {
        foreach (var key in _references
                     .Where(pair => pair.Value.ExpiresAt is { } expiresAt && expiresAt <= now)
                     .Select(pair => pair.Key)
                     .ToArray())
        {
            _references.Remove(key);
        }
    }

    private readonly record struct KeyIndexEntry(string Reference, DateTimeOffset? ExpiresAt);
}
