namespace SwarmKeyDb;

public sealed class InMemoryKeyIndex : IKeyIndex
{
    private readonly Dictionary<string, string> _references = new(StringComparer.Ordinal);

    public Task<string?> GetReferenceAsync(string key, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _references.TryGetValue(key, out var reference);
        return Task.FromResult(reference);
    }

    public Task SetReferenceAsync(string key, string reference, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _references[key] = reference;
        return Task.CompletedTask;
    }

    public Task<bool> RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_references.Remove(key));
    }

    public Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult<IReadOnlyList<string>>(_references.Keys.Order(StringComparer.Ordinal).ToArray());
    }
}
