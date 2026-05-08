namespace SwarmKeyDb;

/// <summary>
/// Wraps an <see cref="IKeyValueStore"/> and automatically prepends a namespace prefix to every key.
/// All operations (get, put, delete, list, TTL) are transparently scoped to the prefix.
/// </summary>
/// <example>
/// <code>
/// var userDb = new NamespacedKeyValueStore(store, "users:alice:");
/// await userDb.PutAsync("profile", profileBytes);  // stored as "users:alice:profile"
/// var keys = await userDb.ListKeysAsync();           // returns ["profile"], not "users:alice:profile"
/// </code>
/// </example>
public sealed class NamespacedKeyValueStore : IKeyValueStore
{
    private readonly IKeyValueStore _inner;
    private readonly string _prefix;

    /// <summary>
    /// Creates a new <see cref="NamespacedKeyValueStore"/> that scopes all keys under <paramref name="prefix"/>.
    /// </summary>
    /// <param name="inner">The underlying store to delegate to.</param>
    /// <param name="prefix">
    /// The prefix to prepend to every key. Should typically end with the separator character
    /// (e.g., <c>"users:alice:"</c>) so that scoped keys remain distinct from exact-match keys.
    /// </param>
    public NamespacedKeyValueStore(IKeyValueStore inner, string prefix)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentException.ThrowIfNullOrEmpty(prefix);
        _inner = inner;
        _prefix = prefix;
    }

    /// <inheritdoc/>
    public Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        _inner.PutAsync(PrefixKey(key), value, cancellationToken);

    /// <inheritdoc/>
    public Task PutWithStrategyAsync(
        string key,
        ReadOnlyMemory<byte> value,
        IMergeStrategy mergeStrategy,
        CancellationToken cancellationToken = default) =>
        _inner.PutWithStrategyAsync(PrefixKey(key), value, mergeStrategy, cancellationToken);

    /// <inheritdoc/>
    public Task MergeAsync(string key, ReadOnlyMemory<byte> incomingValue, CancellationToken cancellationToken = default) =>
        _inner.MergeAsync(PrefixKey(key), incomingValue, cancellationToken);

    /// <inheritdoc/>
    public Task SetKeyOptionsAsync(string key, KeyOptions options, CancellationToken cancellationToken = default) =>
        _inner.SetKeyOptionsAsync(PrefixKey(key), options, cancellationToken);

    /// <inheritdoc/>
    public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.GetAsync(PrefixKey(key), cancellationToken);

    /// <inheritdoc/>
    public Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.DeleteAsync(PrefixKey(key), cancellationToken);

    /// <summary>
    /// Returns all keys within this namespace, with the namespace prefix stripped.
    /// </summary>
    public async Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default)
    {
        var prefixedKeys = await _inner.GetKeysWithPrefixAsync(_prefix, cancellationToken).ConfigureAwait(false);
        return prefixedKeys.Select(StripPrefix).ToArray();
    }

    /// <inheritdoc/>
    public Task<bool> SetTtlAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default) =>
        _inner.SetTtlAsync(PrefixKey(key), ttl, cancellationToken);

    /// <inheritdoc/>
    public Task<(bool Exists, TimeSpan? Ttl)> GetTtlAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.GetTtlAsync(PrefixKey(key), cancellationToken);

    /// <inheritdoc/>
    public Task<bool> RemoveTtlAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.RemoveTtlAsync(PrefixKey(key), cancellationToken);

    private string PrefixKey(string key) => _prefix + key;

    private string StripPrefix(string prefixedKey) => prefixedKey[_prefix.Length..];
}
