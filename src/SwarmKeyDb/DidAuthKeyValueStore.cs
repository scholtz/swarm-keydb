namespace SwarmKeyDb;

/// <summary>
/// Decorator that enforces decentralized identity (DID) authentication before delegating
/// store operations to the wrapped <see cref="IKeyValueStore"/>.
///
/// <para>
/// When DID auth mode is <see cref="DidAuthMode.None"/> (the default), all operations pass through
/// unchanged to preserve backward compatibility.  Otherwise, each operation checks that:
/// </para>
/// <list type="number">
///   <item><description>A DID context is present on the current async call chain.</description></item>
///   <item><description>The DID has been authenticated by the configured <see cref="IDecentralizedIdentityProvider"/>.</description></item>
///   <item><description>The DID has permission for the requested operation on the requested key.</description></item>
/// </list>
/// <para>
/// A failed check throws <see cref="DidAuthorizationException"/>.
/// </para>
/// </summary>
public sealed class DidAuthKeyValueStore : IKeyValueStore
{
    private readonly IKeyValueStore _inner;
    private readonly IDecentralizedIdentityProvider _provider;
    private readonly IDidContextAccessor _contextAccessor;
    private readonly DidAuthMode _mode;

    /// <param name="inner">The wrapped store to delegate to after a successful auth check.</param>
    /// <param name="provider">DID provider used for resolution, authentication, and permission checks.</param>
    /// <param name="contextAccessor">Ambient accessor for the per-request DID context.</param>
    /// <param name="mode">The DID auth mode; when <see cref="DidAuthMode.None"/>, the decorator is a no-op pass-through.</param>
    public DidAuthKeyValueStore(
        IKeyValueStore inner,
        IDecentralizedIdentityProvider provider,
        IDidContextAccessor contextAccessor,
        DidAuthMode mode = DidAuthMode.EthrDid)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(contextAccessor);
        _inner = inner;
        _provider = provider;
        _contextAccessor = contextAccessor;
        _mode = mode;
    }

    /// <inheritdoc/>
    public async Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        await EnsureAuthorizedAsync(key, DidOperation.Write, cancellationToken).ConfigureAwait(false);
        await _inner.PutAsync(key, value, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task PutWithStrategyAsync(
        string key,
        ReadOnlyMemory<byte> value,
        IMergeStrategy mergeStrategy,
        CancellationToken cancellationToken = default)
    {
        await EnsureAuthorizedAsync(key, DidOperation.Write, cancellationToken).ConfigureAwait(false);
        await _inner.PutWithStrategyAsync(key, value, mergeStrategy, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task MergeAsync(string key, ReadOnlyMemory<byte> incomingValue, CancellationToken cancellationToken = default)
    {
        await EnsureAuthorizedAsync(key, DidOperation.Write, cancellationToken).ConfigureAwait(false);
        await _inner.MergeAsync(key, incomingValue, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task SetKeyOptionsAsync(string key, KeyOptions options, CancellationToken cancellationToken = default)
    {
        await EnsureAuthorizedAsync(key, DidOperation.Write, cancellationToken).ConfigureAwait(false);
        await _inner.SetKeyOptionsAsync(key, options, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        await EnsureAuthorizedAsync(key, DidOperation.Read, cancellationToken).ConfigureAwait(false);
        return await _inner.GetAsync(key, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        await EnsureAuthorizedAsync(key, DidOperation.Delete, cancellationToken).ConfigureAwait(false);
        return await _inner.DeleteAsync(key, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default)
    {
        await EnsureAuthorizedAsync(key: null, DidOperation.Read, cancellationToken).ConfigureAwait(false);
        return await _inner.ListKeysAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<bool> SetTtlAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        await EnsureAuthorizedAsync(key, DidOperation.Write, cancellationToken).ConfigureAwait(false);
        return await _inner.SetTtlAsync(key, ttl, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<(bool Exists, TimeSpan? Ttl)> GetTtlAsync(string key, CancellationToken cancellationToken = default)
    {
        await EnsureAuthorizedAsync(key, DidOperation.Read, cancellationToken).ConfigureAwait(false);
        return await _inner.GetTtlAsync(key, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<bool> RemoveTtlAsync(string key, CancellationToken cancellationToken = default)
    {
        await EnsureAuthorizedAsync(key, DidOperation.Write, cancellationToken).ConfigureAwait(false);
        return await _inner.RemoveTtlAsync(key, cancellationToken).ConfigureAwait(false);
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private async Task EnsureAuthorizedAsync(
        string? key,
        DidOperation operation,
        CancellationToken cancellationToken)
    {
        if (_mode == DidAuthMode.None)
        {
            return;
        }

        var context = _contextAccessor.Current;
        if (context is null || string.IsNullOrWhiteSpace(context.Did))
        {
            throw new DidAuthorizationException(
                "DID authorization required: no DID context is set. " +
                "Use the AUTHDID command (or set X-DID header) before performing store operations.");
        }

        // Re-authenticate when a proof is present on the context (e.g. freshly set per-request).
        if (context.Proof is not null)
        {
            var authenticated = await _provider
                .AuthenticateAsync(context.Did, context.Proof, cancellationToken)
                .ConfigureAwait(false);

            if (!authenticated)
            {
                throw new DidAuthorizationException(
                    $"DID authorization failed: the proof presented for '{context.Did}' is invalid.");
            }
        }

        var resolvedKey = key ?? string.Empty;
        var permitted = await _provider
            .CheckPermissionAsync(context.Did, resolvedKey, operation, cancellationToken)
            .ConfigureAwait(false);

        if (!permitted)
        {
            throw new DidAuthorizationException(
                $"DID authorization denied: '{context.Did}' does not have {operation.ToString().ToLowerInvariant()} permission on '{resolvedKey}'.");
        }
    }
}
