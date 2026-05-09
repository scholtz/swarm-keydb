namespace SwarmKeyDb;

/// <summary>
/// Abstracts resolution, authentication, and permission checking for decentralized identities (DIDs).
/// Implement this interface to add support for a new DID method (e.g. <c>did:ethr</c>, <c>did:key</c>).
/// </summary>
public interface IDecentralizedIdentityProvider
{
    /// <summary>
    /// Resolves the given DID string to a <see cref="DidDocument"/>.
    /// Returns <see langword="null"/> when the DID cannot be resolved (e.g. invalid format or unknown method).
    /// </summary>
    /// <param name="did">A fully-qualified DID string, e.g. <c>did:ethr:0x1234…</c>.</param>
    /// <param name="cancellationToken">Propagates notification that the operation should be cancelled.</param>
    Task<DidDocument?> ResolveAsync(string did, CancellationToken cancellationToken = default);

    /// <summary>
    /// Authenticates the caller by verifying that the <paramref name="proof"/> was produced by the
    /// controller of <paramref name="did"/>.
    /// Returns <see langword="true"/> when the proof is valid; <see langword="false"/> otherwise.
    /// </summary>
    /// <param name="did">The DID being authenticated.</param>
    /// <param name="proof">The proof (challenge + signature) presented by the caller.</param>
    /// <param name="cancellationToken">Propagates notification that the operation should be cancelled.</param>
    Task<bool> AuthenticateAsync(string did, DidProof proof, CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks whether the authenticated <paramref name="did"/> is allowed to perform
    /// <paramref name="operation"/> on <paramref name="key"/>.
    /// Returns <see langword="true"/> when access is granted.
    /// </summary>
    /// <param name="did">The authenticated DID of the caller.</param>
    /// <param name="key">The store key being accessed.</param>
    /// <param name="operation">The operation being performed.</param>
    /// <param name="cancellationToken">Propagates notification that the operation should be cancelled.</param>
    Task<bool> CheckPermissionAsync(string did, string key, DidOperation operation, CancellationToken cancellationToken = default);
}
