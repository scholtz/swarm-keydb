namespace SwarmKeyDb;

/// <summary>
/// Minimal representation of a resolved DID document following the W3C DID Core specification.
/// </summary>
public sealed class DidDocument
{
    /// <summary>The DID that this document represents, e.g. <c>did:ethr:0x1234…</c>.</summary>
    public string Did { get; init; } = string.Empty;

    /// <summary>Verification methods listed in the document (public keys / signing keys).</summary>
    public IReadOnlyList<DidVerificationMethod> VerificationMethods { get; init; } = [];

    /// <summary>Controller DID(s) allowed to update this document.</summary>
    public IReadOnlyList<string> Controllers { get; init; } = [];
}
