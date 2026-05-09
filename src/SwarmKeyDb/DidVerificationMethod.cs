namespace SwarmKeyDb;

/// <summary>
/// Represents a verification method entry in a DID document (e.g. an Ethereum secp256k1 key).
/// </summary>
public sealed class DidVerificationMethod
{
    /// <summary>Full DID-URL identifier for this verification method, e.g. <c>did:ethr:0x1234…#controller</c>.</summary>
    public string Id { get; init; } = string.Empty;

    /// <summary>Verification method type, e.g. <c>EcdsaSecp256k1RecoveryMethod2020</c>.</summary>
    public string Type { get; init; } = string.Empty;

    /// <summary>DID of the entity that controls this key.</summary>
    public string Controller { get; init; } = string.Empty;

    /// <summary>Ethereum address (lowercase, 0x-prefixed) associated with this key, if applicable.</summary>
    public string? BlockchainAccountId { get; init; }
}
