namespace SwarmKeyDb;

/// <summary>
/// Determines how decentralized identity (DID) authentication is applied to store operations.
/// </summary>
public enum DidAuthMode
{
    /// <summary>DID authentication is disabled. All operations proceed without DID checks.</summary>
    None = 0,

    /// <summary>
    /// Authenticate callers using a <c>did:ethr</c> DID backed by an Ethereum address.
    /// Callers must present a valid Ethereum personal-sign proof before performing store operations.
    /// </summary>
    EthrDid = 1
}
