namespace SwarmKeyDb;

/// <summary>
/// Ambient per-request DID context: the authenticated DID and (optionally) the proof
/// that established the authentication in the current call chain.
/// </summary>
public sealed class DidContext
{
    /// <param name="did">The authenticated DID string, e.g. <c>did:ethr:0x1234…</c>.</param>
    /// <param name="proof">Optional proof presented during authentication.</param>
    public DidContext(string did, DidProof? proof = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(did);
        Did = did;
        Proof = proof;
    }

    /// <summary>The authenticated DID string.</summary>
    public string Did { get; }

    /// <summary>The proof that authenticated this DID, or <see langword="null"/> if not required.</summary>
    public DidProof? Proof { get; }
}
