namespace SwarmKeyDb;

/// <summary>
/// An Ethereum personal-sign proof used to authenticate a DID controller.
/// The <see cref="Message"/> is the plain-text challenge that was signed;
/// the <see cref="Signature"/> is the resulting 65-byte hex-encoded ECDSA signature
/// (r || s || v, where v is 27 or 28 in legacy Ethereum format).
/// </summary>
public sealed class DidProof
{
    /// <param name="message">Plain-text message that was signed (the challenge).</param>
    /// <param name="signature">65-byte hex-encoded Ethereum personal-sign signature (0x-prefixed).</param>
    public DidProof(string message, string signature)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(message);
        ArgumentException.ThrowIfNullOrWhiteSpace(signature);
        Message = message;
        Signature = signature;
    }

    /// <summary>The plain-text challenge that was signed.</summary>
    public string Message { get; }

    /// <summary>65-byte hex-encoded ECDSA signature (0x-prefixed).</summary>
    public string Signature { get; }
}
