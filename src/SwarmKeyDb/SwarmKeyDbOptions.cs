namespace SwarmKeyDb;

public sealed class SwarmKeyDbOptions
{
    public PrivacyMode PrivacyMode { get; set; } = PrivacyMode.ObliviousHashing;
    public string? PrivacyKeyHex { get; set; }

    /// <summary>
    /// Determines the DID authentication mode applied to store operations.
    /// Defaults to <see cref="DidAuthMode.None"/> (disabled) for backward compatibility.
    /// </summary>
    public DidAuthMode DidMode { get; set; } = DidAuthMode.None;

    /// <summary>
    /// Ethereum JSON-RPC endpoint used by <see cref="EthrDidProvider"/> for on-chain controller lookups.
    /// Required when <see cref="DidMode"/> is <see cref="DidAuthMode.EthrDid"/> and on-chain resolution is needed.
    /// </summary>
    public string? DidRpcUrl { get; set; }

    /// <summary>
    /// DID method string, e.g. <c>"ethr"</c>. Defaults to <c>"ethr"</c>.
    /// Reserved for future multi-method support.
    /// </summary>
    public string DidMethod { get; set; } = "ethr";
}
