namespace SwarmKeyDb;

/// <summary>
/// Configuration options for <see cref="EthrDidProvider"/>.
/// </summary>
public sealed class EthrDidProviderOptions
{
    /// <summary>
    /// Optional Ethereum JSON-RPC endpoint used for on-chain DID controller lookups.
    /// When <see langword="null"/> or empty, on-chain resolution is skipped and the address
    /// embedded in the DID string is used directly.
    /// </summary>
    public string? RpcUrl { get; set; }

    /// <summary>
    /// EIP-155 chain ID used to disambiguate multi-chain DIDs (e.g. <c>did:ethr:5:0x…</c>).
    /// Leave as <see langword="null"/> to accept DIDs on any chain.
    /// </summary>
    public string? ChainId { get; set; }
}
