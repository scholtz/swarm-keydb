namespace SwarmKeyDb;

/// <summary>
/// Provides ambient access to the current per-request DID context, analogous to
/// <see cref="IEthAddressAccessor"/> for raw Ethereum addresses.
/// </summary>
public interface IDidContextAccessor
{
    /// <summary>Gets or sets the DID context for the current asynchronous call chain.</summary>
    DidContext? Current { get; set; }
}
