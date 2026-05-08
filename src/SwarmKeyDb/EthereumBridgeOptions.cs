namespace SwarmKeyDb;

/// <summary>
/// Configuration for the Ethereum bridge that listens to smart contract events
/// and bridges them to SwarmKeyDb operations.
/// </summary>
public sealed class EthereumBridgeOptions
{
    /// <summary>
    /// Whether the Ethereum bridge is enabled. The server starts normally without any Ethereum
    /// dependency when this is false.
    /// Environment variable: ETH_BRIDGE_ENABLED
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Ethereum JSON-RPC endpoint URL.
    /// Use ws:// or wss:// for WebSocket subscriptions (real-time event streaming).
    /// Use http:// or https:// for HTTP polling mode (eth_getLogs).
    /// Environment variable: ETH_RPC_URL
    /// </summary>
    public string? RpcUrl { get; set; }

    /// <summary>
    /// Address of the deployed SwarmKeyDbOracle contract.
    /// Environment variable: ETH_CONTRACT_ADDRESS
    /// </summary>
    public string? ContractAddress { get; set; }

    /// <summary>
    /// Optional Ethereum private key as a 64-character hex string for signing write-back
    /// transactions (recording Swarm hashes on-chain after a write).
    /// Environment variable: ETH_PRIVATE_KEY
    /// </summary>
    public string? PrivateKeyHex { get; set; }

    /// <summary>
    /// Polling interval in seconds when using HTTP RPC mode.
    /// Default: 5 seconds.
    /// </summary>
    public int PollIntervalSeconds { get; set; } = 5;

    /// <summary>
    /// Delay in seconds before attempting to reconnect after a WebSocket or HTTP failure.
    /// Default: 5 seconds.
    /// </summary>
    public int ReconnectDelaySeconds { get; set; } = 5;
}
