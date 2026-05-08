namespace SwarmKeyDb;

public sealed class CrossChainOptions
{
    public bool Enabled { get; set; }
    public List<int> DefaultChainIds { get; set; } = [];
    public List<ChainAdapterOptions> Chains { get; set; } = [];
    public int MaxRetryAttempts { get; set; } = 5;
    public int RetryBaseDelaySeconds { get; set; } = 5;
    public int ReconcileIntervalSeconds { get; set; } = 5;
}

public sealed class ChainAdapterOptions
{
    public int ChainId { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? RpcUrl { get; set; }
    public string? BridgeContractAddress { get; set; }
}
