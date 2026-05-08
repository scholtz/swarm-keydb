namespace SwarmKeyDb;

public sealed class ShardingOptions
{
    public bool Enabled { get; init; }
    public int ShardCount { get; init; } = 1;
    public int VirtualNodesPerNode { get; init; } = 128;
    public IReadOnlyList<ShardNodeOptions> Nodes { get; init; } = [];

    public void Validate()
    {
        if (ShardCount is < 1 or > 64)
        {
            throw new ArgumentOutOfRangeException(nameof(ShardCount), "ShardCount must be between 1 and 64.");
        }

        if (VirtualNodesPerNode <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(VirtualNodesPerNode), "VirtualNodesPerNode must be greater than zero.");
        }

        if (!Enabled)
        {
            return;
        }

        if (Nodes.Count == 0)
        {
            throw new InvalidOperationException("Sharding is enabled but no shard nodes were configured.");
        }

        if (Nodes.Count > 64)
        {
            throw new InvalidOperationException("At most 64 shard nodes are supported.");
        }
    }
}

public sealed class ShardNodeOptions
{
    public string Name { get; init; } = string.Empty;
    public string? BeeUrl { get; init; }
    public string? PostageBatchId { get; init; }
    public string? DataDir { get; init; }
}
