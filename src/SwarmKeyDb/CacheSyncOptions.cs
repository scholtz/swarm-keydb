namespace SwarmKeyDb;

public sealed class CacheSyncOptions
{
    public bool Enabled { get; set; }
    public string NodeId { get; set; } = $"{Environment.MachineName}:{Environment.ProcessId}";
    public IReadOnlyList<string> Peers { get; set; } = [];
    public int SyncIntervalSeconds { get; set; } = 5;
    public string Channel { get; set; } = "swarm-keydb-sync";
}
