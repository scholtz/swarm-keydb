namespace SwarmKeyDb;

public sealed class AsyncProcessingOptions
{
    public bool Enabled { get; set; } = false;
    public int MaxConcurrentWrites { get; set; } = 4;
    public int WriteBatchSize { get; set; } = 64;
    public int BatchFlushIntervalMs { get; set; } = 100;
}
