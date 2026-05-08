namespace SwarmKeyDb;

public sealed class CacheOptions
{
    public bool Enabled { get; set; } = true;
    public int MaxEntries { get; set; } = 1_000;
    public TimeSpan? DefaultEntryTtl { get; set; }
}
