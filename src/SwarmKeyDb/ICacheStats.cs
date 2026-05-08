namespace SwarmKeyDb;

public interface ICacheStats
{
    long Hits { get; }
    long Misses { get; }
    long Evictions { get; }
}
