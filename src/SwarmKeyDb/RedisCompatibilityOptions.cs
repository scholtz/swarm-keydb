namespace SwarmKeyDb;

public sealed class RedisCompatibilityOptions
{
    public static readonly string[] AllowedMaxMemoryPolicies =
    [
        "noeviction",
        "allkeys-lru",
        "volatile-lru",
        "allkeys-random",
        "volatile-random",
        "volatile-ttl"
    ];

    public int ExpiryBudgetMs { get; set; } = 25;
    public int Hz { get; set; } = 10;
    public long MaxMemoryBytes { get; set; } = 0;
    public string MaxMemoryPolicy { get; set; } = "noeviction";
}
