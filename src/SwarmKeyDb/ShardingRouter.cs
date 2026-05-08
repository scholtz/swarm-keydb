namespace SwarmKeyDb;

public sealed record ShardStore(string Name, IKeyValueStore Store);

public sealed class ShardingRouter : IKeyValueStore
{
    private readonly IReadOnlyList<ShardStore> _shards;
    private readonly Dictionary<string, IKeyValueStore> _storesByName;
    private readonly ConsistentHashRing _ring;
    private readonly int _shardCount;

    public ShardingRouter(
        IEnumerable<ShardStore> shards,
        int shardCount,
        int virtualNodesPerNode = 128)
    {
        ArgumentNullException.ThrowIfNull(shards);
        if (shardCount is < 1 or > 64)
        {
            throw new ArgumentOutOfRangeException(nameof(shardCount), "shardCount must be between 1 and 64.");
        }

        _shards = shards
            .Where(static shard => !string.IsNullOrWhiteSpace(shard.Name))
            .DistinctBy(static shard => shard.Name, StringComparer.Ordinal)
            .ToArray();
        if (_shards.Count == 0)
        {
            throw new ArgumentException("At least one shard must be provided.", nameof(shards));
        }

        _shardCount = shardCount;
        _storesByName = _shards.ToDictionary(static shard => shard.Name, static shard => shard.Store, StringComparer.Ordinal);
        _ring = new ConsistentHashRing(_shards.Select(static shard => shard.Name), virtualNodesPerNode);
    }

    public string ResolveShardName(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        // First map to a stable logical bucket (bounded by operator-configured shard count),
        // then map to a physical node via consistent hashing so node set changes move only part of keys.
        var keyHash = ConsistentHashRing.HashToUInt64(key);
        var bucket = JumpConsistentHash(keyHash, _shardCount);
        return _ring.GetNode(CombineHash(keyHash, bucket));
    }

    public Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        GetShardStore(key).PutAsync(key, value, cancellationToken);

    public Task PutWithStrategyAsync(
        string key,
        ReadOnlyMemory<byte> value,
        IMergeStrategy mergeStrategy,
        CancellationToken cancellationToken = default) =>
        GetShardStore(key).PutWithStrategyAsync(key, value, mergeStrategy, cancellationToken);

    public Task MergeAsync(string key, ReadOnlyMemory<byte> incomingValue, CancellationToken cancellationToken = default) =>
        GetShardStore(key).MergeAsync(key, incomingValue, cancellationToken);

    public Task SetKeyOptionsAsync(string key, KeyOptions options, CancellationToken cancellationToken = default) =>
        GetShardStore(key).SetKeyOptionsAsync(key, options, cancellationToken);

    public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default) =>
        GetShardStore(key).GetAsync(key, cancellationToken);

    public Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default) =>
        GetShardStore(key).DeleteAsync(key, cancellationToken);

    public async Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default)
    {
        var listTasks = _shards.Select(shard => shard.Store.ListKeysAsync(cancellationToken)).ToArray();
        var keysByShard = await Task.WhenAll(listTasks).ConfigureAwait(false);
        return keysByShard
            .SelectMany(static keys => keys)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static key => key, StringComparer.Ordinal)
            .ToArray();
    }

    public Task<bool> SetTtlAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default) =>
        GetShardStore(key).SetTtlAsync(key, ttl, cancellationToken);

    public Task<(bool Exists, TimeSpan? Ttl)> GetTtlAsync(string key, CancellationToken cancellationToken = default) =>
        GetShardStore(key).GetTtlAsync(key, cancellationToken);

    public Task<bool> RemoveTtlAsync(string key, CancellationToken cancellationToken = default) =>
        GetShardStore(key).RemoveTtlAsync(key, cancellationToken);

    private IKeyValueStore GetShardStore(string key)
    {
        var shardName = ResolveShardName(key);
        return _storesByName[shardName];
    }

    // https://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf
    private static int JumpConsistentHash(ulong key, int bucketCount)
    {
        long selected = -1;
        long candidate = 0;
        while (candidate < bucketCount)
        {
            selected = candidate;
            key = unchecked(key * 2862933555777941757UL + 1);
            candidate = (long)((selected + 1) * (double)(1L << 31) / ((key >> 33) + 1));
        }

        return (int)selected;
    }

    private static ulong CombineHash(ulong keyHash, int bucket)
    {
        var mixed = keyHash ^ (unchecked((ulong)bucket) * 0x9E3779B97F4A7C15UL);
        mixed ^= mixed >> 33;
        mixed *= 0xFF51AFD7ED558CCDUL;
        mixed ^= mixed >> 33;
        return mixed;
    }
}
