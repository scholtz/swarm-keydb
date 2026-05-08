using StackExchange.Redis;

namespace SwarmKeyDb.Migrate;

public sealed class RedisMigrationSource : IMigrationSource
{
    private readonly IDatabase _database;

    public RedisMigrationSource(IDatabase database)
    {
        _database = database;
    }

    public async Task<long?> GetApproximateTotalKeysAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var result = await _database.ExecuteAsync("DBSIZE").ConfigureAwait(false);
        return (long?)result;
    }

    public async Task<ScanBatch> ScanAsync(ulong cursor, string matchPattern, int count, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var result = await _database.ExecuteAsync(
                "SCAN",
                cursor.ToString(),
                "MATCH",
                matchPattern,
                "COUNT",
                count.ToString())
            .ConfigureAwait(false);

        var values = (RedisResult[]?)result ?? [];
        var nextCursor = values.Length > 0 ? ulong.Parse(values[0].ToString() ?? "0") : 0;
        var keyResults = values.Length > 1 ? (RedisResult[]?)values[1] ?? [] : [];
        var keys = keyResults.Select(static value => value.ToString() ?? string.Empty)
            .Where(static value => !string.IsNullOrWhiteSpace(value))
            .ToArray();

        return new ScanBatch
        {
            NextCursor = nextCursor,
            Keys = keys
        };
    }

    public async Task<MigrationEntry?> ReadEntryAsync(string key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var redisKey = (RedisKey)key;
        var type = await _database.KeyTypeAsync(redisKey).ConfigureAwait(false);
        if (type == RedisType.None)
        {
            return null;
        }

        var ttl = await _database.KeyTimeToLiveAsync(redisKey).ConfigureAwait(false);
        var normalizedTtl = ttl is { } parsedTtl && parsedTtl > TimeSpan.Zero ? (TimeSpan?)parsedTtl : null;

        return type switch
        {
            RedisType.String => await ReadStringEntryAsync(redisKey, normalizedTtl).ConfigureAwait(false),
            RedisType.Hash => await ReadHashEntryAsync(redisKey, normalizedTtl).ConfigureAwait(false),
            RedisType.List => await ReadListEntryAsync(redisKey, normalizedTtl).ConfigureAwait(false),
            RedisType.Set => await ReadSetEntryAsync(redisKey, normalizedTtl).ConfigureAwait(false),
            RedisType.SortedSet => await ReadSortedSetEntryAsync(redisKey, normalizedTtl).ConfigureAwait(false),
            _ => throw new NotSupportedException($"Redis type '{type}' is not supported for key '{key}'.")
        };
    }

    private async Task<MigrationEntry?> ReadStringEntryAsync(RedisKey key, TimeSpan? ttl)
    {
        var value = await _database.StringGetAsync(key).ConfigureAwait(false);
        if (value.IsNull)
        {
            return null;
        }

        var payload = (byte[]?)value;
        return new MigrationEntry
        {
            Key = key!,
            Type = RedisDataType.String,
            Payload = payload ?? [],
            Ttl = ttl
        };
    }

    private async Task<MigrationEntry> ReadHashEntryAsync(RedisKey key, TimeSpan? ttl)
    {
        var values = await _database.HashGetAllAsync(key).ConfigureAwait(false);
        var payload = RedisPayloadSerializer.SerializeHash(
            values.Select(static item => ((byte[]?)item.Name ?? [], (byte[]?)item.Value ?? [])).ToArray());

        return new MigrationEntry
        {
            Key = key!,
            Type = RedisDataType.Hash,
            Payload = payload,
            Ttl = ttl
        };
    }

    private async Task<MigrationEntry> ReadListEntryAsync(RedisKey key, TimeSpan? ttl)
    {
        var values = await _database.ListRangeAsync(key).ConfigureAwait(false);
        var payload = RedisPayloadSerializer.SerializeList(values.Select(static value => (byte[]?)value ?? []).ToArray());

        return new MigrationEntry
        {
            Key = key!,
            Type = RedisDataType.List,
            Payload = payload,
            Ttl = ttl
        };
    }

    private async Task<MigrationEntry> ReadSetEntryAsync(RedisKey key, TimeSpan? ttl)
    {
        var values = await _database.SetMembersAsync(key).ConfigureAwait(false);
        var payload = RedisPayloadSerializer.SerializeSet(values.Select(static value => (byte[]?)value ?? []).ToArray());

        return new MigrationEntry
        {
            Key = key!,
            Type = RedisDataType.Set,
            Payload = payload,
            Ttl = ttl
        };
    }

    private async Task<MigrationEntry> ReadSortedSetEntryAsync(RedisKey key, TimeSpan? ttl)
    {
        var values = await _database.SortedSetRangeByRankWithScoresAsync(key).ConfigureAwait(false);
        var payload = RedisPayloadSerializer.SerializeSortedSet(values.Select(static value => ((byte[]?)value.Element ?? [], value.Score)).ToArray());

        return new MigrationEntry
        {
            Key = key!,
            Type = RedisDataType.SortedSet,
            Payload = payload,
            Ttl = ttl
        };
    }
}

public sealed class RedisMigrationDestination : IMigrationDestination
{
    private readonly IDatabase _database;

    public RedisMigrationDestination(IDatabase database)
    {
        _database = database;
    }

    public Task WriteEntryAsync(MigrationEntry entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return _database.StringSetAsync((RedisKey)entry.Key, entry.Payload, entry.Ttl);
    }

    public async Task<DestinationValue?> ReadValueAsync(string key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var redisKey = (RedisKey)key;
        var value = await _database.StringGetAsync(redisKey).ConfigureAwait(false);
        if (value.IsNull)
        {
            return null;
        }

        var ttl = await _database.KeyTimeToLiveAsync(redisKey).ConfigureAwait(false);
        var payload = (byte[]?)value;
        var normalizedTtl = ttl is { } parsedTtl && parsedTtl > TimeSpan.Zero ? (TimeSpan?)parsedTtl : null;
        return new DestinationValue
        {
            Payload = payload ?? [],
            Ttl = normalizedTtl
        };
    }
}
