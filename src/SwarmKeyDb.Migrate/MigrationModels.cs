using System.Text.Json;

namespace SwarmKeyDb.Migrate;

public sealed class MigrationOptions
{
    public required Uri SourceUri { get; init; }
    public required Uri DestinationUri { get; init; }
    public bool DryRun { get; init; }
    public string? Prefix { get; init; }
    public required string CheckpointPath { get; init; }
    public bool Validate { get; init; }
    public int ValidateSamplePercent { get; init; } = 5;
    public int ScanCount { get; init; } = 500;
    public bool EnablePrivacy { get; init; }
    public string? PrivacyKeyHex { get; init; }
}

public enum RedisDataType
{
    String,
    Hash,
    List,
    Set,
    SortedSet
}

public sealed class MigrationEntry
{
    public required string Key { get; init; }
    public required RedisDataType Type { get; init; }
    public required byte[] Payload { get; init; }
    public TimeSpan? Ttl { get; init; }
}

public sealed class DestinationValue
{
    public required byte[] Payload { get; init; }
    public TimeSpan? Ttl { get; init; }
}

public sealed class ScanBatch
{
    public required ulong NextCursor { get; init; }
    public required IReadOnlyList<string> Keys { get; init; }
}

public sealed class MigrationCheckpoint
{
    public ulong Cursor { get; init; }
    public ulong? PendingBatchNextCursor { get; init; }
    public List<string> PendingBatchKeys { get; init; } = [];
    public int PendingBatchIndex { get; init; }

    public static MigrationCheckpoint Start => new();
}

public sealed class MigrationProgress
{
    public long ScannedKeys { get; init; }
    public long MigratedKeys { get; init; }
    public long ErrorCount { get; init; }
    public long? EstimatedTotalKeys { get; init; }
    public TimeSpan Elapsed { get; init; }

    public double KeysPerSecond => Elapsed.TotalSeconds <= 0
        ? 0
        : MigratedKeys / Elapsed.TotalSeconds;

    public TimeSpan? EstimatedRemaining =>
        EstimatedTotalKeys is null || KeysPerSecond <= 0
            ? null
            : TimeSpan.FromSeconds(Math.Max(0, (EstimatedTotalKeys.Value - MigratedKeys) / KeysPerSecond));
}

public sealed class ValidationMismatch
{
    public required string Key { get; init; }
    public required string Reason { get; init; }
}

public sealed class MigrationResult
{
    public required MigrationProgress Progress { get; init; }
    public required IReadOnlyList<ValidationMismatch> ValidationMismatches { get; init; }
}

public interface IMigrationSource
{
    Task<long?> GetApproximateTotalKeysAsync(CancellationToken cancellationToken);
    Task<ScanBatch> ScanAsync(ulong cursor, string matchPattern, int count, CancellationToken cancellationToken);
    Task<MigrationEntry?> ReadEntryAsync(string key, CancellationToken cancellationToken);
}

public interface IMigrationDestination
{
    Task WriteEntryAsync(MigrationEntry entry, CancellationToken cancellationToken);
    Task<DestinationValue?> ReadValueAsync(string key, CancellationToken cancellationToken);
}

public interface IMigrationCheckpointStore
{
    Task<MigrationCheckpoint> LoadAsync(CancellationToken cancellationToken);
    Task SaveAsync(MigrationCheckpoint checkpoint, CancellationToken cancellationToken);
    Task DeleteAsync(CancellationToken cancellationToken);
}

public interface IMigrationReporter
{
    void ReportProgress(MigrationProgress progress);
    void ReportError(string key, Exception exception);
    void ReportSummary(MigrationResult result);
}

public static class RedisPayloadSerializer
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    public static byte[] SerializeHash(IReadOnlyList<(byte[] Field, byte[] Value)> entries)
    {
        var normalized = entries
            .OrderBy(static entry => Convert.ToBase64String(entry.Field), StringComparer.Ordinal)
            .Select(static entry => new HashItem(Convert.ToBase64String(entry.Field), Convert.ToBase64String(entry.Value)))
            .ToArray();
        return JsonSerializer.SerializeToUtf8Bytes(new HashPayload("hash", normalized), SerializerOptions);
    }

    public static byte[] SerializeList(IReadOnlyList<byte[]> values)
    {
        var normalized = values.Select(static value => Convert.ToBase64String(value)).ToArray();
        return JsonSerializer.SerializeToUtf8Bytes(new ListPayload("list", normalized), SerializerOptions);
    }

    public static byte[] SerializeSet(IReadOnlyList<byte[]> values)
    {
        var normalized = values
            .Select(static value => Convert.ToBase64String(value))
            .OrderBy(static value => value, StringComparer.Ordinal)
            .ToArray();
        return JsonSerializer.SerializeToUtf8Bytes(new SetPayload("set", normalized), SerializerOptions);
    }

    public static byte[] SerializeSortedSet(IReadOnlyList<(byte[] Member, double Score)> values)
    {
        var normalized = values
            .OrderBy(static value => value.Score)
            .ThenBy(static value => Convert.ToBase64String(value.Member), StringComparer.Ordinal)
            .Select(static value => new SortedSetItem(Convert.ToBase64String(value.Member), value.Score))
            .ToArray();
        return JsonSerializer.SerializeToUtf8Bytes(new SortedSetPayload("zset", normalized), SerializerOptions);
    }

    private sealed record HashPayload(string Type, HashItem[] Entries);
    private sealed record HashItem(string FieldBase64, string ValueBase64);
    private sealed record ListPayload(string Type, string[] ValuesBase64);
    private sealed record SetPayload(string Type, string[] ValuesBase64);
    private sealed record SortedSetPayload(string Type, SortedSetItem[] Entries);
    private sealed record SortedSetItem(string MemberBase64, double Score);
}
