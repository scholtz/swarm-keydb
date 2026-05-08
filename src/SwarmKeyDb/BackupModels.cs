using System.Text.Json.Serialization;

namespace SwarmKeyDb;

public sealed record OperationProgress(string Operation, int Processed, int Total, string? Key = null);

public sealed record BackupResult(string Reference, int KeyCount);

public sealed record RestoreResult(int RestoredKeyCount);

public sealed record KeyRotationResult(string ManifestReference, int RotatedKeyCount, string BackupReference);

internal sealed record BackupSnapshot(
    [property: JsonPropertyName("version")] int Version,
    [property: JsonPropertyName("createdAtUtc")] DateTimeOffset CreatedAtUtc,
    [property: JsonPropertyName("entries")] IReadOnlyList<BackupSnapshotEntry> Entries);

internal sealed record BackupSnapshotEntry(
    [property: JsonPropertyName("key")] string Key,
    [property: JsonPropertyName("valueBase64")] string ValueBase64,
    [property: JsonPropertyName("expiresAtUtc")] DateTimeOffset? ExpiresAtUtc);

internal sealed record RotationManifest(
    [property: JsonPropertyName("version")] int Version,
    [property: JsonPropertyName("rotatedAtUtc")] DateTimeOffset RotatedAtUtc,
    [property: JsonPropertyName("keyCount")] int KeyCount,
    [property: JsonPropertyName("backupReference")] string BackupReference);
