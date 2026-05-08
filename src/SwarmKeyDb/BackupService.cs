using System.Text.Json;

namespace SwarmKeyDb;

public sealed class BackupService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };
    private static readonly byte[] SnapshotMagic = [0x53, 0x4B, 0x42, 0x31];

    private readonly IKeyValueStore _store;
    private readonly ISwarmClient _swarmClient;
    private readonly IEncryptionKeyProvider? _keyProvider;

    public BackupService(IKeyValueStore store, ISwarmClient swarmClient, IEncryptionKeyProvider? keyProvider = null)
    {
        _store = store;
        _swarmClient = swarmClient;
        _keyProvider = keyProvider;
    }

    public async Task<BackupResult> BackupAsync(IProgress<OperationProgress>? progress = null, CancellationToken cancellationToken = default)
    {
        var keys = await _store.ListKeysAsync(cancellationToken).ConfigureAwait(false);
        var createdAt = DateTimeOffset.UtcNow;
        var entries = new List<BackupSnapshotEntry>(keys.Count);

        for (var i = 0; i < keys.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var key = keys[i];
            var value = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
            progress?.Report(new OperationProgress("backup", i + 1, keys.Count, key));
            if (value is null)
            {
                continue;
            }

            var ttl = await _store.GetTtlAsync(key, cancellationToken).ConfigureAwait(false);
            var expiresAt = ttl.Exists && ttl.Ttl is { } ttlValue && ttlValue > TimeSpan.Zero
                ? createdAt.Add(ttlValue)
                : null;
            entries.Add(new BackupSnapshotEntry(key, Convert.ToBase64String(value), expiresAt));
        }

        var snapshot = new BackupSnapshot(1, createdAt, entries);
        var payload = JsonSerializer.SerializeToUtf8Bytes(snapshot, JsonOptions);
        var protectedPayload = ProtectIfNeeded(payload);
        var reference = await _swarmClient.UploadAsync(protectedPayload, cancellationToken).ConfigureAwait(false);
        return new BackupResult(ToSwarmUri(reference), entries.Count);
    }

    internal async Task<BackupSnapshot> ReadSnapshotAsync(string swarmReference, string? key, CancellationToken cancellationToken)
    {
        var reference = NormalizeReference(swarmReference);
        var payload = await _swarmClient.DownloadAsync(reference, cancellationToken).ConfigureAwait(false);
        payload = UnprotectIfNeeded(payload, key);
        var snapshot = JsonSerializer.Deserialize<BackupSnapshot>(payload, JsonOptions)
            ?? throw new InvalidOperationException("Backup snapshot is invalid or corrupted.");
        if (snapshot.Version != 1)
        {
            throw new InvalidOperationException($"Backup snapshot version '{snapshot.Version}' is not supported.");
        }

        return snapshot;
    }

    private byte[] ProtectIfNeeded(byte[] payload)
    {
        var key = _keyProvider?.GetCurrentKey();
        return key is null ? payload : AesGcmEnvelope.Encrypt(payload, key, SnapshotMagic);
    }

    private static byte[] UnprotectIfNeeded(byte[] payload, string? key)
    {
        if (!AesGcmEnvelope.HasMagic(payload, SnapshotMagic))
        {
            return payload;
        }

        if (string.IsNullOrWhiteSpace(key))
        {
            throw new InvalidOperationException("Backup snapshot is encrypted; provide the matching Ethereum private key.");
        }

        return AesGcmEnvelope.Decrypt(payload, EncryptionKeyMaterial.ResolveManagementKey(key), SnapshotMagic);
    }

    internal static string NormalizeReference(string swarmReference)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(swarmReference);
        return swarmReference.StartsWith("swarm://", StringComparison.OrdinalIgnoreCase)
            ? swarmReference["swarm://".Length..]
            : swarmReference;
    }

    internal static string ToSwarmUri(string reference) => $"swarm://{reference}";
}
