namespace SwarmKeyDb;

public sealed class RestoreService
{
    private readonly BackupService _backupService;
    private readonly IKeyValueStore _store;
    private readonly IEncryptionKeyProvider? _keyProvider;

    public RestoreService(BackupService backupService, IKeyValueStore store, IEncryptionKeyProvider? keyProvider = null)
    {
        _backupService = backupService;
        _store = store;
        _keyProvider = keyProvider;
    }

    public async Task<RestoreResult> RestoreAsync(
        string swarmReference,
        string? key,
        IProgress<OperationProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        var snapshot = await _backupService.ReadSnapshotAsync(swarmReference, key, cancellationToken).ConfigureAwait(false);
        if (!string.IsNullOrWhiteSpace(key) && _keyProvider is not null)
        {
            _keyProvider.Update(new EncryptionOptions
            {
                Enabled = true,
                EthPrivateKeyHex = key.Trim()
            });
        }

        var restored = 0;
        for (var i = 0; i < snapshot.Entries.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var entry = snapshot.Entries[i];
            progress?.Report(new OperationProgress("restore", i + 1, snapshot.Entries.Count, entry.Key));
            if (entry.ExpiresAtUtc is { } expiresAt && expiresAt <= DateTimeOffset.UtcNow)
            {
                continue;
            }

            var value = Convert.FromBase64String(entry.ValueBase64);
            await _store.PutAsync(entry.Key, value, cancellationToken).ConfigureAwait(false);
            if (entry.ExpiresAtUtc is { } remainingExpiry)
            {
                var remaining = remainingExpiry - DateTimeOffset.UtcNow;
                if (remaining > TimeSpan.Zero)
                {
                    await _store.SetTtlAsync(entry.Key, remaining, cancellationToken).ConfigureAwait(false);
                }
            }

            restored++;
        }

        return new RestoreResult(restored);
    }
}
