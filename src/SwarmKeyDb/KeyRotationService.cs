using System.Text.Json;

namespace SwarmKeyDb;

public sealed class KeyRotationService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private readonly IKeyValueStore _store;
    private readonly ISwarmClient _swarmClient;
    private readonly IEncryptionKeyProvider _keyProvider;
    private readonly BackupService _backupService;
    private readonly SemaphoreSlim _gate = new(1, 1);

    public KeyRotationService(IKeyValueStore store, ISwarmClient swarmClient, IEncryptionKeyProvider keyProvider, BackupService backupService)
    {
        _store = store;
        _swarmClient = swarmClient;
        _keyProvider = keyProvider;
        _backupService = backupService;
    }

    public async Task<KeyRotationResult> RotateAsync(
        string oldKey,
        string newKey,
        IProgress<OperationProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(oldKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(newKey);

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var previousOptions = _keyProvider.GetOptions();
            _keyProvider.Update(new EncryptionOptions
            {
                Enabled = true,
                EthPrivateKeyHex = oldKey.Trim()
            });

            var keys = await _store.ListKeysAsync(cancellationToken).ConfigureAwait(false);
            var stagedEntries = new List<(string Key, byte[] Value, TimeSpan? Ttl)>(keys.Count);
            try
            {
                for (var i = 0; i < keys.Count; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var key = keys[i];
                    var value = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false)
                        ?? throw new InvalidOperationException($"Key '{key}' could not be read during rotation.");
                    var ttl = await _store.GetTtlAsync(key, cancellationToken).ConfigureAwait(false);
                    stagedEntries.Add((key, value, ttl.Exists ? ttl.Ttl : null));
                    progress?.Report(new OperationProgress("rotate-read", i + 1, keys.Count, key));
                }

                _keyProvider.Update(new EncryptionOptions
                {
                    Enabled = true,
                    EthPrivateKeyHex = newKey.Trim()
                });

                for (var i = 0; i < stagedEntries.Count; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var entry = stagedEntries[i];
                    await _store.PutAsync(entry.Key, entry.Value, cancellationToken).ConfigureAwait(false);
                    if (entry.Ttl is { } ttl && ttl > TimeSpan.Zero)
                    {
                        await _store.SetTtlAsync(entry.Key, ttl, cancellationToken).ConfigureAwait(false);
                    }

                    progress?.Report(new OperationProgress("rotate-write", i + 1, stagedEntries.Count, entry.Key));
                }

                var backup = await _backupService.BackupAsync(progress, cancellationToken).ConfigureAwait(false);
                var manifestPayload = JsonSerializer.SerializeToUtf8Bytes(
                    new RotationManifest(1, DateTimeOffset.UtcNow, stagedEntries.Count, backup.Reference),
                    JsonOptions);
                var manifestReference = await _swarmClient.UploadAsync(manifestPayload, cancellationToken).ConfigureAwait(false);
                return new KeyRotationResult(BackupService.ToSwarmUri(manifestReference), stagedEntries.Count, backup.Reference);
            }
            catch
            {
                _keyProvider.Update(previousOptions);
                throw;
            }
        }
        finally
        {
            _gate.Release();
        }
    }
}
