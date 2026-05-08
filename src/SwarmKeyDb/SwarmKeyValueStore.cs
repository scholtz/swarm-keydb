namespace SwarmKeyDb;

public sealed class SwarmKeyValueStore : IKeyValueStore
{
    private readonly ISwarmClient _swarmClient;
    private readonly IKeyIndex _index;

    public SwarmKeyValueStore(ISwarmClient swarmClient, IKeyIndex index)
    {
        _swarmClient = swarmClient;
        _index = index;
    }

    public async Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        ValidateKey(key);
        var reference = await _swarmClient.UploadAsync(value, cancellationToken).ConfigureAwait(false);
        await _index.SetReferenceAsync(key, reference, cancellationToken).ConfigureAwait(false);
    }

    public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ValidateKey(key);
        var reference = await _index.GetReferenceAsync(key, cancellationToken).ConfigureAwait(false);
        return reference is null
            ? null
            : await _swarmClient.DownloadAsync(reference, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        ValidateKey(key);
        return await _index.RemoveAsync(key, cancellationToken).ConfigureAwait(false);
    }

    public Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default) =>
        _index.ListKeysAsync(cancellationToken);

    private static void ValidateKey(string key)
    {
        if (string.IsNullOrEmpty(key))
        {
            throw new ArgumentException("Keys must not be empty.", nameof(key));
        }
    }
}
