using System.Text;
using System.Text.Json;

namespace SwarmKeyDb;

public sealed class SwarmKeyDbClient
{
    private readonly IKeyValueStore _store;

    public SwarmKeyDbClient(IKeyValueStore store)
    {
        _store = store;
    }

    public Task PutBytesAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        _store.PutAsync(key, value, cancellationToken);

    public Task<byte[]?> GetBytesAsync(string key, CancellationToken cancellationToken = default) =>
        _store.GetAsync(key, cancellationToken);

    public Task PutStringAsync(string key, string value, CancellationToken cancellationToken = default) =>
        _store.PutAsync(key, Encoding.UTF8.GetBytes(value), cancellationToken);

    public async Task<string?> GetStringAsync(string key, CancellationToken cancellationToken = default)
    {
        var bytes = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
        return bytes is null ? null : Encoding.UTF8.GetString(bytes);
    }

    public Task PutJsonAsync<T>(string key, T value, CancellationToken cancellationToken = default) =>
        _store.PutAsync(key, JsonSerializer.SerializeToUtf8Bytes(value), cancellationToken);

    public async Task<T?> GetJsonAsync<T>(string key, CancellationToken cancellationToken = default)
    {
        var bytes = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
        return bytes is null ? default : JsonSerializer.Deserialize<T>(bytes);
    }

    public Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default) =>
        _store.DeleteAsync(key, cancellationToken);

    public Task<IReadOnlyList<string>> KeysAsync(CancellationToken cancellationToken = default) =>
        _store.ListKeysAsync(cancellationToken);
}
