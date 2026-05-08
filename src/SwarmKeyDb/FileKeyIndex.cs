using System.Text.Json;

namespace SwarmKeyDb;

public sealed class FileKeyIndex : IKeyIndex
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };
    private readonly string _path;
    private readonly SemaphoreSlim _gate = new(1, 1);

    public FileKeyIndex(string path)
    {
        _path = path;
    }

    public async Task<string?> GetReferenceAsync(string key, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var index = await ReadIndexAsync(cancellationToken).ConfigureAwait(false);
            index.TryGetValue(key, out var reference);
            return reference;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task SetReferenceAsync(string key, string reference, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var index = await ReadIndexAsync(cancellationToken).ConfigureAwait(false);
            index[key] = reference;
            await WriteIndexAsync(index, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<bool> RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var index = await ReadIndexAsync(cancellationToken).ConfigureAwait(false);
            if (!index.Remove(key))
            {
                return false;
            }

            await WriteIndexAsync(index, cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var index = await ReadIndexAsync(cancellationToken).ConfigureAwait(false);
            return index.Keys.Order(StringComparer.Ordinal).ToArray();
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task<Dictionary<string, string>> ReadIndexAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(_path))
        {
            return new Dictionary<string, string>(StringComparer.Ordinal);
        }

        await using var stream = File.OpenRead(_path);
        var entries = await JsonSerializer.DeserializeAsync<Dictionary<string, string>>(stream, JsonOptions, cancellationToken).ConfigureAwait(false);
        return new Dictionary<string, string>(entries ?? new Dictionary<string, string>(), StringComparer.Ordinal);
    }

    private async Task WriteIndexAsync(Dictionary<string, string> index, CancellationToken cancellationToken)
    {
        var directory = Path.GetDirectoryName(_path);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        var tempPath = _path + ".tmp";
        await using (var stream = File.Create(tempPath))
        {
            await JsonSerializer.SerializeAsync(stream, index, JsonOptions, cancellationToken).ConfigureAwait(false);
        }

        File.Move(tempPath, _path, true);
    }
}
