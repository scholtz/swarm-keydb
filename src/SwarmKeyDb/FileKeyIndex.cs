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
            var changed = RemoveExpiredEntries(index, DateTimeOffset.UtcNow);
            if (changed)
            {
                await WriteIndexAsync(index, cancellationToken).ConfigureAwait(false);
            }

            return index.TryGetValue(key, out var entry) ? entry.Reference : null;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task SetReferenceAsync(string key, string reference, DateTimeOffset? expiresAt = null, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var index = await ReadIndexAsync(cancellationToken).ConfigureAwait(false);
            index[key] = new KeyIndexEntry(reference, expiresAt);
            await WriteIndexAsync(index, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<bool> SetExpiryAsync(string key, DateTimeOffset expiresAt, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var index = await ReadIndexAsync(cancellationToken).ConfigureAwait(false);
            var changed = RemoveExpiredEntries(index, DateTimeOffset.UtcNow);
            if (!index.TryGetValue(key, out var entry))
            {
                if (changed)
                {
                    await WriteIndexAsync(index, cancellationToken).ConfigureAwait(false);
                }

                return false;
            }

            index[key] = entry with { ExpiresAt = expiresAt };
            await WriteIndexAsync(index, cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<(bool Exists, DateTimeOffset? ExpiresAt)> GetExpiryAsync(string key, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var index = await ReadIndexAsync(cancellationToken).ConfigureAwait(false);
            var changed = RemoveExpiredEntries(index, DateTimeOffset.UtcNow);
            if (changed)
            {
                await WriteIndexAsync(index, cancellationToken).ConfigureAwait(false);
            }

            return index.TryGetValue(key, out var entry)
                ? (true, entry.ExpiresAt)
                : (false, null);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<bool> RemoveExpiryAsync(string key, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var index = await ReadIndexAsync(cancellationToken).ConfigureAwait(false);
            var changed = RemoveExpiredEntries(index, DateTimeOffset.UtcNow);
            if (!index.TryGetValue(key, out var entry) || entry.ExpiresAt is null)
            {
                if (changed)
                {
                    await WriteIndexAsync(index, cancellationToken).ConfigureAwait(false);
                }

                return false;
            }

            index[key] = entry with { ExpiresAt = null };
            await WriteIndexAsync(index, cancellationToken).ConfigureAwait(false);
            return true;
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
            var changed = RemoveExpiredEntries(index, DateTimeOffset.UtcNow);
            if (!index.Remove(key))
            {
                if (changed)
                {
                    await WriteIndexAsync(index, cancellationToken).ConfigureAwait(false);
                }

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
            var changed = RemoveExpiredEntries(index, DateTimeOffset.UtcNow);
            if (changed)
            {
                await WriteIndexAsync(index, cancellationToken).ConfigureAwait(false);
            }

            return index.Keys.Order(StringComparer.Ordinal).ToArray();
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task<Dictionary<string, KeyIndexEntry>> ReadIndexAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(_path))
        {
            return new Dictionary<string, KeyIndexEntry>(StringComparer.Ordinal);
        }

        await using var stream = File.OpenRead(_path);
        var element = await JsonSerializer.DeserializeAsync<JsonElement>(stream, JsonOptions, cancellationToken).ConfigureAwait(false);
        return DeserializeEntries(element);
    }

    private async Task WriteIndexAsync(Dictionary<string, KeyIndexEntry> index, CancellationToken cancellationToken)
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

    private static Dictionary<string, KeyIndexEntry> DeserializeEntries(JsonElement element)
    {
        var index = new Dictionary<string, KeyIndexEntry>(StringComparer.Ordinal);
        if (element.ValueKind != JsonValueKind.Object)
        {
            return index;
        }

        foreach (var property in element.EnumerateObject())
        {
            if (property.Value.ValueKind == JsonValueKind.String)
            {
                var legacyReference = property.Value.GetString();
                if (!string.IsNullOrEmpty(legacyReference))
                {
                    index[property.Name] = new KeyIndexEntry(legacyReference, null);
                }

                continue;
            }

            if (property.Value.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            var reference = TryGetString(property.Value, "Reference") ?? TryGetString(property.Value, "reference");
            if (string.IsNullOrEmpty(reference))
            {
                continue;
            }

            index[property.Name] = new KeyIndexEntry(
                reference,
                TryGetDateTimeOffset(property.Value, "ExpiresAt") ?? TryGetDateTimeOffset(property.Value, "expiresAt"));
        }

        return index;
    }

    private static string? TryGetString(JsonElement value, string propertyName)
    {
        return value.TryGetProperty(propertyName, out var property) && property.ValueKind == JsonValueKind.String
            ? property.GetString()
            : null;
    }

    private static DateTimeOffset? TryGetDateTimeOffset(JsonElement value, string propertyName)
    {
        if (!value.TryGetProperty(propertyName, out var property) || property.ValueKind != JsonValueKind.String)
        {
            return null;
        }

        return DateTimeOffset.TryParse(property.GetString(), out var expiresAt) ? expiresAt : null;
    }

    private static bool RemoveExpiredEntries(Dictionary<string, KeyIndexEntry> index, DateTimeOffset now)
    {
        var expiredKeys = index
            .Where(pair => pair.Value.ExpiresAt is { } expiresAt && expiresAt <= now)
            .Select(pair => pair.Key)
            .ToArray();

        foreach (var key in expiredKeys)
        {
            index.Remove(key);
        }

        return expiredKeys.Length > 0;
    }

    private sealed record KeyIndexEntry(string Reference, DateTimeOffset? ExpiresAt);
}
