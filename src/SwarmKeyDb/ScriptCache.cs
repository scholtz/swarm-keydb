using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace SwarmKeyDb;

/// <summary>
/// Thread-safe SHA1-keyed cache for Lua script source code.
/// SHA1 is computed over the raw UTF-8 bytes of the script source, matching Redis semantics.
/// </summary>
public sealed class ScriptCache
{
    private readonly ConcurrentDictionary<string, string> _cache = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Stores a script in the cache and returns its SHA1 hex digest.
    /// If the same SHA1 already exists the call is idempotent.
    /// </summary>
    public string Store(string script)
    {
        var sha1 = ComputeSha1(script);
        _cache.TryAdd(sha1, script);
        return sha1;
    }

    /// <summary>
    /// Stores a script and reports whether it was newly inserted.
    /// </summary>
    public bool TryStore(string script, out string sha1)
    {
        sha1 = ComputeSha1(script);
        return _cache.TryAdd(sha1, script);
    }

    /// <summary>
    /// Stores a script for a known SHA1 digest when the digest matches the script source.
    /// Returns <c>false</c> when the digest does not match.
    /// </summary>
    public bool TryStoreReplicated(string sha1, string script)
    {
        if (!string.Equals(ComputeSha1(script), sha1, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        _cache.TryAdd(sha1, script);
        return true;
    }

    /// <summary>
    /// Retrieves a script by its SHA1 hex digest. Returns <c>null</c> on cache miss.
    /// </summary>
    public string? Get(string sha1) =>
        _cache.TryGetValue(sha1, out var script) ? script : null;

    /// <summary>
    /// Returns <c>true</c> when the given SHA1 exists in the cache.
    /// </summary>
    public bool Exists(string sha1) => _cache.ContainsKey(sha1);

    /// <summary>
    /// Removes all cached scripts.
    /// </summary>
    public void Flush() => _cache.Clear();

    /// <summary>
    /// Returns the current number of cached scripts.
    /// </summary>
    public int Count => _cache.Count;

    /// <summary>
    /// Returns a snapshot of all scripts keyed by SHA1 digest.
    /// </summary>
    public IReadOnlyDictionary<string, string> Snapshot() => new Dictionary<string, string>(_cache, StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Computes the SHA1 hex digest of a script source string.
    /// </summary>
    public static string ComputeSha1(string script)
    {
        var bytes = SHA1.HashData(Encoding.UTF8.GetBytes(script));
        return Convert.ToHexStringLower(bytes);
    }
}
