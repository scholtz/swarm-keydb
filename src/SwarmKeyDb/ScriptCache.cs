using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace SwarmKeyDb;

/// <summary>
/// Thread-safe SHA1-keyed cache for Lua script source code.
/// SHA1 is computed over the raw UTF-8 bytes of the script source, matching Redis semantics.
/// The cache is node-local and is not replicated across instances.
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
    /// Computes the SHA1 hex digest of a script source string.
    /// </summary>
    public static string ComputeSha1(string script)
    {
        var bytes = SHA1.HashData(Encoding.UTF8.GetBytes(script));
        return Convert.ToHexStringLower(bytes);
    }
}
