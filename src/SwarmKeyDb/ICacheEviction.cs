namespace SwarmKeyDb;

/// <summary>
/// Provides a mechanism to evict a single key from the in-memory cache without deleting the
/// underlying value from the Swarm/backend storage.  Implemented by <see cref="CachingKeyValueStore"/>
/// and propagated through outer decorator stores so that
/// <c>SwarmKeyDb.SwarmConsistency.ConsistencyVerificationMiddleware</c> can perform targeted
/// cache evictions when a consistency check fails.
/// </summary>
public interface ICacheEviction
{
    /// <summary>
    /// Removes <paramref name="key"/> from the in-memory cache.  The key is not removed from
    /// the persistent backend; the next read will perform a cache-miss fetch from Swarm.
    /// </summary>
    void EvictFromCache(string key);
}
