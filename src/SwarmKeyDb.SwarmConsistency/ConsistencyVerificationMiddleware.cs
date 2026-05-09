using System.Security.Cryptography;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace SwarmKeyDb.SwarmConsistency;

public sealed class ConsistencyVerificationMiddleware :
    IKeyValueStore,
    IBackendMetadataProvider,
    ICacheStats,
    IOfflineStatusProvider,
    IConsistencyVerificationStatusProvider
{
    private readonly IKeyValueStore _inner;
    private readonly ISwarmConsistencyVerifier _verifier;
    private readonly ConsistencyOptions _options;
    private readonly ILogger<ConsistencyVerificationMiddleware> _logger;
    private long _totalVerifications;
    private long _violations;
    private long _cacheEvictionsByVerification;
    private long _totalLatencyMs;
    private long _worstLatencyMs;
    private long _lastVerificationUnixMs;

    public ConsistencyVerificationMiddleware(
        IKeyValueStore inner,
        ISwarmConsistencyVerifier verifier,
        IOptions<ConsistencyOptions> options,
        ILogger<ConsistencyVerificationMiddleware> logger)
    {
        _inner = inner;
        _verifier = verifier;
        _options = options.Value;
        _logger = logger;
    }

    public long Hits => (_inner as ICacheStats)?.Hits ?? 0;
    public long Misses => (_inner as ICacheStats)?.Misses ?? 0;
    public long Evictions => (_inner as ICacheStats)?.Evictions ?? 0;
    public long QueueDepth => (_inner as IOfflineStatusProvider)?.QueueDepth ?? 0;
    public DateTimeOffset? LastSuccessfulSyncUtc => (_inner as IOfflineStatusProvider)?.LastSuccessfulSyncUtc;
    public bool IsOffline => (_inner as IOfflineStatusProvider)?.IsOffline ?? false;
    public OfflineMode Mode => (_inner as IOfflineStatusProvider)?.Mode ?? OfflineMode.Never;

    public Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        _inner.PutAsync(key, value, cancellationToken);

    public Task PutWithStrategyAsync(string key, ReadOnlyMemory<byte> value, IMergeStrategy mergeStrategy, CancellationToken cancellationToken = default) =>
        _inner.PutWithStrategyAsync(key, value, mergeStrategy, cancellationToken);

    public Task MergeAsync(string key, ReadOnlyMemory<byte> incomingValue, CancellationToken cancellationToken = default) =>
        _inner.MergeAsync(key, incomingValue, cancellationToken);

    public Task SetKeyOptionsAsync(string key, KeyOptions options, CancellationToken cancellationToken = default) =>
        _inner.SetKeyOptionsAsync(key, options, cancellationToken);

    public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        var value = await _inner.GetAsync(key, cancellationToken).ConfigureAwait(false);
        if (value is null || !_options.Enabled)
        {
            return value;
        }

        var reference = await TryGetBackendReferenceAsync(key, cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(reference))
        {
            return value;
        }

        var expectedHash = SHA256.HashData(value);
        var violation = await EvaluateVerificationAsync(key, () => _verifier.VerifyContentHashAsync(reference, expectedHash, cancellationToken), cancellationToken).ConfigureAwait(false);
        if (violation is not null)
        {
            return await HandleViolationAndRefetchAsync(key, violation, cancellationToken).ConfigureAwait(false);
        }

        var feedRevision = _options.ExpectedFeedRevisionResolver?.Invoke(key);
        if (feedRevision is { } expectedRevision)
        {
            violation = await EvaluateVerificationAsync(key, () => _verifier.VerifyFeedRevisionAsync(reference, expectedRevision, cancellationToken), cancellationToken).ConfigureAwait(false);
            if (violation is not null)
            {
                return await HandleViolationAndRefetchAsync(key, violation, cancellationToken).ConfigureAwait(false);
            }
        }

        var manifestLineage = _options.ExpectedManifestLineageResolver?.Invoke(key);
        if (manifestLineage is { } lineage)
        {
            violation = await EvaluateVerificationAsync(key, () => _verifier.VerifyManifestLineageAsync(lineage.ManifestRef, lineage.Ancestors, cancellationToken), cancellationToken).ConfigureAwait(false);
            if (violation is not null)
            {
                return await HandleViolationAndRefetchAsync(key, violation, cancellationToken).ConfigureAwait(false);
            }
        }

        return value;
    }

    public Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.DeleteAsync(key, cancellationToken);

    public Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default) =>
        _inner.ListKeysAsync(cancellationToken);

    public Task<bool> SetTtlAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default) =>
        _inner.SetTtlAsync(key, ttl, cancellationToken);

    public Task<(bool Exists, TimeSpan? Ttl)> GetTtlAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.GetTtlAsync(key, cancellationToken);

    public Task<bool> RemoveTtlAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.RemoveTtlAsync(key, cancellationToken);

    public Task<string?> GetBackendMetadataAsync(string key, CancellationToken cancellationToken = default) =>
        (_inner as IBackendMetadataProvider)?.GetBackendMetadataAsync(key, cancellationToken) ?? Task.FromResult<string?>(null);

    public ConsistencyVerificationSnapshot GetSnapshot()
    {
        var total = Interlocked.Read(ref _totalVerifications);
        var violations = Interlocked.Read(ref _violations);
        var successCount = Math.Max(0, total - violations);
        var successRate = total == 0 ? 1D : successCount / (double)total;
        return new ConsistencyVerificationSnapshot(
            LastVerificationUtc: ReadLastVerificationUtc(),
            TotalVerifications: total,
            ViolationCount: violations,
            SuccessRate: successRate,
            WorstLatencyMs: Interlocked.Read(ref _worstLatencyMs),
            EvictionByVerificationTotal: Interlocked.Read(ref _cacheEvictionsByVerification));
    }

    private async Task<string?> TryGetBackendReferenceAsync(string key, CancellationToken cancellationToken)
    {
        if (_inner is IBackendMetadataProvider metadataProvider)
        {
            return await metadataProvider.GetBackendMetadataAsync(key, cancellationToken).ConfigureAwait(false);
        }

        return null;
    }

    private async Task<VerificationResult?> EvaluateVerificationAsync(string key, Func<Task<VerificationResult>> action, CancellationToken cancellationToken)
    {
        try
        {
            var result = await action().ConfigureAwait(false);
            TrackResult(result);
            if (result.IsValid)
            {
                return null;
            }

            return await RecordViolationAsync(key, result, cancellationToken).ConfigureAwait(false);
        }
        catch (QuorumNotMetException ex)
        {
            var result = ex.Results.OrderByDescending(static item => item.Latency).FirstOrDefault()
                         ?? VerificationResult.Failed(ex.VerificationType, "quorum", TimeSpan.Zero, ex.Threshold.ToString(), ex.Succeeded.ToString(), ex.Message);
            TrackResult(result);
            return await RecordViolationAsync(key, result, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Records a violation, logs a warning, and — in strict mode — throws immediately.
    /// Returns the <see cref="VerificationResult"/> so the caller can act on it in warn mode.
    /// </summary>
    private Task<VerificationResult?> RecordViolationAsync(string key, VerificationResult result, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _violations);
        _logger.LogWarning(
            "Consistency verification failed for key {Key}. Type={VerificationType}, Node={NodeUrl}, Expected={ExpectedValue}, Actual={ActualValue}, Reason={FailureReason}, LatencyMs={LatencyMs}",
            key,
            result.VerificationType,
            result.NodeUrl,
            result.ExpectedValue,
            result.ActualValue,
            result.FailureReason,
            result.Latency.TotalMilliseconds);

        if (_options.FailureMode == ConsistencyFailureMode.Strict)
        {
            throw new ConsistencyViolationException(key, result);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult<VerificationResult?>(result);
    }

    /// <summary>
    /// Called when verification fails in warn mode.  Evicts the stale cache entry,
    /// invokes the operator callback, and re-fetches the value from the backend so
    /// the next read bypasses the (now-evicted) cache entry.
    /// </summary>
    private async Task<byte[]?> HandleViolationAndRefetchAsync(string key, VerificationResult violation, CancellationToken cancellationToken)
    {
        // Evict from the in-memory cache (if the inner chain supports it) so the next
        // read goes through to Swarm instead of serving a stale/divergent cached value.
        if (_inner is ICacheEviction cacheEviction)
        {
            cacheEviction.EvictFromCache(key);
            Interlocked.Increment(ref _cacheEvictionsByVerification);
            _logger.LogInformation(
                "Evicted cache entry for key {Key} after consistency verification failure.", key);
        }

        // Invoke the operator-supplied callback (if any) for observability / alerting.
        _options.OnVerificationFailure?.Invoke(key, violation);

        // Re-fetch from the backend; this will be a cache miss (the entry was just evicted)
        // and will return the authoritative value directly from Swarm.
        return await _inner.GetAsync(key, cancellationToken).ConfigureAwait(false);
    }

    private void TrackResult(VerificationResult result)
    {
        Interlocked.Increment(ref _totalVerifications);
        Interlocked.Exchange(ref _lastVerificationUnixMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        var latencyMs = (long)Math.Ceiling(result.Latency.TotalMilliseconds);
        Interlocked.Add(ref _totalLatencyMs, latencyMs);
        UpdateWorstLatency(latencyMs);
    }

    private void UpdateWorstLatency(long candidate)
    {
        while (true)
        {
            var current = Interlocked.Read(ref _worstLatencyMs);
            if (candidate <= current)
            {
                return;
            }

            if (Interlocked.CompareExchange(ref _worstLatencyMs, candidate, current) == current)
            {
                return;
            }
        }
    }

    private DateTimeOffset? ReadLastVerificationUtc()
    {
        var unixMs = Interlocked.Read(ref _lastVerificationUnixMs);
        return unixMs <= 0 ? null : DateTimeOffset.FromUnixTimeMilliseconds(unixMs);
    }
}
