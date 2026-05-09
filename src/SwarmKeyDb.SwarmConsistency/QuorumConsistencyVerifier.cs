namespace SwarmKeyDb.SwarmConsistency;

public sealed class QuorumConsistencyVerifier : ISwarmConsistencyVerifier
{
    private readonly IReadOnlyList<ISwarmConsistencyVerifier> _verifiers;
    private readonly int _threshold;

    public QuorumConsistencyVerifier(IReadOnlyList<ISwarmConsistencyVerifier> verifiers, int threshold)
    {
        _verifiers = verifiers;
        _threshold = threshold;
        if (_verifiers.Count == 0)
        {
            throw new ArgumentException("At least one verifier is required.", nameof(verifiers));
        }

        if (_threshold <= 0 || _threshold > _verifiers.Count)
        {
            throw new ArgumentOutOfRangeException(nameof(threshold), $"Threshold must be between 1 and {_verifiers.Count}.");
        }
    }

    public Task<VerificationResult> VerifyFeedRevisionAsync(string topic, ulong expectedRevision, CancellationToken ct) =>
        VerifyAsync("feed-revision", verifier => verifier.VerifyFeedRevisionAsync(topic, expectedRevision, ct), ct);

    public Task<VerificationResult> VerifyContentHashAsync(string reference, byte[] expectedHash, CancellationToken ct) =>
        VerifyAsync("content-hash", verifier => verifier.VerifyContentHashAsync(reference, expectedHash, ct), ct);

    public Task<VerificationResult> VerifyManifestLineageAsync(string manifestRef, IReadOnlyList<string> expectedAncestors, CancellationToken ct) =>
        VerifyAsync("manifest-lineage", verifier => verifier.VerifyManifestLineageAsync(manifestRef, expectedAncestors, ct), ct);

    private async Task<VerificationResult> VerifyAsync(string verificationType, Func<ISwarmConsistencyVerifier, Task<VerificationResult>> operation, CancellationToken ct)
    {
        var tasks = _verifiers.Select(operation).ToArray();
        var results = await Task.WhenAll(tasks).ConfigureAwait(false);
        var valid = results.Where(static result => result.IsValid).ToArray();
        if (valid.Length >= _threshold)
        {
            return valid.OrderBy(result => result.Latency).First();
        }

        throw new QuorumNotMetException(verificationType, _threshold, valid.Length, results);
    }
}
