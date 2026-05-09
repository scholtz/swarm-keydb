namespace SwarmKeyDb.SwarmConsistency;

/// <summary>
/// Verifies authenticity of data read from Bee/Swarm before values are returned to callers.
/// </summary>
public interface ISwarmConsistencyVerifier
{
    /// <summary>
    /// Verifies that the Bee feed revision for <paramref name="topic"/> matches <paramref name="expectedRevision"/>.
    /// </summary>
    /// <param name="topic">Feed topic value (hex), or <c>owner/topic</c> when owner override is needed.</param>
    /// <param name="expectedRevision">Expected feed revision index.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A verification result that includes expected/actual values, node URL, and diagnostics.</returns>
    Task<VerificationResult> VerifyFeedRevisionAsync(string topic, ulong expectedRevision, CancellationToken ct);

    /// <summary>
    /// Verifies that bytes loaded from <paramref name="reference"/> hash to <paramref name="expectedHash"/> (SHA-256).
    /// </summary>
    /// <param name="reference">Swarm reference/hash.</param>
    /// <param name="expectedHash">Expected SHA-256 hash bytes.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A verification result that includes expected/actual values, node URL, and diagnostics.</returns>
    Task<VerificationResult> VerifyContentHashAsync(string reference, byte[] expectedHash, CancellationToken ct);

    /// <summary>
    /// Verifies that a manifest contains all required lineage references in <paramref name="expectedAncestors"/>.
    /// </summary>
    /// <param name="manifestRef">Manifest reference/hash.</param>
    /// <param name="expectedAncestors">Ancestor references that must be present in lineage.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A verification result that includes expected/actual values, node URL, and diagnostics.</returns>
    Task<VerificationResult> VerifyManifestLineageAsync(string manifestRef, IReadOnlyList<string> expectedAncestors, CancellationToken ct);
}
