namespace SwarmKeyDb;

/// <summary>
/// Access-control policy that grants or denies store operations based on
/// <see cref="VerifiableCredential"/> claims presented by the caller.
///
/// <para>A credential grants access when ALL of the following hold:</para>
/// <list type="bullet">
///   <item><description>The credential is not expired.</description></item>
///   <item><description>The caller's DID is listed in <see cref="VerifiableCredential.SubjectDids"/>.</description></item>
///   <item><description>The <c>operation</c> claim is <c>*</c> or matches the requested <see cref="DidOperation"/>.</description></item>
///   <item><description>The <c>keyPattern</c> claim is absent, or the requested key starts with the pattern prefix.</description></item>
/// </list>
/// </summary>
public sealed class VerifiableCredentialAclPolicy
{
    /// <summary>
    /// Evaluates <paramref name="credentials"/> and returns <see langword="true"/> when at least one
    /// non-expired credential grants <paramref name="did"/> the right to perform
    /// <paramref name="operation"/> on <paramref name="key"/>.
    /// </summary>
    public Task<bool> IsAllowedAsync(
        string did,
        string key,
        DidOperation operation,
        IEnumerable<VerifiableCredential> credentials,
        CancellationToken cancellationToken = default)
    {
        foreach (var vc in credentials)
        {
            if (vc.IsExpired)
            {
                continue;
            }

            if (!vc.SubjectDids.Contains(did, StringComparer.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!OperationMatches(vc, operation))
            {
                continue;
            }

            if (!KeyMatches(vc, key))
            {
                continue;
            }

            return Task.FromResult(true);
        }

        return Task.FromResult(false);
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private static bool OperationMatches(VerifiableCredential vc, DidOperation operation)
    {
        if (!vc.Claims.TryGetValue("operation", out var opClaim))
        {
            // No operation constraint means all operations are permitted by this credential.
            return true;
        }

        if (string.Equals(opClaim, "*", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        var opName = operation.ToString();
        return string.Equals(opClaim, opName, StringComparison.OrdinalIgnoreCase);
    }

    private static bool KeyMatches(VerifiableCredential vc, string key)
    {
        if (!vc.Claims.TryGetValue("keyPattern", out var pattern) || string.IsNullOrEmpty(pattern))
        {
            // No key constraint means all keys are permitted.
            return true;
        }

        return key.StartsWith(pattern, StringComparison.Ordinal);
    }
}
