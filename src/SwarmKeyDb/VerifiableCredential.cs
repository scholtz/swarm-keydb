namespace SwarmKeyDb;

/// <summary>
/// Minimal representation of a W3C Verifiable Credential used for DID-based access control.
/// </summary>
public sealed class VerifiableCredential
{
    /// <summary>Credential identifier (URI).</summary>
    public string? Id { get; init; }

    /// <summary>DID of the issuer.</summary>
    public string Issuer { get; init; } = string.Empty;

    /// <summary>DID(s) of the subject(s) this credential applies to.</summary>
    public IReadOnlyList<string> SubjectDids { get; init; } = [];

    /// <summary>
    /// Credential subject claims as key-value pairs.
    /// Recognised keys: <c>operation</c> (read|write|delete|*), <c>keyPattern</c> (glob-style key prefix).
    /// </summary>
    public IReadOnlyDictionary<string, string> Claims { get; init; } =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>UTC date/time after which this credential is no longer valid, or <see langword="null"/> if it does not expire.</summary>
    public DateTimeOffset? ExpiresAt { get; init; }

    /// <summary>Returns <see langword="true"/> when the credential has passed its expiry date.</summary>
    public bool IsExpired => ExpiresAt.HasValue && DateTimeOffset.UtcNow > ExpiresAt.Value;
}
