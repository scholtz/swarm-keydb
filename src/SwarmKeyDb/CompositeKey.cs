namespace SwarmKeyDb;

/// <summary>
/// Utility for building and validating composite keys from multiple segments.
/// </summary>
/// <example>
/// <code>
/// var key = CompositeKey.Of("users", userId, "profile"); // "users:alice:profile"
/// var key = CompositeKey.Of('/', "users", userId, "profile"); // "users/alice/profile"
/// </code>
/// </example>
public static class CompositeKey
{
    /// <summary>
    /// The default separator character used between key segments.
    /// </summary>
    public const char DefaultSeparator = ':';

    /// <summary>
    /// Constructs a composite key from the given segments using the default separator <c>:</c>.
    /// </summary>
    /// <param name="segments">One or more non-empty string segments. No segment may contain the separator character.</param>
    /// <exception cref="ArgumentException">Thrown when a segment is null, empty, or contains the separator character.</exception>
    public static string Of(params string[] segments) => Of(DefaultSeparator, segments);

    /// <summary>
    /// Constructs a composite key from the given segments using the specified separator.
    /// </summary>
    /// <param name="separator">The separator character to join segments with.</param>
    /// <param name="segments">One or more non-empty string segments. No segment may contain the separator character.</param>
    /// <exception cref="ArgumentException">Thrown when segments is empty or a segment is null, empty, or contains the separator character.</exception>
    public static string Of(char separator, params string[] segments)
    {
        ArgumentNullException.ThrowIfNull(segments);
        if (segments.Length == 0)
        {
            throw new ArgumentException("At least one segment is required.", nameof(segments));
        }

        for (var i = 0; i < segments.Length; i++)
        {
            var segment = segments[i];
            if (string.IsNullOrEmpty(segment))
            {
                throw new ArgumentException($"Segment at index {i} must not be null or empty.", nameof(segments));
            }

            if (segment.Contains(separator))
            {
                throw new ArgumentException(
                    $"Segment '{segment}' at index {i} must not contain the separator character '{separator}'.",
                    nameof(segments));
            }
        }

        return string.Join(separator, segments);
    }
}
