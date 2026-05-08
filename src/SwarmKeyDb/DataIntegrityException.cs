namespace SwarmKeyDb;

public sealed class DataIntegrityException : InvalidOperationException
{
    public DataIntegrityException(
        string key,
        string? expectedHash = null,
        string? actualHash = null,
        string? detail = null,
        Exception? innerException = null)
        : base(BuildMessage(key, expectedHash, actualHash, detail), innerException)
    {
        KeyName = key;
        ExpectedHash = expectedHash;
        ActualHash = actualHash;
    }

    public string KeyName { get; }

    public string? ExpectedHash { get; }

    public string? ActualHash { get; }

    private static string BuildMessage(string key, string? expectedHash, string? actualHash, string? detail)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);

        if (!string.IsNullOrWhiteSpace(detail))
        {
            return $"Data integrity check failed for key '{key}'. {detail} The data may have been corrupted or tampered with.";
        }

        return $"Data integrity check failed for key '{key}'. Expected hash: {expectedHash ?? "<unknown>"}, got: {actualHash ?? "<unknown>"}. The data may have been corrupted or tampered with.";
    }
}
