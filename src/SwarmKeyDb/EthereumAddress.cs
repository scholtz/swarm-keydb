namespace SwarmKeyDb;

public static class EthereumAddress
{
    public static string Normalize(string address)
    {
        if (string.IsNullOrWhiteSpace(address))
        {
            throw new ArgumentException("Ethereum address must not be empty.", nameof(address));
        }

        var normalized = address.Trim();
        if (!normalized.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
        {
            normalized = "0x" + normalized;
        }

        if (normalized.Length != 42)
        {
            throw new ArgumentException("Ethereum address must be a 20-byte hex string prefixed with 0x.", nameof(address));
        }

        for (var i = 2; i < normalized.Length; i++)
        {
            if (!Uri.IsHexDigit(normalized[i]))
            {
                throw new ArgumentException("Ethereum address must contain only hexadecimal characters.", nameof(address));
            }
        }

        return "0x" + normalized[2..].ToLowerInvariant();
    }
}
