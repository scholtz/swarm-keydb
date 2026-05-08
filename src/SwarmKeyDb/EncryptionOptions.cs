namespace SwarmKeyDb;

public sealed class EncryptionOptions
{
    public bool Enabled { get; set; } = false;
    public EncryptionAlgorithm Algorithm { get; set; } = EncryptionAlgorithm.AesGcm256;

    /// <summary>
    /// 32-byte AES-256 key as a 64-character hex string.
    /// Environment variable: SWARM_KEYDB_ENCRYPTION_KEY
    /// </summary>
    public string? KeyHex { get; set; }

    /// <summary>
    /// Ethereum private key as a 64-character hex string. When set, the AES key is derived
    /// from this key using HKDF-SHA256 with app-specific info bytes.
    /// Environment variable: SWARM_KEYDB_ENCRYPTION_ETH_KEY
    /// </summary>
    public string? EthPrivateKeyHex { get; set; }
}
