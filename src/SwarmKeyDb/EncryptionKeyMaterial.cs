using System.Security.Cryptography;
using System.Text;

namespace SwarmKeyDb;

internal static class EncryptionKeyMaterial
{
    public static byte[] ResolveManagementKey(string keyText)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyText);
        return EncryptingKeyValueStore.DeriveKeyFromEthPrivateKey(keyText.Trim());
    }

    public static byte[]? TryResolveKey(EncryptionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (!options.Enabled)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(options.KeyHex))
        {
            var keyBytes = Convert.FromHexString(options.KeyHex.Trim());
            if (keyBytes.Length != 32)
            {
                throw new InvalidOperationException(
                    "SWARM_KEYDB_ENCRYPTION_KEY must be a 64-character hex string representing a 32-byte AES-256 key.");
            }

            return keyBytes;
        }

        if (!string.IsNullOrWhiteSpace(options.EthPrivateKeyHex))
        {
            return ResolveManagementKey(options.EthPrivateKeyHex);
        }

        throw new InvalidOperationException(
            "Encryption is enabled (SWARM_KEYDB_ENCRYPTION_ENABLED=true) but no key is configured. " +
            "Set SWARM_KEYDB_ENCRYPTION_KEY (64-char hex) or SWARM_KEYDB_ENCRYPTION_ETH_KEY (Ethereum private key hex).");
    }
}
