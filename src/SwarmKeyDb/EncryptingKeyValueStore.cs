using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace SwarmKeyDb;

/// <summary>
/// A decorator that transparently encrypts values on write and decrypts on read using
/// AES-256-GCM. Each encrypted blob is prefixed with a 2-byte magic header (0xAE 0x73),
/// followed by a 12-byte random nonce and a 16-byte GCM authentication tag, then the
/// ciphertext. Legacy values without the magic header are returned as-is.
/// </summary>
public sealed class EncryptingKeyValueStore : IKeyValueStore
{
    // Magic bytes that identify an encrypted blob stored by this class.
    private static readonly byte[] Magic = [0xAE, 0x73];

    private const int NonceSize = 12;  // 96-bit nonce for AES-GCM
    private const int TagSize = 16;    // 128-bit authentication tag for AES-GCM
    private const int HeaderSize = 2 + NonceSize + TagSize; // Magic + Nonce + Tag

    private readonly IKeyValueStore _inner;
    private readonly byte[] _key;
    private readonly ILogger<EncryptingKeyValueStore> _logger;

    public EncryptingKeyValueStore(
        IKeyValueStore inner,
        IOptions<EncryptionOptions> options,
        ILogger<EncryptingKeyValueStore> logger)
    {
        _inner = inner;
        _logger = logger;
        _key = ResolveKey(options.Value);
    }

    public async Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        var encrypted = Encrypt(value.Span, _key);

        _logger.LogDebug(
            "Encrypted key '{Key}': {OriginalSize} → {EncryptedSize} bytes",
            key, value.Length, encrypted.Length);

        await _inner.PutAsync(key, encrypted, cancellationToken).ConfigureAwait(false);
    }

    public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        var data = await _inner.GetAsync(key, cancellationToken).ConfigureAwait(false);
        if (data is null)
        {
            return null;
        }

        // If magic bytes are absent this is legacy unencrypted data — return as-is.
        if (data.Length < HeaderSize || data[0] != Magic[0] || data[1] != Magic[1])
        {
            return data;
        }

        // AesGcm.Decrypt throws CryptographicException if the authentication tag does not match.
        return Decrypt(data, _key);
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

    private static byte[] Encrypt(ReadOnlySpan<byte> plaintext, byte[] key)
    {
        // Generate a fresh random nonce for every write (non-deterministic).
        var nonce = new byte[NonceSize];
        RandomNumberGenerator.Fill(nonce);

        var ciphertext = new byte[plaintext.Length];
        var tag = new byte[TagSize];

        using var aes = new AesGcm(key, TagSize);
        aes.Encrypt(nonce, plaintext, ciphertext, tag);

        // Layout: [Magic(2)] [Nonce(12)] [Tag(16)] [Ciphertext(n)]
        var result = new byte[HeaderSize + ciphertext.Length];
        Magic.CopyTo(result, 0);
        nonce.CopyTo(result, Magic.Length);
        tag.CopyTo(result, Magic.Length + NonceSize);
        ciphertext.CopyTo(result, HeaderSize);
        return result;
    }

    private static byte[] Decrypt(byte[] data, byte[] key)
    {
        var nonce = data.AsSpan(Magic.Length, NonceSize);
        var tag = data.AsSpan(Magic.Length + NonceSize, TagSize);
        var ciphertext = data.AsSpan(HeaderSize);

        var plaintext = new byte[ciphertext.Length];
        using var aes = new AesGcm(key, TagSize);
        aes.Decrypt(nonce, ciphertext, tag, plaintext);
        return plaintext;
    }

    /// <summary>
    /// Derives a 32-byte AES-256 key from the Ethereum private key using HKDF-SHA256
    /// with an app-specific info label. This ensures a consistent, domain-separated key
    /// independent of the raw private key material.
    /// </summary>
    public static byte[] DeriveKeyFromEthPrivateKey(string ethPrivateKeyHex)
    {
        // Strip optional '0x' prefix; do not strip leading zero digits as they are significant.
        var hex = ethPrivateKeyHex.Trim();
        if (hex.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
        {
            hex = hex[2..];
        }

        var privKeyBytes = Convert.FromHexString(hex);

        // Ensure exactly 32 bytes (zero-pad on the left if the hex had leading zeros stripped).
        if (privKeyBytes.Length > 32)
        {
            throw new ArgumentException("Ethereum private key must be at most 32 bytes (64 hex chars).", nameof(ethPrivateKeyHex));
        }

        var keyMaterial = new byte[32];
        privKeyBytes.CopyTo(keyMaterial, 32 - privKeyBytes.Length);

        var salt = Encoding.UTF8.GetBytes("SwarmKeyDb-v1-salt");
        var info = Encoding.UTF8.GetBytes("SwarmKeyDb-v1-AES256GCM");
        return HKDF.DeriveKey(HashAlgorithmName.SHA256, keyMaterial, 32, salt: salt, info: info);
    }

    private static byte[] ResolveKey(EncryptionOptions options)
    {
        if (!options.Enabled)
        {
            // Return a dummy key; the store won't be registered unless encryption is enabled.
            return new byte[32];
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
            return DeriveKeyFromEthPrivateKey(options.EthPrivateKeyHex.Trim());
        }

        throw new InvalidOperationException(
            "Encryption is enabled (SWARM_KEYDB_ENCRYPTION_ENABLED=true) but no key is configured. " +
            "Set SWARM_KEYDB_ENCRYPTION_KEY (64-char hex) or SWARM_KEYDB_ENCRYPTION_ETH_KEY (Ethereum private key hex).");
    }
}
