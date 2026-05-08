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
public sealed class EncryptingKeyValueStore : IKeyValueStore, IAccessControlVerifier
{
    // Magic bytes that identify an encrypted blob stored by this class.
    private static readonly byte[] Magic = [0xAE, 0x73];

    private readonly IKeyValueStore _inner;
    private readonly IEncryptionKeyProvider _keyProvider;
    private readonly ILogger<EncryptingKeyValueStore> _logger;

    public EncryptingKeyValueStore(
        IKeyValueStore inner,
        IOptions<EncryptionOptions> options,
        ILogger<EncryptingKeyValueStore> logger)
        : this(inner, new MutableEncryptionKeyProvider(options.Value), logger)
    {
    }

    public EncryptingKeyValueStore(
        IKeyValueStore inner,
        IEncryptionKeyProvider keyProvider,
        ILogger<EncryptingKeyValueStore> logger)
    {
        _inner = inner;
        _logger = logger;
        _keyProvider = keyProvider;
    }

    public async Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        var keyBytes = _keyProvider.GetCurrentKey();
        if (keyBytes is null)
        {
            await _inner.PutAsync(key, value, cancellationToken).ConfigureAwait(false);
            return;
        }

        var encrypted = Encrypt(value.Span, keyBytes);

        _logger.LogDebug(
            "Encrypted key '{Key}': {OriginalSize} → {EncryptedSize} bytes",
            key, value.Length, encrypted.Length);

        await _inner.PutAsync(key, encrypted, cancellationToken).ConfigureAwait(false);
    }

    public Task PutWithStrategyAsync(
        string key,
        ReadOnlyMemory<byte> value,
        IMergeStrategy mergeStrategy,
        CancellationToken cancellationToken = default) =>
        _inner.PutWithStrategyAsync(key, EncryptIfEnabled(value.Span), mergeStrategy, cancellationToken);

    public Task MergeAsync(string key, ReadOnlyMemory<byte> incomingValue, CancellationToken cancellationToken = default) =>
        _inner.MergeAsync(key, EncryptIfEnabled(incomingValue.Span), cancellationToken);

    public Task SetKeyOptionsAsync(string key, KeyOptions options, CancellationToken cancellationToken = default) =>
        _inner.SetKeyOptionsAsync(key, options, cancellationToken);

    public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        var data = await _inner.GetAsync(key, cancellationToken).ConfigureAwait(false);
        if (data is null)
        {
            return null;
        }

        // If magic bytes are absent this is legacy unencrypted data — return as-is.
        if (!AesGcmEnvelope.HasMagic(data, Magic))
        {
            return data;
        }

        var keyBytes = _keyProvider.GetCurrentKey();
        if (keyBytes is null)
        {
            throw new InvalidOperationException($"Encrypted value for key '{key}' cannot be read because no encryption key is configured.");
        }

        // AesGcm.Decrypt throws CryptographicException if the authentication tag does not match.
        return Decrypt(data, keyBytes);
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

    public void EnsureReadAccess()
    {
        if (_inner is IAccessControlVerifier verifier)
        {
            verifier.EnsureReadAccess();
        }
    }

    public void EnsureWriteAccess()
    {
        if (_inner is IAccessControlVerifier verifier)
        {
            verifier.EnsureWriteAccess();
        }
    }

    private static byte[] Encrypt(ReadOnlySpan<byte> plaintext, byte[] key)
        => AesGcmEnvelope.Encrypt(plaintext, key, Magic);

    private static byte[] Decrypt(byte[] data, byte[] key)
        => AesGcmEnvelope.Decrypt(data, key, Magic);

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

    private ReadOnlyMemory<byte> EncryptIfEnabled(ReadOnlySpan<byte> plaintext)
    {
        var keyBytes = _keyProvider.GetCurrentKey();
        return keyBytes is null
            ? plaintext.ToArray()
            : Encrypt(plaintext, keyBytes);
    }

    private static byte[] ResolveKey(EncryptionOptions options)
    {
        var key = EncryptionKeyMaterial.TryResolveKey(options);
        if (key is null)
        {
            // Return a dummy key; the store won't be registered unless encryption is enabled.
            return new byte[32];
        }

        return key;
    }
}
