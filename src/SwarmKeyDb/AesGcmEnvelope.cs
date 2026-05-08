using System.Security.Cryptography;

namespace SwarmKeyDb;

internal static class AesGcmEnvelope
{
    private const int NonceSize = 12;
    private const int TagSize = 16;

    public static byte[] Encrypt(ReadOnlySpan<byte> plaintext, byte[] key, byte[] magic)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(magic);

        var nonce = new byte[NonceSize];
        RandomNumberGenerator.Fill(nonce);

        var ciphertext = new byte[plaintext.Length];
        var tag = new byte[TagSize];

        using var aes = new AesGcm(key, TagSize);
        aes.Encrypt(nonce, plaintext, ciphertext, tag);

        var result = new byte[magic.Length + NonceSize + TagSize + ciphertext.Length];
        magic.CopyTo(result, 0);
        nonce.CopyTo(result, magic.Length);
        tag.CopyTo(result, magic.Length + NonceSize);
        ciphertext.CopyTo(result, magic.Length + NonceSize + TagSize);
        return result;
    }

    public static byte[] Decrypt(byte[] data, byte[] key, byte[] magic)
    {
        ArgumentNullException.ThrowIfNull(data);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(magic);

        if (!HasMagic(data, magic))
        {
            throw new InvalidOperationException("Encrypted payload is missing the expected magic header.");
        }

        var headerSize = magic.Length + NonceSize + TagSize;
        var nonce = data.AsSpan(magic.Length, NonceSize);
        var tag = data.AsSpan(magic.Length + NonceSize, TagSize);
        var ciphertext = data.AsSpan(headerSize);
        var plaintext = new byte[ciphertext.Length];
        using var aes = new AesGcm(key, TagSize);
        aes.Decrypt(nonce, ciphertext, tag, plaintext);
        return plaintext;
    }

    public static bool HasMagic(ReadOnlySpan<byte> data, byte[] magic)
    {
        if (data.Length < magic.Length + NonceSize + TagSize)
        {
            return false;
        }

        for (var i = 0; i < magic.Length; i++)
        {
            if (data[i] != magic[i])
            {
                return false;
            }
        }

        return true;
    }
}
