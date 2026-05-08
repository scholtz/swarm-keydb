using System.Security.Cryptography;
using System.Text;

namespace SwarmKeyDb;

public sealed class HmacSha256KeyStrategy : IKeyPrivacyStrategy
{
    private readonly byte[] _secret;

    public HmacSha256KeyStrategy(byte[] secret)
    {
        ArgumentNullException.ThrowIfNull(secret);
        if (secret.Length == 0)
        {
            throw new ArgumentException("HMAC secret must not be empty.", nameof(secret));
        }

        _secret = secret.ToArray();
    }

    public PrivacyMode Mode => PrivacyMode.ObliviousHashing;

    public string DeriveToken(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        var keyBytes = Encoding.UTF8.GetBytes(key);
        using var hmac = new HMACSHA256(_secret);
        return Convert.ToHexStringLower(hmac.ComputeHash(keyBytes));
    }

    public static HmacSha256KeyStrategy FromHexKey(string keyHex)
    {
        if (string.IsNullOrWhiteSpace(keyHex))
        {
            throw new ArgumentException("Privacy key hex must not be empty.", nameof(keyHex));
        }

        return new HmacSha256KeyStrategy(Convert.FromHexString(keyHex));
    }
}
