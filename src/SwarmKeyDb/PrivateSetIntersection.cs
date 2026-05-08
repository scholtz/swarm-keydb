using System.Security.Cryptography;
using System.Text;

namespace SwarmKeyDb;

public static class PrivateSetIntersection
{
    public static IReadOnlyList<string> BuildBlindSet(IEnumerable<string> keyTokens, string sessionSalt)
    {
        ArgumentNullException.ThrowIfNull(keyTokens);
        ArgumentNullException.ThrowIfNull(sessionSalt);

        var salt = Encoding.UTF8.GetBytes(sessionSalt);
        return keyTokens
            .Select(token => Convert.ToHexStringLower(SHA256.HashData(Combine(salt, Encoding.UTF8.GetBytes(token)))))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
    }

    public static IReadOnlyList<string> IntersectBlindSets(IEnumerable<string> left, IEnumerable<string> right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return left.Intersect(right, StringComparer.Ordinal).ToArray();
    }

    private static byte[] Combine(byte[] left, byte[] right)
    {
        var output = new byte[left.Length + right.Length];
        Buffer.BlockCopy(left, 0, output, 0, left.Length);
        Buffer.BlockCopy(right, 0, output, left.Length, right.Length);
        return output;
    }
}
