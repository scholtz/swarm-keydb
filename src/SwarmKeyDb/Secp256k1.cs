using System.Numerics;

namespace SwarmKeyDb;

/// <summary>
/// Minimal secp256k1 ECDSA primitives required to recover an Ethereum address from a signature.
/// This implements the standard algorithm described in SEC 1 v2.0 §4.1.6.
/// </summary>
internal static class Secp256k1
{
    // ── Curve parameters ─────────────────────────────────────────────────────
    // p  — field prime
    private static readonly BigInteger P = BigInteger.Parse(
        "00FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F",
        System.Globalization.NumberStyles.HexNumber);

    // n  — curve order
    private static readonly BigInteger N = BigInteger.Parse(
        "00FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141",
        System.Globalization.NumberStyles.HexNumber);

    // Generator point G
    private static readonly BigInteger Gx = BigInteger.Parse(
        "0079BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798",
        System.Globalization.NumberStyles.HexNumber);

    private static readonly BigInteger Gy = BigInteger.Parse(
        "00483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8",
        System.Globalization.NumberStyles.HexNumber);

    // Infinity sentinel
    private static readonly (BigInteger X, BigInteger Y) Infinity = (BigInteger.MinusOne, BigInteger.MinusOne);

    // ── Public API ────────────────────────────────────────────────────────────

    /// <summary>
    /// Recovers the lowercase 0x-prefixed Ethereum address that produced
    /// an Ethereum personal-sign signature over <paramref name="messageHash"/>.
    /// </summary>
    /// <param name="messageHash">32-byte Keccak-256 hash of the prefixed message.</param>
    /// <param name="signature">65-byte signature: r (32) ‖ s (32) ‖ v (1).  v may be 0/1 or 27/28.</param>
    /// <returns>Recovered address on success; <see langword="null"/> on failure.</returns>
    public static string? RecoverAddress(byte[] messageHash, byte[] signature)
    {
        if (messageHash.Length != 32 || signature.Length != 65)
        {
            return null;
        }

        var r = new BigInteger(signature.AsSpan(0, 32), isUnsigned: true, isBigEndian: true);
        var s = new BigInteger(signature.AsSpan(32, 32), isUnsigned: true, isBigEndian: true);
        var v = signature[64];
        if (v >= 27)
        {
            v -= 27;
        }

        if (v != 0 && v != 1)
        {
            return null;
        }

        if (r <= BigInteger.Zero || r >= N || s <= BigInteger.Zero || s >= N)
        {
            return null;
        }

        var z = new BigInteger(messageHash, isUnsigned: true, isBigEndian: true);

        // R = the secp256k1 point whose x-coordinate is r and whose y-parity matches v.
        var y = ComputeY(r, odd: v == 1);
        if (!y.HasValue)
        {
            return null;
        }

        var R = (X: r, Y: y.Value);

        // Q = r⁻¹ · (s · R − z · G)
        var rInv = ModInverse(r, N);
        var sR = PointMul(R, s);
        var zG = PointMul((Gx, Gy), (N - (z % N)) % N);
        var diff = PointAdd(sR, zG);
        var Q = PointMul(diff, rInv);

        if (Q == Infinity)
        {
            return null;
        }

        // Encode uncompressed public key (64 bytes, no 0x04 prefix).
        var qBytes = new byte[64];
        CopyBigEndian(Q.X, qBytes.AsSpan(0, 32));
        CopyBigEndian(Q.Y, qBytes.AsSpan(32, 32));

        // Ethereum address = last 20 bytes of keccak256(qBytes).
        var hash = KeccakHash.Compute(qBytes);
        return "0x" + Convert.ToHexString(hash.AsSpan(12)).ToLowerInvariant();
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private static BigInteger? ComputeY(BigInteger x, bool odd)
    {
        // y² = x³ + 7  (mod P)
        var y2 = (BigInteger.ModPow(x, 3, P) + 7) % P;
        // Candidate square root: y = y2^((P+1)/4) mod P  (works because P ≡ 3 mod 4)
        var y = BigInteger.ModPow(y2, (P + 1) / 4, P);
        if (BigInteger.ModPow(y, 2, P) != y2)
        {
            return null; // x is not on the curve
        }

        var isOdd = y % 2 != 0;
        if (isOdd != odd)
        {
            y = P - y;
        }

        return y;
    }

    private static (BigInteger X, BigInteger Y) PointAdd(
        (BigInteger X, BigInteger Y) p1,
        (BigInteger X, BigInteger Y) p2)
    {
        if (p1 == Infinity)
        {
            return p2;
        }

        if (p2 == Infinity)
        {
            return p1;
        }

        if (p1.X == p2.X)
        {
            if ((p1.Y + p2.Y) % P == 0)
            {
                return Infinity; // point at infinity
            }

            return PointDouble(p1);
        }

        var lam = ModMul(p2.Y - p1.Y, ModInverse(p2.X - p1.X, P), P);
        var x3 = (lam * lam - p1.X - p2.X) % P;
        if (x3 < 0)
        {
            x3 += P;
        }

        var y3 = (lam * (p1.X - x3) - p1.Y) % P;
        if (y3 < 0)
        {
            y3 += P;
        }

        return (x3, y3);
    }

    private static (BigInteger X, BigInteger Y) PointDouble((BigInteger X, BigInteger Y) p)
    {
        if (p == Infinity)
        {
            return Infinity;
        }

        var lam = ModMul(3 * p.X * p.X, ModInverse(2 * p.Y, P), P);
        var x3 = (lam * lam - 2 * p.X) % P;
        if (x3 < 0)
        {
            x3 += P;
        }

        var y3 = (lam * (p.X - x3) - p.Y) % P;
        if (y3 < 0)
        {
            y3 += P;
        }

        return (x3, y3);
    }

    private static (BigInteger X, BigInteger Y) PointMul(
        (BigInteger X, BigInteger Y) p,
        BigInteger k)
    {
        k %= N;
        if (k < 0)
        {
            k += N;
        }

        var result = Infinity;
        var addend = p;
        while (k > BigInteger.Zero)
        {
            if ((k & 1) == 1)
            {
                result = PointAdd(result, addend);
            }

            addend = PointDouble(addend);
            k >>= 1;
        }

        return result;
    }

    /// <summary>Modular inverse using Fermat's little theorem (valid because P and N are prime).</summary>
    private static BigInteger ModInverse(BigInteger a, BigInteger m)
    {
        a %= m;
        if (a < 0)
        {
            a += m;
        }

        return BigInteger.ModPow(a, m - 2, m);
    }

    /// <summary>Modular multiplication that correctly handles negative intermediates.</summary>
    private static BigInteger ModMul(BigInteger a, BigInteger b, BigInteger m)
    {
        var r = a * b % m;
        if (r < 0)
        {
            r += m;
        }

        return r;
    }

    /// <summary>Writes <paramref name="value"/> into <paramref name="dest"/> as big-endian, zero-padded.</summary>
    private static void CopyBigEndian(BigInteger value, Span<byte> dest)
    {
        dest.Clear();
        var bytes = value.ToByteArray(isUnsigned: true, isBigEndian: true);
        if (bytes.Length > dest.Length)
        {
            // Trim leading zeros (should not happen for valid curve points)
            bytes.AsSpan(bytes.Length - dest.Length).CopyTo(dest);
        }
        else
        {
            bytes.CopyTo(dest[(dest.Length - bytes.Length)..]);
        }
    }
}
