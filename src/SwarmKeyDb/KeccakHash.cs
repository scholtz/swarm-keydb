namespace SwarmKeyDb;

/// <summary>
/// Minimal Keccak-256 implementation — Ethereum's native hash function.
/// This is NOT the NIST SHA3-256; it uses the original Keccak padding (0x01)
/// rather than the NIST SHA3 padding (0x06).
///
/// Reference: https://keccak.team/keccak_specs_summary.html
/// </summary>
public static class KeccakHash
{
    // Keccak-f[1600] round constants (iota step)
    private static readonly ulong[] RoundConstants =
    {
        0x0000000000000001UL, 0x0000000000008082UL, 0x800000000000808AUL,
        0x8000000080008000UL, 0x000000000000808BUL, 0x0000000080000001UL,
        0x8000000080008081UL, 0x8000000000008009UL, 0x000000000000008AUL,
        0x0000000000000088UL, 0x0000000080008009UL, 0x000000008000000AUL,
        0x000000008000808BUL, 0x800000000000008BUL, 0x8000000000008089UL,
        0x8000000000008003UL, 0x8000000000008002UL, 0x8000000000000080UL,
        0x000000000000800AUL, 0x800000008000000AUL, 0x8000000080008081UL,
        0x8000000000008080UL, 0x0000000080000001UL, 0x8000000080008008UL
    };

    /// <summary>Computes the Keccak-256 hash of UTF-8 encoded <paramref name="input"/>.</summary>
    public static string ComputeHex(string input) =>
        Convert.ToHexString(Compute(System.Text.Encoding.UTF8.GetBytes(input))).ToLowerInvariant();

    /// <summary>Computes the Keccak-256 hash of <paramref name="input"/> bytes.</summary>
    public static byte[] Compute(byte[] input)
    {
        const int rate = 136;      // 1088 bits / 8 = 136 bytes (for 256-bit output)
        const int hashLen = 32;    // 256 bits / 8

        // Pad: append 0x01 at input.Length, then 0x80 at the last byte of the padded block.
        int paddedLen = ((input.Length / rate) + 1) * rate;
        var padded = new byte[paddedLen];
        Buffer.BlockCopy(input, 0, padded, 0, input.Length);
        padded[input.Length] ^= 0x01;       // Keccak domain separator (NOT 0x06 for SHA3)
        padded[paddedLen - 1] ^= 0x80;

        // Absorb
        var state = new ulong[25];
        for (int offset = 0; offset < paddedLen; offset += rate)
        {
            for (int i = 0; i < rate / 8; i++)
            {
                state[i] ^= ReadLaneLE(padded, offset + i * 8);
            }

            KeccakF1600(state);
        }

        // Squeeze (first 32 bytes of state = first 4 lanes, little-endian)
        var hash = new byte[hashLen];
        for (int i = 0; i < 4; i++)
        {
            WriteLaneLE(state[i], hash, i * 8);
        }

        return hash;
    }

    // Read a 64-bit lane in little-endian byte order
    private static ulong ReadLaneLE(byte[] data, int offset) =>
        (ulong)data[offset]
        | ((ulong)data[offset + 1] << 8)
        | ((ulong)data[offset + 2] << 16)
        | ((ulong)data[offset + 3] << 24)
        | ((ulong)data[offset + 4] << 32)
        | ((ulong)data[offset + 5] << 40)
        | ((ulong)data[offset + 6] << 48)
        | ((ulong)data[offset + 7] << 56);

    // Write a 64-bit lane in little-endian byte order
    private static void WriteLaneLE(ulong lane, byte[] output, int offset)
    {
        output[offset]     = (byte)lane;
        output[offset + 1] = (byte)(lane >> 8);
        output[offset + 2] = (byte)(lane >> 16);
        output[offset + 3] = (byte)(lane >> 24);
        output[offset + 4] = (byte)(lane >> 32);
        output[offset + 5] = (byte)(lane >> 40);
        output[offset + 6] = (byte)(lane >> 48);
        output[offset + 7] = (byte)(lane >> 56);
    }

    private static ulong RotL(ulong x, int n) => (x << n) | (x >> (64 - n));

    // Swap helper: atomically assigns `v` to `target`, returning original value of `target`.
    private static ulong Swap(ref ulong target, ulong v)
    {
        var old = target;
        target = v;
        return old;
    }

    // Keccak-f[1600] permutation — 24 rounds of θ, ρ+π, χ, ι
    private static void KeccakF1600(ulong[] s)
    {
        ulong t, c0, c1, c2, c3, c4;

        for (int r = 0; r < 24; r++)
        {
            // θ step — mix each column
            c0 = s[0] ^ s[5] ^ s[10] ^ s[15] ^ s[20];
            c1 = s[1] ^ s[6] ^ s[11] ^ s[16] ^ s[21];
            c2 = s[2] ^ s[7] ^ s[12] ^ s[17] ^ s[22];
            c3 = s[3] ^ s[8] ^ s[13] ^ s[18] ^ s[23];
            c4 = s[4] ^ s[9] ^ s[14] ^ s[19] ^ s[24];

            t = c4 ^ RotL(c1, 1); s[0] ^= t; s[5] ^= t; s[10] ^= t; s[15] ^= t; s[20] ^= t;
            t = c0 ^ RotL(c2, 1); s[1] ^= t; s[6] ^= t; s[11] ^= t; s[16] ^= t; s[21] ^= t;
            t = c1 ^ RotL(c3, 1); s[2] ^= t; s[7] ^= t; s[12] ^= t; s[17] ^= t; s[22] ^= t;
            t = c2 ^ RotL(c4, 1); s[3] ^= t; s[8] ^= t; s[13] ^= t; s[18] ^= t; s[23] ^= t;
            t = c3 ^ RotL(c0, 1); s[4] ^= t; s[9] ^= t; s[14] ^= t; s[19] ^= t; s[24] ^= t;

            // ρ + π steps combined via the "last lane" trick.
            // The 24 non-origin lanes are processed in a single cycle determined by π:
            // (x,y) → (y, (2x+3y) mod 5), starting from (1,0).
            // Each lane is rotated by its ρ offset before being placed at its π destination.
            t = s[1];
            t = Swap(ref s[10], RotL(t,  1));
            t = Swap(ref s[ 7], RotL(t,  3));
            t = Swap(ref s[11], RotL(t,  6));
            t = Swap(ref s[17], RotL(t, 10));
            t = Swap(ref s[18], RotL(t, 15));
            t = Swap(ref s[ 3], RotL(t, 21));
            t = Swap(ref s[ 5], RotL(t, 28));
            t = Swap(ref s[16], RotL(t, 36));
            t = Swap(ref s[ 8], RotL(t, 45));
            t = Swap(ref s[21], RotL(t, 55));
            t = Swap(ref s[24], RotL(t,  2));
            t = Swap(ref s[ 4], RotL(t, 14));
            t = Swap(ref s[15], RotL(t, 27));
            t = Swap(ref s[23], RotL(t, 41));
            t = Swap(ref s[19], RotL(t, 56));
            t = Swap(ref s[13], RotL(t,  8));
            t = Swap(ref s[12], RotL(t, 25));
            t = Swap(ref s[ 2], RotL(t, 43));
            t = Swap(ref s[20], RotL(t, 62));
            t = Swap(ref s[14], RotL(t, 18));
            t = Swap(ref s[22], RotL(t, 39));
            t = Swap(ref s[ 9], RotL(t, 61));
            t = Swap(ref s[ 6], RotL(t, 20));
            s[1] = RotL(t, 44);

            // χ step — non-linear layer
            for (int j = 0; j < 25; j += 5)
            {
                c0 = s[j]; c1 = s[j + 1]; c2 = s[j + 2]; c3 = s[j + 3]; c4 = s[j + 4];
                s[j]     = c0 ^ (~c1 & c2);
                s[j + 1] = c1 ^ (~c2 & c3);
                s[j + 2] = c2 ^ (~c3 & c4);
                s[j + 3] = c3 ^ (~c4 & c0);
                s[j + 4] = c4 ^ (~c0 & c1);
            }

            // ι step — add round constant
            s[0] ^= RoundConstants[r];
        }
    }
}
