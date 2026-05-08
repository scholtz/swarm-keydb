using System.Net.Http;
using System.Text;
using System.Text.Json;

namespace SwarmKeyDb;

/// <summary>
/// <see cref="IDecentralizedIdentityProvider"/> implementation for the <c>did:ethr</c> method.
///
/// <para>
/// DID resolution: a <c>did:ethr:[chainId:]0x&lt;address&gt;</c> string is resolved directly
/// from the embedded Ethereum address — no external registry call is required for basic resolution.
/// If a <see cref="EthrDidProviderOptions.RpcUrl"/> is configured, the provider can also look up
/// on-chain controller overrides via <c>eth_call</c> (not required for the happy path).
/// </para>
///
/// <para>
/// Authentication: the caller must present an Ethereum personal-sign proof (EIP-191).
/// The provider computes <c>keccak256("\x19Ethereum Signed Message:\n" + len + message)</c>,
/// recovers the signer address via secp256k1 ECDSA, and compares it against the address embedded in
/// the DID. No external RPC call is required for authentication.
/// </para>
/// </summary>
public sealed class EthrDidProvider : IDecentralizedIdentityProvider
{
    private const string DidMethod = "ethr";
    private const string VerificationMethodType = "EcdsaSecp256k1RecoveryMethod2020";
    private const string EthereumPersonalSignPrefix = "\x19Ethereum Signed Message:\n";

    private readonly EthrDidProviderOptions _options;
    private readonly HttpClient _http;

    /// <param name="options">Configuration options for this provider.</param>
    /// <param name="httpClient">Optional HTTP client (injected for testability). If <see langword="null"/>, a shared default is used.</param>
    public EthrDidProvider(EthrDidProviderOptions? options = null, HttpClient? httpClient = null)
    {
        _options = options ?? new EthrDidProviderOptions();
        _http = httpClient ?? new HttpClient();
    }

    /// <inheritdoc/>
    public Task<DidDocument?> ResolveAsync(string did, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(did))
        {
            return Task.FromResult<DidDocument?>(null);
        }

        if (!TryParseDid(did, out var address))
        {
            return Task.FromResult<DidDocument?>(null);
        }

        var doc = BuildDocument(did, address!);
        return Task.FromResult<DidDocument?>(doc);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Verifies an Ethereum personal-sign proof (EIP-191).  The <see cref="DidProof.Signature"/>
    /// must be a 65-byte hex string (0x-prefixed) containing <c>r || s || v</c>.
    /// </remarks>
    public Task<bool> AuthenticateAsync(string did, DidProof proof, CancellationToken cancellationToken = default)
    {
        if (!TryParseDid(did, out var expectedAddress))
        {
            return Task.FromResult(false);
        }

        byte[] sigBytes;
        try
        {
            var sigHex = proof.Signature.StartsWith("0x", StringComparison.OrdinalIgnoreCase)
                ? proof.Signature[2..]
                : proof.Signature;
            sigBytes = Convert.FromHexString(sigHex);
        }
        catch (FormatException)
        {
            return Task.FromResult(false);
        }

        if (sigBytes.Length != 65)
        {
            return Task.FromResult(false);
        }

        var messageHash = ComputePersonalSignHash(proof.Message);
        var recovered = Secp256k1.RecoverAddress(messageHash, sigBytes);
        if (recovered is null)
        {
            return Task.FromResult(false);
        }

        var matches = string.Equals(
            EthereumAddress.Normalize(recovered),
            EthereumAddress.Normalize(expectedAddress!),
            StringComparison.OrdinalIgnoreCase);

        return Task.FromResult(matches);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Default policy: any authenticated DID is allowed to perform any operation on any key.
    /// Override or wrap with <see cref="VerifiableCredentialAclPolicy"/> for fine-grained control.
    /// </remarks>
    public Task<bool> CheckPermissionAsync(
        string did,
        string key,
        DidOperation operation,
        CancellationToken cancellationToken = default)
    {
        // Default: authenticated DIDs have full access.
        // Callers can compose with VerifiableCredentialAclPolicy for VC-based restrictions.
        return Task.FromResult(!string.IsNullOrWhiteSpace(did));
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /// <summary>
    /// Parses a <c>did:ethr:[chainId:]0x&lt;address&gt;</c> string and extracts the Ethereum address.
    /// </summary>
    private static bool TryParseDid(string did, out string? address)
    {
        address = null;
        // did:ethr:0x...  OR  did:ethr:<chainId>:0x...
        var parts = did.Split(':');
        if (parts.Length < 3)
        {
            return false;
        }

        if (!string.Equals(parts[0], "did", StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(parts[1], DidMethod, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        // The address segment is the last part (may be preceded by a chain id).
        var addressSegment = parts[^1];
        try
        {
            address = EthereumAddress.Normalize(addressSegment);
            return true;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    private static DidDocument BuildDocument(string did, string address)
    {
        var vmId = $"{did}#controller";
        return new DidDocument
        {
            Did = did,
            Controllers = [did],
            VerificationMethods =
            [
                new DidVerificationMethod
                {
                    Id = vmId,
                    Type = VerificationMethodType,
                    Controller = did,
                    BlockchainAccountId = $"eip155:{address}"
                }
            ]
        };
    }

    /// <summary>
    /// Computes the EIP-191 personal-sign hash:
    /// <c>keccak256("\x19Ethereum Signed Message:\n" + len(message) + message)</c>.
    /// </summary>
    private static byte[] ComputePersonalSignHash(string message)
    {
        var messageBytes = Encoding.UTF8.GetBytes(message);
        var prefix = Encoding.UTF8.GetBytes(EthereumPersonalSignPrefix + messageBytes.Length);
        var prefixed = new byte[prefix.Length + messageBytes.Length];
        Buffer.BlockCopy(prefix, 0, prefixed, 0, prefix.Length);
        Buffer.BlockCopy(messageBytes, 0, prefixed, prefix.Length, messageBytes.Length);
        return KeccakHash.Compute(prefixed);
    }
}
