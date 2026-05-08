using Microsoft.Extensions.Options;

namespace SwarmKeyDb;

public sealed class AclKeyValueStore : IKeyValueStore
{
    private readonly IKeyValueStore _inner;
    private readonly IEthAddressAccessor _ethAddressAccessor;
    private readonly bool _enabled;
    private readonly AclMode _mode;
    private readonly IReadOnlyDictionary<string, AclPermission> _entries;

    public AclKeyValueStore(IKeyValueStore inner, IOptions<AclOptions> options, IEthAddressAccessor ethAddressAccessor)
    {
        _inner = inner;
        _ethAddressAccessor = ethAddressAccessor;

        var aclOptions = options.Value;
        _enabled = aclOptions.Enabled;
        _mode = aclOptions.Mode;
        _entries = BuildEntries(aclOptions);
    }

    public async Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        EnsurePermission(Operation.Write);
        await _inner.PutAsync(key, value, cancellationToken).ConfigureAwait(false);
    }

    public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        EnsurePermission(Operation.Read);
        return await _inner.GetAsync(key, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        EnsurePermission(Operation.Write);
        return await _inner.DeleteAsync(key, cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default)
    {
        EnsurePermission(Operation.Read);
        return await _inner.ListKeysAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> SetTtlAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        EnsurePermission(Operation.Write);
        return await _inner.SetTtlAsync(key, ttl, cancellationToken).ConfigureAwait(false);
    }

    public async Task<(bool Exists, TimeSpan? Ttl)> GetTtlAsync(string key, CancellationToken cancellationToken = default)
    {
        EnsurePermission(Operation.Read);
        return await _inner.GetTtlAsync(key, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> RemoveTtlAsync(string key, CancellationToken cancellationToken = default)
    {
        EnsurePermission(Operation.Write);
        return await _inner.RemoveTtlAsync(key, cancellationToken).ConfigureAwait(false);
    }

    private void EnsurePermission(Operation operation)
    {
        if (!_enabled)
        {
            return;
        }

        var rawAddress = _ethAddressAccessor.CurrentAddress;
        if (string.IsNullOrWhiteSpace(rawAddress))
        {
            throw new AccessDeniedException("Access denied: missing caller Ethereum address. Provide X-Eth-Address or AUTHADDR.");
        }

        string address;
        try
        {
            address = EthereumAddress.Normalize(rawAddress);
        }
        catch (ArgumentException)
        {
            throw new AccessDeniedException($"Access denied: invalid Ethereum address '{rawAddress}'.");
        }

        var hasEntry = _entries.TryGetValue(address, out var permission);
        var allowed = _mode switch
        {
            AclMode.Allowlist => hasEntry && Allows(permission, operation),
            AclMode.Denylist => !hasEntry || !Allows(permission, operation),
            _ => false
        };

        if (!allowed)
        {
            throw new AccessDeniedException($"Access denied: address {address} does not have {operation.ToString().ToLowerInvariant()} permission.");
        }
    }

    private static bool Allows(AclPermission permission, Operation operation) =>
        permission == AclPermission.Admin ||
        (operation == Operation.Read && permission == AclPermission.Read) ||
        (operation == Operation.Write && permission == AclPermission.Write);

    private static IReadOnlyDictionary<string, AclPermission> BuildEntries(AclOptions options)
    {
        if (!options.Enabled)
        {
            return new Dictionary<string, AclPermission>(StringComparer.Ordinal);
        }

        if (options.Entries.Count == 0)
        {
            throw new InvalidOperationException(
                "ACL is enabled (SWARM_KEYDB_ACL_ENABLED=true) but SWARM_KEYDB_ACL_ENTRIES is empty or invalid. " +
                "Configure a JSON array of {\"address\":\"0x...\",\"permission\":\"read|write|admin\"} entries.");
        }

        var entries = new Dictionary<string, AclPermission>(StringComparer.Ordinal);
        foreach (var entry in options.Entries)
        {
            var normalizedAddress = EthereumAddress.Normalize(entry.EthAddress);
            if (!entries.TryAdd(normalizedAddress, entry.Permission))
            {
                throw new InvalidOperationException($"ACL contains duplicate entry for Ethereum address {normalizedAddress}.");
            }
        }

        return entries;
    }

    private enum Operation
    {
        Read,
        Write
    }
}
