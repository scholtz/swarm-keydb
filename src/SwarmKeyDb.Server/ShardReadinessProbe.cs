using SwarmKeyDb;

namespace SwarmKeyDb.Server;

public sealed record ShardHealthStatus(string Shard, bool Ready, string Message, int? KeyCount);

public interface IShardHealthProvider
{
    Task<IReadOnlyList<ShardHealthStatus>> GetShardHealthAsync(CancellationToken cancellationToken = default);
}

public sealed record ShardReadinessRegistration(string Shard, IReadinessProbe Probe, IKeyValueStore Store);

public sealed class CompositeShardReadinessProbe : IReadinessProbe, IShardHealthProvider
{
    private readonly IReadOnlyList<ShardReadinessRegistration> _registrations;

    public CompositeShardReadinessProbe(IEnumerable<ShardReadinessRegistration> registrations)
    {
        ArgumentNullException.ThrowIfNull(registrations);
        _registrations = registrations.ToArray();
        if (_registrations.Count == 0)
        {
            throw new ArgumentException("At least one shard readiness registration is required.", nameof(registrations));
        }
    }

    public async Task<(bool Ready, string Message)> CheckAsync(CancellationToken cancellationToken = default)
    {
        var shardHealth = await GetShardHealthAsync(cancellationToken).ConfigureAwait(false);
        var failed = shardHealth.Count(static shard => !shard.Ready);
        return failed == 0
            ? (true, "all shards ready")
            : (false, $"{failed} shard(s) unavailable");
    }

    public async Task<IReadOnlyList<ShardHealthStatus>> GetShardHealthAsync(CancellationToken cancellationToken = default)
    {
        var checks = _registrations.Select(registration => CheckShardAsync(registration, cancellationToken)).ToArray();
        var statuses = await Task.WhenAll(checks).ConfigureAwait(false);
        return statuses.OrderBy(static status => status.Shard, StringComparer.Ordinal).ToArray();
    }

    private static async Task<ShardHealthStatus> CheckShardAsync(ShardReadinessRegistration registration, CancellationToken cancellationToken)
    {
        var (ready, message) = await registration.Probe.CheckAsync(cancellationToken).ConfigureAwait(false);
        int? keyCount = null;
        try
        {
            var keys = await registration.Store.ListKeysAsync(cancellationToken).ConfigureAwait(false);
            keyCount = keys.Count;
        }
        catch (Exception ex)
        {
            ready = false;
            message = $"key listing failed for shard '{registration.Shard}': {ex.Message}";
        }

        return new ShardHealthStatus(registration.Shard, ready, message, keyCount);
    }
}
