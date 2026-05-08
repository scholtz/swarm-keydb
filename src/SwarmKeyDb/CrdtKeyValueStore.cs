using System.Collections.Concurrent;
using System.Text.Json;

namespace SwarmKeyDb;

/// <summary>
/// Decorator that stores values as CRDT envelopes and merges concurrent writes deterministically.
/// </summary>
public sealed class CrdtKeyValueStore : IKeyValueStore, IAccessControlVerifier
{
    private const string EnvelopeType = "swarm-keydb/crdt-v1";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private readonly IKeyValueStore _inner;
    private readonly string _nodeId;
    private readonly ConcurrentDictionary<string, KeyOptions> _keyOptions = new(StringComparer.Ordinal);

    public CrdtKeyValueStore(IKeyValueStore inner, string? nodeId = null)
    {
        _inner = inner;
        _nodeId = string.IsNullOrWhiteSpace(nodeId) ? Environment.MachineName + ":" + Guid.NewGuid().ToString("N") : nodeId;
    }

    public Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        PutWithStrategyAsync(key, value, GetKeyStrategy(key), cancellationToken);

    public async Task PutWithStrategyAsync(
        string key,
        ReadOnlyMemory<byte> value,
        IMergeStrategy mergeStrategy,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(mergeStrategy);

        var existing = await ReadEnvelopeAsync(key, cancellationToken).ConfigureAwait(false);
        var incoming = BuildIncoming(existing, value.ToArray());
        var merged = mergeStrategy.Merge(key, existing?.Value, incoming);
        await _inner.PutAsync(key, SerializeEnvelope(merged, mergeStrategy.Name), cancellationToken).ConfigureAwait(false);
    }

    public Task MergeAsync(string key, ReadOnlyMemory<byte> incomingValue, CancellationToken cancellationToken = default) =>
        PutAsync(key, incomingValue, cancellationToken);

    public Task SetKeyOptionsAsync(string key, KeyOptions options, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(options);
        _keyOptions[key] = options;
        return Task.CompletedTask;
    }

    public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        var bytes = await _inner.GetAsync(key, cancellationToken).ConfigureAwait(false);
        if (bytes is null)
        {
            return null;
        }

        return TryDeserializeEnvelope(bytes, out var envelope)
            ? envelope.Value.Value
            : bytes;
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

    private IMergeStrategy GetKeyStrategy(string key) =>
        _keyOptions.TryGetValue(key, out var options) && options.MergeStrategy is not null
            ? options.MergeStrategy
            : LwwRegisterMergeStrategy.Instance;

    private async Task<StoredEnvelope?> ReadEnvelopeAsync(string key, CancellationToken cancellationToken)
    {
        var existingBytes = await _inner.GetAsync(key, cancellationToken).ConfigureAwait(false);
        if (existingBytes is null)
        {
            return null;
        }

        if (TryDeserializeEnvelope(existingBytes, out var envelope))
        {
            return envelope;
        }

        return new StoredEnvelope(
            new CrdtValue(existingBytes, VectorClock.Empty, DateTimeOffset.UnixEpoch, string.Empty),
            LwwRegisterMergeStrategy.Instance.Name);
    }

    private CrdtValue BuildIncoming(StoredEnvelope? existing, byte[] value)
    {
        var baseClock = existing?.Value.VectorClock ?? VectorClock.Empty;
        var clock = baseClock.Increment(_nodeId);
        return new CrdtValue(value, clock, DateTimeOffset.UtcNow, _nodeId);
    }

    private static byte[] SerializeEnvelope(CrdtValue value, string mergeStrategy)
    {
        var envelope = new SerializedEnvelope(
            EnvelopeType,
            mergeStrategy,
            value.Value,
            value.VectorClock.Entries,
            value.TimestampUtc,
            value.WriterNodeId);
        return JsonSerializer.SerializeToUtf8Bytes(envelope, JsonOptions);
    }

    private static bool TryDeserializeEnvelope(byte[] data, out StoredEnvelope envelope)
    {
        try
        {
            var serialized = JsonSerializer.Deserialize<SerializedEnvelope>(data, JsonOptions);
            if (serialized is null || !string.Equals(serialized.Type, EnvelopeType, StringComparison.Ordinal))
            {
                envelope = new StoredEnvelope(
                    new CrdtValue(Array.Empty<byte>(), VectorClock.Empty, DateTimeOffset.UnixEpoch, string.Empty),
                    LwwRegisterMergeStrategy.Instance.Name);
                return false;
            }

            envelope = new StoredEnvelope(
                new CrdtValue(
                    serialized.Value ?? [],
                    new VectorClock(serialized.VectorClock ?? new Dictionary<string, long>(StringComparer.Ordinal)),
                    serialized.TimestampUtc,
                    serialized.WriterNodeId ?? string.Empty),
                serialized.MergeStrategy ?? LwwRegisterMergeStrategy.Instance.Name);
            return true;
        }
        catch (JsonException)
        {
            envelope = new StoredEnvelope(
                new CrdtValue(Array.Empty<byte>(), VectorClock.Empty, DateTimeOffset.UnixEpoch, string.Empty),
                LwwRegisterMergeStrategy.Instance.Name);
            return false;
        }
    }

    private sealed record SerializedEnvelope(
        string Type,
        string MergeStrategy,
        byte[] Value,
        IReadOnlyDictionary<string, long> VectorClock,
        DateTimeOffset TimestampUtc,
        string WriterNodeId);

    private sealed record StoredEnvelope(CrdtValue Value, string MergeStrategy);
}
