using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using SwarmKeyDb;
using SwarmKeyDb.Migrate;
using SwarmKeyDb.Server;
using SwarmKeyDb.SwarmConsistency;

namespace SwarmKeyDb.Tests;

internal sealed record Settings(bool Enabled, int Count);

internal sealed class StubHttpMessageHandler : HttpMessageHandler
{
    private readonly Func<HttpRequestMessage, HttpResponseMessage> _handler;

    public StubHttpMessageHandler(Func<HttpRequestMessage, HttpResponseMessage> handler)
    {
        _handler = handler;
    }

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
        Task.FromResult(_handler(request));
}

internal sealed class MutableSwarmClient : ISwarmClient
{
    private readonly Dictionary<string, byte[]> _objects = new(StringComparer.Ordinal);

    public Task<string> UploadAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var bytes = data.ToArray();
        var reference = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(bytes)).ToLowerInvariant();
        _objects[reference] = bytes;
        return Task.FromResult(reference);
    }

    public Task<byte[]> DownloadAsync(string reference, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!_objects.TryGetValue(reference, out var data))
        {
            throw new KeyNotFoundException($"Swarm reference '{reference}' was not found.");
        }

        return Task.FromResult(data.ToArray());
    }

    public void Corrupt(string reference, Func<byte[], byte[]> mutator)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reference);
        ArgumentNullException.ThrowIfNull(mutator);

        if (!_objects.TryGetValue(reference, out var data))
        {
            throw new KeyNotFoundException($"Swarm reference '{reference}' was not found.");
        }

        _objects[reference] = mutator(data.ToArray());
    }

    public void Remove(string reference)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reference);
        _objects.Remove(reference);
    }
}

internal sealed class FailingSwarmClient : ISwarmClient
{
    private readonly Exception _uploadException;
    private readonly Exception _downloadException;

    public FailingSwarmClient(Exception uploadException, Exception? downloadException = null)
    {
        _uploadException = uploadException;
        _downloadException = downloadException ?? uploadException;
    }

    public Task<string> UploadAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
        Task.FromException<string>(_uploadException);

    public Task<byte[]> DownloadAsync(string reference, CancellationToken cancellationToken = default) =>
        Task.FromException<byte[]>(_downloadException);
}

internal sealed class CountingKeyValueStore : IKeyValueStore
{
    private readonly Dictionary<string, byte[]> _values = new(StringComparer.Ordinal);
    private readonly Dictionary<string, DateTimeOffset> _expiries = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _getCalls = new(StringComparer.Ordinal);
    private readonly object _sync = new();

    public Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        lock (_sync)
        {
            _values[key] = value.ToArray();
            _expiries.Remove(key);
        }

        return Task.CompletedTask;
    }

    public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        lock (_sync)
        {
            _getCalls[key] = GetCallCount(key) + 1;
            if (_expiries.TryGetValue(key, out var expiresAt) && expiresAt <= DateTimeOffset.UtcNow)
            {
                _values.Remove(key);
                _expiries.Remove(key);
                return Task.FromResult<byte[]?>(null);
            }

            return Task.FromResult(_values.TryGetValue(key, out var value) ? value.ToArray() : null);
        }
    }

    public Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        lock (_sync)
        {
            var removed = _values.Remove(key);
            _expiries.Remove(key);
            return Task.FromResult(removed);
        }
    }

    public Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default)
    {
        lock (_sync)
        {
            return Task.FromResult<IReadOnlyList<string>>(_values.Keys.OrderBy(static key => key, StringComparer.Ordinal).ToArray());
        }
    }

    public Task<bool> SetTtlAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        lock (_sync)
        {
            if (!_values.ContainsKey(key))
            {
                return Task.FromResult(false);
            }

            _expiries[key] = DateTimeOffset.UtcNow.Add(ttl);
            return Task.FromResult(true);
        }
    }

    public Task<(bool Exists, TimeSpan? Ttl)> GetTtlAsync(string key, CancellationToken cancellationToken = default)
    {
        lock (_sync)
        {
            if (!_values.ContainsKey(key))
            {
                return Task.FromResult((false, (TimeSpan?)null));
            }

            if (!_expiries.TryGetValue(key, out var expiresAt))
            {
                return Task.FromResult((true, (TimeSpan?)null));
            }

            return Task.FromResult((true, (TimeSpan?)(expiresAt - DateTimeOffset.UtcNow)));
        }
    }

    public Task<bool> RemoveTtlAsync(string key, CancellationToken cancellationToken = default)
    {
        lock (_sync)
        {
            return Task.FromResult(_expiries.Remove(key));
        }
    }

    public int GetCallCount(string key)
    {
        lock (_sync)
        {
            return _getCalls.TryGetValue(key, out var count) ? count : 0;
        }
    }
}

internal sealed class DelayedWriteKeyValueStore : IKeyValueStore
{
    private readonly int _writeDelayMs;
    private readonly Dictionary<string, byte[]> _values = new(StringComparer.Ordinal);
    private readonly object _sync = new();
    private int _activeWrites;
    private int _maxObservedConcurrentWrites;

    public DelayedWriteKeyValueStore(int writeDelayMs)
    {
        _writeDelayMs = writeDelayMs;
    }

    public int MaxObservedConcurrentWrites => Volatile.Read(ref _maxObservedConcurrentWrites);

    public async Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        var active = Interlocked.Increment(ref _activeWrites);
        while (true)
        {
            var currentMax = Volatile.Read(ref _maxObservedConcurrentWrites);
            if (active <= currentMax)
            {
                break;
            }

            if (Interlocked.CompareExchange(ref _maxObservedConcurrentWrites, active, currentMax) == currentMax)
            {
                break;
            }
        }

        try
        {
            await Task.Delay(_writeDelayMs, cancellationToken);
            lock (_sync)
            {
                _values[key] = value.ToArray();
            }
        }
        finally
        {
            Interlocked.Decrement(ref _activeWrites);
        }
    }

    public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default) =>
        Task.FromResult(GetValueCopy(key));

    public Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default) =>
        Task.FromResult(DeleteKey(key));

    public Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyList<string>>(ListKeysSnapshot());

    private byte[]? GetValueCopy(string key)
    {
        lock (_sync)
        {
            return _values.TryGetValue(key, out var value) ? value.ToArray() : null;
        }
    }

    private bool DeleteKey(string key)
    {
        lock (_sync)
        {
            return _values.Remove(key);
        }
    }

    private IReadOnlyList<string> ListKeysSnapshot()
    {
        lock (_sync)
        {
            return _values.Keys.OrderBy(static key => key, StringComparer.Ordinal).ToArray();
        }
    }
}

internal sealed class MetadataCountingStore : IKeyValueStore, IBackendMetadataProvider
{
    private readonly Dictionary<string, byte[]> _values = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _references = new(StringComparer.Ordinal);

    public Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        _values[key] = value.ToArray();
        return Task.CompletedTask;
    }

    public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default) =>
        Task.FromResult(_values.TryGetValue(key, out var value) ? value.ToArray() : null);

    public Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default) =>
        Task.FromResult(_values.Remove(key));

    public Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyList<string>>(_values.Keys.ToArray());

    public Task<string?> GetBackendMetadataAsync(string key, CancellationToken cancellationToken = default) =>
        Task.FromResult(_references.TryGetValue(key, out var reference) ? reference : null);

    public Task SetReferenceAsync(string key, string reference)
    {
        _references[key] = reference;
        return Task.CompletedTask;
    }
}

internal sealed class StaticConsistencyVerifier : ISwarmConsistencyVerifier
{
    private readonly VerificationResult _result;

    public StaticConsistencyVerifier(VerificationResult result)
    {
        _result = result;
    }

    public Task<VerificationResult> VerifyFeedRevisionAsync(string topic, ulong expectedRevision, CancellationToken ct) =>
        Task.FromResult(_result with { VerificationType = "feed-revision" });

    public Task<VerificationResult> VerifyContentHashAsync(string reference, byte[] expectedHash, CancellationToken ct) =>
        Task.FromResult(_result with { VerificationType = "content-hash" });

    public Task<VerificationResult> VerifyManifestLineageAsync(string manifestRef, IReadOnlyList<string> expectedAncestors, CancellationToken ct) =>
        Task.FromResult(_result with { VerificationType = "manifest-lineage" });
}

internal sealed class TestLogger<T> : Microsoft.Extensions.Logging.ILogger<T>
{
    public List<string> Messages { get; } = [];

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

    public void Log<TState>(
        Microsoft.Extensions.Logging.LogLevel logLevel,
        Microsoft.Extensions.Logging.EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        Messages.Add(formatter(state, exception));
    }
}

internal sealed class FlakyChainAdapter : IChainAdapter
{
    private readonly NamespacedChainAdapter _inner;
    private int _failuresRemaining;

    public FlakyChainAdapter(IKeyValueStore store, ChainAdapterOptions options, int failuresBeforeSuccess)
    {
        _inner = new NamespacedChainAdapter(store, options);
        _failuresRemaining = failuresBeforeSuccess;
        ChainId = _inner.ChainId;
        Name = _inner.Name;
        RpcUrl = _inner.RpcUrl;
        BridgeContractAddress = _inner.BridgeContractAddress;
    }

    public int ChainId { get; }
    public string Name { get; }
    public string? RpcUrl { get; }
    public string? BridgeContractAddress { get; }

    public string GetNamespacedKey(string key) => _inner.GetNamespacedKey(key);

    public Task DeleteAsync(string key, CancellationToken cancellationToken = default) =>
        _inner.DeleteAsync(key, cancellationToken);

    public async Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        if (_failuresRemaining-- > 0)
        {
            throw new InvalidOperationException("FlakyChainAdapter: simulated failure for testing.");
        }

        await _inner.PutAsync(key, value, cancellationToken).ConfigureAwait(false);
    }
}

internal sealed class FakeCacheStats : ICacheStats
{
    public long Hits { get; init; }
    public long Misses { get; init; }
    public long Evictions { get; init; }
}

internal sealed class StaticReadinessProbe : IReadinessProbe
{
    private readonly bool _ready;
    private readonly string _message;

    public StaticReadinessProbe(bool ready, string message)
    {
        _ready = ready;
        _message = message;
    }

    public Task<(bool Ready, string Message)> CheckAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult((_ready, _message));
}

internal sealed class StaticShardHealthProvider : IShardHealthProvider
{
    private readonly IReadOnlyList<ShardHealthStatus> _statuses;

    public StaticShardHealthProvider(IReadOnlyList<ShardHealthStatus> statuses)
    {
        _statuses = statuses;
    }

    public Task<IReadOnlyList<ShardHealthStatus>> GetShardHealthAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(_statuses);
}

internal sealed class StaticBackendStatusProvider : IBackendStatusProvider
{
    private readonly IReadOnlyList<BackendStatus> _statuses;

    public StaticBackendStatusProvider(IReadOnlyList<BackendStatus> statuses)
    {
        _statuses = statuses;
    }

    public Task<IReadOnlyList<BackendStatus>> GetStatusAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(_statuses);
}

internal sealed class StaticOfflineStatusProvider : IOfflineStatusProvider
{
    public StaticOfflineStatusProvider(long queueDepth, DateTimeOffset? lastSuccessfulSyncUtc, bool isOffline = false, OfflineMode mode = OfflineMode.Auto)
    {
        QueueDepth = queueDepth;
        LastSuccessfulSyncUtc = lastSuccessfulSyncUtc;
        IsOffline = isOffline;
        Mode = mode;
    }

    public long QueueDepth { get; }
    public DateTimeOffset? LastSuccessfulSyncUtc { get; }
    public bool IsOffline { get; }
    public OfflineMode Mode { get; }
}

internal sealed class StaticConsistencyVerificationStatusProvider : IConsistencyVerificationStatusProvider
{
    private readonly ConsistencyVerificationSnapshot _snapshot;

    public StaticConsistencyVerificationStatusProvider(ConsistencyVerificationSnapshot snapshot)
    {
        _snapshot = snapshot;
    }

    public ConsistencyVerificationSnapshot GetSnapshot() => _snapshot;
}

internal sealed class StaticCacheSyncStatusProvider : ICacheSyncStatusProvider
{
    private readonly CacheSyncSnapshot _snapshot;

    public StaticCacheSyncStatusProvider(CacheSyncSnapshot snapshot)
    {
        _snapshot = snapshot;
    }

    public CacheSyncSnapshot GetSnapshot() => _snapshot;
}

internal sealed class MutableCacheSyncStatusProvider : ICacheSyncStatusProvider
{
    private CacheSyncSnapshot _snapshot;

    public MutableCacheSyncStatusProvider(CacheSyncSnapshot snapshot)
    {
        _snapshot = snapshot;
    }

    public CacheSyncSnapshot GetSnapshot() => _snapshot;
}

internal sealed class RecordingResyncCoordinator : IResyncCoordinator
{
    private ResyncStatusSnapshot _snapshot = new(
        InProgress: false,
        CurrentMode: "idle",
        LastResyncUtc: null,
        LastMode: "none",
        KeysReplayedLastRun: 0,
        KeysReplayedTotal: 0,
        LastError: null,
        CorrelationId: null);

    public Action<ResyncLifecycleContext>? OnResyncStarted { get; set; }
    public Action<ResyncLifecycleContext>? OnResyncCompleted { get; set; }
    public Action<ResyncLifecycleContext>? OnResyncFailed { get; set; }

    public ResyncStatusSnapshot GetSnapshot() => _snapshot;

    public Task<ResyncOperationResult> TriggerResyncAsync(ResyncMode mode = ResyncMode.Auto, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var normalizedMode = mode == ResyncMode.Auto ? ResyncMode.Partial : mode;
        var startedAt = DateTimeOffset.UtcNow;
        var completedAt = startedAt.AddMilliseconds(50);
        var correlationId = Guid.NewGuid().ToString("N");
        var keys = normalizedMode == ResyncMode.Full ? 2 : 1;
        var context = new ResyncLifecycleContext(correlationId, normalizedMode, keys, 0, startedAt, completedAt, null);
        OnResyncStarted?.Invoke(context with { CompletedAtUtc = null });
        _snapshot = new ResyncStatusSnapshot(
            InProgress: false,
            CurrentMode: "idle",
            LastResyncUtc: completedAt,
            LastMode: normalizedMode.ToString().ToLowerInvariant(),
            KeysReplayedLastRun: keys,
            KeysReplayedTotal: _snapshot.KeysReplayedTotal + keys,
            LastError: null,
            CorrelationId: correlationId);
        OnResyncCompleted?.Invoke(context);
        return Task.FromResult(new ResyncOperationResult(normalizedMode, keys, 0, TimeSpan.FromMilliseconds(50), completedAt));
    }
}

internal sealed class ToggleConnectivityProbe : IConnectivityProbe
{
    private volatile bool _connected;

    public ToggleConnectivityProbe(bool initiallyConnected)
    {
        _connected = initiallyConnected;
    }

    public void SetConnected(bool connected) => _connected = connected;

    public Task<bool> IsConnectedAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_connected);
    }
}

internal sealed class ConnectivityBoundKeyValueStore : IKeyValueStore
{
    private readonly CountingKeyValueStore _inner;
    private readonly ToggleConnectivityProbe _probe;

    public ConnectivityBoundKeyValueStore(CountingKeyValueStore inner, ToggleConnectivityProbe probe)
    {
        _inner = inner;
        _probe = probe;
    }

    public async Task PutAsync(string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        await EnsureConnectedAsync(cancellationToken);
        await _inner.PutAsync(key, value, cancellationToken);
    }

    public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        await EnsureConnectedAsync(cancellationToken);
        return await _inner.GetAsync(key, cancellationToken);
    }

    public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        await EnsureConnectedAsync(cancellationToken);
        return await _inner.DeleteAsync(key, cancellationToken);
    }

    public async Task<IReadOnlyList<string>> ListKeysAsync(CancellationToken cancellationToken = default)
    {
        await EnsureConnectedAsync(cancellationToken);
        return await _inner.ListKeysAsync(cancellationToken);
    }

    private async Task EnsureConnectedAsync(CancellationToken cancellationToken)
    {
        if (!await _probe.IsConnectedAsync(cancellationToken))
        {
            throw new HttpRequestException("simulated offline");
        }
    }
}

internal sealed class CaptureLogger<T> : ILogger<T>
{
    public List<Dictionary<string, string>> Scopes { get; } = [];

    public IDisposable BeginScope<TState>(TState state) where TState : notnull
    {
        if (state is IEnumerable<KeyValuePair<string, object>> keyValuePairs)
        {
            var scopeValues = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var pair in keyValuePairs)
            {
                scopeValues[pair.Key] = pair.Value?.ToString() ?? string.Empty;
            }

            Scopes.Add(scopeValues);
        }

        return NullScope.Instance;
    }

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();
        public void Dispose()
        {
        }
    }
}

internal static class TestNetHelpers
{
    public static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }
}

internal sealed class FakeMigrationSource : IMigrationSource
{
    private readonly Dictionary<string, MigrationEntry> _entries;
    private readonly string[] _orderedKeys;

    public FakeMigrationSource(IEnumerable<MigrationEntry> entries)
    {
        _entries = entries.ToDictionary(static entry => entry.Key, StringComparer.Ordinal);
        _orderedKeys = _entries.Keys.OrderBy(static key => key, StringComparer.Ordinal).ToArray();
    }

    public Task<long?> GetApproximateTotalKeysAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult<long?>(_entries.Count);
    }

    public Task<ScanBatch> ScanAsync(ulong cursor, string matchPattern, int count, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var prefix = matchPattern.EndsWith('*')
            ? matchPattern[..^1]
            : matchPattern;
        var filtered = _orderedKeys.Where(key => key.StartsWith(prefix, StringComparison.Ordinal)).ToArray();
        var offset = (int)cursor;
        var keys = filtered.Skip(offset).Take(count).ToArray();
        var nextCursor = (ulong)(offset + keys.Length);
        if (nextCursor >= (ulong)filtered.Length)
        {
            nextCursor = 0;
        }

        return Task.FromResult(new ScanBatch
        {
            NextCursor = nextCursor,
            Keys = keys
        });
    }

    public Task<MigrationEntry?> ReadEntryAsync(string key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!_entries.TryGetValue(key, out var entry))
        {
            return Task.FromResult<MigrationEntry?>(null);
        }

        return Task.FromResult<MigrationEntry?>(new MigrationEntry
        {
            Key = entry.Key,
            Type = entry.Type,
            Payload = entry.Payload.ToArray(),
            Ttl = entry.Ttl
        });
    }
}

internal sealed class FakeMigrationDestination : IMigrationDestination
{
    private readonly Dictionary<string, DestinationValue> _values = new(StringComparer.Ordinal);

    public int WriteCount { get; private set; }

    public Task WriteEntryAsync(MigrationEntry entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        WriteCount++;
        _values[entry.Key] = new DestinationValue
        {
            Payload = entry.Payload.ToArray(),
            Ttl = entry.Ttl
        };
        return Task.CompletedTask;
    }

    public Task<DestinationValue?> ReadValueAsync(string key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_values.TryGetValue(key, out var value)
            ? new DestinationValue
            {
                Payload = value.Payload.ToArray(),
                Ttl = value.Ttl
            }
            : null);
    }
}

internal sealed class InMemoryMigrationCheckpointStore : IMigrationCheckpointStore
{
    private MigrationCheckpoint _checkpoint = MigrationCheckpoint.Start;

    public Task<MigrationCheckpoint> LoadAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_checkpoint);
    }

    public Task SaveAsync(MigrationCheckpoint checkpoint, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _checkpoint = checkpoint;
        return Task.CompletedTask;
    }

    public Task DeleteAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _checkpoint = MigrationCheckpoint.Start;
        return Task.CompletedTask;
    }
}

internal sealed class BlockingStream : Stream
{
    private readonly System.Collections.Concurrent.ConcurrentQueue<byte[]> _queue = new();
    private readonly SemaphoreSlim _dataAvailable = new(0);
    private byte[]? _current;
    private int _currentOffset;
    private volatile bool _closed;

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    public override void Flush() { }

    public override void Write(byte[] buffer, int offset, int count)
    {
        var data = new byte[count];
        Buffer.BlockCopy(buffer, offset, data, 0, count);
        _queue.Enqueue(data);
        _dataAvailable.Release();
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        var data = new byte[count];
        Buffer.BlockCopy(buffer, offset, data, 0, count);
        _queue.Enqueue(data);
        _dataAvailable.Release();
        return Task.CompletedTask;
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        var data = buffer.ToArray();
        _queue.Enqueue(data);
        _dataAvailable.Release();
        return ValueTask.CompletedTask;
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        return ReadAsync(buffer, offset, count, CancellationToken.None).GetAwaiter().GetResult();
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        while (true)
        {
            if (_current is not null)
            {
                var available = _current.Length - _currentOffset;
                var toCopy = Math.Min(available, count);
                Buffer.BlockCopy(_current, _currentOffset, buffer, offset, toCopy);
                _currentOffset += toCopy;
                if (_currentOffset >= _current.Length)
                {
                    _current = null;
                    _currentOffset = 0;
                }

                return toCopy;
            }

            if (_closed) return 0;

            await _dataAvailable.WaitAsync(cancellationToken);
            if (_queue.TryDequeue(out var data))
            {
                _current = data;
                _currentOffset = 0;
            }
        }
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        var tmp = new byte[buffer.Length];
        var read = await ReadAsync(tmp, 0, tmp.Length, cancellationToken);
        tmp.AsMemory(0, read).CopyTo(buffer);
        return read;
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        _closed = true;
        _dataAvailable.Release(); // Unblock any waiting reads
        base.Dispose(disposing);
    }
}

internal sealed class SilentMigrationReporter : IMigrationReporter
{
    public void ReportProgress(MigrationProgress progress)
    {
    }

    public void ReportError(string key, Exception exception)
    {
    }

    public void ReportSummary(MigrationResult result)
    {
    }
}

/// <summary>Controllable mock of <see cref="IDecentralizedIdentityProvider"/> for unit tests.</summary>
internal sealed class MockDecentralizedIdentityProvider : IDecentralizedIdentityProvider
{
    private readonly bool _authenticateResult;
    private readonly bool _permissionResult;

    public MockDecentralizedIdentityProvider(bool authenticateResult, bool permissionResult)
    {
        _authenticateResult = authenticateResult;
        _permissionResult = permissionResult;
    }

    public Task<DidDocument?> ResolveAsync(string did, CancellationToken cancellationToken = default) =>
        Task.FromResult<DidDocument?>(new DidDocument { Did = did });

    public Task<bool> AuthenticateAsync(string did, DidProof proof, CancellationToken cancellationToken = default) =>
        Task.FromResult(_authenticateResult);

    public Task<bool> CheckPermissionAsync(string did, string key, DidOperation operation, CancellationToken cancellationToken = default) =>
        Task.FromResult(_permissionResult);
}
