using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace SwarmKeyDb;

public sealed class RedisCommandProcessor : IDisposable
{
    private const string WrongTypeError = "WRONGTYPE Operation against a key holding the wrong kind of value";
    private const string BusyGroupError = "BUSYGROUP Consumer Group name already exists";
    private const string OomError = "OOM command not allowed when used memory > 'maxmemory'.";
    private static readonly byte[] StreamValueMagicPrefix = "SKDBSTREAM1:"u8.ToArray();
    private static readonly JsonSerializerOptions StreamJsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private readonly IKeyValueStore _store;
    private readonly IEthAddressAccessor? _ethAddressAccessor;
    private readonly IDidContextAccessor? _didContextAccessor;
    private readonly IDecentralizedIdentityProvider? _didProvider;
    private readonly BackupService? _backupService;
    private readonly RestoreService? _restoreService;
    private readonly KeyRotationService? _keyRotationService;
    private readonly IRedisCommandObserver? _observer;
    private readonly IResyncCoordinator? _resyncCoordinator;
    private readonly PubSubManager? _pubSubManager;
    private readonly ILogger<RedisCommandProcessor> _logger;
    private readonly StreamTrimOptions _streamTrimOptions;
    private readonly RedisCompatibilityOptions _compatibilityOptions;
    private readonly SemaphoreSlim _mutationGate = new(1, 1);
    private readonly ConcurrentDictionary<long, ClientConnection> _clientConnections = new();
    private readonly ConcurrentDictionary<string, long> _keySizes = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _keyLastAccessUnixMs = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DateTimeOffset?> _keyExpiryHints = new(StringComparer.Ordinal);
    private static readonly AsyncLocal<ClientContext?> CurrentClientContext = new();
    private long _nextClientId;
    private long _nextExpiryRunUnixMs;
    private long _expiryScanCount;
    private double _expiryScanDurationSecondsSum;
    private long _expiryKeysDeletedTotal;
    private long _expiryBudgetExceededTotal;
    private long _evictionTotal;
    private long _totalCommandsProcessed;
    private readonly Random _random = new();

    // RESP3 / client-tracking telemetry
    private long _resp3ConnectionsTotal;
    private long _activeResp3Connections;

    // Per-connection tracking registrations: clientId → push channel writer
    private readonly ConcurrentDictionary<long, TrackingRegistration> _trackingConnections = new();

    private static readonly IReadOnlyDictionary<string, CommandSpec> CommandSpecs = CreateCommandSpecs();

    // Scripting
    private readonly ScriptEngine _scriptEngine;
    private readonly ScriptCache _scriptCache;
    private readonly ScriptReplicationManager? _scriptReplicationManager;

    // Scripting telemetry counters
    private long _scriptEvalTotal;
    private long _scriptEvalShaTotal;
    private long _scriptErrorTotal;
    private long _scriptTimeoutTotal;
    private readonly long[] _scriptExecDurationBucketCounts = new long[ScriptExecDurationBucketUpperBounds.Length];
    private long _scriptExecDurationCount;
    private double _scriptExecDurationSumSeconds;

    // Shared key-version tracker for WATCH support (incremented on every successful mutation)
    private readonly ConcurrentDictionary<string, long> _keyVersions = new(StringComparer.Ordinal);
    private long _versionClock;

    // Transaction telemetry counters
    private long _txStartedTotal;
    private long _txCommittedTotal;
    private long _txAbortedTotal;
    private long _txWatchConflictTotal;
    private readonly long[] _txQueueDepthBucketCounts = new long[TransactionQueueDepthBucketUpperBounds.Length];
    private readonly long[] _txExecDurationBucketCounts = new long[TransactionExecDurationBucketUpperBounds.Length];
    private long _txQueueDepthCount;
    private double _txQueueDepthSum;
    private long _txExecDurationCount;
    private double _txExecDurationSumSeconds;
    private long _streamPendingEntriesTotal;
    private long _streamXAckTotal;
    private long _streamXClaimTotal;
    private long _streamBlockedReadersTotal;
    private long _streamXReadWakeupTotal;
    private long _streamTrimmedTotal;
    private readonly object _streamWaitersGate = new();
    private readonly Dictionary<string, HashSet<StreamReadWaiter>> _streamReadWaiters = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Dictionary<string, Queue<StreamReadWaiter>>> _streamReadGroupWaiters = new(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _streamBlockedReadersByStream = new(StringComparer.Ordinal);

    public static readonly double[] TransactionQueueDepthBucketUpperBounds = [0, 1, 2, 4, 8, 16, 32];
    public static readonly double[] TransactionExecDurationBucketUpperBounds = [0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5];
    public static readonly double[] ScriptExecDurationBucketUpperBounds = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];

    public RedisCommandProcessor(
        IKeyValueStore store,
        IEthAddressAccessor? ethAddressAccessor = null,
        BackupService? backupService = null,
        RestoreService? restoreService = null,
        KeyRotationService? keyRotationService = null,
        IRedisCommandObserver? observer = null,
        ILogger<RedisCommandProcessor>? logger = null,
        IDidContextAccessor? didContextAccessor = null,
        IDecentralizedIdentityProvider? didProvider = null,
        IResyncCoordinator? resyncCoordinator = null,
        PubSubManager? pubSubManager = null,
        StreamTrimOptions? streamTrimOptions = null,
        ScriptEngine? scriptEngine = null,
        ScriptCache? scriptCache = null,
        ScriptReplicationManager? scriptReplicationManager = null,
        RedisCompatibilityOptions? compatibilityOptions = null)
    {
        _store = store;
        _ethAddressAccessor = ethAddressAccessor;
        _didContextAccessor = didContextAccessor;
        _didProvider = didProvider;
        _backupService = backupService;
        _restoreService = restoreService;
        _keyRotationService = keyRotationService;
        _observer = observer;
        _logger = logger ?? NullLogger<RedisCommandProcessor>.Instance;
        _resyncCoordinator = resyncCoordinator;
        _pubSubManager = pubSubManager;
        _streamTrimOptions = streamTrimOptions ?? new StreamTrimOptions();
        _scriptEngine = scriptEngine ?? new ScriptEngine();
        _scriptCache = scriptCache ?? new ScriptCache();
        _scriptReplicationManager = scriptReplicationManager;
        _compatibilityOptions = compatibilityOptions ?? new RedisCompatibilityOptions();
    }

    public async Task ProcessAsync(Stream input, Stream output, CancellationToken cancellationToken = default)
    {
        var reader = new RespReader(input);
        var writer = new RespWriter(output);
        string? currentAddress = null;
        DidContext? currentDidContext = null;
        int protocolVersion = 2; // negotiated per connection; upgrades via HELLO

        if (_ethAddressAccessor is not null)
        {
            _ethAddressAccessor.CurrentAddress = null;
        }

        if (_didContextAccessor is not null)
        {
            _didContextAccessor.Current = null;
        }

        // Per-connection pub/sub state
        var connectionId = Guid.NewGuid().ToString("N");
        var clientId = Interlocked.Increment(ref _nextClientId);
        var clientConnection = new ClientConnection(clientId, ResolveRemoteEndpoint(output));
        _clientConnections[clientId] = clientConnection;
        var channelSubs = new HashSet<string>(StringComparer.Ordinal);
        var patternSubs = new HashSet<string>(StringComparer.Ordinal);
        // Bounded push channel: DropWrite so a slow subscriber doesn't block the publisher
        var pushChannel = Channel.CreateBounded<RespValue>(new BoundedChannelOptions(256)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true
        });

        // Per-connection transaction state
        var tx = new TransactionState();

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                RespValue? request;
                var isSubscribed = channelSubs.Count + patternSubs.Count > 0;

                if (isSubscribed)
                {
                    // In subscription mode: interleave push message delivery with command reading.
                    // Drain any queued push messages before blocking on the next command.
                    while (pushChannel.Reader.TryRead(out var pending))
                    {
                        await writer.WriteAsync(pending, cancellationToken).ConfigureAwait(false);
                    }

                    // Concurrently wait for either a new incoming command or a push message.
                    var readTask = reader.ReadAsync(cancellationToken);
                    while (!readTask.IsCompleted)
                    {
                        var pushWaiter = pushChannel.Reader.WaitToReadAsync(cancellationToken).AsTask();
                        await Task.WhenAny(readTask, pushWaiter).ConfigureAwait(false);

                        // Flush any push messages that arrived while waiting
                        while (pushChannel.Reader.TryRead(out var push))
                        {
                            await writer.WriteAsync(push, cancellationToken).ConfigureAwait(false);
                        }
                    }

                    try
                    {
                        request = await readTask.ConfigureAwait(false);
                    }
                    catch (EndOfStreamException)
                    {
                        break;
                    }
                    catch (Exception ex) when (ex is InvalidDataException or FormatException or OverflowException)
                    {
                        await writer.WriteAsync(RespValue.Error("ERR Protocol error: invalid RESP frame"), cancellationToken).ConfigureAwait(false);
                        continue;
                    }
                }
                else
                {
                    try
                    {
                        request = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                    }
                    catch (EndOfStreamException)
                    {
                        break;
                    }
                    catch (Exception ex) when (ex is InvalidDataException or FormatException or OverflowException)
                    {
                        await writer.WriteAsync(RespValue.Error("ERR Protocol error: invalid RESP frame"), cancellationToken).ConfigureAwait(false);
                        continue;
                    }
                }

                if (request is null)
                {
                    break;
                }

                CurrentClientContext.Value = new ClientContext(clientId);

                if (_ethAddressAccessor is not null)
                {
                    _ethAddressAccessor.CurrentAddress = currentAddress;
                }

                if (_didContextAccessor is not null)
                {
                    _didContextAccessor.Current = currentDidContext;
                }

                var command = request.Type == RespType.Array && request.Items is { Count: > 0 }
                    ? request.Items[0].AsString().ToUpperInvariant()
                    : string.Empty;
                _clientConnections.AddOrUpdate(
                    clientId,
                    _ => clientConnection,
                    (_, state) => state with
                    {
                        LastCommand = command,
                        LastSeenUtc = DateTimeOffset.UtcNow
                    });

                // Handle pub/sub commands that require direct access to the writer and connection state
                if (_pubSubManager is not null && command is "SUBSCRIBE" or "UNSUBSCRIBE" or "PSUBSCRIBE" or "PUNSUBSCRIBE")
                {
                    await HandlePubSubCommandAsync(command, request, connectionId, pushChannel.Writer, channelSubs, patternSubs, writer, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                // In subscription mode only PING, RESET, HELLO, and QUIT are permitted
                if (isSubscribed && command is not ("PING" or "RESET" or "HELLO" or "QUIT"))
                {
                    await writer.WriteAsync(
                        RespValue.Error($"ERR Can't call '{command.ToLowerInvariant()}' in subscribe mode"),
                        cancellationToken).ConfigureAwait(false);
                    continue;
                }

                // HELLO: per-connection protocol negotiation (must not be queued in MULTI)
                if (command == "HELLO")
                {
                    if (tx.InMulti)
                    {
                        await writer.WriteAsync(RespValue.Error("ERR Command not allowed inside a transaction"), cancellationToken).ConfigureAwait(false);
                        continue;
                    }

                    var helloResult = HandleHello(request, clientId, ref protocolVersion, ref currentAddress, writer);
                    await writer.WriteAsync(helloResult, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                // RESET: restore connection to factory state
                if (command == "RESET")
                {
                    // Unsubscribe from all channels/patterns
                    if (_pubSubManager is not null)
                    {
                        foreach (var ch in channelSubs.ToArray())
                        {
                            _pubSubManager.Unsubscribe(connectionId, ch);
                        }

                        foreach (var pat in patternSubs.ToArray())
                        {
                            _pubSubManager.PUnsubscribe(connectionId, pat);
                        }
                    }

                    channelSubs.Clear();
                    patternSubs.Clear();
                    tx.Reset();

                    // Downgrade from RESP3 → 2 if needed
                    if (protocolVersion == 3)
                    {
                        Interlocked.Decrement(ref _activeResp3Connections);
                        protocolVersion = 2;
                        writer.ProtocolVersion = 2;
                    }

                    // Remove tracking registration
                    _trackingConnections.TryRemove(clientId, out _);

                    // Clear auth state
                    currentAddress = null;
                    currentDidContext = null;
                    if (_ethAddressAccessor is not null)
                    {
                        _ethAddressAccessor.CurrentAddress = null;
                    }

                    if (_didContextAccessor is not null)
                    {
                        _didContextAccessor.Current = null;
                    }

                    await writer.WriteAsync(RespValue.SimpleString("RESET"), cancellationToken).ConfigureAwait(false);
                    continue;
                }

                // CLIENT TRACKING requires access to the per-connection push channel
                if (command == "CLIENT" && request.Items is { Count: >= 3 } clientItems
                    && clientItems[1].AsString().ToUpperInvariant() == "TRACKING")
                {
                    var trackingResult = HandleClientTracking(request, clientId, pushChannel.Writer);
                    await writer.WriteAsync(trackingResult, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                // Transaction commands (MULTI/EXEC/DISCARD/WATCH/UNWATCH) require per-connection state
                if (command is "MULTI" or "EXEC" or "DISCARD" or "WATCH" or "UNWATCH")
                {
                    var txResponse = await HandleTransactionCommandAsync(command, request, tx, cancellationToken).ConfigureAwait(false);
                    await writer.WriteAsync(txResponse, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                // Queue commands while inside a MULTI block
                if (tx.InMulti)
                {
                    var queueResponse = TryQueueCommand(command, tx, request);
                    await writer.WriteAsync(queueResponse, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var response = await ExecuteAsync(request, cancellationToken).ConfigureAwait(false);

                // Bump key versions for successful mutations so WATCH can detect changes
                if (response.Type != RespType.Error && request.Items is { Count: > 0 } mutatedItems)
                {
                    BumpMutatedKeys(command, mutatedItems, clientId);
                }

                if (TryGetAuthorizedAddress(request, response, out var authorizedAddress))
                {
                    currentAddress = authorizedAddress;
                }

                if (TryGetAuthorizedDidContext(request, response, out var authorizedDidContext))
                {
                    currentDidContext = authorizedDidContext;
                }

                await writer.WriteAsync(response, cancellationToken).ConfigureAwait(false);

                // Drain any queued push messages (CLIENT TRACKING invalidations) after each command reply.
                while (pushChannel.Reader.TryRead(out var push))
                {
                    await writer.WriteAsync(push, cancellationToken).ConfigureAwait(false);
                }

                if (IsQuit(request))
                {
                    break;
                }
            }
        }
        finally
        {
            // Clean up all subscriptions for this connection
            if (_pubSubManager is not null)
            {
                _pubSubManager.RemoveConnection(connectionId);
            }

            _clientConnections.TryRemove(clientId, out _);
            _trackingConnections.TryRemove(clientId, out _);
            CurrentClientContext.Value = null;

            if (protocolVersion == 3)
            {
                Interlocked.Decrement(ref _activeResp3Connections);
            }

            pushChannel.Writer.TryComplete();

            if (_ethAddressAccessor is not null)
            {
                _ethAddressAccessor.CurrentAddress = null;
            }

            if (_didContextAccessor is not null)
            {
                _didContextAccessor.Current = null;
            }
        }
    }

    // --- Transaction support ---

    /// <summary>Per-connection transaction state (local to <see cref="ProcessAsync"/>).</summary>
    private sealed class TransactionState
    {
        public bool InMulti;
        public bool HasQueueError;
        public readonly List<RespValue> CommandQueue = [];
        public readonly Dictionary<string, long> WatchedVersions = new(StringComparer.Ordinal);

        public void Reset()
        {
            InMulti = false;
            HasQueueError = false;
            CommandQueue.Clear();
            WatchedVersions.Clear();
        }
    }

    private sealed record StreamData(
        IReadOnlyList<StreamEntry> Entries,
        ulong LastTimestamp,
        ulong LastSequence,
        IReadOnlyDictionary<string, ConsumerGroupState>? Groups = null);
    private sealed record StreamEntry(string Id, ulong Timestamp, ulong Sequence, IReadOnlyList<StreamField> Fields);
    private sealed record StreamField(byte[] Name, byte[] Value);
    private sealed record ConsumerGroupState(
        string LastDeliveredId,
        ulong LastDeliveredTimestamp,
        ulong LastDeliveredSequence,
        IReadOnlyDictionary<string, PendingEntryState>? Pending = null,
        IReadOnlyDictionary<string, ConsumerState>? Consumers = null);
    private sealed record PendingEntryState(
        string Id,
        ulong Timestamp,
        ulong Sequence,
        string Consumer,
        long LastDeliveredUnixMs,
        int DeliveryCount);
    private sealed record ConsumerState(string Name, long LastSeenUnixMs);
    private sealed class StreamReadWaiter
    {
        private int _released;

        public StreamReadWaiter(IReadOnlyList<string> keys, string? groupName)
        {
            Keys = keys;
            GroupName = groupName;
            Signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public IReadOnlyList<string> Keys { get; }
        public string? GroupName { get; }
        public TaskCompletionSource Signal { get; }

        public bool TryRelease()
        {
            if (Interlocked.Exchange(ref _released, 1) != 0)
            {
                return false;
            }

            Signal.TrySetResult();
            return true;
        }
    }

    /// <summary>Commands that are valid inside a MULTI block (unknown commands abort the transaction).</summary>
    private static readonly HashSet<string> KnownQueueableCommands = new(StringComparer.OrdinalIgnoreCase)
    {
        "PING", "ECHO", "AUTHADDR", "AUTHDID",
        "SET", "SETEX", "PSETEX", "GET", "INCR", "INCRBY", "DECR", "DECRBY",
        "DEL", "MDEL", "MGET", "MSET", "MSETNX",
        "EXISTS", "EXPIRE", "PEXPIRE", "EXPIREAT",
        "TTL", "PTTL", "PERSIST",
        "INFO", "COMMAND", "CLIENT", "CONFIG",
        "XADD", "XTRIM", "XRANGE", "XREVRANGE", "XLEN", "XREAD",
        "XGROUP", "XREADGROUP", "XACK", "XPENDING", "XCLAIM", "XAUTOCLAIM",
        "KEYS", "SCAN", "TYPE",
        "BACKUP", "RESTOREDB", "ROTATEKEY", "BACKENDMETA",
        "SWARM.RESYNC", "PUBLISH", "PUBSUB", "QUIT",
        "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE"
    };

    /// <summary>
    /// Handles MULTI, EXEC, DISCARD, WATCH, and UNWATCH commands with per-connection state.
    /// </summary>
    private async Task<RespValue> HandleTransactionCommandAsync(
        string command,
        RespValue request,
        TransactionState tx,
        CancellationToken cancellationToken)
    {
        var args = request.Items ?? (IReadOnlyList<RespValue>)[];

        switch (command)
        {
            case "MULTI":
                if (tx.InMulti)
                {
                    return RespValue.Error("ERR MULTI calls can not be nested");
                }
                tx.InMulti = true;
                Interlocked.Increment(ref _txStartedTotal);
                return RespValue.SimpleString("OK");

            case "EXEC":
                if (!tx.InMulti)
                {
                    return RespValue.Error("ERR EXEC without MULTI");
                }
                return await ExecTransactionAsync(tx, cancellationToken).ConfigureAwait(false);

            case "DISCARD":
                if (!tx.InMulti)
                {
                    return RespValue.Error("ERR DISCARD without MULTI");
                }
                ObserveTransactionQueueDepth(tx.CommandQueue.Count);
                tx.Reset();
                Interlocked.Increment(ref _txAbortedTotal);
                return RespValue.SimpleString("OK");

            case "WATCH":
                if (tx.InMulti)
                {
                    return RespValue.Error("ERR WATCH inside MULTI is not allowed");
                }
                if (args.Count < 2)
                {
                    return RespValue.Error("ERR wrong number of arguments for 'WATCH' command");
                }
                for (var i = 1; i < args.Count; i++)
                {
                    var watchKey = args[i].AsString();
                    tx.WatchedVersions[watchKey] = GetKeyVersion(watchKey);
                }
                return RespValue.SimpleString("OK");

            case "UNWATCH":
                tx.WatchedVersions.Clear();
                return RespValue.SimpleString("OK");

            default:
                return RespValue.Error($"ERR unknown command '{command}'");
        }
    }

    /// <summary>
    /// Executes all queued commands atomically.  Returns a null array if any watched key changed.
    /// Returns EXECABORT if a syntax error was queued.
    /// </summary>
    private async Task<RespValue> ExecTransactionAsync(TransactionState tx, CancellationToken cancellationToken)
    {
        var queueDepth = tx.CommandQueue.Count;
        ObserveTransactionQueueDepth(queueDepth);
        var stopwatch = Stopwatch.StartNew();

        try
        {
            // Syntax error during queuing → abort the whole transaction
            if (tx.HasQueueError)
            {
                tx.Reset();
                return RespValue.Error("EXECABORT Transaction discarded because of previous errors.");
            }

            // WATCH conflict check
            foreach (var (watchedKey, watchedVersion) in tx.WatchedVersions)
            {
                if (GetKeyVersion(watchedKey) != watchedVersion)
                {
                    tx.Reset();
                    Interlocked.Increment(ref _txWatchConflictTotal);
                    Interlocked.Increment(ref _txAbortedTotal);
                    return RespValue.NullArray();
                }
            }

            // Snapshot and clear the queue before executing so that nested commands behave correctly
            var queue = tx.CommandQueue.ToArray();
            tx.Reset();

            var results = new RespValue[queue.Length];
            for (var i = 0; i < queue.Length; i++)
            {
                results[i] = await ExecuteAsync(queue[i], cancellationToken).ConfigureAwait(false);

                // Propagate mutations so WATCH on other connections sees them
                if (results[i].Type != RespType.Error && queue[i].Items is { Count: > 0 } qItems)
                {
                    BumpMutatedKeys(qItems[0].AsString().ToUpperInvariant(), qItems);
                }
            }

            Interlocked.Increment(ref _txCommittedTotal);
            return RespValue.Array(results);
        }
        finally
        {
            stopwatch.Stop();
            ObserveExecDuration(stopwatch.Elapsed);
        }
    }

    /// <summary>
    /// Attempts to queue a command during a MULTI block.
    /// Unknown commands mark the transaction for abort on EXEC and return an error.
    /// </summary>
    private static RespValue TryQueueCommand(string command, TransactionState tx, RespValue request)
    {
        if (!KnownQueueableCommands.Contains(command))
        {
            tx.HasQueueError = true;
            return RespValue.Error($"ERR unknown command `{command}`");
        }

        tx.CommandQueue.Add(request);
        return RespValue.SimpleString("QUEUED");
    }

    /// <summary>Retrieves the current mutation version for a key (0 if never written).</summary>
    private long GetKeyVersion(string key) => _keyVersions.GetValueOrDefault(key, 0L);

    /// <summary>Stamps the key with a new monotonically-increasing version after a successful mutation.</summary>
    private void NotifyKeyMutated(string key) =>
        _keyVersions[key] = Interlocked.Increment(ref _versionClock);

    /// <summary>
    /// Bumps the mutation version for all keys affected by a successful write command.
    /// Called after every successful non-transaction command execution.
    /// </summary>
    private void BumpMutatedKeys(string command, IReadOnlyList<RespValue> args, long sourceClientId = 0)
    {
        switch (command)
        {
            case "SET":
            case "SETEX":
            case "PSETEX":
            case "INCR":
            case "INCRBY":
            case "DECR":
            case "DECRBY":
            case "EXPIRE":
            case "PEXPIRE":
            case "EXPIREAT":
            case "PERSIST":
                if (args.Count >= 2)
                {
                    var key = args[1].AsString();
                    NotifyKeyMutated(key);
                    NotifyTrackingClients(key, sourceClientId);
                }
                break;

            case "DEL":
            case "MDEL":
                for (var i = 1; i < args.Count; i++)
                {
                    var key = args[i].AsString();
                    NotifyKeyMutated(key);
                    NotifyTrackingClients(key, sourceClientId);
                }
                break;

            case "MSET":
            case "MSETNX":
                for (var i = 1; i < args.Count - 1; i += 2)
                {
                    var key = args[i].AsString();
                    NotifyKeyMutated(key);
                    NotifyTrackingClients(key, sourceClientId);
                }
                break;

            case "XADD":
            case "XTRIM":
            case "XGROUP":
            case "XREADGROUP":
            case "XACK":
            case "XCLAIM":
            case "XAUTOCLAIM":
                if (args.Count >= 2)
                {
                    var key = args[1].AsString();
                    NotifyKeyMutated(key);
                    NotifyTrackingClients(key, sourceClientId);
                }
                break;
        }
    }

    /// <summary>
    /// Sends a RESP3 push invalidation message to all connections with active client tracking,
    /// excluding the source connection (to implement default NOLOOP behaviour).
    /// </summary>
    private void NotifyTrackingClients(string key, long sourceClientId)
    {
        if (_trackingConnections.IsEmpty)
        {
            return;
        }

        var invalidateMsg = RespValue.Push(new[]
        {
            RespValue.BulkString("invalidate"),
            RespValue.Array(new[] { RespValue.BulkString(key) })
        });

        foreach (var (id, reg) in _trackingConnections)
        {
            // Skip the connection that triggered the mutation (NOLOOP behaviour)
            if (id == sourceClientId)
            {
                continue;
            }

            reg.PushWriter.TryWrite(invalidateMsg);
        }
    }

    /// <summary>Returns a snapshot of transaction telemetry counters.</summary>
    public TransactionMetricsSnapshot GetTransactionMetrics() => new(
        Interlocked.Read(ref _txStartedTotal),
        Interlocked.Read(ref _txCommittedTotal),
        Interlocked.Read(ref _txAbortedTotal),
        Interlocked.Read(ref _txWatchConflictTotal),
        new TransactionHistogramSnapshot(
            TransactionQueueDepthBucketUpperBounds,
            SnapshotBucketCounts(_txQueueDepthBucketCounts),
            Interlocked.Read(ref _txQueueDepthCount),
            Volatile.Read(ref _txQueueDepthSum)),
        new TransactionHistogramSnapshot(
            TransactionExecDurationBucketUpperBounds,
            SnapshotBucketCounts(_txExecDurationBucketCounts),
            Interlocked.Read(ref _txExecDurationCount),
            Volatile.Read(ref _txExecDurationSumSeconds)));

    public StreamMetricsSnapshot GetStreamMetrics()
    {
        var groupCount = 0L;
        var idleConsumerCount = 0L;
        var streamLengthBytesTotal = 0L;
        var streamLengthBytesByStream = new Dictionary<string, long>(StringComparer.Ordinal);

        foreach (var key in _store.ListKeysAsync().GetAwaiter().GetResult())
        {
            var bytes = _store.GetAsync(key).GetAwaiter().GetResult();
            if (bytes is null || !TryReadStream(bytes, out var stream) || stream is null)
            {
                continue;
            }

            streamLengthBytesTotal += bytes.Length;
            streamLengthBytesByStream[key] = bytes.Length;

            if (stream.Groups is null)
            {
                continue;
            }

            foreach (var group in stream.Groups.Values)
            {
                groupCount++;
                if (group.Consumers is null)
                {
                    continue;
                }

                var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                idleConsumerCount += group.Consumers.Values.LongCount(consumer => consumer.LastSeenUnixMs <= 0 || nowMs - consumer.LastSeenUnixMs > 60_000);
            }
        }

        Dictionary<string, long> blockedReadersByStream;
        lock (_streamWaitersGate)
        {
            blockedReadersByStream = _streamBlockedReadersByStream.ToDictionary(static pair => pair.Key, static pair => pair.Value, StringComparer.Ordinal);
        }

        return new StreamMetricsSnapshot(
            Math.Max(0, Interlocked.Read(ref _streamPendingEntriesTotal)),
            Interlocked.Read(ref _streamXAckTotal),
            Interlocked.Read(ref _streamXClaimTotal),
            groupCount,
            idleConsumerCount,
            Math.Max(0, Interlocked.Read(ref _streamBlockedReadersTotal)),
            Interlocked.Read(ref _streamXReadWakeupTotal),
            blockedReadersByStream,
            Interlocked.Read(ref _streamTrimmedTotal),
            Math.Max(0, streamLengthBytesTotal),
            streamLengthBytesByStream);
    }

    private static long[] SnapshotBucketCounts(long[] source)
    {
        var snapshot = new long[source.Length];
        for (var i = 0; i < source.Length; i++)
        {
            snapshot[i] = Volatile.Read(ref source[i]);
        }

        return snapshot;
    }

    private void ObserveTransactionQueueDepth(int depth)
    {
        var depthValue = Math.Max(0, depth);
        ObserveHistogram(_txQueueDepthBucketCounts, TransactionQueueDepthBucketUpperBounds, depthValue);
        Interlocked.Increment(ref _txQueueDepthCount);
        AddDouble(ref _txQueueDepthSum, depthValue);
    }

    private void ObserveExecDuration(TimeSpan duration)
    {
        var seconds = Math.Max(0D, duration.TotalSeconds);
        ObserveHistogram(_txExecDurationBucketCounts, TransactionExecDurationBucketUpperBounds, seconds);
        Interlocked.Increment(ref _txExecDurationCount);
        AddDouble(ref _txExecDurationSumSeconds, seconds);
    }

    private static void ObserveHistogram(long[] bucketCounts, IReadOnlyList<double> bucketBounds, double value)
    {
        for (var i = 0; i < bucketBounds.Count; i++)
        {
            if (value <= bucketBounds[i])
            {
                Interlocked.Increment(ref bucketCounts[i]);
            }
        }
    }

    private static void AddDouble(ref double target, double delta)
    {
        while (true)
        {
            var current = Volatile.Read(ref target);
            var updated = current + delta;
            if (Interlocked.CompareExchange(ref target, updated, current) == current)
            {
                return;
            }
        }
    }

    /// <summary>
    /// Handles SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, and PUNSUBSCRIBE commands,
    /// writing subscription confirmation replies directly to the output writer.
    /// </summary>
    private async Task HandlePubSubCommandAsync(
        string command,
        RespValue request,
        string connectionId,
        ChannelWriter<RespValue> pushWriter,
        HashSet<string> channelSubs,
        HashSet<string> patternSubs,
        RespWriter writer,
        CancellationToken cancellationToken)
    {
        var args = request.Items!;
        var isPattern = command is "PSUBSCRIBE" or "PUNSUBSCRIBE";
        var isSubscribe = command is "SUBSCRIBE" or "PSUBSCRIBE";
        var replyType = isPattern
            ? (isSubscribe ? "psubscribe" : "punsubscribe")
            : (isSubscribe ? "subscribe" : "unsubscribe");

        if (isSubscribe)
        {
            if (args.Count < 2)
            {
                await writer.WriteAsync(
                    RespValue.Error($"ERR wrong number of arguments for '{command.ToLowerInvariant()}'"),
                    cancellationToken).ConfigureAwait(false);
                return;
            }

            for (var i = 1; i < args.Count; i++)
            {
                var target = args[i].AsString();
                int total;
                if (isPattern)
                {
                    total = _pubSubManager!.PSubscribe(connectionId, target, pushWriter);
                    patternSubs.Add(target);
                }
                else
                {
                    total = _pubSubManager!.Subscribe(connectionId, target, pushWriter);
                    channelSubs.Add(target);
                }

                _logger.LogDebug("Connection {ConnectionId} {Command} {Target}. Total={Total}.", connectionId, command, target, total);
                await writer.WriteAsync(
                    RespValue.Array(new[]
                    {
                        RespValue.BulkString(replyType),
                        RespValue.BulkString(target),
                        RespValue.IntegerValue(total)
                    }),
                    cancellationToken).ConfigureAwait(false);
            }
        }
        else
        {
            // UNSUBSCRIBE / PUNSUBSCRIBE
            // If no arguments, unsubscribe from all
            IReadOnlyList<string> targets;
            if (args.Count < 2)
            {
                targets = isPattern
                    ? patternSubs.ToArray()
                    : channelSubs.ToArray();
            }
            else
            {
                targets = args.Skip(1).Select(a => a.AsString()).ToArray();
            }

            if (targets.Count == 0)
            {
                // Nothing to unsubscribe from; send a single reply with count 0
                await writer.WriteAsync(
                    RespValue.Array(new[]
                    {
                        RespValue.BulkString(replyType),
                        RespValue.BulkString((string?)null),
                        RespValue.IntegerValue(0)
                    }),
                    cancellationToken).ConfigureAwait(false);
                return;
            }

            foreach (var target in targets)
            {
                int remaining;
                if (isPattern)
                {
                    remaining = _pubSubManager!.PUnsubscribe(connectionId, target);
                    patternSubs.Remove(target);
                }
                else
                {
                    remaining = _pubSubManager!.Unsubscribe(connectionId, target);
                    channelSubs.Remove(target);
                }

                _logger.LogDebug("Connection {ConnectionId} {Command} {Target}. Remaining={Remaining}.", connectionId, command, target, remaining);
                await writer.WriteAsync(
                    RespValue.Array(new[]
                    {
                        RespValue.BulkString(replyType),
                        RespValue.BulkString(target),
                        RespValue.IntegerValue(remaining)
                    }),
                    cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public async Task<RespValue> ExecuteAsync(RespValue request, CancellationToken cancellationToken = default)
    {
        var correlationId = Guid.NewGuid().ToString("N");
        var stopwatch = Stopwatch.StartNew();
        var command = request.Type == RespType.Array && request.Items is { Count: > 0 }
            ? request.Items[0].AsString().ToUpperInvariant()
            : "INVALID";
        RespValue response;

        using var scope = _logger.BeginScope(new Dictionary<string, object>
        {
            ["correlationId"] = correlationId,
            ["command"] = command
        });

        if (request.Type != RespType.Array || request.Items is null || request.Items.Count == 0)
        {
            response = RespValue.Error("ERR expected command array");
            _logger.LogWarning("Command rejected because request was not a RESP command array.");
            RecordCommand(command, response, stopwatch.Elapsed, correlationId);
            return response;
        }

        var args = request.Items;
        command = args[0].AsString().ToUpperInvariant();
        try
        {
            await MaybeRunAdaptiveExpiryAsync(cancellationToken).ConfigureAwait(false);

            response = command switch
            {
                "PING" => args.Count > 1 ? RespValue.BulkString(args[1].Bytes) : RespValue.SimpleString("PONG"),
                "INFO" => await InfoAsync(args, cancellationToken).ConfigureAwait(false),
                "COMMAND" => CommandCommand(args),
                "CLIENT" => ClientCommand(args),
                "CONFIG" => ConfigCommand(args),
                "ECHO" => RequireArity(args, 2) ?? RespValue.BulkString(args[1].Bytes),
                "AUTHADDR" => SetCallerAddress(args),
                "AUTHDID" => await SetDidContextAsync(args, cancellationToken).ConfigureAwait(false),
                "SET" => await SetAsync(args, cancellationToken).ConfigureAwait(false),
                "SETEX" => await SetExAsync(args, milliseconds: false, cancellationToken).ConfigureAwait(false),
                "PSETEX" => await SetExAsync(args, milliseconds: true, cancellationToken).ConfigureAwait(false),
                "GET" => await GetAsync(args, cancellationToken).ConfigureAwait(false),
                "INCR" => await IncrByAsync(args, 1, cancellationToken).ConfigureAwait(false),
                "INCRBY" => await IncrByAsync(args, null, cancellationToken).ConfigureAwait(false),
                "DECR" => await IncrByAsync(args, -1, cancellationToken).ConfigureAwait(false),
                "DECRBY" => await DecrByAsync(args, cancellationToken).ConfigureAwait(false),
                "DEL" => await DelAsync(args, cancellationToken).ConfigureAwait(false),
                "MDEL" => await MDelAsync(args, cancellationToken).ConfigureAwait(false),
                "MGET" => await MGetAsync(args, cancellationToken).ConfigureAwait(false),
                "MSET" => await MSetAsync(args, cancellationToken).ConfigureAwait(false),
                "MSETNX" => await MSetNxAsync(args, cancellationToken).ConfigureAwait(false),
                "EXISTS" => await ExistsAsync(args, cancellationToken).ConfigureAwait(false),
                "EXPIRE" => await ExpireAsync(args, milliseconds: false, absolute: false, cancellationToken).ConfigureAwait(false),
                "PEXPIRE" => await ExpireAsync(args, milliseconds: true, absolute: false, cancellationToken).ConfigureAwait(false),
                "EXPIREAT" => await ExpireAsync(args, milliseconds: false, absolute: true, cancellationToken).ConfigureAwait(false),
                "TTL" => await TtlAsync(args, milliseconds: false, cancellationToken).ConfigureAwait(false),
                "PTTL" => await TtlAsync(args, milliseconds: true, cancellationToken).ConfigureAwait(false),
                "PERSIST" => await PersistAsync(args, cancellationToken).ConfigureAwait(false),
                "KEYS" => await KeysAsync(args, cancellationToken).ConfigureAwait(false),
                "SCAN" => await ScanAsync(args, cancellationToken).ConfigureAwait(false),
                "TYPE" => await TypeAsync(args, cancellationToken).ConfigureAwait(false),
                "XADD" => await XAddAsync(args, cancellationToken).ConfigureAwait(false),
                "XTRIM" => await XTrimAsync(args, cancellationToken).ConfigureAwait(false),
                "XRANGE" => await XRangeAsync(args, reverse: false, cancellationToken).ConfigureAwait(false),
                "XREVRANGE" => await XRangeAsync(args, reverse: true, cancellationToken).ConfigureAwait(false),
                "XLEN" => await XLenAsync(args, cancellationToken).ConfigureAwait(false),
                "XREAD" => await XReadAsync(args, cancellationToken).ConfigureAwait(false),
                "XGROUP" => await XGroupAsync(args, cancellationToken).ConfigureAwait(false),
                "XREADGROUP" => await XReadGroupAsync(args, cancellationToken).ConfigureAwait(false),
                "XACK" => await XAckAsync(args, cancellationToken).ConfigureAwait(false),
                "XPENDING" => await XPendingAsync(args, cancellationToken).ConfigureAwait(false),
                "XCLAIM" => await XClaimAsync(args, cancellationToken).ConfigureAwait(false),
                "XAUTOCLAIM" => await XAutoClaimAsync(args, cancellationToken).ConfigureAwait(false),
                "BACKUP" => await BackupAsync(args, cancellationToken).ConfigureAwait(false),
                "RESTOREDB" => await RestoreDbAsync(args, cancellationToken).ConfigureAwait(false),
                "ROTATEKEY" => await RotateKeyAsync(args, cancellationToken).ConfigureAwait(false),
                "BACKENDMETA" => await BackendMetaAsync(args, cancellationToken).ConfigureAwait(false),
                "SWARM.RESYNC" => await SwarmResyncAsync(args, cancellationToken).ConfigureAwait(false),
                "PUBLISH" => PublishCommand(args),
                "PUBSUB" => PubSubCommand(args),
                "EVAL" => await EvalAsync(args, cancellationToken).ConfigureAwait(false),
                "EVALSHA" => await EvalShaAsync(args, cancellationToken).ConfigureAwait(false),
                "SCRIPT" => await ScriptCommandAsync(args, cancellationToken).ConfigureAwait(false),
                "QUIT" => RespValue.SimpleString("OK"),
                _ => RespValue.Error($"ERR unknown command '{command}'")
            };
        }
        catch (AccessDeniedException ex)
        {
            response = RespValue.Error("ERR " + ex.Message);
        }
        catch (DidAuthorizationException ex)
        {
            response = RespValue.Error("ERR " + ex.Message);
        }
        catch (ArgumentException ex)
        {
            response = RespValue.Error("ERR " + ex.Message);
        }
        catch (InvalidOperationException ex)
        {
            response = RespValue.Error("ERR " + ex.Message);
        }
        catch (OverflowException)
        {
            response = RespValue.Error("ERR value is not an integer or out of range");
        }

        Interlocked.Increment(ref _totalCommandsProcessed);
        if (response.Type == RespType.Error)
        {
            _logger.LogWarning("Command {Command} failed with protocol error: {Error}.", command, response.Text);
        }
        else
        {
            _logger.LogDebug("Command {Command} completed successfully.", command);
        }

        RecordCommand(command, response, stopwatch.Elapsed, correlationId);
        return response;
    }

    private RespValue SetCallerAddress(IReadOnlyList<RespValue> args)
    {
        var arityError = RequireArity(args, 2);
        if (arityError is not null)
        {
            return arityError;
        }

        if (_ethAddressAccessor is null)
        {
            return RespValue.Error("ERR AUTHADDR is not available.");
        }

        _ethAddressAccessor.CurrentAddress = EthereumAddress.Normalize(args[1].AsString());
        return RespValue.SimpleString("OK");
    }

    /// <summary>
    /// Handles the <c>AUTHDID &lt;did&gt; [&lt;proof_message&gt; &lt;proof_signature&gt;]</c> command.
    /// Registers the caller's DID (and optional Ethereum personal-sign proof) for subsequent operations.
    /// When a proof is provided it is verified immediately.
    /// </summary>
    private async Task<RespValue> SetDidContextAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 2)
        {
            return RespValue.Error("ERR wrong number of arguments for 'AUTHDID' command");
        }

        if (_didContextAccessor is null)
        {
            return RespValue.Error("ERR AUTHDID is not available.");
        }

        var did = args[1].AsString();
        DidProof? proof = null;

        if (args.Count >= 4)
        {
            var message = args[2].AsString();
            var signature = args[3].AsString();
            proof = new DidProof(message, signature);

            // Verify the proof immediately if a provider is configured.
            if (_didProvider is not null)
            {
                var ok = await _didProvider.AuthenticateAsync(did, proof, cancellationToken).ConfigureAwait(false);
                if (!ok)
                {
                    return RespValue.Error("ERR DID authentication failed: invalid proof.");
                }
            }
        }

        _didContextAccessor.Current = new DidContext(did, proof);
        return RespValue.SimpleString("OK");
    }

    private async Task<RespValue> BackupAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 1);
        if (arityError is not null)
        {
            return arityError;
        }

        if (_backupService is null)
        {
            return RespValue.Error("ERR BACKUP is not available.");
        }

        var result = await _backupService.BackupAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        return RespValue.BulkString(result.Reference);
    }

    private async Task<RespValue> RestoreDbAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count is not 2 and not 3)
        {
            return RespValue.Error("ERR wrong number of arguments for 'RESTOREDB'");
        }

        if (_restoreService is null)
        {
            return RespValue.Error("ERR RESTOREDB is not available.");
        }

        var key = args.Count == 3 ? args[2].AsString() : null;
        var result = await _restoreService.RestoreAsync(args[1].AsString(), key, cancellationToken: cancellationToken).ConfigureAwait(false);
        return RespValue.IntegerValue(result.RestoredKeyCount);
    }

    private async Task<RespValue> RotateKeyAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 3);
        if (arityError is not null)
        {
            return arityError;
        }

        if (_keyRotationService is null)
        {
            return RespValue.Error("ERR ROTATEKEY is not available.");
        }

        var result = await _keyRotationService.RotateAsync(args[1].AsString(), args[2].AsString(), cancellationToken: cancellationToken).ConfigureAwait(false);
        return RespValue.BulkString(result.ManifestReference);
    }

    private async Task<RespValue> BackendMetaAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 2);
        if (arityError is not null)
        {
            return arityError;
        }

        if (_store is not IBackendMetadataProvider metadataProvider)
        {
            return RespValue.Error("ERR BACKENDMETA is not available.");
        }

        var metadata = await metadataProvider.GetBackendMetadataAsync(args[1].AsString(), cancellationToken).ConfigureAwait(false);
        return metadata is null ? RespValue.BulkString((string?)null) : RespValue.BulkString(metadata);
    }

    private async Task<RespValue> SwarmResyncAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count > 2)
        {
            return RespValue.Error("ERR wrong number of arguments for 'SWARM.RESYNC' command");
        }

        if (_resyncCoordinator is null || ReferenceEquals(_resyncCoordinator, NoOpResyncCoordinator.Instance))
        {
            return RespValue.Error("ERR SWARM.RESYNC is not available.");
        }

        var mode = ResyncMode.Auto;
        if (args.Count == 2)
        {
            mode = args[1].AsString().ToUpperInvariant() switch
            {
                "PARTIAL" => ResyncMode.Partial,
                "FULL" => ResyncMode.Full,
                _ => throw new ArgumentException("invalid resync mode. expected PARTIAL or FULL")
            };
        }

        var result = await _resyncCoordinator.TriggerResyncAsync(mode, cancellationToken).ConfigureAwait(false);
        var payload = JsonSerializer.Serialize(new
        {
            status = "ok",
            mode = result.Mode.ToString().ToLowerInvariant(),
            keysReplayed = result.KeysReplayed,
            versionGap = result.VersionGap,
            durationSeconds = Math.Round(result.Duration.TotalSeconds, 6),
            completedAtUtc = result.CompletedAtUtc
        });
        return RespValue.BulkString(payload);
    }

    private async Task<RespValue> SetAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count != 3 && args.Count != 5)
        {
            return RespValue.Error("ERR wrong number of arguments for 'SET'");
        }

        var ttl = TryParseSetExpiryOption(args, out var setError);
        if (setError is not null)
        {
            return RespValue.Error(setError);
        }

        var key = args[1].AsString();
        var value = args[2].Bytes ?? Array.Empty<byte>();
        if (!await EnsureWritableMemoryAsync(key, value.Length, ttl is not null, cancellationToken).ConfigureAwait(false))
        {
            return RespValue.Error(OomError);
        }

        await _store.PutAsync(key, value, cancellationToken).ConfigureAwait(false);
        if (ttl is { } expiry)
        {
            var (_, ttlError) = await TrySetTtlAsync(key, expiry, cancellationToken).ConfigureAwait(false);
            if (ttlError is not null)
            {
                return ttlError;
            }
        }

        _keySizes[key] = value.Length;
        _keyLastAccessUnixMs[key] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        _keyExpiryHints[key] = ttl is null ? null : DateTimeOffset.UtcNow.Add(ttl.Value);

        return RespValue.SimpleString("OK");
    }

    private async Task<RespValue> SetExAsync(IReadOnlyList<RespValue> args, bool milliseconds, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 4);
        if (arityError is not null)
        {
            return arityError;
        }

        if (!long.TryParse(args[2].AsString(), out var ttlValue) || ttlValue <= 0)
        {
            return RespValue.Error($"ERR invalid expire time in '{args[0].AsString().ToLowerInvariant()}' command");
        }

        if (!TryParseRelativeTtl(ttlValue, milliseconds, out var ttl))
        {
            return RespValue.Error("ERR value is not an integer or out of range");
        }

        var key = args[1].AsString();
        var value = args[3].Bytes ?? Array.Empty<byte>();
        if (!await EnsureWritableMemoryAsync(key, value.Length, hasExpiry: true, cancellationToken).ConfigureAwait(false))
        {
            return RespValue.Error(OomError);
        }

        await _store.PutAsync(key, value, cancellationToken).ConfigureAwait(false);
        var (_, ttlError) = await TrySetTtlAsync(key, ttl, cancellationToken).ConfigureAwait(false);
        if (ttlError is not null)
        {
            return ttlError;
        }

        _keySizes[key] = value.Length;
        _keyLastAccessUnixMs[key] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        _keyExpiryHints[key] = DateTimeOffset.UtcNow.Add(ttl);

        return RespValue.SimpleString("OK");
    }

    private async Task<RespValue> GetAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 2);
        if (arityError is not null)
        {
            return arityError;
        }

        var key = args[1].AsString();
        var value = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
        if (value is not null)
        {
            _keySizes[key] = value.Length;
            _keyLastAccessUnixMs[key] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        }

        return RespValue.BulkString(value);
    }

    private async Task<RespValue> IncrByAsync(IReadOnlyList<RespValue> args, long? fixedDelta, CancellationToken cancellationToken)
    {
        if (fixedDelta is null && args.Count != 3)
        {
            return RespValue.Error($"ERR wrong number of arguments for '{args[0].AsString()}'");
        }

        if (fixedDelta is not null && args.Count != 2)
        {
            return RespValue.Error($"ERR wrong number of arguments for '{args[0].AsString()}'");
        }

        var key = args[1].AsString();
        var delta = fixedDelta ?? long.Parse(args[2].AsString());
        var current = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
        var currentValue = 0L;
        if (current is not null && !long.TryParse(System.Text.Encoding.UTF8.GetString(current), out currentValue))
        {
            return RespValue.Error("ERR value is not an integer or out of range");
        }

        checked
        {
            currentValue += delta;
        }

        var serialized = System.Text.Encoding.UTF8.GetBytes(currentValue.ToString(System.Globalization.CultureInfo.InvariantCulture));
        if (!await EnsureWritableMemoryAsync(key, serialized.Length, _keyExpiryHints.ContainsKey(key) && _keyExpiryHints[key] is not null, cancellationToken).ConfigureAwait(false))
        {
            return RespValue.Error(OomError);
        }

        await _store.PutAsync(key, serialized, cancellationToken).ConfigureAwait(false);
        _keySizes[key] = serialized.Length;
        _keyLastAccessUnixMs[key] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return RespValue.IntegerValue(currentValue);
    }

    private async Task<RespValue> DecrByAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 3);
        if (arityError is not null)
        {
            return arityError;
        }

        if (!long.TryParse(args[2].AsString(), out var decrement))
        {
            return RespValue.Error("ERR value is not an integer or out of range");
        }

        checked
        {
            decrement = -decrement;
        }

        return await IncrByAsync(new[] { args[0], args[1], RespValue.BulkString(decrement.ToString(System.Globalization.CultureInfo.InvariantCulture)) }, null, cancellationToken).ConfigureAwait(false);
    }

    private async Task<RespValue> DelAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        return await DeleteManyAsync(args, cancellationToken).ConfigureAwait(false);
    }

    private async Task<RespValue> MDelAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        return await DeleteManyAsync(args, cancellationToken).ConfigureAwait(false);
    }

    private async Task<RespValue> DeleteManyAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 2)
        {
            return RespValue.Error($"ERR wrong number of arguments for '{args[0].AsString()}'");
        }

        var deleted = 0;
        for (var i = 1; i < args.Count; i++)
        {
            if (await _store.DeleteAsync(args[i].AsString(), cancellationToken).ConfigureAwait(false))
            {
                _keySizes.TryRemove(args[i].AsString(), out _);
                _keyLastAccessUnixMs.TryRemove(args[i].AsString(), out _);
                _keyExpiryHints.TryRemove(args[i].AsString(), out _);
                deleted++;
            }
        }

        return RespValue.IntegerValue(deleted);
    }

    private async Task<RespValue> MGetAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 2)
        {
            return RespValue.Error("ERR wrong number of arguments for 'MGET'");
        }

        var values = new RespValue[args.Count - 1];
        for (var i = 1; i < args.Count; i++)
        {
            values[i - 1] = RespValue.BulkString(await _store.GetAsync(args[i].AsString(), cancellationToken).ConfigureAwait(false));
        }

        return RespValue.Array(values);
    }

    private async Task<RespValue> MSetAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 3 || args.Count % 2 == 0)
        {
            return RespValue.Error("ERR wrong number of arguments for 'MSET'");
        }

        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            for (var i = 1; i < args.Count; i += 2)
            {
                var key = args[i].AsString();
                var value = args[i + 1].Bytes ?? Array.Empty<byte>();
                if (!await EnsureWritableMemoryAsync(key, value.Length, false, cancellationToken).ConfigureAwait(false))
                {
                    return RespValue.Error(OomError);
                }

                await _store.PutAsync(key, value, cancellationToken).ConfigureAwait(false);
                _keySizes[key] = value.Length;
                _keyLastAccessUnixMs[key] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                _keyExpiryHints[key] = null;
            }
        }
        finally
        {
            _mutationGate.Release();
        }

        return RespValue.SimpleString("OK");
    }

    private async Task<RespValue> MSetNxAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 3 || args.Count % 2 == 0)
        {
            return RespValue.Error("ERR wrong number of arguments for 'MSETNX'");
        }

        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            for (var i = 1; i < args.Count; i += 2)
            {
                if (await _store.GetAsync(args[i].AsString(), cancellationToken).ConfigureAwait(false) is not null)
                {
                    return RespValue.IntegerValue(0);
                }
            }

            for (var i = 1; i < args.Count; i += 2)
            {
                var key = args[i].AsString();
                var value = args[i + 1].Bytes ?? Array.Empty<byte>();
                if (!await EnsureWritableMemoryAsync(key, value.Length, false, cancellationToken).ConfigureAwait(false))
                {
                    return RespValue.Error(OomError);
                }

                await _store.PutAsync(key, value, cancellationToken).ConfigureAwait(false);
                _keySizes[key] = value.Length;
                _keyLastAccessUnixMs[key] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                _keyExpiryHints[key] = null;
            }
        }
        finally
        {
            _mutationGate.Release();
        }

        return RespValue.IntegerValue(1);
    }

    private async Task<RespValue> ExistsAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 2)
        {
            return RespValue.Error("ERR wrong number of arguments for 'EXISTS'");
        }

        var found = 0;
        for (var i = 1; i < args.Count; i++)
        {
            if (await _store.GetAsync(args[i].AsString(), cancellationToken).ConfigureAwait(false) is not null)
            {
                found++;
            }
        }

        return RespValue.IntegerValue(found);
    }

    private async Task<RespValue> ExpireAsync(IReadOnlyList<RespValue> args, bool milliseconds, bool absolute, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 3);
        if (arityError is not null)
        {
            return arityError;
        }

        if (!long.TryParse(args[2].AsString(), out var ttlValue))
        {
            return RespValue.Error("ERR value is not an integer or out of range");
        }

        if (absolute)
        {
            DateTimeOffset expiresAt;
            try
            {
                expiresAt = DateTimeOffset.FromUnixTimeSeconds(ttlValue);
            }
            catch (ArgumentOutOfRangeException)
            {
                return RespValue.Error("ERR value is not an integer or out of range");
            }

            var ttl = expiresAt - DateTimeOffset.UtcNow;
            if (ttl <= TimeSpan.Zero)
            {
                var keyToDelete = args[1].AsString();
                var deleted = await _store.DeleteAsync(keyToDelete, cancellationToken).ConfigureAwait(false);
                if (deleted)
                {
                    _keySizes.TryRemove(keyToDelete, out _);
                    _keyLastAccessUnixMs.TryRemove(keyToDelete, out _);
                    _keyExpiryHints.TryRemove(keyToDelete, out _);
                }

                return RespValue.IntegerValue(deleted ? 1 : 0);
            }

            var key = args[1].AsString();
            var (absoluteUpdated, ttlError) = await TrySetTtlAsync(key, ttl, cancellationToken).ConfigureAwait(false);
            if (ttlError is not null)
            {
                return ttlError;
            }

            if (absoluteUpdated)
            {
                _keyExpiryHints[key] = DateTimeOffset.UtcNow.Add(ttl);
            }

            return RespValue.IntegerValue(absoluteUpdated ? 1 : 0);
        }

        if (ttlValue <= 0)
        {
            var key = args[1].AsString();
            var deleted = await _store.DeleteAsync(key, cancellationToken).ConfigureAwait(false);
            if (deleted)
            {
                _keySizes.TryRemove(key, out _);
                _keyLastAccessUnixMs.TryRemove(key, out _);
                _keyExpiryHints.TryRemove(key, out _);
            }

            return RespValue.IntegerValue(deleted ? 1 : 0);
        }

        if (!TryParseRelativeTtl(ttlValue, milliseconds, out var relativeTtl))
        {
            return RespValue.Error("ERR value is not an integer or out of range");
        }

        var keyWithRelativeTtl = args[1].AsString();
        var (updated, relativeTtlError) = await TrySetTtlAsync(keyWithRelativeTtl, relativeTtl, cancellationToken).ConfigureAwait(false);
        if (relativeTtlError is not null)
        {
            return relativeTtlError;
        }

        if (updated)
        {
            _keyExpiryHints[keyWithRelativeTtl] = DateTimeOffset.UtcNow.Add(relativeTtl);
        }

        return RespValue.IntegerValue(updated ? 1 : 0);
    }

    private async Task<RespValue> TtlAsync(IReadOnlyList<RespValue> args, bool milliseconds, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 2);
        if (arityError is not null)
        {
            return arityError;
        }

        var (exists, ttl) = await _store.GetTtlAsync(args[1].AsString(), cancellationToken).ConfigureAwait(false);
        if (!exists)
        {
            return RespValue.IntegerValue(-2);
        }

        if (ttl is null)
        {
            return RespValue.IntegerValue(-1);
        }

        var value = milliseconds
            ? (long)Math.Floor(ttl.Value.TotalMilliseconds)
            : (long)Math.Floor(ttl.Value.TotalSeconds);
        return RespValue.IntegerValue(Math.Max(0, value));
    }

    private async Task<RespValue> PersistAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 2);
        if (arityError is not null)
        {
            return arityError;
        }

        var key = args[1].AsString();
        var removed = await _store.RemoveTtlAsync(key, cancellationToken).ConfigureAwait(false);
        if (removed)
        {
            _keyExpiryHints[key] = null;
        }

        return RespValue.IntegerValue(removed ? 1 : 0);
    }

    private async Task<RespValue> KeysAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 2);
        if (arityError is not null)
        {
            return arityError;
        }

        var keys = await _store.ListKeysAsync(cancellationToken).ConfigureAwait(false);
        var regex = GlobToRegex(args[1].AsString());
        return RespValue.Array(keys.Where(key => regex.IsMatch(key)).Select(RespValue.BulkString).ToArray());
    }

    private async Task<RespValue> ScanAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 2 || !int.TryParse(args[1].AsString(), out var cursor) || cursor < 0)
        {
            return RespValue.Error("ERR invalid cursor");
        }

        var pattern = "*";
        var count = 10;
        for (var i = 2; i < args.Count; i += 2)
        {
            if (i + 1 >= args.Count)
            {
                return RespValue.Error("ERR syntax error");
            }

            var option = args[i].AsString().ToUpperInvariant();
            if (option == "MATCH")
            {
                pattern = args[i + 1].AsString();
            }
            else if (option == "COUNT" && int.TryParse(args[i + 1].AsString(), out var parsedCount) && parsedCount > 0)
            {
                count = parsedCount;
            }
            else
            {
                return RespValue.Error("ERR syntax error");
            }
        }

        var regex = GlobToRegex(pattern);
        var keys = (await _store.ListKeysAsync(cancellationToken).ConfigureAwait(false))
            .Where(key => regex.IsMatch(key))
            .ToArray();
        var batch = keys.Skip(cursor).Take(count).Select(RespValue.BulkString).ToArray();
        var nextCursor = cursor + batch.Length >= keys.Length ? 0 : cursor + batch.Length;
        return RespValue.Array(new[]
        {
            RespValue.BulkString(nextCursor.ToString()),
            RespValue.Array(batch)
        });
    }

    private StreamReadWaiter RegisterStreamWaiter(IReadOnlyList<string> keys, string? groupName)
    {
        var distinctKeys = keys.Distinct(StringComparer.Ordinal).ToArray();
        var waiter = new StreamReadWaiter(distinctKeys, groupName);
        lock (_streamWaitersGate)
        {
            Interlocked.Increment(ref _streamBlockedReadersTotal);
            foreach (var key in distinctKeys)
            {
                if (_streamBlockedReadersByStream.TryGetValue(key, out var count))
                {
                    _streamBlockedReadersByStream[key] = count + 1;
                }
                else
                {
                    _streamBlockedReadersByStream[key] = 1;
                }

                if (groupName is null)
                {
                    if (!_streamReadWaiters.TryGetValue(key, out var waiters))
                    {
                        waiters = new HashSet<StreamReadWaiter>();
                        _streamReadWaiters[key] = waiters;
                    }

                    waiters.Add(waiter);
                    continue;
                }

                if (!_streamReadGroupWaiters.TryGetValue(key, out var groups))
                {
                    groups = new Dictionary<string, Queue<StreamReadWaiter>>(StringComparer.Ordinal);
                    _streamReadGroupWaiters[key] = groups;
                }

                if (!groups.TryGetValue(groupName, out var queue))
                {
                    queue = new Queue<StreamReadWaiter>();
                    groups[groupName] = queue;
                }

                queue.Enqueue(waiter);
            }
        }

        return waiter;
    }

    private void UnregisterStreamWaiter(StreamReadWaiter waiter)
    {
        lock (_streamWaitersGate)
        {
            foreach (var key in waiter.Keys)
            {
                if (_streamBlockedReadersByStream.TryGetValue(key, out var count))
                {
                    if (count <= 1)
                    {
                        _streamBlockedReadersByStream.Remove(key);
                    }
                    else
                    {
                        _streamBlockedReadersByStream[key] = count - 1;
                    }
                }

                if (waiter.GroupName is null)
                {
                    if (_streamReadWaiters.TryGetValue(key, out var waiters))
                    {
                        waiters.Remove(waiter);
                        if (waiters.Count == 0)
                        {
                            _streamReadWaiters.Remove(key);
                        }
                    }

                    continue;
                }

                if (!_streamReadGroupWaiters.TryGetValue(key, out var groups) ||
                    !groups.TryGetValue(waiter.GroupName, out var queue))
                {
                    continue;
                }

                if (queue.Count > 0)
                {
                    var retained = queue.Where(candidate => !ReferenceEquals(candidate, waiter)).ToArray();
                    queue.Clear();
                    foreach (var candidate in retained)
                    {
                        queue.Enqueue(candidate);
                    }
                }

                if (queue.Count == 0)
                {
                    groups.Remove(waiter.GroupName);
                    if (groups.Count == 0)
                    {
                        _streamReadGroupWaiters.Remove(key);
                    }
                }
            }

            Interlocked.Decrement(ref _streamBlockedReadersTotal);
        }
    }

    private void NotifyStreamWaiters(string key)
    {
        List<StreamReadWaiter> wakeups;
        lock (_streamWaitersGate)
        {
            wakeups = [];
            if (_streamReadWaiters.TryGetValue(key, out var streamWaiters))
            {
                wakeups.AddRange(streamWaiters);
            }

            if (_streamReadGroupWaiters.TryGetValue(key, out var groups))
            {
                foreach (var queue in groups.Values)
                {
                    while (queue.Count > 0)
                    {
                        var waiter = queue.Dequeue();
                        if (waiter.Signal.Task.IsCompleted)
                        {
                            continue;
                        }

                        wakeups.Add(waiter);
                        break;
                    }
                }
            }
        }

        var wakeupCount = 0;
        foreach (var waiter in wakeups.Distinct())
        {
            if (waiter.TryRelease())
            {
                wakeupCount++;
            }
        }

        if (wakeupCount > 0)
        {
            Interlocked.Add(ref _streamXReadWakeupTotal, wakeupCount);
        }
    }

    private async Task<RespValue> XAddAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 5)
        {
            return RespValue.Error("ERR wrong number of arguments for 'XADD' command");
        }

        var key = args[1].AsString();
        var maxLen = _streamTrimOptions.DefaultMaxLen;
        var approximate = _streamTrimOptions.DefaultMaxLenApproximate;
        var index = 2;
        if (index < args.Count && args[index].AsString().Equals("MAXLEN", StringComparison.OrdinalIgnoreCase))
        {
            index++;
            if (index >= args.Count)
            {
                return RespValue.Error("ERR wrong number of arguments for 'XADD' command");
            }

            var mode = args[index].AsString();
            if (mode == "~")
            {
                approximate = true;
                index++;
            }
            else if (mode == "=")
            {
                index++;
            }

            if (index >= args.Count || !long.TryParse(args[index].AsString(), out var parsedMaxLen) || parsedMaxLen < 0)
            {
                return RespValue.Error("ERR value is not an integer or out of range");
            }

            maxLen = parsedMaxLen;
            index++;
        }

        if (index >= args.Count)
        {
            return RespValue.Error("ERR wrong number of arguments for 'XADD' command");
        }

        var idToken = args[index].AsString();
        index++;
        if (args.Count - index < 2 || ((args.Count - index) % 2) != 0)
        {
            return RespValue.Error("ERR wrong number of arguments for 'XADD' command");
        }

        var fields = new List<StreamField>((args.Count - index) / 2);
        for (var i = index; i < args.Count; i += 2)
        {
            fields.Add(new StreamField(args[i].Bytes ?? Array.Empty<byte>(), args[i + 1].Bytes ?? Array.Empty<byte>()));
        }

        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var existing = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
            if (!TryReadStream(existing, out var stream))
            {
                return RespValue.Error(WrongTypeError);
            }

            stream ??= new StreamData(Array.Empty<StreamEntry>(), 0, 0);
            if (!TryResolveXAddId(idToken, stream.LastTimestamp, stream.LastSequence, out var timestamp, out var sequence, out var id, out var error))
            {
                return RespValue.Error(error!);
            }

            var entries = stream.Entries.ToList();
            entries.Add(new StreamEntry(id!, timestamp, sequence, fields));
            var updated = new StreamData(entries, timestamp, sequence, stream.Groups);
            var removedPending = 0;
            if (maxLen is { } trimTo)
            {
                updated = TrimByMaxLen(updated, trimTo, approximate, out var removedEntries, out removedPending);
                if (removedEntries > 0)
                {
                    Interlocked.Add(ref _streamTrimmedTotal, removedEntries);
                }
            }

            if (removedPending > 0)
            {
                Interlocked.Add(ref _streamPendingEntriesTotal, -removedPending);
            }

            await _store.PutAsync(key, SerializeStream(updated), cancellationToken).ConfigureAwait(false);
            NotifyStreamWaiters(key);
            return RespValue.BulkString(id);
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    private async Task<RespValue> XTrimAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 4)
        {
            return RespValue.Error("ERR wrong number of arguments for 'XTRIM' command");
        }

        var key = args[1].AsString();
        var strategy = args[2].AsString().ToUpperInvariant();
        var index = 3;
        var approximate = false;
        if (index < args.Count)
        {
            var mode = args[index].AsString();
            if (mode == "~")
            {
                approximate = true;
                index++;
            }
            else if (mode == "=")
            {
                index++;
            }
        }

        if (index != args.Count - 1)
        {
            return RespValue.Error("ERR syntax error");
        }

        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var existing = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
            if (!TryReadStream(existing, out var stream))
            {
                return RespValue.Error(WrongTypeError);
            }

            if (stream is null || stream.Entries.Count == 0)
            {
                return RespValue.IntegerValue(0);
            }

            StreamData updated;
            int removedEntries;
            int removedPending;
            switch (strategy)
            {
                case "MAXLEN":
                    if (!long.TryParse(args[index].AsString(), out var maxLen) || maxLen < 0)
                    {
                        return RespValue.Error("ERR invalid arguments");
                    }

                    updated = TrimByMaxLen(stream, maxLen, approximate, out removedEntries, out removedPending);
                    break;
                case "MINID":
                    if (!TryParseStreamId(args[index].AsString(), out var minTs, out var minSeq))
                    {
                        return RespValue.Error("ERR invalid arguments");
                    }

                    updated = TrimByMinId(stream, minTs, minSeq, approximate, out removedEntries, out removedPending);
                    break;
                default:
                    return RespValue.Error("ERR syntax error");
            }

            if (removedEntries <= 0)
            {
                return RespValue.IntegerValue(0);
            }

            if (removedPending > 0)
            {
                Interlocked.Add(ref _streamPendingEntriesTotal, -removedPending);
            }

            Interlocked.Add(ref _streamTrimmedTotal, removedEntries);
            await _store.PutAsync(key, SerializeStream(updated), cancellationToken).ConfigureAwait(false);
            return RespValue.IntegerValue(removedEntries);
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    private async Task<RespValue> XRangeAsync(IReadOnlyList<RespValue> args, bool reverse, CancellationToken cancellationToken)
    {
        var command = reverse ? "XREVRANGE" : "XRANGE";
        if (args.Count != 4 && args.Count != 6)
        {
            return RespValue.Error($"ERR wrong number of arguments for '{command}' command");
        }

        long? count = null;
        if (args.Count == 6)
        {
            if (!args[4].AsString().Equals("COUNT", StringComparison.OrdinalIgnoreCase))
            {
                return RespValue.Error("ERR syntax error");
            }

            if (!long.TryParse(args[5].AsString(), out var parsedCount) || parsedCount <= 0)
            {
                return RespValue.Error("ERR value is not an integer or out of range");
            }

            count = parsedCount;
        }

        var key = args[1].AsString();
        var existing = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
        if (!TryReadStream(existing, out var stream))
        {
            return RespValue.Error(WrongTypeError);
        }

        if (stream is null || stream.Entries.Count == 0)
        {
            return RespValue.Array(Array.Empty<RespValue>());
        }

        var startToken = reverse ? args[3].AsString() : args[2].AsString();
        var endToken = reverse ? args[2].AsString() : args[3].AsString();
        if (!TryParseRangeBound(startToken, isStart: true, out var startTs, out var startSeq) ||
            !TryParseRangeBound(endToken, isStart: false, out var endTs, out var endSeq))
        {
            return RespValue.Error("ERR Invalid stream ID specified as stream command argument");
        }

        IEnumerable<StreamEntry> query = reverse
            ? stream.Entries.OrderByDescending(static entry => entry.Timestamp).ThenByDescending(static entry => entry.Sequence)
            : stream.Entries.OrderBy(static entry => entry.Timestamp).ThenBy(static entry => entry.Sequence);

        query = query.Where(entry =>
            CompareStreamIds(entry.Timestamp, entry.Sequence, startTs, startSeq) >= 0 &&
            CompareStreamIds(entry.Timestamp, entry.Sequence, endTs, endSeq) <= 0);

        if (count is { } countValue)
        {
            query = query.Take((int)countValue);
        }

        return RespValue.Array(query.Select(ToRangeRespValue).ToArray());
    }

    private async Task<RespValue> XLenAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 2);
        if (arityError is not null)
        {
            return arityError;
        }

        var existing = await _store.GetAsync(args[1].AsString(), cancellationToken).ConfigureAwait(false);
        if (!TryReadStream(existing, out var stream))
        {
            return RespValue.Error(WrongTypeError);
        }

        return RespValue.IntegerValue(stream?.Entries.Count ?? 0);
    }

    private async Task<RespValue> XReadAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 4)
        {
            return RespValue.Error("ERR wrong number of arguments for 'XREAD' command");
        }

        var index = 1;
        var count = 1_000L;
        long? blockMilliseconds = null;
        while (index < args.Count)
        {
            var option = args[index].AsString().ToUpperInvariant();
            if (option == "COUNT")
            {
                if (index + 1 >= args.Count || !long.TryParse(args[index + 1].AsString(), out count) || count <= 0)
                {
                    return RespValue.Error("ERR value is not an integer or out of range");
                }

                index += 2;
                continue;
            }

            if (option == "BLOCK")
            {
                if (index + 1 >= args.Count || !long.TryParse(args[index + 1].AsString(), out var parsedBlock) || parsedBlock < 0)
                {
                    return RespValue.Error("ERR value is not an integer or out of range");
                }

                blockMilliseconds = parsedBlock;
                index += 2;
                continue;
            }

            break;
        }

        if (index >= args.Count || !args[index].AsString().Equals("STREAMS", StringComparison.OrdinalIgnoreCase))
        {
            return RespValue.Error("ERR syntax error");
        }

        var remaining = args.Count - index - 1;
        if (remaining < 2 || (remaining % 2) != 0)
        {
            return RespValue.Error("ERR syntax error");
        }

        var streamCount = remaining / 2;
        var keys = new string[streamCount];
        var idTokens = new string[streamCount];
        for (var i = 0; i < streamCount; i++)
        {
            keys[i] = args[index + 1 + i].AsString();
            idTokens[i] = args[index + 1 + streamCount + i].AsString();
        }

        var (streamStarts, startError) = await ResolveXReadStartsAsync(keys, idTokens, cancellationToken).ConfigureAwait(false);
        if (startError is not null)
        {
            return startError;
        }

        if (streamStarts is null)
        {
            return RespValue.Error("ERR Invalid stream ID specified as stream command argument");
        }

        var deadlineUtc = blockMilliseconds is > 0
            ? DateTime.UtcNow.AddMilliseconds(blockMilliseconds.Value)
            : (DateTime?)null;
        while (true)
        {
            var response = await TryReadStreamsAsync(streamStarts, count, cancellationToken).ConfigureAwait(false);
            if (response is not null)
            {
                return response;
            }

            if (!blockMilliseconds.HasValue)
            {
                return RespValue.NullArray();
            }

            var waiter = RegisterStreamWaiter(keys, groupName: null);
            try
            {
                if (blockMilliseconds.Value > 0)
                {
                    var remainingTimeout = deadlineUtc!.Value - DateTime.UtcNow;
                    if (remainingTimeout <= TimeSpan.Zero)
                    {
                        return RespValue.NullArray();
                    }

                    using var timeoutCts = new CancellationTokenSource(remainingTimeout);
                    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
                    try
                    {
                        await waiter.Signal.Task.WaitAsync(linkedCts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                    {
                        return RespValue.NullArray();
                    }
                }
                else
                {
                    await waiter.Signal.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                UnregisterStreamWaiter(waiter);
            }
        }
    }

    private async Task<(List<(string Key, ulong Timestamp, ulong Sequence)>? Starts, RespValue? Error)> ResolveXReadStartsAsync(
        IReadOnlyList<string> keys,
        IReadOnlyList<string> idTokens,
        CancellationToken cancellationToken)
    {
        var starts = new List<(string Key, ulong Timestamp, ulong Sequence)>(keys.Count);
        for (var i = 0; i < keys.Count; i++)
        {
            if (idTokens[i] == "$")
            {
                var existing = await _store.GetAsync(keys[i], cancellationToken).ConfigureAwait(false);
                if (!TryReadStream(existing, out var stream))
                {
                    return (null, RespValue.Error(WrongTypeError));
                }

                var timestamp = stream?.LastTimestamp ?? 0;
                var sequence = stream?.LastSequence ?? 0;
                starts.Add((keys[i], timestamp, sequence));
                continue;
            }

            if (!TryParseStreamId(idTokens[i], out var timestampToken, out var sequenceToken))
            {
                return (null, null);
            }

            starts.Add((keys[i], timestampToken, sequenceToken));
        }

        return (starts, null);
    }

    private async Task<RespValue?> TryReadStreamsAsync(
        IReadOnlyList<(string Key, ulong Timestamp, ulong Sequence)> starts,
        long count,
        CancellationToken cancellationToken)
    {
        var streamResponses = new List<RespValue>(starts.Count);
        foreach (var start in starts)
        {
            var existing = await _store.GetAsync(start.Key, cancellationToken).ConfigureAwait(false);
            if (!TryReadStream(existing, out var stream))
            {
                return RespValue.Error(WrongTypeError);
            }

            if (stream is null)
            {
                continue;
            }

            var selected = stream.Entries
                .Where(entry => CompareStreamIds(entry.Timestamp, entry.Sequence, start.Timestamp, start.Sequence) > 0)
                .Take((int)Math.Min(int.MaxValue, count))
                .ToArray();
            if (selected.Length == 0)
            {
                continue;
            }

            streamResponses.Add(RespValue.Array(new RespValue[]
            {
                RespValue.BulkString(start.Key),
                RespValue.Array(selected.Select(ToRangeRespValue).ToArray())
            }));
        }

        return streamResponses.Count == 0 ? null : RespValue.Array(streamResponses.ToArray());
    }

    private async Task<RespValue> XGroupAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 2)
        {
            return RespValue.Error("ERR wrong number of arguments for 'XGROUP' command");
        }

        var subCommand = args[1].AsString().ToUpperInvariant();
        return subCommand switch
        {
            "CREATE" => await XGroupCreateAsync(args, cancellationToken).ConfigureAwait(false),
            "SETID" => await XGroupSetIdAsync(args, cancellationToken).ConfigureAwait(false),
            "DESTROY" => await XGroupDestroyAsync(args, cancellationToken).ConfigureAwait(false),
            "DELCONSUMER" => await XGroupDelConsumerAsync(args, cancellationToken).ConfigureAwait(false),
            _ => RespValue.Error("ERR Unknown XGROUP subcommand")
        };
    }

    private async Task<RespValue> XGroupCreateAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 5)
        {
            return RespValue.Error("ERR wrong number of arguments for 'XGROUP CREATE' command");
        }

        var key = args[2].AsString();
        var groupName = args[3].AsString();
        var idToken = args[4].AsString();
        var mkStream = false;
        for (var i = 5; i < args.Count; i++)
        {
            var option = args[i].AsString().ToUpperInvariant();
            if (option == "MKSTREAM")
            {
                mkStream = true;
                continue;
            }

            if (option == "ENTRIESREAD")
            {
                if (i + 1 >= args.Count || !long.TryParse(args[i + 1].AsString(), out _))
                {
                    return RespValue.Error("ERR value is not an integer or out of range");
                }

                i++;
                continue;
            }

            return RespValue.Error("ERR syntax error");
        }

        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var existing = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
            if (!TryReadStream(existing, out var stream))
            {
                return RespValue.Error(WrongTypeError);
            }

            if (stream is null && !mkStream)
            {
                return RespValue.Error("ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use MKSTREAM to create an empty stream automatically.");
            }

            stream ??= new StreamData(Array.Empty<StreamEntry>(), 0, 0, new Dictionary<string, ConsumerGroupState>(StringComparer.Ordinal));
            var groups = CloneGroups(stream);
            if (groups.ContainsKey(groupName))
            {
                return RespValue.Error(BusyGroupError);
            }

            if (!TryResolveGroupIdToken(idToken, stream, out var lastTs, out var lastSeq, out var lastId, out var idError))
            {
                return RespValue.Error(idError!);
            }

            groups[groupName] = new ConsumerGroupState(lastId!, lastTs, lastSeq, new Dictionary<string, PendingEntryState>(StringComparer.Ordinal), new Dictionary<string, ConsumerState>(StringComparer.Ordinal));
            await _store.PutAsync(key, SerializeStream(new StreamData(stream.Entries, stream.LastTimestamp, stream.LastSequence, groups)), cancellationToken).ConfigureAwait(false);
            return RespValue.SimpleString("OK");
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    private async Task<RespValue> XGroupSetIdAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count != 5)
        {
            return RespValue.Error("ERR wrong number of arguments for 'XGROUP SETID' command");
        }

        var key = args[2].AsString();
        var groupName = args[3].AsString();
        var idToken = args[4].AsString();
        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var existing = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
            if (!TryReadStream(existing, out var stream))
            {
                return RespValue.Error(WrongTypeError);
            }

            if (stream is null)
            {
                return RespValue.Error(NoGroupError(key, groupName));
            }

            var groups = CloneGroups(stream);
            if (!groups.TryGetValue(groupName, out var group))
            {
                return RespValue.Error(NoGroupError(key, groupName));
            }

            if (!TryResolveGroupIdToken(idToken, stream, out var ts, out var seq, out var id, out var error))
            {
                return RespValue.Error(error!);
            }

            groups[groupName] = group with { LastDeliveredId = id!, LastDeliveredTimestamp = ts, LastDeliveredSequence = seq };
            await _store.PutAsync(key, SerializeStream(stream with { Groups = groups }), cancellationToken).ConfigureAwait(false);
            return RespValue.SimpleString("OK");
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    private async Task<RespValue> XGroupDestroyAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count != 4)
        {
            return RespValue.Error("ERR wrong number of arguments for 'XGROUP DESTROY' command");
        }

        var key = args[2].AsString();
        var groupName = args[3].AsString();
        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var existing = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
            if (!TryReadStream(existing, out var stream))
            {
                return RespValue.Error(WrongTypeError);
            }

            if (stream is null)
            {
                return RespValue.IntegerValue(0);
            }

            var groups = CloneGroups(stream);
            if (!groups.TryGetValue(groupName, out var group))
            {
                return RespValue.IntegerValue(0);
            }

            Interlocked.Add(ref _streamPendingEntriesTotal, -(group.Pending?.Count ?? 0));
            groups.Remove(groupName);
            await _store.PutAsync(key, SerializeStream(stream with { Groups = groups }), cancellationToken).ConfigureAwait(false);
            return RespValue.IntegerValue(1);
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    private async Task<RespValue> XGroupDelConsumerAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count != 5)
        {
            return RespValue.Error("ERR wrong number of arguments for 'XGROUP DELCONSUMER' command");
        }

        var key = args[2].AsString();
        var groupName = args[3].AsString();
        var consumer = args[4].AsString();
        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var existing = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
            if (!TryReadStream(existing, out var stream))
            {
                return RespValue.Error(WrongTypeError);
            }

            if (stream is null)
            {
                return RespValue.Error(NoGroupError(key, groupName));
            }

            var groups = CloneGroups(stream);
            if (!groups.TryGetValue(groupName, out var group))
            {
                return RespValue.Error(NoGroupError(key, groupName));
            }

            var pending = ClonePending(group);
            var removed = 0;
            foreach (var entry in pending.ToList())
            {
                if (!entry.Value.Consumer.Equals(consumer, StringComparison.Ordinal))
                {
                    continue;
                }

                pending.Remove(entry.Key);
                removed++;
            }

            var consumers = CloneConsumers(group);
            consumers.Remove(consumer);
            Interlocked.Add(ref _streamPendingEntriesTotal, -removed);
            groups[groupName] = group with { Pending = pending, Consumers = consumers };
            await _store.PutAsync(key, SerializeStream(stream with { Groups = groups }), cancellationToken).ConfigureAwait(false);
            return RespValue.IntegerValue(removed);
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    private async Task<RespValue> XReadGroupAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 7)
        {
            return RespValue.Error("ERR wrong number of arguments for 'XREADGROUP' command");
        }

        var index = 1;
        if (!args[index].AsString().Equals("GROUP", StringComparison.OrdinalIgnoreCase) || index + 2 >= args.Count)
        {
            return RespValue.Error("ERR syntax error");
        }

        var groupName = args[index + 1].AsString();
        var consumerName = args[index + 2].AsString();
        index += 3;

        var count = 1_000L;
        long? blockMilliseconds = null;
        var noAck = false;
        while (index < args.Count)
        {
            var option = args[index].AsString().ToUpperInvariant();
            if (option == "COUNT")
            {
                if (index + 1 >= args.Count || !long.TryParse(args[index + 1].AsString(), out count) || count <= 0)
                {
                    return RespValue.Error("ERR value is not an integer or out of range");
                }

                index += 2;
                continue;
            }

            if (option == "BLOCK")
            {
                if (index + 1 >= args.Count || !long.TryParse(args[index + 1].AsString(), out var parsedBlock) || parsedBlock < 0)
                {
                    return RespValue.Error("ERR value is not an integer or out of range");
                }

                blockMilliseconds = parsedBlock;
                index += 2;
                continue;
            }

            if (option == "NOACK")
            {
                noAck = true;
                index++;
                continue;
            }

            break;
        }

        if (index >= args.Count || !args[index].AsString().Equals("STREAMS", StringComparison.OrdinalIgnoreCase))
        {
            return RespValue.Error("ERR syntax error");
        }

        var remaining = args.Count - index - 1;
        if (remaining < 2 || (remaining % 2) != 0)
        {
            return RespValue.Error("ERR syntax error");
        }

        var streamCount = remaining / 2;
        var keys = new string[streamCount];
        var startIdTokens = new string[streamCount];
        for (var i = 0; i < streamCount; i++)
        {
            keys[i] = args[index + 1 + i].AsString();
            startIdTokens[i] = args[index + 1 + streamCount + i].AsString();
        }

        var blockableKeys = keys
            .Where((_, i) => startIdTokens[i] == ">")
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        var deadlineUtc = blockMilliseconds is > 0
            ? DateTime.UtcNow.AddMilliseconds(blockMilliseconds.Value)
            : (DateTime?)null;
        while (true)
        {
            var response = await TryReadGroupStreamsAsync(
                groupName,
                consumerName,
                keys,
                startIdTokens,
                count,
                noAck,
                cancellationToken).ConfigureAwait(false);
            if (response is not null)
            {
                return response;
            }

            if (!blockMilliseconds.HasValue || blockableKeys.Length == 0)
            {
                return RespValue.NullArray();
            }

            var waiter = RegisterStreamWaiter(blockableKeys, groupName);
            try
            {
                if (blockMilliseconds.Value > 0)
                {
                    var remainingTimeout = deadlineUtc!.Value - DateTime.UtcNow;
                    if (remainingTimeout <= TimeSpan.Zero)
                    {
                        return RespValue.NullArray();
                    }

                    using var timeoutCts = new CancellationTokenSource(remainingTimeout);
                    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
                    try
                    {
                        await waiter.Signal.Task.WaitAsync(linkedCts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                    {
                        return RespValue.NullArray();
                    }
                }
                else
                {
                    await waiter.Signal.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                UnregisterStreamWaiter(waiter);
            }
        }
    }

    private async Task<RespValue?> TryReadGroupStreamsAsync(
        string groupName,
        string consumerName,
        IReadOnlyList<string> keys,
        IReadOnlyList<string> startIdTokens,
        long count,
        bool noAck,
        CancellationToken cancellationToken)
    {
        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var streamResponses = new List<RespValue>(keys.Count);
            var updates = new List<(string Key, StreamData Stream)>(keys.Count);
            var deliveredFromNew = 0;
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            for (var i = 0; i < keys.Count; i++)
            {
                var key = keys[i];
                var startIdToken = startIdTokens[i];
                var existing = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
                if (!TryReadStream(existing, out var stream))
                {
                    return RespValue.Error(WrongTypeError);
                }

                if (stream is null)
                {
                    return RespValue.Error(NoGroupErrorForReadGroup(key, groupName));
                }

                var groups = CloneGroups(stream);
                if (!groups.TryGetValue(groupName, out var group))
                {
                    return RespValue.Error(NoGroupErrorForReadGroup(key, groupName));
                }

                var consumers = CloneConsumers(group);
                var pending = ClonePending(group);
                consumers[consumerName] = new ConsumerState(consumerName, nowMs);

                List<StreamEntry> selected;
                if (startIdToken == ">")
                {
                    selected = stream.Entries
                        .Where(entry => CompareStreamIds(entry.Timestamp, entry.Sequence, group.LastDeliveredTimestamp, group.LastDeliveredSequence) > 0)
                        .Take((int)Math.Min(int.MaxValue, count))
                        .ToList();
                }
                else
                {
                    if (!TryParseStreamId(startIdToken, out var startTs, out var startSeq))
                    {
                        return RespValue.Error("ERR Invalid stream ID specified as stream command argument");
                    }

                    selected = pending.Values
                        .Where(entry => entry.Consumer.Equals(consumerName, StringComparison.Ordinal) &&
                                        CompareStreamIds(entry.Timestamp, entry.Sequence, startTs, startSeq) >= 0)
                        .OrderBy(static item => item.Timestamp)
                        .ThenBy(static item => item.Sequence)
                        .Take((int)Math.Min(int.MaxValue, count))
                        .Select(item => stream.Entries.FirstOrDefault(candidate => candidate.Id.Equals(item.Id, StringComparison.Ordinal)))
                        .Where(static entry => entry is not null)
                        .Select(static entry => entry!)
                        .ToList();
                }

                if (selected.Count == 0)
                {
                    continue;
                }

                foreach (var entry in selected)
                {
                    if (startIdToken == ">")
                    {
                        group = group with
                        {
                            LastDeliveredId = entry.Id,
                            LastDeliveredTimestamp = entry.Timestamp,
                            LastDeliveredSequence = entry.Sequence
                        };
                    }

                    if (noAck)
                    {
                        continue;
                    }

                    if (pending.TryGetValue(entry.Id, out var existingPending))
                    {
                        pending[entry.Id] = existingPending with
                        {
                            Consumer = consumerName,
                            LastDeliveredUnixMs = nowMs,
                            DeliveryCount = existingPending.DeliveryCount + 1
                        };
                    }
                    else
                    {
                        pending[entry.Id] = new PendingEntryState(entry.Id, entry.Timestamp, entry.Sequence, consumerName, nowMs, 1);
                        deliveredFromNew++;
                    }
                }

                groups[groupName] = group with { Pending = pending, Consumers = consumers };
                updates.Add((key, stream with { Groups = groups }));
                streamResponses.Add(RespValue.Array(new RespValue[]
                {
                    RespValue.BulkString(key),
                    RespValue.Array(selected.Select(ToRangeRespValue).ToArray())
                }));
            }

            if (streamResponses.Count == 0)
            {
                return null;
            }

            foreach (var update in updates)
            {
                await _store.PutAsync(update.Key, SerializeStream(update.Stream), cancellationToken).ConfigureAwait(false);
            }

            if (deliveredFromNew > 0)
            {
                Interlocked.Add(ref _streamPendingEntriesTotal, deliveredFromNew);
            }

            return RespValue.Array(streamResponses.ToArray());
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    private async Task<RespValue> XAckAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 4)
        {
            return RespValue.Error("ERR wrong number of arguments for 'XACK' command");
        }

        var key = args[1].AsString();
        var groupName = args[2].AsString();
        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var existing = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
            if (!TryReadStream(existing, out var stream))
            {
                return RespValue.Error(WrongTypeError);
            }

            if (stream is null)
            {
                return RespValue.Error(NoGroupError(key, groupName));
            }

            var groups = CloneGroups(stream);
            if (!groups.TryGetValue(groupName, out var group))
            {
                return RespValue.Error(NoGroupError(key, groupName));
            }

            var pending = ClonePending(group);
            var acked = 0;
            for (var i = 3; i < args.Count; i++)
            {
                var id = args[i].AsString();
                if (!pending.Remove(id))
                {
                    continue;
                }

                acked++;
            }

            if (acked > 0)
            {
                Interlocked.Add(ref _streamPendingEntriesTotal, -acked);
                Interlocked.Add(ref _streamXAckTotal, acked);
            }

            groups[groupName] = group with { Pending = pending };
            await _store.PutAsync(key, SerializeStream(stream with { Groups = groups }), cancellationToken).ConfigureAwait(false);
            return RespValue.IntegerValue(acked);
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    private async Task<RespValue> XPendingAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count is not 3 and < 6)
        {
            return RespValue.Error("ERR wrong number of arguments for 'XPENDING' command");
        }

        var key = args[1].AsString();
        var groupName = args[2].AsString();
        var existing = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
        if (!TryReadStream(existing, out var stream))
        {
            return RespValue.Error(WrongTypeError);
        }

        if (stream is null)
        {
            return RespValue.Error(NoGroupError(key, groupName));
        }

        var groups = CloneGroups(stream);
        if (!groups.TryGetValue(groupName, out var group))
        {
            return RespValue.Error(NoGroupError(key, groupName));
        }

        var pending = ClonePending(group).Values
            .OrderBy(static item => item.Timestamp)
            .ThenBy(static item => item.Sequence)
            .ToList();
        if (args.Count == 3)
        {
            if (pending.Count == 0)
            {
                return RespValue.Array(new RespValue[]
                {
                    RespValue.IntegerValue(0),
                    RespValue.BulkString((string?)null),
                    RespValue.BulkString((string?)null),
                    RespValue.Array(Array.Empty<RespValue>())
                });
            }

            var byConsumer = pending
                .GroupBy(static item => item.Consumer, StringComparer.Ordinal)
                .OrderBy(static groupBy => groupBy.Key, StringComparer.Ordinal)
                .Select(groupBy => RespValue.Array(new[]
                {
                    RespValue.BulkString(groupBy.Key),
                    RespValue.IntegerValue(groupBy.LongCount())
                }))
                .ToArray();

            return RespValue.Array(new RespValue[]
            {
                RespValue.IntegerValue(pending.Count),
                RespValue.BulkString(pending[0].Id),
                RespValue.BulkString(pending[^1].Id),
                RespValue.Array(byConsumer)
            });
        }

        var index = 3;
        long? minIdleMs = null;
        if (args[index].AsString().Equals("IDLE", StringComparison.OrdinalIgnoreCase))
        {
            if (index + 1 >= args.Count || !long.TryParse(args[index + 1].AsString(), out var parsedIdle) || parsedIdle < 0)
            {
                return RespValue.Error("ERR value is not an integer or out of range");
            }

            minIdleMs = parsedIdle;
            index += 2;
        }

        if (index + 2 >= args.Count)
        {
            return RespValue.Error("ERR wrong number of arguments for 'XPENDING' command");
        }

        var startToken = args[index].AsString();
        var endToken = args[index + 1].AsString();
        if (!TryParseRangeBound(startToken, isStart: true, out var startTs, out var startSeq) ||
            !TryParseRangeBound(endToken, isStart: false, out var endTs, out var endSeq))
        {
            return RespValue.Error("ERR Invalid stream ID specified as stream command argument");
        }

        if (!long.TryParse(args[index + 2].AsString(), out var count) || count <= 0)
        {
            return RespValue.Error("ERR value is not an integer or out of range");
        }

        var consumer = index + 3 < args.Count ? args[index + 3].AsString() : null;
        if (index + 4 < args.Count)
        {
            return RespValue.Error("ERR syntax error");
        }

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var detailed = pending
            .Where(item => CompareStreamIds(item.Timestamp, item.Sequence, startTs, startSeq) >= 0 &&
                           CompareStreamIds(item.Timestamp, item.Sequence, endTs, endSeq) <= 0 &&
                           (consumer is null || item.Consumer.Equals(consumer, StringComparison.Ordinal)) &&
                           (!minIdleMs.HasValue || nowMs - item.LastDeliveredUnixMs >= minIdleMs.Value))
            .Take((int)Math.Min(int.MaxValue, count))
            .Select(item => RespValue.Array(new[]
            {
                RespValue.BulkString(item.Id),
                RespValue.BulkString(item.Consumer),
                RespValue.IntegerValue(Math.Max(0, nowMs - item.LastDeliveredUnixMs)),
                RespValue.IntegerValue(item.DeliveryCount)
            }))
            .ToArray();
        return RespValue.Array(detailed);
    }

    private async Task<RespValue> XClaimAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 6)
        {
            return RespValue.Error("ERR wrong number of arguments for 'XCLAIM' command");
        }

        var key = args[1].AsString();
        var groupName = args[2].AsString();
        var consumer = args[3].AsString();
        if (!long.TryParse(args[4].AsString(), out var minIdleMs) || minIdleMs < 0)
        {
            return RespValue.Error("ERR value is not an integer or out of range");
        }

        var ids = args.Skip(5).Select(static value => value.AsString()).ToArray();
        return await ClaimPendingEntriesAsync(key, groupName, consumer, minIdleMs, ids, cancellationToken).ConfigureAwait(false);
    }

    private async Task<RespValue> XAutoClaimAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 6)
        {
            return RespValue.Error("ERR wrong number of arguments for 'XAUTOCLAIM' command");
        }

        var key = args[1].AsString();
        var groupName = args[2].AsString();
        var consumer = args[3].AsString();
        if (!long.TryParse(args[4].AsString(), out var minIdleMs) || minIdleMs < 0)
        {
            return RespValue.Error("ERR value is not an integer or out of range");
        }

        var startToken = args[5].AsString();
        if (!TryParseStreamId(startToken, out var startTs, out var startSeq))
        {
            return RespValue.Error("ERR Invalid stream ID specified as stream command argument");
        }

        var count = 100L;
        if (args.Count > 6)
        {
            if (args.Count != 8 || !args[6].AsString().Equals("COUNT", StringComparison.OrdinalIgnoreCase) ||
                !long.TryParse(args[7].AsString(), out count) || count <= 0)
            {
                return RespValue.Error("ERR syntax error");
            }
        }

        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var existing = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
            if (!TryReadStream(existing, out var stream))
            {
                return RespValue.Error(WrongTypeError);
            }

            if (stream is null)
            {
                return RespValue.Error(NoGroupError(key, groupName));
            }

            var groups = CloneGroups(stream);
            if (!groups.TryGetValue(groupName, out var group))
            {
                return RespValue.Error(NoGroupError(key, groupName));
            }

            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var pending = ClonePending(group);
            var consumers = CloneConsumers(group);
            consumers[consumer] = new ConsumerState(consumer, nowMs);

            var claims = new List<StreamEntry>();
            var claimedCount = 0L;
            var nextId = "0-0";
            foreach (var pel in pending.Values.OrderBy(static value => value.Timestamp).ThenBy(static value => value.Sequence))
            {
                if (CompareStreamIds(pel.Timestamp, pel.Sequence, startTs, startSeq) < 0)
                {
                    continue;
                }

                nextId = pel.Id;
                if (claimedCount >= count)
                {
                    break;
                }

                if (nowMs - pel.LastDeliveredUnixMs < minIdleMs)
                {
                    continue;
                }

                var entry = stream.Entries.FirstOrDefault(candidate => candidate.Id.Equals(pel.Id, StringComparison.Ordinal));
                if (entry is null)
                {
                    continue;
                }

                pending[pel.Id] = pel with { Consumer = consumer, LastDeliveredUnixMs = nowMs, DeliveryCount = pel.DeliveryCount + 1 };
                claims.Add(entry);
                claimedCount++;
            }

            if (claimedCount > 0)
            {
                Interlocked.Add(ref _streamXClaimTotal, claimedCount);
            }

            groups[groupName] = group with { Pending = pending, Consumers = consumers };
            await _store.PutAsync(key, SerializeStream(stream with { Groups = groups }), cancellationToken).ConfigureAwait(false);
            return RespValue.Array(new RespValue[]
            {
                RespValue.BulkString(nextId),
                RespValue.Array(claims.Select(ToRangeRespValue).ToArray()),
                RespValue.Array(Array.Empty<RespValue>())
            });
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    private async Task<RespValue> ClaimPendingEntriesAsync(
        string key,
        string groupName,
        string consumer,
        long minIdleMs,
        IReadOnlyList<string> ids,
        CancellationToken cancellationToken)
    {
        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var existing = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
            if (!TryReadStream(existing, out var stream))
            {
                return RespValue.Error(WrongTypeError);
            }

            if (stream is null)
            {
                return RespValue.Error(NoGroupError(key, groupName));
            }

            var groups = CloneGroups(stream);
            if (!groups.TryGetValue(groupName, out var group))
            {
                return RespValue.Error(NoGroupError(key, groupName));
            }

            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var pending = ClonePending(group);
            var consumers = CloneConsumers(group);
            consumers[consumer] = new ConsumerState(consumer, nowMs);
            var claimedEntries = new List<StreamEntry>();
            var claimed = 0L;
            foreach (var id in ids)
            {
                if (!pending.TryGetValue(id, out var pel))
                {
                    continue;
                }

                if (nowMs - pel.LastDeliveredUnixMs < minIdleMs)
                {
                    continue;
                }

                var entry = stream.Entries.FirstOrDefault(candidate => candidate.Id.Equals(id, StringComparison.Ordinal));
                if (entry is null)
                {
                    continue;
                }

                pending[id] = pel with { Consumer = consumer, LastDeliveredUnixMs = nowMs, DeliveryCount = pel.DeliveryCount + 1 };
                claimedEntries.Add(entry);
                claimed++;
            }

            if (claimed > 0)
            {
                Interlocked.Add(ref _streamXClaimTotal, claimed);
            }

            groups[groupName] = group with { Pending = pending, Consumers = consumers };
            await _store.PutAsync(key, SerializeStream(stream with { Groups = groups }), cancellationToken).ConfigureAwait(false);
            return RespValue.Array(claimedEntries.Select(ToRangeRespValue).ToArray());
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    private async Task<RespValue> TypeAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 2);
        if (arityError is not null)
        {
            return arityError;
        }

        var value = await _store.GetAsync(args[1].AsString(), cancellationToken).ConfigureAwait(false);
        if (value is null)
        {
            return RespValue.SimpleString("none");
        }

        return RespValue.SimpleString(IsSerializedStreamValue(value) ? "stream" : "string");
    }

    private static RespValue? RequireArity(IReadOnlyList<RespValue> args, int expected) =>
        args.Count == expected ? null : RespValue.Error($"ERR wrong number of arguments for '{args[0].AsString()}'");

    private static bool IsQuit(RespValue request) =>
        request.Type == RespType.Array && request.Items is { Count: > 0 } && request.Items[0].AsString().Equals("QUIT", StringComparison.OrdinalIgnoreCase);

    private static RespValue ToRangeRespValue(StreamEntry entry)
    {
        var fields = new List<RespValue>(entry.Fields.Count * 2);
        foreach (var field in entry.Fields)
        {
            fields.Add(RespValue.BulkString(field.Name));
            fields.Add(RespValue.BulkString(field.Value));
        }

        return RespValue.Array(new[]
        {
            RespValue.BulkString(entry.Id),
            RespValue.Array(fields)
        });
    }

    private static StreamData TrimByMaxLen(
        StreamData stream,
        long maxLen,
        bool approximate,
        out int removedEntries,
        out int removedPending)
    {
        removedEntries = 0;
        removedPending = 0;
        if (stream.Entries.Count == 0)
        {
            return stream;
        }

        long targetLen = maxLen;
        if (approximate && maxLen > 0)
        {
            var slack = (long)Math.Floor(maxLen * 0.1);
            targetLen = maxLen + slack;
        }

        if (targetLen < 0 || stream.Entries.Count <= targetLen)
        {
            return stream;
        }

        var removeCountLong = stream.Entries.Count - targetLen;
        if (removeCountLong <= 0)
        {
            return stream;
        }

        var removeCount = (int)Math.Min(int.MaxValue, removeCountLong);
        return TrimFirstEntries(stream, removeCount, out removedEntries, out removedPending);
    }

    private static StreamData TrimByMinId(
        StreamData stream,
        ulong thresholdTimestamp,
        ulong thresholdSequence,
        bool approximate,
        out int removedEntries,
        out int removedPending)
    {
        removedEntries = 0;
        removedPending = 0;
        if (stream.Entries.Count == 0)
        {
            return stream;
        }

        var removeCount = 0;
        foreach (var entry in stream.Entries)
        {
            if (CompareStreamIds(entry.Timestamp, entry.Sequence, thresholdTimestamp, thresholdSequence) >= 0)
            {
                break;
            }

            removeCount++;
        }

        if (removeCount <= 0)
        {
            return stream;
        }

        if (approximate && removeCount < stream.Entries.Count)
        {
            var slack = Math.Max(1, (int)Math.Ceiling(stream.Entries.Count * 0.1));
            removeCount = Math.Max(0, removeCount - Math.Min(removeCount, slack));
        }

        if (removeCount <= 0)
        {
            return stream;
        }

        return TrimFirstEntries(stream, removeCount, out removedEntries, out removedPending);
    }

    private static StreamData TrimFirstEntries(StreamData stream, int removeCount, out int removedEntries, out int removedPending)
    {
        removedEntries = 0;
        removedPending = 0;
        if (removeCount <= 0 || stream.Entries.Count == 0)
        {
            return stream;
        }

        var boundedRemoveCount = Math.Min(removeCount, stream.Entries.Count);
        var removed = stream.Entries.Take(boundedRemoveCount).ToArray();
        var kept = stream.Entries.Skip(boundedRemoveCount).ToArray();
        if (removed.Length == 0)
        {
            return stream;
        }

        removedEntries = removed.Length;
        var removedIds = removed.Select(static entry => entry.Id).ToHashSet(StringComparer.Ordinal);
        if (stream.Groups is null || stream.Groups.Count == 0)
        {
            return stream with { Entries = kept };
        }

        var groups = CloneGroups(stream);
        var groupsChanged = false;
        foreach (var groupEntry in groups.ToArray())
        {
            var group = groupEntry.Value;
            var pending = ClonePending(group);
            var pendingRemovedForGroup = 0;
            foreach (var pendingId in pending.Keys.ToArray())
            {
                if (!removedIds.Contains(pendingId))
                {
                    continue;
                }

                pending.Remove(pendingId);
                pendingRemovedForGroup++;
            }

            if (pendingRemovedForGroup == 0)
            {
                continue;
            }

            removedPending += pendingRemovedForGroup;
            groups[groupEntry.Key] = group with { Pending = pending };
            groupsChanged = true;
        }

        return new StreamData(
            kept,
            stream.LastTimestamp,
            stream.LastSequence,
            groupsChanged ? groups : stream.Groups);
    }

    private static bool TryResolveXAddId(string token, ulong lastTs, ulong lastSeq, out ulong timestamp, out ulong sequence, out string? id, out string? error)
    {
        timestamp = 0;
        sequence = 0;
        id = null;
        error = null;

        if (token == "*")
        {
            timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (timestamp < lastTs)
            {
                timestamp = lastTs;
            }

            sequence = timestamp == lastTs ? checked(lastSeq + 1UL) : 0;
            id = $"{timestamp}-{sequence}";
            return true;
        }

        var separator = token.IndexOf('-');
        if (separator <= 0 || separator >= token.Length - 1)
        {
            error = "ERR Invalid stream ID specified as stream command argument";
            return false;
        }

        if (!ulong.TryParse(token.AsSpan(0, separator), out timestamp))
        {
            error = "ERR Invalid stream ID specified as stream command argument";
            return false;
        }

        var sequenceToken = token[(separator + 1)..];
        if (sequenceToken == "*")
        {
            if (timestamp < lastTs)
            {
                error = "ERR The ID specified in XADD is equal or smaller than the target stream top item";
                return false;
            }

            sequence = timestamp == lastTs ? checked(lastSeq + 1UL) : 0;
            if (timestamp == 0 && sequence == 0)
            {
                sequence = 1;
            }

            id = $"{timestamp}-{sequence}";
            return true;
        }

        if (!ulong.TryParse(sequenceToken, out sequence))
        {
            error = "ERR Invalid stream ID specified as stream command argument";
            return false;
        }

        if (timestamp == 0 && sequence == 0)
        {
            error = "ERR The ID specified in XADD must be greater than 0-0";
            return false;
        }

        if (CompareStreamIds(timestamp, sequence, lastTs, lastSeq) <= 0)
        {
            error = "ERR The ID specified in XADD is equal or smaller than the target stream top item";
            return false;
        }

        id = token;
        return true;
    }

    private static bool TryParseRangeBound(string token, bool isStart, out ulong timestamp, out ulong sequence)
    {
        if (token == "-")
        {
            timestamp = 0;
            sequence = 0;
            return true;
        }

        if (token == "+")
        {
            timestamp = ulong.MaxValue;
            sequence = ulong.MaxValue;
            return true;
        }

        var separator = token.IndexOf('-');
        if (separator < 0)
        {
            if (!ulong.TryParse(token, out timestamp))
            {
                sequence = 0;
                return false;
            }

            sequence = isStart ? 0 : ulong.MaxValue;
            return true;
        }

        if (separator <= 0 || separator >= token.Length - 1 || !ulong.TryParse(token.AsSpan(0, separator), out timestamp))
        {
            timestamp = 0;
            sequence = 0;
            return false;
        }

        var seqToken = token[(separator + 1)..];
        if (seqToken == "*")
        {
            sequence = isStart ? 0 : ulong.MaxValue;
            return true;
        }

        return ulong.TryParse(seqToken, out sequence);
    }

    private static bool TryParseStreamId(string token, out ulong timestamp, out ulong sequence)
    {
        timestamp = 0;
        sequence = 0;
        var separator = token.IndexOf('-');
        if (separator <= 0 || separator >= token.Length - 1)
        {
            return false;
        }

        return ulong.TryParse(token.AsSpan(0, separator), out timestamp) &&
               ulong.TryParse(token.AsSpan(separator + 1), out sequence);
    }

    private static bool TryResolveGroupIdToken(string token, StreamData stream, out ulong timestamp, out ulong sequence, out string? id, out string? error)
    {
        timestamp = 0;
        sequence = 0;
        id = null;
        error = null;
        if (token == "$")
        {
            timestamp = stream.LastTimestamp;
            sequence = stream.LastSequence;
            id = $"{timestamp}-{sequence}";
            return true;
        }

        if (!TryParseStreamId(token, out timestamp, out sequence))
        {
            error = "ERR Invalid stream ID specified as stream command argument";
            return false;
        }

        id = token;
        return true;
    }

    private static Dictionary<string, ConsumerGroupState> CloneGroups(StreamData stream) =>
        stream.Groups is null
            ? new Dictionary<string, ConsumerGroupState>(StringComparer.Ordinal)
            : stream.Groups.ToDictionary(static pair => pair.Key, static pair => pair.Value, StringComparer.Ordinal);

    private static Dictionary<string, PendingEntryState> ClonePending(ConsumerGroupState group) =>
        group.Pending is null
            ? new Dictionary<string, PendingEntryState>(StringComparer.Ordinal)
            : group.Pending.ToDictionary(static pair => pair.Key, static pair => pair.Value, StringComparer.Ordinal);

    private static Dictionary<string, ConsumerState> CloneConsumers(ConsumerGroupState group) =>
        group.Consumers is null
            ? new Dictionary<string, ConsumerState>(StringComparer.Ordinal)
            : group.Consumers.ToDictionary(static pair => pair.Key, static pair => pair.Value, StringComparer.Ordinal);

    private static string NoGroupError(string key, string groupName) =>
        $"NOGROUP No such key '{key}' or consumer group '{groupName}'";

    private static string NoGroupErrorForReadGroup(string key, string groupName) =>
        $"NOGROUP No such key '{key}' or consumer group '{groupName}' in XREADGROUP with GROUP option";

    private static int CompareStreamIds(ulong leftTs, ulong leftSeq, ulong rightTs, ulong rightSeq)
    {
        var timestampComparison = leftTs.CompareTo(rightTs);
        return timestampComparison != 0 ? timestampComparison : leftSeq.CompareTo(rightSeq);
    }

    private static bool TryReadStream(byte[]? bytes, out StreamData? stream)
    {
        stream = null;
        if (bytes is null)
        {
            return true;
        }

        if (!IsSerializedStreamValue(bytes))
        {
            return false;
        }

        try
        {
            stream = JsonSerializer.Deserialize<StreamData>(bytes.AsSpan(StreamValueMagicPrefix.Length), StreamJsonOptions) ??
                new StreamData(Array.Empty<StreamEntry>(), 0, 0);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static bool IsSerializedStreamValue(byte[] bytes)
    {
        if (bytes.Length <= StreamValueMagicPrefix.Length)
        {
            return false;
        }

        return bytes.AsSpan(0, StreamValueMagicPrefix.Length).SequenceEqual(StreamValueMagicPrefix);
    }

    private static byte[] SerializeStream(StreamData stream)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(stream, StreamJsonOptions);
        var result = new byte[StreamValueMagicPrefix.Length + payload.Length];
        StreamValueMagicPrefix.CopyTo(result, 0);
        payload.CopyTo(result, StreamValueMagicPrefix.Length);
        return result;
    }

    private static bool TryGetAuthorizedAddress(RespValue request, RespValue response, out string? address)
    {
        address = null;
        if (response.Type == RespType.Error ||
            request.Type != RespType.Array ||
            request.Items is not { Count: 2 } items ||
            !items[0].AsString().Equals("AUTHADDR", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        address = EthereumAddress.Normalize(items[1].AsString());
        return true;
    }

    private static bool TryGetAuthorizedDidContext(RespValue request, RespValue response, out DidContext? context)
    {
        context = null;
        if (response.Type == RespType.Error ||
            request.Type != RespType.Array ||
            request.Items is null ||
            !request.Items[0].AsString().Equals("AUTHDID", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var items = request.Items;
        if (items.Count < 2)
        {
            return false;
        }

        var did = items[1].AsString();
        DidProof? proof = null;
        if (items.Count >= 4)
        {
            proof = new DidProof(items[2].AsString(), items[3].AsString());
        }

        context = new DidContext(did, proof);
        return true;
    }

    private async Task<RespValue> InfoAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count > 2)
        {
            return RespValue.Error("ERR wrong number of arguments for 'INFO'");
        }

        var section = args.Count == 2 ? args[1].AsString().ToLowerInvariant() : "default";
        var now = DateTimeOffset.UtcNow;
        var uptimeSeconds = Math.Max(1, (long)(DateTime.UtcNow - Process.GetCurrentProcess().StartTime.ToUniversalTime()).TotalSeconds);
        var connectedClients = _clientConnections.Count;
        var keys = await _store.ListKeysAsync(cancellationToken).ConfigureAwait(false);
        var usedMemory = await EstimateUsedMemoryBytesAsync(cancellationToken).ConfigureAwait(false);
        var withExpiry = 0L;
        foreach (var key in keys)
        {
            var (exists, ttl) = await _store.GetTtlAsync(key, cancellationToken).ConfigureAwait(false);
            if (exists && ttl is not null)
            {
                withExpiry++;
            }
        }

        var sections = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["server"] = $"# Server\r\nredis_version:7.0.0\r\nswarmkeydb_version:0.1.0\r\nuptime_in_seconds:{uptimeSeconds}\r\n",
            ["clients"] = $"# Clients\r\nconnected_clients:{connectedClients}\r\n",
            ["memory"] = $"# Memory\r\nused_memory:{usedMemory}\r\nmaxmemory:{_compatibilityOptions.MaxMemoryBytes}\r\nmaxmemory_policy:{_compatibilityOptions.MaxMemoryPolicy}\r\n",
            ["stats"] = $"# Stats\r\ntotal_commands_processed:{Interlocked.Read(ref _totalCommandsProcessed)}\r\nexpired_keys:{Interlocked.Read(ref _expiryKeysDeletedTotal)}\r\nevicted_keys:{Interlocked.Read(ref _evictionTotal)}\r\n",
            ["replication"] = "# Replication\r\nrole:master\r\nconnected_slaves:0\r\n",
            ["cpu"] = $"# CPU\r\nused_cpu_sys:0\r\nused_cpu_user:0\r\nused_cpu_sys_children:0\r\nused_cpu_user_children:0\r\n",
            ["keyspace"] = $"# Keyspace\r\ndb0:keys={keys.Count},expires={withExpiry},avg_ttl=0\r\n"
        };

        var selected = section switch
        {
            "default" or "all" => new[] { "server", "clients", "memory", "stats", "replication", "cpu", "keyspace" },
            _ => sections.ContainsKey(section) ? new[] { section } : Array.Empty<string>()
        };

        var payload = string.Concat(selected.Select(name => sections[name]));
        return RespValue.BulkString(payload);
    }

    private RespValue CommandCommand(IReadOnlyList<RespValue> args)
    {
        if (args.Count == 1)
        {
            var rows = CommandSpecs.Values
                .OrderBy(static spec => spec.Name, StringComparer.Ordinal)
                .Select(ToCommandSpecResp)
                .ToArray();
            return RespValue.Array(rows);
        }

        var sub = args[1].AsString().ToUpperInvariant();
        return sub switch
        {
            "COUNT" when args.Count == 2 => RespValue.IntegerValue(CommandSpecs.Count),
            "INFO" => RespValue.Array(args.Skip(2).Select(arg => CommandSpecs.TryGetValue(arg.AsString().ToUpperInvariant(), out var spec) ? ToCommandSpecResp(spec) : RespValue.BulkString((byte[]?)null)).ToArray()),
            "DOCS" => RespValue.Array(args.Skip(2).Select(arg => CommandSpecs.TryGetValue(arg.AsString().ToUpperInvariant(), out var spec) ? RespValue.Array(new[]
            {
                RespValue.BulkString("summary"),
                RespValue.BulkString($"SwarmKeyDb support for {spec.Name}."),
                RespValue.BulkString("since"),
                RespValue.BulkString("1.0")
            }) : RespValue.BulkString((byte[]?)null)).ToArray()),
            _ => RespValue.Error("ERR unknown subcommand or wrong number of arguments for 'COMMAND'")
        };
    }

    private static RespValue ToCommandSpecResp(CommandSpec spec) => RespValue.Array(new[]
    {
        RespValue.BulkString(spec.Name.ToLowerInvariant()),
        RespValue.IntegerValue(spec.Arity),
        RespValue.Array(spec.Flags.Select(RespValue.BulkString).ToArray()),
        RespValue.IntegerValue(1),
        RespValue.IntegerValue(1),
        RespValue.IntegerValue(1)
    });

    private RespValue ClientCommand(IReadOnlyList<RespValue> args)
    {
        if (args.Count < 2)
        {
            return RespValue.Error("ERR wrong number of arguments for 'CLIENT'");
        }

        var sub = args[1].AsString().ToUpperInvariant();
        var current = CurrentClientContext.Value;
        return sub switch
        {
            "ID" when args.Count == 2 => RespValue.IntegerValue(current?.ClientId ?? 0),
            "GETNAME" when args.Count == 2 => RespValue.BulkString(current is not null && _clientConnections.TryGetValue(current.ClientId, out var client) ? client.Name : null),
            "SETNAME" when args.Count == 3 => SetClientName(current, args[2].AsString()),
            "LIST" when args.Count == 2 => RespValue.BulkString(string.Join("\n", _clientConnections.Values.OrderBy(static c => c.Id).Select(c => $"id={c.Id} addr={c.Address} name={c.Name ?? ""} cmd={c.LastCommand}")) + "\n"),
            "INFO" when args.Count == 2 => RespValue.BulkString(BuildCurrentClientInfo(current)),
            _ => RespValue.Error("ERR unknown subcommand or wrong number of arguments for 'CLIENT'")
        };
    }

    private RespValue SetClientName(ClientContext? current, string name)
    {
        if (current is null)
        {
            return RespValue.Error("ERR CLIENT SETNAME is only available for connected clients");
        }

        if (string.IsNullOrWhiteSpace(name) || name.Contains(' ', StringComparison.Ordinal))
        {
            return RespValue.Error("ERR Client names cannot contain spaces, newlines or special characters.");
        }

        _clientConnections.AddOrUpdate(
            current.ClientId,
            _ => new ClientConnection(current.ClientId, "unknown") { Name = name },
            (_, state) => state with { Name = name });
        return RespValue.SimpleString("OK");
    }

    private string BuildCurrentClientInfo(ClientContext? current)
    {
        if (current is null || !_clientConnections.TryGetValue(current.ClientId, out var client))
        {
            return "id=0 addr=unknown name= cmd=unknown age=0 idle=0 flags=N db=0";
        }

        var age = Math.Max(0, (long)(DateTimeOffset.UtcNow - client.ConnectedAtUtc).TotalSeconds);
        var idle = Math.Max(0, (long)(DateTimeOffset.UtcNow - client.LastSeenUtc).TotalSeconds);
        return $"id={client.Id} addr={client.Address} name={client.Name ?? ""} age={age} idle={idle} flags=N db=0 cmd={client.LastCommand}";
    }

    /// <summary>
    /// Handles the HELLO command. Updates the per-connection protocol version and returns server info.
    /// Modelled on Redis 7.x HELLO semantics.
    /// </summary>
    private RespValue HandleHello(
        RespValue request,
        long clientId,
        ref int protocolVersion,
        ref string? currentAddress,
        RespWriter writer)
    {
        var args = request.Items ?? (IReadOnlyList<RespValue>)[];

        // Parse requested protocol version (arg 1, optional)
        int requestedProto = protocolVersion; // default: no change
        if (args.Count >= 2)
        {
            var protoArg = args[1].AsString();
            if (!int.TryParse(protoArg, out requestedProto) || requestedProto is not (2 or 3))
            {
                return RespValue.Error("NOPROTO unsupported protocol version");
            }
        }

        // AUTH sub-option: HELLO [proto] AUTH <username> <password>
        // We only support password-based auth; username is ignored (treated as default user).
        if (args.Count >= 5 && args[2].AsString().ToUpperInvariant() == "AUTH")
        {
            // args[3] = username (ignored), args[4] = password
            var password = args[4].AsString();
            if (!string.IsNullOrEmpty(_compatibilityOptions.RequirePass) &&
                password != _compatibilityOptions.RequirePass)
            {
                return RespValue.Error("WRONGPASS invalid username-password pair or user is disabled.");
            }
        }
        else if (args.Count >= 4 && args[2].AsString().ToUpperInvariant() == "AUTH")
        {
            // HELLO proto AUTH password (without username)
            var password = args[3].AsString();
            if (!string.IsNullOrEmpty(_compatibilityOptions.RequirePass) &&
                password != _compatibilityOptions.RequirePass)
            {
                return RespValue.Error("WRONGPASS invalid username-password pair or user is disabled.");
            }
        }

        // SETNAME sub-option
        // HELLO [proto] [AUTH …] SETNAME <name>
        var setnameIdx = -1;
        for (var i = 2; i < args.Count - 1; i++)
        {
            if (args[i].AsString().ToUpperInvariant() == "SETNAME")
            {
                setnameIdx = i;
                break;
            }
        }

        if (setnameIdx >= 0)
        {
            var nameArg = args[setnameIdx + 1].AsString();
            SetClientName(CurrentClientContext.Value, nameArg);
        }

        // Apply the new protocol version
        if (requestedProto != protocolVersion)
        {
            if (requestedProto == 3)
            {
                Interlocked.Increment(ref _resp3ConnectionsTotal);
                Interlocked.Increment(ref _activeResp3Connections);
            }
            else if (protocolVersion == 3)
            {
                Interlocked.Decrement(ref _activeResp3Connections);
            }

            protocolVersion = requestedProto;
            writer.ProtocolVersion = requestedProto;
        }

        return BuildHelloResponse(clientId, protocolVersion);
    }

    /// <summary>Builds the HELLO server-info response (Map in RESP3, flat array in RESP2).</summary>
    private static RespValue BuildHelloResponse(long clientId, int proto)
    {
        // Key-value pairs matching Redis 7.x HELLO response shape
        var pairs = new RespValue[]
        {
            RespValue.BulkString("server"),    RespValue.BulkString("swarmkeydb"),
            RespValue.BulkString("version"),   RespValue.BulkString("7.0.0"),
            RespValue.BulkString("proto"),     RespValue.IntegerValue(proto),
            RespValue.BulkString("id"),        RespValue.IntegerValue(clientId),
            RespValue.BulkString("mode"),      RespValue.BulkString("standalone"),
            RespValue.BulkString("role"),      RespValue.BulkString("master"),
            RespValue.BulkString("modules"),   RespValue.Array([])
        };
        return RespValue.Map(pairs);
    }

    /// <summary>
    /// Handles CLIENT TRACKING ON|OFF [REDIRECT client-id] [BCAST] [PREFIX prefix] [NOLOOP].
    /// Basic opt-in tracking that fires push invalidations when tracked keys are modified.
    /// </summary>
    private RespValue HandleClientTracking(RespValue request, long clientId, ChannelWriter<RespValue> pushWriter)
    {
        var args = request.Items ?? (IReadOnlyList<RespValue>)[];
        if (args.Count < 3)
        {
            return RespValue.Error("ERR wrong number of arguments for 'CLIENT|TRACKING' command");
        }

        var onOff = args[2].AsString().ToUpperInvariant();
        switch (onOff)
        {
            case "ON":
                _trackingConnections[clientId] = new TrackingRegistration(pushWriter);
                return RespValue.SimpleString("OK");

            case "OFF":
                _trackingConnections.TryRemove(clientId, out _);
                return RespValue.SimpleString("OK");

            default:
                return RespValue.Error("ERR syntax error in CLIENT TRACKING command");
        }
    }

    /// <summary>Returns a snapshot of RESP3 / client-tracking telemetry counters.</summary>
    public Resp3MetricsSnapshot GetResp3Metrics() => new(
        Interlocked.Read(ref _resp3ConnectionsTotal),
        Interlocked.Read(ref _activeResp3Connections),
        _trackingConnections.Count);

    private RespValue ConfigCommand(IReadOnlyList<RespValue> args)
    {
        if (args.Count < 2)
        {
            return RespValue.Error("ERR wrong number of arguments for 'CONFIG'");
        }

        var sub = args[1].AsString().ToUpperInvariant();
        return sub switch
        {
            "GET" when args.Count == 3 => ConfigGet(args[2].AsString()),
            "SET" when args.Count == 4 => ConfigSet(args[2].AsString(), args[3].AsString()),
            "REWRITE" when args.Count == 2 => RespValue.SimpleString("OK"),
            "RESETSTAT" when args.Count == 2 => ConfigResetStat(),
            _ => RespValue.Error("ERR wrong number of arguments for 'CONFIG' command")
        };
    }

    private RespValue ConfigGet(string pattern)
    {
        var values = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["maxmemory"] = _compatibilityOptions.MaxMemoryBytes.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["maxmemory-policy"] = _compatibilityOptions.MaxMemoryPolicy,
            ["hz"] = _compatibilityOptions.Hz.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["swarm-keydb-expiry-budget-ms"] = _compatibilityOptions.ExpiryBudgetMs.ToString(System.Globalization.CultureInfo.InvariantCulture)
        };

        var regex = GlobToRegex(pattern);
        var items = new List<RespValue>();
        foreach (var pair in values.Where(pair => regex.IsMatch(pair.Key)))
        {
            items.Add(RespValue.BulkString(pair.Key));
            items.Add(RespValue.BulkString(pair.Value));
        }

        return RespValue.Array(items);
    }

    private RespValue ConfigSet(string key, string value)
    {
        switch (key.ToLowerInvariant())
        {
            case "maxmemory":
                if (!TryParseMemoryBytes(value, out var maxMemory))
                {
                    return RespValue.Error("ERR Invalid argument 'maxmemory'");
                }

                _compatibilityOptions.MaxMemoryBytes = maxMemory;
                return RespValue.SimpleString("OK");

            case "maxmemory-policy":
                var normalized = value.ToLowerInvariant();
                if (!RedisCompatibilityOptions.AllowedMaxMemoryPolicies.Contains(normalized, StringComparer.Ordinal))
                {
                    return RespValue.Error("ERR Invalid argument 'maxmemory-policy'");
                }

                _compatibilityOptions.MaxMemoryPolicy = normalized;
                return RespValue.SimpleString("OK");

            case "hz":
                if (!int.TryParse(value, out var hz) || hz <= 0)
                {
                    return RespValue.Error("ERR Invalid argument 'hz'");
                }

                _compatibilityOptions.Hz = hz;
                return RespValue.SimpleString("OK");

            case "swarm-keydb-expiry-budget-ms":
                if (!int.TryParse(value, out var budget) || budget <= 0)
                {
                    return RespValue.Error("ERR Invalid argument 'swarm-keydb-expiry-budget-ms'");
                }

                _compatibilityOptions.ExpiryBudgetMs = budget;
                return RespValue.SimpleString("OK");

            default:
                return RespValue.Error("ERR Unsupported CONFIG parameter");
        }
    }

    private RespValue ConfigResetStat()
    {
        Interlocked.Exchange(ref _totalCommandsProcessed, 0);
        Interlocked.Exchange(ref _expiryKeysDeletedTotal, 0);
        Interlocked.Exchange(ref _expiryBudgetExceededTotal, 0);
        Interlocked.Exchange(ref _evictionTotal, 0);
        return RespValue.SimpleString("OK");
    }

    private async Task MaybeRunAdaptiveExpiryAsync(CancellationToken cancellationToken)
    {
        var nowUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var intervalMs = Math.Max(1, 1000 / Math.Max(1, _compatibilityOptions.Hz));
        var dueAt = Volatile.Read(ref _nextExpiryRunUnixMs);
        if (nowUnixMs < dueAt)
        {
            return;
        }

        Interlocked.Exchange(ref _nextExpiryRunUnixMs, nowUnixMs + intervalMs);
        var stopwatch = Stopwatch.StartNew();
        IReadOnlyList<string> keys;
        try
        {
            keys = await _store.ListKeysAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (AccessDeniedException)
        {
            return;
        }

        foreach (var key in keys)
        {
            (bool exists, TimeSpan? ttl) ttlInfo;
            try
            {
                ttlInfo = await _store.GetTtlAsync(key, cancellationToken).ConfigureAwait(false);
            }
            catch (AccessDeniedException)
            {
                return;
            }

            var (exists, ttl) = ttlInfo;
            if (!exists || ttl is null)
            {
                continue;
            }

            _keyExpiryHints[key] = DateTimeOffset.UtcNow.Add(ttl.Value);
            if (ttl <= TimeSpan.Zero && await _store.DeleteAsync(key, cancellationToken).ConfigureAwait(false))
            {
                _keySizes.TryRemove(key, out _);
                _keyLastAccessUnixMs.TryRemove(key, out _);
                _keyExpiryHints.TryRemove(key, out _);
                Interlocked.Increment(ref _expiryKeysDeletedTotal);
            }

            if (stopwatch.ElapsedMilliseconds >= _compatibilityOptions.ExpiryBudgetMs)
            {
                Interlocked.Increment(ref _expiryBudgetExceededTotal);
                break;
            }
        }

        stopwatch.Stop();
        Interlocked.Increment(ref _expiryScanCount);
        AddDouble(ref _expiryScanDurationSecondsSum, Math.Max(0D, stopwatch.Elapsed.TotalSeconds));
    }

    private async Task<long> EstimateUsedMemoryBytesAsync(CancellationToken cancellationToken)
    {
        var total = 0L;
        foreach (var key in await _store.ListKeysAsync(cancellationToken).ConfigureAwait(false))
        {
            var value = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
            if (value is null)
            {
                continue;
            }

            var size = key.Length + value.Length;
            _keySizes[key] = size;
            total += size;
        }

        return Math.Max(0, total);
    }

    private async Task<bool> EnsureWritableMemoryAsync(string key, int newValueLength, bool hasExpiry, CancellationToken cancellationToken)
    {
        if (_compatibilityOptions.MaxMemoryBytes <= 0)
        {
            return true;
        }

        var existingSize = _keySizes.TryGetValue(key, out var existing) ? existing : 0L;
        var targetSize = key.Length + Math.Max(0, newValueLength);
        var used = await EstimateUsedMemoryBytesAsync(cancellationToken).ConfigureAwait(false);
        if (used - existingSize + targetSize <= _compatibilityOptions.MaxMemoryBytes)
        {
            return true;
        }

        if (_compatibilityOptions.MaxMemoryPolicy.Equals("noeviction", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        while (used - existingSize + targetSize > _compatibilityOptions.MaxMemoryBytes)
        {
            var evictionCandidate = await SelectEvictionCandidateAsync(key, hasExpiry, cancellationToken).ConfigureAwait(false);
            if (evictionCandidate is null)
            {
                return false;
            }

            if (!await _store.DeleteAsync(evictionCandidate, cancellationToken).ConfigureAwait(false))
            {
                return false;
            }

            _keySizes.TryRemove(evictionCandidate, out _);
            _keyLastAccessUnixMs.TryRemove(evictionCandidate, out _);
            _keyExpiryHints.TryRemove(evictionCandidate, out _);
            Interlocked.Increment(ref _evictionTotal);
            used = await EstimateUsedMemoryBytesAsync(cancellationToken).ConfigureAwait(false);
        }

        return true;
    }

    private async Task<string?> SelectEvictionCandidateAsync(string incomingKey, bool incomingHasExpiry, CancellationToken cancellationToken)
    {
        var candidates = (await _store.ListKeysAsync(cancellationToken).ConfigureAwait(false))
            .Where(key => !key.Equals(incomingKey, StringComparison.Ordinal))
            .ToArray();
        if (candidates.Length == 0)
        {
            return null;
        }

        var policy = _compatibilityOptions.MaxMemoryPolicy.ToLowerInvariant();
        var volatileOnly = policy.StartsWith("volatile", StringComparison.Ordinal);
        var workingSet = volatileOnly
            ? candidates.Where(HasExpiryHint).ToArray()
            : candidates;
        if (workingSet.Length == 0)
        {
            return null;
        }

        return policy switch
        {
            "allkeys-random" or "volatile-random" => workingSet[_random.Next(workingSet.Length)],
            "allkeys-lru" or "volatile-lru" => workingSet.OrderBy(key => _keyLastAccessUnixMs.TryGetValue(key, out var value) ? value : 0L).FirstOrDefault(),
            "volatile-ttl" => workingSet.OrderBy(key => _keyExpiryHints.TryGetValue(key, out var expiry) && expiry is not null ? expiry.Value.ToUnixTimeMilliseconds() : long.MaxValue).FirstOrDefault(),
            _ => incomingHasExpiry ? null : workingSet[_random.Next(workingSet.Length)]
        };
    }

    private bool HasExpiryHint(string key) =>
        _keyExpiryHints.TryGetValue(key, out var expiry) && expiry is not null;

    private static bool TryParseMemoryBytes(string input, out long bytes)
    {
        bytes = 0;
        var trimmed = input.Trim();
        if (long.TryParse(trimmed, out bytes))
        {
            return bytes >= 0;
        }

        var suffixIndex = trimmed.TakeWhile(static c => char.IsDigit(c)).Count();
        if (suffixIndex == 0 || suffixIndex >= trimmed.Length)
        {
            return false;
        }

        var numberPart = trimmed[..suffixIndex];
        var suffix = trimmed[suffixIndex..].ToLowerInvariant();
        if (!long.TryParse(numberPart, out var value) || value < 0)
        {
            return false;
        }

        bytes = suffix switch
        {
            "kb" => value * 1024L,
            "mb" => value * 1024L * 1024L,
            "gb" => value * 1024L * 1024L * 1024L,
            _ => -1
        };
        return bytes >= 0;
    }

    private static TimeSpan? TryParseSetExpiryOption(IReadOnlyList<RespValue> args, out string? error)
    {
        error = null;
        if (args.Count == 3)
        {
            return null;
        }

        var option = args[3].AsString().ToUpperInvariant();
        if (!long.TryParse(args[4].AsString(), out var value))
        {
            error = "ERR value is not an integer or out of range";
            return null;
        }

        TimeSpan ttl;
        switch (option)
        {
            case "EX":
                if (!TryParseRelativeTtl(value, milliseconds: false, out ttl))
                {
                    error = "ERR value is not an integer or out of range";
                    return null;
                }
                break;
            case "PX":
                if (!TryParseRelativeTtl(value, milliseconds: true, out ttl))
                {
                    error = "ERR value is not an integer or out of range";
                    return null;
                }
                break;
            case "EXAT":
                try
                {
                    ttl = DateTimeOffset.FromUnixTimeSeconds(value) - DateTimeOffset.UtcNow;
                }
                catch (ArgumentOutOfRangeException)
                {
                    error = "ERR value is not an integer or out of range";
                    return null;
                }

                break;
            default:
                error = "ERR syntax error";
                return null;
        }

        if (ttl <= TimeSpan.Zero)
        {
            error = "ERR invalid expire time in 'set' command";
            return null;
        }

        return ttl;
    }

    private static bool TryParseRelativeTtl(long value, bool milliseconds, out TimeSpan ttl)
    {
        try
        {
            ttl = milliseconds ? TimeSpan.FromMilliseconds(value) : TimeSpan.FromSeconds(value);
            if (ttl > DateTimeOffset.MaxValue - DateTimeOffset.UtcNow)
            {
                ttl = default;
                return false;
            }

            return true;
        }
        catch (OverflowException)
        {
            ttl = default;
            return false;
        }
        catch (ArgumentOutOfRangeException)
        {
            ttl = default;
            return false;
        }
    }

    private async Task<(bool Updated, RespValue? Error)> TrySetTtlAsync(string key, TimeSpan ttl, CancellationToken cancellationToken)
    {
        try
        {
            return (await _store.SetTtlAsync(key, ttl, cancellationToken).ConfigureAwait(false), null);
        }
        catch (ArgumentException)
        {
            return (false, RespValue.Error("ERR value is not an integer or out of range"));
        }
        catch (OverflowException)
        {
            return (false, RespValue.Error("ERR value is not an integer or out of range"));
        }
    }

    private static Regex GlobToRegex(string pattern)
    {
        var escaped = Regex.Escape(pattern).Replace("\\*", ".*").Replace("\\?", ".");
        return new Regex("^" + escaped + "$", RegexOptions.CultureInvariant);
    }

    // ─── Scripting: EVAL / EVALSHA / SCRIPT ──────────────────────────────────────

    private async Task<RespValue> EvalAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        // EVAL script numkeys [key [key ...]] [arg [arg ...]]
        if (args.Count < 3)
        {
            return RespValue.Error($"ERR wrong number of arguments for '{args[0].AsString()}'");
        }

        var script = args[1].AsString();
        if (!int.TryParse(args[2].AsString(), out var numKeys) || numKeys < 0)
        {
            return RespValue.Error("ERR value is not an integer or out of range");
        }

        if (3 + numKeys > args.Count)
        {
            return RespValue.Error("ERR Number of keys can't be greater than number of args");
        }

        var keys = args.Skip(3).Take(numKeys).Select(static a => a.AsString()).ToList();
        var argv = args.Skip(3 + numKeys).Select(static a => a.AsString()).ToList();

        Interlocked.Increment(ref _scriptEvalTotal);

        // Cache the script so it can be retrieved by EVALSHA afterwards.
        if (_scriptCache.TryStore(script, out var sha1) && _scriptReplicationManager?.Enabled == true)
        {
            await _scriptReplicationManager.PublishLoadedScriptAsync(sha1, script, cancellationToken).ConfigureAwait(false);
        }

        return await RunScriptAsync(script, keys, argv, cancellationToken).ConfigureAwait(false);
    }

    private async Task<RespValue> EvalShaAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        // EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]
        if (args.Count < 3)
        {
            return RespValue.Error($"ERR wrong number of arguments for '{args[0].AsString()}'");
        }

        var sha1 = args[1].AsString().ToLowerInvariant();
        if (!int.TryParse(args[2].AsString(), out var numKeys) || numKeys < 0)
        {
            return RespValue.Error("ERR value is not an integer or out of range");
        }

        var script = _scriptCache.Get(sha1);
        if (script is null)
        {
            if (_scriptReplicationManager is not null)
            {
                _ = await _scriptReplicationManager.TryRecoverMissingScriptAsync(sha1, cancellationToken).ConfigureAwait(false);
                script = _scriptCache.Get(sha1);
            }

            if (script is null)
            {
                return _scriptReplicationManager?.Enabled == true
                    ? RespValue.Error("NOSCRIPT No matching script. Please use EVAL.")
                    : RespValue.Error("NOSCRIPT No matching script. Please use EVAL. Script replication is not enabled.");
            }
        }

        if (3 + numKeys > args.Count)
        {
            return RespValue.Error("ERR Number of keys can't be greater than number of args");
        }

        var keys = args.Skip(3).Take(numKeys).Select(static a => a.AsString()).ToList();
        var argv = args.Skip(3 + numKeys).Select(static a => a.AsString()).ToList();

        Interlocked.Increment(ref _scriptEvalShaTotal);

        return await RunScriptAsync(script, keys, argv, cancellationToken).ConfigureAwait(false);
    }

    private async Task<RespValue> ScriptCommandAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        if (args.Count < 2)
        {
            return RespValue.Error("ERR wrong number of arguments for 'SCRIPT'");
        }

        var subCommand = args[1].AsString().ToUpperInvariant();

        switch (subCommand)
        {
            case "LOAD":
            {
                if (args.Count != 3)
                {
                    return RespValue.Error("ERR wrong number of arguments for 'SCRIPT|LOAD'");
                }

                var source = args[2].AsString();
                _ = _scriptCache.TryStore(source, out var sha1);
                if (_scriptReplicationManager?.Enabled == true)
                {
                    await _scriptReplicationManager.PublishLoadedScriptAsync(sha1, source, cancellationToken).ConfigureAwait(false);
                }

                return RespValue.BulkString(sha1);
            }

            case "EXISTS":
            {
                if (args.Count < 3)
                {
                    return RespValue.Error("ERR wrong number of arguments for 'SCRIPT|EXISTS'");
                }

                var results = new List<RespValue>(args.Count - 2);
                for (var i = 2; i < args.Count; i++)
                {
                    results.Add(RespValue.IntegerValue(_scriptCache.Exists(args[i].AsString().ToLowerInvariant()) ? 1 : 0));
                }

                return RespValue.Array(results);
            }

            case "FLUSH":
            {
                // SCRIPT FLUSH [ASYNC|SYNC] — mode is ignored (single-node cache)
                _scriptCache.Flush();
                if (_scriptReplicationManager?.Enabled == true)
                {
                    await _scriptReplicationManager.PublishFlushAsync(cancellationToken).ConfigureAwait(false);
                }

                return RespValue.SimpleString("OK");
            }

            case "KILL":
            {
                // SCRIPT KILL terminates a running non-write script.
                // In this implementation scripts run on Tasks; we cannot kill them mid-flight.
                // Return NOTBUSY to match Redis when no script is currently running.
                return RespValue.Error("NOTBUSY No scripts in execution right now.");
            }

            default:
                return RespValue.Error($"ERR unknown subcommand '{subCommand}' for 'SCRIPT'");
        }
    }

    private async Task<RespValue> RunScriptAsync(
        string scriptSource,
        IReadOnlyList<string> keys,
        IReadOnlyList<string> argv,
        CancellationToken cancellationToken)
    {
        var sw = Stopwatch.StartNew();
        var result = await _scriptEngine.ExecuteAsync(
            scriptSource,
            keys,
            argv,
            async (cmd, cmdArgs) =>
            {
                // Build a synthetic RESP request and dispatch through ExecuteAsync.
                var items = new List<RespValue>(1 + cmdArgs.Count) { RespValue.BulkString(cmd) };
                items.AddRange(cmdArgs.Select(static a => RespValue.BulkString(a)));
                return await ExecuteAsync(RespValue.Array(items), cancellationToken).ConfigureAwait(false);
            }).ConfigureAwait(false);

        sw.Stop();

        if (result.Type == RespType.Error)
        {
            Interlocked.Increment(ref _scriptErrorTotal);
            if (result.Text?.StartsWith("BUSY", StringComparison.Ordinal) == true)
            {
                Interlocked.Increment(ref _scriptTimeoutTotal);
            }
        }

        RecordScriptDuration(sw.Elapsed);
        return result;
    }

    private void RecordScriptDuration(TimeSpan elapsed)
    {
        var seconds = elapsed.TotalSeconds;
        lock (_scriptExecDurationBucketCounts)
        {
            Interlocked.Increment(ref _scriptExecDurationCount);
            var currentSum = Volatile.Read(ref _scriptExecDurationSumSeconds);
            Volatile.Write(ref _scriptExecDurationSumSeconds, currentSum + seconds);
            for (var i = 0; i < ScriptExecDurationBucketUpperBounds.Length; i++)
            {
                if (seconds <= ScriptExecDurationBucketUpperBounds[i])
                {
                    Interlocked.Increment(ref _scriptExecDurationBucketCounts[i]);
                }
            }
        }
    }

    /// <summary>Returns a snapshot of scripting telemetry counters.</summary>
    public ScriptMetricsSnapshot GetScriptMetrics()
    {
        long[] bucketSnapshot;
        long durationCount;
        double durationSum;
        lock (_scriptExecDurationBucketCounts)
        {
            bucketSnapshot = (long[])_scriptExecDurationBucketCounts.Clone();
            durationCount = Interlocked.Read(ref _scriptExecDurationCount);
            durationSum = Volatile.Read(ref _scriptExecDurationSumSeconds);
        }

        var replicationMetrics = _scriptReplicationManager?.GetMetricsSnapshot() ?? default;
        return new ScriptMetricsSnapshot(
            Interlocked.Read(ref _scriptEvalTotal),
            Interlocked.Read(ref _scriptEvalShaTotal),
            Interlocked.Read(ref _scriptErrorTotal),
            Interlocked.Read(ref _scriptTimeoutTotal),
            replicationMetrics.SentTotal,
            replicationMetrics.ReceivedTotal,
            replicationMetrics.CacheMissRecoveredTotal,
            replicationMetrics.FlushPropagatedTotal,
            replicationMetrics.CacheSize,
            new ScriptDurationHistogramSnapshot(
                ScriptExecDurationBucketUpperBounds,
                bucketSnapshot,
                durationCount,
                durationSum));
    }

    public CompatibilityMetricsSnapshot GetCompatibilityMetrics() => new(
        ExpiryScanDurationSeconds: Interlocked.Read(ref _expiryScanCount) == 0
            ? 0D
            : Volatile.Read(ref _expiryScanDurationSecondsSum) / Math.Max(1, Interlocked.Read(ref _expiryScanCount)),
        ExpiryKeysDeletedTotal: Interlocked.Read(ref _expiryKeysDeletedTotal),
        ExpiryBudgetExceededTotal: Interlocked.Read(ref _expiryBudgetExceededTotal),
        MemoryUsedBytes: _keySizes.Sum(static pair => pair.Value),
        MemoryLimitBytes: _compatibilityOptions.MaxMemoryBytes,
        EvictionTotal: Interlocked.Read(ref _evictionTotal));

    private RespValue PublishCommand(IReadOnlyList<RespValue> args)
    {
        var arityError = RequireArity(args, 3);
        if (arityError is not null)
        {
            return arityError;
        }

        if (_pubSubManager is null)
        {
            return RespValue.IntegerValue(0);
        }

        var channel = args[1].AsString();
        var message = args[2].Bytes ?? Array.Empty<byte>();
        var count = _pubSubManager.Publish(channel, message);
        _logger.LogDebug("PUBLISH {Channel}: delivered to {Count} subscriber(s).", channel, count);
        return RespValue.IntegerValue(count);
    }

    private RespValue PubSubCommand(IReadOnlyList<RespValue> args)
    {
        if (args.Count < 2)
        {
            return RespValue.Error("ERR wrong number of arguments for 'PUBSUB'");
        }

        var subCommand = args[1].AsString().ToUpperInvariant();

        if (_pubSubManager is null)
        {
            return subCommand switch
            {
                "CHANNELS" => RespValue.Array(Array.Empty<RespValue>()),
                "NUMSUB" => RespValue.Array(Array.Empty<RespValue>()),
                "NUMPAT" => RespValue.IntegerValue(0),
                _ => RespValue.Error($"ERR unknown subcommand or wrong number of arguments for '{subCommand}' command")
            };
        }

        switch (subCommand)
        {
            case "CHANNELS":
            {
                var pattern = args.Count >= 3 ? args[2].AsString() : null;
                var channels = _pubSubManager.GetChannels(pattern);
                return RespValue.Array(channels.Select(static c => RespValue.BulkString(c)).ToArray());
            }

            case "NUMSUB":
            {
                var channelNames = args.Skip(2).Select(static a => a.AsString()).ToArray();
                var counts = _pubSubManager.GetNumSub(channelNames);
                var items = new List<RespValue>(channelNames.Length * 2);
                foreach (var ch in channelNames)
                {
                    items.Add(RespValue.BulkString(ch));
                    items.Add(RespValue.IntegerValue(counts.TryGetValue(ch, out var c) ? c : 0));
                }

                return RespValue.Array(items);
            }

            case "NUMPAT":
                return RespValue.IntegerValue(_pubSubManager.GetNumPat());

            default:
                return RespValue.Error($"ERR unknown subcommand or wrong number of arguments for '{subCommand}' command");
        }
    }

    public void Dispose()
    {
        _mutationGate.Dispose();
    }

    private void RecordCommand(string command, RespValue response, TimeSpan elapsed, string correlationId)
    {
        var operation = MapCommandToOperation(command);
        var succeeded = response.Type != RespType.Error;
        var errorType = succeeded ? null : ClassifyError(response.Text);
        _observer?.OnCommandCompleted(command, operation, succeeded, errorType, elapsed, correlationId);
    }

    private static string MapCommandToOperation(string command) => command switch
    {
        "GET" => "get",
        "SET" or "SETEX" or "PSETEX" or "INCR" or "INCRBY" or "DECR" or "DECRBY" => "put",
        "DEL" => "delete",
        "KEYS" or "SCAN" => "list",
        "MGET" or "MSET" or "MSETNX" or "MDEL" => "batch",
        "XADD" or "XTRIM" or "XRANGE" or "XREVRANGE" or "XLEN" or "XREAD" or "XGROUP" or "XREADGROUP" or "XACK" or "XPENDING" or "XCLAIM" or "XAUTOCLAIM" => "stream",
        "PUBLISH" => "pubsub",
        "SUBSCRIBE" or "UNSUBSCRIBE" or "PSUBSCRIBE" or "PUNSUBSCRIBE" or "PUBSUB" => "pubsub",
        "MULTI" or "EXEC" or "DISCARD" or "WATCH" or "UNWATCH" => "transaction",
        "INFO" or "COMMAND" or "CLIENT" or "CONFIG" => "compatibility",
        "EVAL" or "EVALSHA" or "SCRIPT" => "script",
        _ => "other"
    };

    private static IReadOnlyDictionary<string, CommandSpec> CreateCommandSpecs()
    {
        var specs = new[]
        {
            new CommandSpec("PING", -1, ["fast"]),
            new CommandSpec("ECHO", 2, ["fast"]),
            new CommandSpec("SET", -3, ["write"]),
            new CommandSpec("SETEX", 4, ["write"]),
            new CommandSpec("PSETEX", 4, ["write"]),
            new CommandSpec("GET", 2, ["readonly", "fast"]),
            new CommandSpec("INCR", 2, ["write", "fast"]),
            new CommandSpec("INCRBY", 3, ["write", "fast"]),
            new CommandSpec("DECR", 2, ["write", "fast"]),
            new CommandSpec("DECRBY", 3, ["write", "fast"]),
            new CommandSpec("DEL", -2, ["write"]),
            new CommandSpec("MGET", -2, ["readonly"]),
            new CommandSpec("MSET", -3, ["write"]),
            new CommandSpec("MSETNX", -3, ["write"]),
            new CommandSpec("EXISTS", -2, ["readonly"]),
            new CommandSpec("EXPIRE", 3, ["write"]),
            new CommandSpec("PEXPIRE", 3, ["write"]),
            new CommandSpec("EXPIREAT", 3, ["write"]),
            new CommandSpec("TTL", 2, ["readonly"]),
            new CommandSpec("PTTL", 2, ["readonly"]),
            new CommandSpec("PERSIST", 2, ["write"]),
            new CommandSpec("TYPE", 2, ["readonly"]),
            new CommandSpec("KEYS", 2, ["readonly"]),
            new CommandSpec("SCAN", -2, ["readonly"]),
            new CommandSpec("INFO", -1, ["readonly"]),
            new CommandSpec("COMMAND", -1, ["readonly"]),
            new CommandSpec("CLIENT", -2, ["readonly"]),
            new CommandSpec("CONFIG", -2, ["admin"]),
            new CommandSpec("BACKUP", 1, ["admin"]),
            new CommandSpec("RESTOREDB", -2, ["admin"]),
            new CommandSpec("ROTATEKEY", 3, ["admin"]),
            new CommandSpec("BACKENDMETA", 2, ["readonly"]),
            new CommandSpec("SWARM.RESYNC", -1, ["admin"]),
            new CommandSpec("PUBLISH", 3, ["pubsub"]),
            new CommandSpec("PUBSUB", -2, ["pubsub"]),
            new CommandSpec("SUBSCRIBE", -2, ["pubsub"]),
            new CommandSpec("UNSUBSCRIBE", -1, ["pubsub"]),
            new CommandSpec("PSUBSCRIBE", -2, ["pubsub"]),
            new CommandSpec("PUNSUBSCRIBE", -1, ["pubsub"]),
            new CommandSpec("MULTI", 1, ["transaction"]),
            new CommandSpec("EXEC", 1, ["transaction"]),
            new CommandSpec("DISCARD", 1, ["transaction"]),
            new CommandSpec("WATCH", -2, ["transaction"]),
            new CommandSpec("UNWATCH", 1, ["transaction"]),
            new CommandSpec("EVAL", -3, ["scripting"]),
            new CommandSpec("EVALSHA", -3, ["scripting"]),
            new CommandSpec("SCRIPT", -2, ["scripting"])
        };
        return specs.ToDictionary(static spec => spec.Name, StringComparer.OrdinalIgnoreCase);
    }

    private static string ResolveRemoteEndpoint(Stream stream) =>
        stream is NetworkStream ? "tcp" : "unknown:0";

    private static string ClassifyError(string? errorText)
    {
        if (string.IsNullOrWhiteSpace(errorText))
        {
            return "unknown";
        }

        var normalized = errorText.StartsWith("ERR ", StringComparison.OrdinalIgnoreCase)
            ? errorText[4..]
            : errorText;
        var token = normalized.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).FirstOrDefault();
        return string.IsNullOrWhiteSpace(token) ? "unknown" : token.ToLowerInvariant();
    }

    private sealed record CommandSpec(string Name, int Arity, IReadOnlyList<string> Flags);
    private sealed record ClientContext(long ClientId);
    private sealed record ClientConnection(
        long Id,
        string Address,
        string? Name = null,
        string LastCommand = "none",
        DateTimeOffset ConnectedAtUtc = default,
        DateTimeOffset LastSeenUtc = default)
    {
        public ClientConnection(long id, string address) : this(
            id,
            address,
            Name: null,
            LastCommand: "none",
            ConnectedAtUtc: DateTimeOffset.UtcNow,
            LastSeenUtc: DateTimeOffset.UtcNow)
        {
        }
    }

    /// <summary>Holds the push channel writer for a connection that has activated CLIENT TRACKING.</summary>
    private sealed record TrackingRegistration(ChannelWriter<RespValue> PushWriter);
}

/// <summary>Snapshot of per-processor transaction telemetry counters.</summary>
public sealed record TransactionMetricsSnapshot(
    long StartedTotal,
    long CommittedTotal,
    long AbortedTotal,
    long WatchConflictTotal,
    TransactionHistogramSnapshot QueueDepth,
    TransactionHistogramSnapshot ExecDuration);

/// <summary>Histogram snapshot used for transaction telemetry.</summary>
public sealed record TransactionHistogramSnapshot(
    IReadOnlyList<double> BucketUpperBounds,
    IReadOnlyList<long> BucketCounts,
    long Count,
    double Sum);

public sealed record StreamMetricsSnapshot(
    long PendingEntriesTotal,
    long XAckTotal,
    long XClaimTotal,
    long GroupCount,
    long IdleConsumerCount,
    long BlockedReaders,
    long XReadWakeupTotal,
    IReadOnlyDictionary<string, long> BlockedReadersByStream,
    long TrimmedTotal,
    long StreamLengthBytesTotal,
    IReadOnlyDictionary<string, long> StreamLengthBytesByStream);

/// <summary>Snapshot of scripting telemetry counters.</summary>
public sealed record ScriptMetricsSnapshot(
    long EvalTotal,
    long EvalShaTotal,
    long ErrorTotal,
    long TimeoutTotal,
    long ReplicationSentTotal,
    long ReplicationReceivedTotal,
    long CacheMissRecoveredTotal,
    long FlushPropagatedTotal,
    int CacheSize,
    ScriptDurationHistogramSnapshot ExecDuration);

/// <summary>Histogram snapshot used for script execution duration telemetry.</summary>
public sealed record ScriptDurationHistogramSnapshot(
    IReadOnlyList<double> BucketUpperBounds,
    IReadOnlyList<long> BucketCounts,
    long Count,
    double Sum);

public sealed record CompatibilityMetricsSnapshot(
    double ExpiryScanDurationSeconds,
    long ExpiryKeysDeletedTotal,
    long ExpiryBudgetExceededTotal,
    long MemoryUsedBytes,
    long MemoryLimitBytes,
    long EvictionTotal);

/// <summary>Snapshot of RESP3 protocol and client-tracking telemetry counters.</summary>
public sealed record Resp3MetricsSnapshot(
    long Resp3ConnectionsTotal,
    long ActiveResp3Connections,
    long ClientTrackingConnections);
