using System.Collections.Concurrent;
using System.Diagnostics;
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
    private readonly SemaphoreSlim _mutationGate = new(1, 1);

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
    private readonly object _streamWaitersGate = new();
    private readonly Dictionary<string, HashSet<StreamReadWaiter>> _streamReadWaiters = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Dictionary<string, Queue<StreamReadWaiter>>> _streamReadGroupWaiters = new(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _streamBlockedReadersByStream = new(StringComparer.Ordinal);

    public static readonly double[] TransactionQueueDepthBucketUpperBounds = [0, 1, 2, 4, 8, 16, 32];
    public static readonly double[] TransactionExecDurationBucketUpperBounds = [0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5];

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
        PubSubManager? pubSubManager = null)
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
    }

    public async Task ProcessAsync(Stream input, Stream output, CancellationToken cancellationToken = default)
    {
        var reader = new RespReader(input);
        var writer = new RespWriter(output);
        string? currentAddress = null;
        DidContext? currentDidContext = null;

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
                }

                if (request is null)
                {
                    break;
                }

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

                // Handle pub/sub commands that require direct access to the writer and connection state
                if (_pubSubManager is not null && command is "SUBSCRIBE" or "UNSUBSCRIBE" or "PSUBSCRIBE" or "PUNSUBSCRIBE")
                {
                    await HandlePubSubCommandAsync(command, request, connectionId, pushChannel.Writer, channelSubs, patternSubs, writer, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                // In subscription mode only PING, RESET, and QUIT are permitted
                if (isSubscribed && command is not ("PING" or "RESET" or "QUIT"))
                {
                    await writer.WriteAsync(
                        RespValue.Error($"ERR Can't call '{command.ToLowerInvariant()}' in subscribe mode"),
                        cancellationToken).ConfigureAwait(false);
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
                    BumpMutatedKeys(command, mutatedItems);
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
        "SET", "SETEX", "PSETEX", "GET",
        "DEL", "MDEL", "MGET", "MSET", "MSETNX",
        "EXISTS", "EXPIRE", "PEXPIRE", "EXPIREAT",
        "TTL", "PTTL", "PERSIST",
        "XADD", "XRANGE", "XREVRANGE", "XLEN", "XREAD",
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
    private void BumpMutatedKeys(string command, IReadOnlyList<RespValue> args)
    {
        switch (command)
        {
            case "SET":
            case "SETEX":
            case "PSETEX":
            case "EXPIRE":
            case "PEXPIRE":
            case "EXPIREAT":
            case "PERSIST":
                if (args.Count >= 2)
                {
                    NotifyKeyMutated(args[1].AsString());
                }
                break;

            case "DEL":
            case "MDEL":
                for (var i = 1; i < args.Count; i++)
                {
                    NotifyKeyMutated(args[i].AsString());
                }
                break;

            case "MSET":
            case "MSETNX":
                for (var i = 1; i < args.Count - 1; i += 2)
                {
                    NotifyKeyMutated(args[i].AsString());
                }
                break;

            case "XADD":
            case "XGROUP":
            case "XREADGROUP":
            case "XACK":
            case "XCLAIM":
            case "XAUTOCLAIM":
                if (args.Count >= 2)
                {
                    NotifyKeyMutated(args[1].AsString());
                }
                break;
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

        foreach (var key in _store.ListKeysAsync().GetAwaiter().GetResult())
        {
            var bytes = _store.GetAsync(key).GetAwaiter().GetResult();
            if (!TryReadStream(bytes, out var stream) || stream is null || stream.Groups is null)
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
            blockedReadersByStream);
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
        var deadlineUtc = blockMilliseconds is > 0
            ? DateTime.UtcNow.AddMilliseconds(blockMilliseconds.Value)
            : (DateTime?)null;
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
            response = command switch
            {
                "PING" => args.Count > 1 ? RespValue.BulkString(args[1].Bytes) : RespValue.SimpleString("PONG"),
                "ECHO" => RequireArity(args, 2) ?? RespValue.BulkString(args[1].Bytes),
                "AUTHADDR" => SetCallerAddress(args),
                "AUTHDID" => await SetDidContextAsync(args, cancellationToken).ConfigureAwait(false),
                "SET" => await SetAsync(args, cancellationToken).ConfigureAwait(false),
                "SETEX" => await SetExAsync(args, milliseconds: false, cancellationToken).ConfigureAwait(false),
                "PSETEX" => await SetExAsync(args, milliseconds: true, cancellationToken).ConfigureAwait(false),
                "GET" => await GetAsync(args, cancellationToken).ConfigureAwait(false),
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

        await _store.PutAsync(args[1].AsString(), args[2].Bytes ?? Array.Empty<byte>(), cancellationToken).ConfigureAwait(false);
        if (ttl is { } expiry)
        {
            var (updated, ttlError) = await TrySetTtlAsync(args[1].AsString(), expiry, cancellationToken).ConfigureAwait(false);
            if (ttlError is not null)
            {
                return ttlError;
            }
        }

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

        await _store.PutAsync(args[1].AsString(), args[3].Bytes ?? Array.Empty<byte>(), cancellationToken).ConfigureAwait(false);
        var (_, ttlError) = await TrySetTtlAsync(args[1].AsString(), ttl, cancellationToken).ConfigureAwait(false);
        if (ttlError is not null)
        {
            return ttlError;
        }

        return RespValue.SimpleString("OK");
    }

    private async Task<RespValue> GetAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 2);
        if (arityError is not null)
        {
            return arityError;
        }

        return RespValue.BulkString(await _store.GetAsync(args[1].AsString(), cancellationToken).ConfigureAwait(false));
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
                await _store.PutAsync(args[i].AsString(), args[i + 1].Bytes ?? Array.Empty<byte>(), cancellationToken).ConfigureAwait(false);
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
                await _store.PutAsync(args[i].AsString(), args[i + 1].Bytes ?? Array.Empty<byte>(), cancellationToken).ConfigureAwait(false);
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
                return RespValue.IntegerValue(await _store.DeleteAsync(args[1].AsString(), cancellationToken).ConfigureAwait(false) ? 1 : 0);
            }

            var (absoluteUpdated, ttlError) = await TrySetTtlAsync(args[1].AsString(), ttl, cancellationToken).ConfigureAwait(false);
            if (ttlError is not null)
            {
                return ttlError;
            }

            return RespValue.IntegerValue(absoluteUpdated ? 1 : 0);
        }

        if (ttlValue <= 0)
        {
            return RespValue.IntegerValue(await _store.DeleteAsync(args[1].AsString(), cancellationToken).ConfigureAwait(false) ? 1 : 0);
        }

        if (!TryParseRelativeTtl(ttlValue, milliseconds, out var relativeTtl))
        {
            return RespValue.Error("ERR value is not an integer or out of range");
        }

        var (updated, relativeTtlError) = await TrySetTtlAsync(args[1].AsString(), relativeTtl, cancellationToken).ConfigureAwait(false);
        if (relativeTtlError is not null)
        {
            return relativeTtlError;
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

        return RespValue.IntegerValue(await _store.RemoveTtlAsync(args[1].AsString(), cancellationToken).ConfigureAwait(false) ? 1 : 0);
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
        long? maxLen = null;
        var approximate = false;
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

            if (maxLen is { } trimTo && entries.Count > trimTo)
            {
                var removeCount = entries.Count - (int)trimTo;
                if (approximate && removeCount == 1 && entries.Count > trimTo + 32)
                {
                    removeCount = entries.Count - (int)trimTo;
                }

                entries.RemoveRange(0, removeCount);
            }

            var updated = new StreamData(entries, timestamp, sequence, stream.Groups);
            await _store.PutAsync(key, SerializeStream(updated), cancellationToken).ConfigureAwait(false);
            NotifyStreamWaiters(key);
            return RespValue.BulkString(id);
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
                    var remaining = deadlineUtc!.Value - DateTime.UtcNow;
                    if (remaining <= TimeSpan.Zero)
                    {
                        return RespValue.NullArray();
                    }

                    using var timeoutCts = new CancellationTokenSource(remaining);
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
                    var remaining = deadlineUtc!.Value - DateTime.UtcNow;
                    if (remaining <= TimeSpan.Zero)
                    {
                        return RespValue.NullArray();
                    }

                    using var timeoutCts = new CancellationTokenSource(remaining);
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
        "SET" or "SETEX" or "PSETEX" => "put",
        "DEL" => "delete",
        "KEYS" or "SCAN" => "list",
        "MGET" or "MSET" or "MSETNX" or "MDEL" => "batch",
        "XADD" or "XRANGE" or "XREVRANGE" or "XLEN" or "XREAD" or "XGROUP" or "XREADGROUP" or "XACK" or "XPENDING" or "XCLAIM" or "XAUTOCLAIM" => "stream",
        "PUBLISH" => "pubsub",
        "SUBSCRIBE" or "UNSUBSCRIBE" or "PSUBSCRIBE" or "PUNSUBSCRIBE" or "PUBSUB" => "pubsub",
        "MULTI" or "EXEC" or "DISCARD" or "WATCH" or "UNWATCH" => "transaction",
        _ => "other"
    };

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
    IReadOnlyDictionary<string, long> BlockedReadersByStream);
