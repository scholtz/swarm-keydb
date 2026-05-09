using System.Diagnostics;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace SwarmKeyDb;

public sealed class RedisCommandProcessor : IDisposable
{
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

                var response = await ExecuteAsync(request, cancellationToken).ConfigureAwait(false);
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

    private async Task<RespValue> TypeAsync(IReadOnlyList<RespValue> args, CancellationToken cancellationToken)
    {
        var arityError = RequireArity(args, 2);
        if (arityError is not null)
        {
            return arityError;
        }

        return RespValue.SimpleString(await _store.GetAsync(args[1].AsString(), cancellationToken).ConfigureAwait(false) is null ? "none" : "string");
    }

    private static RespValue? RequireArity(IReadOnlyList<RespValue> args, int expected) =>
        args.Count == expected ? null : RespValue.Error($"ERR wrong number of arguments for '{args[0].AsString()}'");

    private static bool IsQuit(RespValue request) =>
        request.Type == RespType.Array && request.Items is { Count: > 0 } && request.Items[0].AsString().Equals("QUIT", StringComparison.OrdinalIgnoreCase);

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
        "PUBLISH" => "pubsub",
        "SUBSCRIBE" or "UNSUBSCRIBE" or "PSUBSCRIBE" or "PUNSUBSCRIBE" or "PUBSUB" => "pubsub",
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
