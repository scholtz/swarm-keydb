using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace SwarmKeyDb;

/// <summary>
/// Thread-safe Pub/Sub manager that tracks per-connection channel and pattern subscriptions,
/// routes published messages to matching subscribers, and propagates messages across nodes
/// via the <see cref="ICacheSyncBus"/>.
/// </summary>
public sealed class PubSubManager : IDisposable
{
    private const string CrossNodeEventReason = "__pubsub__:";

    private sealed record Subscription(string ConnectionId, ChannelWriter<RespValue> Writer);

    private readonly object _gate = new();
    private readonly Dictionary<string, List<Subscription>> _channels = new(StringComparer.Ordinal);
    private readonly Dictionary<string, List<Subscription>> _patterns = new(StringComparer.Ordinal);

    // Per-connection tracking for efficient cleanup and count queries
    private readonly Dictionary<string, HashSet<string>> _connectionChannels = new(StringComparer.Ordinal);
    private readonly Dictionary<string, HashSet<string>> _connectionPatterns = new(StringComparer.Ordinal);

    private readonly ICacheSyncBus? _syncBus;
    private readonly string _nodeId;
    private readonly ILogger<PubSubManager> _logger;
    private IDisposable? _syncBusSubscription;

    private long _messagesPublishedTotal;
    private long _messagesDroppedTotal;

    public long MessagesPublishedTotal => Interlocked.Read(ref _messagesPublishedTotal);
    public long MessagesDroppedTotal => Interlocked.Read(ref _messagesDroppedTotal);

    public PubSubManager(
        ICacheSyncBus? syncBus = null,
        string? nodeId = null,
        ILogger<PubSubManager>? logger = null)
    {
        _syncBus = syncBus;
        _nodeId = nodeId ?? Guid.NewGuid().ToString("N");
        _logger = logger ?? NullLogger<PubSubManager>.Instance;

        if (_syncBus is not null)
        {
            _syncBusSubscription = _syncBus.SubscribeInvalidations(OnCacheSyncEventAsync);
        }
    }

    // ---------------------------------------------------------------------------
    // Channel subscriptions
    // ---------------------------------------------------------------------------

    /// <summary>Subscribes a connection to a channel. Returns the connection's total subscription count.</summary>
    public int Subscribe(string connectionId, string channel, ChannelWriter<RespValue> writer)
    {
        ArgumentNullException.ThrowIfNull(connectionId);
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentNullException.ThrowIfNull(writer);

        lock (_gate)
        {
            if (!_channels.TryGetValue(channel, out var subs))
            {
                subs = new List<Subscription>();
                _channels[channel] = subs;
            }

            if (!subs.Any(s => s.ConnectionId == connectionId))
            {
                subs.Add(new Subscription(connectionId, writer));
            }

            if (!_connectionChannels.TryGetValue(connectionId, out var channelSet))
            {
                channelSet = new HashSet<string>(StringComparer.Ordinal);
                _connectionChannels[connectionId] = channelSet;
            }

            channelSet.Add(channel);

            var channelCount = _connectionChannels.TryGetValue(connectionId, out var cc) ? cc.Count : 0;
            var patternCount = _connectionPatterns.TryGetValue(connectionId, out var pc) ? pc.Count : 0;
            _logger.LogDebug("Connection {ConnectionId} subscribed to channel {Channel}. Total subscriptions: {Count}.", connectionId, channel, channelCount + patternCount);
            return channelCount + patternCount;
        }
    }

    /// <summary>Unsubscribes a connection from a channel. Returns the connection's remaining total subscription count.</summary>
    public int Unsubscribe(string connectionId, string channel)
    {
        ArgumentNullException.ThrowIfNull(connectionId);
        ArgumentNullException.ThrowIfNull(channel);

        lock (_gate)
        {
            if (_channels.TryGetValue(channel, out var subs))
            {
                subs.RemoveAll(s => s.ConnectionId == connectionId);
                if (subs.Count == 0)
                {
                    _channels.Remove(channel);
                }
            }

            if (_connectionChannels.TryGetValue(connectionId, out var channelSet))
            {
                channelSet.Remove(channel);
                if (channelSet.Count == 0)
                {
                    _connectionChannels.Remove(connectionId);
                }
            }

            var channelCount = _connectionChannels.TryGetValue(connectionId, out var cc) ? cc.Count : 0;
            var patternCount = _connectionPatterns.TryGetValue(connectionId, out var pc) ? pc.Count : 0;
            _logger.LogDebug("Connection {ConnectionId} unsubscribed from channel {Channel}. Remaining: {Count}.", connectionId, channel, channelCount + patternCount);
            return channelCount + patternCount;
        }
    }

    /// <summary>Returns all channels the connection is currently subscribed to.</summary>
    public IReadOnlyList<string> GetConnectionChannels(string connectionId)
    {
        lock (_gate)
        {
            return _connectionChannels.TryGetValue(connectionId, out var channelSet)
                ? channelSet.ToArray()
                : Array.Empty<string>();
        }
    }

    // ---------------------------------------------------------------------------
    // Pattern subscriptions
    // ---------------------------------------------------------------------------

    /// <summary>Subscribes a connection to a glob pattern. Returns the connection's total subscription count.</summary>
    public int PSubscribe(string connectionId, string pattern, ChannelWriter<RespValue> writer)
    {
        ArgumentNullException.ThrowIfNull(connectionId);
        ArgumentNullException.ThrowIfNull(pattern);
        ArgumentNullException.ThrowIfNull(writer);

        lock (_gate)
        {
            if (!_patterns.TryGetValue(pattern, out var subs))
            {
                subs = new List<Subscription>();
                _patterns[pattern] = subs;
            }

            if (!subs.Any(s => s.ConnectionId == connectionId))
            {
                subs.Add(new Subscription(connectionId, writer));
            }

            if (!_connectionPatterns.TryGetValue(connectionId, out var patternSet))
            {
                patternSet = new HashSet<string>(StringComparer.Ordinal);
                _connectionPatterns[connectionId] = patternSet;
            }

            patternSet.Add(pattern);

            var channelCount = _connectionChannels.TryGetValue(connectionId, out var cc) ? cc.Count : 0;
            var patternCount = _connectionPatterns.TryGetValue(connectionId, out var pc) ? pc.Count : 0;
            _logger.LogDebug("Connection {ConnectionId} subscribed to pattern {Pattern}. Total: {Count}.", connectionId, pattern, channelCount + patternCount);
            return channelCount + patternCount;
        }
    }

    /// <summary>Unsubscribes a connection from a glob pattern. Returns the connection's remaining total subscription count.</summary>
    public int PUnsubscribe(string connectionId, string pattern)
    {
        ArgumentNullException.ThrowIfNull(connectionId);
        ArgumentNullException.ThrowIfNull(pattern);

        lock (_gate)
        {
            if (_patterns.TryGetValue(pattern, out var subs))
            {
                subs.RemoveAll(s => s.ConnectionId == connectionId);
                if (subs.Count == 0)
                {
                    _patterns.Remove(pattern);
                }
            }

            if (_connectionPatterns.TryGetValue(connectionId, out var patternSet))
            {
                patternSet.Remove(pattern);
                if (patternSet.Count == 0)
                {
                    _connectionPatterns.Remove(connectionId);
                }
            }

            var channelCount = _connectionChannels.TryGetValue(connectionId, out var cc) ? cc.Count : 0;
            var patternCount = _connectionPatterns.TryGetValue(connectionId, out var pc) ? pc.Count : 0;
            _logger.LogDebug("Connection {ConnectionId} unsubscribed from pattern {Pattern}. Remaining: {Count}.", connectionId, pattern, channelCount + patternCount);
            return channelCount + patternCount;
        }
    }

    /// <summary>Returns all patterns the connection is currently subscribed to.</summary>
    public IReadOnlyList<string> GetConnectionPatterns(string connectionId)
    {
        lock (_gate)
        {
            return _connectionPatterns.TryGetValue(connectionId, out var patternSet)
                ? patternSet.ToArray()
                : Array.Empty<string>();
        }
    }

    /// <summary>Removes all channel and pattern subscriptions for a connection.</summary>
    public void RemoveConnection(string connectionId)
    {
        ArgumentNullException.ThrowIfNull(connectionId);

        lock (_gate)
        {
            if (_connectionChannels.TryGetValue(connectionId, out var channelSet))
            {
                foreach (var channel in channelSet)
                {
                    if (_channels.TryGetValue(channel, out var subs))
                    {
                        subs.RemoveAll(s => s.ConnectionId == connectionId);
                        if (subs.Count == 0)
                        {
                            _channels.Remove(channel);
                        }
                    }
                }

                _connectionChannels.Remove(connectionId);
            }

            if (_connectionPatterns.TryGetValue(connectionId, out var patternSet))
            {
                foreach (var pattern in patternSet)
                {
                    if (_patterns.TryGetValue(pattern, out var subs))
                    {
                        subs.RemoveAll(s => s.ConnectionId == connectionId);
                        if (subs.Count == 0)
                        {
                            _patterns.Remove(pattern);
                        }
                    }
                }

                _connectionPatterns.Remove(connectionId);
            }
        }
    }

    // ---------------------------------------------------------------------------
    // Publish
    // ---------------------------------------------------------------------------

    /// <summary>
    /// Publishes a message to all channel and pattern subscribers on this node,
    /// and fans it out to peer nodes via <see cref="ICacheSyncBus"/>.
    /// Returns the number of subscribers that received the message locally.
    /// </summary>
    public int Publish(string channel, byte[] message)
    {
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentNullException.ThrowIfNull(message);

        Interlocked.Increment(ref _messagesPublishedTotal);
        _logger.LogDebug("Publishing message to channel {Channel}.", channel);

        var count = DeliverLocally(channel, message);

        // Cross-node fan-out: encode the message in the CacheInvalidationEvent
        if (_syncBus is not null)
        {
            var messageBase64 = Convert.ToBase64String(message);
            var ev = new CacheInvalidationEvent(
                SourceNodeId: _nodeId,
                Key: channel,
                VersionStamp: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                TimestampUtc: DateTimeOffset.UtcNow,
                Reason: CrossNodeEventReason + messageBase64);

            _ = _syncBus.PublishInvalidationAsync(ev).ConfigureAwait(false);
        }

        return count;
    }

    // ---------------------------------------------------------------------------
    // PUBSUB inspection commands
    // ---------------------------------------------------------------------------

    /// <summary>Returns all active channels (with at least one subscriber) matching the optional glob pattern.</summary>
    public IReadOnlyList<string> GetChannels(string? pattern = null)
    {
        lock (_gate)
        {
            IEnumerable<string> activeChannels = _channels
                .Where(static kv => kv.Value.Count > 0)
                .Select(static kv => kv.Key);

            if (string.IsNullOrEmpty(pattern) || pattern == "*")
            {
                return activeChannels.ToArray();
            }

            var regex = GlobToRegex(pattern);
            return activeChannels.Where(c => regex.IsMatch(c)).ToArray();
        }
    }

    /// <summary>Returns the subscriber count for each of the specified channels.</summary>
    public IReadOnlyDictionary<string, long> GetNumSub(IReadOnlyList<string> channels)
    {
        lock (_gate)
        {
            var result = new Dictionary<string, long>(channels.Count, StringComparer.Ordinal);
            foreach (var channel in channels)
            {
                result[channel] = _channels.TryGetValue(channel, out var subs) ? subs.Count : 0;
            }

            return result;
        }
    }

    /// <summary>Returns the total number of active pattern subscriptions across all connections.</summary>
    public long GetNumPat()
    {
        lock (_gate)
        {
            return _patterns.Values.Sum(static p => (long)p.Count);
        }
    }

    /// <summary>Returns the total number of unique subscribed connections (channel + pattern, de-duplicated).</summary>
    public long GetSubscribersTotal()
    {
        lock (_gate)
        {
            var all = new HashSet<string>(_connectionChannels.Keys, StringComparer.Ordinal);
            foreach (var id in _connectionPatterns.Keys)
            {
                all.Add(id);
            }

            return all.Count;
        }
    }

    // ---------------------------------------------------------------------------
    // Private helpers
    // ---------------------------------------------------------------------------

    private int DeliverLocally(string channel, byte[] message)
    {
        List<Subscription> channelSubs;
        List<(string Pattern, Subscription Sub)> patternSubs;

        lock (_gate)
        {
            channelSubs = _channels.TryGetValue(channel, out var cs) ? cs.ToList() : new List<Subscription>(0);

            patternSubs = new List<(string, Subscription)>();
            foreach (var (pattern, pSubs) in _patterns)
            {
                var regex = GlobToRegex(pattern);
                if (!regex.IsMatch(channel))
                {
                    continue;
                }

                foreach (var sub in pSubs)
                {
                    patternSubs.Add((pattern, sub));
                }
            }
        }

        var count = 0;

        foreach (var sub in channelSubs)
        {
            var push = RespValue.Array(new[]
            {
                RespValue.BulkString("message"),
                RespValue.BulkString(channel),
                RespValue.BulkString(message)
            });

            if (sub.Writer.TryWrite(push))
            {
                count++;
            }
            else
            {
                Interlocked.Increment(ref _messagesDroppedTotal);
                _logger.LogDebug("Dropped message for connection {ConnectionId} on channel {Channel} (buffer full).", sub.ConnectionId, channel);
            }
        }

        foreach (var (pattern, sub) in patternSubs)
        {
            var push = RespValue.Array(new[]
            {
                RespValue.BulkString("pmessage"),
                RespValue.BulkString(pattern),
                RespValue.BulkString(channel),
                RespValue.BulkString(message)
            });

            if (sub.Writer.TryWrite(push))
            {
                count++;
            }
            else
            {
                Interlocked.Increment(ref _messagesDroppedTotal);
                _logger.LogDebug("Dropped pmessage for connection {ConnectionId} on pattern {Pattern} (buffer full).", sub.ConnectionId, pattern);
            }
        }

        return count;
    }

    private Task OnCacheSyncEventAsync(CacheInvalidationEvent ev)
    {
        // Ignore our own events to prevent loops
        if (string.Equals(ev.SourceNodeId, _nodeId, StringComparison.Ordinal))
        {
            return Task.CompletedTask;
        }

        // Check if this is a cross-node pub/sub event
        if (!ev.Reason.StartsWith(CrossNodeEventReason, StringComparison.Ordinal))
        {
            return Task.CompletedTask;
        }

        var channel = ev.Key;
        var messageBase64 = ev.Reason[CrossNodeEventReason.Length..];
        byte[] message;
        try
        {
            message = Convert.FromBase64String(messageBase64);
        }
        catch (FormatException)
        {
            _logger.LogWarning("Received malformed cross-node pub/sub event for channel {Channel}.", channel);
            return Task.CompletedTask;
        }

        _logger.LogDebug("Received cross-node pub/sub message on channel {Channel} from node {NodeId}.", channel, ev.SourceNodeId);
        DeliverLocally(channel, message);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Converts a Redis glob pattern to a compiled regular expression.
    /// Supports <c>*</c> (any sequence), <c>?</c> (single character), and <c>[abc]</c> / <c>[!abc]</c> character classes.
    /// </summary>
    public static Regex GlobToRegex(string pattern)
    {
        var sb = new StringBuilder("^");
        var i = 0;
        while (i < pattern.Length)
        {
            var c = pattern[i];
            switch (c)
            {
                case '*':
                    sb.Append(".*");
                    break;
                case '?':
                    sb.Append('.');
                    break;
                case '[':
                    sb.Append('[');
                    i++;
                    if (i < pattern.Length && pattern[i] == '!')
                    {
                        sb.Append('^');
                        i++;
                    }

                    while (i < pattern.Length && pattern[i] != ']')
                    {
                        sb.Append(Regex.Escape(pattern[i].ToString()));
                        i++;
                    }

                    sb.Append(']');
                    break;
                default:
                    sb.Append(Regex.Escape(c.ToString()));
                    break;
            }

            i++;
        }

        sb.Append('$');
        return new Regex(sb.ToString(), RegexOptions.CultureInvariant);
    }

    public void Dispose()
    {
        _syncBusSubscription?.Dispose();
    }
}
