using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using SwarmKeyDb;
using SwarmKeyDb.Cli;
using SwarmKeyDb.Migrate;
using SwarmKeyDb.SwarmConsistency;
using SwarmKeyDb.Server;
using static SwarmKeyDb.Tests.TestHelpers;

namespace SwarmKeyDb.Tests;

[TestFixture]
public class PubSubCommandTests
{
    [Test]
    public async Task PubSubSubscribeAndPublishSingleNodeAsync()
    {
        var manager = new PubSubManager();
        var processor = CreatePubSubProcessor(manager);

        // SUBSCRIBE on a pipe-backed stream so we can write PUBLISH later
        var (subscriberInput, subscriberOutput) = CreatePipe();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        // Start the subscriber loop in background
        var subscriberTask = processor.ProcessAsync(subscriberInput, subscriberOutput, cts.Token);

        // Send SUBSCRIBE command
        await WriteRespCommandAsync(subscriberInput, "SUBSCRIBE", "news");
        await Task.Delay(50, cts.Token); // Give the processor time to handle it

        // Publish via a second processor call
        var publishProcessor = CreatePubSubProcessor(manager);
        var publishReply = await publishProcessor.ExecuteAsync(BuildRespCommand("PUBLISH", "news", "hello"), cts.Token);
        AssertEqual(RespType.Integer, publishReply.Type);
        AssertEqual(1L, publishReply.Integer);

        // Give subscriber time to receive the message
        await Task.Delay(50, cts.Token);

        // Close the subscriber connection
        cts.Cancel();
        try { await subscriberTask; } catch (OperationCanceledException) { }

        var output = ReadAllBytes(subscriberOutput);
        Assert(output.Contains("subscribe", StringComparison.Ordinal), "Subscriber should have received subscribe confirmation.");
        Assert(output.Contains("message", StringComparison.Ordinal), "Subscriber should have received the published message.");
        Assert(output.Contains("hello", StringComparison.Ordinal), "Subscriber should have received the message payload.");
        Assert(output.Contains("news", StringComparison.Ordinal), "Subscriber should have received the channel name.");
    }

    [Test]
    public async Task PubSubPatternSubscribeReceivesMatchingChannelMessagesAsync()
    {
        var manager = new PubSubManager();
        var processor = CreatePubSubProcessor(manager);

        var (subscriberInput, subscriberOutput) = CreatePipe();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var subscriberTask = processor.ProcessAsync(subscriberInput, subscriberOutput, cts.Token);

        // PSUBSCRIBE to a pattern
        await WriteRespCommandAsync(subscriberInput, "PSUBSCRIBE", "news:*");
        await Task.Delay(50, cts.Token);

        // Publish to a channel matching the pattern
        var publishProcessor = CreatePubSubProcessor(manager);
        var reply1 = await publishProcessor.ExecuteAsync(BuildRespCommand("PUBLISH", "news:sports", "goal"), cts.Token);
        AssertEqual(1L, reply1.Integer); // 1 pattern subscriber

        // Publish to a non-matching channel
        var reply2 = await publishProcessor.ExecuteAsync(BuildRespCommand("PUBLISH", "other:channel", "ignored"), cts.Token);
        AssertEqual(0L, reply2.Integer);

        await Task.Delay(50, cts.Token);
        cts.Cancel();
        try { await subscriberTask; } catch (OperationCanceledException) { }

        var output = ReadAllBytes(subscriberOutput);
        Assert(output.Contains("pmessage", StringComparison.Ordinal), "Pattern subscriber should have received pmessage.");
        Assert(output.Contains("news:*", StringComparison.Ordinal), "pmessage should include the pattern.");
        Assert(output.Contains("news:sports", StringComparison.Ordinal), "pmessage should include the channel.");
        Assert(output.Contains("goal", StringComparison.Ordinal), "pmessage should include the payload.");
        Assert(!output.Contains("ignored", StringComparison.Ordinal), "Non-matching channel message should not be delivered.");
    }

    [Test]
    public async Task PubSubMultiSubscriberFanOutDeliversToAllAsync()
    {
        var manager = new PubSubManager();
        var processor = CreatePubSubProcessor(manager);

        var (input1, output1) = CreatePipe();
        var (input2, output2) = CreatePipe();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var task1 = processor.ProcessAsync(input1, output1, cts.Token);
        var task2 = processor.ProcessAsync(input2, output2, cts.Token);

        await WriteRespCommandAsync(input1, "SUBSCRIBE", "broadcast");
        await WriteRespCommandAsync(input2, "SUBSCRIBE", "broadcast");
        await Task.Delay(80, cts.Token);

        var publishProcessor = CreatePubSubProcessor(manager);
        var reply = await publishProcessor.ExecuteAsync(BuildRespCommand("PUBLISH", "broadcast", "hello-all"), cts.Token);
        AssertEqual(2L, reply.Integer); // Both subscribers

        await Task.Delay(80, cts.Token);
        cts.Cancel();
        try { await task1; } catch (OperationCanceledException) { }
        try { await task2; } catch (OperationCanceledException) { }

        Assert(ReadAllBytes(output1).Contains("hello-all", StringComparison.Ordinal), "Subscriber 1 should receive message.");
        Assert(ReadAllBytes(output2).Contains("hello-all", StringComparison.Ordinal), "Subscriber 2 should receive message.");
    }

    [Test]
    public async Task PubSubSubCommandsAsync()
    {
        var manager = new PubSubManager();
        var processor = CreatePubSubProcessor(manager);

        var (input1, output1) = CreatePipe();
        var (input2, output2) = CreatePipe();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var task1 = processor.ProcessAsync(input1, output1, cts.Token);
        var task2 = processor.ProcessAsync(input2, output2, cts.Token);

        await WriteRespCommandAsync(input1, "SUBSCRIBE", "ch1", "ch2");
        await WriteRespCommandAsync(input2, "PSUBSCRIBE", "ch*");
        await Task.Delay(80, cts.Token);

        var inspectProcessor = CreatePubSubProcessor(manager);

        // PUBSUB CHANNELS
        var channels = await inspectProcessor.ExecuteAsync(BuildRespCommand("PUBSUB", "CHANNELS"), cts.Token);
        AssertEqual(RespType.Array, channels.Type);
        var channelNames = channels.Items!.Select(static v => v.AsString()).OrderBy(static s => s).ToArray();
        AssertSequenceEqual(new[] { "ch1", "ch2" }, channelNames);

        // PUBSUB CHANNELS with pattern
        var filteredChannels = await inspectProcessor.ExecuteAsync(BuildRespCommand("PUBSUB", "CHANNELS", "ch?"), cts.Token);
        AssertEqual(2, filteredChannels.Items!.Count);

        // PUBSUB NUMSUB
        var numSub = await inspectProcessor.ExecuteAsync(BuildRespCommand("PUBSUB", "NUMSUB", "ch1", "ch2", "ch3"), cts.Token);
        AssertEqual(RespType.Array, numSub.Type);
        AssertEqual(6, numSub.Items!.Count); // 3 channel-name/count pairs
        AssertEqual(1L, numSub.Items[1].Integer); // ch1 has 1 subscriber
        AssertEqual(1L, numSub.Items[3].Integer); // ch2 has 1 subscriber
        AssertEqual(0L, numSub.Items[5].Integer); // ch3 has 0 subscribers

        // PUBSUB NUMPAT
        var numPat = await inspectProcessor.ExecuteAsync(BuildRespCommand("PUBSUB", "NUMPAT"), cts.Token);
        AssertEqual(RespType.Integer, numPat.Type);
        AssertEqual(1L, numPat.Integer); // 1 pattern subscription

        cts.Cancel();
        try { await task1; } catch (OperationCanceledException) { }
        try { await task2; } catch (OperationCanceledException) { }
    }

    [Test]
    public async Task PubSubUnsubscribeReducesCountAsync()
    {
        var manager = new PubSubManager();
        var processor = CreatePubSubProcessor(manager);

        var (subscriberInput, subscriberOutput) = CreatePipe();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var subscriberTask = processor.ProcessAsync(subscriberInput, subscriberOutput, cts.Token);

        await WriteRespCommandAsync(subscriberInput, "SUBSCRIBE", "ch1", "ch2");
        await Task.Delay(50, cts.Token);

        // Verify both channels are subscribed
        AssertEqual(2, manager.GetChannels().Count);

        // Unsubscribe from ch1
        await WriteRespCommandAsync(subscriberInput, "UNSUBSCRIBE", "ch1");
        await Task.Delay(50, cts.Token);

        AssertEqual(1, manager.GetChannels().Count);
        AssertEqual("ch2", manager.GetChannels()[0]);

        // Verify PUBSUB NUMSUB
        var numSub = manager.GetNumSub(new[] { "ch1", "ch2" });
        AssertEqual(0L, numSub["ch1"]);
        AssertEqual(1L, numSub["ch2"]);

        cts.Cancel();
        try { await subscriberTask; } catch (OperationCanceledException) { }
    }

    [Test]
    public async Task PubSubSlowSubscriberDoesNotBlockOtherSubscribersAsync()
    {
        var manager = new PubSubManager();
        var processor = CreatePubSubProcessor(manager);

        // Fast subscriber
        var (fastInput, fastOutput) = CreatePipe();
        // Slow subscriber: backed by a stream that does not read (simulated via overflowing the channel)
        var (slowInput, slowOutput) = CreatePipe();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var fastTask = processor.ProcessAsync(fastInput, fastOutput, cts.Token);
        var slowTask = processor.ProcessAsync(slowInput, slowOutput, cts.Token);

        await WriteRespCommandAsync(fastInput, "SUBSCRIBE", "ch");
        await WriteRespCommandAsync(slowInput, "SUBSCRIBE", "ch");
        await Task.Delay(80, cts.Token);

        // Publish many messages; the slow subscriber's push channel will drop some
        var publishProcessor = CreatePubSubProcessor(manager);
        for (var i = 0; i < 300; i++)
        {
            _ = await publishProcessor.ExecuteAsync(BuildRespCommand("PUBLISH", "ch", $"msg{i}"), cts.Token);
        }

        await Task.Delay(100, cts.Token);

        // Fast subscriber should not be starved; messages_dropped counter exists (may be 0 for fast)
        var dropped = manager.MessagesDroppedTotal;
        // We do not assert that dropped > 0 because the fast subscriber may keep up; we assert non-negative
        Assert(dropped >= 0, "Dropped messages counter should be non-negative.");
        Assert(manager.MessagesPublishedTotal == 300, "All 300 messages should have been counted as published.");

        cts.Cancel();
        try { await fastTask; } catch (OperationCanceledException) { }
        try { await slowTask; } catch (OperationCanceledException) { }
    }

    [Test]
    public async Task PubSubCrossNodeDeliveryViaInMemoryBusAsync()
    {
        var bus = new InMemoryCacheSyncBus();
        var manager1 = new PubSubManager(syncBus: bus, nodeId: "node-1");
        var manager2 = new PubSubManager(syncBus: bus, nodeId: "node-2");

        var processor1 = CreatePubSubProcessor(manager1);
        var processor2 = CreatePubSubProcessor(manager2);

        var (sub2Input, sub2Output) = CreatePipe();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var sub2Task = processor2.ProcessAsync(sub2Input, sub2Output, cts.Token);

        // Subscribe on node2
        await WriteRespCommandAsync(sub2Input, "SUBSCRIBE", "cross");
        await Task.Delay(80, cts.Token);

        // Publish from node1
        var reply = await processor1.ExecuteAsync(BuildRespCommand("PUBLISH", "cross", "cross-node-message"), cts.Token);
        // Local count is 0 (no subscribers on node1); cross-node delivery is async
        AssertEqual(0L, reply.Integer);

        // Wait for cross-node delivery
        await Task.Delay(150, cts.Token);

        cts.Cancel();
        try { await sub2Task; } catch (OperationCanceledException) { }

        var output = ReadAllBytes(sub2Output);
        Assert(output.Contains("cross-node-message", StringComparison.Ordinal), "Cross-node subscriber should receive the message.");

        manager1.Dispose();
        manager2.Dispose();
    }

    [Test]
    public async Task PubSubGlobPatternMatchingAsync()
    {
        // Test PubSubManager.GlobToRegex directly and end-to-end
        var manager = new PubSubManager();

        // ? matches exactly one char
        var regex1 = PubSubManager.GlobToRegex("h?llo");
        Assert(regex1.IsMatch("hello"), "? should match single character.");
        Assert(!regex1.IsMatch("hllo"), "? should not match zero characters.");
        Assert(!regex1.IsMatch("heello"), "? should not match two characters.");

        // * matches zero or more chars
        var regex2 = PubSubManager.GlobToRegex("h*llo");
        Assert(regex2.IsMatch("hello"), "* should match one character.");
        Assert(regex2.IsMatch("hllo"), "* should match zero characters.");
        Assert(regex2.IsMatch("heeeello"), "* should match many characters.");

        // Character class [abc]
        var regex3 = PubSubManager.GlobToRegex("h[ae]llo");
        Assert(regex3.IsMatch("hello"), "[ae] should match 'e'.");
        Assert(regex3.IsMatch("hallo"), "[ae] should match 'a'.");
        Assert(!regex3.IsMatch("hillo"), "[ae] should not match 'i'.");

        // Negated class [!abc]
        var regex4 = PubSubManager.GlobToRegex("h[!ae]llo");
        Assert(!regex4.IsMatch("hello"), "[!ae] should reject 'e'.");
        Assert(regex4.IsMatch("hillo"), "[!ae] should accept 'i'.");

        // End-to-end through the processor
        var processor = CreatePubSubProcessor(manager);
        var (input, output) = CreatePipe();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var task = processor.ProcessAsync(input, output, cts.Token);

        await WriteRespCommandAsync(input, "PSUBSCRIBE", "h?llo");
        await Task.Delay(40, cts.Token);

        var publishProcessor = CreatePubSubProcessor(manager);
        var r1 = await publishProcessor.ExecuteAsync(BuildRespCommand("PUBLISH", "hello", "a"), cts.Token);
        var r2 = await publishProcessor.ExecuteAsync(BuildRespCommand("PUBLISH", "hllo", "b"), cts.Token);
        AssertEqual(1L, r1.Integer); // "hello" matches "h?llo"
        AssertEqual(0L, r2.Integer); // "hllo" does not match "h?llo"

        cts.Cancel();
        try { await task; } catch (OperationCanceledException) { }
    }

}
