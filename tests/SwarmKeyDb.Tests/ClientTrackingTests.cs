using System.Text;
using NUnit.Framework;
using SwarmKeyDb;
using static SwarmKeyDb.Tests.TestHelpers;

namespace SwarmKeyDb.Tests;

/// <summary>
/// Integration tests for CLIENT TRACKING push invalidation.
/// Uses two concurrent ProcessAsync sessions to verify cross-connection tracking behavior.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ClientTrackingTests
{
    /// <summary>
    /// Opens two concurrent ProcessAsync loops on the same processor.
    /// Connection A activates CLIENT TRACKING ON.
    /// Connection B writes a key.
    /// We then send a PING on connection A to flush the push channel,
    /// and verify that connection A received a push invalidation frame.
    /// </summary>
    [Test]
    public async Task ClientTracking_WriteFromOtherConnection_DeliversInvalidationPush()
    {
        var processor = CreateProcessor();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        // ---------- connection A: HELLO 3, CLIENT TRACKING ON ----------
        var pipeA = new System.IO.Pipelines.Pipe();
        var pipeAOut = new System.IO.Pipelines.Pipe();

        async Task SessionA()
        {
            await processor.ProcessAsync(pipeA.Reader.AsStream(), pipeAOut.Writer.AsStream(), cts.Token);
        }

        // Write HELLO 3 + CLIENT TRACKING ON to connection A's input
        var helloBytes = Encoding.UTF8.GetBytes(
            RespCommand("HELLO", "3") +
            RespCommand("CLIENT", "TRACKING", "ON"));
        await pipeA.Writer.WriteAsync(helloBytes, cts.Token);

        var sessionATask = Task.Run(SessionA, cts.Token);

        // Give A time to process the setup commands
        await Task.Delay(200, cts.Token);

        // ---------- connection B: SET the tracked key ----------
        await ExecuteAsync(processor, RespCommand("SET", "tracked-key", "new-value"));

        // Give the push a moment to be queued on A's push channel
        await Task.Delay(100, cts.Token);

        // Send a PING on connection A to trigger the push channel drain
        var pingBytes = Encoding.UTF8.GetBytes(RespCommand("PING"));
        await pipeA.Writer.WriteAsync(pingBytes, cts.Token);
        await Task.Delay(200, cts.Token);

        // ---------- Close connection A ----------
        pipeA.Writer.Complete();
        try
        {
            await sessionATask.WaitAsync(TimeSpan.FromSeconds(3));
        }
        catch (TimeoutException)
        {
            // Acceptable: session loop is still blocked on the pipe read
        }

        pipeAOut.Writer.Complete();

        // Read all bytes written to A's output
        var buf = new MemoryStream();
        try
        {
            while (pipeAOut.Reader.TryRead(out var result))
            {
                foreach (var segment in result.Buffer)
                {
                    buf.Write(segment.Span);
                }

                pipeAOut.Reader.AdvanceTo(result.Buffer.End);
                if (result.IsCompleted)
                {
                    break;
                }
            }
        }
        catch { /* pipe may already be closed */ }

        var raw = Encoding.UTF8.GetString(buf.ToArray());

        // Expect the HELLO 3 map response and +OK for TRACKING ON to have arrived
        Assert(raw.Contains("+OK", StringComparison.Ordinal),
            $"Expected +OK for CLIENT TRACKING ON in session A output, got: {raw}");

        // Expect a push invalidation: >2\r\n ... invalidate ... tracked-key
        Assert(raw.Contains("invalidate", StringComparison.Ordinal),
            $"Expected 'invalidate' push message in session A output after write on session B, got: {raw}");
        Assert(raw.Contains("tracked-key", StringComparison.Ordinal),
            $"Expected 'tracked-key' in invalidation push, got: {raw}");
    }

    /// <summary>
    /// After CLIENT TRACKING OFF, writes to tracked keys must NOT produce push invalidations.
    /// </summary>
    [Test]
    public async Task ClientTracking_Off_NoInvalidationDelivered()
    {
        var processor = CreateProcessor();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var pipeA = new System.IO.Pipelines.Pipe();
        var pipeAOut = new System.IO.Pipelines.Pipe();

        async Task SessionA()
        {
            await processor.ProcessAsync(pipeA.Reader.AsStream(), pipeAOut.Writer.AsStream(), cts.Token);
        }

        // Turn tracking ON then OFF, then issue a PING so the drain loop can run
        var setupBytes = Encoding.UTF8.GetBytes(
            RespCommand("HELLO", "3") +
            RespCommand("CLIENT", "TRACKING", "ON") +
            RespCommand("CLIENT", "TRACKING", "OFF"));
        await pipeA.Writer.WriteAsync(setupBytes, cts.Token);

        var sessionATask = Task.Run(SessionA, cts.Token);
        await Task.Delay(200, cts.Token);

        // Write to a key from connection B — tracking is now OFF so no invalidation should arrive
        await ExecuteAsync(processor, RespCommand("SET", "no-track-key", "value"));
        await Task.Delay(100, cts.Token);

        // Send PING to flush any accidental pushes
        var pingBytes = Encoding.UTF8.GetBytes(RespCommand("PING"));
        await pipeA.Writer.WriteAsync(pingBytes, cts.Token);
        await Task.Delay(200, cts.Token);

        // Close A
        pipeA.Writer.Complete();
        try
        {
            await sessionATask.WaitAsync(TimeSpan.FromSeconds(3));
        }
        catch (TimeoutException) { }

        pipeAOut.Writer.Complete();

        var buf = new MemoryStream();
        try
        {
            while (pipeAOut.Reader.TryRead(out var result))
            {
                foreach (var segment in result.Buffer)
                {
                    buf.Write(segment.Span);
                }

                pipeAOut.Reader.AdvanceTo(result.Buffer.End);
                if (result.IsCompleted)
                {
                    break;
                }
            }
        }
        catch { }

        var raw = Encoding.UTF8.GetString(buf.ToArray());

        // We should NOT see an invalidation after TRACKING OFF
        Assert(!raw.Contains("no-track-key", StringComparison.Ordinal),
            $"Expected no invalidation after CLIENT TRACKING OFF, but got: {raw}");
    }

    /// <summary>
    /// CLIENT TRACKING ON/OFF without HELLO 3 (RESP2) should still work.
    /// Invalidation messages are delivered as RESP2 arrays (push degrades to array).
    /// </summary>
    [Test]
    public async Task ClientTracking_Resp2_OnOff_ReturnsOk()
    {
        var processor = CreateProcessor();
        var response = await ExecuteAsync(processor,
            RespCommand("CLIENT", "TRACKING", "ON") +
            RespCommand("CLIENT", "TRACKING", "OFF"));

        var okCount = CountOccurrences(response, "+OK\r\n");
        Assert(okCount >= 2, $"Expected at least 2 +OK responses, got: {response}");
    }

    private static int CountOccurrences(string source, string target)
    {
        var count = 0;
        var index = 0;
        while ((index = source.IndexOf(target, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += target.Length;
        }

        return count;
    }
}
