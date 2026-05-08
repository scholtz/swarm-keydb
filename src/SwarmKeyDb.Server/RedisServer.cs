using System.Net;
using System.Net.Sockets;
using SwarmKeyDb;

namespace SwarmKeyDb.Server;

public sealed class RedisServer
{
    private readonly TcpListener _listener;
    private readonly RedisCommandProcessor _processor;

    public RedisServer(IPAddress address, int port, RedisCommandProcessor processor)
    {
        _listener = new TcpListener(address, port);
        _processor = processor;
    }

    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        _listener.Start();
        Console.WriteLine($"SwarmKeyDb Redis server listening on {_listener.LocalEndpoint}");

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var client = await _listener.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
                _ = Task.Run(() => HandleClientAsync(client, cancellationToken), cancellationToken);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        finally
        {
            _listener.Stop();
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken cancellationToken)
    {
        await using var stream = client.GetStream();
        using (client)
        {
            await _processor.ProcessAsync(stream, stream, cancellationToken).ConfigureAwait(false);
        }
    }
}
