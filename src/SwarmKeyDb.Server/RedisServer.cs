using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using SwarmKeyDb;

namespace SwarmKeyDb.Server;

public sealed class RedisServer
{
    private readonly TcpListener _listener;
    private readonly RedisCommandProcessor _processor;
    private readonly Action? _onClientConnected;
    private readonly Action? _onClientDisconnected;
    private readonly ILogger<RedisServer> _logger;

    public RedisServer(
        IPAddress address,
        int port,
        RedisCommandProcessor processor,
        Action? onClientConnected = null,
        Action? onClientDisconnected = null,
        ILogger<RedisServer>? logger = null)
    {
        _listener = new TcpListener(address, port);
        _processor = processor;
        _onClientConnected = onClientConnected;
        _onClientDisconnected = onClientDisconnected;
        _logger = logger ?? NullLogger<RedisServer>.Instance;
    }

    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        _listener.Start();
        _logger.LogInformation("SwarmKeyDb Redis server listening on {Endpoint}", _listener.LocalEndpoint);

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
            _onClientConnected?.Invoke();
            try
            {
                await _processor.ProcessAsync(stream, stream, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _onClientDisconnected?.Invoke();
            }
        }
    }
}
