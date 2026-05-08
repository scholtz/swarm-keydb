using SwarmKeyDb;

namespace SwarmKeyDb.Server;

public sealed class InstrumentedSwarmClient : ISwarmClient
{
    private readonly ISwarmClient _inner;
    private readonly MonitoringMetrics _metrics;

    public InstrumentedSwarmClient(ISwarmClient inner, MonitoringMetrics metrics)
    {
        _inner = inner;
        _metrics = metrics;
    }

    public async Task<string> UploadAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        var reference = await _inner.UploadAsync(data, cancellationToken).ConfigureAwait(false);
        _metrics.OnSwarmWrite();
        return reference;
    }

    public async Task<byte[]> DownloadAsync(string reference, CancellationToken cancellationToken = default)
    {
        var payload = await _inner.DownloadAsync(reference, cancellationToken).ConfigureAwait(false);
        _metrics.OnSwarmRead();
        return payload;
    }
}
