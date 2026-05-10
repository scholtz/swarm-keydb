using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace SwarmKeyDb;

public sealed class BeeSwarmClient : ISwarmClient, IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly string _postageBatchId;
    private readonly bool _disposeClient;

    public BeeSwarmClient(Uri beeEndpoint, string postageBatchId)
        : this(new HttpClient { BaseAddress = beeEndpoint }, postageBatchId, disposeClient: true)
    {
    }

    public BeeSwarmClient(HttpClient httpClient, string postageBatchId, bool disposeClient = false)
    {
        if (string.IsNullOrWhiteSpace(postageBatchId))
        {
            throw new ArgumentException("A Bee postage batch id is required for uploads.", nameof(postageBatchId));
        }

        var host = httpClient.BaseAddress?.Host;

        _httpClient = httpClient;
        _postageBatchId = postageBatchId;
        _disposeClient = disposeClient;
    }

    public async Task<string> UploadAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        using var content = new ByteArrayContent(data.ToArray());
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        using var request = new HttpRequestMessage(HttpMethod.Post, "bytes") { Content = content };
        request.Headers.TryAddWithoutValidation("swarm-postage-batch-id", _postageBatchId);
        request.Headers.TryAddWithoutValidation("swarm-pin", "true");

        using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        var upload = await JsonSerializer.DeserializeAsync<BeeUploadResponse>(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(upload?.Reference))
        {
            throw new InvalidOperationException("Bee did not return a Swarm reference.");
        }

        return upload.Reference;
    }

    public async Task<byte[]> DownloadAsync(string reference, CancellationToken cancellationToken = default)
    {
        using var response = await _httpClient.GetAsync($"bytes/{Uri.EscapeDataString(reference)}", cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (_disposeClient)
        {
            _httpClient.Dispose();
        }
    }

    private sealed class BeeUploadResponse
    {
        [JsonPropertyName("reference")]
        public string? Reference { get; set; }
    }
}
