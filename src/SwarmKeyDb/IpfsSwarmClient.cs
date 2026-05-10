using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace SwarmKeyDb;

public sealed class IpfsSwarmClient : ISwarmClient, ISwarmDeletionClient, IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly bool _disposeClient;
    private readonly bool _pinOnWrite;

    public IpfsSwarmClient(Uri apiEndpoint, bool pinOnWrite = true)
        : this(new HttpClient { BaseAddress = apiEndpoint }, pinOnWrite, disposeClient: true)
    {
    }

    public IpfsSwarmClient(HttpClient httpClient, bool pinOnWrite = true, bool disposeClient = false)
    {
        _httpClient = httpClient;
        _disposeClient = disposeClient;
        _pinOnWrite = pinOnWrite;
    }

    public async Task<string> UploadAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        using var content = new MultipartFormDataContent();
        using var fileContent = new ByteArrayContent(data.ToArray());
        fileContent.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        content.Add(fileContent, "file", "value.bin");

        var endpoint = $"api/v0/add?pin={(_pinOnWrite ? "true" : "false")}&cid-version=1&quieter=true";
        using var response = await _httpClient.PostAsync(endpoint, content, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();

        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        var upload = await JsonSerializer.DeserializeAsync<IpfsAddResponse>(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(upload?.Hash))
        {
            throw new InvalidOperationException("IPFS add did not return a CID hash.");
        }

        return upload.Hash;
    }

    public async Task<byte[]> DownloadAsync(string reference, CancellationToken cancellationToken = default)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, $"api/v0/cat?arg={Uri.EscapeDataString(reference)}");
        using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> DeleteAsync(string reference, CancellationToken cancellationToken = default)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, $"api/v0/pin/rm?arg={Uri.EscapeDataString(reference)}");
        using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
        if (response.IsSuccessStatusCode)
        {
            return true;
        }

        var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        if (body.Contains("not pinned", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        response.EnsureSuccessStatusCode();
        return false;
    }

    public void Dispose()
    {
        if (_disposeClient)
        {
            _httpClient.Dispose();
        }
    }

    private sealed class IpfsAddResponse
    {
        [JsonPropertyName("Hash")]
        public string? Hash { get; set; }
    }
}
