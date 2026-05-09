using System.Net.Http.Json;

namespace SwarmKeyDb;

public interface IConnectivityProbe
{
    Task<bool> IsConnectedAsync(CancellationToken cancellationToken = default);
}

public sealed class AlwaysConnectedProbe : IConnectivityProbe
{
    public Task<bool> IsConnectedAsync(CancellationToken cancellationToken = default) => Task.FromResult(true);
}

public sealed class HttpHealthConnectivityProbe : IConnectivityProbe, IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly string _healthPath;

    public HttpHealthConnectivityProbe(Uri endpoint, string healthPath = "health")
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        _httpClient = new HttpClient
        {
            BaseAddress = endpoint,
            Timeout = TimeSpan.FromSeconds(3)
        };
        _healthPath = string.IsNullOrWhiteSpace(healthPath) ? "health" : healthPath.TrimStart('/');
    }

    public async Task<bool> IsConnectedAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var response = await _httpClient.GetAsync(_healthPath, cancellationToken).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
            {
                return false;
            }

            if (response.Content.Headers.ContentType?.MediaType?.Contains("json", StringComparison.OrdinalIgnoreCase) == true)
            {
                var payload = await response.Content.ReadFromJsonAsync<HealthPayload>(cancellationToken).ConfigureAwait(false);
                return payload?.Status is null ||
                    payload.Status.Equals("ok", StringComparison.OrdinalIgnoreCase) ||
                    payload.Status.Equals("healthy", StringComparison.OrdinalIgnoreCase);
            }

            return true;
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
        {
            return false;
        }
    }

    public void Dispose() => _httpClient.Dispose();

    private sealed class HealthPayload
    {
        public string? Status { get; set; }
    }
}
