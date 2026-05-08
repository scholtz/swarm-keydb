using System.Net.Http.Json;

namespace SwarmKeyDb.Server;

public interface IReadinessProbe
{
    Task<(bool Ready, string Message)> CheckAsync(CancellationToken cancellationToken = default);
}

public sealed class AlwaysReadyProbe : IReadinessProbe
{
    public Task<(bool Ready, string Message)> CheckAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult((true, "local backend ready"));
}

public sealed class BeeReadinessProbe : IReadinessProbe, IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly string _postageBatchId;

    public BeeReadinessProbe(Uri beeEndpoint, string postageBatchId)
    {
        _httpClient = new HttpClient
        {
            BaseAddress = beeEndpoint,
            Timeout = TimeSpan.FromSeconds(3)
        };
        _postageBatchId = postageBatchId;
    }

    public async Task<(bool Ready, string Message)> CheckAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var healthResponse = await _httpClient.GetAsync("health", cancellationToken).ConfigureAwait(false);
            if (!healthResponse.IsSuccessStatusCode)
            {
                return (false, $"bee health endpoint returned {(int)healthResponse.StatusCode}");
            }

            using var stampResponse = await _httpClient.GetAsync($"stamps/{Uri.EscapeDataString(_postageBatchId)}", cancellationToken).ConfigureAwait(false);
            if (!stampResponse.IsSuccessStatusCode)
            {
                return (false, $"postage batch id {_postageBatchId} is not valid");
            }

            var stamp = await stampResponse.Content.ReadFromJsonAsync<BeeStampResponse>(cancellationToken).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(stamp?.BatchID))
            {
                return (false, "postage stamp response did not include a batch id");
            }

            return (true, "bee backend ready");
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
        {
            return (false, $"bee node unreachable: {ex.Message}");
        }
    }

    private sealed class BeeStampResponse
    {
        public string? BatchID { get; set; }
    }

    public void Dispose() => _httpClient.Dispose();
}
