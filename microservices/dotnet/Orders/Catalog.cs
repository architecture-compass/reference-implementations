using System.Collections.Concurrent;
using System.Net;
using Polly;
using Polly.CircuitBreaker;

namespace Orders;

public record CatalogPrice(string Sku, string Price, string Currency);
public class UnknownSkuException(string sku) : Exception($"unknown sku {sku}");
public class CatalogUnavailableException(string message) : Exception($"catalog unavailable: {message}");

/// <summary>Catalog client: 500 ms timeout, 2 retries, circuit breaker, last-known-price fallback.</summary>
public class Catalog
{
    private readonly HttpClient _http;
    private readonly ResiliencePipeline _breaker;
    private readonly ConcurrentDictionary<string, CatalogPrice> _lastKnown = new();
    private readonly ILogger _log = LoggerFactory.Create(b => b.AddConsole()).CreateLogger<Catalog>();

    public Catalog(string baseUrl)
    {
        _http = new HttpClient { BaseAddress = new Uri(baseUrl), Timeout = TimeSpan.FromMilliseconds(500) };

        // Opens when every call in the window fails (at least 3 calls), for 10 seconds.
        // An unknown SKU is a correct answer from the catalog, so it is not handled as a failure.
        _breaker = new ResiliencePipelineBuilder()
            .AddCircuitBreaker(new CircuitBreakerStrategyOptions
            {
                FailureRatio = 1.0,
                MinimumThroughput = 3,
                SamplingDuration = TimeSpan.FromSeconds(30),
                BreakDuration = TimeSpan.FromSeconds(10),
                ShouldHandle = new PredicateBuilder().Handle<Exception>(e => e is not UnknownSkuException),
            })
            .Build();
    }

    private async Task<CatalogPrice> Fetch(string sku, CancellationToken ct)
    {
        using var resp = await _http.GetAsync($"/prices/{Uri.EscapeDataString(sku)}", ct);
        if (resp.StatusCode == HttpStatusCode.NotFound) throw new UnknownSkuException(sku);
        resp.EnsureSuccessStatusCode();
        return (await resp.Content.ReadFromJsonAsync<CatalogPrice>(ct))!;
    }

    public async Task<CatalogPrice> GetPrice(string sku)
    {
        Exception? lastError = null;
        for (var attempt = 0; attempt < 3; attempt++) // 1 call + 2 retries
        {
            try
            {
                var price = await _breaker.ExecuteAsync(ct => new ValueTask<CatalogPrice>(Fetch(sku, ct)));
                _lastKnown[sku] = price;
                return price;
            }
            catch (UnknownSkuException) { throw; }
            catch (BrokenCircuitException e) { lastError = e; break; } // don't hammer an open breaker
            catch (Exception e) { lastError = e; await Task.Delay(100); }
        }
        if (_lastKnown.TryGetValue(sku, out var cached))
        {
            _log.LogWarning("catalog unavailable, using last known price for {Sku}: {Error}", sku, lastError?.Message);
            return cached;
        }
        throw new CatalogUnavailableException(lastError?.Message ?? "unknown error");
    }
}
