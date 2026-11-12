package com.architecturecompass.orders;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

/** Catalog client: 500 ms timeout, 2 retries, circuit breaker, last-known-price fallback. */
@Component
class CatalogClient {

    record Price(String sku, String price, String currency) {}

    static class UnknownSkuException extends RuntimeException {
        UnknownSkuException(String sku) { super("unknown sku " + sku); }
    }

    static class CatalogUnavailableException extends RuntimeException {
        CatalogUnavailableException(Throwable cause) { super("catalog unavailable: " + cause.getMessage(), cause); }
    }

    private static final Logger log = LoggerFactory.getLogger(CatalogClient.class);

    private final RestClient http;
    private final CircuitBreaker breaker;
    private final Map<String, Price> lastKnown = new ConcurrentHashMap<>();

    CatalogClient() {
        SimpleClientHttpRequestFactory timeouts = new SimpleClientHttpRequestFactory();
        timeouts.setConnectTimeout(Duration.ofMillis(500));
        timeouts.setReadTimeout(Duration.ofMillis(500));
        this.http = RestClient.builder()
                .baseUrl(System.getenv("CATALOG_URL"))
                .requestFactory(timeouts)
                .build();

        // A window of the last 3 calls, all failing, opens the breaker for 10 seconds.
        // An unknown SKU is a correct answer from the catalog, not a failure.
        this.breaker = CircuitBreaker.of("catalog", CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(3)
                .minimumNumberOfCalls(3)
                .failureRateThreshold(100)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .ignoreExceptions(UnknownSkuException.class)
                .build());
    }

    private Price fetch(String sku) {
        try {
            return http.get().uri("/prices/{sku}", sku).retrieve().body(Price.class);
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) throw new UnknownSkuException(sku);
            throw e;
        }
    }

    Price price(String sku) {
        RuntimeException lastError = null;
        for (int attempt = 0; attempt < 3; attempt++) { // 1 call + 2 retries
            try {
                Price p = breaker.executeSupplier(() -> fetch(sku));
                lastKnown.put(sku, p);
                return p;
            } catch (UnknownSkuException e) {
                throw e;
            } catch (CallNotPermittedException e) {
                lastError = e;
                break; // breaker is open: don't keep trying
            } catch (RuntimeException e) {
                lastError = e;
                sleep(100);
            }
        }
        Price cached = lastKnown.get(sku);
        if (cached != null) {
            log.warn("catalog unavailable, using last known price for {}: {}", sku, lastError.getMessage());
            return cached;
        }
        throw new CatalogUnavailableException(lastError);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
