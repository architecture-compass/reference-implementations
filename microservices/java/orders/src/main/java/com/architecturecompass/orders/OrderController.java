package com.architecturecompass.orders;

import java.util.Map;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
class OrderController {

    private final OrderService orders;

    OrderController(OrderService orders) {
        this.orders = orders;
    }

    @PostMapping("/orders")
    ResponseEntity<?> placeOrder(@RequestHeader(value = "Idempotency-Key", required = false) String key,
                                 @RequestBody(required = false) OrderService.OrderRequest req) {
        if (key == null || key.isBlank()) {
            return error(HttpStatus.BAD_REQUEST, "Idempotency-Key header is required");
        }
        if (req == null || isBlank(req.customerId()) || isBlank(req.sku()) || req.quantity() == null || req.quantity() < 1) {
            return error(HttpStatus.BAD_REQUEST, "customerId, sku and a positive quantity are required");
        }
        var existing = orders.findByKey(key); // same key seen before
        if (existing.isPresent()) {
            return ResponseEntity.accepted().body(existing.get());
        }
        try {
            return ResponseEntity.accepted().body(orders.placeOrder(req, key));
        } catch (CatalogClient.UnknownSkuException e) {
            return error(HttpStatus.NOT_FOUND, "unknown sku");
        } catch (CatalogClient.CatalogUnavailableException e) {
            return error(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
        } catch (DuplicateKeyException e) { // concurrent request with the same key
            return ResponseEntity.accepted().body(orders.findByKey(key).orElseThrow());
        }
    }

    @GetMapping("/orders/{id}")
    ResponseEntity<?> getOrder(@PathVariable String id) {
        return orders.findById(id)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> error(HttpStatus.NOT_FOUND, "order not found"));
    }

    @GetMapping("/health")
    Map<String, String> health() {
        return Map.of("status", "ok");
    }

    private static ResponseEntity<?> error(HttpStatus status, String message) {
        return ResponseEntity.status(status).body(Map.of("error", message));
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
