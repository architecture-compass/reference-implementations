package com.architecturecompass.orders;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

@Service
class OrderService {

    static final String TOPIC_ORDER_PLACED = "orders.order-placed";

    record OrderRequest(String customerId, String sku, Integer quantity) {}

    record OrderView(String orderId, String status, String amount, String currency) {}

    private final JdbcTemplate jdbc;
    private final CatalogClient catalog;
    private final TransactionTemplate tx;
    private final JsonMapper json = JsonMapper.builder().build();

    OrderService(JdbcTemplate jdbc, CatalogClient catalog, TransactionTemplate tx) {
        this.jdbc = jdbc;
        this.catalog = catalog;
        this.tx = tx;
    }

    Optional<OrderView> findByKey(String key) {
        return jdbc.query(
                "SELECT id::text, status, amount, currency FROM orders WHERE idempotency_key = ?",
                (rs, i) -> new OrderView(rs.getString(1), rs.getString(2),
                        rs.getBigDecimal(3).setScale(2).toPlainString(), rs.getString(4)),
                key).stream().findFirst();
    }

    Optional<OrderView> findById(String id) {
        return jdbc.query(
                "SELECT id::text, status, amount, currency FROM orders WHERE id::text = ?",
                (rs, i) -> new OrderView(rs.getString(1), rs.getString(2),
                        rs.getBigDecimal(3).setScale(2).toPlainString(), rs.getString(4)),
                id).stream().findFirst();
    }

    /** Prices the order outside the transaction, then saves order + event atomically. */
    OrderView placeOrder(OrderRequest req, String idempotencyKey) {
        CatalogClient.Price price = catalog.price(req.sku());
        BigDecimal amount = new BigDecimal(price.price())
                .multiply(BigDecimal.valueOf(req.quantity()))
                .setScale(2, RoundingMode.HALF_UP);
        UUID orderId = UUID.randomUUID();
        save(orderId, req, amount, price.currency(), idempotencyKey);
        return new OrderView(orderId.toString(), "PENDING", amount.toPlainString(), price.currency());
    }

    /** The order and its OrderPlaced event commit together, or not at all. */
    private void save(UUID orderId, OrderRequest req, BigDecimal amount, String currency, String key) {
        tx.executeWithoutResult(status -> insertOrderAndEvent(orderId, req, amount, currency, key));
    }

    private void insertOrderAndEvent(UUID orderId, OrderRequest req, BigDecimal amount, String currency, String key) {
        jdbc.update("""
                INSERT INTO orders (id, customer_id, sku, quantity, amount, currency, status, idempotency_key)
                VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?)""",
                orderId, req.customerId(), req.sku(), req.quantity(), amount, currency, key);

        Map<String, String> event = new LinkedHashMap<>();
        event.put("eventId", UUID.randomUUID().toString());
        event.put("eventType", "OrderPlaced");
        event.put("orderId", orderId.toString());
        event.put("customerId", req.customerId());
        event.put("amount", amount.toPlainString());
        event.put("currency", currency);
        event.put("occurredAt", Instant.now().truncatedTo(ChronoUnit.SECONDS).toString());

        jdbc.update("""
                INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, topic, payload)
                VALUES (?, 'Order', ?, 'OrderPlaced', ?, ?::jsonb)""",
                UUID.randomUUID(), orderId.toString(), TOPIC_ORDER_PLACED, json.writeValueAsString(event));
    }

    /** Idempotent: a redelivered PaymentCaptured changes nothing. */
    @Transactional
    void confirm(String eventId, String orderId) {
        int firstTime = jdbc.update(
                "INSERT INTO processed_events (event_id) VALUES (?::uuid) ON CONFLICT DO NOTHING", eventId);
        if (firstTime == 0) return;
        jdbc.update("UPDATE orders SET status = 'CONFIRMED', updated_at = now() WHERE id::text = ?", orderId);
    }
}
