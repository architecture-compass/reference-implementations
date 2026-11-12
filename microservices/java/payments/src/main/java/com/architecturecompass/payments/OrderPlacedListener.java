package com.architecturecompass.payments;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Captures the payment and records the event in one transaction. A redelivered event,
 * or a second event for the same order, changes nothing. The Kafka offset is committed
 * only after the transaction succeeds.
 */
@Component
class OrderPlacedListener {

    static final String TOPIC_PAYMENT_CAPTURED = "payments.payment-captured";
    private static final Logger log = LoggerFactory.getLogger(OrderPlacedListener.class);

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final JsonMapper json = JsonMapper.builder().build();

    OrderPlacedListener(JdbcTemplate jdbc, TransactionTemplate tx) {
        this.jdbc = jdbc;
        this.tx = tx;
    }

    @KafkaListener(topics = "orders.order-placed", groupId = "payments")
    void onOrderPlaced(String message) {
        JsonNode evt;
        try {
            evt = json.readTree(message);
        } catch (JacksonException e) {
            log.error("skipping malformed event");
            return;
        }
        String eventId = evt.path("eventId").asString();
        String orderId = evt.path("orderId").asString();
        String amount = evt.path("amount").asString();
        tx.executeWithoutResult(status -> capture(eventId, orderId, amount));
    }

    private void capture(String eventId, String orderId, String amount) {
        int firstTime = jdbc.update(
                "INSERT INTO processed_events (event_id) VALUES (?::uuid) ON CONFLICT DO NOTHING", eventId);
        if (firstTime == 0) return; // duplicate delivery

        UUID paymentId = UUID.randomUUID();
        int captured = jdbc.update("""
                INSERT INTO payments (id, order_id, amount, status) VALUES (?, ?::uuid, ?::numeric, 'CAPTURED')
                ON CONFLICT (order_id) DO NOTHING""", paymentId, orderId, amount);
        if (captured == 0) return; // this order was already paid

        // A real service would call the payment provider here, using the
        // order ID as the provider's idempotency key.
        Map<String, String> event = new LinkedHashMap<>();
        event.put("eventId", UUID.randomUUID().toString());
        event.put("eventType", "PaymentCaptured");
        event.put("orderId", orderId);
        event.put("paymentId", paymentId.toString());
        event.put("amount", amount);
        event.put("occurredAt", Instant.now().truncatedTo(ChronoUnit.SECONDS).toString());

        jdbc.update("""
                INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, topic, payload)
                VALUES (?, 'Payment', ?, 'PaymentCaptured', ?, ?::jsonb)""",
                UUID.randomUUID(), orderId, TOPIC_PAYMENT_CAPTURED, json.writeValueAsString(event));
    }
}
