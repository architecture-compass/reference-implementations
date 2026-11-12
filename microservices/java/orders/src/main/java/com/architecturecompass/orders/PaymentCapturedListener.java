package com.architecturecompass.orders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Confirms orders when payment is captured. The offset is committed only after success. */
@Component
class PaymentCapturedListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentCapturedListener.class);

    private final OrderService orders;
    private final JsonMapper json = JsonMapper.builder().build();

    PaymentCapturedListener(OrderService orders) {
        this.orders = orders;
    }

    @KafkaListener(topics = "payments.payment-captured", groupId = "orders")
    void onPaymentCaptured(String message) {
        JsonNode evt;
        try {
            evt = json.readTree(message);
        } catch (JacksonException e) {
            log.error("skipping malformed event");
            return;
        }
        orders.confirm(evt.path("eventId").asString(), evt.path("orderId").asString());
    }
}
