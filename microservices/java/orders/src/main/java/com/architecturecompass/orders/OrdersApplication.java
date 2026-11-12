package com.architecturecompass.orders;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Orders service (Java): accepts orders, writes them with an OrderPlaced event to the
 * transactional outbox, relays the outbox to Kafka, and confirms orders when
 * PaymentCaptured arrives.
 */
@SpringBootApplication
@EnableScheduling
public class OrdersApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrdersApplication.class, args);
    }
}
