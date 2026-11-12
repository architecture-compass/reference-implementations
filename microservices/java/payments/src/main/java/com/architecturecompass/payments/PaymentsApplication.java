package com.architecturecompass.payments;

import java.util.Map;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Payments service (Java): consumes OrderPlaced, captures each order's payment at most
 * once, and publishes PaymentCaptured through its own transactional outbox.
 */
@SpringBootApplication
@EnableScheduling
@RestController
public class PaymentsApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentsApplication.class, args);
    }

    @GetMapping("/health")
    Map<String, String> health() {
        return Map.of("status", "ok");
    }
}
