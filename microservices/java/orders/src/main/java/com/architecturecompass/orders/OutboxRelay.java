package com.architecturecompass.orders;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Every 500 ms: lock unpublished outbox rows, send them to Kafka, mark them published.
 * If Kafka fails, the transaction rolls back and the rows are retried next time.
 */
@Component
class OutboxRelay {

    private record Row(UUID id, String topic, String key, String payload) {}

    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafka;

    OutboxRelay(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka) {
        this.jdbc = jdbc;
        this.kafka = kafka;
    }

    @Scheduled(fixedDelay = 500)
    @Transactional
    public void relay() throws Exception {
        List<Row> rows = jdbc.query("""
                SELECT id, topic, aggregate_id, payload::text FROM outbox
                WHERE published_at IS NULL ORDER BY created_at LIMIT 100
                FOR UPDATE SKIP LOCKED""",
                (rs, i) -> new Row(rs.getObject(1, UUID.class), rs.getString(2), rs.getString(3), rs.getString(4)));
        for (Row row : rows) {
            kafka.send(row.topic(), row.key(), row.payload()).get(10, TimeUnit.SECONDS);
            jdbc.update("UPDATE outbox SET published_at = now() WHERE id = ?", row.id());
        }
    }
}
