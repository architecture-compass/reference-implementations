//! Outbox relay: every 500 ms, lock unpublished rows, send them to Kafka, mark them
//! published. If Kafka fails, the transaction rolls back and the rows are retried.

use rdkafka::producer::{FutureProducer, FutureRecord};
use sqlx::{PgPool, Row};
use std::time::Duration;
use uuid::Uuid;

async fn relay_once(db: &PgPool, producer: &FutureProducer) -> Result<(), String> {
    let mut tx = db.begin().await.map_err(|e| e.to_string())?;
    let rows = sqlx::query(
        "SELECT id, topic, aggregate_id, payload::text AS payload FROM outbox
         WHERE published_at IS NULL ORDER BY created_at LIMIT 100 FOR UPDATE SKIP LOCKED",
    )
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;

    let mut ids: Vec<Uuid> = Vec::with_capacity(rows.len());
    for row in &rows {
        let topic: String = row.get("topic");
        let key: String = row.get("aggregate_id");
        let payload: String = row.get("payload");
        producer
            .send(FutureRecord::to(&topic).key(&key).payload(&payload), Duration::from_secs(10))
            .await
            .map_err(|(e, _)| e.to_string())?;
        ids.push(row.get("id"));
    }
    if !ids.is_empty() {
        sqlx::query("UPDATE outbox SET published_at = now() WHERE id = ANY($1)")
            .bind(&ids)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
    }
    tx.commit().await.map_err(|e| e.to_string())
}

pub async fn relay(db: PgPool, producer: FutureProducer) {
    let mut ticker = tokio::time::interval(Duration::from_millis(500));
    loop {
        ticker.tick().await;
        if let Err(e) = relay_once(&db, &producer).await {
            tracing::error!(error = %e, "outbox relay failed; rows stay unpublished and are retried");
        }
    }
}
