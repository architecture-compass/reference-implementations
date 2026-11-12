//! Payments service (Rust): consumes OrderPlaced, captures each order's payment at most
//! once, and publishes PaymentCaptured through its own transactional outbox.

mod outbox;

use axum::{routing::get, Json, Router};
use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    producer::FutureProducer,
    ClientConfig, Message,
};
use rust_decimal::Decimal;
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{str::FromStr, time::Duration};
use uuid::Uuid;

const TOPIC_ORDER_PLACED: &str = "orders.order-placed";
const TOPIC_PAYMENT_CAPTURED: &str = "payments.payment-captured";

/// Capture the payment and record the event in one transaction.
/// A redelivered event, or a second event for the same order, changes nothing.
async fn handle(db: &PgPool, payload: &[u8]) -> sqlx::Result<()> {
    let Ok(evt) = serde_json::from_slice::<serde_json::Value>(payload) else {
        tracing::error!("skipping malformed event");
        return Ok(());
    };
    let parsed = (
        evt["eventId"].as_str().and_then(|s| Uuid::parse_str(s).ok()),
        evt["orderId"].as_str().and_then(|s| Uuid::parse_str(s).ok()),
        evt["amount"].as_str().and_then(|s| Decimal::from_str(s).ok()),
    );
    let (Some(event_id), Some(order_id), Some(amount)) = parsed else {
        tracing::error!("skipping malformed event");
        return Ok(());
    };

    let mut tx = db.begin().await?;
    let first = sqlx::query("INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING")
        .bind(event_id)
        .execute(&mut *tx)
        .await?;
    if first.rows_affected() == 1 {
        let payment_id = Uuid::new_v4();
        let captured = sqlx::query(
            "INSERT INTO payments (id, order_id, amount, status) VALUES ($1, $2, $3, 'CAPTURED')
             ON CONFLICT (order_id) DO NOTHING",
        )
        .bind(payment_id)
        .bind(order_id)
        .bind(amount)
        .execute(&mut *tx)
        .await?;
        if captured.rows_affected() == 1 {
            // A real service would call the payment provider here, using the
            // order ID as the provider's idempotency key.
            let event = json!({
                "eventId": Uuid::new_v4().to_string(),
                "eventType": "PaymentCaptured",
                "orderId": order_id.to_string(),
                "paymentId": payment_id.to_string(),
                "amount": format!("{amount:.2}"),
                "occurredAt": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            });
            sqlx::query(
                "INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, topic, payload)
                 VALUES ($1, 'Payment', $2, 'PaymentCaptured', $3, $4::jsonb)",
            )
            .bind(Uuid::new_v4())
            .bind(order_id.to_string())
            .bind(TOPIC_PAYMENT_CAPTURED)
            .bind(event.to_string())
            .execute(&mut *tx)
            .await?;
        }
    }
    tx.commit().await
}

async fn consume(db: PgPool, consumer: StreamConsumer) {
    loop {
        match consumer.recv().await {
            Err(e) => tracing::error!(error = %e, "kafka receive failed"),
            Ok(msg) => {
                let payload = msg.payload().unwrap_or_default();
                // Retry until the database accepts it; only then commit the offset.
                while let Err(e) = handle(&db, payload).await {
                    tracing::error!(error = %e, "processing failed, retrying");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                if let Err(e) = consumer.commit_message(&msg, CommitMode::Sync) {
                    tracing::error!(error = %e, "offset commit failed");
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect(&std::env::var("DATABASE_URL").expect("DATABASE_URL"))
        .await
        .expect("database");
    let brokers = std::env::var("KAFKA_BROKERS").expect("KAFKA_BROKERS");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .create()
        .expect("kafka producer");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "payments")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("kafka consumer");
    consumer.subscribe(&[TOPIC_ORDER_PLACED]).expect("subscribe");

    tokio::spawn(outbox::relay(db.clone(), producer));
    tokio::spawn(consume(db, consumer));

    let app = Router::new().route("/health", get(|| async { Json(json!({ "status": "ok" })) }));
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await.expect("bind");
    tracing::info!(port, "payments service listening");
    axum::serve(listener, app).await.expect("server");
}
