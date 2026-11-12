//! Orders service (Rust): accepts orders, writes them with an OrderPlaced event to the
//! transactional outbox, relays the outbox to Kafka, and confirms orders when
//! PaymentCaptured arrives. Catalog calls use timeout, retries and a circuit breaker
//! with a last-known-price fallback.

mod breaker;
mod outbox;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use breaker::CircuitBreaker;
use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    producer::FutureProducer,
    ClientConfig, Message,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use uuid::Uuid;

const TOPIC_ORDER_PLACED: &str = "orders.order-placed";
const TOPIC_PAYMENT_CAPTURED: &str = "payments.payment-captured";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderRequest {
    customer_id: Option<String>,
    sku: Option<String>,
    quantity: Option<i32>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OrderView {
    order_id: String,
    status: String,
    amount: String,
    currency: String,
}

#[derive(Clone, Deserialize)]
struct Price {
    price: String,
    currency: String,
}

// ---------- Catalog client: timeout + retries + circuit breaker + fallback ----------

enum CatalogError {
    UnknownSku,
    Unavailable(String),
}

struct Catalog {
    base_url: String,
    http: reqwest::Client,
    breaker: CircuitBreaker,
    cache: RwLock<HashMap<String, Price>>,
}

impl Catalog {
    fn new(base_url: String) -> Self {
        Self {
            base_url,
            http: reqwest::Client::builder().timeout(Duration::from_millis(500)).build().unwrap(),
            breaker: CircuitBreaker::new(3, Duration::from_secs(10)),
            cache: RwLock::new(HashMap::new()),
        }
    }

    async fn fetch(&self, sku: &str) -> Result<Price, CatalogError> {
        let resp = self
            .http
            .get(format!("{}/prices/{}", self.base_url, sku))
            .send()
            .await
            .map_err(|e| CatalogError::Unavailable(e.to_string()))?;
        match resp.status() {
            reqwest::StatusCode::NOT_FOUND => Err(CatalogError::UnknownSku),
            s if s.is_success() => resp.json().await.map_err(|e| CatalogError::Unavailable(e.to_string())),
            s => Err(CatalogError::Unavailable(format!("catalog returned {s}"))),
        }
    }

    async fn price(&self, sku: &str) -> Result<Price, CatalogError> {
        let mut last_error = String::new();
        for _ in 0..3 {
            // 1 call + 2 retries
            if !self.breaker.allow() {
                last_error = "circuit breaker is open".into();
                break; // don't hammer an open breaker
            }
            match self.fetch(sku).await {
                Ok(p) => {
                    self.breaker.record_success();
                    self.cache.write().await.insert(sku.to_string(), p.clone());
                    return Ok(p);
                }
                Err(CatalogError::UnknownSku) => {
                    self.breaker.record_success(); // a correct answer, not a failure
                    return Err(CatalogError::UnknownSku);
                }
                Err(CatalogError::Unavailable(e)) => {
                    self.breaker.record_failure();
                    last_error = e;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
        if let Some(p) = self.cache.read().await.get(sku) {
            tracing::warn!(sku, error = %last_error, "catalog unavailable, using last known price");
            return Ok(p.clone());
        }
        Err(CatalogError::Unavailable(last_error))
    }
}

// ---------- HTTP API ----------

struct AppState {
    db: PgPool,
    catalog: Catalog,
}

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({ "error": message.into() }))).into_response()
}

fn view(row: &sqlx::postgres::PgRow) -> OrderView {
    let amount: Decimal = row.get("amount");
    OrderView {
        order_id: row.get("id"),
        status: row.get("status"),
        amount: format!("{amount:.2}"),
        currency: row.get("currency"),
    }
}

async fn find_by_key(db: &PgPool, key: &str) -> sqlx::Result<Option<OrderView>> {
    let row = sqlx::query("SELECT id::text AS id, status, amount, currency FROM orders WHERE idempotency_key = $1")
        .bind(key)
        .fetch_optional(db)
        .await?;
    Ok(row.as_ref().map(view))
}

async fn place_order(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Option<Json<OrderRequest>>,
) -> Response {
    let Some(key) = headers.get("idempotency-key").and_then(|v| v.to_str().ok()).filter(|k| !k.is_empty()) else {
        return error(StatusCode::BAD_REQUEST, "Idempotency-Key header is required");
    };
    let (customer_id, sku, quantity) = match body {
        Some(Json(OrderRequest { customer_id: Some(c), sku: Some(s), quantity: Some(q) }))
            if !c.is_empty() && !s.is_empty() && q > 0 => (c, s, q),
        _ => return error(StatusCode::BAD_REQUEST, "customerId, sku and a positive quantity are required"),
    };

    match find_by_key(&state.db, key).await {
        Ok(Some(existing)) => return (StatusCode::ACCEPTED, Json(existing)).into_response(), // same key seen before
        Ok(None) => {}
        Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }

    let price = match state.catalog.price(&sku).await {
        Ok(p) => p,
        Err(CatalogError::UnknownSku) => return error(StatusCode::NOT_FOUND, "unknown sku"),
        Err(CatalogError::Unavailable(e)) => {
            return error(StatusCode::SERVICE_UNAVAILABLE, format!("catalog unavailable: {e}"))
        }
    };
    let Ok(unit) = Decimal::from_str(&price.price) else {
        return error(StatusCode::BAD_GATEWAY, "invalid price from catalog");
    };
    let amount = (unit * Decimal::from(quantity)).round_dp(2);
    let amount_str = format!("{amount:.2}");
    let order_id = Uuid::new_v4();
    let event = json!({
        "eventId": Uuid::new_v4().to_string(),
        "eventType": "OrderPlaced",
        "orderId": order_id.to_string(),
        "customerId": customer_id,
        "amount": amount_str,
        "currency": price.currency,
        "occurredAt": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
    });

    // The order and its event commit together, or not at all.
    let result: sqlx::Result<()> = async {
        let mut tx = state.db.begin().await?;
        sqlx::query(
            "INSERT INTO orders (id, customer_id, sku, quantity, amount, currency, status, idempotency_key)
             VALUES ($1, $2, $3, $4, $5, $6, 'PENDING', $7)",
        )
        .bind(order_id).bind(&customer_id).bind(&sku).bind(quantity).bind(amount).bind(&price.currency).bind(key)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, topic, payload)
             VALUES ($1, 'Order', $2, 'OrderPlaced', $3, $4::jsonb)",
        )
        .bind(Uuid::new_v4()).bind(order_id.to_string()).bind(TOPIC_ORDER_PLACED).bind(event.to_string())
        .execute(&mut *tx)
        .await?;
        tx.commit().await
    }
    .await;

    match result {
        Ok(()) => (
            StatusCode::ACCEPTED,
            Json(OrderView { order_id: order_id.to_string(), status: "PENDING".into(), amount: amount_str, currency: price.currency }),
        )
            .into_response(),
        // Concurrent request with the same key.
        Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("23505") => {
            match find_by_key(&state.db, key).await {
                Ok(Some(existing)) => (StatusCode::ACCEPTED, Json(existing)).into_response(),
                _ => error(StatusCode::INTERNAL_SERVER_ERROR, "conflict"),
            }
        }
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn get_order(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    let row = sqlx::query("SELECT id::text AS id, status, amount, currency FROM orders WHERE id::text = $1")
        .bind(id)
        .fetch_optional(&state.db)
        .await;
    match row {
        Ok(Some(r)) => Json(view(&r)).into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, "order not found"),
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

// ---------- PaymentCaptured consumer (idempotent) ----------

async fn confirm(db: &PgPool, payload: &[u8]) -> sqlx::Result<()> {
    let Ok(evt) = serde_json::from_slice::<serde_json::Value>(payload) else {
        tracing::error!("skipping malformed event");
        return Ok(());
    };
    let (Some(event_id), Some(order_id)) = (evt["eventId"].as_str(), evt["orderId"].as_str()) else {
        tracing::error!("skipping malformed event");
        return Ok(());
    };
    let Ok(event_id) = Uuid::parse_str(event_id) else { return Ok(()) };
    let mut tx = db.begin().await?;
    let first = sqlx::query("INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING")
        .bind(event_id)
        .execute(&mut *tx)
        .await?;
    if first.rows_affected() == 1 {
        sqlx::query("UPDATE orders SET status = 'CONFIRMED', updated_at = now() WHERE id::text = $1")
            .bind(order_id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await
}

async fn consume_payments(db: PgPool, consumer: StreamConsumer) {
    loop {
        match consumer.recv().await {
            Err(e) => tracing::error!(error = %e, "kafka receive failed"),
            Ok(msg) => {
                let payload = msg.payload().unwrap_or_default();
                // Retry until the database accepts it; only then commit the offset.
                while let Err(e) = confirm(&db, payload).await {
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
        .max_connections(10)
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
        .set("group.id", "orders")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("kafka consumer");
    consumer.subscribe(&[TOPIC_PAYMENT_CAPTURED]).expect("subscribe");

    tokio::spawn(outbox::relay(db.clone(), producer));
    tokio::spawn(consume_payments(db.clone(), consumer));

    let state = Arc::new(AppState { db, catalog: Catalog::new(std::env::var("CATALOG_URL").expect("CATALOG_URL")) });
    let app = Router::new()
        .route("/orders", post(place_order))
        .route("/orders/{id}", get(get_order))
        .route("/health", get(|| async { Json(json!({ "status": "ok" })) }))
        .with_state(state);

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await.expect("bind");
    tracing::info!(port, "orders service listening");
    axum::serve(listener, app).await.expect("server");
}
