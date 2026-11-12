"""Orders service (Python): accepts orders, writes them with an OrderPlaced event to the
transactional outbox, relays the outbox to Kafka, and confirms orders when
PaymentCaptured arrives. Catalog calls use timeout, retries and a circuit breaker
with a last-known-price fallback."""

import json
import logging
import os
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal

import httpx
import pybreaker
from confluent_kafka import Consumer, Producer
from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse
from psycopg import errors
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

log = logging.getLogger("orders")
logging.basicConfig(level=logging.INFO)

TOPIC_ORDER_PLACED = "orders.order-placed"
TOPIC_PAYMENT_CAPTURED = "payments.payment-captured"

pool = ConnectionPool(os.environ["DATABASE_URL"], min_size=1, max_size=10, open=False)
brokers = os.environ["KAFKA_BROKERS"]
stop = threading.Event()


class OrderRequest(BaseModel):
    customerId: str = Field(min_length=1)
    sku: str = Field(min_length=1)
    quantity: int = Field(gt=0)


# ---------- Catalog client: timeout + retries + circuit breaker + fallback ----------

class UnknownSku(Exception):
    pass


class CatalogUnavailable(Exception):
    pass


class Catalog:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.http = httpx.Client(timeout=0.5)
        # Opens after 3 consecutive failures, tries again after 10 seconds.
        # An unknown SKU is a valid answer, not a catalog failure.
        self.breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=10, exclude=[UnknownSku])
        self.cache: dict[str, dict] = {}
        self.lock = threading.Lock()

    def _fetch(self, sku: str) -> dict:
        resp = self.http.get(f"{self.base_url}/prices/{sku}")
        if resp.status_code == 404:
            raise UnknownSku(sku)
        resp.raise_for_status()
        return resp.json()

    def price(self, sku: str) -> dict:
        last_error: Exception | None = None
        for _ in range(3):  # 1 call + 2 retries
            try:
                p = self.breaker.call(self._fetch, sku)
                with self.lock:
                    self.cache[sku] = p
                return p
            except UnknownSku:
                raise
            except pybreaker.CircuitBreakerError as e:
                last_error = e
                break  # don't hammer an open breaker
            except Exception as e:  # timeout, connection refused, 5xx
                last_error = e
                time.sleep(0.1)
        with self.lock:
            if sku in self.cache:
                log.warning("catalog unavailable, using last known price for %s: %s", sku, last_error)
                return self.cache[sku]
        raise CatalogUnavailable(str(last_error))


catalog = Catalog(os.environ["CATALOG_URL"])


# ---------- Outbox relay ----------

def relay_outbox() -> None:
    producer = Producer({"bootstrap.servers": brokers, "acks": "all", "enable.idempotence": True})
    while not stop.is_set():
        try:
            with pool.connection() as conn, conn.transaction():
                rows = conn.execute(
                    """SELECT id, topic, aggregate_id, payload::text FROM outbox
                       WHERE published_at IS NULL ORDER BY created_at LIMIT 100
                       FOR UPDATE SKIP LOCKED"""
                ).fetchall()
                if rows:
                    for _, topic, key, payload in rows:
                        producer.produce(topic, key=key, value=payload)
                    if producer.flush(10) > 0:
                        raise RuntimeError("Kafka did not acknowledge all messages")
                    conn.execute(
                        "UPDATE outbox SET published_at = now() WHERE id = ANY(%s)",
                        ([r[0] for r in rows],),
                    )
        except Exception:
            log.exception("outbox relay failed; rows stay unpublished and are retried")
        stop.wait(0.5)


# ---------- PaymentCaptured consumer (idempotent) ----------

def consume_payments() -> None:
    consumer = Consumer({
        "bootstrap.servers": brokers,
        "group.id": "orders",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_PAYMENT_CAPTURED])
    while not stop.is_set():
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        try:
            evt = json.loads(msg.value())
            with pool.connection() as conn, conn.transaction():
                first_time = conn.execute(
                    "INSERT INTO processed_events (event_id) VALUES (%s) ON CONFLICT DO NOTHING",
                    (evt["eventId"],),
                ).rowcount
                if first_time:
                    conn.execute(
                        "UPDATE orders SET status = 'CONFIRMED', updated_at = now() WHERE id = %s",
                        (evt["orderId"],),
                    )
            consumer.commit(message=msg, asynchronous=False)
        except (ValueError, KeyError):
            log.error("skipping malformed event")
            consumer.commit(message=msg, asynchronous=False)
        except Exception:
            log.exception("processing failed; message will be redelivered")
            consumer.seek(msg)  # not committed: reprocess this message
            time.sleep(1)
    consumer.close()


@asynccontextmanager
async def lifespan(_: FastAPI):
    pool.open(wait=True)
    workers = [threading.Thread(target=f, daemon=True) for f in (relay_outbox, consume_payments)]
    for w in workers:
        w.start()
    yield
    stop.set()
    pool.close()


app = FastAPI(title="Orders", lifespan=lifespan)


def view(row) -> dict:
    order_id, status, amount, currency = row
    return {"orderId": str(order_id), "status": status, "amount": f"{amount:.2f}", "currency": currency}


def find_by_key(key: str):
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT id, status, amount, currency FROM orders WHERE idempotency_key = %s", (key,)
        ).fetchone()
    return view(row) if row else None


@app.post("/orders")
def place_order(req: OrderRequest, idempotency_key: str | None = Header(default=None)):
    if not idempotency_key:
        return JSONResponse({"error": "Idempotency-Key header is required"}, status_code=400)

    if existing := find_by_key(idempotency_key):  # same key seen before
        return JSONResponse(existing, status_code=202)

    try:
        price = catalog.price(req.sku)
    except UnknownSku:
        return JSONResponse({"error": "unknown sku"}, status_code=404)
    except CatalogUnavailable as e:
        return JSONResponse({"error": f"catalog unavailable: {e}"}, status_code=503)

    amount = (Decimal(price["price"]) * req.quantity).quantize(Decimal("0.01"))
    order_id = uuid.uuid4()
    event = {
        "eventId": str(uuid.uuid4()),
        "eventType": "OrderPlaced",
        "orderId": str(order_id),
        "customerId": req.customerId,
        "amount": f"{amount:.2f}",
        "currency": price["currency"],
        "occurredAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    try:
        # The order and its event commit together, or not at all.
        with pool.connection() as conn, conn.transaction():
            conn.execute(
                """INSERT INTO orders (id, customer_id, sku, quantity, amount, currency, status, idempotency_key)
                   VALUES (%s, %s, %s, %s, %s, %s, 'PENDING', %s)""",
                (order_id, req.customerId, req.sku, req.quantity, amount, price["currency"], idempotency_key),
            )
            conn.execute(
                """INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, topic, payload)
                   VALUES (%s, 'Order', %s, 'OrderPlaced', %s, %s)""",
                (uuid.uuid4(), str(order_id), TOPIC_ORDER_PLACED, json.dumps(event)),
            )
    except errors.UniqueViolation:  # concurrent request with the same key
        return JSONResponse(find_by_key(idempotency_key), status_code=202)

    return JSONResponse(
        {"orderId": str(order_id), "status": "PENDING", "amount": f"{amount:.2f}", "currency": price["currency"]},
        status_code=202,
    )


@app.get("/orders/{order_id}")
def get_order(order_id: str):
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT id, status, amount, currency FROM orders WHERE id::text = %s", (order_id,)
        ).fetchone()
    if not row:
        return JSONResponse({"error": "order not found"}, status_code=404)
    return view(row)


@app.get("/health")
def health():
    return {"status": "ok"}
