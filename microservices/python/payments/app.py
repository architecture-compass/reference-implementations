"""Payments service (Python): consumes OrderPlaced, captures each order's payment at
most once, and publishes PaymentCaptured through its own transactional outbox."""

import json
import logging
import os
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer
from fastapi import FastAPI
from psycopg_pool import ConnectionPool

log = logging.getLogger("payments")
logging.basicConfig(level=logging.INFO)

TOPIC_ORDER_PLACED = "orders.order-placed"
TOPIC_PAYMENT_CAPTURED = "payments.payment-captured"

pool = ConnectionPool(os.environ["DATABASE_URL"], min_size=1, max_size=5, open=False)
brokers = os.environ["KAFKA_BROKERS"]
stop = threading.Event()


def handle(evt: dict) -> None:
    """Capture the payment and record the event in one transaction.
    A redelivered event, or a second event for the same order, changes nothing."""
    with pool.connection() as conn, conn.transaction():
        first_time = conn.execute(
            "INSERT INTO processed_events (event_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (evt["eventId"],),
        ).rowcount
        if not first_time:
            return  # duplicate delivery
        payment_id = uuid.uuid4()
        captured = conn.execute(
            """INSERT INTO payments (id, order_id, amount, status) VALUES (%s, %s, %s, 'CAPTURED')
               ON CONFLICT (order_id) DO NOTHING""",
            (payment_id, evt["orderId"], evt["amount"]),
        ).rowcount
        if not captured:
            return  # this order was already paid
        # A real service would call the payment provider here, using the
        # order ID as the provider's idempotency key.
        event = {
            "eventId": str(uuid.uuid4()),
            "eventType": "PaymentCaptured",
            "orderId": evt["orderId"],
            "paymentId": str(payment_id),
            "amount": evt["amount"],
            "occurredAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        conn.execute(
            """INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, topic, payload)
               VALUES (%s, 'Payment', %s, 'PaymentCaptured', %s, %s)""",
            (uuid.uuid4(), evt["orderId"], TOPIC_PAYMENT_CAPTURED, json.dumps(event)),
        )


def consume() -> None:
    consumer = Consumer({
        "bootstrap.servers": brokers,
        "group.id": "payments",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_ORDER_PLACED])
    while not stop.is_set():
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        try:
            handle(json.loads(msg.value()))
            consumer.commit(message=msg, asynchronous=False)
        except (ValueError, KeyError):
            log.error("skipping malformed event")
            consumer.commit(message=msg, asynchronous=False)
        except Exception:
            log.exception("processing failed; message will be redelivered")
            consumer.seek(msg)
            time.sleep(1)
    consumer.close()


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


@asynccontextmanager
async def lifespan(_: FastAPI):
    pool.open(wait=True)
    for f in (consume, relay_outbox):
        threading.Thread(target=f, daemon=True).start()
    yield
    stop.set()
    pool.close()


app = FastAPI(title="Payments", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}
