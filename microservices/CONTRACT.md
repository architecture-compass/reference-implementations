# Microservices contract

Every language implementation exposes exactly this API and these events, so any Orders service works with any Payments service.

## Orders service (port 8080 in the container, 8081 on your machine)

### `POST /orders`

Header `Idempotency-Key` is **required**. Repeating a request with the same key returns the original order instead of creating a new one.

```json
{ "customerId": "c-42", "sku": "SKU-1", "quantity": 2 }
```

Response `202 Accepted`:

```json
{ "orderId": "7f9e…", "status": "PENDING", "amount": "59.98", "currency": "USD" }
```

| Status | When |
|---|---|
| `202` | Order accepted (new, or an earlier order with the same key) |
| `400` | Missing `Idempotency-Key`, or invalid body |
| `404` | Unknown SKU |
| `503` | Catalog unavailable and no cached price for this SKU |

### `GET /orders/{orderId}`

`200` with the same shape as above (`status` becomes `CONFIRMED` once payment is captured), or `404`.

### `GET /health`

`200 {"status":"ok"}`

## Payments service (port 8080 in the container)

`GET /health` only. All of its work happens through events.

## Catalog (stub, port 8083 on your machine)

`GET /prices/{sku}` → `200 {"sku":"SKU-1","price":"29.99","currency":"USD"}` or `404`.

Orders calls it with a **500 ms timeout**, up to **2 retries**, and a **circuit breaker** (opens after 3 consecutive failures, retries after 10 seconds). When the call fails, Orders falls back to the last price it saw for that SKU.

## Events

Both topics have 3 partitions. The message **key is the `orderId`**, so all events for one order stay in order. Amounts are decimal **strings** so no language rounds them as floating point.

### `orders.order-placed` (published by Orders)

```json
{
  "eventId": "b1c2…",
  "eventType": "OrderPlaced",
  "orderId": "7f9e…",
  "customerId": "c-42",
  "amount": "59.98",
  "currency": "USD",
  "occurredAt": "2026-11-12T10:15:00Z"
}
```

### `payments.payment-captured` (published by Payments)

```json
{
  "eventId": "d4e5…",
  "eventType": "PaymentCaptured",
  "orderId": "7f9e…",
  "paymentId": "a8b9…",
  "amount": "59.98",
  "occurredAt": "2026-11-12T10:15:01Z"
}
```

## Database

Both services use PostgreSQL. The schema in [`infra/postgres/init.sql`](infra/postgres/init.sql) is shared by every language.

## Required behavior

Every implementation must:

1. Write the business change and its event to the **outbox in one transaction**.
2. Publish outbox rows with a **relay** that polls every 500 ms (`FOR UPDATE SKIP LOCKED`) and marks rows published.
3. Make every consumer **idempotent**: record `eventId` in `processed_events` in the same transaction as the change.
4. Capture each order's payment **at most once**, even if `OrderPlaced` is delivered twice.
5. Protect the catalog call with **timeout, retries and a circuit breaker**, falling back to the last known price.

[`demo.sh`](demo.sh) checks all five.
