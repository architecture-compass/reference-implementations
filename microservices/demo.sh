#!/usr/bin/env bash
# Proves the five required behaviors against whichever language is running.
#   docker compose --profile go up -d --build
#   ./demo.sh
set -uo pipefail
cd "$(dirname "$0")"

ORDERS=${ORDERS_URL:-http://localhost:8081}
pass=0; fail=0
ok()   { echo "  PASS  $1"; pass=$((pass+1)); }
bad()  { echo "  FAIL  $1"; fail=$((fail+1)); }
field(){ sed -n "s/.*\"$1\" *: *\"\{0,1\}\([^\",}]*\)\"\{0,1\}.*/\1/p"; }
psql_payments(){ docker compose exec -T postgres psql -U app -d payments -tAc "$1" | tr -d '[:space:]'; }
place(){ curl -s -o /tmp/demo_body -w '%{http_code}' -X POST "$ORDERS/orders" \
          -H 'Content-Type: application/json' -H "Idempotency-Key: $1" \
          -d "{\"customerId\":\"c-42\",\"sku\":\"$2\",\"quantity\":$3}"; }

echo "Waiting for the Orders service at $ORDERS ..."
for i in $(seq 1 90); do curl -sf "$ORDERS/health" >/dev/null && break; sleep 2; done

run=$(date +%s%N)
echo
echo "1. Place an order"
code=$(place "key-$run-a" SKU-1 2); id=$(field orderId < /tmp/demo_body); amount=$(field amount < /tmp/demo_body)
[ "$code" = 202 ] && [ -n "$id" ] && ok "202 Accepted, order $id" || bad "expected 202 with an orderId, got $code: $(cat /tmp/demo_body)"
[ "$amount" = "59.98" ] && ok "amount 59.98 (2 x 29.99 from the catalog)" || bad "expected amount 59.98, got '$amount'"

echo "2. Repeat the request with the same Idempotency-Key"
code=$(place "key-$run-a" SKU-1 2); id2=$(field orderId < /tmp/demo_body)
[ "$id2" = "$id" ] && ok "same order returned, nothing duplicated" || bad "expected order $id, got '$id2' ($code)"

echo "3. Outbox -> Kafka -> Payments -> Kafka -> Orders"
status=""
for i in $(seq 1 30); do status=$(curl -s "$ORDERS/orders/$id" | field status); [ "$status" = CONFIRMED ] && break; sleep 1; done
[ "$status" = CONFIRMED ] && ok "order CONFIRMED after payment captured" || bad "order still '$status' after 30s"

echo "4. Redeliver the same OrderPlaced event (simulating an at-least-once duplicate)"
event=$(docker compose exec -T postgres psql -U app -d orders -tAc \
  "SELECT payload::text FROM outbox WHERE aggregate_id = '$id' AND event_type = 'OrderPlaced'")
echo "$id|$event" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic orders.order-placed \
  --property parse.key=true --property key.separator='|' >/dev/null 2>&1
sleep 5
n=$(psql_payments "SELECT count(*) FROM payments WHERE order_id = '$id'")
[ "$n" = 1 ] && ok "still exactly 1 payment for the order" || bad "expected 1 payment, found $n"

echo "5. Stop the catalog: cached prices keep orders flowing"
docker compose stop catalog >/dev/null 2>&1
code=$(place "key-$run-b" SKU-1 1); a=$(field amount < /tmp/demo_body)
[ "$code" = 202 ] && [ "$a" = "29.99" ] && ok "SKU-1 accepted at last known price 29.99" || bad "expected 202 at 29.99, got $code: $(cat /tmp/demo_body)"
code=$(place "key-$run-c" SKU-3 1)
[ "$code" = 503 ] && ok "SKU-3 (never priced) rejected with 503" || bad "expected 503 for an unpriced SKU, got $code"
docker compose start catalog >/dev/null 2>&1

echo
echo "Result: $pass passed, $fail failed"
[ "$fail" = 0 ]
