#!/usr/bin/env bash
# End-to-end check of the lakehouse: CDC -> bronze -> silver -> gold, replays, time travel
# and maintenance. Starts from a clean slate every time.
#   ./demo.sh python      # or: ./demo.sh java
set -uo pipefail
cd "$(dirname "$0")"

LANGUAGE=${1:-python}
case "$LANGUAGE" in python|java) ;; *) echo "usage: $0 python|java"; exit 2 ;; esac

pass=0; fail=0
ok()  { echo "  PASS  $1"; pass=$((pass+1)); }
bad() { echo "  FAIL  $1"; fail=$((fail+1)); }
num() { sed -n "s/.*\"$1\": *\([0-9]*\).*/\1/p"; }

pipeline() { docker compose run --rm -T "pipeline-$LANGUAGE" "$@" 2>/dev/null | sed 's/^/     /'; }
check()    { docker compose run --rm -T check "$@" 2>/dev/null; }
sql()      { docker compose exec -T postgres psql -U postgres -d shop -qc "$1" >/dev/null; }
events()   { docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
               --bootstrap-server localhost:9092 --topic shop.public.orders 2>/dev/null \
             | awk -F: '{n += $3} END {print n + 0}'; }
wait_for_events() {
  for _ in $(seq 1 60); do [ "$(events)" -ge "$1" ] && return 0; sleep 2; done
  echo "  timed out waiting for $1 change events"; return 1
}

echo "Starting from a clean slate (all demo data is removed) ..."
docker compose --profile '*' down -v >/dev/null 2>&1
docker compose build -q "pipeline-$LANGUAGE" check
docker compose up -d --wait >/dev/null 2>&1 || docker compose up -d >/dev/null 2>&1

echo
echo "1. Debezium snapshots the 8 existing orders into Kafka; the pipeline loads them"
wait_for_events 8
pipeline run
r=$(check state); echo "     $r" | cut -c1-200
[ "$(echo "$r" | num bronze_rows)" = 8 ] && ok "bronze has 8 change events" || bad "expected 8 bronze rows"
echo "$r" | grep -q '"silver_matches_source": true' && ok "silver matches the source database" || bad "silver differs from source"
echo "$r" | grep -q '"gold_matches_source": true' && ok "gold revenue matches the source database" || bad "gold differs from source"
first_snapshot=$(echo "$r" | num silver_snapshot)

echo "2. Change the source: pay one order, delete one, add one"
sql "UPDATE orders SET status = 'PAID' WHERE order_id = 'o-1006'"
sql "DELETE FROM orders WHERE order_id = 'o-1003'"
sql "INSERT INTO orders VALUES ('o-1009', 'c-7', 'gus@example.com', 'PAID', 75.00, 'USD', now())"
wait_for_events 11
pipeline run
r=$(check state); echo "     $r" | cut -c1-200
[ "$(echo "$r" | num bronze_rows)" = 11 ] && ok "bronze keeps all 11 events (full history)" || bad "expected 11 bronze rows"
echo "$r" | grep -q '"silver_matches_source": true' && ok "silver applied the update, delete and insert" || bad "silver differs from source"
echo "$r" | grep -q '"gold_matches_source": true' && ok "gold revenue updated" || bad "gold differs from source"
second_snapshot=$(echo "$r" | num silver_snapshot)

echo "3. Run again with nothing new: checkpoints prevent reprocessing"
pipeline run
r=$(check state)
[ "$(echo "$r" | num bronze_rows)" = 11 ] && [ "$(echo "$r" | num silver_snapshot)" = "$second_snapshot" ] \
  && ok "no duplicate events, no new silver version" || bad "pipeline reprocessed data"

echo "4. Time travel: read silver as it was after the first run"
before=$(check status-at "$first_snapshot" o-1006)
echo "     o-1006 at snapshot $first_snapshot: $before"
[ "$before" = PENDING ] && ok "old version still readable (o-1006 was PENDING)" || bad "expected PENDING, got $before"

echo "5. Table maintenance: compaction, snapshot expiry, orphan-file cleanup"
pipeline maintain
check state >/dev/null && ok "tables still match the source after maintenance" || bad "maintenance changed the data"

echo
echo "Result: $pass passed, $fail failed"
[ "$fail" = 0 ]
