#!/usr/bin/env bash
# Checks ingestion, grounded answers with citations, permissions and refusal.
#   docker compose --profile python up -d --build
#   ./demo.sh
set -uo pipefail
cd "$(dirname "$0")"

API=${API_URL:-http://localhost:8090}
pass=0; fail=0
ok()  { echo "  PASS  $1"; pass=$((pass+1)); }
bad() { echo "  FAIL  $1"; fail=$((fail+1)); }
num() { sed -n "s/.*\"$1\" *: *\([0-9]*\).*/\1/p"; }
ask() { curl -s --max-time 180 -X POST "$API/ask" -H 'Content-Type: application/json' \
          -d "{\"question\":\"$1\",\"groups\":[$2]}"; }

echo "Waiting for the API at $API ..."
for i in $(seq 1 120); do curl -sf "$API/health" >/dev/null && break; sleep 2; done

echo
echo "1. Ingest the policy documents"
r=$(curl -s --max-time 300 -X POST "$API/ingest"); echo "     $r"
[ "$(echo "$r" | num documents)" = 5 ] && [ "$(echo "$r" | num chunks)" -ge 15 ] \
  && ok "5 documents chunked" || bad "expected 5 documents and at least 15 chunks"

echo "2. Ingest again: nothing changed, nothing re-embedded"
r=$(curl -s --max-time 300 -X POST "$API/ingest"); echo "     $r"
[ "$(echo "$r" | num embedded)" = 0 ] && ok "0 chunks re-embedded" || bad "expected 0 re-embedded"

echo "3. Answer with a citation"
r=$(ask "How many weeks of parental leave do employees get?" '"all-staff"'); echo "     $r" | cut -c1-240
echo "$r" | grep -q '"found":true' && echo "$r" | grep -q '"documentId":"parental-leave"' \
  && ok "found, cites parental-leave" || bad "expected an answer citing parental-leave"
echo "$r" | grep -q "16" && ok "answer mentions 16 weeks" || bad "expected the answer to mention 16 weeks"

echo "4. Permissions: staff cannot retrieve manager-only documents"
r=$(ask "What is the merit increase budget for my team?" '"all-staff"'); echo "     $r" | cut -c1-240
echo "$r" | grep -q 'manager-compensation-guidelines' \
  && bad "a manager-only document was cited for a staff user" || ok "manager-only document not cited for staff"

echo "5. The same question from a manager"
r=$(ask "What is the merit increase budget for my team?" '"all-staff","managers"'); echo "     $r" | cut -c1-240
echo "$r" | grep -q '"documentId":"manager-compensation-guidelines"' \
  && ok "manager sees manager-compensation-guidelines" || bad "expected a citation of manager-compensation-guidelines"

echo "6. Refuse questions the documents can't answer"
r=$(ask "What is the capital of Australia?" '"all-staff"'); echo "     $r" | cut -c1-240
echo "$r" | grep -q '"found":false' && ok "not found, no made-up answer" || bad "expected found:false"

echo
echo "Result: $pass passed, $fail failed"
[ "$fail" = 0 ]
