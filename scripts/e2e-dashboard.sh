#!/usr/bin/env bash
set -euo pipefail

# End-to-end check: real Go API + real Next.js dashboard, no mock backend.
# Requires a completed dashboard build (npm --prefix dashboard run build).

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
API_PORT="${AQS_E2E_API_PORT:-18080}"
DASH_PORT="${AQS_E2E_DASH_PORT:-13000}"
TOKEN="e2e-$(date +%s)-token"
PASSWORD="e2e-$(date +%s)-password"
TMP="$(mktemp -d)"
API_PID=""
DASH_PID=""

cleanup() {
  if [[ -n "$DASH_PID" ]]; then kill "$DASH_PID" 2>/dev/null || true; fi
  if [[ -n "$API_PID" ]]; then kill "$API_PID" 2>/dev/null || true; fi
  wait "$DASH_PID" 2>/dev/null || true
  wait "$API_PID" 2>/dev/null || true
  rm -rf "$TMP"
}
trap cleanup EXIT

if [[ ! -d "$ROOT/dashboard/.next" ]]; then
  echo "dashboard build missing; run: npm --prefix dashboard run build" >&2
  exit 1
fi

cd "$ROOT"
go build -o "$TMP/sched" ./cmd/sched

AQS_DB="$TMP/aqs.db" AQS_API_TOKEN="$TOKEN" \
  "$TMP/sched" serve --listen "127.0.0.1:$API_PORT" >"$TMP/api.log" 2>&1 &
API_PID=$!

(
  cd "$ROOT/dashboard"
  AQS_API_URL="http://127.0.0.1:$API_PORT" AQS_API_TOKEN="$TOKEN" \
    DASHBOARD_PASSWORD="$PASSWORD" \
    exec node node_modules/next/dist/bin/next start -H 127.0.0.1 -p "$DASH_PORT" >"$TMP/dashboard.log" 2>&1
) &
DASH_PID=$!

for _ in $(seq 1 120); do
  if curl -fsS -u "admin:$PASSWORD" "http://127.0.0.1:$DASH_PORT/" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
if ! curl -fsS -u "admin:$PASSWORD" "http://127.0.0.1:$DASH_PORT/" | grep -q "Keep work moving"; then
  echo "dashboard did not become ready:" >&2
  cat "$TMP/dashboard.log" >&2
  exit 1
fi

JOB_ID="$(curl -fsS -u "admin:$PASSWORD" \
  -H "Origin: http://127.0.0.1:$DASH_PORT" \
  -H "Content-Type: application/json" \
  -d '{"name":"dashboard-e2e","schedule":"@every 1m","command":"echo hi"}' \
  "http://127.0.0.1:$DASH_PORT/api/scheduler/jobs" |
  node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s).id))')"

COUNT="$(curl -fsS -u "admin:$PASSWORD" \
  "http://127.0.0.1:$DASH_PORT/api/scheduler/jobs?limit=2" |
  node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const d=JSON.parse(s);console.log(Array.isArray(d)?d.length:-1)})')"
if [[ "$COUNT" != "1" ]]; then
  echo "unexpected job count: $COUNT" >&2
  exit 1
fi

curl -fsS -u "admin:$PASSWORD" \
  -H "Origin: http://127.0.0.1:$DASH_PORT" \
  -X DELETE "http://127.0.0.1:$DASH_PORT/api/scheduler/jobs/$JOB_ID" -o /dev/null

echo "dashboard E2E passed (job $JOB_ID)"
