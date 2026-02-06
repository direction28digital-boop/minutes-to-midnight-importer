#!/usr/bin/env bash
set -euo pipefail

# Always run from repo root
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Load env vars from .env.local if present
if [ -f ".env.local" ]; then
  set -a
  source ".env.local"
  set +a
fi

# Defaults (override per run: DAILY_CAP=50 DRY_RUN=1 ...)
DAILY_CAP="${DAILY_CAP:-200}"
DRY_RUN="${DRY_RUN:-0}"

VENV_PY="$ROOT/.venv-events/bin/python"

if [ ! -x "$VENV_PY" ]; then
  echo "Missing venv python at: $VENV_PY"
  echo "Create it with: python3 -m venv .venv-events && ./.venv-events/bin/python -m pip install requests"
  exit 1
fi

if [ -z "${SERPAPI_API_KEY:-}" ]; then
  echo "Missing SERPAPI_API_KEY in environment (.env.local)."
  exit 1
fi

remain () {
  "$VENV_PY" - <<'PY'
import os, requests
key = os.environ["SERPAPI_API_KEY"]
r = requests.get("https://serpapi.com/account.json", params={"api_key": key}, timeout=30)
r.raise_for_status()
print(int(r.json().get("total_searches_left") or 0))
PY
}

# Configurable knobs (override if you want)
HUBS_FILE="${HUBS_FILE:-data_events/dataRan/hubs.all.from_app.json}"
TERMS="${TERMS:-dog friendly events,dog adoption event}"
DATE_FILTERS="${DATE_FILTERS:-date:week,date:next_week,date:month,date:next_month}"
STARTS="${STARTS:-0}"
THROTTLE="${THROTTLE:-4.0}"
CONCURRENCY="${CONCURRENCY:-1}"

R_BEFORE="$(remain)"
RUN_NOW=$(( R_BEFORE < DAILY_CAP ? R_BEFORE : DAILY_CAP ))

echo "remaining_before=$R_BEFORE daily_cap=$DAILY_CAP running_now=$RUN_NOW dry_run=$DRY_RUN"

# If no credits, exit cleanly (even in DRY_RUN)
if [ "$RUN_NOW" -le 0 ]; then
  echo "No SerpAPI credits left."
  exit 0
fi

# DRY RUN: prove the cap logic without spending
if [ "$DRY_RUN" = "1" ]; then
  echo "DRY_RUN=1 — would run burn script with max-searches=$RUN_NOW"
  exit 0
fi

mkdir -p "$ROOT/data_events/raw/monthly"
MONTH="$(date +%Y-%m)"
OUT_FILE="$ROOT/data_events/raw/monthly/google_events.hubs_all.${MONTH}.jsonl"

# Log a manifest so you can audit usage later
mkdir -p "$ROOT/data_events/dataRan/serpapi_runs"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
MANIFEST="$ROOT/data_events/dataRan/serpapi_runs/${RUN_ID}.json"

"$VENV_PY" - <<PY
import json, os
payload = {
  "runId": "${RUN_ID}",
  "remainingBefore": ${R_BEFORE},
  "dailyCap": ${DAILY_CAP},
  "runningNow": ${RUN_NOW},
  "hubsFile": "${HUBS_FILE}",
  "terms": "${TERMS}",
  "dateFilters": "${DATE_FILTERS}",
  "starts": "${STARTS}",
  "throttleSeconds": float("${THROTTLE}"),
  "concurrency": int("${CONCURRENCY}"),
  "outFile": "${OUT_FILE}",
}
with open("${MANIFEST}", "w", encoding="utf-8") as f:
  json.dump(payload, f, ensure_ascii=False, indent=2)
print("wrote_manifest:", "${MANIFEST}")
PY

# Run the burn script (this is the only thing that spends credits)
"$VENV_PY" scripts/serpapi_google_events_burn.py \
  --hubs "$HUBS_FILE" \
  --out "$OUT_FILE" \
  --max-searches "$RUN_NOW" \
  --terms "$TERMS" \
  --date-filters "$DATE_FILTERS" \
  --starts "$STARTS" \
  --concurrency "$CONCURRENCY" \
  --throttle "$THROTTLE" \
  --resume

R_AFTER="$(remain)"
echo "remaining_after=$R_AFTER"
