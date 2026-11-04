#!/usr/bin/env bash


# --- Configuration ---
INPUT_FILE="./analyses_id.json"
STATE_FILE="./processed_ids.txt"
LOGSTASH_URL="http://localhost:5666"
SLEEP_INTERVAL=90  # Sleep in seconds to let Logstash/ES drain events
CURL_TIMEOUT=30

# Create state file if it doesn't exist
touch "$STATE_FILE"

if [[ ! -f "$INPUT_FILE" ]]; then
  echo "[-] Error: Input file $INPUT_FILE not found." >&2
  exit 1
fi

echo "[*] Starting ingestion process..."
echo "[*] Tracking state in: $STATE_FILE"

# Process file line by line
while IFS= read -r line || [[ -n "$line" ]]; do
  # Ignore empty lines
  [[ -z "$line" ]] && continue

  # Extract fields using sed/grep (no jq required)
  STATUS=$(echo "$line" | grep -o '"status"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"status"[[:space:]]*:[[:space:]]*"//;s/"//')
  ANALYSIS_ID=$(echo "$line" | grep -o '"analyses_id"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"analyses_id"[[:space:]]*:[[:space:]]*"//;s/"//')
  
  #check if field was named "analysis_id" instead of "analyses_id"
  if [[ -z "$ANALYSIS_ID" ]]; then
    ANALYSIS_ID=$(echo "$line" | grep -o '"analysis_id"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"analysis_id"[[:space:]]*:[[:space:]]*"//;s/"//')
  fi

  INVESTIGATION_ID=$(echo "$line" | grep -o '"investigation_id"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"investigation_id"[[:space:]]*:[[:space:]]*"//;s/"//')

  if [[ "$STATUS" != "complete" ]]; then
    continue
  fi

  if [[ -z "$ANALYSIS_ID" ]]; then
    continue
  fi

  # Skip if already marked as processed
  if grep -Fxq "$ANALYSIS_ID" "$STATE_FILE"; then
    echo "[~] Skipping already processed: $ANALYSIS_ID"
    continue
  fi

  echo "[+] Ingesting Analysis: $ANALYSIS_ID (Investigation: $INVESTIGATION_ID)..."

  # Send HTTP request and capture HTTP status code
  HTTP_STATUS=$(curl -s -m "$CURL_TIMEOUT" -o /dev/null -w "%{http_code}" \
    -X POST "$LOGSTASH_URL" \
    -H "Content-Type: application/json" \
    -d "{\"analysis_id\": \"$ANALYSIS_ID\", \"investigation_id\": \"$INVESTIGATION_ID\"}")

  # Handle Logstash Response & Checkpointing
  if [[ "$HTTP_STATUS" =~ ^20[0-2]$ ]]; then
    # Atomically record ID upon successful delivery
    echo "$ANALYSIS_ID" >> "$STATE_FILE"
    echo "[✓] Successfully queued (HTTP $HTTP_STATUS). Draining pipeline (sleeping ${SLEEP_INTERVAL}s)..."
    sleep "$SLEEP_INTERVAL"
  else
    echo "[-] FAILED: Logstash returned HTTP $HTTP_STATUS for Analysis $ANALYSIS_ID." >&2
    echo "[-] Execution halted to avoid losing progress. Fix Logstash and re-run the script to resume." >&2
    exit 1
  fi

done < "$INPUT_FILE"

echo "[✓] Ingestion complete. All matching IDs have been processed."