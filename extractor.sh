#!/usr/bin/env bash
# ==============================================================================
# Volcano Forensic Artifacts Exporter Daemon
# Streams DB results directly to NDJSON files for Logstash file-stream ingestion.
# ==============================================================================

set -uo pipefail

# --- Database Configuration ---
DB_HOST="10.250.3.15"
DB_PORT="5432"
DB_NAME="volcano"
DB_USER="logstash_ro"
export PGPASSWORD="YOUR_POSTGRES_PASSWORD_HERE"

# --- Storage & Path Settings ---
OUT_DIR="/var/log/volcano_artifacts/incoming"
STATE_FILE="/var/lib/volcano_exporter/processed_analyses.txt"
INITIAL_MARKER="/var/lib/volcano_exporter/.initial_sync_done"

# --- Scheduling & Throttling Settings ---
POLL_INTERVAL=60       # Interval between polling cycles (seconds)
LOOKBACK_DAYS=3        # Rolling window for ongoing execution
ANALYSIS_SLEEP=2       # Cool-down between analyses to pace disk write IOPS

mkdir -p "$OUT_DIR" "$(dirname "$STATE_FILE")"
touch "$STATE_FILE"

RUNNING=1
trap 'RUNNING=0; echo "[*] Termination received. Exiting daemon loop...";' SIGINT SIGTERM

echo "[*] Volcano File Exporter started. Watching for completed analyses..."

while [ "$RUNNING" -eq 1 ]; do

  # 1. Determine query scope: Full backfill on first run; 3-day window ongoing
  if [ ! -f "$INITIAL_MARKER" ]; then
    echo "[!] Initial run detected: Commencing full historical scan..."
    DATE_CLAUSE=""
  else
    DATE_CLAUSE="AND date_analyzed >= (NOW() - INTERVAL '${LOOKBACK_DAYS} days')"
  fi

  # 2. Query completed analyses
  ANALYSES_LIST=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -At -c "
    SELECT id 
    FROM investigations.analyses 
    WHERE status = 'complete' 
      AND pending_delete = false 
      ${DATE_CLAUSE}
    ORDER BY date_analyzed ASC;
  " 2>/dev/null)

  if [ -n "$ANALYSES_LIST" ]; then
    for AID in $ANALYSES_LIST; do
      [ "$RUNNING" -eq 0 ] && break
      [ -z "$AID" ] && continue

      # Skip if already exported
      if grep -Fxq "$AID" "$STATE_FILE"; then
        continue
      fi

      echo "[+] Processing Analysis ID: $AID"
      TMP_FILE="${OUT_DIR}/${AID}.ndjson.tmp"
      FINAL_FILE="${OUT_DIR}/${AID}.ndjson"

      # 3. Export artifacts directly as single-line NDJSON objects
      psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -At -c "
        SELECT json_build_object(
          'analysis_id', '$AID',
          'source_table', table_name,
          'artifact', artifact_data::json
        )::text
        FROM artifacts.get_all_analyses_artifacts(CAST('$AID' AS uuid));
      " > "$TMP_FILE" 2>/dev/null

      EXIT_CODE=$?

      if [ $EXIT_CODE -eq 0 ] && [ -s "$TMP_FILE" ]; then
        # Atomically rename to notify Logstash/File watcher
        mv "$TMP_FILE" "$FINAL_FILE"
        chmod 640 "$FINAL_FILE"
        
        # Checkpoint state
        echo "$AID" >> "$STATE_FILE"
        echo "[✓] Successfully exported $AID to $FINAL_FILE"
        sleep "$ANALYSIS_SLEEP"
      elif [ $EXIT_CODE -eq 0 ] && [ ! -s "$TMP_FILE" ]; then
        # Zero artifacts associated with this analysis
        rm -f "$TMP_FILE"
        echo "$AID" >> "$STATE_FILE"
        echo "[~] Analysis $AID completed with 0 artifacts. Marked done."
      else
        echo "[-] Error querying DB for analysis $AID. Will retry on next loop." >&2
        rm -f "$TMP_FILE"
      fi
    done
  fi

  # Mark initial sync complete after first pass
  if [ ! -f "$INITIAL_MARKER" ]; then
    touch "$INITIAL_MARKER"
    echo "[✓] Full initial backfill pass complete. Switching to ${LOOKBACK_DAYS}-day rolling window."
  fi

  # Wait before next poll interval
  SLEPT=0
  while [ "$SLEPT" -lt "$POLL_INTERVAL" ] && [ "$RUNNING" -eq 1 ]; do
    sleep 1
    ((SLEPT++))
  done

done

echo "[*] Volcano File Exporter cleanly stopped."