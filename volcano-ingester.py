#!/usr/bin/env python3
"""
Volcano Analyses Ingestion Daemon
Monitors investigations.analyses for completed runs and dispatches them
sequentially to Logstash HTTP input with atomic state management and rate limiting.
"""

import os
import sys
import time
import json
import signal
import logging
from datetime import datetime, timezone
import requests
import psycopg2
from psycopg2.extras import RealDictCursor

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(process)d] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("volcano_ingester")

# Load Configuration from Environment
DB_HOST = os.getenv("DB_HOST", "10.250.3.15")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "volcano")
DB_USER = os.getenv("DB_USER", "logstash_ro")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SSLMODE = os.getenv("DB_SSLMODE", "prefer")
DB_SCHEMA = os.getenv("DB_SCHEMA", "investigations")
DB_TABLE = os.getenv("DB_TABLE", "analyses")

LOGSTASH_URL = os.getenv("LOGSTASH_URL", "http://localhost:5666")
LOGSTASH_TIMEOUT = int(os.getenv("LOGSTASH_TIMEOUT", "30"))

STATE_FILE = os.getenv("STATE_FILE_PATH", "/var/lib/volcano-ingester/ingestion_state.json")
HISTORY_FILE = os.getenv("HISTORY_FILE_PATH", "/var/lib/volcano-ingester/processed_analyses.ndjson")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
DISPATCH_SLEEP = int(os.getenv("DISPATCH_SLEEP_SECONDS", "45"))
INITIAL_DATE_STR = os.getenv("INITIAL_DATE", "1970-01-01T00:00:00+00:00")

# Global stop event for graceful shutdown
SHUTDOWN_REQUESTED = False

def handle_shutdown(signum, frame):
    global SHUTDOWN_REQUESTED
    logger.info(f"Received termination signal ({signum}). Finishing current task and exiting gracefully...")
    SHUTDOWN_REQUESTED = True

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


class StateManager:
    """Handles atomic read/write of state tracking file and history log."""
    
    def __init__(self, state_path, history_path, default_start_date):
        self.state_path = state_path
        self.history_path = history_path
        self.default_start_date = default_start_date
        self.state = self.load_state()

    def load_state(self):
        if os.path.exists(self.state_path):
            try:
                with open(self.state_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    logger.info(f"Loaded existing state: last_watermark={data.get('last_watermark')}, processed_count={len(data.get('processed_ids', []))}")
                    return {
                        "last_watermark": data.get("last_watermark", self.default_start_date),
                        "processed_ids": set(data.get("processed_ids", []))
                    }
            except Exception as e:
                logger.error(f"Failed to read state file, falling back to default: {e}")
        
        return {
            "last_watermark": self.default_start_date,
            "processed_ids": set()
        }

    def save_state(self, watermark, analysis_id):
        """Atomic write using temporary file replacement to prevent corrupt state on sudden crash."""
        self.state["last_watermark"] = watermark
        self.state["processed_ids"].add(str(analysis_id))

        # Cap memory set to prevent infinite growth while preserving idempotency for recent timestamps
        if len(self.state["processed_ids"]) > 100000:
            self.state["processed_ids"] = set(list(self.state["processed_ids"])[-50000:])

        dir_name = os.path.dirname(self.state_path)
        temp_file = os.path.join(dir_name, f".state.tmp.{os.getpid()}")
        try:
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump({
                    "last_watermark": self.state["last_watermark"],
                    "processed_ids": list(self.state["processed_ids"]),
                    "last_updated": datetime.now(timezone.utc).isoformat()
                }, f, indent=2)
            os.replace(temp_file, self.state_path)
            os.chmod(self.state_path, 0o600)
        except Exception as e:
            logger.error(f"Critical error updating state file: {e}")
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def append_history(self, record, status_code):
        """Append historical tracking entry into NDJSON file."""
        entry = {
            "analysis_id": str(record["id"]),
            "investigation_id": str(record["investigation_id"]),
            "status": record["status"],
            "date_analyzed": record["date_analyzed"].isoformat() if hasattr(record["date_analyzed"], "isoformat") else str(record["date_analyzed"]),
            "dispatched_at": datetime.now(timezone.utc).isoformat(),
            "logstash_http_status": status_code
        }
        try:
            with open(self.history_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
            os.chmod(self.history_path, 0o640)
        except Exception as e:
            logger.warning(f"Failed to append to history log: {e}")


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        sslmode=DB_SSLMODE,
        connect_timeout=15
    )


def fetch_new_analyses(conn, watermark):
    """
    Fetches completed analyses with date_analyzed >= watermark.
    Ordered chronologically to advance the cursor sequentially.
    """
    query = f"""
        SELECT 
            id, 
            investigation_id, 
            status, 
            date_analyzed 
        FROM {DB_SCHEMA}.{DB_TABLE}
        WHERE status = 'complete' 
          AND pending_delete = false
          AND date_analyzed >= %s
        ORDER BY date_analyzed ASC, id ASC
        LIMIT 200;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (watermark,))
        return cur.fetchall()


def dispatch_to_logstash(analysis_id, investigation_id):
    """Sends payload to Logstash HTTP endpoint with retry logic."""
    payload = {
        "analysis_id": str(analysis_id),
        "investigation_id": str(investigation_id)
    }
    headers = {"Content-Type": "application/json"}
    
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(
                LOGSTASH_URL,
                json=payload,
                headers=headers,
                timeout=LOGSTASH_TIMEOUT
            )
            if response.status_code in (200, 201, 202):
                return True, response.status_code
            logger.warning(f"Logstash returned HTTP {response.status_code} (attempt {attempt}/{max_retries})")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Connection to Logstash failed ({e}) (attempt {attempt}/{max_retries})")
        
        if attempt < max_retries and not SHUTDOWN_REQUESTED:
            time.sleep(5)
            
    return False, None


def main():
    logger.info("Initializing Volcano Analysis Ingestion Service...")
    state_mgr = StateManager(STATE_FILE, HISTORY_FILE, INITIAL_DATE_STR)

    while not SHUTDOWN_REQUESTED:
        conn = None
        try:
            conn = get_db_connection()
            watermark = state_mgr.state["last_watermark"]
            records = fetch_new_analyses(conn, watermark)

            if records:
                logger.info(f"Discovered {len(records)} candidate analyses on/after watermark {watermark}")
                
                for row in records:
                    if SHUTDOWN_REQUESTED:
                        break

                    analysis_id = str(row["id"])
                    row_date_iso = row["date_analyzed"].isoformat()

                    # Deduplicate in case multiple records share the same watermark timestamp
                    if analysis_id in state_mgr.state["processed_ids"]:
                        continue

                    logger.info(f"[+] Processing Analysis ID: {analysis_id} (Analyzed: {row_date_iso})")
                    
                    success, code = dispatch_to_logstash(analysis_id, row["investigation_id"])
                    
                    if success:
                        state_mgr.append_history(row, code)
                        state_mgr.save_state(row_date_iso, analysis_id)
                        logger.info(f"[✓] Dispatched successfully. Pacing throttle: sleeping {DISPATCH_SLEEP}s...")
                        
                        # Throttled sleep with interruption check
                        slept = 0
                        while slept < DISPATCH_SLEEP and not SHUTDOWN_REQUESTED:
                            time.sleep(1)
                            slept += 1
                    else:
                        logger.error(f"[X] Failed to dispatch Analysis ID {analysis_id} after retries. Pausing before next check.")
                        break
            else:
                logger.debug("No new completed analyses found. Entering poll interval.")

        except psycopg2.Error as pge:
            logger.error(f"PostgreSQL error: {pge}")
        except Exception as ex:
            logger.error(f"Unexpected operational error: {ex}", exc_info=True)
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

        # Wait for the next polling cycle
        poll_slept = 0
        while poll_slept < POLL_INTERVAL and not SHUTDOWN_REQUESTED:
            time.sleep(1)
            poll_slept += 1

    logger.info("Service shutdown completed cleanly.")

if __name__ == "__main__":
    main()