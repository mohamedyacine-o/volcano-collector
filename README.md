# Volcano Artifact Collector

A production-grade, low-overhead Python collector that extracts DFIR artifact data from the Volcano PostgreSQL database and writes structured, rotating NDJSON files for downstream ingestion via Logstash and Elasticsearch.

---

## 📋 Table of Contents
- [Architecture & Data Flow](#architecture--data-flow)
- [Prerequisites & System Requirements](#prerequisites--system-requirements)
  - [1. PostgreSQL Database Grants & Access](#1-postgresql-database-grants--access)
    - [Access to `artifacts` Schema (Required for all modes)](#a-access-to-artifacts-schema-required-for-all-modes)
    - [Access to `investigations` Schema (Required for Mode 'queue')](#b-access-to-investigations-schema-required-for-mode-queue)
    - [`pg_hba.conf` Configuration](#c-pg_hbaconf-configuration)
  - [2. File System Paths & Permissions (`chmod` / `chown`)](#2-file-system-paths--permissions-chmod--chown)
  - [3. Network, Ports & IP Bindings](#3-network-ports--ip-bindings)
  - [4. HTTP Layer-7 Security (Optional API Key)](#4-http-layer-7-security-optional-api-key)
- [Execution Modes](#execution-modes)
  - [Mode 1 – Continuous Table-Incremental (`service`)](#mode-1--continuous-table-incremental-service)
  - [Mode 2 – One-Shot Analysis Export (`once`)](#mode-2--one-shot-analysis-export-once)
  - [Mode 3 – Multi-Threaded HTTP Listener (`http`)](#mode-3--multi-threaded-http-listener-http)
  - [Mode 4 – Analysis-Centric Queue Worker (`queue`)](#mode-4--analysis-centric-queue-worker-queue)
- [Incremental Extraction & Non-Consecutive IDs](#incremental-extraction--non-consecutive-ids)
- [Offline Simulation (No PostgreSQL Required)](#offline-simulation-no-postgresql-required)
- [Quick Start Guide](#quick-start-guide)
- [Configuration Reference](#configuration-reference)
- [Output Format & Elasticsearch Pipeline](#output-format--elasticsearch-pipeline)
- [Production Deployment (Systemd)](#production-deployment-systemd)

---

## Architecture & Data Flow

```
┌────────────────────────────────────────────────────────┐
│              Volcano PostgreSQL Database               │
│  Schema: investigations │  Schema: artifacts           │
│  - analyses (UUIDs)     │  - 143+ forensic tables      │
│                         │  - get_all_analyses_artifacts│
└────────────┬────────────┴──────────────┬───────────────┘
             │                           │
             │ Discovers analyses        │ Streams rows via cursor
             ▼                           ▼
┌────────────────────────────────────────────────────────┐
│               Volcano Artifact Collector               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │ Mode 1       │  │ Mode 2       │  │ Mode 3       │  │
│  │ Table-Incr   │  │ One-Shot     │  │ HTTP Server  │  │
│  │ (id > last)  │  │ (Single UUID)│  │ (Threaded)   │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  │
│         │                 │                 │          │
│         │   ┌─────────────┴──────────────┐  │          │
│         │   │ Mode 4: Queue Worker       │  │          │
│         │   │ - Day 0: Backfill 1-by-1   │  │          │
│         │   │ - Ongoing: Polls every X s │  │          │
│         │   │ - Throttle: Paces Logstash │  │          │
│         │   │ - State: analyses_queue.json  │          │
│         │   └─────────────┬──────────────┘  │          │
│         ▼                 ▼                 ▼          │
│  ┌──────────────────────────────────────────────────┐  │
│  │   Rotating File Writer (Batch Flush, Size/Date)  │  │
│  └────────────────────────┬─────────────────────────┘  │
└───────────────────────────┼────────────────────────────┘
                            │ Writes NDJSON
                            ▼
      /var/log/volcano_artifacts/*.jsonl (chmod 640)
                            │
                            │ File tail / Logstash file input
                            ▼
             Elasticsearch Ingest Pipeline
               logs-volcano.artifacts-1.0.0
         (routes to process / network / file / registry)
```

---

## Prerequisites & System Requirements

### 1. PostgreSQL Database Grants & Access

The collector requires a dedicated database user (e.g., `logstash_ro` or `volcano_collector`). 

Connect to the PostgreSQL server as superuser:
```bash
psql -U postgres -d volcano
```

#### A. Access to `artifacts` Schema (Required for all modes)
Grants access to all 143+ forensic artifact tables and the function returning an analysis's artifacts:
```sql
GRANT CONNECT ON DATABASE volcano TO logstash_ro;
GRANT USAGE ON SCHEMA artifacts TO logstash_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA artifacts TO logstash_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA artifacts GRANT SELECT ON TABLES TO logstash_ro;
GRANT EXECUTE ON FUNCTION artifacts.get_all_analyses_artifacts(uuid) TO logstash_ro;
GRANT SELECT ON TABLE information_schema.tables TO logstash_ro;
GRANT SELECT ON TABLE information_schema.columns TO logstash_ro;
```

#### B. Access to `investigations` Schema (Required for Mode 'queue')
> ⚠️ **CRITICAL REQUIREMENT FOR MODE `queue`:**
> Mode `queue` directly queries the `investigations.analyses` table to discover existing analyses for Day 0 backfill and to periodically discover new analyses. Without access to the `investigations` schema, Mode `queue` will fail with `psycopg2.errors.InsufficientPrivilege: permission denied for schema investigations`.

Execute the following grants:
```sql
-- 1. Grant USAGE on the investigations schema
GRANT USAGE ON SCHEMA investigations TO logstash_ro;

-- 2. Grant SELECT on all existing tables in investigations (analyses, investigations, etc.)
GRANT SELECT ON ALL TABLES IN SCHEMA investigations TO logstash_ro;

-- 3. Automatically grant SELECT on future tables created under investigations
ALTER DEFAULT PRIVILEGES IN SCHEMA investigations GRANT SELECT ON TABLES TO logstash_ro;
```

#### C. `pg_hba.conf` Configuration
Ensure the collector host IP is permitted to connect:
```text
# TYPE  DATABASE  USER         ADDRESS             METHOD
host    volcano   logstash_ro  10.250.3.0/24       scram-sha-256
```
*(Reload PostgreSQL after editing: `sudo systemctl reload postgresql`)*

---

### 2. File System Paths & Permissions (`chmod` / `chown`)

To preserve forensic integrity and ensure least privilege, create a dedicated system user and set strict ownership:

```bash
# 1. Create a non-privileged service user and group
sudo useradd --system --no-create-home --shell /usr/sbin/nologin volcano

# 2. Create the artifact output directory
sudo mkdir -p /var/log/volcano_artifacts
sudo chown -R volcano:volcano /var/log/volcano_artifacts
sudo chmod 750 /var/log/volcano_artifacts

# 3. Secure configuration files (containing DB credentials and API keys)
sudo mkdir -p /etc/volcano-collector
sudo cp config.example.yaml /etc/volcano-collector/config.yaml
sudo cp .env.example /etc/volcano-collector/.env
sudo chown -R root:volcano /etc/volcano-collector
sudo chmod 750 /etc/volcano-collector
sudo chmod 640 /etc/volcano-collector/config.yaml
sudo chmod 640 /etc/volcano-collector/.env
```

| File / Directory | Recommended Path | Permissions | Owner:Group | Description |
|---|---|---|---|---|
| **Output Directory** | `/var/log/volcano_artifacts/` | `750` (`drwxr-x---`) | `volcano:volcano` | Directory where `.jsonl` files and checkpoints are written |
| **Output Files** | `/var/log/volcano_artifacts/*.jsonl` | `640` (`-rw-r-----`) | `volcano:volcano` | Rotating NDJSON files tailed by Logstash / Filebeat |
| **Queue State File** | `/var/log/volcano_artifacts/analyses_queue.json` | `640` (`-rw-r-----`) | `volcano:volcano` | Tracks pending, completed, and failed analyses in Mode `queue` |
| **Checkpoint State** | `/var/log/volcano_artifacts/artifact_state.json` | `640` (`-rw-r-----`) | `volcano:volcano` | Tracks `max_id` per table for Mode `service` |
| **Skipped Log** | `/var/log/volcano_artifacts/skipped_analyses.log` | `640` (`-rw-r-----`) | `volcano:volcano` | Audit trail of analyses returning 0 rows |
| **Config File** | `/etc/volcano-collector/config.yaml` | `640` (`-rw-r-----`) | `root:volcano` | YAML configuration |
| **Env File** | `/etc/volcano-collector/.env` | `600` or `640` | `root:volcano` | Sensitive database credentials and API key |

---

### 3. Network, Ports & IP Bindings

| Direction | Service | Host / IP | Port | Protocol | Purpose |
|---|---|---|---|---|---|
| **Outbound** | PostgreSQL | Volcano DB Host (e.g. `10.250.3.15`) | `5432` | TCP | Database connection & server-side streaming cursor |
| **Inbound** | HTTP Mode 3 | `127.0.0.1` (or internal IP) | `8080` (configurable) | TCP | On-demand export trigger (`POST /export`, `GET /health`) |
| **Outbound** | Logstash/ES | Forwarder / SIEM | `5044` / `9200` | TCP | Ingestion of tailed `.jsonl` files (handled by Logstash/Filebeat) |

> 🔒 **Security Best Practice for HTTP Mode:**
> By default, bind `HTTP_LISTEN_HOST=127.0.0.1` if triggering exports locally from the same host. If listening across the internal network, bind to the private interface IP (`10.250.x.x`), and protect via firewall or reverse proxy. Never expose Mode 3 directly to public subnets (`0.0.0.0`).

---

### 4. HTTP Layer-7 Security (Optional API Key)

HTTP security is **optional and off by default** for initial friction-free testing:
1. Configure an API key via environment variable:
   ```bash
   export HTTP_API_KEY="YourSecureRandomKeyHere"
   ```
   Or in `config.yaml`:
   ```yaml
   security:
     api_key: "YourSecureRandomKeyHere"
   ```
   Or CLI: `--api-key "YourSecureRandomKeyHere"`

2. When set, requests to `POST /export` must supply the key via either:
   - Header: `X-API-Key: YourSecureRandomKeyHere`
   - Authorization: `Authorization: Bearer YourSecureRandomKeyHere`
   - If missing or invalid, the collector rejects the request with `HTTP 401 Unauthorized`.
   - `GET /health` remains unauthenticated for standard load-balancer and Kubernetes probes.

---

## Execution Modes

### Mode 1 – Continuous Table-Incremental (`service`)
Iterates over all 143+ tables in the `artifacts` schema and fetches rows where `id > last_id`:
```bash
python3 -m collector --mode service
```
- Reuses DB connection across cycles.
- Updates `artifact_state.json`.

---

### Mode 2 – One-Shot Analysis Export (`once`)
Exports all artifact rows for a specific `analysis_id` across all tables into a dedicated JSONL file, then exits:
```bash
python3 -m collector --mode once --analysis-id 123e4567-e89b-12d3-a456-426614174000
```
- Validates UUID format before connecting.
- Empty analyses are logged to `skipped_analyses.log`.

---

### Mode 3 – Multi-Threaded HTTP Listener (`http`)
Runs a multi-threaded HTTP server accepting on-demand export triggers:
```bash
python3 -m collector --mode http --http-port 8080
```
- `GET /health` → Returns `200 OK` `{"status": "ok"}`
- `POST /export` → Triggers extraction for payload:
  ```json
  {
    "analysis_id": "aa701130-29e8-46a7-a0e8-a9036ddcbff1",
    "investigation_id": "inv-98765",
    "prefix": "custom_prefix"
  }
  ```

---

### Mode 4 – Analysis-Centric Queue Worker (`queue`) — *Recommended for Production*
Extracts all `analyses_id`s from `investigations.analyses`, stores them in a local JSON queue file, collects artifacts **1-by-1 with throttling**, and monitors for newly completed analyses every `poll_interval`:
```bash
python3 -m collector --mode queue
```

#### How Mode `queue` Works:
1. **Day 0 (Initial Backfill):**
   - Connects to `investigations.analyses` and queries all completed analyses.
   - Saves them to `/var/log/volcano_artifacts/analyses_queue.json`.
   - Processes each analysis sequentially (1-by-1).
2. **Ongoing Discovery:**
   - Every `POLL_INTERVAL_SECONDS` (default: 300s), queries `investigations.analyses` for newly completed analyses.
   - Enqueues new analyses into `analyses_queue.json` without duplicating already completed ones.
3. **Logstash Heap Protection (Throttling):**
   - By default, sleeps `--throttle 1.0` seconds between analyses.
   - Prevents overwhelming Logstash memory (1GB / 8GB heap) by ensuring Logstash finishes flushing pipeline buffers before the next 6MB+ analysis arrives.
4. **Crash-Resilience & Resumability:**
   - State is stored in `analyses_queue.json` atomically using temporary file swaps.
   - If stopped or restarted, it resumes right at the next pending analysis.
5. **Investigation Context:**
   - Automatically injects the true `investigation_id` and analysis metadata from `investigations.analyses` into every artifact line.

Customizing Queue Mode:
```bash
# Custom schema, table, status filter, and 2-second pacing:
python3 -m collector --mode queue \
  --analyses-schema investigations \
  --analyses-table analyses \
  --status-filter completed \
  --throttle 2.0 \
  --poll-interval 120
```

---

## Incremental Extraction & Non-Consecutive IDs

In the Volcano database:
1. Each artifact table contains an `id` column (`bigint`).
2. IDs are **not strictly consecutive (`id + 1`)** — there are jumps, gaps, and independent sequences across tables.
3. Analyses and investigations use UUIDs.

**How the collector handles this:**
- The incremental extractor executes:
  ```sql
  SELECT id, analyses_id, to_jsonb(t.*) AS artifact_data
  FROM artifacts.<table_name> t
  WHERE t.id > %s
  ORDER BY t.id
  ```
- Because it queries `WHERE t.id > last_id` and advances `last_id = max(max_id, row['id'])`, it naturally supports arbitrary gaps between IDs.
- If a table fails (e.g. query timeout), the collector logs a warning, skips advancing the checkpoint for that specific table, and safely continues with other tables.
- Any `investigation_id` present in the row or artifact payload is automatically extracted and retained.

---

## Offline Simulation (No PostgreSQL Required)

You can validate the entire collector pipeline immediately without access to a live PostgreSQL Volcano DB:

```bash
python3 simulate_collector.py
```

What the simulation verifies:
1. **Incremental Service Mode**: Multiple tables, non-sequential IDs (jumps from 105 to 776394 to 890012), checkpoint state creation and updates.
2. **One-Shot Mode**: Full table extraction by UUID, empty analysis skip-tracking in `skipped_analyses.log`.
3. **HTTP Server Mode**: Multi-threaded concurrency, `GET /health`, 401 unauthorized rejection without API key, 200 OK export with `X-API-Key`, 400 Bad Request rejection for malformed UUIDs.
4. **Elasticsearch Pipeline Compliance**: Verifies that every written JSON line matches the structure expected by the `logs-volcano.artifacts-1.0.0` Elasticsearch ingest pipeline.
5. **Mode 4 Queue Worker**: Discovers analyses from simulated `investigations.analyses`, writes `analyses_queue.json`, processes Day 0 items 1-by-1, and dynamically discovers and processes new analyses during recurring intervals.

---

## Quick Start Guide

```bash
# 1. Navigate to collector directory
cd volcano-collector

# 2. Set up virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env with your Volcano DB credentials

# 5. Test with dry-run (validates configuration)
python3 -m collector --dry-run

# 6. Test simulation
python3 simulate_collector.py

# 7. Start the queue worker (Recommended for production)
python3 -m collector --mode queue
```

---

## Configuration Reference

All settings can be specified via CLI flags, environment variables, or `config.yaml`. CLI arguments take highest precedence.

| Setting | CLI Flag | Environment Variable | Default | Description |
|---|---|---|---|---|
| **Mode** | `--mode`, `-m` | `COLLECTOR_MODE` | `service` | `service`, `once`, `http`, or `queue` |
| **DB Host** | `--db-host` | `VOLCANO_DB_HOST` | `localhost` | PostgreSQL host |
| **DB Port** | `--db-port` | `VOLCANO_DB_PORT` | `5432` | PostgreSQL port |
| **DB Name** | `--db-name` | `VOLCANO_DB_NAME` | `volcano` | Database name |
| **DB User** | `--db-user` | `VOLCANO_DB_USER` | `logstash_ro` | Database username |
| **DB Pass** | `--db-pass` | `VOLCANO_DB_PASS` | `postgres` | Database password |
| **Output Dir** | `--output-dir` | `ARTIFACTS_OUTPUT_DIR` | `/tmp/volcano_artifacts` | Directory for JSONL files |
| **Prefix 1** | `--prefix-1` | `OUTPUT_PREFIX_1` | `volcano_artifacts` | Prefix for continuous service / queue |
| **Prefix 2** | `--prefix-2` | `OUTPUT_PREFIX_2` | `volcano_one_shot` | Prefix for one-shot exports |
| **Prefix 3** | `--prefix-3` | `OUTPUT_PREFIX_3` | `volcano_http` | Prefix for HTTP exports |
| **Max Size** | `--max-size` | `MAX_FILE_SIZE_MB` | `100` | Max JSONL file size in MiB |
| **Daily Rotate**| `--no-daily-rotate` | `ROTATE_DAILY` | `true` | Rotate files at midnight UTC |
| **Poll Interval**| `--poll-interval` | `POLL_INTERVAL_SECONDS` | `300` | Seconds between discovery / poll cycles |
| **Analyses Schema**| `--analyses-schema`| `ANALYSES_SCHEMA` | `investigations` | Schema for analyses table in Mode `queue` |
| **Analyses Table**| `--analyses-table`| `ANALYSES_TABLE` | `analyses` | Table name for analyses in Mode `queue` |
| **Status Filter**| `--status-filter` | `ANALYSES_STATUS_FILTER` | `completed` | Status filter in Mode `queue` (empty = all) |
| **Throttle** | `--throttle` | `ANALYSES_THROTTLE_SECONDS`| `1.0` | Delay between analyses in Mode `queue` (s) |
| **HTTP Port** | `--http-port` | `HTTP_LISTEN_PORT` | `8080` | Port for HTTP listener |
| **HTTP Host** | `--http-host` | `HTTP_LISTEN_HOST` | `0.0.0.0` | Bind IP for HTTP listener |
| **API Key** | `--api-key` | `HTTP_API_KEY` | `""` (disabled) | Optional API key for Mode 3 |
| **Fetch Size** | `--fetch-size` | `DB_FETCH_SIZE` | `1000` | Server-side cursor batch size |
| **Flush Interval**| `--flush-interval`| `FLUSH_INTERVAL` | `1000` | Line interval for disk flush |
| **Analysis ID**| `-a`, `--analysis-id`| `ANALYSIS_ID` | `None` | Target UUID for Mode 2 |

---

## Output Format & Elasticsearch Pipeline

Each line written to the `.jsonl` files is an NDJSON event matching the Elasticsearch `logs-volcano.artifacts-1.0.0` ingest pipeline schema:

```json
{
  "artifact_table": "windows_process",
  "analysis_id": "aa701130-29e8-46a7-a0e8-a9036ddcbff1",
  "investigation_id": "inv-98765",
  "artifact_data": {
    "id": 776394,
    "process_id": 3169,
    "commit_charge": 16,
    "p_offset": 32186344,
    "protection": 4,
    "exe_at_base": false,
    "length": 65536
  }
}
```

The Elasticsearch pipeline automatically inspects `ctx.artifact_table` and routes the document to the corresponding sub-pipeline:
- `windows_process`, `unix_process`, `windows_service`, etc. → `logs-volcano.process-1.0.0`
- `connection`, `socket`, `arp_entry`, `dns_entry`, etc. → `logs-volcano.network-1.0.0`
- `cached_file`, `windows_mft_entry`, `mount_point`, etc. → `logs-volcano.file-1.0.0`
- `windows_registry_*` → `logs-volcano.registry-1.0.0`

---

## Production Deployment (Systemd)

To run the collector as a resilient Linux daemon on the production server in Queue Mode:

Create `/etc/systemd/system/volcano-collector.service`:

```ini
[Unit]
Description=Volcano Artifact Collector Queue Service
After=network.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=volcano
Group=volcano
WorkingDirectory=/opt/volcano-collector
EnvironmentFile=/etc/volcano-collector/.env
ExecStart=/opt/volcano-collector/.venv/bin/python3 -m collector --mode queue --config /etc/volcano-collector/config.yaml

Restart=always
RestartSec=10
LimitNOFILE=65536

# Security hardening
ProtectSystem=strict
ReadWritePaths=/var/log/volcano_artifacts
ProtectHome=true
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now volcano-collector
sudo systemctl status volcano-collector
```
