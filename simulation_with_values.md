# Volcano Collector – End-to-End Simulation with Real Values

This document illustrates the complete lifecycle of artifact data extraction for **Mode 4 (Analysis-Centric Queue)** and **Mode 1 (Table-Centric Incremental)**. It shows the exact SQL queries executed, the internal data structures, the state files on disk, the NDJSON output lines, and how Logstash ships the data into the Elasticsearch ingest pipeline.

---

## 📑 Summary of Execution Modes Covered

| Mode | Trigger Point | DB Discovery Target | Output Grouping | Logstash Safety |
|---|---|---|---|---|
| **Mode 4 (`queue`)** | Analysis Queue | `investigations.analyses` | All 143+ tables per completed analysis | **High** (Paced 1-by-1 with `--throttle 1.0s`) |
| **Mode 1 (`service`)** | Polling Interval | `artifacts.*` (all tables) | Scattered rows where `id > last_seen_id` | **Variable** (Spikes if multiple tables write simultaneously) |

---

# 🚀 PART 1: Mode 4 – Analysis-Centric Queue Worker (`--mode queue`)

In Mode 4, the collector acts as a sequential queue worker. It discovers completed investigations, writes them to a persistent queue file, and extracts their forensic artifacts one analysis at a time.

---

### Step 1: Discovery Query on `investigations.analyses`

When the collector starts (Day 0) or when the polling interval triggers, it queries the `investigations` namespace:

```sql
SELECT 
    analyses_id AS analysis_id,
    investigation_id AS investigation_id
FROM investigations.analyses
WHERE status = 'completed'
ORDER BY create_time ASC;
```

#### Real Values Returned by PostgreSQL:
```text
              analysis_id             |       investigation_id
--------------------------------------+------------------------------
 aa701130-29e8-46a7-a0e8-a9036ddcbff1 | inv-2026-endpoint-secops-01
 bb812241-3af9-47b8-b1f9-b0047eedc002 | inv-2026-endpoint-secops-02
 00000000-0000-0000-0000-000000000000 | inv-2026-endpoint-secops-03
(3 rows)
```

---

### Step 2: Format Stored in the Queue File (`analyses_queue.json`)

The discovered analyses are enqueued into `/var/log/volcano_artifacts/analyses_queue.json` using an atomic file swap (`.tmp` → rename):

```json
{
  "total_discovered": 3,
  "last_sync_time": "2026-11-03T13:15:00.124510+00:00",
  "pending": [
    {
      "analysis_id": "aa701130-29e8-46a7-a0e8-a9036ddcbff1",
      "investigation_id": "inv-2026-endpoint-secops-01",
      "discovered_at": "2026-11-03T13:15:00.125100+00:00"
    },
    {
      "analysis_id": "bb812241-3af9-47b8-b1f9-b0047eedc002",
      "investigation_id": "inv-2026-endpoint-secops-02",
      "discovered_at": "2026-11-03T13:15:00.125120+00:00"
    },
    {
      "analysis_id": "00000000-0000-0000-0000-000000000000",
      "investigation_id": "inv-2026-endpoint-secops-03",
      "discovered_at": "2026-11-03T13:15:00.125130+00:00"
    }
  ],
  "completed": {},
  "failed": {}
}
```

---

### Step 3: Query Executed for Each `analysis_id`

The worker pops the next item from `pending` (FIFO) and opens a server-side cursor to execute Volcano's stored procedure:

```sql
-- Executed with cursor "artifact_stream_<random_hex>"
SELECT table_name, artifact_data 
FROM artifacts.get_all_analyses_artifacts('aa701130-29e8-46a7-a0e8-a9036ddcbff1'::uuid);
```

#### What the Database Function Returns:
| `table_name` | `artifact_data` (`jsonb`) |
|---|---|
| `windows_process` | `{"id": 776394, "process_id": 3169, "commit_charge": 16, "exe_at_base": false, "protection": 4, "p_offset": 32186344, "length": 65536, ...}` |
| `connection` | `{"id": 1204, "source_ip": "10.0.1.25", "destination_ip": "198.51.100.42", "source_port": 49210, "destination_port": 443, "protocol": "TCP"}` |
| `antivirus_scan` | `{"id": 7, "threat_name": "Trojan.GenericKD.68124", "resource_path": "C:\\Windows\\Temp\\beacon.bin", "source": "Windows Defender"}` |

---

### Step 4: What is Written to the Output File (`.jsonl`)

The collector formats each row into a standardized NDJSON string and writes it to `/var/log/volcano_artifacts/volcano_artifacts_20261103_001.jsonl`:

#### 📄 Example 1: `windows_process` (Process Execution)
```json
{"artifact_table": "windows_process", "analysis_id": "aa701130-29e8-46a7-a0e8-a9036ddcbff1", "investigation_id": "inv-2026-endpoint-secops-01", "artifact_data": {"id": 776394, "analyses_id": "aa701130-29e8-46a7-a0e8-a9036ddcbff1", "process_id": 3169, "commit_charge": 16, "control_area": null, "p_offset": 32186344, "range_flags": 0, "exe_at_base": false, "vad_type": null, "protection": 4, "erased_header": false, "artifact_notes": null, "length": 65536}}
```

#### 📄 Example 2: `connection` (Network C2 Traffic)
```json
{"artifact_table": "connection", "analysis_id": "aa701130-29e8-46a7-a0e8-a9036ddcbff1", "investigation_id": "inv-2026-endpoint-secops-01", "artifact_data": {"id": 1204, "analyses_id": "aa701130-29e8-46a7-a0e8-a9036ddcbff1", "source_ip": "10.0.1.25", "destination_ip": "198.51.100.42", "source_port": 49210, "destination_port": 443, "protocol": "TCP", "p_offset": 4819200, "v_offset": 140735118}}
```

#### 📄 Example 3: `antivirus_scan` (Threat Detection)
```json
{"artifact_table": "antivirus_scan", "analysis_id": "aa701130-29e8-46a7-a0e8-a9036ddcbff1", "investigation_id": "inv-2026-endpoint-secops-01", "artifact_data": {"id": 7, "analyses_id": "aa701130-29e8-46a7-a0e8-a9036ddcbff1", "threat_name": "Trojan.GenericKD.68124", "resource_path": "C:\\Windows\\Temp\\beacon.bin", "source": "Windows Defender", "start_time": "2026-11-03T10:12:00Z", "p_offset": 120048}}
```

---

### Step 5: State Updates After Extraction

1. **`analyses_queue.json` is updated:**
```json
{
  "total_discovered": 3,
  "last_sync_time": "2026-11-03T13:15:00.124510+00:00",
  "pending": [
    {
      "analysis_id": "bb812241-3af9-47b8-b1f9-b0047eedc002",
      "investigation_id": "inv-2026-endpoint-secops-02"
    },
    {
      "analysis_id": "00000000-0000-0000-0000-000000000000",
      "investigation_id": "inv-2026-endpoint-secops-03"
    }
  ],
  "completed": {
    "aa701130-29e8-46a7-a0e8-a9036ddcbff1": {
      "completed_at": "2026-11-03T13:15:02.412890+00:00",
      "rows_exported": 3
    }
  },
  "failed": {}
}
```

2. **Empty Analysis Handling (`00000000-...`):**
   When `00000000-0000-0000-0000-000000000000` returns 0 rows, it is written to `/var/log/volcano_artifacts/skipped_analyses.log`:
   ```text
   2026-11-03T13:15:04.120000+00:00 | 00000000-0000-0000-0000-000000000000 | SKIPPED | no artifact data found
   ```
   And removed from pending so it never blocks the queue.

3. **Throttling:**
   The collector sleeps `1.0s` (`--throttle 1.0`) before starting analysis #2, allowing Logstash to flush its buffers without JVM heap exhaustion.

---

### Step 6: What Logstash Collects & Ships to Elasticsearch

#### Logstash File Input (`tail`):
```ruby
input {
  file {
    path => "/var/log/volcano_artifacts/*.jsonl"
    mode => "tail"
    codec => "json"
  }
}
output {
  elasticsearch {
    hosts => ["https://elasticsearch:9200"]
    index => "logs-volcano.artifacts-default"
    pipeline => "logs-volcano.artifacts-1.0.0"
  }
}
```

#### Elasticsearch Ingest Pipeline Processing (`logs-volcano.artifacts-1.0.0`):
1. **Timestamping:** `@timestamp` is set to ingest time (or extracted from artifact).
2. **Metadata Setup:** `event.kind = "event"`, `event.module = "volcano"`.
3. **Pipeline Routing:** Inspects `ctx.artifact_table`:
   - `windows_process` → Routed to `logs-volcano.process-1.0.0` (maps `process_id` to `process.pid`).
   - `connection` → Routed to `logs-volcano.network-1.0.0` (maps `source_ip` to `source.ip`, `destination_port` to `destination.port`, runs GeoIP).
   - `antivirus_scan` → Routed to file/malware sub-pipeline.
4. **Painless Cleaning:** Removes all null fields (e.g. `control_area: null`, `vad_type: null`).

---

# ⚡ PART 2: Mode 1 – Continuous Table-Incremental (`--mode service`)

In Mode 1, the collector iterates through all 143+ tables in the `artifacts` schema and fetches rows where `id > last_seen_id`.

---

### Step 1: Discover All Artifact Tables
The collector queries `information_schema`:
```sql
SELECT DISTINCT c.table_name
FROM information_schema.columns c
JOIN information_schema.tables t
  ON c.table_schema = t.table_schema AND c.table_name = t.table_name
WHERE c.table_schema = 'artifacts'
  AND t.table_type = 'BASE TABLE'
  AND c.column_name = 'analyses_id'
ORDER BY c.table_name;
```
*Discovers 143+ tables: `antivirus_scan`, `arp_entry`, `auditd_record`, `autoruns`, `windows_process`, etc.*

---

### Step 2: Per-Table Incremental Query with Gaps
The collector reads `/var/log/volcano_artifacts/artifact_state.json`. Suppose `windows_process` was last seen at `id = 105`.

```sql
-- Executed with cursor "incr_windows_process_<random_hex>"
SELECT id, analyses_id, to_jsonb(t.*) AS artifact_data
FROM artifacts.windows_process t
WHERE t.id > 105
ORDER BY t.id;
```

#### Database Returns Rows (Notice the non-sequential jump to 776394):
```text
   id   |             analyses_id              |               artifact_data
--------+--------------------------------------+-------------------------------------------
 776394 | aa701130-29e8-46a7-a0e8-a9036ddcbff1 | {"id": 776394, "process_id": 3169, ...}
 890012 | bb812241-3af9-47b8-b1f9-b0047eedc002 | {"id": 890012, "process_id": 1120, ...}
```

---

### Step 3: Checkpoint State File (`artifact_state.json`)

The state file records the highest `id` seen per table:

```json
{
  "antivirus_scan": 7,
  "arp_entry": 42,
  "auditd_record": 10520,
  "autoruns": 89,
  "connection": 1204,
  "windows_process": 890012
}
```
*On the next polling cycle, the collector only asks for rows with `id > 890012` for `windows_process`, `id > 1204` for `connection`, etc.*

---

### Step 4: What is Written to the Output File (`.jsonl`)

#### 📄 Example 1: `windows_process`
```json
{"artifact_table": "windows_process", "analysis_id": "aa701130-29e8-46a7-a0e8-a9036ddcbff1", "investigation_id": "", "artifact_data": {"id": 776394, "analyses_id": "aa701130-29e8-46a7-a0e8-a9036ddcbff1", "process_id": 3169, "commit_charge": 16, "control_area": null, "p_offset": 32186344, "range_flags": 0, "exe_at_base": false, "vad_type": null, "protection": 4, "erased_header": false, "artifact_notes": null, "length": 65536}}
```

#### 📄 Example 2: `arp_entry` (Network Layer Discovery)
```json
{"artifact_table": "arp_entry", "analysis_id": "aa701130-29e8-46a7-a0e8-a9036ddcbff1", "investigation_id": "", "artifact_data": {"id": 42, "analyses_id": "aa701130-29e8-46a7-a0e8-a9036ddcbff1", "mac_address": "00:1A:2B:3C:4D:5E", "network_address": "192.168.1.1", "device_name": "eth0", "device_netns": 0, "p_offset": 84120, "v_offset": 992014}}
```

#### 📄 Example 3: `auditd_record` (Linux Audit Daemon Record)
```json
{"artifact_table": "auditd_record", "analysis_id": "bb812241-3af9-47b8-b1f9-b0047eedc002", "investigation_id": "", "artifact_data": {"id": 10520, "analyses_id": "bb812241-3af9-47b8-b1f9-b0047eedc002", "type": "SYSCALL", "aid": "1002", "create_time": "2026-11-03T11:00:22Z", "full_path": "/usr/bin/ssh", "filename": "ssh", "line": "type=SYSCALL arch=c000003e syscall=59 success=yes exit=0 a0=55e7a9b0", "record_type": "EXECVE"}}
```

---

# 🔍 Direct Comparison: Mode 4 vs Mode 1

| Feature | Mode 4 (`--mode queue`) | Mode 1 (`--mode service`) |
|---|---|---|
| **SQL Queries per Cycle** | 1 discovery query (`investigations.analyses`) + 1 query per analysis | 143 queries (1 per artifact table) |
| **`investigation_id` Quality** | **Populated directly** from `investigations.analyses` | Often empty unless present in raw table |
| **Pacing / Backfill** | **Controlled 1-by-1 (`--throttle 1.0s`)**, protects 1GB heap | Ingests all available rows in bulk per table |
| **Forensic Grouping** | Complete snapshot per machine analysis | Rows arriving in independent table chunks |
| **Crash Recovery** | Resumes at `pending[0]` from `analyses_queue.json` | Resumes at `max_id` from `artifact_state.json` |

---

*This document is saved as part of the project documentation in [simulation_with_values.md](file:///Users/mohamed-yacine/Documents/octodet-client/volcano/volcano-collector/simulation_with_values.md).*
