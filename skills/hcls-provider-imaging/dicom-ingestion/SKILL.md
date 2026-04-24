---
name: dicom-ingestion
description: "End-to-end DICOM metadata ingestion pipeline on Snowflake. Multi-table normalized loading, cascading Dynamic Tables, Snowpipe auto-ingest, Stream/Task orchestration, error handling, CDC patterns, and data validation. Use when: ingest DICOM, imaging pipeline, load images, PACS integration, stage DICOM, stream images, dynamic table imaging, Snowpipe DICOM, CDC imaging, dead letter queue."
parent_skill: hcls-provider-imaging
---

# DICOM Data Ingestion Pipeline

## When to Load

Healthcare-imaging router routes here when intent matches INGEST.

## Prerequisites

- Database `{database}` with schema `{schema}`
- Role with CREATE STAGE, CREATE TABLE, CREATE DYNAMIC TABLE, CREATE STREAM, CREATE TASK, CREATE PIPE privileges
- For schema creation: run `dicom-parser/SKILL.md` first (19-table model)
- Storage integration for external stages (S3/Azure/GCS)

## SQL References

All DDLs are in the `references/` directory:
- **`references/stage_and_raw.sql`** — Stage setup, file formats, raw landing table, batch COPY INTO
- **`references/cascading_dynamic_tables.sql`** — 5 cascading DTs + validation summary DT
- **`references/stream_task_snowpipe.sql`** — Snowpipe, stream/task tree, error table, CDC upserts, validation queries

## Created Objects

| Object | Name |
|--------|------|
| Raw Table | `DICOM_RAW` (change-tracked) |
| Stream | `DICOM_RAW_STREAM` |
| Tasks | `TASK_PROCESS_DICOM_RAW` (5min), `TASK_REFRESH_SEARCH_CORPUS` (60min) |
| Dynamic Tables | `DT_DICOM_PATIENTS`, `DT_DICOM_STUDIES`, `DT_DICOM_SERIES`, `DT_DICOM_INSTANCES`, `DT_DICOM_EQUIPMENT` |
| Stage/Formats | `DICOM_STAGE`, `DICOM_JSON_FORMAT`, `DICOM_CSV_FORMAT` |

## Step 1: Choose Ingestion Pattern

**Ask** user:

```
Select ingestion pattern:
1. One-time batch     — COPY INTO for historical backfill
2. Cascading DTs      — Dynamic Tables for continuous normalized ingestion
3. Stream/Task        — Event-driven with task tree dependencies
4. Snowpipe           — Auto-ingest from cloud storage on file arrival
```

| Pattern | Best For | Latency | Complexity |
|---------|----------|---------|------------|
| Batch | Historical backfill | N/A | Low |
| Cascading DTs | Continuous multi-table normalization | 5-15 min | Medium |
| Stream/Task | Conditional logic, error routing | 1-5 min | High |
| Snowpipe | Cloud storage auto-ingest | ~1 min | Medium |

MANDATORY STOPPING POINT: Confirm pattern before proceeding.

## Step 2: Stage Setup

Create stage, file formats from `references/stage_and_raw.sql`. For external stages, adapt URL/integration per cloud provider.

## Step 3: Raw Landing Table

Create `DICOM_RAW` with change tracking from `references/stage_and_raw.sql`. Run batch COPY INTO.

MANDATORY STOPPING POINT: Verify row counts.

## Step 4: Multi-Table Normalized Ingestion (Cascading Dynamic Tables)

Cascade: Patient → Study → Series → Instance → Equipment. Each DT deduplicates on natural keys using GROUP BY or QUALIFY, with HASH() surrogate keys.

Create all 5 cascading DTs from `references/cascading_dynamic_tables.sql`.

```
DICOM_RAW --> DT_DICOM_PATIENTS (L1)
                  --> DT_DICOM_STUDIES (L2)
                        --> DT_DICOM_SERIES (L3)
                              --> DT_DICOM_INSTANCES (L4)
                              --> DT_DICOM_EQUIPMENT (L3)
```

MANDATORY STOPPING POINT: Verify cascade refresh states.

```sql
SELECT TABLE_NAME, REFRESH_STATE, LAST_REFRESH_TIME
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE SCHEMA_NAME = '{schema}' ORDER BY TABLE_NAME;
```

## Step 5: Snowpipe Auto-Ingest

Create `DICOM_INGEST_PIPE` from `references/stream_task_snowpipe.sql`. For Azure use `INTEGRATION = 'AZURE_EVENT_NOTIFICATION'`; for GCS use `INTEGRATION = 'GCS_PUB_SUB_NOTIFICATION'`.

## Step 6: Stream/Task Orchestration

Create stream + task tree from `references/stream_task_snowpipe.sql`.

```
TASK_PROCESS_DICOM_PATIENTS (root, 5 min, stream-gated)
    +--> TASK_PROCESS_DICOM_STUDIES --> TASK_PROCESS_DICOM_SERIES
    +--> TASK_ROUTE_INGESTION_ERRORS (parallel)
```

Resume bottom-up: `ALTER TASK <leaf> RESUME;` then root last.

MANDATORY STOPPING POINT: Verify task execution history.

## Step 7: Error Handling & Dead Letter Queue

Create `DICOM_INGESTION_ERRORS` from `references/stream_task_snowpipe.sql`.

| Category | Cause | Resolution |
|----------|-------|------------|
| `PARSE_ERROR` | Malformed JSON | Fix source export, re-stage |
| `VALIDATION_ERROR` | Missing PatientID/StudyInstanceUID | Check PACS config |
| `DUPLICATE` | SOPInstanceUID exists | Skip or CDC upsert |
| `TYPE_CAST_ERROR` | Date/number conversion | Review tag format |
| `REFERENTIAL_ERROR` | Orphaned series/instance | Re-ingest parent |

Use `TRY_TO_DATE()`, `TRY_TO_NUMBER()`, `TRY_TO_TIMESTAMP_NTZ()`, `TRY_PARSE_JSON()` throughout all SQL.

## Step 8: CDC Pattern for Study Updates

Handle re-reads, amendments, corrections via MERGE upserts from `references/stream_task_snowpipe.sql`.

For DT pipelines, deduplication is handled by QUALIFY in DT_DICOM_INSTANCES (Step 4).

## Step 9: Data Validation

Run validation queries from `references/stream_task_snowpipe.sql` (referential integrity, completeness scoring, cross-table consistency).

Create `DT_DICOM_VALIDATION_SUMMARY` from `references/cascading_dynamic_tables.sql`.

MANDATORY STOPPING POINT: Review validation. Zero orphans and low error count required.

## Stopping Points

- After Step 1: Confirm ingestion pattern
- After Step 3: Verify raw table row counts
- After Step 4: Confirm DT cascade refresh states
- After Step 6: Verify task execution history
- After Step 9: Review validation summary

## Output

- `DICOM_RAW` with change tracking
- Cascading DTs: `DT_DICOM_PATIENTS` → `DT_DICOM_STUDIES` → `DT_DICOM_SERIES` → `DT_DICOM_INSTANCES` → `DT_DICOM_EQUIPMENT`
- `DICOM_INGEST_PIPE` (if Snowpipe selected)
- `DICOM_RAW_STREAM` + task tree (if Stream/Task selected)
- `DICOM_INGESTION_ERRORS` dead-letter queue
- `DT_DICOM_VALIDATION_SUMMARY` for continuous monitoring
- CDC upsert patterns for study amendments
