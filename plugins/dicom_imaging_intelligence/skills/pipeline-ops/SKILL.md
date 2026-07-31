---
name: pipeline-ops
description: "Operate the running DICOM ingestion pipeline: resume or suspend the task DAG, monitor outstanding work per source, retry quarantined files, reconcile deleted files, and adjust the GPU business-hours window. Triggers: dicom pipeline status, resume dicom pipeline, suspend dicom tasks, dicom quarantine, retry failed dicom, dicom monitoring, reconcile deletes, gpu window, dicom pipeline not running, dicom tasks."
---

# Operate the DICOM pipeline

Day-two operations: start and stop the schedule, see what is outstanding, and
deal with failures.

## Paths & connection (set once)

```bash
CONN="<snowflake connection name>"
DB="<target_database>"
SCHEMA="<target_schema>"
```

Run everything as the `owner_role` from config.

## Monitor first

Always start here — it tells you whether there is a problem at all:

```sql
SELECT * FROM V_DICOM_PIPELINE_STATUS;
```

| Column | Meaning | What to do if non-zero |
|---|---|---|
| `PENDING_METADATA` | discovered, not parsed | normal between runs; persistent means the DAG is suspended or failing |
| `PENDING_BASE64` | parsed, no image rendered | same |
| `PENDING_EMBEDDING` | rendered, no vector | expected while the GPU service is outside its window |
| `QUARANTINED` | failed a step | inspect; see below |
| `AWAITING_RECONCILE` | gone from the source | run `SP_RECONCILE_DELETES` |

```sql
SELECT * FROM V_DICOM_LOAD_ERRORS_RECENT;
SHOW TASKS IN SCHEMA IDENTIFIER(:db_schema);
```

## Start and stop the schedule

Order matters, so it is encoded in procedures rather than left to whoever is
running it. Snowflake requires a DAG's children resumed before its root, and the
root suspended before its children:

```sql
CALL SP_RESUME_PIPELINE();    -- children first, then root
CALL SP_SUSPEND_PIPELINE();   -- root first
```

MANDATORY STOPPING POINT: `SP_RESUME_PIPELINE` starts recurring spend. Confirm
the schedule is what the user wants first — in particular, the default 5-minute
scan is set for live ingest and is pure waste against the bundled IDC demo
source, which is static public data that will never change.

To change the interval:

```sql
ALTER TASK TASK_DICOM_SCAN SUSPEND;
ALTER TASK TASK_DICOM_SCAN SET SCHEDULE = '60 MINUTE';
ALTER TASK TASK_DICOM_SCAN RESUME;
```

Scan cost scales with the number of objects under each prefix, so on a large
bucket the fix is narrower prefixes, not a longer interval.

## Run a step by hand

Each is safe to call repeatedly and only does outstanding work:

```sql
CALL SP_SCAN_ALL_SOURCES();          -- discover new / changed (md5) / deleted
CALL SP_LOAD_METADATA(5000);         -- parse NEW
CALL SP_REFRESH_SERIES_REGISTRY();   -- rebuild the derived registry
CALL SP_CONVERT_IMAGES(2000);        -- render base64
CALL SP_EMBED_PENDING(2000);         -- GPU embed
CALL SP_RECONCILE_DELETES();         -- purge downstream rows for vanished files
```

The batch-limit argument bounds cost per call. Raise it for a deliberate
one-shot backfill; leave it low for background runs.

## Quarantined files

A file that cannot be parsed or rendered lands in `DICOM_LOAD_ERRORS` and is
marked `ERROR`. One bad file never aborts a batch.

```sql
SELECT PIPELINE_STEP, FILE_NAME, ERROR_MESSAGE, OCCURRED_AT
FROM V_DICOM_LOAD_ERRORS_RECENT;
```

`PIPELINE_STEP` is `METADATA` (tag extraction) or `BASE64` (pixel rendering).
Common causes: not actually a DICOM file, a compressed transfer syntax pydicom
cannot decode, or no `SERIES_INSTANCE_UID` tag.

After fixing the source file, put it back in the queue:

```sql
UPDATE DICOM_FILE_MANIFEST SET STATUS = 'NEW'
 WHERE SOURCE_NAME = '<source>' AND STATUS = 'ERROR';

UPDATE DICOM_LOAD_ERRORS SET RESOLVED = TRUE
 WHERE SOURCE_NAME = '<source>' AND NOT RESOLVED;
```

Re-queuing also clears that file's stale metadata, image, and embedding rows on
the next parse, so the whole chain regenerates rather than serving values derived
from the previous contents.

## Deletes

The scan marks vanished files `DELETED` but does not remove anything. That split
is deliberate: a misconfigured prefix can mark a lot of files deleted at once,
and it is better for that to be visible before rows are purged.

```sql
SELECT SOURCE_NAME, COUNT(*) FROM DICOM_FILE_MANIFEST
WHERE STATUS = 'DELETED' GROUP BY 1;
```

If that count is a surprise, check `DICOM_SOURCE_PREFIX` before reconciling —
a disabled prefix marks its files deleted. Otherwise:

```sql
CALL SP_RECONCILE_DELETES();
CALL SP_REFRESH_SERIES_REGISTRY();   -- drops any now-empty series
```

## The GPU window

```sql
SHOW TASKS LIKE 'TASK_GPU_%' IN SCHEMA IDENTIFIER(:db_schema);
```

Defaults: resume 07:00 Mon-Fri, suspend 19:00 daily. Suspend runs every day
including weekends as a backstop, so a manual Saturday resume still gets cleaned
up rather than running until Monday.

```sql
ALTER TASK TASK_GPU_RESUME SUSPEND;
ALTER TASK TASK_GPU_RESUME SET SCHEDULE = 'USING CRON 0 8 * * MON-FRI Europe/London';
ALTER TASK TASK_GPU_RESUME RESUME;
```

Outside the window `SP_EMBED_PENDING` returns `SKIPPED` and leaves work queued
rather than failing — otherwise a nightly run would count toward
`SUSPEND_TASK_AFTER_NUM_FAILURES` and eventually disable the task.

To stop GPU spend right now:

```sql
ALTER SERVICE <DB>.<SCHEMA>.<service_name> SUSPEND;
```

Check for idle nodes too — a pool in state `IDLE` with non-zero `idle_nodes` is
still billing, and `MIN_NODES` above 0 prevents it scaling to zero:

```sql
SHOW COMPUTE POOLS LIKE 'DICOM_%';
```

## Task failures

```sql
SELECT NAME, STATE, SCHEDULED_TIME, ERROR_CODE, ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(RESULT_LIMIT => 50))
WHERE DATABASE_NAME = '<DB>' ORDER BY SCHEDULED_TIME DESC;
```

A task that hit `SUSPEND_TASK_AFTER_NUM_FAILURES` (3 on the root) is suspended
and will not retry until resumed. Fix the cause, then `SP_RESUME_PIPELINE`.

## Hand-off

- `/dicom-imaging-intelligence:onboard-source` — add another source
- `/dicom-imaging-intelligence:teardown` — remove everything
