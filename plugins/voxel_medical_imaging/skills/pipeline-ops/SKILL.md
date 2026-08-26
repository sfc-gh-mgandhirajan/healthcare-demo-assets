---
name: pipeline-ops
description: Operate the Voxel DICOM ingestion pipeline - scan sources, load metadata, replay errors, reconcile deletes, resume or suspend tasks, diagnose a stalled or slow backfill. Use when files are not appearing in the catalog, a load is slow, errors need replaying, or the task graph is suspended. Triggers - pipeline stuck, backfill slow, replay errors, resume tasks, reconcile deletes, files missing from catalog, task suspended.
---

# Operate the Voxel ingestion pipeline

Two tasks: `TASK_VOXEL_SCAN` (root, enumerates stages into the manifest) then
`TASK_VOXEL_EXTRACT` (child, runs the vectorized UDTF over pending files).

## First question: is anything actually running?

```sql
SELECT * FROM VOXEL_DB.IMAGING.V_TASK_HEALTH;
SELECT STATE, COUNT(*) FROM VOXEL_DB.IMAGING.DICOM_FILE_MANIFEST GROUP BY STATE;
```

`SUSPENDED` is the **default state after deploy** — tasks are created suspended so
a deploy never starts spending money on its own. That is not a fault.

```sql
CALL VOXEL_DB.IMAGING.SP_RESUME_PIPELINE();
CALL VOXEL_DB.IMAGING.SP_SUSPEND_PIPELINE();
```

Resume the **root** only; the child resumes with it. A child task cannot carry
`SUSPEND_TASK_AFTER_NUM_FAILURES` — that is root-only and inherited.

## Manual run

```sql
CALL VOXEL_DB.IMAGING.SP_SCAN_ALL_SOURCES();      -- stage LIST -> manifest
CALL VOXEL_DB.IMAGING.SP_LOAD_METADATA(5000);     -- extract up to N pending files
```

## Diagnosing a slow backfill

The bug worth knowing about: **partition count is derived from the actual batch,
not the requested limit.** An earlier version computed buckets from `BATCH_LIMIT`,
so a 2848-file backfill with `BATCH_LIMIT = 50000` produced 1563 partitions
averaging 1.8 files each — destroying the batching the vectorized UDTF exists for.

It still completed, silently, at a fraction of the possible throughput. That is why
this needs measuring rather than trusting.

```sql
-- Partition sizing on the last extract. Files-per-partition should be ~8
-- (udtf_max_batch_size), not ~1.
SELECT QUERY_ID, PARTITIONS_TOTAL, ROWS_PRODUCED,
       ROUND(TOTAL_ELAPSED_TIME/1000.0, 1) AS SEC
FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 50))
WHERE QUERY_TEXT ILIKE '%EXTRACT_DICOM_HEADER_AND_FRAMES%'
ORDER BY START_TIME DESC;
```

Use `INFORMATION_SCHEMA.QUERY_HISTORY` for anything recent. `ACCOUNT_USAGE` lags
up to 45 minutes and SPCS metering up to 180, so a fresh run looks like it never
happened.

## Errors

```sql
SELECT ERR_CODE, ERROR_CLASS, COUNT(*) AS N
FROM VOXEL_DB.IMAGING.DICOM_LOAD_ERRORS e
JOIN VOXEL_DB.IMAGING.DICOM_ERROR_CATALOG c USING (ERR_CODE)
GROUP BY 1, 2 ORDER BY N DESC;
```

Five classes, and the class determines whether replay is pointless:

| Class | Meaning | Replay? |
|---|---|---|
| `DATA` | the file is malformed | No — fix or exclude the file |
| `CAPACITY` | file exceeded `max_object_bytes` | Only after raising the limit |
| `CONFIG` | stage/permission/path problem | Yes, after fixing config |
| `CAPABILITY` | transfer syntax not supported | No — needs code |
| `INVARIANT` | our own assumption broke | No — this is a bug, read the detail |

```sql
CALL VOXEL_DB.IMAGING.SP_REPLAY_ERROR_CLASS('CONFIG');
CALL VOXEL_DB.IMAGING.SP_REPLAY_FILE('<file_name>');
```

Replaying `DATA` or `CAPABILITY` burns compute to reproduce the same failure.

## Deletes

Object storage deletes are invisible to a scan — a scan sees what *is* there, never
what vanished.

```sql
CALL VOXEL_DB.IMAGING.SP_RECONCILE_DELETES();
```

Without this the catalog accumulates rows pointing at bytes that no longer exist,
and the viewer 503s on them with "catalogued but not on the mounted stage".

## Frame plane coverage

```sql
SELECT RESOLUTION, COUNT(*) AS N,
       SUM(CASE WHEN IS_SERVABLE THEN 1 ELSE 0 END) AS SERVABLE
FROM VOXEL_DB.IMAGING.V_FRAME_COVERAGE
GROUP BY RESOLUTION ORDER BY N DESC;
```

| RESOLUTION | Meaning |
|---|---|
| `ARITHMETIC` | offsets are closed-form; **zero rows stored** |
| `STORED` | encapsulated; offsets in `DICOM_FRAME_OFFSET` |
| `INCOMPLETE` | fewer stored offsets than frames — **not servable** |
| `UNSUPPORTED` | transfer syntax not in `DICOM_CAPABILITY` |
| `MISSING` | no offsets and not arithmetic — investigate |
| `NO_PIXELS` | header-only object; correct to have none |

`ARITHMETIC` at 100% is the expected result **for native-uncompressed instances** and
is the whole cost argument: no per-frame offset rows exist to store, prime, evict, or
invalidate on that path.

On the currently deployed corpus expect a **mix** — roughly 2,848 `ARITHMETIC` plus 7
`STORED` (`BOT` / `ITEM_SCAN` / `EOT` sources), and 43 rows in `DICOM_FRAME_OFFSET`.
That is correct, not a regression. The compressed instances were added deliberately to
exercise the stored-offset path, which had never run and turned out to contain a
uniform -8 byte Extended Offset Table error. Do not "clean up" those rows.

A related trap: `STORED` rows are the *offset* plane. The RAM frame ring is a separate
tier with its own priming job and eviction budget, and "no cache to invalidate" applies
only to offsets. If frames are slow, that is a hot-ring or ingress question, not an
offset-coverage question.

`V_CAPABILITY_GAPS` lists transfer syntaxes present in the data but absent from
`DICOM_CAPABILITY` — that is the work queue for supporting new syntaxes.

## Adding a source

```sql
INSERT INTO VOXEL_DB.IMAGING.DICOM_SOURCE_CONFIG (SOURCE_NAME, STAGE_NAME, ...) ...;
INSERT INTO VOXEL_DB.IMAGING.DICOM_SOURCE_PREFIX (SOURCE_NAME, PREFIX) ...;
```

Then add a `SOURCE_SCOPE_MAP` row, or **nobody will be entitled to see it** — the
row access policy maps entitlement through `SOURCE_NAME`, so an unmapped source is
invisible to every non-owner role.

## Cost

```sql
SELECT * FROM VOXEL_DB.BENCH.V_COST_HOURLY ORDER BY HOUR DESC LIMIT 24;
SELECT * FROM VOXEL_DB.BENCH.V_COST_FRESHNESS;
```

Check `V_COST_FRESHNESS` before quoting any number. Metering views bucket by hour,
so a run inside the current hour is under-reported and looks cheaper than it is.

Suspend the warehouse when idle:

```sql
ALTER WAREHOUSE VOXEL_WH SUSPEND;
```
