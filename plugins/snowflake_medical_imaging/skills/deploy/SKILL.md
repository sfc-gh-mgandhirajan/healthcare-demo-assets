---
name: deploy
description: Deploy Phase 1 (account, core, pipeline, bench, validate) into a Snowflake account. Renders from config.json, provisions objects, backfills the corpus, and runs the invariant checks. Triggers: deploy imaging, deploy phase 1, install imaging platform, set up medical imaging.
---

# Deploy Phase 1

Ingest, frame plane, and cost receipts. Does **not** deploy the DICOMweb service,
segmentation, or de-identification — those are later phases gated on the Phase 1
measurement.

## Preconditions

Confirm before starting, and stop if any is unmet:

1. `config.json` exists at the plugin root. If not, copy `$PLUGIN_DIR/assets/config.sample.json`
   to `$PLUGIN_DIR/config.json` and ask the user for `target_database`, `target_schema`,
   `warehouse`, `owner_role`.
2. The connection's role can reach `ACCOUNTADMIN` — the `account` phase creates
   compute pools, an image repository, a network rule, and an external access
   integration, none of which SYSADMIN can create.
3. `uv` is available locally. The renderer needs `jinja2` + `pyyaml`, and `uv run
   --with` supplies them per-invocation — do not create a virtualenv inside the
   plugin directory.

## Paths & connection (set once)

```bash
PLUGIN_DIR="<absolute path to this plugin>"
WORK="$(mktemp -d)"
CONN="<snowflake connection name>"
CONFIG="$PLUGIN_DIR/config.json"
```

Never write into `$PLUGIN_DIR` — all rendered output goes to `$WORK`.

## Cost gate — state this before provisioning

Tell the user, in plain numbers, what the account phase will create:

- 1 warehouse, size from `warehouse_size`, `INITIALLY_SUSPENDED`
- 2 compute pools, both `INITIALLY_SUSPENDED` — **no nodes, no billing until a
  service lands on them**
- 1 image repository (storage only)
- 1 network rule + 1 external access integration (no compute cost)

The backfill itself resumes the warehouse and runs the extraction UDTF over the
configured corpus. On the bundled 2,848-file IDC demo that is roughly 45–65
seconds on a MEDIUM warehouse. A customer's own corpus scales roughly linearly at
~60 files/sec per MEDIUM.

Get explicit confirmation before running the account phase.

## Steps

### 1. Render

Always render into a scratch directory. Never write into the plugin directory.

```bash
uv run --with jinja2 --with pyyaml python "$PLUGIN_DIR/assets/renderer/render.py" \
  --manifest "$PLUGIN_DIR/assets/build_manifest.yaml" \
  --templates "$PLUGIN_DIR/assets/templates" \
  --config "$CONFIG" \
  --out "$WORK/rendered" \
  --phase account core pipeline bench validate
```

The renderer uses `StrictUndefined`, so a missing config key fails here rather than
emitting `None` into a `CREATE` and failing halfway through a deploy.

**Leak check** before executing anything — catch a config that still carries the
sample values:

```bash
grep -rl 'VOXEL_DB\|voxel-dev' "$WORK/rendered" || echo "clean"
```

If the user's config intentionally uses those names, that is fine; just confirm it
is intentional rather than an unedited copy of the sample.

### 2. Account phase (ACCOUNTADMIN)

```bash
snow sql -c "$CONN" --enable-templating NONE -f "$WORK/rendered/00_ACCOUNT_SETUP.sql"
```

`--enable-templating NONE` is **required** on every `snow sql` call here: the CLI
treats `&` as a variable marker and these files contain `&` in comments.

**Verify the cost property.** The output includes a `BILLING_CHECK` column for both
pools. Both must read `OK`. Anything else means `INITIALLY_SUSPENDED` did not take
effect and the deployment is already billing for idle nodes — stop and investigate
rather than continuing.

### 3. Core, pipeline, bench

Run in numeric order. Numbering encodes dependencies; reference data (04) precedes
the frame plane (05) because `V_FRAME_COVERAGE` joins `DICOM_CAPABILITY`, and
`CREATE VIEW` validates references at creation.

```
01_DATABASE_SCHEMA  02_STAGES  03_TABLES_CATALOG  04_REFERENCE_DATA
05_FRAME_PLANE      06_UDFS    07_MASKING_POLICIES
10_PIPELINE_CONFIG  11_PIPELINE_PROCEDURES  12_PIPELINE_TASKS  13_INITIAL_BACKFILL
20_BENCH_LEDGER     21_BENCH_PROFILES
```

Checkpoints worth reading rather than skipping:

- **02** — `STAGE_VOLUME_CHECK` must be `OK`. A FAIL means `pread_stage` is not
  internal, and SPCS stage volumes cannot mount external stages. The DICOMweb phase
  would otherwise fail later at `CREATE SERVICE` with an error naming the service,
  not the stage.
- **04** — expect 16 capability rows (13 encapsulated, 3 native) and 19 error codes
  across 5 classes.
- **12** — both tasks must report `state = suspended`.
- **13** — the `RESOLUTION` distribution is the interesting output. A healthy CT
  corpus is overwhelmingly `ARITHMETIC`, which is the cost argument: those instances
  need no offset storage, no cache, and no warming job.

### 4. Validate

```bash
snow sql -c "$CONN" --enable-templating NONE -f "$WORK/rendered/CHECKS.sql"
```

15 checks. Interpret as follows:

- Any **FAIL** — stop and fix. `INVARIANT_0001` and `INVARIANT_0003` in particular
  mean the system would serve wrong pixels at HTTP 200.
- `PHI_RAW_METADATA_UNMASKED` = **WARN** is **expected in Phase 1**. Report it to
  the user explicitly rather than glossing over it: `RAW_METADATA` is unmasked, so
  `SELECT RAW_METADATA:"00100010"` returns PatientName in cleartext. The `deid`
  phase closes it. Do not describe Phase 1 as PHI-safe.
- `FRAME_ARITHMETIC_IDENTITY` = PASS is the check that keeps the sparse frame plane
  honest. If it ever fails, every native frame offset is wrong and nothing else in
  the system objects.

### 5. Report

Give the user:

- files/sec and elapsed from the `13_INITIAL_BACKFILL` output
- `SELECT * FROM BENCH.V_BENCH_FRAME_PLANE_SAVINGS` — offset rows avoided
- the CHECKS table, with the expected WARN called out as expected
- the fact that tasks are suspended and how to start them

## After deploying

```sql
CALL SP_RESUME_PIPELINE();      -- children before root
CALL SP_SUSPEND_PIPELINE();     -- root before children
```

Record a measured baseline:

```sql
CALL BENCH.SP_BENCH_INGEST('vectorized');
SELECT * FROM BENCH.V_BENCH_UNIT_ECONOMICS ORDER BY STARTED_AT DESC;
```

`COST_TOTAL` and `COST_PER_STUDY` are NULL when
`SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY` is not readable by the
current role. That is deliberate — a fabricated `$/credit` is worse than an honest
NULL. Credits are still reported. To get currency figures, run the bench with a
role that can read `ORGANIZATION_USAGE`.

## Failure modes

| Symptom | Cause |
|---|---|
| `Invalid partitioning expression; must be a column` | A call site passed an expression to `PARTITION BY`. The bucket must be materialized. |
| `ambiguous column name 'FILE_NAME'` | The table function was not aliased. |
| NULL `FILE_SIZE` on extraction output | Outer table columns are NULL on partitioned-UDTF rows. Join back on `FILE_NAME`. |
| `Cannot set parameter SUSPEND_TASK_AFTER_NUM_FAILURES on non-root task` | It is root-only; children inherit. |
| Backfill far slower than ~60 files/sec per MEDIUM | Check the partition count in the `SP_LOAD_METADATA` return string. It should be about `files / udtf_max_batch_size`. |
| Pool reports a state other than SUSPENDED after account setup | `INITIALLY_SUSPENDED` did not apply. The deployment is billing. |

## Do not

- Resume tasks without being asked. A deploy must not silently start consuming
  credits.
- Attach the build EAI to any serving service. It is build-only; a serving
  container holds a session token and PHI read access, so an EAI on it is
  unrestricted egress from inside the PHI perimeter regardless of code. Verify with
  `DESCRIBE SERVICE` — `external_access_integrations` must be null.
- Describe Phase 1 as PHI-complete. The `RAW_METADATA` gap is real and asserted.
- Report a benchmark number without its warehouse size and partition count.
