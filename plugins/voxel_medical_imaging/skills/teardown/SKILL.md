---
name: teardown
description: Remove Voxel from a Snowflake account, or stop it billing without deleting it. Use when an evaluation is over, when costs need to stop immediately, or to verify nothing was left behind. Triggers - teardown, remove voxel, uninstall, stop billing, clean up, drop everything, what is still costing money.
---

# Tear down Voxel

Two different requests, and conflating them is expensive in one direction and
destructive in the other.

## "Stop it billing" — reversible, do this first

Nothing is deleted. All data and the ingress URL survive.

```sql
CALL VOXEL_DB.IMAGING.SP_SUSPEND_PIPELINE();
ALTER SERVICE VOXEL_DB.IMAGING.VOXEL_DICOMWEB_SVC SUSPEND;
ALTER COMPUTE POOL VOXEL_WEB_POOL SUSPEND;
ALTER COMPUTE POOL VOXEL_GPU_POOL SUSPEND;
ALTER WAREHOUSE VOXEL_WH SUSPEND;
```

This is almost always what someone actually wants when they say costs are too high
mid-evaluation. Offer it before dropping anything.

**Compute pools bill while ACTIVE or IDLE, not only while busy.** An idle pool with
no service on it still bills. `INITIALLY_SUSPENDED = TRUE` is set at creation
because the default is `FALSE`.

What still costs money after suspending:

- **Storage** for `VOXEL_PREAD_STG` (the internal copy of DICOM files) — usually the
  largest residual. On the current corpus that is **2,855 files, ~1.4 GiB**; confirm
  with `SELECT COUNT(*), SUM(SIZE) FROM DIRECTORY(@VOXEL_DB.IMAGING.VOXEL_PREAD_STG)`
  rather than assuming, since it scales with whatever was ingested
- **Storage** for images in `VOXEL_IMAGES`
- Time Travel retention on catalog tables

## Export the evidence before you drop anything

`DROP DATABASE` destroys `CONFORMANCE_RESULT` and `FRAME_LATENCY_SAMPLE`, which hold
the only record of the external harness run (23 assertions, 0 failing) and the frame
latency measurements. Those are **not cheap to recreate**: the harness needs a live
ingress and a PAT, and a PAT cannot be minted from a session that authenticates with
one — so it needs an interactive login.

```sql
SELECT * FROM VOXEL_DB.IMAGING.CONFORMANCE_RESULT;
SELECT * FROM VOXEL_DB.IMAGING.FRAME_LATENCY_SAMPLE;
SELECT * FROM VOXEL_DB.IMAGING.V_CONFORMANCE_SCORE;
```

Save them somewhere outside the database first. Rebuilding the platform is scripted and
takes minutes; re-earning the measurements takes a running service and a browser.

## Paths & connection (set once)

```bash
PLUGIN_DIR="<absolute path to this plugin>"
WORK="$(mktemp -d)"
CONN="<snowflake connection name>"
CONFIG="$PLUGIN_DIR/config.json"
```

Never write into `$PLUGIN_DIR` — the rendered teardown script goes to `$WORK`.

## "Remove it" — destructive

```bash
uv run --with jinja2 --with pyyaml python "$PLUGIN_DIR/assets/renderer/render.py" \
  --manifest "$PLUGIN_DIR/assets/build_manifest.yaml" \
  --templates "$PLUGIN_DIR/assets/templates" \
  --config "$CONFIG" --out "$WORK/rendered" --only TEARDOWN
```

The rendered script is **inert until `confirmed = TRUE`** in `config.json`. Running
it unconfirmed prints what it would drop and exits. That is a deliberate speed bump
— this drops PHI-bearing tables.

```bash
snow sql -c "$CONN" --enable-templating NONE -f "$WORK/rendered/TEARDOWN.sql"
```

### Order matters

1. Suspend tasks — a running task recreates rows mid-teardown
2. Suspend and drop the service — a running service holds the compute pool
3. Detach policies — a policy attached to a column blocks its own drop with
   `cannot be dropped/replaced as it is associated with one or more entities`
4. Drop schema objects
5. Drop the database
6. Drop compute pools — only possible once no service references them
7. Drop the EAI and network rule
8. Drop `VOXEL_RADIOLOGIST`

### Account-level objects live outside the database

Dropping `VOXEL_DB` does **not** remove these, and they are what a "we removed it"
claim usually gets wrong:

- `VOXEL_WEB_POOL`, `VOXEL_GPU_POOL` — **still billing if not suspended**
- `VOXEL_WH`
- `VOXEL_BUILD_EAI` and its network rule
- `VOXEL_RADIOLOGIST` role
- Service role grants (dropped with the service)

## Verify — the part that is usually skipped

The teardown script **re-enumerates after dropping** rather than asserting success,
because the failure mode is a compute pool that survives and quietly bills for
weeks.

```sql
SHOW COMPUTE POOLS LIKE 'VOXEL%';
SHOW WAREHOUSES LIKE 'VOXEL%';
SHOW DATABASES LIKE 'VOXEL%';
SHOW ROLES LIKE 'VOXEL%';
SHOW INTEGRATIONS LIKE 'VOXEL%';
```

Every one must return zero rows. A surviving compute pool is the expensive residual;
a surviving role is untidy but free.

Also check the image repository is gone with the database:

```sql
SHOW IMAGE REPOSITORIES IN ACCOUNT;
```

## Data that is not yours to delete

The external stage points at **public IDC data**. Teardown drops the stage
definition, not the bucket — correct, since it is not ours.

But `VOXEL_PREAD_STG` is an **internal** copy of DICOM files, made because SPCS
stage volumes mount internal stages only. If a deployment pointed at customer PHI,
that internal stage contains PHI and dropping the database drops it. Confirm that is
intended before running teardown against anything other than open data.

## Partial teardown

Removing the viewer but keeping the catalog and pipeline:

```sql
DROP SERVICE VOXEL_DB.IMAGING.VOXEL_DICOMWEB_SVC;
ALTER COMPUTE POOL VOXEL_WEB_POOL SUSPEND;
DROP COMPUTE POOL VOXEL_WEB_POOL;
```

The catalog, frame plane, and policies are independent of the service and keep
working. Nothing in the ingestion path depends on the viewer existing.
