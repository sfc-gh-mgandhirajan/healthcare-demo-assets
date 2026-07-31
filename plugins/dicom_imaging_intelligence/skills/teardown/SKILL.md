---
name: teardown
description: "Remove everything the DICOM Imaging Intelligence plugin created from a Snowflake account: suspends tasks, stops services, drops the database, both compute pools, the external access integration, and the network rule. The fastest way to stop compute-pool and GPU billing after an evaluation. Triggers: dicom teardown, remove dicom, drop dicom, uninstall dicom pipeline, clean up dicom, stop dicom billing, delete imaging intelligence."
---

# Tear down the DICOM Imaging Intelligence deployment

Removes every object the plugin created. This is the fastest way to stop
compute-pool and GPU spend after an evaluation.

## This is destructive

MANDATORY STOPPING POINT — before rendering or running anything:

1. Tell the user exactly what will be dropped (list it from the config values,
   not generically).
2. Confirm they are pointed at the right account: show
   `SELECT CURRENT_ACCOUNT(), CURRENT_REGION();`
3. Ask explicitly whether the database contains anything they added themselves.
   `DROP DATABASE` takes the whole schema, including tables the plugin never
   created.

Do not proceed on an implied yes. "Clean up the demo" is not confirmation to drop
a database.

### Shared compute pools are the sharp edge

Teardown drops the pools **named in `config.json`**. If two deployments in this
account were configured with the same `app_pool` / `gpu_pool` / `eai_name` — which
is easy and often sensible, since pools are expensive — then tearing down one
deployment takes the other's compute with it.

Before running, check whether anything else is using them:

```sql
SHOW COMPUTE POOLS LIKE '<app_pool>';   -- num_services > 1 is a warning sign
SHOW COMPUTE POOLS LIKE '<gpu_pool>';
SHOW DATABASES;                          -- any other DICOM deployment here?
```

If the pools are shared, do **not** run the full teardown. Drop just the database
and leave the account-level objects alone:

```sql
DROP DATABASE IF EXISTS <target_database>;
```

That removes every schema-level object the plugin created — tables, UDFs,
procedures, tasks, views, stages, the secret, policies, search services, agent,
model, and the app — while leaving shared compute and the EAI for the other
deployment.

## Paths & connection (set once)

```bash
PLUGIN_DIR="<absolute path to this plugin>"
CONN="<snowflake connection name>"
CONFIG="$PLUGIN_DIR/config.json"
WORKDIR="$(mktemp -d)"
```

## What gets dropped

| Object | From config |
|---|---|
| Database (and everything in it) | `target_database` |
| App compute pool | `app_pool` |
| GPU compute pool | `gpu_pool` |
| External access integration | `eai_name` |
| Network rule | inside the database |
| Secret, stages, UDFs, procedures, tasks, views, agent, model, service | inside the database |

Kept unless explicitly opted in:

| Object | Why |
|---|---|
| Warehouse (`warehouse`) | `COMPUTE_WH` and similar are commonly shared; dropping one can break unrelated work |
| Workspace (`workspace_name`) | may contain the user's own notebooks |

## Step 1: Render

```bash
uv run --with jinja2 --with pyyaml python "$PLUGIN_DIR/assets/renderer/render.py" \
  --manifest "$PLUGIN_DIR/assets/build_manifest.yaml" \
  --templates "$PLUGIN_DIR/assets/templates" \
  --out "$WORKDIR/rendered" --config "$CONFIG" --phase teardown
```

Teardown is its own phase precisely so no deploy path can reach it.

## Step 2: Dry run

The rendered script is **inert by default**. Run it as-is first — it reports what
it would drop and changes nothing:

```bash
snow sql -f "$WORKDIR/rendered/TEARDOWN.sql" -c "$CONN" \
    --role ACCOUNTADMIN --enable-templating NONE
```

Show the user that output. It is the authoritative list.

## Step 3: Arm and run

Only after explicit confirmation, flip the guard in the rendered file:

```bash
sed -i '' "s/confirmed       BOOLEAN DEFAULT FALSE/confirmed       BOOLEAN DEFAULT TRUE/" \
    "$WORKDIR/rendered/TEARDOWN.sql"
```

To also drop the warehouse or workspace, set `drop_warehouse` / `drop_workspace`
to `TRUE` the same way — but ask first, separately.

```bash
snow sql -f "$WORKDIR/rendered/TEARDOWN.sql" -c "$CONN" \
    --role ACCOUNTADMIN --enable-templating NONE
```

Needs `ACCOUNTADMIN`: compute pools, the EAI, and the network rule cannot be
dropped by `SYSADMIN`.

## Order, and why it matters

The script already does this; it is documented so a partial failure is readable.

1. **Suspend tasks** — stop new work before removing its targets.
2. **`ALTER COMPUTE POOL ... STOP ALL`** — a pool with live services cannot be
   dropped. This also stops the Streamlit and GPU containers.
3. **Drop the EAI** — it holds a reference to the secret inside the database, so
   it goes before the database rather than being orphaned.
4. **Drop the database** — cascades tables, UDFs, procedures, tasks, views,
   stages, the secret, policies, search services, agent, model, and the app.
5. **Drop the compute pools** — now that nothing runs on them.

## Step 4: Verify

The script ends with three counts that should all be zero. If a compute pool
survives, something is still running on it:

```sql
SHOW COMPUTE POOLS LIKE 'DICOM_%';
```

A pool stuck with `num_services > 0` after the database is gone usually means a
service in another schema is using it. `ALTER COMPUTE POOL <name> STOP ALL` then
retry the drop.

## Partial teardown

Often what the user actually wants. Offer these before dropping everything:

```sql
-- Stop all recurring spend, keep all data:
CALL SP_SUSPEND_PIPELINE();
ALTER SERVICE <DB>.<SCHEMA>.<service_name> SUSPEND;
ALTER COMPUTE POOL <gpu_pool> SUSPEND;
ALTER COMPUTE POOL <app_pool> SUSPEND;

-- Drop just the GPU side (the expensive part), keep the pipeline and app:
DROP SERVICE IF EXISTS <DB>.<SCHEMA>.<service_name>;
DROP MODEL IF EXISTS <DB>.<SCHEMA>.<model_name>;
ALTER COMPUTE POOL <gpu_pool> SUSPEND;
```

Suspending gets a demo account to near-zero cost while keeping everything
recoverable, which is usually the right answer between demos.
