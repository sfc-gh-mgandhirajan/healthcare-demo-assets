---
name: derived-build
description: "Phase 3 of the Patient Experience Command Center deployment. Renders the derived data product objects (DENORM / MDL / voice / SV / Cortex Search / agent / VW) from templates bound to the approved conformance tables, deploys them in dependency order, and validates. Triggers: patient experience build, derived build, generate derived data product, deploy patient experience objects, phase 3 patient experience."
---

# Phase 3 - Generate & Deploy the Derived Data Product

Turn the approved conformance tables + accelerator templates into live objects in the target schema. Deterministic core of the accelerator - no hand-written object SQL.

## Preconditions
- Phase 2 readiness gate PASSED (re-check here; refuse to build on FAIL).
- `TARGET_DATABASE.TARGET_SCHEMA` exists and the current role can create objects in it.
- A warehouse is available for dynamic-table refresh.

## Paths & connection (set once)
- `PLUGIN_DIR` - the plugin root (the directory containing `.cortex-plugin/`).
- `WORKDIR` - a fresh scratch dir: `WORKDIR=$(mktemp -d)`.
- `CONN` - the Snowflake CLI connection name (`snow ... -c "$CONN"`).
- `CONFIG` - path to the customer's `config.json`.

## Build engine
1. Load `$PLUGIN_DIR/assets/build_manifest.yaml` - the ordered list of target objects (`object_name`, `object_type`, `template`, `grain`, `consumes`, `depends_on`, `readiness`, `params`).
2. **Dump the approved bindings** from the 3 conformance tables:
   ```bash
   python3 "$PLUGIN_DIR/assets/renderer/dump_bindings.py" --conn "$CONN" \
     --database <TARGET_DATABASE> --schema <TARGET_SCHEMA> --out "$WORKDIR/bindings"
   ```
3. **Render** every manifest object to SQL DDL (deterministic Jinja, `StrictUndefined`). Do NOT hand-author the DDL:
   ```bash
   python3 "$PLUGIN_DIR/assets/renderer/render.py" --bindings "$WORKDIR/bindings" \
     --manifest "$PLUGIN_DIR/assets/build_manifest.yaml" --templates "$PLUGIN_DIR/assets/templates" \
     --out "$WORKDIR/rendered" --config "$CONFIG"
   ```
4. **Deploy** each rendered artifact in `depends_on` order by executing the file directly (idempotent `CREATE OR REPLACE`):
   ```bash
   snow sql -c "$CONN" -f "$WORKDIR/rendered/<OBJECT_NAME>.sql"
   ```
   Never paste or hand-edit the SQL - the deployed object must be the rendered artifact.

## ML tier (object_type `notebook`) - environment-specific compute, resolved at deploy time
The XGBoost risk model trains in a headless **Notebook Project Object (NPO)** - the documented Snowflake
Workspaces production path. `object_type: notebook` (and its `object_type: stage` companion `PX_NOTEBOOKS`)
render + deploy like any other object, with two extras the skill owns:

1. **Resolve environment-specific compute into `config.json` (do NOT hardcode).** Compute pool name and
   Container Runtime version vary per customer. Before rendering the NPO, discover + validate and write the
   chosen values into `$CONFIG` (keys `compute_pool`, `runtime`); the template reads them via
   `cfg.get('compute_pool', params.compute_pool)` with the manifest `params` as fallback defaults.
   ```bash
   # pick a NOTEBOOK-eligible pool the role can use; create a small CPU pool if none exists
   snow sql -c "$CONN" -q "SHOW COMPUTE POOLS"          # inspect names/state/ALLOWED workloads
   # if none usable:
   # snow sql -c "$CONN" -q "CREATE COMPUTE POOL IF NOT EXISTS PX_ML_POOL MIN_NODES=1 MAX_NODES=1 INSTANCE_FAMILY=CPU_X64_S AUTO_SUSPEND_SECS=600"
   ```
   Set a valid `runtime` (e.g. a current `Vx.y-CPU-PYx.yy`); if `EXECUTE NOTEBOOK PROJECT` reports an
   invalid runtime, the error lists valid versions - update `config.json` and re-render (do not edit the
   rendered SQL). `QUERY_WAREHOUSE` comes from the existing `warehouse` config key.
2. **PUT the notebook `ml_assets` to the NPO stage** (from the manifest object's `ml_assets` list) AFTER
   deploying `PX_NOTEBOOKS` and BEFORE deploying `PX_TRAIN_RISK_MODEL`:
   ```bash
   snow sql -c "$CONN" -q "PUT 'file://$PLUGIN_DIR/assets/ml/train_risk_model.ipynb' @<TARGET_DATABASE>.<TARGET_SCHEMA>.PX_NOTEBOOKS OVERWRITE=TRUE AUTO_COMPRESS=FALSE"
   snow sql -c "$CONN" -q "PUT 'file://$PLUGIN_DIR/assets/ml/environment.yml'        @<TARGET_DATABASE>.<TARGET_SCHEMA>.PX_NOTEBOOKS OVERWRITE=TRUE AUTO_COMPRESS=FALSE"
   ```
   Then `snow sql -c "$CONN" -f "$WORKDIR/rendered/PX_TRAIN_RISK_MODEL.sql"` runs CREATE + EXECUTE NOTEBOOK
   PROJECT headless on Container Runtime (snowflake-ml-python / xgboost / scikit-learn preinstalled). It
   blocks until training + model registration + `MDL_DRIVER_IMPORTANCE` write complete.
   (If a legacy `NOTEBOOK` object of the same name exists from an earlier build, `DROP NOTEBOOK IF EXISTS`
   it first - a `NOTEBOOK PROJECT` cannot share the name.)

## Build order (from manifest depends_on)
`DENORM_* + CALL_TRANSCRIPT` -> `COMMENT_ENRICHED` -> `COMMENT_SEARCH` -> `MDL_*` (star/journey/points-lost/equity/driver) -> `FS_PX_RISK_FEATURES` -> (train + register `MDL_PX_RISK_MODEL`) -> `MDL_DISSAT_RISK` -> `DT_ACTION_WORKQUEUE` + `ACTION_EVENT_LOG` -> `SV` + `AGT` -> `MDL_TAB_INSIGHTS` -> `VW_*` -> `GRANT_APP_ACCESS`.

## Validate
Run `$PLUGIN_DIR/assets/validate/checks.sql` (with `{{TARGET_DATABASE}}`/`{{TARGET_SCHEMA}}` substituted) after deploy. Returns `CHECK_NAME | STATUS | DETAIL`:
- **FAIL** (must fix): required-backed objects empty (survey DENORM, MDL_STAR_RATING), composite/star out of range.
- **WARN** (informational): optional-backed objects empty (grievance/callback/equity/voice/risk) - expected when the provider lacks that source.
Report the table; proceed only when there are no FAILs.

## Hand-off
On PASS, tell the user to run `/patient-experience-command-center:app-deploy`.

> This skill + the manifest, templates, and renderer are the make-or-break asset. Build and harden first; validate against the reference sandbox.
