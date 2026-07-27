---
name: derived-build
description: "Phase 3 of the Pharmacy Cost Command Center deployment. Renders the derived data product objects (DENORM / MDL / COVERAGE_POLICY / VW / semantic view / Cortex Search / agent) from templates bound to the customer's approved conformance tables, then deploys them in dependency order and validates. Triggers: pharmacy cost build, derived build, generate derived data product, deploy pharmacy objects, phase 3 pharmacy."
---

# Phase 3 — Generate & Deploy the Derived Data Product

Turn the approved conformance tables + accelerator templates into live objects in the target schema. This is the deterministic core of the accelerator.

## Preconditions
- Phase 2 readiness gate PASSED (re-check it here; refuse to build on FAIL).
- `TARGET_DATABASE.TARGET_SCHEMA` exists and the current role can create objects in it.
- A warehouse is available for dynamic-table refresh.

## Paths & connection (set once)
Commands use shell variables so this works wherever the plugin is installed:
- `PLUGIN_DIR` - the plugin root (the directory containing `.cortex-plugin/`).
- `WORKDIR` - a fresh scratch dir: `WORKDIR=$(mktemp -d)`.
- `CONN` - the Snowflake CLI connection name (`snow ... -c "$CONN"`).
- `CONFIG` - path to the customer's `config.json`.

## Build engine
1. Load `$PLUGIN_DIR/assets/build_manifest.yaml` — the ordered list of target objects. Each entry declares:
   `object_name`, `object_type` (dynamic_table | table | view | semantic_view | cortex_search | agent), `template` (file under `assets/templates/`), `grain`, `consumes` (canonical element IDs + relationship IDs), `params` (e.g., MDL assumptions, TARGET_LAG), `depends_on`, and `readiness` (required contract IDs).
2. **Dump the approved bindings** from the 3 conformance tables to the JSON the renderer consumes (deterministic, APPROVED-only):
   ```bash
   python3 "$PLUGIN_DIR/assets/renderer/dump_bindings.py" --conn "$CONN" \
     --database <TARGET_DATABASE> --schema <TARGET_SCHEMA> --out "$WORKDIR/bindings"
   ```
   This writes `elements.json` / `relationships.json` / `crosswalk.json` (element ID -> physical `SOURCE_OBJECT.SOURCE_COLUMN`; relationship ID -> physical join columns; coded element -> `SOURCE_VALUE`/`CANONICAL_VALUE`). If Phase 2's render preflight already produced these, reuse them.
3. Run `render.py` (deterministic Jinja) over those bindings to render each template to SQL DDL. The renderer is the single source of truth for placeholder resolution — do NOT hand-author the DDL.
   ```bash
   python3 "$PLUGIN_DIR/assets/renderer/render.py" --bindings "$WORKDIR/bindings" \
     --manifest "$PLUGIN_DIR/assets/build_manifest.yaml" --templates "$PLUGIN_DIR/assets/templates" \
     --out "$WORKDIR/rendered" --config "$CONFIG"
   ```
4. Deploy in `depends_on` order using `CREATE OR REPLACE` (idempotent) via `snow sql -c "$CONN"`. Default target for iterative testing: `<TARGET_SCHEMA>` or a sandbox suffix if the user requests one.

## Build order (from manifest)
`DENORM_*` + `COVERAGE_POLICY` -> `MDL_*` -> `SV` + `COVERAGE_POLICY_CHUNKS` + `CSS` + `AGT` -> `ACTION_QUEUE` + `ACTION_EVENT_LOG` -> `VW_*`.

## Validate
Run `$PLUGIN_DIR/assets/validate/checks.sql` (with `{{TARGET_DATABASE}}`/`{{TARGET_SCHEMA}}` substituted, via `snow sql -c "$CONN"`) after deploy. It returns `CHECK_NAME | STATUS | DETAIL`:
- **FAIL** (must fix): required-backed views empty (`VW_KPI_SUMMARY`, `VW_PMPM_TREND`, `VW_SPEND_BY_BENEFIT`, `VW_HIGH_COST_CLASSES`, `VW_TREND_PVM`, `VW_TREND_CONTRIBUTORS`), `NET_PMPM_CURRENT` null/non-positive, or `MDL_TAB_INSIGHTS` != 4 rows.
- **WARN** (informational, not a failure): optional-backed views empty (PA / rebate / formulary / adherence / waste / bridge) — expected when the customer lacks that source; and any value outside the advisory PMPM band.
Report the table. Proceed only when there are no FAILs (WARNs are fine).

## Hand-off
On PASS, tell the user to run `/pharmacy-cost-command-center:app-deploy`.

> This skill (plus the manifest, templates, and renderer) is the make-or-break asset — built and hardened first (milestone M2), validated against the reference golden tables.
