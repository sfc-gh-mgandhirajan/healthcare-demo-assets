# Pharmacy Cost Command Center — Solution Accelerator (CoCo Plugin)

Point Cortex Code at your Snowflake account and deploy the **Pharmacy Cost Command Center** on your own pharmacy + medical + membership data — no hand-written SQL.

## The methodology

Two engines joined by one seam: an **agentic onboarding** engine adapts any messy customer estate onto a **Canonical Logical Contract**, then a **deterministic delivery** engine renders a certified, composable derived product — with **zero rows of data moved**.

- 📄 **Framework overview:** [`docs/Agentic_Solution_Accelerator_Framework.pdf`](docs/Agentic_Solution_Accelerator_Framework.pdf)

## What it does

The accelerator ships a **canonical logical contract** (the business elements, join graph, and vocabulary the solution needs) plus **build templates** and the **app**. You supply only the mapping from your source to that contract. The plugin then generates and deploys everything downstream.

## Run order (4 phases)

| Phase | Skill | What happens |
|---|---|---|
| 1 | `/pharmacy-cost-command-center:conformance-discover` | Profiles your source and proposes rows for the 3 conformance tables (elements, relationships, code crosswalk). |
| 2 | `/pharmacy-cost-command-center:conformance-review` | Iterative review loop: you approve/adjust mappings; a readiness gate confirms all required elements are mapped. |
| 3 | `/pharmacy-cost-command-center:derived-build` | Renders the derived data product objects (DENORM / MDL / COVERAGE_POLICY / VW / SV / Cortex Search / Agent) from templates bound to your approved contract, and deploys them. |
| 4 | `/pharmacy-cost-command-center:app-deploy` | Deploys the bundled Next.js app (App Runtime / SPCS) on the derived layer via `snow app deploy`, bakes the data target into the app, grants read access, and smoke-tests it. |

## Prerequisites

- A role with CREATE DATABASE / SCHEMA / TABLE / DYNAMIC TABLE / VIEW / SEMANTIC VIEW privileges (or a pre-created target DB.SCHEMA with USAGE + CREATE).
- A warehouse for dynamic-table refresh.
- Source pharmacy claims, medical claims, membership, and (optionally) clinical/UM/reference data.
- Coverage-policy PDFs staged (optional; enables policy citations and appropriateness).

### Per-account prerequisites (verify before running on a NEW account)
The Snowflake CLI can't fully verify these; confirm them on the target account first. Run `assets/validate/preflight_account.sql` for a best-effort automated check.

- **Snowflake CLI + connection**: `snow` installed and a named connection to the target account (used as `-c <conn>` throughout). `snow app setup --help` must succeed (Phase 4).
- **Cortex availability in the target region**:
  - Cortex Analyst + a Cortex Agent (the agent uses `orchestration: auto`, so it adapts to available models).
  - `AI_COMPLETE` with the `insight_format_model` in `config.json` (default `claude-opus-4-8`); override it if that model isn't in the region.
  - Cortex Search (for the coverage-policy service).
- **Snowflake App Runtime enabled (Phase 4 only)**: an available **compute pool** and an **external access integration** for the container build. If `snow app deploy` reports these missing, that's an account-setup task, not a plugin defect.
- **Roles**: the role that runs `snow app deploy` becomes the app's owner and the grantee for `GRANT_APP_ACCESS`; it must be able to read the derived objects (or already own them).

## Install from GitHub

This plugin is a standard CoCo plugin: `.cortex-plugin/plugin.json` + `skills/` + `agents/` + `assets/` at the plugin root (skills and agents are auto-discovered — no manifest list needed).

- **Repo layout**: the plugin can be the repo root, or live under a subpath (e.g. `plugins/<name>/`). Either works.
- **Install source string**: `github:<org>/<repo>[/<subpath>]#<branch>` (e.g. `github:sfc-gh-jrag/pharmacy_cost_command_center#main`). Use the CoCo catalog / `github-plugin-installer` flow to install; it resolves that source, expecting `.cortex-plugin/plugin.json` at the resolved path.
- **Publish** (from a clean checkout of the plugin dir):
  ```bash
  git init && git add . && git commit -m "Pharmacy Cost Command Center accelerator"
  gh repo create <org>/<repo> --private --source . --remote origin --push
  ```
  The bundled `.gitignore` keeps `__pycache__/`, `.DS_Store`, `node_modules/`, and `.next/` out of the repo.

## Configuration

The whole accelerator is driven by **one file**: a `config.json` you create per customer. Copy the shipped template and edit it:

```bash
cp assets/config.sample.json config.json   # then edit the values below
```

The renderer (`assets/renderer/render.py`) reads it as strict JSON, so your `config.json` must have **no comments**. Object names (VW_*, MDL_*, `SV_...`, `CSS_...`, `AGT_PHARMACY_COST_COMMAND_CENTER`) are accelerator-owned and identical for every customer — they are baked into the templates, not this file.

| Key | Required | Phase | Meaning |
|---|---|---|---|
| `target_database` | Yes | 1,3,4 | Database where the derived data product + app object are created. |
| `target_schema` | Yes | 1,3,4 | Schema holding the accelerator-owned objects (fixed names). |
| `warehouse` | Yes | 3,4 | Warehouse for dynamic-table refresh and app queries. |
| `target_lag` | No (default `1 hour`) | 3 | Dynamic-table / Cortex Search refresh lag. |
| `insight_format_model` | No (default `claude-opus-4-8`) | 3 | Model that structures `MDL_TAB_INSIGHTS`. Change if your region lacks it. |
| `cardinality_profile` | No | 3 | `{relationship_id: max_rows_per_key}` measured in Phase 1. A value `> 1` makes the renderer de-dup that join (prevents fan-out inflating spend). Omit to fall back to the relationship's declared cardinality. |
| `app_name` | No (default `PHARMACY_COST_COMMAND_CENTER`) | 4 | Snowflake App object name. |
| `app_stage` | No (default `PHARMACY_COST_COMMAND_CENTER_CODE`) | 4 | Code stage (inside `target_schema`) for the app artifacts. |
| `app_role` | Yes (for grants) | 4 | Grantee in `GRANT_APP_ACCESS`. Must be the role that **owns** the Application Service (= the role running `snow app deploy`), because the app queries with owner's rights. The deploy skill derives this from `SHOW APPLICATION SERVICES` (the `owner` column) and overrides this value; set it as a placeholder. No-op if that role already owns the derived objects. |

## Layout

```
pharmacy-cost-command-center/
  .cortex-plugin/plugin.json
  skills/
    conformance-discover/SKILL.md
    conformance-review/SKILL.md
    derived-build/SKILL.md
    app-deploy/SKILL.md
  agents/conformance-mapper.md
  assets/
    contract/          # canonical logical contract (accelerator-owned):
                       #   logical_elements.json, logical_objects.json,
                       #   logical_relationships.json, canonical_vocabulary.json,
                       #   conformance_tables.sql
    templates/         # Jinja SQL templates for the derived objects
    build_manifest.yaml
    config.sample.json # copy -> config.json, edit per customer (see Configuration)
    renderer/
      render.py        # bindings + templates -> SQL DDL
      dump_bindings.py # APPROVED conformance tables -> elements/relationships/crosswalk JSON
    validate/
      readiness_gate.sql # Phase 2 structural gate (PASS/FAIL)
      checks.sql         # Phase 3 post-build validation (PASS/FAIL/WARN)
    app/               # app source (Next.js App Runtime) + snowflake.yml.j2
```

## Accelerator-owned vs customer-specific

- **Accelerator-owned (ships in this plugin):** logical contract, SQL/agent templates, MDL algorithms, semantic view + agent definitions, the app, and `build_manifest.yaml`.
- **Customer-specific (produced at runtime):** the 3 populated conformance tables + your policy docs. Nothing else.

> Development note: this plugin is developed and validated in the runtime location `<workspace>/.cortex/plugins/`. Once vetted it is promoted into the accelerator's wiki `Build_artifacts/plugin/` as the versioned artifact.
