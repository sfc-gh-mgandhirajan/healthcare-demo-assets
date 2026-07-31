# Patient Experience Command Center — Solution Accelerator (CoCo Plugin)

Point Cortex Code at your Snowflake account and deploy the **Patient Experience Command Center** on your own patient-experience data (HCAHPS survey, patient comments, calls, grievances, callbacks, unit operations) — no hand-written SQL.

## What it does

The accelerator ships a **canonical logical contract** (the business elements, join graph, and vocabulary the solution needs), **build templates** (including native Cortex AISQL voice enrichment and a custom XGBoost detractor/grievance-risk model with SHAP), and the **app**. You supply only the mapping from your source to that contract. The plugin then generates and deploys everything downstream, then stands up the command-center app.

## Run order (4 phases)

| Phase | Skill | What happens |
|---|---|---|
| 1 | `/patient-experience-command-center:conformance-discover` | Profiles your source and proposes rows for the 3 conformance tables (elements, relationships, code crosswalk). |
| 2 | `/patient-experience-command-center:conformance-review` | Iterative review loop: you approve/adjust mappings; a readiness gate confirms all required elements are mapped. |
| 3 | `/patient-experience-command-center:derived-build` | Renders the derived data product objects (DENORM / MDL / voice enrichment / risk model / SV / Cortex Search / Agent / VW) from templates bound to your approved contract, and deploys them. |
| 4 | `/patient-experience-command-center:app-deploy` | Deploys the bundled Next.js app (App Runtime / SPCS) on the derived layer via `snow app deploy`, bakes the data target into the app, grants read/write access, and smoke-tests it. |

## Prerequisites

- A role with CREATE DATABASE / SCHEMA / TABLE / DYNAMIC TABLE / VIEW / SEMANTIC VIEW / STAGE / MODEL privileges (or a pre-created target DB.SCHEMA with USAGE + CREATE).
- A warehouse for dynamic-table refresh.
- Source HCAHPS survey responses and patient comments; optionally calls/transcripts, grievances, callbacks, and unit-operations data (each optional source unlocks the matching panels; the app degrades gracefully when a source is absent).

### Per-account prerequisites (verify before running on a NEW account)
The Snowflake CLI can't fully verify these; confirm them on the target account first.

- **Snowflake CLI + connection**: `snow` installed and a named connection to the target account (used as `-c <conn>` throughout). `snow app setup --help` must succeed (Phase 4).
- **Cortex availability in the target region**:
  - Cortex Analyst + a Cortex Agent (the agent uses `orchestration: auto`, so it adapts to available models).
  - Cortex AISQL functions used by the voice-enrichment tier (sentiment / theme extraction over patient comments).
  - Cortex Search (for the patient-comment search service `COMMENT_SEARCH`).
- **ML compute (Phase 3)**: a NOTEBOOK-eligible **compute pool** and a valid Container Runtime version for the headless XGBoost training NPO. Set them in `config.json` (`compute_pool`, `runtime`); the derived-build skill discovers/validates eligible values.
- **Snowflake App Runtime enabled (Phase 4)**: an available **compute pool** (`app_compute_pool`) and an **external access integration** (`build_eai`) for the container build. If `snow app deploy` reports these missing, that's an account-setup task, not a plugin defect.
- **Roles**: the role that runs `snow app deploy` becomes the app's owner and the grantee for `GRANT_APP_ACCESS`; it must be able to read the derived objects (or already own them).

## Install from GitHub

This plugin is a standard CoCo plugin: `.cortex-plugin/plugin.json` + `skills/` + `agents/` + `assets/` at the plugin root (skills and agents are auto-discovered — no manifest list needed).

- **Repo layout**: the plugin can be the repo root, or live under a subpath (e.g. `plugins/<name>/`). Either works.
- **Install source string**: `github:<org>/<repo>[/<subpath>]#<branch>` (e.g. `github:sfc-gh-jrag/patient_experience_command_center#main`). Use the CoCo catalog / `github-plugin-installer` flow to install; it resolves that source, expecting `.cortex-plugin/plugin.json` at the resolved path.
- **Publish** (from a clean checkout of the plugin dir):
  ```bash
  git init && git add . && git commit -m "Patient Experience Command Center accelerator"
  gh repo create <org>/<repo> --public --source . --remote origin --push
  ```
  The bundled `.gitignore` keeps `__pycache__/`, `.DS_Store`, `node_modules/`, and `.next/` out of the repo.

## Configuration

The whole accelerator is driven by **one file**: a `config.json` you create per customer. Copy the shipped template and edit it:

```bash
cp assets/config.sample.json config.json   # then edit the values below
```

The renderer (`assets/renderer/render.py`) reads it as strict JSON, so your `config.json` must have **no comments**. Object names (`VW_*`, `MDL_*`, `SV_PATIENT_EXPERIENCE_INTELLIGENCE`, `COMMENT_SEARCH`, `AGT_PATIENT_EXPERIENCE`, `ACTION_EVENT_LOG`) are accelerator-owned and identical for every customer — they are baked into the templates, not this file.

| Key | Required | Phase | Meaning |
|---|---|---|---|
| `target_database` | Yes | 1,3,4 | Database where the derived data product + app object are created. |
| `target_schema` | Yes | 1,3,4 | Schema holding the accelerator-owned objects (fixed names). |
| `warehouse` | Yes | 3,4 | Warehouse for dynamic-table refresh and app queries. |
| `target_lag` | No (default `1 hour`) | 3 | Dynamic-table / Cortex Search refresh lag. |
| `compute_pool` | Yes (for ML tier) | 3 | NOTEBOOK-eligible compute pool for the headless XGBoost training NPO. |
| `runtime` | Yes (for ML tier) | 3 | Container Runtime version for the NPO (e.g. `V2.6-CPU-PY3.12`). If invalid, the error lists valid versions — update and re-render. |
| `app_name` | No (default `PATIENT_EXPERIENCE_COMMAND_CENTER`) | 4 | Snowflake App object name. |
| `app_compute_pool` | Yes | 4 | SPCS compute pool used for both the app build and the running service. |
| `build_eai` | Yes | 4 | External access integration used to `npm ci` at build time (e.g. `ALLOW_ALL_EAI`). |
| `app_workspace_stage` | No (default `SNOWFLAKE_APPS`) | 4 | Code workspace stage (inside `target_schema`) for the app artifacts. |
| `app_role` | Yes (for grants) | 4 | Grantee in `GRANT_APP_ACCESS`. Must be the role that **owns** the Application Service (= the role running `snow app deploy`), because the app queries with owner's rights. The deploy skill derives this from `SHOW APPLICATION SERVICES` (the `owner` column) and overrides this value; set it as a placeholder. No-op if that role already owns the derived objects. |

## Layout

```
patient-experience-command-center/
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
    templates/         # Jinja SQL templates for the derived objects (+ app_grants.sql.j2)
    ml/                # XGBoost training notebook + environment.yml (headless NPO)
    build_manifest.yaml
    config.sample.json # copy -> config.json, edit per customer (see Configuration)
    renderer/
      render.py        # bindings + templates -> SQL DDL
      dump_bindings.py # APPROVED conformance tables -> elements/relationships/crosswalk JSON
      seed_contract.py # seeds the contract skeleton in Phase 1
    validate/
      readiness_gate.sql # Phase 2 structural gate (PASS/FAIL)
      checks.sql         # Phase 3 post-build validation (PASS/FAIL/WARN)
    app/               # app source (Next.js App Runtime) + snowflake.yml.j2 + app-data-contract.md
```

## Accelerator-owned vs customer-specific

- **Accelerator-owned (ships in this plugin):** logical contract, SQL/agent templates, MDL algorithms, voice-enrichment + risk-model pipeline, semantic view + agent definitions, the app, and `build_manifest.yaml`.
- **Customer-specific (produced at runtime):** the 3 populated conformance tables. Nothing else.

> Development note: this plugin is developed and validated in the runtime location `<workspace>/.cortex/plugins/`. Once vetted it is promoted into the accelerator's wiki `Build_artifacts/plugin/` as the versioned artifact.
