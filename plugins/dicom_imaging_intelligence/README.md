# DICOM Imaging Intelligence — Solution Accelerator (CoCo Plugin)

Point Cortex Code at your Snowflake account and stand up an **incremental DICOM
ingestion pipeline** plus an imaging-search app on your own stage or object
storage — no hand-written SQL.

## What it does

Ships the pipeline, the app, and a 10-series public demo source. You supply a
`config.json` and, optionally, a stage of your own DICOM files. The plugin renders
and deploys everything downstream.

The pipeline is genuinely incremental, not a rebuild on a schedule:

- **Config-driven sources.** Swapping storage is a row in `DICOM_SOURCE_CONFIG`.
- **md5 change detection.** A file overwritten in place is reprocessed; without
  md5 it would keep its name and size and go silently stale.
- **Error quarantine.** A file that will not parse lands in `DICOM_LOAD_ERRORS`.
  One bad file never aborts a batch.
- **Delete detection**, split from purging so a misconfigured prefix is visible
  before rows are removed.
- **Derived series registry**, keyed on the real `SERIES_INSTANCE_UID` tag rather
  than a path segment, so any bucket layout works.

## Run order

| Phase | Skill | What happens |
|---|---|---|
| **all** | `/dicom-imaging-intelligence:deploy-all` | **One command for a complete demo.** Sequences 1, 3, and 4 below with a confirmation gate before each phase that costs money. |
| 1 | `/dicom-imaging-intelligence:deploy` | Renders from `config.json`, provisions account objects, creates UDFs / tables / policies / agent / pipeline, runs an initial backfill. |
| 2 | `/dicom-imaging-intelligence:onboard-source` | Points it at your own DICOM stage: storage integration, stage, source + prefix rows, backfill. |
| 3 | `/dicom-imaging-intelligence:image-search-deploy` | Registers MedSigLIP, deploys the GPU service, embeds images, creates BYO-vector search. Optional. |
| 4 | `/dicom-imaging-intelligence:app-deploy` | Deploys the Streamlit in Snowflake app on the container runtime. |
| — | `/dicom-imaging-intelligence:pipeline-ops` | Day two: resume/suspend, monitor, retry quarantine, reconcile deletes, GPU window. |
| — | `/dicom-imaging-intelligence:teardown` | Removes everything. Inert until explicitly armed. |

`deploy` does **not** include the app — `app-deploy` is separate, because the two
fail for unrelated reasons (SQL privileges vs SPCS availability) and a partial
deploy reported as success is worse than a clean failure. Use `deploy-all` when
you want the whole thing in one go; it delegates to the individual skills rather
than reimplementing them.

Phases 2 and 3 are independent. A customer who only wants their own data and no
GPU can stop after 2.

## Prerequisites

- Snowflake CLI (`snow`) with a named connection to the target account.
- `uv` available locally — the renderer needs `jinja2` + `pyyaml`.
- A role with CREATE DATABASE / SCHEMA / TABLE / VIEW / FUNCTION / PROCEDURE /
  TASK / AGENT privileges, plus a warehouse.

### Per-account prerequisites (verify before running on a NEW account)

- **`ACCOUNTADMIN` for one file.** `00_account_setup` creates two compute pools,
  a network rule, and an external access integration — none of which `SYSADMIN`
  can create. If you do not hold it, hand that one rendered file to whoever does
  and start at the `core` phase. This is an account-setup task, not a plugin
  defect.
- **Cortex availability in the region**: Cortex Search, Cortex Agents, and
  `AI_COMPLETE`.
- **Snowpark Container Services** for the app (phase 4) and the GPU model service
  (phase 3): an available compute pool and an EAI for the container build.
- **A HuggingFace account that has accepted the gated model terms** at
  https://huggingface.co/google/medsiglip-448 — needed only for phase 3. A
  syntactically valid token still returns 401 if the terms were never accepted,
  and the error looks identical to an invalid token.

## Install

```bash
# development — edits are live
ln -sfn "$PWD/plugins/dicom_imaging_intelligence" \
        ~/.snowflake/cortex/plugins/dicom-imaging-intelligence

# or copy for distribution
cp -R plugins/dicom_imaging_intelligence \
      ~/.snowflake/cortex/plugins/dicom-imaging-intelligence
```

Skills are auto-discovered from `skills/`; there is no manifest list to maintain.

## Configuration

Everything is driven by **one file** you create per deployment:

```bash
cp assets/config.sample.json config.json   # then edit
```

Read as strict JSON — **no comments**. `config.json` is gitignored; do not commit
it.

| Key | Required | Phase | Meaning |
|---|---|---|---|
| `target_database` | Yes | all | Database for every object the plugin creates. |
| `target_schema` | Yes | all | Schema inside it. |
| `warehouse` | Yes | all | UDF execution, search refresh, app queries. |
| `warehouse_size` | No | 1 | Created only if the warehouse does not exist. `MEDIUM` suits a 2,848-file backfill. |
| `owner_role` | Yes | all | Owns everything from the `core` phase on. Schema ownership is handed to it by `00`. |
| `app_pool` / `app_pool_family` | Yes | 1, 4 | Compute pool for the Streamlit container runtime. |
| `gpu_pool` / `gpu_pool_family` | Yes | 1, 3 | Compute pool for MedSigLIP inference. |
| `eai_name` / `network_rule` | Yes | 1, 3, 4 | Egress for package installs and the model download. |
| `hf_secret` | Yes | 3 | Name of the Snowflake SECRET holding the HuggingFace token. **The token itself is never a config value** — see below. |
| `app_name` | Yes | 4 | Streamlit object name. |
| `workspace_name` | No | teardown | Only used by the opt-in workspace drop. |
| `source_name` / `source_stage` / `source_url` | Yes | 1 | The bundled IDC demo source. |
| `include_demo_series` | No (default `true`) | 1 | `false` deploys the pipeline with **no** source registered — use `onboard-source`. Also skips the IDC stage and the curated label seed. |
| `include_demo_clinical` | No (default `true`) | 1 | `false` skips the synthetic TAVR star schema. The clinical tabs and the agent's clinical questions then have nothing to read. |
| `scan_schedule` | No (default `5 MINUTE`) | 1 | Root task interval. See the note below. |
| `gpu_resume_cron` / `gpu_suspend_cron` | No | 1 | GPU business-hours window. |
| `window_center` / `window_width` | No (default `40` / `400`) | 1 | DICOM window **fallback** for files with no window tags. Chest/lung wants roughly `-600` / `1500`. |
| `model_name` / `service_name` / `embed_dim` | No | 3 | Change together if you swap models. |
| `search_service` / `text_search_service` / `agent_name` | No | 1, 3 | Cortex object names. |
| `target_lag` | No (default `1 day`) | 3 | Cortex Search refresh lag. |

### The HuggingFace token is not in config.json

It is resolved at phase 3 from `HF_TOKEN`, then a gitignored `.env`, then a hidden
prompt, and piped to Snowflake over **stdin** — so it never lands in a file, in
`argv` (visible in the process list), or in shell history.

If a token is ever pasted into a chat transcript, treat it as compromised: revoke
and rotate it at https://huggingface.co/settings/tokens.

### A note on the 5-minute scan interval

The bundled IDC source is **static public data**. Polling it every 5 minutes
finds nothing, forever. The default is set for a customer with live ingest; for
the demo, leave the tasks suspended and call the procedures by hand. Scan cost
scales with the number of objects under each prefix, so on a large bucket the fix
is narrower prefixes rather than a longer interval.

## Layout

```
dicom_imaging_intelligence/
  .cortex-plugin/plugin.json
  skills/
    deploy-all/           deploy/               onboard-source/
    image-search-deploy/  app-deploy/           pipeline-ops/
    teardown/
  assets/
    build_manifest.yaml   # object -> template, phase, role, depends_on
    config.sample.json    # copy -> config.json
    renderer/render.py    # config + templates -> runnable SQL
    templates/
      00_account_setup.sql.j2 … 13_initial_backfill.sql.j2
      byo_search.sql.j2   checks.sql.j2   teardown.sql.j2
      app_snowflake_yml.j2  app_config_json.j2
    app/                  # Streamlit source (vendored, see below)
    notebooks/            # MedSigLIP notebook + ML Jobs script + requirements
```

Nothing is written into the plugin directory at runtime; every skill renders into
`$(mktemp -d)`.

**Numbered templates.** The prefixes are the dependency order — this pipeline is
linear and was validated in exactly that sequence. `build_manifest.yaml` also
records `depends_on` explicitly.

## Accelerator-owned vs deployment-specific

- **Ships in this plugin:** the SQL templates, the pipeline procedures, the UDFs,
  the agent and search definitions, the app, the notebook, and
  `build_manifest.yaml`.
- **Produced per deployment:** `config.json`, the source + prefix rows, and the
  rendered SQL. Nothing else.

> **Upstream.** `assets/app/` and `assets/notebooks/` are vendored from
> `snowflake-dicom-explorer`, which remains the reference implementation and is
> where they are developed and validated. If you change the app, change it there
> first and re-vendor, or the two copies drift.

## Two things to resolve before real patient data

Both are inherited from the reference implementation and are called out here
because a plugin makes them easy to deploy without noticing.

**PHI.** The masking policy covers `PATIENT_ID` and `PATIENT_NAME`. Real clinical
DICOM carries identifiers in many more tags — institution, referring and
performing physician, accession number, study dates, device serials, free-text
comments — and `EXTRACT_DICOM_METADATA` sweeps all of them into `RAW_METADATA`.
Worse, some modalities have **burned-in annotations in the pixel data**, which no
tag-level policy can mask and which flow into rendered previews and embeddings.
Before pointing this at real patients, decide on de-identification: pre-ingest
de-id, a tag allowlist instead of extract-everything, and a `BurnedInAnnotation`
check.

**Egress.** The network rule is broad by default so the MedSigLIP download works
on a fresh account. That is fine for a sandbox and probably not for a customer.
The durable fix is staging the model weights into Snowflake once and dropping
egress entirely.
