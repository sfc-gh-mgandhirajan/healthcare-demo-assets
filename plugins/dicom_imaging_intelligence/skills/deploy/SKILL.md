---
name: deploy
description: "Deploy the DICOM Imaging Intelligence pipeline into a Snowflake account: renders the SQL from config.json, provisions account objects, creates the UDFs/tables/policies/agent, stands up the incremental ingestion pipeline, and runs an initial backfill. Triggers: deploy dicom, dicom imaging deploy, install dicom pipeline, set up dicom explorer, deploy imaging intelligence, dicom phase 1."
---

# Deploy the DICOM Imaging Intelligence pipeline

Renders every SQL object from `config.json` and executes it in dependency order.
Idempotent: safe to re-run to pick up config changes or a new template.

## Preconditions

- `snow` CLI with a named connection to the target account.
- **Two roles.** The `account` phase needs `ACCOUNTADMIN` (compute pools, network
  rules, and external access integrations cannot be created by `SYSADMIN`).
  Everything after it runs as `owner_role` from config. If you do not hold
  ACCOUNTADMIN, hand that one file to whoever does and start at the `core` phase.
- `uv` available locally (the renderer needs `jinja2` + `pyyaml`).
- Cortex Search and Cortex Agents available in the target region.

## Paths & connection (set once)

```bash
PLUGIN_DIR="<absolute path to this plugin>"
WORKDIR="$(mktemp -d)"
CONN="<snowflake connection name>"
CONFIG="$PLUGIN_DIR/config.json"
```

Never write into `$PLUGIN_DIR` — all rendered output goes to `$WORKDIR`.

## Step 1: Configuration

If `$CONFIG` does not exist, copy the template and confirm the values with the
user before rendering:

```bash
cp "$PLUGIN_DIR/assets/config.sample.json" "$CONFIG"
```

Ask about these; the rest have sane defaults:

| Key | Ask because |
|---|---|
| `target_database` / `target_schema` | where everything lands |
| `warehouse` | must exist or be creatable; used for UDF execution and refresh |
| `owner_role` | owns every object from the `core` phase onward |
| `include_demo_series` | `true` deploys the public 10-series IDC demo. Set `false` for a customer who only wants their own stage. |
| `include_demo_clinical` | `true` deploys the synthetic TAVR star schema that the clinical tabs and agent read |

`config.json` is read as strict JSON — **no comments**. It is gitignored; do not
commit it.

> The HuggingFace token is deliberately NOT a config key. It is only needed for
> image search, and it is handled in that skill so it never lands in a file.

## Step 2: Render

```bash
uv run --with jinja2 --with pyyaml python "$PLUGIN_DIR/assets/renderer/render.py" \
  --manifest "$PLUGIN_DIR/assets/build_manifest.yaml" \
  --templates "$PLUGIN_DIR/assets/templates" \
  --out "$WORKDIR/rendered" \
  --config "$CONFIG"
```

The renderer uses `StrictUndefined`, so a template referencing a key that
`config.json` omits fails here rather than emitting a broken `CREATE`. It prints
one entry per rendered object plus anything skipped by an `enabled_if` flag.

**Sanity-check the render before touching the account.** Confirm no name from
another account leaked through:

```bash
grep -rl 'SF_CLINICAL_DB' "$WORKDIR/rendered" || echo "clean"
```

## Step 3: Account phase (ACCOUNTADMIN)

```bash
snow sql -f "$WORKDIR/rendered/00_ACCOUNT_SETUP.sql" -c "$CONN" \
    --role ACCOUNTADMIN --enable-templating NONE
```

Creates the warehouse, both compute pools, the network rule, the EAI, and the
database/schema — then hands schema ownership to `owner_role`, which is required
or the agent creation in `09` fails with "must have CREATE AGENT granted on
SCHEMA".

Always pass `--enable-templating NONE`. The CLI treats `&` as a variable marker
and these files contain `&` in comments.

## Step 4: Core and pipeline phases

Run in the order the renderer reported (the numeric prefixes ARE the dependency
order):

```bash
for f in 01_DATABASE_SCHEMA 02_SOURCE_STAGE 03_UDFS 04_METADATA_TABLE \
         05_MASKING_POLICIES 06_TEXT_SEARCH 08_CLINICAL_DEMO 09_AGENT \
         10_PIPELINE_CONFIG 11_PIPELINE_PROCEDURES 12_PIPELINE_TASKS; do
    [ -f "$WORKDIR/rendered/$f.sql" ] || continue      # skipped by enabled_if
    snow sql -f "$WORKDIR/rendered/$f.sql" -c "$CONN" --enable-templating NONE
done
```

Stop on the first failure and report it rather than continuing — later objects
depend on earlier ones.

`12` creates every task **SUSPENDED**. That is intentional: a deploy must never
silently start spending. Resuming is a separate, explicit step.

## Step 5: Initial backfill

MANDATORY STOPPING POINT: this is the first step that costs real compute — it
parses every discovered file through a Python UDF. Tell the user how many files
the scan found and confirm before running.

```bash
snow sql -f "$WORKDIR/rendered/13_INITIAL_BACKFILL.sql" -c "$CONN" --enable-templating NONE
```

For the bundled IDC demo that is 2,848 files, roughly 90 seconds on a Medium
warehouse. For a customer stage it scales with their file count; if it is large,
skip this and let the scheduled tasks work through it in bounded batches instead.

## Step 6: Validate

```bash
snow sql -f "$WORKDIR/rendered/CHECKS.sql" -c "$CONN" --enable-templating NONE
```

Every row is `CHECK_NAME | STATUS | DETAIL`. `PASS` and `WARN` are both
acceptable — `WARN` means an optional component is simply not deployed yet (no
embeddings before the image-search phase, for example). Investigate any `FAIL`;
`metadata_no_fanout`, `image_orphans`, and `embedding_orphans` failing all point
at data corruption rather than a missing step.

## Hand-off

Report what deployed, then offer the next steps:

- `/dicom-imaging-intelligence:onboard-source` — point it at your own DICOM stage
- `/dicom-imaging-intelligence:image-search-deploy` — MedSigLIP vision-language search
- `/dicom-imaging-intelligence:app-deploy` — the Streamlit app
- `/dicom-imaging-intelligence:pipeline-ops` — resume the schedule, monitor, retry failures
