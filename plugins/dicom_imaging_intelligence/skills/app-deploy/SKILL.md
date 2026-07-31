---
name: app-deploy
description: "Deploy the DICOM Explorer Streamlit in Snowflake app on top of a deployed pipeline: renders the deploy manifest and the app's target config, pushes the bundle to the container runtime, and smoke-tests it. Triggers: dicom app deploy, deploy dicom explorer app, streamlit dicom, stand up dicom app, deploy imaging app, dicom ui."
---

# Deploy the DICOM Explorer app

A 7-tab Streamlit in Snowflake app on the container runtime, plus a floating
Cortex Agent chat panel.

## Preconditions

- `deploy` has run: the tables, UDFs, and (for the clinical tabs) the demo star
  schema exist.
- The app compute pool from config (`app_pool`) exists and is ACTIVE.
- The EAI from config exists — the container runtime needs it to install packages.
- Image Search needs `image-search-deploy`; every other tab works without it.

## Paths & connection (set once)

```bash
PLUGIN_DIR="<absolute path to this plugin>"
CONN="<snowflake connection name>"
CONFIG="$PLUGIN_DIR/config.json"
WORKDIR="$(mktemp -d)"
```

## Step 1: Assemble the bundle in a scratch dir

Copy the vendored app, then render the two generated files into it. Never deploy
from `$PLUGIN_DIR` directly — `snow app`/`snow streamlit` writes build output
next to the manifest, and the plugin directory should stay clean.

```bash
cp -R "$PLUGIN_DIR/assets/app/." "$WORKDIR/app/"

uv run --with jinja2 --with pyyaml python "$PLUGIN_DIR/assets/renderer/render.py" \
  --manifest "$PLUGIN_DIR/assets/build_manifest.yaml" \
  --templates "$PLUGIN_DIR/assets/templates" \
  --out "$WORKDIR/app" --config "$CONFIG" --phase app
```

That writes two files with their real names (not `.sql`):

| File | Why |
|---|---|
| `snowflake.yml` | the deploy manifest — app name, database, schema, warehouse, compute pool, EAI |
| `app_config.json` | tells the app which database/schema hold its objects |

## Step 2: Why app_config.json exists

The app queries `@@FQ@@.OBJECT` and substitutes the real target at runtime, so
`streamlit_app.py` is never templated. It resolves the target in this order:

1. `app_config.json` beside the app — written just now from `config.json`
2. `DICOM_DB` / `DICOM_SCHEMA` env vars (local development)
3. the session's `CURRENT_DATABASE()` / `CURRENT_SCHEMA()`

Order 3 is the fallback rather than the primary on purpose. In Streamlit in
Snowflake the app object lives in the target schema, so session context is
usually right — but the app resolves this at import, so if it is ever wrong the
app fails to load at all. Writing the target explicitly removes that dependency.

The **stage** is deliberately not in `app_config.json`. The app reads it from
`DICOM_SOURCE_CONFIG`, so the viewer follows whatever sources are actually
registered rather than a value frozen at deploy time.

## Step 3: Deploy

```bash
snow streamlit deploy dicom_explorer_sf --replace -p "$WORKDIR/app" -c "$CONN"
```

`dicom_explorer_sf` is the fixed entity key in `snowflake.yml`; the Snowflake
object name comes from `app_name` in config. `-p` points at the directory holding
`snowflake.yml`.

The command prints the app URL. Give it to the user.

## Step 4: Grants (only if the app runs as a different role)

If the deploying role owns the data objects, nothing is needed. Otherwise grant
the app's role read access:

```sql
GRANT USAGE ON DATABASE <DB> TO ROLE <app_role>;
GRANT USAGE ON SCHEMA <DB>.<SCHEMA> TO ROLE <app_role>;
GRANT SELECT ON ALL TABLES IN SCHEMA <DB>.<SCHEMA> TO ROLE <app_role>;
GRANT USAGE ON ALL FUNCTIONS IN SCHEMA <DB>.<SCHEMA> TO ROLE <app_role>;
GRANT USAGE ON ALL PROCEDURES IN SCHEMA <DB>.<SCHEMA> TO ROLE <app_role>;
GRANT READ ON STAGE <DB>.<SCHEMA>.<source_stage> TO ROLE <app_role>;
```

Note the stage grant: without `READ ON STAGE` the metadata tabs work but the
DICOM viewer renders nothing, because `BUILD_SCOPED_FILE_URL` cannot reach the
pixel data. That is a confusing partial failure, so grant it explicitly.

## Step 5: Smoke test

Open the URL and confirm:

- **Pipeline Overview** — counts match `V_DICOM_PIPELINE_STATUS`
- **Metadata Explorer** — rows load, filters work
- **DICOM Viewer** — a slice renders (this is the stage-read path)
- **Governance** — `PATIENT_ID` / `PATIENT_NAME` show masked for an unauthorized role
- **Image Search** — returns hits, or a clear message if embeddings are absent

If the app fails at load with a message about not determining the
database/schema, `app_config.json` did not make it into the bundle — confirm it
is listed in `artifacts` in the rendered `snowflake.yml`.

## Notes

- The app auto-detects its environment: in Snowflake it uses the SPCS session
  token and `st.connection("snowflake")`; locally it uses `snowflake.connector`
  with your CLI connection.
- To run it locally, work from inside the bundle directory so Streamlit picks up
  `.streamlit/config.toml` — launching from a parent directory starts the app but
  silently drops the theme:

```bash
cd "$WORKDIR/app" && pip install -r requirements.txt
SNOWFLAKE_CONNECTION_NAME="$CONN" DICOM_DB=<DB> DICOM_SCHEMA=<SCHEMA> \
    streamlit run streamlit_app.py
```

## Hand-off

- `/dicom-imaging-intelligence:pipeline-ops` — keep the data fresh
- `/dicom-imaging-intelligence:image-search-deploy` — if Image Search showed no embeddings
