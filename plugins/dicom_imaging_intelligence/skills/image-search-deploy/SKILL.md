---
name: image-search-deploy
description: "Deploy MedSigLIP vision-language image search for the DICOM pipeline: registers the gated google/medsiglip-448 model, stands up the GPU SPCS inference service, embeds pending images, and creates the Cortex Search BYO-vector service and SEARCH_DICOM_IMAGES procedure. Triggers: dicom image search, medsiglip, deploy image search, vision language search, embed dicom images, gpu service dicom, byo vector search, semantic image search."
---

# Deploy MedSigLIP image search

Stands up the GPU model service, embeds the rendered images, and wires up
vector search.

This is the most expensive phase in the plugin. It is optional — every other tab
and the text metadata search work without it.

## Preconditions

- `deploy` has run and `DICOM_BASE64_IMAGES` has rows (check
  `V_DICOM_PIPELINE_STATUS.PENDING_EMBEDDING`).
- A GPU compute pool exists (`gpu_pool` in config; created by the `account` phase).
- The external access integration exists and permits HuggingFace egress.
- **A HuggingFace token whose account has accepted the gated model terms** at
  https://huggingface.co/google/medsiglip-448

> The gated-model failure is worth knowing in advance: a syntactically valid
> token still returns 401 if that HF account never accepted the terms, and the
> error is indistinguishable from an invalid token. Check this first.

## Paths & connection (set once)

```bash
PLUGIN_DIR="<absolute path to this plugin>"
CONN="<snowflake connection name>"
CONFIG="$PLUGIN_DIR/config.json"
WORKDIR="$(mktemp -d)"
```

## Step 1: The HuggingFace secret

Resolution order — never write the token into `config.json` or any file:

1. `HF_TOKEN` already exported in the environment
2. a `HF_TOKEN=` line in a gitignored `.env`
3. an interactive hidden prompt

Create the secret by piping over **stdin**, so the token never appears in argv
(visible in the process list) or in shell history:

```bash
printf '%s\n' \
  "CREATE OR REPLACE SECRET <DB>.<SCHEMA>.<hf_secret> TYPE = GENERIC_STRING SECRET_STRING = '$HF_TOKEN';" \
  "GRANT USAGE ON SECRET <DB>.<SCHEMA>.<hf_secret> TO ROLE <owner_role>;" \
  "ALTER EXTERNAL ACCESS INTEGRATION <eai_name> SET ALLOWED_AUTHENTICATION_SECRETS = ('<DB>.<SCHEMA>.<hf_secret>');" \
  | snow sql -i -c "$CONN" --enable-templating NONE
```

If the user pastes a token directly into the conversation, do NOT use it: tell
them it is now in the transcript, ask them to revoke and rotate it at
https://huggingface.co/settings/tokens, and re-run with the env-var path.

## Step 2: Register the model and deploy the GPU service

Two equivalent paths. Both dispatch the heavy work (model download, registration)
to the GPU pool rather than doing it locally.

**Script (default — automatable, runs from anywhere):**

```bash
python -m venv "$WORKDIR/venv" && source "$WORKDIR/venv/bin/activate"
pip install -r "$PLUGIN_DIR/assets/notebooks/requirements.txt"

python "$PLUGIN_DIR/assets/notebooks/run_medsiglip_pipeline.py" \
    --connection "$CONN" --config "$CONFIG" --phase a
```

`--config` is what makes this account-portable; without it the script falls back
to its built-in defaults.

**Workspace notebook (if the user prefers a UI):** upload
`assets/notebooks/medsiglip_log_model.ipynb` to a Snowflake **Workspace** and run
it there, attaching the EAI and the secret.

```bash
cortex ws cp "$PLUGIN_DIR/assets/notebooks/medsiglip_log_model.ipynb" \
    "snow://workspace/<workspace>/versions/live/" -c "$CONN"
```

Use a Workspace, not a legacy stage-backed `CREATE NOTEBOOK` object.

MANDATORY STOPPING POINT: this step provisions a GPU node and downloads a
multi-GB model. Confirm before starting, and warn that the model download takes
minutes even on a healthy egress path.

> If the download appears to hang with no output for several minutes, suspect
> egress before anything else. Sample the job log length twice a minute apart; if
> it is byte-identical, the container cannot reach the CDN and the network rule
> needs widening rather than more waiting.

## Step 3: Embed

```sql
CALL SP_EMBED_PENDING(100000);
```

Or via the script: `--phase b`.

If this returns `SKIPPED: service ... is SUSPENDED`, that is not an error — the
GPU service is outside its business-hours window. Resume it and re-run:

```sql
ALTER SERVICE <DB>.<SCHEMA>.<service_name> RESUME;
```

## Step 4: Vector search service and procedure

```bash
uv run --with jinja2 --with pyyaml python "$PLUGIN_DIR/assets/renderer/render.py" \
  --manifest "$PLUGIN_DIR/assets/build_manifest.yaml" \
  --templates "$PLUGIN_DIR/assets/templates" \
  --out "$WORKDIR/rendered" --config "$CONFIG" --phase image-search

snow sql -f "$WORKDIR/rendered/BYO_SEARCH.sql" -c "$CONN" --enable-templating NONE
```

Creates the Cortex Search service over the embeddings and the
`SEARCH_DICOM_IMAGES` procedure the agent's image-search tool calls.

## Step 5: Relevance sanity check

Embeddings that compute cleanly can still be useless if the query encoder is
misaligned, and nothing errors when that happens. Check that a cardiac query
outranks unrelated series:

```sql
WITH q AS (
  SELECT <DB>.<SCHEMA>.<service_name>!embed_text(
    'cardiac CT angiography with calcified aortic valve'
  ):EMBEDDING::VECTOR(FLOAT, <embed_dim>) AS QVEC
)
SELECT e.LABEL, e.COLLECTION,
       ROUND(MAX(VECTOR_COSINE_SIMILARITY(e.EMBEDDING, q.QVEC)), 4) AS BEST_SIM
FROM <DB>.<SCHEMA>.DICOM_EMBEDDINGS e, q
GROUP BY e.LABEL, e.COLLECTION
ORDER BY BEST_SIM DESC;
```

Cardiac/CTA series should sit clearly above unrelated chest screening series. If
the ordering looks arbitrary, suspect the text encoder's padding mode before
anything else — that is the failure that produces plausible-but-wrong rankings.

## Step 6: Suspend the GPU when idle

```sql
ALTER SERVICE <DB>.<SCHEMA>.<service_name> SUSPEND;
```

`TASK_GPU_SUSPEND` does this on a schedule, but suspend it now if you are done —
an idle GPU node bills. Suspending the service releases the node and the pool
then scales to zero via its own `AUTO_SUSPEND_SECS`.

`DICOM_EMBEDDINGS` and the search index persist independently, so text search,
the viewer, and the clinical tabs keep working while the GPU is down. Only new
embeddings and image-search *queries* need it up.

## Hand-off

- `/dicom-imaging-intelligence:app-deploy` — the app's Image Search tab now works
- `/dicom-imaging-intelligence:pipeline-ops` — set the GPU business-hours window
