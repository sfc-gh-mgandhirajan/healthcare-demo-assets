---
name: deploy-all
description: "End-to-end deployment of DICOM Imaging Intelligence in one command: pipeline, optional MedSigLIP GPU image search, and the Streamlit app, with a confirmation gate before each phase that costs money and a report of exactly what completed if something fails. Triggers: deploy everything dicom, dicom end to end, full dicom deploy, deploy all dicom, stand up the whole dicom demo, dicom one command, complete dicom deployment."
---

# Deploy everything, end to end

Sequences the three deployment skills in order with a gate before each expensive
phase. Use this to stand up a complete demo; use the individual skills when you
want one phase or are debugging.

This skill **delegates** — it does not reimplement the phases. Each step loads the
relevant skill and follows it. That is deliberate: duplicating their bodies here
would guarantee the copies drift.

## What it runs

| Step | Delegates to | Costs |
|---|---|---|
| 1 | `deploy` | warehouse + UDF compute for the backfill |
| 2 | `image-search-deploy` (optional) | **GPU node** + a multi-GB model download |
| 3 | `app-deploy` | app compute pool |
| 4 | validation + report | negligible |

Step 2 is optional and skippable. Everything except the Image Search tab works
without it.

## Collect once, reuse everywhere

Gather these up front and pass them into every delegated skill so the user is not
asked the same question three times:

```bash
PLUGIN_DIR="<absolute path to this plugin>"     # ~/.snowflake/cortex/plugins/dicom-imaging-intelligence
CONN="<snowflake connection name>"
CONFIG="<path to config.json for THIS account>"
```

Keep `CONFIG` outside `$PLUGIN_DIR` when deploying to more than one account —
otherwise the second deployment overwrites the first's config.

## Step 0: Preflight

Confirm the target before creating anything. Report all four results together;
do not fix them silently.

```bash
snow sql -c "$CONN" --enable-templating NONE -q "
SELECT CURRENT_ACCOUNT() AS ACCOUNT, CURRENT_REGION() AS REGION, CURRENT_ROLE() AS ROLE;"

# ACCOUNTADMIN is required for step 1's account phase
snow sql -c "$CONN" --role ACCOUNTADMIN --enable-templating NONE -q "SELECT CURRENT_ROLE();"
```

After confirming the account, **immediately resume the GPU pool** so nodes are
provisioning while the pipeline phase runs. By the time image-search needs them
they are already warm:

```bash
snow sql -c "$CONN" --role ACCOUNTADMIN --enable-templating NONE -q "
ALTER COMPUTE POOL <gpu_pool> RESUME;
ALTER COMPUTE POOL <gpu_pool> SET MIN_NODES = 3 MAX_NODES = 3 AUTO_SUSPEND_SECS = 3600;"
```

This avoids the deadlock where the ML Job, model build, and inference service
all need a node simultaneously but only 1 exists.

| Check | If it fails |
|---|---|
| Connection resolves | wrong `-c` name, or a broken entry in `connections.toml` |
| `ACCOUNTADMIN` assumable | you cannot run the account phase; hand `00_ACCOUNT_SETUP.sql` to an admin and start from the core phase |
| `config.json` exists and is valid JSON | copy `assets/config.sample.json` and edit; **no comments allowed** |
| Target database does not already exist | pick a different `target_database`, or you may collide with an existing deployment |

MANDATORY STOPPING POINT: show the user the account, region, and target database
and get explicit confirmation. Deploying a demo into the wrong account is the
single most expensive mistake available here.

## Step 1: Pipeline

**Load** `/dicom-imaging-intelligence:deploy` and follow it end to end with the
`PLUGIN_DIR` / `CONN` / `CONFIG` already collected.

It renders, runs the account phase, the core phase, the pipeline phase, the
initial backfill, and the validation checks. It has its own gate before the
backfill.

Do not continue to step 2 until its checks come back with no `FAIL`. A `WARN` for
`embeddings_present` is expected at this point — there are no embeddings yet.

Record: rendered object count, backfill row counts, and the check summary.

## Step 2: Image search (optional)

Ask before starting. Two reasons to skip:

- It needs a HuggingFace token whose account has **accepted the gated model terms**
  for `google/medsiglip-448`. Without that, the download fails with a 401 that is
  indistinguishable from an invalid token.
- It provisions a GPU node and downloads a multi-GB model. This is the most
  expensive thing the plugin does.

If yes: **Load** `/dicom-imaging-intelligence:image-search-deploy` and follow it.

If no: say plainly that the app's Image Search tab will report no embeddings, and
that this phase can be run later at any time without redeploying anything else.

## Step 3: Streamlit app

**Load** `/dicom-imaging-intelligence:app-deploy` and follow it.

Runs regardless of whether step 2 happened. Report the app URL — that is the
artifact the user actually wants.

## Step 4: Report

Print a ledger of what completed, using real numbers rather than checkmarks:

```
Account            <account> / <region>
Target             <database>.<schema>

1  Pipeline        OK    <n> objects, <n> files, <n> metadata rows, <n> series
2  Image search    OK | SKIPPED | FAILED
3  Streamlit app   OK    <url>
4  Checks          <n> PASS, <n> WARN, <n> FAIL

Tasks              SUSPENDED (nothing recurring is billing)
```

Then state the two things a user forgets:

- **Nothing recurring is running yet.** Tasks were created suspended. Start the
  schedule with `/dicom-imaging-intelligence:pipeline-ops` — and note that the
  bundled IDC source is static public data, so a 5-minute scan against it finds
  nothing forever.
- **GPU pool auto-suspends after 1 hour of inactivity.** No manual intervention
  needed. The pool will scale to zero once all services are idle for 3600s. To
  force immediate shutdown: `ALTER COMPUTE POOL <gpu_pool> SUSPEND;`

## If a step fails

Stop. Do not attempt later steps — they depend on earlier ones, and a partial
deploy reported as success is worse than a clean failure.

Report:

1. Which steps completed, with their numbers.
2. The actual error, not a paraphrase.
3. The single command to resume from, so nothing already done is repeated:

| Failed in | Resume with |
|---|---|
| Step 1, account phase | fix the role, then `/dicom-imaging-intelligence:deploy` |
| Step 1, after account phase | `/dicom-imaging-intelligence:deploy` (idempotent; re-running is cheap and the backfill adopts already-parsed files rather than reparsing) |
| Step 2 | `/dicom-imaging-intelligence:image-search-deploy` |
| Step 3 | `/dicom-imaging-intelligence:app-deploy` |

Every phase is idempotent, so resuming never requires a teardown first.

## Hand-off

- `/dicom-imaging-intelligence:pipeline-ops` — resume the schedule, monitor, retry failures
- `/dicom-imaging-intelligence:onboard-source` — point it at real DICOM instead of the demo
- `/dicom-imaging-intelligence:teardown` — remove everything
