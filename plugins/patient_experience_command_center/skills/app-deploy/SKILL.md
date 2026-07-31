---
name: app-deploy
description: "Phase 4 of the Patient Experience Command Center deployment. Deploys the bundled Next.js Snowflake App (App Runtime / SPCS) on top of the derived data product built in Phase 3, then grants the app read/write access and smoke-tests it. Triggers: patient experience app deploy, deploy patient experience app, phase 4 patient experience, stand up command center app."
---

# Phase 4 - Deploy the App

Stand up the **Patient Experience Command Center** app on the derived layer from
Phase 3. The app is a pre-built Next.js **Snowflake App Runtime** project bundled at
`assets/app/`. This skill is a thin **deployer**: it parameterizes the bundle,
delegates the actual deploy to the native `snow app deploy` path, grants the app
access to the data product, and smoke-tests it. It does NOT regenerate the app
from the mockup/contract - the validated build is shipped as-is.

## Architecture (how this reuses native tooling)
| Concern | Owner |
|---|---|
| Stage upload, `next build`, image build, service create, endpoint, logs | **native `snow app` tooling** (see the `snowflake-apps` skill) |
| Parameterize `snowflake.yml` (where the app object lands + build/service pool + EAI) | this skill |
| Bake the data-product location into the app source (App Runtime cannot inject env) | this skill |
| Grants on the data product (15 `VW_*`, `ACTION_EVENT_LOG`, `SV`, `COMMENT_SEARCH`, `AGT`) | this skill |
| Post-deploy data smoke test | this skill |

Key design point: App Runtime cannot inject custom runtime env vars (`app.yml` has
no `env:` block) and the auto-injected `SNOWFLAKE_DATABASE`/`SNOWFLAKE_SCHEMA` are
the app's own service context (`SNOWFLAKE_SCHEMA = SERVICE_CONTEXT_NESTED_SCHEMA`,
`SNOWFLAKE_DATABASE` empty) - NOT the data location. So the data product FQN is
**baked into the app source at deploy time**: step 2 rewrites the `PX_DATABASE` and
`PX_SCHEMA` constants in `assets/app/lib/constants.ts` from `config.json`. See
`assets/app/lib/constants.ts`.

## Preconditions
- Phase 3 complete: these objects exist and validate in `TARGET_DATABASE.TARGET_SCHEMA`:
  15 `VW_*` (VW_HOUSE_KPIS, VW_JOURNEY_RIBBON, VW_WAIT_BY_AREA, VW_ACCESS_MOMENTUM,
  VW_MEASURE_BENCHMARK, VW_MEASURE_TREND, VW_EQUITY_GAP, VW_DRIVER_IMPORTANCE,
  VW_UNIT_SCORECARD, VW_POINTS_LOST, VW_DISCHARGE_READINESS, VW_GRIEVANCE_AGING,
  VW_RECOVERY_SLA, VW_VOICE_THEMES, VW_TAB_INSIGHTS), `ACTION_EVENT_LOG`,
  `SV_PATIENT_EXPERIENCE_INTELLIGENCE`, `COMMENT_SEARCH`, `AGT_PATIENT_EXPERIENCE`.
- `config.json` (the same file Phase 3 used) with `target_database`, `target_schema`,
  `warehouse`, plus the app keys:
  - `app_name` (default `PATIENT_EXPERIENCE_COMMAND_CENTER`)
  - `app_role` (fallback grantee; the real grantee is auto-derived in step 4)
  - `app_compute_pool` (SPCS pool used for both build and service)
  - `build_eai` (external access integration used to `npm ci` at build; e.g. `ALLOW_ALL_EAI`)
  - `app_workspace_stage` (code workspace stage, default `SNOWFLAKE_APPS`)
- `snow app setup --help` succeeds (Snowflake CLI present + recent). If not, see the
  `snowflake-apps` skill `references/cli-version-check.md`.
- The account is enabled for Snowflake App Runtime: `app_compute_pool` exists (or the
  role can create one) and `build_eai` exists. If `snow app deploy` reports these are
  missing, that is an account-setup prerequisite, not a plugin defect - surface it.

## Paths & connection (set once)
All commands use shell variables so this skill works wherever the plugin is installed
(a fresh clone, `~/.snowflake/cortex/plugins/...`, or a repo subpath):
- `PLUGIN_DIR` - the plugin root: the directory that contains `.cortex-plugin/`.
- `WORKDIR` - a fresh scratch dir: `WORKDIR=$(mktemp -d)`.
- `CONN` - the user's Snowflake CLI connection name (used as `-c "$CONN"`).
- `CONFIG` - path to the customer's `config.json` (copied from
  `$PLUGIN_DIR/assets/config.sample.json` and edited; the same file Phase 3 used).

## Steps

### 1. Stage the app
Copy the bundled app into the scratch dir (never deploy from the plugin dir; the
bundle carries no `node_modules`/`.next` - the remote build produces them):
```bash
cp -R "$PLUGIN_DIR/assets/app/." "$WORKDIR"
```

### 2. Parameterize (render `snowflake.yml` + bake the data target)
(a) Render `assets/app/snowflake.yml.j2` -> `<workdir>/snowflake.yml` (controls WHERE
the app object lands + build/service compute pool + build EAI + code workspace).
(b) Bake the data location into `<workdir>/lib/constants.ts` (controls WHICH schema
the app QUERIES) - required because App Runtime cannot inject env vars. Both read
`config.json`:
```bash
python3 - "$PLUGIN_DIR" "$WORKDIR" "$CONFIG" <<'PY'
import json, re, sys
from jinja2 import Template
plugin, workdir, config = sys.argv[1], sys.argv[2], sys.argv[3]
cfg = json.load(open(config))
# (a) snowflake.yml
src = open(f"{plugin}/assets/app/snowflake.yml.j2").read()
open(f"{workdir}/snowflake.yml", "w").write(Template(src).render(**cfg))
# (b) bake data-target constants
cf = f"{workdir}/lib/constants.ts"
s = open(cf).read()
s = re.sub(r'export const PX_DATABASE = "[^"]*"', f'export const PX_DATABASE = "{cfg["target_database"]}"', s)
s = re.sub(r'export const PX_SCHEMA = "[^"]*"',   f'export const PX_SCHEMA = "{cfg["target_schema"]}"', s)
open(cf, "w").write(s)
PY
# sanity: the baked constants must show the config's db/schema, not the shipped placeholder
grep -E 'PX_DATABASE|PX_SCHEMA' "$WORKDIR/lib/constants.ts"
```
Do NOT run `snow app setup` when a rendered `snowflake.yml` is present (it would
overwrite it). `app.yml` needs no changes: it declares `install` (`npm ci
--include=dev`) and `run` (`node .next/standalone/server.js`); `next build` runs at
build time (output `standalone`); there is no `env:` key to set.

### 3. Deploy via the native path
From `$WORKDIR` run `snow app deploy -c "$CONN"` (native tooling does stage upload,
`next build`, image build, service create, endpoint). Follow the `snowflake-apps`
deploy conventions: run in background with `--verbose`, redirect to a log file
(piping to `tail` buffers), poll for `Status: ready` / `App ready`, then print the
returned URL. Typical time 2-10 min. The `opentelemetry` ModuleNotFoundError and
"already exists. Upgrading." lines are benign.

### 4. Grant the app access
The app queries with **owner's rights**: the SPCS service user executes as the
**role that OWNS the Application Service object** - which is the role that ran
`snow app deploy`, NOT necessarily `config.app_role`. So the grantee is the app-owner
role. Derive it authoritatively from the deployed app, then run the grants with it:
```bash
# authoritative grantee = owner of the app object just deployed
APP_NAME=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1])).get('app_name','PATIENT_EXPERIENCE_COMMAND_CENTER'))" "$CONFIG")
APP_ROLE=$(snow sql -c "$CONN" --query "SHOW APPLICATION SERVICES;" --format json \
  | python3 -c "import json,sys; rows=json.load(sys.stdin); print(next(r for r in rows if r['name']==sys.argv[1])['owner'])" "$APP_NAME")
# bake it into config so the template renders the right grantee, then render+run
python3 -c "import json,sys; c=json.load(open(sys.argv[1])); c['app_role']=sys.argv[2]; json.dump(c, open(sys.argv[1],'w'), indent=2)" "$CONFIG" "$APP_ROLE"
mkdir -p "$WORKDIR/nobind"
python3 "$PLUGIN_DIR/assets/renderer/render.py" \
  --bindings "$WORKDIR/nobind" --manifest "$PLUGIN_DIR/assets/build_manifest.yaml" \
  --templates "$PLUGIN_DIR/assets/templates" --out "$WORKDIR/rendered" \
  --config "$CONFIG" --only GRANT_APP_ACCESS
snow sql -c "$CONN" -f "$WORKDIR/rendered/GRANT_APP_ACCESS.sql"
```
(`--bindings` may point at an empty dir - Phase-4 templates need no conformance data.)
Idempotent. **If the app-owner role already owns the derived objects** (e.g. the same
role built Phase 3, or a sandbox under ACCOUNTADMIN) the grants are harmless no-ops.
They are **required** when the derived objects are owned by a different role than the
one deploying the app (a least-privilege customer). `config.app_role` is only a
fallback/placeholder; the deployed owner is truth.

### 5. Smoke test (data, not just "it booted")
Against the app URL, confirm the data plane works (the app is behind Snowflake OAuth,
so verify via the endpoints/logs rather than an unauthenticated curl of `/`):
- Home page renders the 5-stage journey board with populated panels - the server
  component fetches the 15 `VW_*`; confirm no `SERVICE_CONTEXT_NESTED_SCHEMA` /
  "does not exist" errors in `snow app events -c "$CONN"` (that symptom = constants
  not baked / grants missing).
- `POST /api/agent` with a canned question -> a streamed answer (agent + SV + search).
- `POST /api/action` with a sample action -> an `OPEN` row logged and read back
  (`ACTION_EVENT_LOG` write path).
Report a PASS/FAIL table with the live anchor values (composite top-box, star rating,
open-grievance count, etc.).

### 6. Report
Print the app URL and the smoke-test results. Tell the user the app reads the
customer's live data product (not sample values).

## Notes
- Deploying the app **into** the derived schema is the supported accelerator pattern
  (zero-config data resolution). A split layout (app object in a different schema than
  the data) would require runtime env injection, which App Runtime cannot do - avoid it.
- The dashboard data routes and the action write path use owner's rights only; no
  `executeAsCaller` is needed. The agent rail calls the Agent Run REST API with the
  service token.
- Debugging 500s: `snow app events -c "$CONN"` fetches App Runtime service logs. There
  is no `SHOW SERVICES` object (App Runtime abstracts it); use `snow app open
  --print-only` for the URL and `snow app teardown` to remove.
- App-facing data contract: `assets/app/app-data-contract.md` (every panel -> exact
  `VW_*`/object + columns).
