---
name: app-deploy
description: "Phase 4 of the Pharmacy Cost Command Center deployment. Deploys the bundled Next.js Snowflake App (App Runtime / SPCS) on top of the derived data product built in Phase 3, then grants the app read access and smoke-tests it. Triggers: pharmacy cost app deploy, deploy pharmacy app, phase 4 pharmacy, stand up command center app."
---

# Phase 4 - Deploy the App

Stand up the **Pharmacy Cost Command Center** app on the derived layer from Phase 3.
The app is a pre-built Next.js **Snowflake App Runtime** project bundled at
`assets/app/`. This skill is a thin **deployer**: it parameterizes the bundle,
delegates the actual deploy to the native `snow app deploy` path, grants the
app read access to the data product, and smoke-tests it. It does NOT regenerate
the app from the mockup/contract - the validated build is shipped as-is.

## Architecture (how this reuses native tooling)
| Concern | Owner |
|---|---|
| Stage upload, container build (`next build` auto-detected), service create, endpoint, logs | **native `snow app` tooling** (see the `snowflake-apps` skill) |
| Parameterize `snowflake.yml` (where the app object lands) | this skill |
| Grants on the data product (12 VW_*, MDL_TAB_INSIGHTS, SV, CSS, AGT) | this skill |
| Post-deploy data smoke test | this skill |

Key design point: App Runtime cannot inject custom runtime env vars (`app.yml`
has no `env:` block) and the auto-injected `SNOWFLAKE_DATABASE`/`SNOWFLAKE_SCHEMA`
are the app's own service context (`SNOWFLAKE_SCHEMA = SERVICE_CONTEXT_NESTED_SCHEMA`,
`SNOWFLAKE_DATABASE` empty) - NOT the data location. So the data product FQN is
**baked into the app source at deploy time**: step 2 rewrites the two
`DEFAULT_DATA_*` constants in `assets/app/lib/constants.ts` from `config.json`.
See `assets/app/lib/constants.ts`.

## Preconditions
- Phase 3 complete: these objects exist and validate in `TARGET_DATABASE.TARGET_SCHEMA`:
  12 `VW_*`, `MDL_TAB_INSIGHTS`, `SV_PHARMACY_COST_INTELLIGENCE`,
  `CSS_COVERAGE_POLICY`, `AGT_PHARMACY_COST_COMMAND_CENTER`.
- `config.json` (the same file Phase 3 used) with: `target_database`,
  `target_schema`, `warehouse`, plus the app keys:
  - `app_name` (default `PHARMACY_COST_COMMAND_CENTER`)
  - `app_stage` (default `PHARMACY_COST_COMMAND_CENTER_CODE`)
  - `app_role` (the role the Application Service runs as - the grantee).
- `snow app setup --help` succeeds (Snowflake CLI present + recent). If not, see
  the `snowflake-apps` skill `references/cli-version-check.md`.
- The account is enabled for Snowflake App Runtime (a compute pool + an external
  access integration for dependency install are available). If `snow app deploy`
  reports these are missing, that is an account-setup prerequisite, not a plugin
  defect - surface it to the user.

## Paths & connection (set once)
All commands below use shell variables so this skill works wherever the plugin is installed (a fresh clone, `~/.snowflake/cortex/plugins/...`, or a repo subpath):
- `PLUGIN_DIR` - the plugin root: the directory that contains `.cortex-plugin/`. Set it to THIS installed plugin's own directory.
- `WORKDIR` - a fresh scratch dir you create: `WORKDIR=$(mktemp -d)`.
- `CONN` - the user's Snowflake CLI connection name (used as `-c "$CONN"`).
- `CONFIG` - path to the customer's `config.json` (copied from `$PLUGIN_DIR/assets/config.sample.json` and edited; the same file Phase 3 used).

## Steps

### 1. Stage the app
Copy the bundled app into the scratch dir (never deploy from the plugin dir; the bundle carries no `node_modules`/`.next` - the remote build produces them):
```bash
cp -R "$PLUGIN_DIR/assets/app/." "$WORKDIR"
```

### 2. Parameterize (render `snowflake.yml` + bake the data target)
(a) Render `assets/app/snowflake.yml.j2` -> `<workdir>/snowflake.yml` (controls
WHERE the app object lands). (b) Bake the data location into
`<workdir>/lib/constants.ts` (controls WHICH schema the app QUERIES) - required
because App Runtime cannot inject env vars. Both read `config.json`:
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
s = re.sub(r'const DEFAULT_DATA_DB = "[^"]*"',     f'const DEFAULT_DATA_DB = "{cfg["target_database"]}"', s)
s = re.sub(r'const DEFAULT_DATA_SCHEMA = "[^"]*"', f'const DEFAULT_DATA_SCHEMA = "{cfg["target_schema"]}"', s)
open(cf, "w").write(s)
PY
```
Do NOT run `snow app setup` when a rendered `snowflake.yml` is present (it would
overwrite). `app.yml` needs no changes: build is auto-detected from
`package.json` (it runs `next build`); metadata lives in its `profile` block;
there is no `env:` key to set.

### 3. Deploy via the native path
From `$WORKDIR` run `snow app deploy -c "$CONN"` (this is where the native tooling
does stage upload, `next build`, image build, service create, endpoint). Follow
the `snowflake-apps` deploy conventions: run in background with `--verbose`, poll
the output, print the returned URL. Typical time 2-10 min.

### 4. Grant the app read access
The app queries with **owner's rights**: the SPCS service user (e.g. `MANAGED_SERVICE_1`)
executes as the **role that OWNS the Application Service object** — which is the
role that ran `snow app deploy`, NOT necessarily `config.app_role`. So the grantee
is the app-owner role. Derive it authoritatively from the deployed app, then run
the grants with it:
```bash
# authoritative grantee = owner of the app object just deployed
APP_ROLE=$(snow sql -c "$CONN" --query \
  "SHOW APPLICATION SERVICES;" --format json \
  | python3 -c "import json,sys; rows=json.load(sys.stdin); print(next(r for r in rows if r['name']=='PHARMACY_COST_COMMAND_CENTER')['owner'])")
# bake it into config so the template renders the right grantee, then render+run
python3 -c "import json,sys; c=json.load(open(sys.argv[1])); c['app_role']=sys.argv[2]; json.dump(c, open(sys.argv[1],'w'), indent=2)" "$CONFIG" "$APP_ROLE"
mkdir -p "$WORKDIR/nobind"
python3 "$PLUGIN_DIR/assets/renderer/render.py" \
  --bindings "$WORKDIR/nobind" --manifest "$PLUGIN_DIR/assets/build_manifest.yaml" \
  --templates "$PLUGIN_DIR/assets/templates" --out "$WORKDIR/rendered" \
  --config "$CONFIG" --only GRANT_APP_ACCESS
snow sql -c "$CONN" -f "$WORKDIR/rendered/GRANT_APP_ACCESS.sql"
```
(`--bindings` may point at an empty dir - Phase-4 templates need no conformance
data.) Idempotent. **If the app-owner role already owns the derived objects**
(e.g. the same role built Phase 3, or sandbox under ACCOUNTADMIN) the grants are
harmless no-ops. They are **required** when the derived objects are owned by a
different role than the one deploying the app (a least-privilege customer).
`config.app_role` is only a fallback/placeholder; the deployed owner is truth.

### 5. Smoke test (data, not just "it booted")
Against the app URL, confirm the data plane works:
- `GET /api/kpi` -> non-null PMPM figures.
- `GET /api/insights` -> 4 rows (one per tab), non-empty `md`.
- `POST /api/agent` with a canned question -> a streamed answer.
Report a PASS/FAIL table with the live anchor values (PMPM, addressable $, etc.).

### 6. Report
Print the app URL and the smoke-test results. Tell the user the app reads the
customer's live data product (not sample values).

## Notes
- Deploying the app **into** the derived schema is the supported accelerator
  pattern (zero-config data resolution). A split layout (app object in a
  different schema than the data) would require setting `PHARMACY_DB`/
  `PHARMACY_SCHEMA`, which App Runtime cannot inject - avoid it.
- The dashboard data routes use owner's rights only; no `executeAsCaller` is
  needed. The agent drawer calls the Agent Run REST API with the service token.
- Mockup reference (design only, not deployed): `wiki/.../pharmacy-cost-command-center-nav3-leftpane.html`.
- App-facing data contract: `docs/app-data-contract.md`.
