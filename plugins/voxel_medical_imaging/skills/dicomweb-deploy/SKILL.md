---
name: dicomweb-deploy
description: Deploy or redeploy the Voxel DICOMweb service (OHIF viewer + gateway). Use when the viewer is down, after changing gateway code or nginx config, when CREATE SERVICE fails, or when a radiologist reports an empty study list. Triggers - deploy viewer, dicomweb service, OHIF, service is down, empty study list, endpoint provisioning, identity not forwarded.
---

# Deploy the Voxel DICOMweb service

Three containers behind **one** public endpoint: nginx router (OHIF + reverse
proxy), FastAPI gateway, prefetch sidecar.

The gateway and the sidecar talk to each other through a **file in the shared
memory volume**, not an in-process queue. That interface has no schema, no
enforcement, and fails silently — see "The prefetch hint contract" before assuming
the warm tier is doing anything.

## Before you touch anything

Read `$PLUGIN_DIR/assets/templates/dicomweb_spec_yaml.j2`. Three things in it are
load-bearing and easy to break:

1. `spec`, `capabilities`, and `serviceRoles` are **top-level siblings**. Nesting
   `capabilities` under `spec` fails with `unknown option 'capabilities' for 'spec'`.
2. Exactly **one** endpoint has `public: true`. A second public endpoint creates a
   second origin, and the ingress proxy's baseline CSP is `connect-src 'self'` —
   every SPA fetch to the other hostname is hard-blocked, and CORS cannot fix it
   because `connect-src` is evaluated in the sending document.
3. The `hotring` memory volume size is bounded by the instance family. On
   `CPU_X64_M` (28 GiB) with 6 GiB of container requests, 12 GiB works and 32 GiB
   fails at `CREATE SERVICE`.

## Paths & connection (set once)

```bash
PLUGIN_DIR="<absolute path to this plugin>"
WORK="$(mktemp -d)"
CONN="<snowflake connection name>"
CONFIG="$PLUGIN_DIR/config.json"
```

Never write into `$PLUGIN_DIR` — rendered config, the assembled build context, and
`upgrade.sql` all live under `$WORK`.

## Build and push

Images are built **locally** and pushed. This is deliberate: it means the ~200 MB
OHIF bundle and its `.wasm` codecs never sync through a workspace filesystem, and
the running service needs **no external access integration** at all. Verify that
last claim after deploy — it is the strongest governance statement here.

The router image bakes in the rendered `nginx.conf`, so render it before building.
The build context is assembled in `$WORK` rather than in the plugin directory, so
a deploy never leaves the plugin tree modified:

```bash
uv run --with jinja2 --with pyyaml python "$PLUGIN_DIR/assets/renderer/render.py" \
  --manifest "$PLUGIN_DIR/assets/build_manifest.yaml" \
  --templates "$PLUGIN_DIR/assets/templates" \
  --config "$CONFIG" --out "$WORK/rendered" --only DICOMWEB_NGINX

cp -R "$PLUGIN_DIR/assets/dicomweb/router" "$WORK/router"
cp "$WORK/rendered/nginx.conf" "$WORK/router/nginx.conf"
```

Validate the config before building — a bad directive costs a full image build:

```bash
docker run --rm -v "$WORK/router/nginx.conf:/etc/nginx/conf.d/default.conf:ro" \
  nginx:1.27-bookworm nginx -t
```

Then build and push:

```bash
snow spcs image-registry login -c "$CONN"
REPO=$(snow sql -c "$CONN" -q "SHOW IMAGE REPOSITORIES IN SCHEMA VOXEL_DB.IMAGING;" \
  | grep -oE '[a-z0-9-]+\.registry\.snowflakecomputing\.com[a-z0-9_/]*')

# --platform linux/amd64 is mandatory. SPCS pools are x86_64 and an arm64 image
# built on Apple silicon fails at start with an exec format error that names the
# container, not the architecture.
docker build --platform linux/amd64 -t "$REPO/voxel-gateway:latest" "$PLUGIN_DIR/assets/dicomweb/gateway"
docker build --platform linux/amd64 -t "$REPO/voxel-router:latest"  "$WORK/router"
docker push "$REPO/voxel-gateway:latest"
docker push "$REPO/voxel-router:latest"
```

## Create or upgrade

```bash
uv run --with jinja2 --with pyyaml python "$PLUGIN_DIR/assets/renderer/render.py" \
  --manifest "$PLUGIN_DIR/assets/build_manifest.yaml" \
  --templates "$PLUGIN_DIR/assets/templates" \
  --config "$CONFIG" --out "$WORK/rendered" --only DICOMWEB_SPEC

# first time
snow spcs service create VOXEL_DICOMWEB_SVC --spec-path "$WORK/rendered/dicomweb_spec.yaml" \
  --compute-pool VOXEL_WEB_POOL --database VOXEL_DB --schema IMAGING -c "$CONN"

# subsequently — upgrade, do not drop and recreate; dropping loses the ingress URL
snow sql -c "$CONN" --enable-templating NONE -f "$WORK/upgrade.sql"
```

There is no spec stage in this deployment, so the upgrade goes inline. Build
`upgrade.sql` as `ALTER SERVICE ... FROM SPECIFICATION $$<yaml>$$;` and run it with
`-f`, not `-q`:

```bash
uv run python - "$WORK" <<'PY'
import sys
from pathlib import Path
work = Path(sys.argv[1])
spec = (work / 'rendered/dicomweb_spec.yaml').read_text()
assert '$$' not in spec          # dollar quoting would terminate early
(work / 'upgrade.sql').write_text(
    'ALTER SERVICE VOXEL_DB.IMAGING.VOXEL_DICOMWEB_SVC\n'
    '  FROM SPECIFICATION $$\n' + spec + '\n$$;\n')
PY
```

Write the SQL to a file rather than passing it with `-q`. A spec is multi-line YAML
containing `#`, quotes and `!`, and zsh history expansion mangles it — this cost
real time more than once.

**An upgrade re-pulls `:latest`, so verify the digest actually changed.** `docker
push` can fail with `UNAUTHORIZED` on an expired registry login while
`ALTER SERVICE` then succeeds against the *old* image, reporting success for a
deploy that did nothing:

```sql
SHOW SERVICE CONTAINERS IN SERVICE VOXEL_DB.IMAGING.VOXEL_DICOMWEB_SVC;
-- compare image_digest against the digest docker push printed
```

Current: gateway `sha256:7617b4d9`, router `sha256:1a2d233c`.

`PENDING / Unschedulable due to insufficient resources` for the first minute or
two is **normal** — the pool node is still provisioning. It is only a problem if it
persists past ~3 minutes, which means the resource requests genuinely do not fit.

## Verification — run all five

These are not optional. **Four of the five failure modes they catch are silent** —
they present as a working viewer, or as HTTP 200, or as a fully green test suite.

### 1. Exactly one public endpoint

```sql
SHOW ENDPOINTS IN SERVICE VOXEL_DB.IMAGING.VOXEL_DICOMWEB_SVC;
```

One row, `is_public = true`. `Endpoints provisioning in progress` for a few
minutes is expected.

### 2. No egress capability

```sql
DESCRIBE SERVICE VOXEL_DB.IMAGING.VOXEL_DICOMWEB_SVC;
```

`external_access_integrations` must be **null**. This is what makes "the viewer
cannot exfiltrate" a property you can prove rather than a promise about code.

### 3. Caller identity reaches the gateway — THE SILENT ONE

If nginx drops the `Sf-Context-*` headers, `auth.Caller.from_headers` raises
`IdentityError` and the request 401s. That is the *designed* behaviour and it is
loud.

The dangerous version is the opposite: an implementation that falls back to
owner rights. Then **every row access policy passes**, no error is logged, and a
PHI disclosure looks exactly like success. `auth.py` refuses to construct an
owner-rights session on a request path specifically to make this impossible — but
verify the forwarding anyway.

Locally, without needing ingress auth:

```bash
# run the router against a header-echo backend in a shared network namespace
docker run -d --name vxecho -p 18000:8000 -p 18080:8080 <echo-image>
docker run -d --name vxrouter --network container:vxecho "$REPO/voxel-router:latest"
curl -s -H 'Sf-Context-Current-User: DR_SMITH' \
        -H 'Sf-Context-Current-User-Token: tok-abc' \
        http://localhost:18000/dicom-web/studies
```

Both headers must appear in the echoed response. Then confirm end-to-end against
the real ingress at `https://<ingress_url>/whoami` — `identity_ok` must be `true`.
If it reports the service user, queries are running as the owner.

Two ways this check lies to you, both encountered for real:

**A 200 from `/whoami` does not mean it worked.** `/whoami` needs its own nginx
`location` block. Without it the path falls through to `location /` and
`try_files ... /index.html`, so nginx serves the **OHIF SPA with HTTP 200**. The
probe that proves queries run as the caller rather than the owner returns HTML and
a success status. Always assert the body parses as JSON:

```bash
curl -s -H "Authorization: Snowflake Token=\"$PAT\"" \
     "https://<ingress_url>/whoami" | head -c 200
# expect JSON. If you see <!DOCTYPE html>, the location block is missing.
```

This is a third silent failure mode on top of the two named at the top of this
skill, and it is the worst of them, because the symptom is indistinguishable from
success unless you look at the body.

**`Bearer` is the wrong scheme for SPCS ingress.** Use:

```
Authorization: Snowflake Token="<pat>"
X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN
```

`Bearer` is correct only for the REST/SQL API on the `.com` host. Against the
`.app` ingress it returns a **302 to the Snowflake login page** — which presents as
a credential problem, not a header-format problem, so the natural response is to
regenerate the PAT repeatedly and never suspect the scheme.

### 4. The row access policy discriminates

Structural checks prove nothing here; a policy whose body always returns TRUE also
"exists". Test behaviourally:

```sql
USE ROLE VOXEL_RADIOLOGIST;
SELECT COUNT(*) FROM VOXEL_DB.IMAGING.DICOM_INSTANCE;   -- entitled: > 0

USE ROLE SYSADMIN;
DELETE FROM VOXEL_DB.IMAGING.USER_STUDY_SCOPE WHERE USER_NAME = '<USER>';

USE ROLE VOXEL_RADIOLOGIST;
SELECT COUNT(*) FROM VOXEL_DB.IMAGING.DICOM_INSTANCE;   -- MUST be 0
```

Revocation takes effect immediately because there is no cached authorization
decision to invalidate. Restore the row afterwards.

### 5. The frame plane, not just a frame — a native-only smoke test is not enough

If you verify frame serving against a native CT, you have exercised **arithmetic**
offset resolution only — native instances compute offsets closed-form and never read
`DICOM_FRAME_OFFSET`. The stored-offset branch is a completely separate code path,
and it can be broken while every native read is perfect.

Include a **multi-frame encapsulated** instance. `WADO.encapsulated_frame_serving` in
`$PLUGIN_DIR/tests/conformance_harness.py` is the assertion that does this, and it exists because
an Extended Offset Table off-by-8 sat undetected behind a fully green suite.

## Granting a radiologist access

Two independent grants, on purpose. Endpoint access and row scope are separate
decisions, so handing someone the viewer does not hand them the whole corpus.

```sql
GRANT ROLE VOXEL_RADIOLOGIST TO USER "<user>";
GRANT SERVICE ROLE VOXEL_DB.IMAGING.VOXEL_DICOMWEB_SVC!VOXEL_VIEWER
  TO ROLE VOXEL_RADIOLOGIST;
INSERT INTO VOXEL_DB.IMAGING.USER_STUDY_SCOPE (USER_NAME, SITE, DEPARTMENT)
  VALUES ('<USER_UPPERCASE>', 'DEMO', 'RESEARCH');
```

`USER_NAME` must match `CURRENT_USER()` exactly — uppercase for a normal Snowflake
user. A lowercase row matches nothing and the symptom is an **empty study list**,
not an error.

Note: the service role name contains `!`, which zsh expands inside double quotes.
Put this in a file and use `snow sql -f`, or the statement will be mangled.

## The prefetch hint contract

The gateway appends one relative path per line to `$HOTRING/_hints` with `O_APPEND`;
`prefetch.py` tails that file from its last read offset and copies each file into the
RAM ring. That is the entire protocol.

It is a **cross-container interface with no schema and no enforcement**, and when it
breaks nothing errors. The original build put hints on an in-process `queue.Queue` in
the gateway while the sidecar — a *separate container* — tailed a file nobody wrote.
The sidecar starved, 100% of frame reads came off the stage volume, the viewer worked
perfectly, and the only symptom anywhere in the system was
`X-Voxel-Served-From: stage` on every response.

Check it directly:

```sql
-- should be a mix; all-'stage' after sustained traffic means the hints are not landing
SELECT SERVED_FROM, COUNT(*) FROM VOXEL_DB.IMAGING.FRAME_LATENCY_SAMPLE GROUP BY 1;
```

```bash
snow spcs service logs VOXEL_DB.IMAGING.VOXEL_DICOMWEB_SVC \
  --container-name prefetch --instance-id 0 --num-lines 20 -c "$CONN"
# healthy: repeated "warmed N/N hinted file(s); ring=X MiB"
# broken:  only the startup line, forever
```

**Before you tune anything here, know that the RAM tier is not currently justified.**
Measured single-frame reads over the ingress, cold-start outliers excluded: warm p50
**79 ms** (n=6, avg 340 KiB) against stage p50 **81 ms** (n=22, avg 343 KiB). A 2 ms
delta is noise. Latency is dominated by the ingress round-trip, not the storage read,
and the stage volume is already in the node's page cache — the ring is competing with
RAM that was effectively free.

So do not spend time sizing `VOXEL_HOTRING_BUDGET_BYTES` expecting a speedup. The
tier is retained on the argument that a larger corpus would diverge, and that
argument is currently unevidenced.

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| `unknown option 'capabilities' for 'spec'` | `capabilities` nested under `spec` | Dedent to top level |
| `invalid value NNGi for 'memory.size'` | memory volume exceeds node RAM | Lower `hotring_size_gi` |
| `exec format error` | arm64 image | Rebuild with `--platform linux/amd64` |
| `underscores_in_headers is not allowed here` | directive in a `location` block | Move to `server` level (once) |
| Viewer loads, study list empty | `USER_STUDY_SCOPE` case mismatch, or no row | Insert uppercase username |
| Every frame request 401s | `Sf-Context-*` not forwarded | Check `proxy_set_header` in nginx.conf |
| Compressed frames render as nothing | `.wasm` served as wrong MIME | Confirm `application/wasm` |
| `X-Voxel-Served-From: stage` on **every** response | gateway is not writing `$HOTRING/_hints`, so the sidecar has nothing to warm | Check the hint file exists and grows; see "The prefetch hint contract" below. Check this **before** touching any budget |
| Frames slow, prefetch log shows evictions | eviction budget exceeds volume size | `VOXEL_HOTRING_BUDGET_BYTES` ≈ 80% of volume |
| `restricted by a Projection Policy: FILE_NAME` | Phase 4 deployed, viewer role not allowlisted | Add `VOXEL_RADIOLOGIST` to `PROTECT_FILE_PATH` |

## Cost

The compute pool bills whenever it is not suspended, whether or not anyone is
viewing. `INITIALLY_SUSPENDED = TRUE` is set on both pools because the default is
`FALSE` and an idle pool bills.

```sql
ALTER COMPUTE POOL VOXEL_WEB_POOL SUSPEND;   -- stops the meter
```

Suspending the pool stops the service. The ingress URL survives, so resuming does
not require regranting anything.
