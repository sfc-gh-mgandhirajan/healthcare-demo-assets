# Snowflake for Medical Imaging

A Snowflake-native medical imaging platform that turns a bucket of DICOM files
into a governed, queryable catalog, serves that catalog to a standard radiology
viewer over real DICOMweb, runs GPU organ segmentation against it, and measures
its own cost.

Everything runs inside one Snowflake account. There is no external application
server, no side database, and no separate metadata store to keep in sync.

---

## 1. What it does

| Capability | Implementation |
| --- | --- |
| **Catalog** | DICOM header parsing in a vectorized Python UDTF, landing one row per SOP instance with the full DICOM JSON model retained as `VARIANT`. |
| **Serving** | DICOMweb `QIDO-RS` / `WADO-RS` implemented in a container service, driven directly by SQL against the catalog. `STOW-RS` returns `501` by design. |
| **Viewing** | OHIF 3.13.3, built at image build time, served from the same ingress as the API so the viewer and its data share an origin and an identity. |
| **Segmentation** | MONAI Vista3D on a GPU compute pool, writing per-organ volumes back into the catalog and emitting a standards-conformant DICOM-SEG. |
| **Governance** | Masking, projection and row-access policies attached to catalog columns, so PHI posture is enforced by the database rather than by the application. |
| **Cost** | A benchmark ledger that records credits consumed per ingest run and computes steady-state cost at zero users. |

---

## 2. The three design decisions that matter

Most of the engineering in this repo follows from three choices. They are worth
understanding before reading the code.

### 2.1 Read the header, not the file

A DICOM file is a header followed by a large pixel payload. To catalog a study
you need only the header. The ingest UDTF issues a ranged read, parses up to
the `PixelData` element, records the byte offset where pixel data begins, and
stops.

On the reference corpus this reads **0.72% of the corpus by volume** — 102 MiB of
header out of 13.96 GiB of files — to produce a complete catalog of 29,837
instances. Ingest cost scales with study *count*, not study *size*.

### 2.2 Compute frame offsets, don't store them

The obvious way to serve an individual frame from a multi-frame object is to
store a byte offset per frame. That is one row per frame, which for a large
corpus is orders of magnitude more rows than there are files.

For uncompressed transfer syntaxes every frame is the same size, so the offset
is closed-form:

```
frame_offset = PIXEL_DATA_START + n * frame_bytes
```

The platform stores the two constants per *instance* and derives the rest. Offsets are
only materialized for the encapsulated (compressed) case, where frame lengths
genuinely vary and must be recovered from the Basic Offset Table, the Extended
Offset Table, or an item scan. On the reference corpus that is 43 stored rows
against 29,830 instances resolved arithmetically.

The arithmetic is done in **bits**, not bytes, because 1-bit `BINARY` DICOM-SEG
frames are not byte-aligned.

### 2.3 Governance belongs to the database

The gateway executes as the calling user. It holds no service credentials for
data access and applies no filtering of its own. A radiologist's row and column
visibility is decided by policies attached to the catalog tables, which means the
same restrictions apply whether the caller arrives through the viewer, through a
SQL client, or through a notebook.

This is verifiable: the invariant checks confirm each policy is attached to the
columns it claims to protect, and the de-identification salt lives in a
separately-secured table rather than in the policy body, so `GET_DDL` on the
policy does not disclose it.

---

## 3. Repository layout

```
assets/
  build_manifest.yaml        Declares every deployable object: template, phase, grain
  config.sample.json         Every knob. Copy to config.json and edit.
  renderer/render.py         Jinja2 renderer (StrictUndefined) -> one SQL file per object
  templates/                 24 templates, the entire platform as SQL
  dicomweb/
    gateway/                 DICOMweb implementation (Python)
    router/                  nginx + OHIF build
  segmentation/              Vista3D model registration and inference entrypoint
docs/
  ARCHITECTURE.md            End-to-end architecture, 9 diagrams
  DATA_MODEL.md              Table-level data model, 5 diagrams
skills/                      Six operational runbooks, executable as a CoCo plugin
                             (see 7.1 — symlink into ~/.snowflake/cortex/plugins)
tests/                       Unit tests plus standards-conformance harnesses
```

### 3.1 The templates are the platform

There is no imperative deployment path. `assets/templates/` contains the
complete definition of every table, view, procedure, task, policy, function,
stage, service spec and grant. `build_manifest.yaml` declares which template
produces which object and in which phase it belongs.

Deployment is: render the manifest against `config.json`, then execute the
resulting SQL in phase order. The renderer uses `StrictUndefined`, so a config
key referenced by a template but absent from `config.json` fails at render time
rather than producing a half-configured deployment.

Object numbering encodes ordering and grouping:

| Range | Phase | Contents |
| --- | --- | --- |
| `00` | `account` | Warehouse, role grants, account-level prerequisites |
| `01`–`07` | `core` | Database, schema, stages, catalog tables, reference data, frame plane, UDFs, masking policies |
| `10`–`13` | `pipeline` | Pipeline config, procedures, task DAG, initial backfill |
| `20`–`21` | `bench` | Benchmark ledger and cost profiles |
| `30` | `seg` | Segmentation queue, results, DICOM-SEG writer |
| `40`–`41` | `deid` | Tag-plane and pixel-plane de-identification |
| `50`–`51` | `conformance` | Standards conformance matrix and compatibility projections |
| — | `dicomweb` | Service spec, nginx config, service grants |
| — | `validate` | Invariant checks |
| — | `teardown` | Complete removal, including everything that bills |

### 3.2 Skills

`skills/` is a CoCo plugin (`.cortex-plugin/plugin.json`). Each skill is an
operational runbook with the diagnostic queries inline:

| Skill | Purpose |
| --- | --- |
| `deploy` | Deploy from scratch: render, provision, backfill, validate |
| `dicomweb-deploy` | Deploy or repair the viewer and gateway service |
| `pipeline-ops` | Scan, load, replay errors, reconcile deletes, diagnose a stalled backfill |
| `segment` | Run Vista3D over queued series and control GPU spend |
| `bench` | Run the ingest benchmark and compute unit economics |
| `teardown` | Remove the platform, or stop it billing without deleting it |

---

## 4. Deployed footprint

A complete deployment provisions, in `VOXEL_DB`:

- **22 tables** — catalog, frame plane, pipeline state, queues, results, ledgers
- **28 views** — 20 analytic/serving in `IMAGING`, 8 in `BENCH`
- **15 procedures**, **5 functions** — ingest, segmentation, DICOM-SEG writing, de-identification
- **5 stages** — source, prefetch, model, image repo, scratch
- **3 serverless tasks** — scan, load, reconcile
- **2 compute pools** — CPU for serving, GPU for inference (created suspended)
- **1 container service**, 3 containers — gateway, router, OHIF
- **10 policy attachments** — masking, projection, row access

Pixel bytes are never copied into Snowflake-managed storage. The catalog holds
metadata and byte offsets; pixel data is read from the source bucket on demand
through an external stage.

---

## 5. Verification

Correctness claims in this repo are checked rather than asserted.

| Harness | What it proves |
| --- | --- |
| `assets/templates/checks.sql.j2` | 17 invariants over a live deployment: policy attachment, referential integrity, offset consistency, queue state, salt hygiene |
| `tests/` (73 unit tests) | Header parsing, VR typing, offset arithmetic, gateway response shapes, auth behavior |
| `tests/verify_frame_offsets.py` | Frame offsets resolve to real frame boundaries in the actual files |
| `tests/conformance_harness.py` | DICOMweb responses against the PS3.18 conformance matrix |
| `tests/validate_seg_anatomy.py` | Segmented organ volumes fall in physiologic range |
| `tests/verify_seg_discoverable.py` | Generated DICOM-SEG is discoverable and loadable by the viewer |
| `tests/build_compressed_corpus.py` | Synthesizes encapsulated multi-frame objects to exercise the offset-table paths the open corpus does not contain |

The conformance matrix distinguishes `MATCH`, `MISMATCH` and `UNTESTED`. It
currently reports 32 `MATCH`, 8 `UNTESTED`, 0 `MISMATCH`; the `UNTESTED` rows are
labeled as such rather than being quietly counted as passes.

---

## 6. Measured characteristics

Against the reference corpus (29,837 instances / 361 series / 357 studies / 347
patients / 6 modalities / 13.96 GiB) on a `MEDIUM` Gen2 warehouse:

| Metric | Value |
| --- | --- |
| Ingest wall clock | 429.6 s |
| Throughput | 69.5 files/sec |
| Errors | 0 |
| Credits consumed | ~0.52 |
| Corpus bytes read | 102.3 MiB (0.716%) |
| Frame offsets stored | 43 rows for 29,837 instances |
| Steady-state cost at zero users | Serving pool only; GPU pool provisioned suspended |

Cost is reported in **credits**, not dollars. Rate information is not available
from `ORGANIZATION_USAGE` in every account, and a credit figure is verifiable
where a derived dollar figure is not.

---

## 7. Getting started

```bash
# 1. Configure
cp assets/config.sample.json config.json
$EDITOR config.json          # database, schema, warehouse, role, source bucket

# 2. Render (uv supplies jinja2 + pyyaml per-invocation; no virtualenv needed)
uv run --with jinja2 --with pyyaml python assets/renderer/render.py \
  --manifest assets/build_manifest.yaml \
  --templates assets/templates \
  --config config.json \
  --out /tmp/voxel-rendered

# 3. Deploy, in phase order
snow sql --enable-templating NONE -f /tmp/voxel-rendered/00_ACCOUNT_SETUP.sql
# ... through the phase table in section 3.1

# 4. Validate
snow sql --enable-templating NONE -f /tmp/voxel-rendered/CHECKS.sql
```

Or drive it through the `deploy` skill, which does the same thing with
per-phase verification between steps.

`--enable-templating NONE` is required: the generated SQL contains `&`
characters that the CLI would otherwise interpret as client-side variables.

`config.json` is gitignored — it carries a real database, warehouse and bucket
name.

### 7.1 Installing as a Cortex Code plugin

This directory is a self-contained CoCo plugin — `.cortex-plugin/plugin.json`
plus `skills/`. There is no registry to join; installation is a symlink:

```bash
ln -s "$PWD" ~/.snowflake/cortex/plugins/snowflake-medical-imaging
```

The six skills then resolve as `snowflake-medical-imaging:deploy`,
`snowflake-medical-imaging:pipeline-ops`, and so on.

Skills take a `PLUGIN_DIR` and render into a `mktemp -d` scratch directory, so
they work from any working directory and never write into the installed plugin
tree.

---

## 8. What this deliberately does not do

Being explicit about scope, because each of these is a defensible omission
rather than an oversight:

| Not implemented | Why |
| --- | --- |
| `STOW-RS` ingest | Returns `501`. Ingest is bucket-driven and idempotent; an HTTP write path would create a second, unreconciled source of truth. |
| Pixel data in Snowflake storage | Offsets into the source bucket cost nothing to store and cannot drift from the files. |
| Compressed-transfer-syntax frame arithmetic | Frame lengths genuinely vary; those offsets are materialized instead of derived. |
| MPR / 3D reconstruction in the viewer | Gated off. It requires client-side volume loading that the current serving path does not guarantee. |
| Dollar-denominated cost | Requires rate-sheet data that is not reliably available. Credits are reported instead. |
| Multi-tenant isolation | Single-account, single-tenant by design. Isolation would be a row-access-policy extension, not an application change. |

---

## 9. Further reading

- **`docs/ARCHITECTURE.md`** — system context, ingest metadata and pixel paths,
  read path as a sequence diagram, deployment topology, deploy ordering,
  governance enforcement, the segmentation loop, and orchestration.
- **`docs/DATA_MODEL.md`** — entity relationships for the ingest core,
  segmentation, governance, and conformance/cost planes, plus the view layer.
