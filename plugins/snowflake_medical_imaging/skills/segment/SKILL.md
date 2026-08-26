---
name: segment
description: Run MONAI Vista3D segmentation over queued CT series on a GPU compute pool, and land results in SEG_RESULT. Use when the seg queue has PENDING work, after ingesting new CT, when a segmentation failed, or when GPU spend needs controlling. Triggers - run segmentation, vista3d, seg queue, segment series, organ volumes, GPU inference, register model.
---

# Run Vista3D segmentation

Queue-driven GPU inference. The SQL plane (`SEG_QUEUE` / `SEG_RESULT` / `SEG_RUN`,
atomic claim, reaper, unit economics) is created by `30_SEGMENTATION`. The model and
the driver live in `$PLUGIN_DIR/assets/segmentation/`.

## Cost first, because this is the only GPU in the system

`VOXEL_GPU_POOL` is `GPU_NV_S` (1x A10G, 24 GB) at roughly **1.14 credits/hour**, and
it bills whenever it is not SUSPENDED — running a job is not required. Measured:

| | |
|---|---|
| per series | ~48 s (35 s volume assembly, 12 s inference) |
| first run after any dependency change | + ~12 min container image build, **on the GPU pool** |
| one series end to end, including build | ~1.0 credit |

The image build is CPU-bound but runs on the GPU pool, so a dependency typo costs A10G
minutes. Change dependencies as rarely as possible, and verify them locally first
(see "Debug locally, not on the GPU").

**Always confirm the pool is suspended when finished:**

```sql
ALTER COMPUTE POOL VOXEL_GPU_POOL SUSPEND;
SHOW COMPUTE POOLS LIKE 'VOXEL_GPU_POOL';   -- state must be SUSPENDED, active_nodes 0
```

`AUTO_SUSPEND_SECS = 300` will do it eventually, but do not rely on it after a failed
run — a job left RUNNING holds the node.

## Enqueue

```sql
CALL VOXEL_DB.IMAGING.SP_SEG_ENQUEUE('CT', 40, 200);   -- modality, min slices, batch
SELECT STATE, COUNT(*), SUM(N_INSTANCES) FROM VOXEL_DB.IMAGING.SEG_QUEUE GROUP BY STATE;
```

`SKIPPED` is a real answer, not a failure. Enqueue gates on
`V_SERIES_GEOMETRY.IS_SEGMENTABLE_VOLUME`, which rejects 4D/multi-phase series where
several instances share a z-position. Vista3D on a 10-phase cardiac stack returns a
confidently wrong segmentation rather than an error, so the gate matters.

## Paths & connection (set once)

```bash
PLUGIN_DIR="<absolute path to this plugin>"
WORK="$(mktemp -d)"
CONN="<snowflake connection name>"
```

Never write into `$PLUGIN_DIR`. `register_vista3d.py` resolves its own imports from
`__file__`, so it runs correctly from any working directory.

## Register the model

Registration runs **locally** and needs no GPU: `Vista3DModel.__init__` is inert, and
torch/monai/pydicom are imported inside methods.

```bash
uv run --with snowflake-ml-python --with pandas \
  python "$PLUGIN_DIR/assets/segmentation/register_vista3d.py" \
  --connection "$CONN" --phase register
```

It uploads the staged bundle (~832 MB) and takes about 90 s. The bundle itself is
pulled from `@VOXEL_MODELS/vista3d`, staged once so **inference needs no external
access integration at all**.

### Do not touch the dependency list casually

`MODEL_PIP_REQUIREMENTS` in `register_vista3d.py` carries floors that each cost a
failed GPU-billed build. The comments explain each one. In particular:

- **Never pin `monai==1.4.0`** even though the bundle's metadata declares it. It
  requires `numpy<2` and the runtime ships numpy 2.x; two numpy builds in one process
  produce `cannot load module more than once per process`.
- **Keep the `numba` / `llvmlite` floors.** Nothing here imports them; they arrive via
  `snowflake-ml-python` -> `shap` -> `numba` -> `llvmlite`. Without floors the resolver
  backtracks into 2021-era sdists that cannot build on Python 3.13.
- **Do not add packages "for safety."** `scikit-image` and `matplotlib` are in the
  bundle's metadata and adding them broke the image. In a managed runtime, every extra
  package that links numpy or torch can shadow a working build.

## Run inference

```bash
uv run --with snowflake-ml-python --with pandas \
  python "$PLUGIN_DIR/assets/segmentation/register_vista3d.py" \
  --connection "$CONN" --phase infer --limit 2
```

`--limit` is the GPU cost control: it is how many series `SP_SEG_CLAIM` takes. Start
at 1.

This uses `mv.run_batch()`, not `create_service()`, deliberately. A service holds a
GPU node for its lifetime; segmentation is bursty, so a batch job that exits is the
cost-correct shape.

Frames are read from **`VOXEL_PREAD_STG`, the internal stage** — not the external
source stage. That is a platform constraint, not a preference:

```
091003: [IDC_OPEN_DATA_STG GET and PUT commands are not supported with external stage]
```

`session.file.get_stream` is a GET. So segmentation depends on `SP_SYNC_PREAD_STAGE`
having run for those series; check `V_PREAD_COVERAGE` if assembly fails.

## Land the results

The model writes a mask to a stage and a manifest row to parquet. Promoting that into
the catalog is a separate SQL step, so a crashed Ray actor cannot leave a half-written
row and a multi-minute inference never holds a transaction open.

Load the parquet into `SEG_RESULT`, then close `SEG_QUEUE`. Key on
`SERIES_INSTANCE_UID` throughout and **never on mask content** — blank slices at volume
boundaries are byte-identical across series, so a content-keyed join silently fans out
and attaches organ volumes to the wrong patient.

## Verify the output is right, not merely present

A finished job and a written mask prove nothing about correctness. Check the volumes
against physiology:

```sql
SELECT STRUCTURE_NAME, ROUND(VOLUME_MM3/1000,1) AS VOLUME_ML
FROM VOXEL_DB.IMAGING.SEG_RESULT
WHERE SERIES_INSTANCE_UID = '<uid>' ORDER BY VOLUME_MM3 DESC;
```

**Liver is the load-bearing check: 1200-1600 mL in a normal adult.** Vista3D's config
applies `ScaleIntensityRanged(a_min=-963.8, a_max=1053.7)`, which assumes Hounsfield
Units. If `RescaleSlope`/`RescaleIntercept` were skipped — a 1024 HU offset on most CT —
the volume clips to near-uniform and no plausible liver can come out. A sane liver is
evidence that HU conversion, slice ordering and the LPS-to-RAS affine are all correct
together. Spleen 100-250 mL is the second check.

If liver volume is implausible, suspect **preprocessing before the model**.

Known on the one series run so far: liver, spleen, stomach and aorta are in range;
both kidneys came out implausibly small, most likely FOV truncation (117 slices at
2.5 mm is 292 mm of z-coverage). Unconfirmed.

## Debug locally, not on the GPU

The step that fails most is workflow instantiation, and **it needs no GPU**. A scratch
CPU venv reproduces it in about two minutes versus ~12 GPU-billed minutes per guess:

```bash
python3 -m venv /tmp/segtest
/tmp/segtest/bin/pip install torch --index-url https://download.pytorch.org/whl/cpu
/tmp/segtest/bin/pip install monai pytorch-ignite pydicom nibabel einops fire
# then create_workflow() against configs/inference.json with a synthetic NIfTI
```

Assert two things there before ever registering:

1. The evaluator instantiates: `wf.parser.get_parsed_content("evaluator")`.
2. **The input override actually took.** `ConfigWorkflow` exposes no settable
   `input_dict`, so `workflow.input_dict = {...}` sets a dead attribute and the bundle
   keeps using the path baked into `inference.json`
   (`/data/Task09_Spleen/imagesTr/spleen_10.nii.gz`). Here that path is absent so it
   errors; anywhere it existed it would segment the wrong file and return the mask
   keyed to your SeriesInstanceUID. Overrides must go through `create_workflow`.

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| `Cannot locate class or function path: 'scripts.inferer.Vista3dInferer'` | bundle root not on `sys.path`; MONAI imports bundle-local `scripts.*` normally | insert bundle root into `sys.path` before `create_workflow` |
| `Failed to instantiate 'Vista3dEvaluator'` listing 8 keywords | `pytorch-ignite` missing; MONAI pulls it via `optional_import` so it fails at construction, not import | add `pytorch-ignite>=0.4.11` |
| `numpy._globals has no attribute '_signature_descriptor'` + `cannot load module more than once per process` | two numpy builds; caused by `monai==1.4.0` requiring numpy<2 | use `monai>=1.5.0` |
| `Cannot install on Python version 3.13.x; only versions >=3.6,<3.10` | resolver backtracked into an ancient `numba`/`llvmlite` sdist | keep the `numba>=0.61` / `llvmlite>=0.44` floors |
| `GET and PUT commands are not supported with external stage` | reading frames from the external source stage | read from `VOXEL_PREAD_STG`; run `SP_SYNC_PREAD_STAGE` |
| Job DONE but no mask on the stage | `predict` caught a per-row exception | read `STATUS` from the `_batch_out` parquet; it carries the traceback |
| Queue rows stuck `CLAIMED` after a crash | claim held by a dead worker | `CALL SP_SEG_REAP(0)` returns them to PENDING |
| Rows went `FAILED` during build iteration | reaper hit `MAX_ATTEMPTS` on series no working model ever attempted | reset to PENDING explicitly; `FAILED` overstates what is known |

## DICOM-SEG and catalog ingest

The mask is written twice: a NIfTI (for analysis) and a **DICOM-SEG** object with real
SNOMED-CT segment codes (for the viewer). To make it visible the SEG must be in the
catalog, not just on a stage:

```sql
COPY FILES INTO @VOXEL_PREAD_STG/voxel_seg/ FROM @VOXEL_SEG_OUT/
  PATTERN = '.*segmentation[.]dcm';
ALTER STAGE VOXEL_PREAD_STG REFRESH;
-- VOXEL_SEG is registered as its OWN source, because SOURCE_NAME is what
-- STUDY_SCOPE_POLICY keys on: derived data must be scopable separately from
-- the images it came from.
CALL SP_SCAN_ALL_SOURCES();
CALL SP_LOAD_METADATA(50);
```

Verify a client can actually find it, which is the property that matters:

```bash
SEG_STUDY_UID=<uid> VOXEL_PAT=... VOXEL_URL=... \
  uv run python "$PLUGIN_DIR/tests/verify_seg_discoverable.py"
```

That asserts QIDO returns the SEG series next to the CT, the SOPClassUID is
Segmentation Storage, `BitsAllocated` is 1, and frames retrieve at the correct
length with advancing offsets.

**SEQUENCES MUST BE EXPANDED IN RAW_METADATA.** A DICOM-SEG is defined entirely by
`SegmentSequence` (labels, SNOMED codes) and `PerFrameFunctionalGroupsSequence`
(frame -> segment + position). The header parser once stored every SQ element as
`{"vr":"SQ","Value":[]}` because QIDO never reads sequence content -- and WADO-RS
`/metadata` is served from the same column, so the viewer received a segmentation
describing nothing and drew nothing, at HTTP 200, with the suite green. If an
overlay is invisible, check `ARRAY_SIZE(RAW_METADATA:"00620002":"Value")` first.

**VALIDATE THE MASK, NOT THE SCREENSHOT.** Run
`uv run --with numpy --with pydicom python "$PLUGIN_DIR/tests/validate_seg_anatomy.py"`.
A mirrored or misregistered mask looks entirely convincing in a viewer; laterality
(liver right, spleen left, IVC right of aorta) is what catches it. Volume must use
the slice INCREMENT, not `SliceThickness` -- overlapping reconstruction is routine
and using thickness double-counts every voxel.

**BIT-PACKED FRAMES ARE A TRAP.** DICOM-SEG BINARY is one bit per pixel.
`bits_allocated // 8` is 0, so every frame becomes a zero-length read served as
HTTP 200. The SQL coverage view uses `(BITS_ALLOCATED / 8)` — a float — so it
disagrees and calls the instance servable. Frame arithmetic must be computed in
bits, and a frame that does not start on a byte boundary must be refused, not
served shifted.

## What is not built
- **Volume assembly dominates runtime** — 35 s against 12 s of inference, from 117
  sequential `get_stream` calls. `V_SEG_UNIT_ECONOMICS.BOTTLENECK` reports `LOAD_BOUND`
  on this evidence, so a larger GPU would buy almost nothing.
- **No Dice / ground truth.** This corpus has no reference masks, so accuracy is
  assessed only by physiological plausibility.
