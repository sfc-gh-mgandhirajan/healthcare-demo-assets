"""Register MONAI Vista3D in the Snowflake Model Registry, and run batch segmentation.

    python assets/segmentation/register_vista3d.py --phase register
    python assets/segmentation/register_vista3d.py --phase infer --limit 2
    python assets/segmentation/register_vista3d.py --phase land
    python assets/segmentation/register_vista3d.py --phase all

`infer` lands its own results and suspends the GPU pool on the way out, including
after a failure. `land` re-runs the landing step alone; it is idempotent, so it is
also the recovery path for a batch that half-completed.

WHY REGISTRATION RUNS LOCALLY AND NOT IN AN ML JOB
-------------------------------------------------
The reference implementation dispatches registration to a compute pool because it
must `snapshot_download` ~1 GB of weights from HuggingFace and needs egress plus a
GPU-capable container to smoke-test the model first.

Neither applies here:

  * The bundle is already staged (VOXEL_MODELS), pulled once from HuggingFace on a
    laptop. Inference therefore needs no external access integration at all --
    that is a real security reduction, not a convenience.
  * Vista3DModel.__init__ is deliberately inert. torch, monai and SimpleITK are
    imported inside methods, so the class can be constructed, and the model
    logged, on a machine with none of them installed.

So registration is a local upload of the bundle plus an explicit signature. That
turns a 15-minute GPU-billed job into about a minute of laptop time, and it means
a mistake in the signature costs nothing to fix.

WHY BATCH INFERENCE AND NOT A PERSISTENT SERVICE
------------------------------------------------
`create_service` holds a GPU node for the service's lifetime. Segmentation here is
queue-driven and bursty: 8 series, then nothing. `mv.run_batch()` starts a job,
runs the queue, and exits, so the GPU bills for the work and not for waiting.

That is also why VOXEL_GPU_POOL keeps AUTO_SUSPEND_SECS = 300 rather than a long
window: the failure mode being avoided is an idle A10G billing overnight.
"""

import argparse
import json
import os
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(REPO_ROOT, "assets", "segmentation"))

DB = "VOXEL_DB"
SCHEMA = "IMAGING"
FQ = f"{DB}.{SCHEMA}"

MODEL_NAME = "VISTA3D"
MODEL_VERSION = "V9"
GPU_POOL = "VOXEL_GPU_POOL"
MODEL_STAGE = f"{FQ}.VOXEL_MODELS"
OUTPUT_STAGE = f"{FQ}.VOXEL_SEG_OUT"
PAYLOAD_STAGE = f"{FQ}.VOXEL_ML_PAYLOAD"
LOCAL_BUNDLE = "/tmp/vista3d"

# Dependencies for the SERVED model. cuda_version must match the CUDA build of the
# torch wheel that gets resolved, or run_batch refuses the GPU request with
# "model ... does not have GPU runtime support".
#
# THE MONAI VERSION IS DELIBERATELY NOT PINNED TO 1.4.0
# ----------------------------------------------------
# The bundle's metadata.json declares monai_version 1.4.0, and pinning that is the
# obvious thing to do. It breaks the container:
#
#   AttributeError: module 'numpy._globals' has no attribute '_signature_descriptor'
#   ImportError: cannot load module more than once per process
#
# `monai==1.4.0` requires `numpy<2.0,>=1.24`. The container runtime already ships
# numpy 2.x with a torch built against it, so pinning MONAI forces a numpy 1.x
# wheel in alongside it and TWO numpy builds end up loaded in one process. That
# second message is the real tell; the AttributeError is a red herring that reads
# like an API change.
#
# This cost four GPU-billed image builds and one wrong diagnosis. The second error
# is also a SWIG message, so SimpleITK was blamed first and removed -- the failure
# survived its removal, which is what finally isolated the numpy pin. Reproducing
# it locally on CPU (tests/ has no coverage for this; /tmp scratch venv was used)
# took two minutes and would have found it immediately: workflow instantiation
# needs no GPU at all.
#
# MONAI 1.6.0 parses and runs this bundle unchanged on numpy 2.5.2, verified
# locally, including Vista3dEvaluator construction and checkpoint restore. So the
# floor is the feature requirement and the ceiling is left to the runtime.
#
# cucim-cu12, mlflow, scikit-image and matplotlib all appear in the bundle's
# required_packages_version and are all omitted. cucim is only used by GPU variants
# of transforms this config does not reference; mlflow only by MLFlowHandler, which
# it does not use; and skimage/matplotlib pull numpy-linked wheels for no benefit.
# In a managed container runtime, declaring MORE dependencies is riskier than
# declaring fewer -- every package that links numpy or torch can shadow the
# working build.
# numpy is pinned EXPLICITLY, and that is the point rather than an afterthought.
# Leaving it unconstrained let the resolver backtrack: unable to satisfy the set, it
# walked numpy versions downward until it reached a 1.19-era sdist with no Python
# 3.13 wheel, and the image build died in that package's setup.py guard:
#
#   RuntimeError: Cannot install on Python version 3.13.12;
#                 only versions >=3.6,<3.10 are supported.
#
# The message never names numpy, and the container runs Python 3.13.12, so an
# explicit modern floor both matches the runtime and stops the backtracking.
# numba and llvmlite are pinned even though NOTHING here imports them. They arrive
# transitively: log_model always injects snowflake-ml-python, which requires shap,
# which requires numba, which requires llvmlite. Both carry a hard Python guard in
# setup.py, and the image build died in it:
#
#   RuntimeError: Cannot install on Python version 3.13.12;
#                 only versions >=3.6,<3.10 are supported.
#
# That range corresponds to 2021-era releases, i.e. the resolver had backtracked a
# long way and then tried to BUILD an sdist that predates Python 3.13. A pip
# dry-run locally resolves the same requirement set cleanly to numba 0.66 /
# llvmlite 0.48 -- dry-runs never build, so they cannot reproduce this. Floors are
# the fix: uv cannot walk below a version that has cp313 wheels.
MODEL_PIP_REQUIREMENTS = [
    "numba>=0.61",             # transitive via shap; first series with cp313 wheels
    "llvmlite>=0.44",          # numba's compiler backend, same guard
    "numpy>=2.1,<3",           # first numpy series with cp313 wheels
    "monai>=1.5.0",            # NOT ==1.4.0: that pins numpy<2 and breaks the runtime
    "pytorch-ignite>=0.4.11",  # 0.4.11 is MONAI's minimum, not an exact requirement
    "pydicom>=2.4.0",          # PydicomReader: pure-python DICOM series assembly
    "highdicom>=0.24.0",       # DICOM-SEG encoding; pure python over pydicom
    "nibabel>=5.2.0",
    "einops",
    "fire",
]
CUDA_VERSION = "12.4"


def session_from_cli(connection):
    from snowflake.snowpark import Session

    builder = Session.builder
    if connection:
        builder = builder.config("connection_name", connection)
    s = builder.getOrCreate()
    s.sql(f"USE SCHEMA {FQ}").collect()
    return s


def ensure_local_bundle(session):
    """The bundle must be on local disk for log_model to package it.

    Pulled from the Snowflake stage rather than HuggingFace so this is
    reproducible and needs no egress: the staged copy is the source of truth.
    """
    weights = os.path.join(LOCAL_BUNDLE, "models", "model.pt")
    if os.path.exists(weights) and os.path.getsize(weights) > 800_000_000:
        print(f"bundle present at {LOCAL_BUNDLE}")
        return LOCAL_BUNDLE

    print(f"fetching bundle from @{MODEL_STAGE}/vista3d ...")
    for sub in ("models", "configs", "docs", "scripts"):
        os.makedirs(os.path.join(LOCAL_BUNDLE, sub), exist_ok=True)
        session.file.get(f"@{MODEL_STAGE}/vista3d/{sub}/",
                         os.path.join(LOCAL_BUNDLE, sub))
    if not os.path.exists(weights):
        raise RuntimeError(f"model.pt missing under {LOCAL_BUNDLE}/models")
    return LOCAL_BUNDLE


def phase_register(session):
    import pandas as pd
    from snowflake.ml.model import custom_model, model_signature
    from snowflake.ml.registry import Registry

    from vista3d_model import Vista3DModel

    bundle_root = ensure_local_bundle(session)

    # Drop service-free model cleanly. Order matters when a service exists: a
    # model backing a live service cannot be dropped.
    session.sql(f"DROP MODEL IF EXISTS {FQ}.{MODEL_NAME}").collect()

    model = Vista3DModel(custom_model.ModelContext(bundle=bundle_root))

    # An EXPLICIT signature, not sample_input_data. Passing sample data makes the
    # registry run predict() to infer the schema, which would download a real
    # series and load 872 MB of weights on a laptop with no GPU and no torch.
    sample_in = pd.DataFrame({
        "SERIES_INSTANCE_UID": ["1.2.3"],
        "FILE_PATHS": ['["a/b.dcm"]'],
        "SOURCE_STAGE": [f"{FQ}.VOXEL_PREAD_STG"],
        "LABEL_PROMPT": ["[1,3]"],
        "OUTPUT_STAGE": [OUTPUT_STAGE],
    })
    sample_out = pd.DataFrame({
        "SERIES_INSTANCE_UID": ["1.2.3"],
        "STATUS": ["SUCCESS"],
        "MASK_PATH": ["1.2.3/mask.nii.gz"],
        "N_LABELS_FOUND": [8],
        "LABEL_VOLUMES_JSON": ['{"1": 1234.5}'],
        "VOXEL_SPACING": ["[1.5,1.5,1.5]"],
        "VOLUME_SHAPE": ["[512,512,120]"],
        "ASSEMBLE_MS": [1000],
        "INFER_MS": [30000],
        "DEVICE": ["cuda"],
        "SEG_PATH": ["1.2.3/segmentation.dcm"],
        "N_SEGMENTS": [8],
    })

    reg = Registry(session=session, database_name=DB, schema_name=SCHEMA)
    print(f"logging {MODEL_NAME}/{MODEL_VERSION} (uploads ~832 MB bundle) ...")
    t0 = time.time()
    mv = reg.log_model(
        model=model,
        model_name=MODEL_NAME,
        version_name=MODEL_VERSION,
        signatures={"predict": model_signature.infer_signature(sample_in, sample_out)},
        # Without this the registry also resolves a warehouse-compatible
        # environment, which cannot satisfy a GPU torch model and fails the log.
        target_platforms=["SNOWPARK_CONTAINER_SERVICES"],
        options={"cuda_version": CUDA_VERSION},
        pip_requirements=MODEL_PIP_REQUIREMENTS,
        comment="MONAI Vista3D CT segmentation. Reads a DICOM series from a stage, "
                "assembles a HU volume with SimpleITK, runs the unmodified bundle "
                "workflow, writes a NIfTI mask.",
    )
    print(f"registered in {time.time() - t0:.0f}s: {mv.model_name}/{mv.version_name}")
    return mv


def claim_series(session, limit):
    """Take work from SEG_QUEUE using the existing atomic claim."""
    worker = f"vista3d-{int(time.time())}"
    rows = session.sql(
        f"CALL {FQ}.SP_SEG_CLAIM('{worker}', {limit})"
    ).collect()
    print(f"claimed {len(rows)} series as {worker}")
    return worker, [r["SERIES_INSTANCE_UID"] for r in rows]


def build_input_frame(session, series_uids):
    """Resolve each series to its file list on the INTERNAL stage.

    IT MUST BE THE INTERNAL STAGE, AND THIS IS A PLATFORM CONSTRAINT
    ---------------------------------------------------------------
    An earlier version of this resolved the stage from DICOM_SOURCE_CONFIG on the
    reasoning that a series belongs to a source, and that segmentation should not
    depend on VOXEL_PREAD_STG -- an internal stage whose real purpose is letting
    the gateway os.pread frame ranges out of a mounted volume.

    That reasoning was wrong, and the platform says so plainly:

        091003: Failure using stage area. Cause: [IDC_OPEN_DATA_STG GET and PUT
        commands are not supported with external stage]

    `session.file.get_stream` is a GET, and GET is not supported on external
    stages at all. The options are therefore SnowflakeFile.open over a
    BUILD_SCOPED_FILE_URL -- which needs a literal stage name in SQL and is
    really a UDF-sandbox API -- or reading the internal copy. The internal copy
    already exists because frame serving needs it, so segmentation reads that.

    The honest consequence: segmentation is coupled to SP_SYNC_PREAD_STAGE having
    run for the series in question. V_PREAD_COVERAGE is the check for that, and a
    series absent from the internal stage fails at assembly rather than producing
    a partial volume.
    """
    import pandas as pd

    uid_list = ", ".join(f"'{u}'" for u in series_uids)
    df = session.sql(f"""
        SELECT SERIES_INSTANCE_UID,
               ARRAY_AGG(FILE_NAME) WITHIN GROUP (ORDER BY FILE_NAME) AS PATHS
        FROM {FQ}.DICOM_INSTANCE
        WHERE SERIES_INSTANCE_UID IN ({uid_list})
        GROUP BY SERIES_INSTANCE_UID
    """).to_pandas()

    if df.empty:
        return pd.DataFrame()
    df = df.rename(columns={"PATHS": "FILE_PATHS"})
    df["FILE_PATHS"] = df["FILE_PATHS"].apply(
        lambda v: v if isinstance(v, str) else json.dumps(list(v))
    )
    df["SOURCE_STAGE"] = f"{FQ}.VOXEL_PREAD_STG"
    df["LABEL_PROMPT"] = None
    df["OUTPUT_STAGE"] = OUTPUT_STAGE
    return df[["SERIES_INSTANCE_UID", "FILE_PATHS", "SOURCE_STAGE",
               "LABEL_PROMPT", "OUTPUT_STAGE"]]


def phase_infer(session, limit):
    from snowflake.ml.registry import Registry

    reg = Registry(session=session, database_name=DB, schema_name=SCHEMA)
    mv = reg.get_model(MODEL_NAME).version(MODEL_VERSION)

    worker, series_uids = claim_series(session, limit)
    if not series_uids:
        print("nothing PENDING in SEG_QUEUE")
        return
    X = build_input_frame(session, series_uids)
    if X.empty:
        raise RuntimeError("claimed series resolved to no files -- check DICOM_INSTANCE")
    print(f"segmenting {len(X)} series, "
          f"{[len(json.loads(p)) for p in X['FILE_PATHS']]} slices")

    session.sql(f"ALTER COMPUTE POOL {GPU_POOL} RESUME IF SUSPENDED").collect()

    # These live in snowflake.ml.model.batch, NOT snowflake.ml.model. Importing
    # from the parent package raises ImportError with a message that reads like
    # the classes do not exist at all.
    from snowflake.ml.model.batch import JobSpec, OutputSpec, SaveMode

    # run_batch takes a SNOWPARK DataFrame, not pandas. Passing pandas fails
    # inside the client rather than at the call site, so convert explicitly.
    sp_df = session.create_dataframe(X)

    t0 = time.time()
    job = mv.run_batch(
        sp_df,
        compute_pool=GPU_POOL,
        output_spec=OutputSpec(
            stage_location=f"@{FQ}.VOXEL_SEG_OUT/_batch_out/",
            mode=SaveMode.OVERWRITE,
        ),
        # One replica, one worker, one row per batch. Vista3D holds 872 MB of
        # weights and runs a 128^3 sliding window on a single 24 GB A10G;
        # concurrent workers on one GPU contend for VRAM rather than doubling
        # throughput, and an OOM here costs the whole batch.
        job_spec=JobSpec(
            gpu_requests="1",
            num_workers=1,
            replicas=1,
            max_batch_rows=1,
        ),
    )
    print(f"batch job submitted: {getattr(job, 'id', job)}")
    print("first run builds a container image; expect 10-20 minutes")
    try:
        job.wait()
    finally:
        print(f"elapsed {time.time() - t0:.0f}s, status={getattr(job, 'status', '?')}")
        try:
            print("--- job logs (tail) ---")
            print(job.get_logs()[-6000:])
        except Exception as exc:  # noqa: BLE001
            print(f"(logs unavailable: {exc})")

    # LAND THE RESULTS. This is not optional and it is not a separate chore.
    #
    # SP_LAND_SEG_RESULTS existed and worked, and nothing called it -- so a fresh
    # deploy ran the GPU, wrote the mask, wrote the DICOM-SEG, and left SEG_RESULT
    # and SEG_RUN empty. Every consumer downstream of the GPU (V_SEG_UNIT_ECONOMICS,
    # validate_seg_anatomy.py, the demo) reads those tables, so the expensive half of
    # the pipeline completed and the observable half stayed blank. A procedure that
    # exists but is never invoked is indistinguishable from one that was never
    # written, and it is worse, because it reads as done.
    phase_land(session)


def phase_land(session):
    """Read the batch job's parquet output into SEG_RESULT / SEG_RUN.

    Idempotent, so running it twice is safe and running it after a partially
    successful batch is the intended recovery path.
    """
    print("landing batch results into SEG_RESULT / SEG_RUN ...")
    msg = session.sql(
        f"CALL {FQ}.SP_LAND_SEG_RESULTS('', '{MODEL_NAME}', '{MODEL_VERSION}')"
    ).collect()[0][0]
    print(f"  {msg}")

    # Print the evidence rather than the fact that a procedure returned. The
    # landing step is only worth anything if the rows are queryable afterwards, and
    # "0 structure rows" is a silent outcome that reads like success in the message
    # above but not here.
    rows = session.sql(f"""
        SELECT STRUCTURE_NAME, ROUND(VOLUME_MM3 / 1000, 1) AS ML
          FROM {FQ}.SEG_RESULT
         ORDER BY VOLUME_MM3 DESC
         LIMIT 20
    """).collect()
    if not rows:
        print("  WARNING: SEG_RESULT is empty after landing. Check that the batch "
              "job wrote parquet to @VOXEL_SEG_OUT/_batch_out/ and that STATUS "
              "is not FAILED for every row.")
    else:
        print(f"  SEG_RESULT now holds {len(rows)} structure row(s):")
        for r in rows:
            print(f"    {r[0]:<24} {r[1]:>8} mL")

    for r in session.sql(f"""
        SELECT MODEL_NAME, MODEL_VERSION, N_RUNS, TOTAL_SLICES,
               ROUND(GPU_SECONDS_PER_SLICE, 4) AS GPU_S_PER_SLICE, BOTTLENECK
          FROM {FQ}.V_SEG_UNIT_ECONOMICS
    """).collect():
        print(f"  unit economics: {r[0]}/{r[1]} | {r[2]} run(s) | {r[3]} slices "
              f"| {r[4]} GPU-s/slice | {r[5]}")


def suspend_gpu_pool(session):
    """Suspend the GPU pool. The single most expensive object in the system.

    GPU_NV_S bills at ~1.14 credits/hour whenever it is not SUSPENDED -- running a
    job is not required. AUTO_SUSPEND_SECS = 300 gets there eventually, but not
    after a run that left a job holding the node, which is exactly the case where
    an operator is distracted by a failure and forgets. Automating it here removes
    the one remaining cost control that depended on someone remembering.
    """
    session.sql(f"ALTER COMPUTE POOL {GPU_POOL} SUSPEND").collect()
    for r in session.sql(f"SHOW COMPUTE POOLS LIKE '{GPU_POOL}'").collect():
        d = r.as_dict()
        print(f"{GPU_POOL}: state={d.get('state')} active_nodes={d.get('active_nodes')}")


def _require_batch_api():
    """Fail before doing anything expensive if snowflake-ml-python is too old.

    Gates BOTH register and infer, because the failure is asymmetric and the
    register side is the silent one:

      * infer  -- ImportError on snowflake.ml.model.batch. Loud, costs nothing.
      * register -- succeeds, and bakes the INSTALLED snowflake-ml-python version
        into the model as a hard pin. An old pin (e.g. ==1.8.1) then conflicts with
        numpy>=2.1 and kills the container image build ~4 minutes in, ON THE GPU
        POOL, with a pip ResolutionImpossible that names numpy and never mentions
        that the real cause was which interpreter ran registration.

    So both phases must run on the same, current snowflake-ml-python. This checks
    the capability rather than a version number, because the release that
    introduced the batch API is not worth encoding here.

    NOTE: `uv run --with snowflake-ml-python` does NOT reliably shadow an ambient
    conda install -- it resolved the new package and still imported conda's 1.8.1.
    A real venv does:

        uv venv /tmp/segenv --python 3.11
        uv pip install --python /tmp/segenv/bin/python snowflake-ml-python pandas pyarrow
        /tmp/segenv/bin/python register_vista3d.py ...
    """
    try:
        import importlib.metadata as md
        installed = md.version("snowflake-ml-python")
    except Exception:
        installed = "unknown"

    try:
        from snowflake.ml.model.batch import JobSpec  # noqa: F401
    except ImportError:
        import snowflake.ml as _sfml
        # snowflake.ml is a namespace package, so __file__ is None; __path__ is what
        # actually identifies WHICH install was picked up, which is the whole point
        # of printing it -- the failure is usually the wrong interpreter, not a
        # missing package.
        where = next(iter(getattr(_sfml, "__path__", []) or []), "unknown")
        raise SystemExit(
            f"snowflake-ml-python {installed} lacks snowflake.ml.model.batch, which\n"
            f"  this script needs for run_batch(), and which is also a proxy for\n"
            f"  'new enough that registration will not bake a numpy-incompatible pin'.\n"
            f"  interpreter: {sys.executable}\n"
            f"  snowflake.ml resolved from: {where}\n\n"
            f"  Do NOT work around this by registering with the old version -- that\n"
            f"  failure lands on the GPU pool. Install a current snowflake-ml-python\n"
            f"  in a dedicated venv and re-run BOTH --phase register and --phase infer\n"
            f"  with that interpreter. See this function's docstring."
        )


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--connection", default=os.environ.get("SNOWFLAKE_CONNECTION_NAME"))
    ap.add_argument("--phase", choices=["register", "infer", "land", "all"],
                    default="all")
    ap.add_argument("--limit", type=int, default=2,
                    help="series to claim per inference run (GPU cost control)")
    ap.add_argument("--no-suspend", action="store_true",
                    help="leave the GPU pool running after inference. It bills while "
                         "ACTIVE or IDLE, so only use this between back-to-back runs.")
    args = ap.parse_args()

    if args.phase in ("register", "infer", "all"):
        _require_batch_api()

    session = session_from_cli(args.connection)
    print(f"connected: account={session.get_current_account()} "
          f"role={session.get_current_role()} wh={session.get_current_warehouse()}")
    try:
        if args.phase in ("register", "all"):
            phase_register(session)
        if args.phase in ("infer", "all"):
            try:
                phase_infer(session, args.limit)
            finally:
                # In a finally: a failed run is the case where the node is most
                # likely still held, and the case where the operator is least
                # likely to remember.
                if not args.no_suspend:
                    suspend_gpu_pool(session)
        if args.phase == "land":
            phase_land(session)
    finally:
        session.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
