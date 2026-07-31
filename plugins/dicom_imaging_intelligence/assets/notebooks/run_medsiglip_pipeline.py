#!/usr/bin/env python
"""
Run the MedSigLIP-448 pipeline against Snowflake from anywhere.

Equivalent to dicom-search/medsiglip_log_model.ipynb, but driven from a local
Python environment. The heavy Python work (HuggingFace download, model
registration) is dispatched to a Snowflake compute pool via Snowflake ML Jobs,
so this script needs no local GPU, no torch, and no 3.5GB model download.

Two phases:

  Phase A - remote (ML Job on the compute pool)
      Downloads google/medsiglip-448, registers it as a CustomModel with
      predict + embed_text, and deploys the SPCS inference service.

  Phase B - local (plain SQL over the Snowpark session)
      Converts DICOM to base64 PNG, embeds every slice by calling the SPCS
      service function directly in SQL, and verifies the result.

The HuggingFace token is mounted into the job container as an environment
variable straight from the Snowflake secret, via spec_overrides. Nothing
reads _snowflake, and no transient token-reader UDF is created. The token
never appears in this file, in argv, or in the job payload.

Usage:
    python dicom-search/run_medsiglip_pipeline.py --connection <name>
    python dicom-search/run_medsiglip_pipeline.py --connection <name> --phase a
    python dicom-search/run_medsiglip_pipeline.py --connection <name> --phase b

Requires: pip install "snowflake-ml-python>=1.26.0"
"""

#   NOTE: this module deliberately does NOT use `from __future__ import
#   annotations`. PEP 563 turns annotations into strings, and snowflake-ml
#   introspects real type objects in two places - ML Jobs session injection
#   (`session: Session`) and CustomModel's `inference_api` input validation.
#   With deferred annotations both break, the second with a misleading
#   "Input for predict method should have type pandas.DataFrame."

import argparse
import json
import sys
import time

# -----------------------------------------------------------------------------
# Defaults. Every one of these can be overridden by the plugin's config.json via
# --config, so this script is not pinned to the account it was written in.
# -----------------------------------------------------------------------------
DEMO_DB = "SF_CLINICAL_DB"
DEMO_SCHEMA = "EXPLORER"
FQ = f"{DEMO_DB}.{DEMO_SCHEMA}"

GPU_POOL = "DICOM_GPU_POOL"
EAI_NAME = "DICOM_EXPLORER_EAI"
HF_SECRET = "HF_TOKEN_SECRET"
PAYLOAD_STAGE = "ML_JOB_PAYLOAD_STG"

MODEL_NAME = "MEDSIGLIP_448"
MODEL_VERSION = "V2"
SERVICE_NAME = "MEDSIGLIP_448_SVC"
HF_REPO = "google/medsiglip-448"
EMBED_DIM = 1152


def apply_config(path):
    """Overlay the plugin's config.json onto the module defaults.

    Only the keys this script cares about are read; the rest of config.json is
    for the SQL templates.
    """
    global DEMO_DB, DEMO_SCHEMA, FQ, GPU_POOL, EAI_NAME, HF_SECRET
    global MODEL_NAME, SERVICE_NAME, EMBED_DIM

    import json as _json
    with open(path) as fh:
        cfg = _json.load(fh)

    DEMO_DB = cfg.get("target_database", DEMO_DB)
    DEMO_SCHEMA = cfg.get("target_schema", DEMO_SCHEMA)
    FQ = f"{DEMO_DB}.{DEMO_SCHEMA}"
    GPU_POOL = cfg.get("gpu_pool", GPU_POOL)
    EAI_NAME = cfg.get("eai_name", EAI_NAME)
    HF_SECRET = cfg.get("hf_secret", HF_SECRET)
    MODEL_NAME = cfg.get("model_name", MODEL_NAME)
    SERVICE_NAME = cfg.get("service_name", SERVICE_NAME)
    EMBED_DIM = int(cfg.get("embed_dim", EMBED_DIM))
    return cfg


def remote_config():
    """The subset the REMOTE job needs, as a JSON string.

    register_and_deploy runs inside an ML Jobs container and cannot rely on this
    module's globals, so its configuration is passed in as a single scalar
    argument. One string keeps the serialization surface as small as possible.
    """
    return json.dumps(
        {
            "fq": FQ,
            "model_name": MODEL_NAME,
            "model_version": MODEL_VERSION,
            "service_name": SERVICE_NAME,
            "gpu_pool": GPU_POOL,
            "hf_repo": HF_REPO,
            "embed_dim": EMBED_DIM,
            "hf_secret": HF_SECRET,
            "database": DEMO_DB,
            "schema": DEMO_SCHEMA,
        }
    )

# Pinned for the SERVED MODEL environment (log_model), where reproducibility
# matters: SigLIP support in transformers moves, and an unpinned upgrade can
# silently change embedding values.
MODEL_PIP_REQUIREMENTS = [
    "transformers==4.44.2",
    "torch==2.4.1",
    "pillow>=10.0.0",
    "huggingface_hub>=0.24.0",
    "sentencepiece",
    "protobuf",
    "accelerate",
]

# Requirements for the JOB container. torch is deliberately NOT pinned here:
# the ML container runtime already ships a working GPU torch, and pinning it
# forces a fresh ~2.5GB CUDA wheel download on every run for no benefit.
JOB_PIP_REQUIREMENTS = [
    "transformers==4.44.2",
    "huggingface_hub>=0.24.0",
    "sentencepiece",
    "protobuf",
    "accelerate",
]


def secret_spec_overrides() -> dict:
    """Mount the Snowflake secret into the job container as $HF_TOKEN.

    This is why the ML Jobs path is cleaner than the notebook: the notebook
    container runtime does not expose the _snowflake module, so reading the
    secret there requires creating and dropping a short-lived reader UDF.
    Here the platform injects it directly.
    """
    return {
        "spec": {
            "containers": [
                {
                    "name": "main",  # primary container is always "main"
                    "secrets": [
                        {
                            "snowflakeSecret": f"{FQ}.{HF_SECRET}",
                            "envVarName": "HF_TOKEN",
                            "secretKeyRef": "secret_string",
                        }
                    ],
                }
            ]
        }
    }


# -----------------------------------------------------------------------------
# Phase A payload - runs remotely on the compute pool.
#
# Everything this function needs must be defined inside it: the body is
# serialized and shipped to the job container, so module-level names from this
# file are not guaranteed to resolve there.
# -----------------------------------------------------------------------------
def register_and_deploy(config_json: str) -> str:
    """Download, register, and deploy MedSigLIP. Runs inside the job container.

    Takes its configuration as a JSON STRING rather than reading module globals,
    because this function is serialized and executed remotely where those globals
    are not dependable. See remote_config().

    The session is built here rather than accepted as an injected parameter.
    ML Jobs can inject a Snowpark session by matching a `session: Session`
    type annotation, but that does NOT work when the module uses
    `from __future__ import annotations` (PEP 563 turns annotations into
    strings, so there is no type object to match) and it fails with a
    confusing:
        TypeError: register_and_deploy() missing 1 required positional argument
    Session.builder.getOrCreate() works for every payload type, so it is the
    safer pattern.
    """
    import base64
    import json as _json
    import os
    from io import BytesIO

    from snowflake.snowpark import Session

    session = Session.builder.getOrCreate()

    import pandas as pd
    from PIL import Image

    # Imported locally, not taken from the module scope: this body runs in the
    # job container, where module-level globals are not dependable.
    _rcfg = _json.loads(config_json)

    fq = _rcfg["fq"]
    model_name, model_version = _rcfg["model_name"], _rcfg["model_version"]
    service_name = _rcfg["service_name"]
    gpu_pool = _rcfg["gpu_pool"]
    hf_repo = _rcfg["hf_repo"]
    embed_dim = int(_rcfg["embed_dim"])
    hf_secret = _rcfg["hf_secret"]

    hf_token = os.environ.get("HF_TOKEN")
    if not hf_token:
        raise RuntimeError(
            "HF_TOKEN not present in the job container. The secret mount did "
            "not apply - check spec_overrides and that the EAI lists "
            f"{fq}.{hf_secret} in ALLOWED_AUTHENTICATION_SECRETS."
        )
    print(f"HF token present ({len(hf_token)} chars).", flush=True)

    # Force the classic cdn-lfs transfer path instead of Xet.
    #
    # hf-xet is preinstalled in the container runtime, so huggingface_hub
    # prefers Xet storage. Xet fans out to hosts like cas-bridge and
    # cas-server.xethub.hf.co; if any of them is not in the egress network
    # rule, the download does not error - it retries silently and the job
    # hangs with no log output at all. That is indistinguishable from a slow
    # download unless you sample the log length over time.
    #
    # cdn-lfs.huggingface.co is allowlisted, so disabling Xet gives a
    # deterministic path. Set before importing huggingface_hub - the flag is
    # read at import time.
    os.environ["HF_HUB_DISABLE_XET"] = "1"
    os.environ.setdefault("HF_HUB_ENABLE_HF_TRANSFER", "0")

    from huggingface_hub import snapshot_download

    print(f"Downloading {hf_repo} (Xet disabled, using cdn-lfs) ...", flush=True)
    dl_start = __import__("time").time()
    try:
        snapshot_path = snapshot_download(
            repo_id=hf_repo, cache_dir="/tmp/medsiglip-448", token=hf_token
        )
    except Exception as e:
        msg = str(e)
        if "401" in msg or "gated" in msg.lower() or "awaiting" in msg.lower():
            raise RuntimeError(
                f"HuggingFace rejected the request for {hf_repo}. This model is "
                f"GATED - accept the terms at https://huggingface.co/{hf_repo} "
                f"with the account that issued this token, then re-run.\n"
                f"Original error: {msg}"
            ) from e
        raise
    print(
        f"Model downloaded to {snapshot_path} "
        f"in {__import__('time').time() - dl_start:.0f}s",
        flush=True,
    )
    print(f"Model downloaded to {snapshot_path}")

    from snowflake.ml.model import custom_model, model_signature
    from snowflake.ml.registry import Registry

    class MedSigLIPModel(custom_model.CustomModel):
        """google/medsiglip-448 dual encoder for image and text embeddings."""

        def __init__(self, context: custom_model.ModelContext) -> None:
            super().__init__(context)
            import torch
            from transformers import AutoModel, AutoProcessor

            path = self.context.path("model_dir")
            self.device = "cuda" if torch.cuda.is_available() else "cpu"
            self.model = AutoModel.from_pretrained(path).to(self.device).eval()
            self.processor = AutoProcessor.from_pretrained(path)

        @custom_model.inference_api
        def predict(self, input_df: pd.DataFrame) -> pd.DataFrame:
            import base64 as _b64
            from io import BytesIO as _BytesIO

            import torch
            from PIL import Image as _Image

            out = []
            for _, row in input_df.iterrows():
                img = _Image.open(
                    _BytesIO(_b64.b64decode(row["IMAGE_BYTES"]))
                ).convert("RGB")
                inputs = self.processor(images=img, return_tensors="pt").to(self.device)
                with torch.no_grad():
                    feats = self.model.get_image_features(**inputs)
                out.append(feats.cpu().numpy().flatten().tolist())
            return pd.DataFrame({"EMBEDDING": out})

        @custom_model.inference_api
        def embed_text(self, input_df: pd.DataFrame) -> pd.DataFrame:
            """padding="max_length" is required.

            SigLIP trains with a fixed 64-token padded context. padding=True
            pads only to the longest sequence in the batch, which yields
            different, degraded text embeddings with no error raised - query
            vectors drift out of alignment with the image vectors and
            retrieval quality quietly drops.
            """
            import torch

            out = []
            for _, row in input_df.iterrows():
                inputs = self.processor(
                    text=row["TEXT"], return_tensors="pt", padding="max_length"
                ).to(self.device)
                with torch.no_grad():
                    feats = self.model.get_text_features(**inputs)
                out.append(feats.cpu().numpy().flatten().tolist())
            return pd.DataFrame({"EMBEDDING": out})

    # Smoke test both encoders before registering.
    buf = BytesIO()
    Image.new("RGB", (448, 448), color=(128, 128, 128)).save(buf, format="PNG")
    sample_image_df = pd.DataFrame(
        {"IMAGE_BYTES": [base64.b64encode(buf.getvalue()).decode("utf-8")]}
    )
    sample_text_df = pd.DataFrame({"TEXT": ["cardiac CT angiography with calcification"]})

    model = MedSigLIPModel(custom_model.ModelContext(model_dir=snapshot_path))
    print(f"Model loaded on device: {model.device}", flush=True)

    image_out = model.predict(sample_image_df)
    text_out = model.embed_text(sample_text_df)
    img_dim, txt_dim = len(image_out["EMBEDDING"][0]), len(text_out["EMBEDDING"][0])
    print(f"Image embedding dims: {img_dim}, text embedding dims: {txt_dim}", flush=True)
    assert img_dim == embed_dim, f"expected {embed_dim}, got {img_dim}"
    assert txt_dim == embed_dim, f"expected {embed_dim}, got {txt_dim}"

    # Drop service before model - a model backing a live service cannot be
    # dropped. Deliberately not wrapped in try/except: silent failure here
    # resurfaces as a confusing log_model error.
    session.sql(f"DROP SERVICE IF EXISTS {fq}.{service_name}").collect()
    session.sql(f"DROP MODEL IF EXISTS {fq}.{model_name}").collect()

    reg = Registry(
        session=session,
        database_name=_rcfg["database"],
        schema_name=_rcfg["schema"],
    )
    mv = reg.log_model(
        model=model,
        model_name=model_name,
        version_name=model_version,
        signatures={
            "predict": model_signature.infer_signature(sample_image_df, image_out),
            "embed_text": model_signature.infer_signature(sample_text_df, text_out),
        },
        # target_platforms: without it the registry also resolves a
        # warehouse-compatible environment, which fails for a GPU torch model.
        target_platforms=["SNOWPARK_CONTAINER_SERVICES"],
        # cuda_version is REQUIRED to get a GPU-capable model image. Without
        # it, create_service(gpu_requests="1") fails with:
        #   "model ... does not have GPU runtime support"
        # 12.1 matches the CUDA build of the pinned torch==2.4.1 wheel.
        options={"cuda_version": "12.1"},
        pip_requirements=[
            "transformers==4.44.2",
            "torch==2.4.1",
            "pillow>=10.0.0",
            "huggingface_hub>=0.24.0",
            "sentencepiece",
            "protobuf",
            "accelerate",
        ],
        comment="MedSigLIP-448 dual encoder: predict(IMAGE_BYTES), embed_text(TEXT)",
    )
    print(f"Registered {mv.model_name}/{mv.version_name}", flush=True)

    print(f"Creating service {service_name} on {gpu_pool} ...", flush=True)
    mv.create_service(
        service_name=service_name,
        service_compute_pool=gpu_pool,
        gpu_requests="1",
        max_instances=1,
    )
    return f"registered {model_name}/{model_version}, service {service_name} initiated"


def wait_for_service(session, timeout_min: int = 45) -> str:
    """Poll until the SPCS service is serving. create_service can return early."""
    import json

    deadline = time.time() + timeout_min * 60
    status = None
    while time.time() < deadline:
        raw = session.sql(
            f"SELECT SYSTEM$GET_SERVICE_STATUS('{FQ}.{SERVICE_NAME}')"
        ).collect()[0][0]
        try:
            parsed = json.loads(raw)
            status = parsed[0].get("status") if isinstance(parsed, list) else str(parsed)
        except (json.JSONDecodeError, TypeError, IndexError):
            status = str(raw)

        print(f"  {time.strftime('%H:%M:%S')}  service status: {status}")
        if status and status.upper() in ("READY", "RUNNING"):
            return status
        if status and status.upper() in ("FAILED", "DONE"):
            raise RuntimeError(
                f"Service entered terminal state {status}. "
                f"Check: SELECT SYSTEM$GET_SERVICE_LOGS('{FQ}.{SERVICE_NAME}', 0, 'model-inference');"
            )
        time.sleep(30)
    raise TimeoutError(f"Service not ready after {timeout_min} minutes (last: {status})")


def phase_a(session) -> None:
    """Dispatch model registration + service deploy to a compute pool."""
    from snowflake.ml.jobs import remote

    session.sql(f"CREATE STAGE IF NOT EXISTS {FQ}.{PAYLOAD_STAGE}").collect()

    print(f"Dispatching ML Job to {GPU_POOL} ...")
    job_fn = remote(
        GPU_POOL,
        stage_name=f"{FQ}.{PAYLOAD_STAGE}",
        session=session,
        pip_requirements=JOB_PIP_REQUIREMENTS,
        external_access_integrations=[EAI_NAME],
        spec_overrides=secret_spec_overrides(),
    )(register_and_deploy)

    job = job_fn(remote_config())
    print(f"Job submitted: {job.id}")
    print("Streaming until completion (model download + registration is slow) ...")

    try:
        result = job.result()
        print(f"Job finished: {result}")
    except Exception:
        print("\n--- job logs ---")
        try:
            print(job.get_logs())
        except Exception as log_err:
            print(f"(could not fetch logs: {log_err})")
        raise

    print("\nWaiting for the SPCS service to serve ...")
    wait_for_service(session)


def phase_b(session) -> None:
    """Convert, embed, and verify by calling the pipeline procedures.

    This used to issue CREATE OR REPLACE TABLE for both DICOM_BASE64_IMAGES and
    DICOM_EMBEDDINGS, which made every run a full rebuild: re-rendering every
    PNG and paying for GPU inference again on rows that were already correct.

    It now calls the same procedures TASK_DICOM_BASE64 and TASK_DICOM_EMBED
    call, so the script and the scheduled pipeline cannot drift apart, and only
    outstanding work is done.

    Embedding still goes through the service function in SQL, inside
    SP_EMBED_PENDING. That keeps each vector on the same row as its FILE_NAME by
    construction. An earlier approach ran the model over a DataFrame and joined
    back on IMAGE_BYTES - blank slices at series boundaries are byte-identical,
    so that join fanned out and silently attached embeddings to the wrong files.
    """
    print("Phase 1: rendering base64 PNGs (SP_CONVERT_IMAGES) ...")
    msg = session.sql(f"CALL {FQ}.SP_CONVERT_IMAGES(100000)").collect()[0][0]
    print(f"  {msg}")

    print("Phase 2: embedding via the SPCS service (SP_EMBED_PENDING) ...")
    msg = session.sql(f"CALL {FQ}.SP_EMBED_PENDING(100000)").collect()[0][0]
    print(f"  {msg}")
    if msg.startswith("SKIPPED"):
        raise RuntimeError(
            "Embedding was skipped because the GPU service is not serving. "
            "Run phase a first, or resume MEDSIGLIP_448_SVC, then re-run phase b."
        )

    staged = session.sql(f"""
        SELECT COUNT(*) AS N, COUNT(DISTINCT FILE_NAME) AS D
        FROM {FQ}.DICOM_BASE64_IMAGES
    """).collect()[0]
    print(f"  staged rows={staged['N']} distinct_files={staged['D']}")
    if staged["N"] != staged["D"]:
        raise RuntimeError("More than one base64 row per file; embeddings would misalign.")

    row = session.sql(f"""
        SELECT
            COUNT(*)                     AS TOTAL_ROWS,
            COUNT(DISTINCT FILE_NAME)    AS DISTINCT_FILES,
            COUNT(DISTINCT SERIES_INSTANCE_UID) AS SERIES,
            COUNT(DISTINCT COLLECTION)   AS COLLECTIONS,
            ANY_VALUE(TYPEOF(EMBEDDING)) AS EMB_TYPE,
            MAX(VECTOR_L2_DISTANCE(EMBEDDING, EMBEDDING)) AS MAX_SELF_DIST
        FROM {FQ}.DICOM_EMBEDDINGS
    """).collect()[0]

    print("\nDICOM_EMBEDDINGS:")
    for k in ("TOTAL_ROWS", "DISTINCT_FILES", "SERIES", "COLLECTIONS", "EMB_TYPE", "MAX_SELF_DIST"):
        print(f"  {k:<15} {row[k]}")

    if row["TOTAL_ROWS"] != row["DISTINCT_FILES"]:
        raise RuntimeError(
            f"Fanout detected: {row['TOTAL_ROWS']} rows but "
            f"{row['DISTINCT_FILES']} distinct files."
        )
    if row["EMB_TYPE"] != "VECTOR":
        raise RuntimeError(f"EMBEDDING is {row['EMB_TYPE']}, expected VECTOR.")

    # Relevance sanity check. A cardiac query should outrank the NLST chest
    # screening series. If the ordering looks arbitrary, suspect the
    # embed_text padding mode before anything else.
    print("\nRelevance check - 'cardiac CT angiography with calcified aortic valve':")
    for r in session.sql(f"""
        WITH q AS (
            SELECT {SERVICE_NAME}!embed_text(
                'cardiac CT angiography with calcified aortic valve'
            ):EMBEDDING::VECTOR(FLOAT, {EMBED_DIM}) AS QVEC
        )
        SELECT e.LABEL, e.COLLECTION,
               ROUND(MAX(VECTOR_COSINE_SIMILARITY(e.EMBEDDING, q.QVEC)), 4) AS BEST_SIM
        FROM {FQ}.DICOM_EMBEDDINGS e, q
        GROUP BY e.LABEL, e.COLLECTION
        ORDER BY BEST_SIM DESC
    """).collect():
        print(f"  {r['BEST_SIM']:>8}  {r['LABEL']}  ({r['COLLECTION']})")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connection", help="Snowflake CLI connection name")
    parser.add_argument(
        "--config",
        help="Path to the plugin's config.json. Overrides the built-in defaults "
             "for database, schema, GPU pool, EAI, secret, model, and service names.",
    )
    parser.add_argument(
        "--phase",
        choices=["a", "b", "all"],
        default="all",
        help="a = register + deploy (remote job); b = embed + verify (SQL); default all",
    )
    args = parser.parse_args()

    if args.config:
        apply_config(args.config)
        print(f"Config loaded from {args.config}: target={FQ} pool={GPU_POOL} service={SERVICE_NAME}")

    from snowflake.snowpark import Session

    builder = Session.builder
    if args.connection:
        builder = builder.config("connection_name", args.connection)
    session = builder.getOrCreate()

    print(f"Connected. account={session.get_current_account()} role={session.get_current_role()}")
    session.sql(f"USE SCHEMA {FQ}").collect()

    try:
        if args.phase in ("a", "all"):
            phase_a(session)
        if args.phase in ("b", "all"):
            phase_b(session)
    finally:
        session.close()

    print("\nDone. Next: snow sql -f dicom-search/byo_search.sql")
    return 0


if __name__ == "__main__":
    sys.exit(main())
