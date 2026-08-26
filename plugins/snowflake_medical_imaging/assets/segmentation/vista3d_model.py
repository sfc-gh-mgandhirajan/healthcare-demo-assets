"""MONAI Vista3D segmentation as a Snowflake registered model.

WHY THIS FILE EXISTS AT ALL
---------------------------
The seg phase shipped as SQL only: SEG_QUEUE / SEG_RESULT / SEG_RUN, an atomic
claim, a reaper, and unit economics. Everything downstream of the queue -- the
thing that actually produces a segmentation -- did not exist. This is that.

DELIBERATELY NOT USING `from __future__ import annotations`
----------------------------------------------------------
PEP 563 turns annotations into strings. snowflake-ml introspects REAL type
objects in two places: ML Jobs session injection (`session: Session`) and
CustomModel's `inference_api` input validation. With deferred annotations the
second fails with a misleading

    Input for predict method should have type pandas.DataFrame.

which sends you looking at your DataFrame instead of your imports. Do not add it.

WHY THE MODEL READS ITS OWN FRAMES
----------------------------------
`run_batch` can stream a stage file into a column
(InputSpec FULL_STAGE_PATH -> RAW_BYTES), which is the natural pattern for
one-file-per-row work. A CT series is 40-600 files that must be assembled into
ONE volume, so that pattern does not fit: there is no column shape that carries
a variable-length set of files.

So the contract is inverted. The DataFrame carries only identifiers -- a
SeriesInstanceUID and its file list -- and the model opens a Snowpark session
inside the container and reads the frames itself. A 512x512x120 CT volume is
~60 MB as int16; keeping that out of the SQL layer is the whole point.

WHY MONAI's PydicomReader RATHER THAN SimpleITK OR HAND-ROLLED STACKING
----------------------------------------------------------------------
Vista3D's preprocessing (configs/inference.json) is:

    Spacingd(pixdim=[1.5,1.5,1.5])           needs true physical spacing
    ScaleIntensityRanged(a_min=-963.82,      needs HOUNSFIELD UNITS
                         a_max=1053.68)
    Orientationd(axcodes="RAS")              needs a valid affine

Every one of those is a correctness requirement, not a preference:

  * Feeding raw stored values instead of HU silently shifts the whole intensity
    window. The model still returns a confident, plausible-looking mask -- of
    the wrong tissue. CT stored values need RescaleSlope/RescaleIntercept
    applied; for many CT series slope=1 intercept=-1024, so forgetting it is a
    1024 HU offset that clips to a nearly uniform volume.
  * Getting slice order wrong mirrors the anatomy.
  * Omitting the affine makes Orientationd a no-op, so an LPS-acquired volume is
    segmented as if it were RAS -- left/right flipped. DICOM is LPS and NIfTI is
    RAS, so that conversion is mandatory, not cosmetic.

This was first written with SimpleITK's ImageSeriesReader, which does all of the
above correctly and sorts geometrically rather than by InstanceNumber. It was
replaced during a misdiagnosis, and that is worth recording accurately.

The container was failing with:

    AttributeError: module 'numpy._globals' has no attribute '_signature_descriptor'
    ImportError: cannot load module more than once per process

The second line is a SWIG message and SimpleITK is a SWIG extension, so SimpleITK
was blamed and removed. The failure survived its removal. The real cause was
pinning `monai==1.4.0`, which requires `numpy<2.0` and therefore installs a second
numpy alongside the runtime's numpy 2.x. Two numpy builds in one process produce
both messages.

PydicomReader is kept anyway, on its own merits rather than the wrong reason: it is
pure pydicom with no compiled extension, it is MONAI's own tested implementation of
the nibabel DICOM affine formula with affine_lps_to_ras=True and rescale applied,
and it removes a heavy binary dependency from a container that already has enough
ABI coupling. Hand-rolling the affine was the other option and is a well-known
source of silently mirrored medical images.

KNOWN LIMITATION, STATED RATHER THAN HIDDEN: PydicomReader sorts slices by
InstanceNumber, whereas SimpleITK sorted by ImagePositionPatient projected on the
slice normal. For a series where those two disagree the volume is assembled in the
wrong order. That is rare, and V_SERIES_GEOMETRY gates enqueue on positional
integrity, but it is a genuinely weaker guarantee and a real regression against the
SimpleITK version.

WHY THE BUNDLE'S OWN WORKFLOW RUNS THE INFERENCE
-----------------------------------------------
Vista3D's postprocessing is VistaPostTransformd -> Invertd -> nan_to_num(255).
`Invertd` maps the prediction back through the preprocessing chain to the
ORIGINAL voxel grid. Reimplementing that -- or forgetting it -- yields a mask at
1.5 mm isotropic, cropped to the foreground bounding box, that no longer aligns
with the source DICOM. It would look correct in isolation and be wrong on
overlay. So MONAI's ConfigWorkflow drives the bundle unchanged and only
`input_dict` / `output_dir` are overridden.
"""

import base64
import json
import os
import shutil
import sys
import tempfile
import time

import pandas as pd
from snowflake.ml.model import custom_model

# Vista3D label indices (configs/../docs/labels.json). Kept as a small explicit
# default rather than "everything": `everything_labels` is 117 classes, and
# sliding-window inference cost scales with the prompt set. A caller can pass
# any subset per row.
DEFAULT_LABEL_PROMPT = [
    1,   # liver
    3,   # spleen
    4,   # pancreas
    5,   # right kidney
    14,  # left kidney
    6,   # aorta
    7,   # inferior vena cava
    12,  # stomach
]


# Vista3D label id -> (name, SNOMED-CT concept id).
#
# REAL CODES, NOT A PRIVATE SCHEME. A DICOM-SEG whose segmented property type is
# a made-up code is still a valid file, but nothing downstream can interpret it:
# a viewer cannot colour it meaningfully and no registry can aggregate it. Using
# SNOMED-CT is the difference between an artifact a machine can read and a blob
# that happens to be DICOM.
SEG_LABELS = {
    1:  ("liver",              "10200004"),
    3:  ("spleen",             "78961009"),
    4:  ("pancreas",           "15776009"),
    5:  ("right kidney",       "9846009"),
    6:  ("aorta",              "15825003"),
    7:  ("inferior vena cava", "64131007"),
    12: ("stomach",            "69695003"),
    14: ("left kidney",        "18639004"),
}


class Vista3DModel(custom_model.CustomModel):
    """Segment a DICOM series with MONAI Vista3D and write a NIfTI mask.

    Input DataFrame columns:
        SERIES_INSTANCE_UID  VARCHAR  identity of the volume, and the join key
        FILE_PATHS           VARCHAR  JSON array of stage-relative .dcm paths
        SOURCE_STAGE         VARCHAR  fully qualified stage holding those files
        LABEL_PROMPT         VARCHAR  JSON array of Vista3D label ids, or NULL
        OUTPUT_STAGE         VARCHAR  stage to write the mask into

    Output DataFrame columns:
        SERIES_INSTANCE_UID, STATUS, MASK_PATH, N_LABELS_FOUND,
        LABEL_VOLUMES_JSON, VOXEL_SPACING, VOLUME_SHAPE,
        ASSEMBLE_MS, INFER_MS, DEVICE, SEG_PATH, N_SEGMENTS

    STATUS is one of:
        SUCCESS                  mask + DICOM-SEG written
        SUCCESS_NO_SEGMENTS      inference ran, found none of the prompted labels
        SUCCESS_SEG_FAILED: ...  mask written, DICOM-SEG encoding failed
        FAILED: ...              no usable output
    """

    def __init__(self, context):
        super().__init__(context)
        self._bundle_root = None
        self._session = None

    # -- lazily-built collaborators ---------------------------------------
    def _get_session(self):
        """Build a Snowpark session from inside the container.

        `get_active_session()` does not exist in an SPCS container. The platform
        mounts an OAuth token at /snowflake/session/token instead. Falling back
        to get_active_session keeps the class usable when logged/tested locally.
        """
        if self._session is not None:
            return self._session
        token_path = "/snowflake/session/token"
        if os.path.exists(token_path):
            from snowflake.snowpark import Session

            with open(token_path) as fh:
                token = fh.read().strip()
            self._session = Session.builder.configs({
                "host": os.environ.get("SNOWFLAKE_HOST", "localhost"),
                "account": os.environ.get("SNOWFLAKE_ACCOUNT", ""),
                "authenticator": "oauth",
                "token": token,
            }).create()
        else:
            from snowflake.snowpark.context import get_active_session

            self._session = get_active_session()
        return self._session

    def _get_bundle_root(self):
        """Materialize the staged bundle into a writable directory, and make it importable.

        Two non-obvious requirements:

        WRITABLE. ConfigWorkflow resolves output_dir to `@bundle_root + '/eval'`
        and MONAI writes there. The model artifact directory is read-only in the
        serving container, so the bundle is copied out once per container.

        ON sys.path. inference.json refers to `scripts.inferer.Vista3dInferer`
        and `scripts.evaluator.Vista3dEvaluator` -- a package that ships INSIDE
        the bundle. MONAI resolves those by plain import, so without the bundle
        root on sys.path instantiation fails with

            ModuleNotFoundError: Cannot locate class or function path:
            'scripts.inferer.Vista3dInferer'

        which reads like a missing dependency rather than a path problem.
        """
        if self._bundle_root is not None:
            return self._bundle_root
        src = self.context.path("bundle")
        dst = os.path.join(tempfile.gettempdir(), "vista3d_bundle")
        if not os.path.isdir(dst):
            shutil.copytree(src, dst)
        if dst not in sys.path:
            sys.path.insert(0, dst)
        self._bundle_root = dst
        return dst

    def _build_workflow(self, nifti_path, label_prompt, out_dir):
        """Create a bundle inference workflow for ONE volume.

        WHY THIS IS REBUILT PER SERIES RATHER THAN CACHED
        ------------------------------------------------
        ConfigWorkflow exposes no settable `input_dict` / `output_dir`. Assigning
        them on the instance -- `workflow.input_dict = {...}` -- silently sets a
        plain Python attribute that the config parser never reads, so the bundle
        goes on using the value baked into inference.json, which ships as

            {'image': '/data/Task09_Spleen/imagesTr/spleen_10.nii.gz',
             'label_prompt': [3]}

        In this container that path does not exist, so it fails. On any machine
        where it DID exist it would have segmented that file and returned the
        mask keyed to our SeriesInstanceUID -- a wrong result with no error.

        Overrides must therefore be passed to create_workflow, which means a new
        workflow per volume and a re-read of the 872 MB checkpoint each time.
        That is a real cost (a few seconds of GPU time per series) accepted in
        exchange for the input actually being the input.

        `device` is deliberately NOT overridden. The bundle already resolves it
        as torch.device('cuda:0' if torch.cuda.is_available() else 'cpu');
        passing the string "cuda" replaces a torch.device with a str and relies
        on downstream coercion.
        """
        from monai.bundle import create_workflow

        bundle_root = self._get_bundle_root()
        return create_workflow(
            workflow_type="infer",
            config_file=os.path.join(bundle_root, "configs", "inference.json"),
            meta_file=os.path.join(bundle_root, "configs", "metadata.json"),
            logging_file=os.path.join(bundle_root, "configs", "logging.conf"),
            bundle_root=bundle_root,
            input_dict={"image": nifti_path, "label_prompt": label_prompt},
            output_dir=out_dir,
        )

    # -- volume assembly ---------------------------------------------------
    def _assemble_volume(self, session, source_stage, file_paths, work_dir):
        """Download a DICOM series and write it as a single NIfTI volume.

        Returns (nifti_path, spacing_tuple, shape_tuple).
        """
        import nibabel as nib
        import numpy as np
        from monai.data import PydicomReader

        dcm_dir = os.path.join(work_dir, "dcm")
        os.makedirs(dcm_dir, exist_ok=True)

        # Flatten to unique local names. Stage-relative paths contain '/', and
        # two series can share a basename, so the path is flattened rather than
        # basenamed -- collisions here would silently drop slices from the
        # volume and shorten it, which no downstream check would catch.
        n_written = 0
        for rel in file_paths:
            local = os.path.join(dcm_dir, rel.replace("/", "__"))
            stream = session.file.get_stream(f"@{source_stage}/{rel}")
            with open(local, "wb") as out:
                out.write(stream.read())
            stream.close()
            n_written += 1
        if n_written != len(file_paths):
            raise RuntimeError(
                f"downloaded {n_written} of {len(file_paths)} slices")

        # affine_lps_to_ras=True converts DICOM LPS to NIfTI RAS. Rescale
        # slope/intercept is applied by get_data, so the array is Hounsfield
        # Units, which is what ScaleIntensityRanged assumes.
        #
        # swap_ij=False IS LOAD-BEARING, and it is not the default.
        #
        # With the default swap_ij=True the reader returns (cols, rows, slices).
        # With False it returns (rows, cols, slices), so recovering the DICOM
        # frame order for DICOM-SEG is a plain transpose(2, 0, 1). Since this
        # corpus is 512x512, a row/col swap would produce a correctly-SHAPED but
        # transposed segmentation that no shape assertion could catch -- an
        # overlay that is silently wrong in a clinical artifact.
        #
        # Verified on a deliberately non-square synthetic series (32 rows x 24
        # cols x 5 slices): swap_ij=False then transpose(2, 0, 1) reproduces every
        # source slice's Hounsfield array exactly.
        reader = PydicomReader(affine_lps_to_ras=True, swap_ij=False)
        arr, meta = reader.get_data(reader.read(dcm_dir))
        affine = meta.get("affine")
        if affine is None:
            raise RuntimeError("PydicomReader returned no affine")
        affine = np.asarray(affine, dtype=np.float64)

        nifti_path = os.path.join(work_dir, "volume.nii.gz")
        nib.save(nib.Nifti1Image(np.asarray(arr), affine), nifti_path)

        # The source files in the SAME order PydicomReader used, so a DICOM-SEG
        # built later lines up frame-for-frame. PydicomReader sorts by
        # InstanceNumber, so that sort is reproduced here rather than assumed
        # from filename order.
        import pydicom

        ordered = sorted(
            (os.path.join(dcm_dir, f) for f in os.listdir(dcm_dir)),
            key=lambda fp: int(
                getattr(pydicom.dcmread(fp, stop_before_pixels=True),
                        "InstanceNumber", 0) or 0),
        )

        # Spacing read back from the affine rather than trusted from tags: this
        # is the number the model actually sees.
        spacing = tuple(float(np.linalg.norm(affine[:3, i])) for i in range(3))
        return (nifti_path, spacing,
                tuple(int(v) for v in np.asarray(arr).shape), ordered)

    def _write_dicom_seg(self, mask_path, source_paths, expected_shape,
                         series_uid, work_dir):
        """Convert the NIfTI mask into a DICOM-SEG object.

        WHY THIS EXISTS
        ---------------
        Vista3D's mask is a NIfTI. OHIF overlays DICOM-SEG. Without this step the
        segmentation is a number in SEG_RESULT and a file on a stage -- real, but
        invisible to the person it is for. DICOM-SEG is what makes it a clinical
        artifact rather than a metric.

        Returns (seg_path, n_segments) or (None, 0) if there is nothing to write.
        """
        import highdicom as hd
        import nibabel as nib
        import numpy as np
        import pydicom
        from pydicom.sr.codedict import codes as dcm_codes

        mask_img = nib.load(mask_path)
        mask = np.asarray(mask_img.dataobj)

        # The mask must be in the grid the volume was assembled in. Invertd in the
        # bundle's postprocessing is what returns it there; if that ever stops
        # happening, fail loudly rather than emit a misaligned overlay.
        if tuple(int(v) for v in mask.shape) != tuple(int(v) for v in expected_shape):
            raise RuntimeError(
                f"mask shape {mask.shape} != volume shape {expected_shape}; "
                "the bundle's Invertd did not restore the original grid")

        # (rows, cols, slices) -> (frames, rows, cols). See the swap_ij note in
        # _assemble_volume: this transpose is only correct because the reader was
        # told not to swap i/j.
        mask = mask.transpose(2, 0, 1)

        source_images = [pydicom.dcmread(fp) for fp in source_paths]

        # highdicom copies the Patient and Study modules from the source image and
        # reads these attributes unconditionally, so a source that omits any of
        # them raises AttributeError from deep inside the constructor:
        #
        #   AttributeError: 'FileDataset' object has no attribute 'PatientBirthDate'
        #
        # They are all DICOM Type 2 -- required to be PRESENT but allowed to be
        # EMPTY -- so filling a missing one with '' is standards-correct rather
        # than fabrication. De-identified public archives routinely drop them, and
        # this corpus is a de-identified public archive.
        for src in source_images:
            for attr in ("PatientID", "PatientName", "PatientBirthDate",
                         "PatientSex", "StudyID", "AccessionNumber",
                         "StudyDate", "StudyTime", "SeriesNumber",
                         "InstanceNumber"):
                if not hasattr(src, attr):
                    setattr(src, attr, "")

        # FrameOfReferenceUID is required by the Segmentation IOD:
        #   ValueError: Source images have no Frame Of Reference UID, but it is
        #   required by the IOD.
        #
        # Unlike the Type 2 attributes above, this one carries meaning: it asserts
        # that these slices share one spatial coordinate system. Synthesizing a
        # single shared UID is therefore only sound because enqueue already gated
        # this series on V_SERIES_GEOMETRY.IS_SEGMENTABLE_VOLUME -- one instance
        # per z-position, consistent orientation. Without that gate this would be
        # asserting a spatial relationship that may not hold.
        missing_for = [s for s in source_images if not getattr(s, "FrameOfReferenceUID", None)]
        if missing_for:
            import pydicom.uid

            shared = pydicom.uid.generate_uid()
            for src in missing_for:
                src.FrameOfReferenceUID = shared

        if len(source_images) != mask.shape[0]:
            raise RuntimeError(
                f"{len(source_images)} source slices but {mask.shape[0]} mask frames")

        present = [int(v) for v in np.unique(mask) if int(v) not in (0, 255)]
        if not present:
            return None, 0

        # One segment per label, renumbered 1..N. DICOM segment numbers must be
        # consecutive from 1, so Vista3D's sparse label ids cannot be used directly.
        descriptions = []
        remapped = np.zeros_like(mask, dtype=np.uint8)
        for new_num, lab in enumerate(sorted(present), start=1):
            name, sct = SEG_LABELS.get(lab, (f"label {lab}", None))
            prop_type = (
                hd.sr.CodedConcept(value=sct, scheme_designator="SCT", meaning=name)
                if sct else dcm_codes.SCT.Tissue
            )
            descriptions.append(
                hd.seg.SegmentDescription(
                    segment_number=new_num,
                    segment_label=name,
                    segmented_property_category=dcm_codes.SCT.AnatomicalStructure,
                    segmented_property_type=prop_type,
                    algorithm_type=hd.seg.SegmentAlgorithmTypeValues.AUTOMATIC,
                    algorithm_identification=hd.AlgorithmIdentificationSequence(
                        name="MONAI Vista3D",
                        version="1.0",
                        family=dcm_codes.cid7162.ArtificialIntelligence,
                    ),
                )
            )
            remapped[mask == lab] = new_num

        seg = hd.seg.Segmentation(
            source_images=source_images,
            pixel_array=remapped,
            # BINARY, not FRACTIONAL: Vista3D emits a hard label per voxel, and
            # declaring FRACTIONAL would imply a confidence this does not carry.
            segmentation_type=hd.seg.SegmentationTypeValues.BINARY,
            segment_descriptions=descriptions,
            series_instance_uid=hd.UID(),
            series_number=9001,
            sop_instance_uid=hd.UID(),
            instance_number=1,
            manufacturer="Voxel",
            manufacturer_model_name="MONAI Vista3D",
            software_versions="vista3d-bundle",
            device_serial_number="voxel-seg",
            content_description=f"Vista3D automatic segmentation ({len(descriptions)} structures)",
            # omit_empty_frames=False keeps a 1:1 frame correspondence with the
            # source series. Dropping empty frames is legal and smaller, but it
            # makes the per-frame mapping non-trivial for anything that assumes
            # frame k belongs to slice k.
            omit_empty_frames=False,
        )

        seg_path = os.path.join(work_dir, "segmentation.dcm")
        seg.save_as(seg_path, enforce_file_format=True)
        return seg_path, len(descriptions)

    # -- inference ---------------------------------------------------------
    @custom_model.inference_api
    def predict(self, input_df: pd.DataFrame) -> pd.DataFrame:
        import numpy as np
        import torch

        session = self._get_session()
        device = "cuda" if torch.cuda.is_available() else "cpu"
        results = []

        for _, row in input_df.iterrows():
            series_uid = row["SERIES_INSTANCE_UID"]
            work_dir = tempfile.mkdtemp(prefix="vx_")
            rec = {
                "SERIES_INSTANCE_UID": series_uid,
                "STATUS": "unknown",
                "MASK_PATH": None,
                "N_LABELS_FOUND": 0,
                "LABEL_VOLUMES_JSON": None,
                "VOXEL_SPACING": None,
                "VOLUME_SHAPE": None,
                "ASSEMBLE_MS": 0,
                "INFER_MS": 0,
                "DEVICE": device,
                "SEG_PATH": None,
                "N_SEGMENTS": 0,
            }
            try:
                file_paths = json.loads(row["FILE_PATHS"])
                if not file_paths:
                    raise ValueError("FILE_PATHS is empty")
                prompt = row.get("LABEL_PROMPT")
                label_prompt = json.loads(prompt) if prompt else list(DEFAULT_LABEL_PROMPT)

                t0 = time.time()
                nifti_path, spacing, shape, ordered_src = self._assemble_volume(
                    session, row["SOURCE_STAGE"], file_paths, work_dir
                )
                rec["ASSEMBLE_MS"] = int((time.time() - t0) * 1000)
                rec["VOXEL_SPACING"] = json.dumps([round(s, 4) for s in spacing])
                rec["VOLUME_SHAPE"] = json.dumps(list(shape))

                out_dir = os.path.join(work_dir, "eval")
                os.makedirs(out_dir, exist_ok=True)

                t1 = time.time()
                # Only the inputs move. Every transform, the sliding-window
                # inferer, and the Invertd that returns the mask to the original
                # voxel grid stay exactly as the bundle ships them.
                workflow = self._build_workflow(nifti_path, label_prompt, out_dir)
                workflow.run()
                rec["INFER_MS"] = int((time.time() - t1) * 1000)

                mask_local = None
                for root, _dirs, files in os.walk(out_dir):
                    for fn in files:
                        if fn.endswith((".nii", ".nii.gz")):
                            mask_local = os.path.join(root, fn)
                            break
                    if mask_local:
                        break
                if mask_local is None:
                    raise RuntimeError(f"bundle produced no mask under {out_dir}")

                import nibabel as nib

                mask_img = nib.load(mask_local)
                arr = np.asarray(mask_img.dataobj)
                zooms = mask_img.header.get_zooms()[:3]
                voxel_ml = float(np.prod(zooms)) / 1000.0
                vols = {}
                for lab in np.unique(arr):
                    lab_i = int(lab)
                    # 0 is background; 255 is the bundle's nan sentinel
                    # (Lambdad nan_to_num(nan=255)), not an anatomical class.
                    if lab_i in (0, 255):
                        continue
                    vols[str(lab_i)] = round(float((arr == lab).sum()) * voxel_ml, 2)

                rec["N_LABELS_FOUND"] = len(vols)
                rec["LABEL_VOLUMES_JSON"] = json.dumps(vols)

                # Key the output on the SeriesInstanceUID. Never on pixel
                # content: blank slices at volume boundaries are byte-identical
                # across series, so a content-keyed join silently fans out and
                # attaches masks to the wrong study.
                mask_stage_path = f"{series_uid}/mask.nii.gz"
                staged = os.path.join(work_dir, "mask.nii.gz")
                shutil.copyfile(mask_local, staged)
                session.file.put(
                    staged,
                    f"@{row['OUTPUT_STAGE']}/{series_uid}/",
                    overwrite=True,
                    auto_compress=False,  # the file is already .gz
                )
                rec["MASK_PATH"] = mask_stage_path

                # ---- DICOM-SEG -------------------------------------------
                # Deliberately NOT fatal. A failure here means the overlay is
                # missing, not that the segmentation is wrong: the mask and the
                # organ volumes are already computed and uploaded. Losing those
                # to a DICOM-SEG encoding problem would be the worse outcome, so
                # the reason is recorded in STATUS and the row still succeeds.
                try:
                    seg_local, n_seg = self._write_dicom_seg(
                        mask_local, ordered_src, shape, series_uid, work_dir
                    )
                    if seg_local:
                        session.file.put(
                            seg_local,
                            f"@{row['OUTPUT_STAGE']}/{series_uid}/",
                            overwrite=True,
                            auto_compress=False,  # a .dcm must not be gzipped
                        )
                        rec["SEG_PATH"] = f"{series_uid}/segmentation.dcm"
                        rec["N_SEGMENTS"] = n_seg
                        rec["STATUS"] = "SUCCESS"
                    else:
                        rec["STATUS"] = "SUCCESS_NO_SEGMENTS"
                except Exception as exc:  # noqa: BLE001
                    rec["STATUS"] = f"SUCCESS_SEG_FAILED: {type(exc).__name__}: {exc}"[:900]

            except Exception as exc:  # noqa: BLE001 - one bad series must not kill the batch
                rec["STATUS"] = f"FAILED: {type(exc).__name__}: {exc}"[:900]
            finally:
                shutil.rmtree(work_dir, ignore_errors=True)

            results.append(rec)

        return pd.DataFrame(results)
