"""DICOMweb routes: QIDO-RS, WADO-RS, STOW-RS.

=============================================================================
THE 90-SECOND RULE SHAPES THIS ENTIRE FILE
=============================================================================

SPCS ingress terminates any connection idle for 90 seconds. A WADO-RS retrieval
of a full series is a long-lived multipart/related response, so the naive
implementation -- assemble all parts, then return -- looks IDLE to the proxy while
it assembles. A large series gets cut off mid-stream, and the client sees a
truncated multipart body rather than a clean DICOM-level error.

So every multipart response here is a generator that writes and FLUSHES each part
the moment its read completes. Bytes are continuously on the wire and the idle
timer never accumulates. nginx cooperates with proxy_buffering off.

The incumbent arrives at the same design from a different direction: their
ProgressiveStreamer yields per frame as each byte-range read completes, which
incidentally keeps the socket busy. Here it is the primary requirement.

=============================================================================
CONTENT TYPES ARE NOT DECORATION
=============================================================================

Each multipart part carries transfer-syntax on its own Content-Type. A viewer
uses that to pick a decoder. Omitting it, or defaulting it, makes the viewer guess
-- and a wrong guess on a compressed frame renders noise rather than failing, so
there is no error to notice.
"""
from __future__ import annotations

import json
import logging
import uuid
from typing import Iterator

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from . import frames, queries
from .auth import Caller, IdentityError

log = logging.getLogger("voxel.dicomweb")
router = APIRouter()

# Transfer syntax UID -> the media type a viewer should decode the part as.
# Native syntaxes have no encapsulated media type; their frames are raw octets.
TRANSFER_SYNTAX_MIME = {
    "1.2.840.10008.1.2": "application/octet-stream",
    "1.2.840.10008.1.2.1": "application/octet-stream",
    "1.2.840.10008.1.2.2": "application/octet-stream",
    "1.2.840.10008.1.2.5": "image/dicom-rle",
    "1.2.840.10008.1.2.4.50": "image/jpeg",
    "1.2.840.10008.1.2.4.51": "image/jpeg",
    "1.2.840.10008.1.2.4.57": "image/jpeg",
    "1.2.840.10008.1.2.4.70": "image/jpeg",
    "1.2.840.10008.1.2.4.80": "image/jls",
    "1.2.840.10008.1.2.4.81": "image/jls",
    "1.2.840.10008.1.2.4.90": "image/jp2",
    "1.2.840.10008.1.2.4.91": "image/jp2",
    "1.2.840.10008.1.2.4.201": "image/jphc",
    "1.2.840.10008.1.2.4.202": "image/jphc",
    "1.2.840.10008.1.2.4.203": "image/jphc",
}

_manifests = frames.ManifestCache()


# ---------------------------------------------------------------------------
# Request plumbing
# ---------------------------------------------------------------------------
def _caller(request: Request) -> Caller:
    """Resolve the end user, or 401.

    IdentityError becomes 401, never 500. A 500 invites a retry and reads as a
    server fault; the truth is that the request carried no attributable identity
    and we refuse to guess.
    """
    try:
        return Caller.from_headers(request.headers)
    except IdentityError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc


def _conn(request: Request):
    caller = _caller(request)
    return request.app.state.sessions.get(caller), caller


def _flat_params(request: Request) -> dict[str, str]:
    return {k: v for k, v in request.query_params.items()}


# ---------------------------------------------------------------------------
# QIDO-RS
#
# UIDs live in the PATH, not in a POST body. That is a deliberate audit decision,
# not REST aesthetics: SNOWFLAKE.ACCOUNT_USAGE.INGRESS_NETWORK_ACCESS_HISTORY
# records USER_NAME, REQUEST_PATH, and BYTES_TX for every request to the service
# endpoint. With the identifiers in the path, that view IS a
# 365-day-retained, platform-maintained disclosure log with zero application
# code -- and BYTES_TX distinguishes a metadata query from an actual pixel
# retrieval. Putting UIDs in a body would throw that away.
# ---------------------------------------------------------------------------
@router.get("/studies")
def qido_studies(request: Request):
    conn, _ = _conn(request)
    try:
        rows = queries.search_studies(conn, _flat_params(request))
    except queries.BadQuery as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(
        content=[_study_json(r) for r in rows],
        media_type="application/dicom+json",
    )


@router.get("/studies/{study_uid}/series")
def qido_series(study_uid: str, request: Request):
    conn, _ = _conn(request)
    try:
        rows = queries.search_series(conn, study_uid, _flat_params(request))
    except queries.BadQuery as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(
        content=[_series_json(r) for r in rows],
        media_type="application/dicom+json",
    )


@router.get("/series")
def qido_all_series(request: Request):
    """Series across all studies.

    Not in the DICOMweb standard, but the incumbent exposes /all_series and OHIF's study
    list benefits from it. Kept for parity so a migrating deployment does not lose
    a working screen.
    """
    conn, _ = _conn(request)
    try:
        rows = queries.search_series(conn, None, _flat_params(request))
    except queries.BadQuery as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(
        content=[_series_json(r) for r in rows],
        media_type="application/dicom+json",
    )


@router.get("/studies/{study_uid}/series/{series_uid}/instances")
def qido_instances(study_uid: str, series_uid: str, request: Request):
    conn, _ = _conn(request)
    try:
        rows = queries.search_instances(conn, study_uid, series_uid, _flat_params(request))
    except queries.BadQuery as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(
        content=[_instance_json(r) for r in rows],
        media_type="application/dicom+json",
    )


# ---------------------------------------------------------------------------
# WADO-RS metadata
# ---------------------------------------------------------------------------
def _metadata_response(rows, what: str):
    """Shared shaping for all three WADO-RS /metadata levels.

    RAW_METADATA is already the DICOM JSON model, so there is no translation. The
    one addition is the transfer syntax in file-meta group 0002, which viewers
    need and which is not part of the dataset proper.
    """
    if not rows:
        raise HTTPException(status_code=404,
                            detail=f"{what} not found or not visible")
    out = []
    for r in rows:
        meta = r["RAW_METADATA"]
        if isinstance(meta, str):
            meta = json.loads(meta)
        if r.get("TRANSFER_SYNTAX_UID"):
            meta["00020010"] = {"vr": "UI", "Value": [r["TRANSFER_SYNTAX_UID"]]}
        out.append(meta)
    return JSONResponse(content=out, media_type="application/dicom+json")


# PS3.18 10.4 defines /metadata at study, series and instance level. Only series
# was implemented; the study route 404'd until a client asked for it.
@router.get("/studies/{study_uid}/metadata")
def wado_study_metadata(study_uid: str, request: Request):
    conn, _ = _conn(request)
    return _metadata_response(queries.study_metadata(conn, study_uid), "study")


@router.get("/studies/{study_uid}/series/{series_uid}/metadata")
def wado_series_metadata(study_uid: str, series_uid: str, request: Request):
    conn, _ = _conn(request)
    return _metadata_response(queries.series_metadata(conn, series_uid), "series")


@router.get("/studies/{study_uid}/series/{series_uid}/instances/{sop_uid}/metadata")
def wado_instance_metadata(study_uid: str, series_uid: str, sop_uid: str,
                           request: Request):
    conn, _ = _conn(request)
    return _metadata_response(
        queries.instance_metadata(conn, series_uid, sop_uid), "instance")


# ---------------------------------------------------------------------------
# WADO-RS frames -- the hot path
# ---------------------------------------------------------------------------
def _load_manifest(conn, series_uid: str) -> frames.SeriesManifest:
    """Build or fetch the resolved offset manifest for a series.

    Called on every frame request, but the cache means the SQL runs once per
    series rather than once per frame. That is the design: the access unit is a
    series, so the cache granularity is a series.
    """
    cached = _manifests.get(series_uid)
    if cached is not None:
        return cached

    plan = queries.series_frame_plan(conn, series_uid)
    if not plan:
        raise HTTPException(status_code=404, detail="series not found or not visible")

    manifest = frames.SeriesManifest(series_uid)

    # Only encapsulated instances need a second query. A wholly-native corpus
    # never touches DICOM_FRAME_OFFSET at all.
    stored_uids = [
        r["SOP_INSTANCE_UID"] for r in plan if (r.get("RESOLUTION") or "") == "STORED"
    ]
    stored = queries.stored_offsets(conn, stored_uids) if stored_uids else {}

    for r in plan:
        sop = r["SOP_INSTANCE_UID"]
        resolution = r.get("RESOLUTION") or ""

        # Refuse rather than serve bytes we are not confident about. A stale
        # offset returns WRONG PIXELS at HTTP 200, which is the worst outcome
        # available -- worse than an error, because nobody notices.
        if not r.get("IS_SERVABLE") or r.get("IS_STALE"):
            log.warning(
                "instance %s excluded: servable=%s stale=%s resolution=%s",
                sop, r.get("IS_SERVABLE"), r.get("IS_STALE"), resolution,
            )
            continue

        if resolution == "ARITHMETIC":
            manifest.add_arithmetic(
                sop_uid=sop,
                path=r["FILE_NAME"],
                syntax=r["TRANSFER_SYNTAX_UID"],
                pixel_data_start=int(r["PIXEL_DATA_START"]),
                rows=int(r["IMAGE_ROWS"]),
                cols=int(r["IMAGE_COLUMNS"]),
                bits_allocated=int(r["BITS_ALLOCATED"]),
                samples=int(r["SAMPLES_PER_PIXEL"]),
                num_frames=int(r["NUMBER_OF_FRAMES"]),
            )
        elif resolution == "STORED":
            manifest.add_stored(
                sop_uid=sop,
                path=r["FILE_NAME"],
                syntax=r["TRANSFER_SYNTAX_UID"],
                rows=stored.get(sop, []),
            )

    _manifests.put(manifest)
    return manifest


def _parse_frame_list(frame_list: str) -> list[int]:
    """DICOMweb sends frames as a comma-separated 1-based list."""
    out: list[int] = []
    for tok in frame_list.split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            n = int(tok)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"bad frame number '{tok}'") from None
        if n < 1:
            raise HTTPException(status_code=400, detail="frame numbers are 1-based")
        out.append(n)
    if not out:
        raise HTTPException(status_code=400, detail="empty frame list")
    if len(out) > 512:
        # A scroll window, not a whole-series dump. An unbounded frame list is how
        # a single request tries to outlive the 90-second ingress timeout.
        raise HTTPException(status_code=400, detail="at most 512 frames per request")
    return out


def _multipart(parts: list[tuple[bytes, str, str]], boundary: str) -> Iterator[bytes]:
    """Yield a multipart/related body, one part at a time.

    Generator rather than a concatenated buffer so each part reaches the wire as
    soon as it exists. See the file header on the 90-second timeout.
    """
    for payload, mime, location in parts:
        head = (
            f"\r\n--{boundary}\r\n"
            f"Content-Type: {mime}\r\n"
            f"Content-Length: {len(payload)}\r\n"
        )
        if location:
            head += f"Content-Location: {location}\r\n"
        head += "\r\n"
        yield head.encode("ascii")
        yield payload
    yield f"\r\n--{boundary}--\r\n".encode("ascii")


@router.get("/studies/{study_uid}/series/{series_uid}/instances/{sop_uid}/frames/{frame_list}")
def wado_frames(
    study_uid: str,
    series_uid: str,
    sop_uid: str,
    frame_list: str,
    request: Request,
):
    conn, caller = _conn(request)
    manifest = _load_manifest(conn, series_uid)

    wanted = _parse_frame_list(frame_list)
    path = manifest.path(sop_uid)
    syntax = manifest.syntax(sop_uid) or ""
    if path is None:
        # Either the instance does not exist, the caller cannot see it, or it was
        # excluded as unservable/stale. All three are 404 from the client's point
        # of view; the distinction is in the log, not the response, because
        # telling a caller "this exists but you may not see it" is itself a leak.
        raise HTTPException(status_code=404, detail="instance not available")

    refs: list[frames.FrameRef] = []
    for n in wanted:
        ref = manifest.frame(sop_uid, n)
        if ref is None:
            raise HTTPException(status_code=404, detail=f"frame {n} not available")
        refs.append(ref)

    payloads = frames.read_frames_warm(path, refs)
    mime = TRANSFER_SYNTAX_MIME.get(syntax, "application/octet-stream")
    part_mime = f"{mime}; transfer-syntax={syntax}" if syntax else mime

    boundary = uuid.uuid4().hex
    parts = [
        (
            payloads[i],
            part_mime,
            f"/dicom-web/studies/{study_uid}/series/{series_uid}"
            f"/instances/{sop_uid}/frames/{wanted[i]}",
        )
        for i in range(len(wanted))
    ]

    # Fire-and-forget prefetch of the rest of the series. Series-open is a
    # near-perfect predictor that every other frame will be wanted within
    # seconds -- a far stronger signal than any access-frequency heuristic
    # computed over history, and it needs no scoring function.
    request.app.state.prefetch_hint(series_uid, [path])

    return StreamingResponse(
        _multipart(parts, boundary),
        media_type=f'multipart/related; type="{mime}"; boundary={boundary}',
        headers={
            # Which tier served this. Makes the cold/warm ratio observable as a
            # metric shift rather than something a user has to report.
            "X-Voxel-Served-From": "warm" if _is_warm(path) else "stage",
            "X-Voxel-Frames": str(len(wanted)),
            "Cache-Control": "private, max-age=86400, immutable",
        },
    )


def _is_warm(path: str) -> bool:
    import os

    return os.path.exists(frames.hotring_path(path))


@router.get("/studies/{study_uid}/series/{series_uid}/instances/{sop_uid}")
def wado_instance(study_uid: str, series_uid: str, sop_uid: str, request: Request):
    """Whole-instance retrieval as application/dicom.

    Reads the file straight off the stage volume. No transcode, no re-encode --
    the bytes a client gets are the bytes that were ingested, which matters for
    anything submission-adjacent and also means this route costs no CPU.
    """
    import os

    conn, _ = _conn(request)
    loc = queries.instance_locator(conn, sop_uid)
    if not loc:
        raise HTTPException(status_code=404, detail="instance not available")

    full = os.path.join(frames.MOUNT, str(loc["FILE_NAME"]).lstrip("/"))
    try:
        fd = os.open(full, os.O_RDONLY)
    except OSError:
        raise HTTPException(
            status_code=503,
            detail="instance is catalogued but its bytes are not on the mounted "
                   "stage. It is probably outside the pread subset.",
        ) from None

    syntax = loc.get("TRANSFER_SYNTAX_UID") or ""
    boundary = uuid.uuid4().hex

    def _stream() -> Iterator[bytes]:
        # Chunked so a large instance never buffers whole in memory and the socket
        # stays busy against the ingress timeout.
        try:
            yield (
                f"\r\n--{boundary}\r\n"
                f"Content-Type: application/dicom; transfer-syntax={syntax}\r\n\r\n"
            ).encode("ascii")
            while True:
                chunk = os.read(fd, 1 << 20)
                if not chunk:
                    break
                yield chunk
            yield f"\r\n--{boundary}--\r\n".encode("ascii")
        finally:
            try:
                os.close(fd)
            except OSError:
                pass

    return StreamingResponse(
        _stream(),
        media_type=f'multipart/related; type="application/dicom"; boundary={boundary}',
    )


# ---------------------------------------------------------------------------
# STOW-RS
# ---------------------------------------------------------------------------
@router.post("/studies")
@router.post("/studies/{study_uid}")
async def stow(request: Request, study_uid: str | None = None):
    """Store instances.

    NOT IMPLEMENTED, and returning 501 rather than a plausible-looking success is
    deliberate. A STOW-RS route that accepts a POST and quietly discards it is
    worse than no route: a modality or a bridge will report a successful send and
    the study will not exist.

    When implemented it writes through this service to the internal stage -- no
    credential vending to the client, no direct-to-cloud PUT. The incumbent vends
    temporary cloud path credentials and caches them ~55 minutes so the client
    PUTs straight to S3/ADLS/GCS, which puts a write-capable credential for the
    PHI bucket in application memory for an hour and moves the write outside the
    governed perimeter, where the platform's own audit views cannot see it.
    """
    raise HTTPException(
        status_code=501,
        detail="STOW-RS is not implemented in this build. Writes must go through "
               "the service to the internal stage; no client-side credential "
               "vending. See assets/dicomweb/gateway/voxel/dicomweb.py.",
    )


# ---------------------------------------------------------------------------
# DICOM JSON shaping
#
# One helper per level. Values are always arrays, per the DICOM JSON model, and a
# missing value is an ABSENT key rather than a null -- viewers treat a present-but-
# null value differently from an absent one.
# ---------------------------------------------------------------------------
def _tag(value, vr: str) -> dict | None:
    if value is None or value == "":
        return None
    if isinstance(value, (list, tuple)):
        vals = [v for v in value if v is not None]
    else:
        vals = [value]
    if not vals:
        return None
    if vr in {"US", "IS", "UL", "SL", "SS"}:
        out = []
        for v in vals:
            try:
                out.append(int(v))
            except (TypeError, ValueError):
                return None
        return {"vr": vr, "Value": out}
    if vr in {"DS", "FL", "FD"}:
        out = []
        for v in vals:
            try:
                out.append(float(v))
            except (TypeError, ValueError):
                return None
        return {"vr": vr, "Value": out}
    return {"vr": vr, "Value": [str(v) for v in vals]}


def _assemble(pairs: list[tuple[str, object, str]]) -> dict:
    out = {}
    for key, value, vr in pairs:
        t = _tag(value, vr)
        if t is not None:
            out[key] = t
    return out


def _study_json(r: dict) -> dict:
    return _assemble([
        ("0020000D", r.get("STUDY_INSTANCE_UID"), "UI"),
        ("00080020", r.get("STUDY_DATE"), "DA"),
        ("00080030", r.get("STUDY_TIME"), "TM"),
        ("00081030", r.get("STUDY_DESCRIPTION"), "LO"),
        ("00100020", r.get("PATIENT_ID"), "LO"),
        ("00100010", r.get("PATIENT_NAME"), "PN"),
        ("00080050", r.get("ACCESSION_NUMBER"), "SH"),
        ("00080061", r.get("MODALITIES_IN_STUDY"), "CS"),
        ("00201206", r.get("NUM_SERIES"), "IS"),
        ("00201208", r.get("NUM_INSTANCES"), "IS"),
    ])


def _series_json(r: dict) -> dict:
    return _assemble([
        ("0020000E", r.get("SERIES_INSTANCE_UID"), "UI"),
        ("0020000D", r.get("STUDY_INSTANCE_UID"), "UI"),
        ("00080060", r.get("MODALITY"), "CS"),
        ("00200011", r.get("SERIES_NUMBER"), "IS"),
        ("0008103E", r.get("SERIES_DESCRIPTION"), "LO"),
        ("00180015", r.get("BODY_PART_EXAMINED"), "CS"),
        ("00201209", r.get("NUM_INSTANCES"), "IS"),
    ])


def _instance_json(r: dict) -> dict:
    return _assemble([
        ("00080018", r.get("SOP_INSTANCE_UID"), "UI"),
        ("00080016", r.get("SOP_CLASS_UID"), "UI"),
        ("0020000E", r.get("SERIES_INSTANCE_UID"), "UI"),
        ("0020000D", r.get("STUDY_INSTANCE_UID"), "UI"),
        ("00200013", r.get("INSTANCE_NUMBER"), "IS"),
        ("00280010", r.get("IMAGE_ROWS"), "US"),
        ("00280011", r.get("IMAGE_COLUMNS"), "US"),
        ("00280100", r.get("BITS_ALLOCATED"), "US"),
        ("00280101", r.get("BITS_STORED"), "US"),
        ("00280002", r.get("SAMPLES_PER_PIXEL"), "US"),
        ("00280008", r.get("NUMBER_OF_FRAMES"), "IS"),
        ("00280004", r.get("PHOTOMETRIC_INTERPRETATION"), "CS"),
        ("00280103", r.get("PIXEL_REPRESENTATION"), "US"),
        ("00281050", r.get("WINDOW_CENTER"), "DS"),
        ("00281051", r.get("WINDOW_WIDTH"), "DS"),
        ("00281053", r.get("RESCALE_SLOPE"), "DS"),
        ("00281052", r.get("RESCALE_INTERCEPT"), "DS"),
        ("00280030", _split(r.get("PIXEL_SPACING")), "DS"),
        ("00180050", r.get("SLICE_THICKNESS"), "DS"),
        ("00201041", r.get("SLICE_LOCATION"), "DS"),
        ("00200032", _split(r.get("IMAGE_POSITION_PATIENT")), "DS"),
        ("00200037", _split(r.get("IMAGE_ORIENTATION_PATIENT")), "DS"),
        ("00020010", r.get("TRANSFER_SYNTAX_UID"), "UI"),
    ])


def _split(value):
    """Multi-valued DICOM strings are stored backslash-delimited."""
    if not value:
        return None
    return str(value).split("\\")
