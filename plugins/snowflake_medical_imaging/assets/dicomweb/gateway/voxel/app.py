"""DICOMweb gateway -- FastAPI application.

Runs on localhost:8080 inside the SPCS service. NOT exposed as an SPCS endpoint;
the nginx router is the only container on the public port. See
dicomweb_spec_yaml.j2 for why exactly one public endpoint exists.
"""
from __future__ import annotations

import logging
import os
import sys
import threading
import time
from queue import Queue

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from .auth import SessionCache, owner_session
from .dicomweb import router as dicomweb_router
from .dicomweb import _manifests
from . import frames, queries

logging.basicConfig(
    stream=sys.stdout,
    level=os.environ.get("VOXEL_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("voxel.app")

app = FastAPI(title="DICOMweb Gateway", docs_url=None, redoc_url=None)
app.include_router(dicomweb_router, prefix="/dicom-web")


# ---------------------------------------------------------------------------
# Prefetch hinting
#
# The gateway does not prefetch inline -- that would put stage-volume I/O on the
# request path for a benefit the current request never sees. It drops a hint on a
# bounded queue that the prefetch sidecar drains.
#
# Bounded and non-blocking on purpose: if the prefetcher falls behind, hints are
# DROPPED rather than backing up into the request path. A slow prefetcher must
# make things cold, never slow.
# ---------------------------------------------------------------------------
_hints: "Queue[tuple[str, list[str]]]" = Queue(maxsize=256)
_hinted: set[str] = set()
_hint_lock = threading.Lock()


def prefetch_hint(series_uid: str, paths: list[str]) -> None:
    """Tell the prefetch sidecar that a series was just opened.

    THE HINT MUST CROSS A CONTAINER BOUNDARY. prefetch.py is a separate
    container in the same service, so an in-process queue.Queue cannot reach
    it -- putting hints only on the queue means the sidecar starves and every
    read is served cold from the stage volume. That was the original bug here,
    and it was invisible from the outside: nothing errors, the viewer works,
    and X-Voxel-Served-From just reads "stage" forever.

    The shared channel is the memory volume both containers mount. The sidecar
    tails $HOTRING/_hints, so appending one path per line is the whole
    protocol. O_APPEND with a single write() keeps concurrent appends from
    interleaving mid-line, which matters because the reader splits on newlines.

    Never raises. A prefetcher is an optimization and a frame request must not
    fail because a hint could not be recorded.
    """
    with _hint_lock:
        if series_uid in _hinted:
            return
        _hinted.add(series_uid)
        if len(_hinted) > 4096:
            _hinted.clear()

    # Reported by /readyz as prefetch_hints.depth. The queue is NOT the transport
    # -- the file below is -- but a nonzero depth that never drains is the signal
    # that hints are being produced while the sidecar is not consuming them, which
    # is the failure that shipped once and produced no error anywhere.
    try:
        _hints.put_nowait((series_uid, paths))
    except Exception:  # noqa: BLE001 - queue full is expected and fine
        pass

    payload = "".join(f"{p}\n" for p in paths).encode()
    if not payload:
        return
    try:
        os.makedirs(frames.HOTRING, exist_ok=True)
        fd = os.open(os.path.join(frames.HOTRING, "_hints"),
                     os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        try:
            os.write(fd, payload)
        finally:
            os.close(fd)
    except OSError as exc:
        log.warning("could not record prefetch hint (continuing): %s", exc)


app.state.sessions = SessionCache(
    ttl_seconds=int(os.environ.get("VOXEL_SESSION_TTL", "60"))
)
app.state.prefetch_hint = prefetch_hint
app.state.started_at = time.time()


# ---------------------------------------------------------------------------
# Health
#
# /healthz and /readyz are deliberately DIFFERENT, and the difference is
# load-bearing.
#
# /healthz  process liveness. Wired to the SPCS readinessProbe.
# /readyz   per-dependency detail for a human.
#
# /readyz RETURNS 200 EVEN WHEN A DEPENDENCY IS DEGRADED. If it returned non-200,
# SPCS would cycle the container on a transient stage-volume or warehouse hiccup
# and turn a degradation into an outage. Degradation is reported in the BODY; only
# a genuinely unservable process is unhealthy.
# ---------------------------------------------------------------------------
@app.get("/healthz", response_class=PlainTextResponse)
def healthz() -> str:
    return "ok"


@app.get("/readyz")
def readyz():
    checks: dict[str, dict] = {}

    # Stage volume. The frame path is a POSIX read against this mount, so if it is
    # missing nothing can be served -- but the catalog is still queryable, so this
    # is degraded rather than dead.
    mount_ok = os.path.isdir(frames.MOUNT)
    n_entries = None
    if mount_ok:
        try:
            with os.scandir(frames.MOUNT) as it:
                n_entries = sum(1 for _ in zip(it, range(64)))
        except OSError as exc:
            mount_ok = False
            n_entries = f"unreadable: {exc}"
    checks["stage_volume"] = {
        "state": "OK" if mount_ok else "DEGRADED",
        "mount": frames.MOUNT,
        "sample_entries": n_entries,
        "note": "frames cannot be served without this; catalog queries still work",
    }

    # Shared memory volume for warmed frames. Absent is fine -- reads fall back to
    # the stage volume, which is slower and correct.
    hot_ok = os.path.isdir(frames.HOTRING)
    checks["hotring"] = {
        "state": "OK" if hot_ok else "DEGRADED",
        "path": frames.HOTRING,
        "note": "optional; absence makes series cold, not broken",
    }

    # Catalog reachability, via the ONE owner-rights session in the codebase.
    # Deliberately not a caller session: /readyz must work before any user arrives.
    try:
        conn = owner_session()
        try:
            who = queries.whoami(conn)
            checks["catalog"] = {
                "state": "OK",
                "as_user": who.get("USR"),
                "database": who.get("DB"),
                "note": "owner-rights probe. Request paths use caller's rights.",
            }
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 - report, never raise
        checks["catalog"] = {"state": "DEGRADED", "error": str(exc)[:300]}

    checks["manifest_cache"] = {"state": "OK", **_manifests.stats()}

    # Hint-queue depth. Not the transport -- $HOTRING/_hints is -- but it is the
    # cheapest available signal that the producer side is alive. Pair it with
    # X-Voxel-Served-From on a frame response: hints being produced while every
    # response still reads "stage" is the sidecar-starvation failure.
    checks["prefetch_hints"] = {
        "state": "OK",
        "depth": _hints.qsize(),
        "series_hinted": len(_hinted),
        "hint_file": os.path.join(frames.HOTRING, "_hints"),
    }
    checks["uptime_seconds"] = round(time.time() - app.state.started_at, 1)

    worst = "OK"
    for v in checks.values():
        if isinstance(v, dict) and v.get("state") == "DEGRADED":
            worst = "DEGRADED"

    # Always 200. See the header note.
    return JSONResponse(status_code=200, content={"state": worst, "checks": checks})


@app.get("/whoami")
def whoami_route(request: Request):
    """Identity assertion probe.

    This is what the deploy verification calls with two differently-entitled
    users. If it reports the SERVICE OWNER rather than the calling user, the
    nginx router is not forwarding Sf-Context-* and every row access policy is
    silently passing.

    Kept as a first-class route rather than a debug flag precisely because that
    failure is invisible any other way.
    """
    from .dicomweb import _conn

    conn, caller = _conn(request)
    who = queries.whoami(conn)
    session_user = str(who.get("USR") or "")
    return {
        "header_user": caller.username,
        "session_user": session_user,
        "session_role": who.get("ROLE"),
        "identity_ok": bool(session_user)
        and session_user.upper() == caller.username.upper(),
        "note": "identity_ok must be true. False means caller identity is not "
                "reaching the gateway and queries are running as the service owner.",
    }


@app.on_event("shutdown")
def _shutdown() -> None:
    """Release both pools of OS-level handles on the way out.

    Sessions were always purged; open file descriptors were not, so
    FileHandles.close_all() existed and was never called. On a clean shutdown the
    kernel reclaims them anyway, which is why nothing ever surfaced -- but an
    unreleased descriptor against a stage volume is a mount the platform cannot
    tear down promptly, and "the OS will handle it" is not a reason to leave a
    written cleanup path unwired.
    """
    app.state.sessions.purge()
    frames.close_handles()
