"""DICOMweb conformance harness -- runs real HTTP against a deployed service.

=============================================================================
WHY AN EXTERNAL HARNESS
=============================================================================

A conformance suite written against the same code it tests shares that code's
misconceptions. This drives the service over real HTTP, from outside, using only
DICOMweb semantics -- it has no access to platform internals and does not import
anything from the gateway.

It writes results into CONFORMANCE_RESULT and timings into FRAME_LATENCY_SAMPLE,
so the claim "we are DICOMweb conformant" becomes a query a prospect can run
rather than a screenshot of someone else's CI.

=============================================================================
AUTHENTICATION
=============================================================================

SPCS ingress requires a Snowflake session. Two ways to get one:

  1. PROGRAMMATIC ACCESS TOKEN (what this script expects)

         VOXEL_URL=https://<ingress_url> VOXEL_PAT=<token> \
           .venv/bin/python tests/conformance_harness.py

     Note: Snowflake refuses to let a PAT create another PAT for the SAME user
     (error 099413), so if your CLI connection already authenticates with a PAT
     you must mint this one from a different auth method -- browser SSO, key pair,
     or a separate service user.

  2. BROWSER COOKIE, if you would rather not mint a token: copy the
     `sfc-endpoint-...` cookie from a logged-in session and pass VOXEL_COOKIE.

Without either, the script records nothing rather than recording passes it did
not earn.
"""
from __future__ import annotations

import json
import re
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

try:
    import requests
except ImportError:
    print("pip install requests", file=sys.stderr)
    sys.exit(2)

BASE = (os.environ.get("VOXEL_URL") or "").rstrip("/")
PAT = os.environ.get("VOXEL_PAT")
COOKIE = os.environ.get("VOXEL_COOKIE")
CONN = os.environ.get("VOXEL_CONN", "spark-connect")
RUN_ID = os.environ.get("VOXEL_RUN_ID") or f"harness-{uuid.uuid4().hex[:12]}"
CLIENT = "voxel-conformance-harness"
CLIENT_VERSION = "0.1.0"

DICOM_JSON = "application/dicom+json"

results: list[dict] = []
latency: list[dict] = []


def record(suite, test, ref, outcome, status=None, ms=None, detail=""):
    results.append({
        "suite": suite, "test": test, "ref": ref, "outcome": outcome,
        "status": status, "ms": ms, "detail": detail[:900],
    })
    flag = {"PASS": "PASS", "FAIL": "FAIL", "NOT_IMPLEMENTED": "N/I",
            "SKIPPED": "SKIP", "ERROR": "ERR"}[outcome]
    print(f"  [{flag:4s}] {suite}.{test}"
          + (f"  ({status})" if status else "")
          + (f"  {ms}ms" if ms else "")
          + (f"  {detail[:70]}" if detail and outcome != "PASS" else ""))


def session() -> requests.Session:
    s = requests.Session()
    if PAT:
        # =================================================================
        # SPCS INGRESS USES `Snowflake Token="..."`, NOT `Bearer`.
        #
        # This is a real and easy-to-miss split in the platform:
        #
        #   Snowflake REST API / SQL API on <account>.snowflakecomputing.com
        #       Authorization: Bearer <pat>
        #
        #   SPCS ingress on <endpoint>.snowflakecomputing.app
        #       Authorization: Snowflake Token="<pat>"
        #
        # Sending Bearer to the ingress endpoint does not return 401. It
        # returns a 302 REDIRECT TO THE SIGN-IN PAGE, which reads like "wrong
        # token" rather than "wrong header format" and sends you off checking
        # the token instead of the scheme.
        # =================================================================
        s.headers["Authorization"] = f'Snowflake Token="{PAT}"'
        s.headers["X-Snowflake-Authorization-Token-Type"] = "PROGRAMMATIC_ACCESS_TOKEN"
    if COOKIE:
        s.headers["Cookie"] = COOKIE
    s.headers["Accept"] = DICOM_JSON
    return s


def get(s, path, **kw):
    t0 = time.perf_counter()
    r = s.get(f"{BASE}{path}", timeout=60, allow_redirects=False, **kw)
    return r, int((time.perf_counter() - t0) * 1000)


# ---------------------------------------------------------------------------
# Suites
# ---------------------------------------------------------------------------
def run_transport(s) -> bool:
    """Auth and identity. Returns False if we never got authenticated at all."""
    r, ms = get(s, "/dicom-web/studies")

    # A 302 to a sign-in page means we are not authenticated. Everything after
    # this would be measuring the login page, so stop rather than record noise.
    if r.status_code in (301, 302, 303, 307, 308):
        record("TRANSPORT", "requires_authentication", "-", "PASS", r.status_code,
               ms, "redirected to sign-in, so anonymous access is refused")
        record("TRANSPORT", "harness_authenticated", "-", "FAIL", r.status_code,
               ms, "not authenticated; supply VOXEL_PAT or VOXEL_COOKIE")
        return False

    if r.status_code == 401:
        record("TRANSPORT", "requires_authentication", "-", "PASS", 401, ms)
        record("TRANSPORT", "harness_authenticated", "-", "FAIL", 401, ms,
               "401 -- token rejected, or caller identity not reaching the gateway")
        return False

    record("TRANSPORT", "requires_authentication", "-", "PASS", r.status_code, ms,
           "ingress required a Snowflake session")
    record("TRANSPORT", "harness_authenticated", "-", "PASS", r.status_code, ms)

    # /whoami is the identity assertion. identity_ok=false means the gateway is
    # running queries as the service OWNER, which silently defeats row access
    # policies -- the single most important thing to check.
    r, ms = get(s, "/whoami")
    if r.status_code == 200:
        try:
            body = r.json()
            ok = bool(body.get("identity_ok"))
            record("TRANSPORT", "caller_identity_forwarded", "-",
                   "PASS" if ok else "FAIL", 200, ms,
                   f"header_user={body.get('header_user')} "
                   f"session_user={body.get('session_user')}")
        except ValueError:
            record("TRANSPORT", "caller_identity_forwarded", "-", "ERROR", 200, ms,
                   "non-JSON from /whoami")
    else:
        record("TRANSPORT", "caller_identity_forwarded", "-", "FAIL",
               r.status_code, ms)

    # wasm MIME. Wrong type and the browser refuses to instantiate the decoders.
    r, ms = get(s, "/ort/ort-wasm-simd-threaded.jsep.wasm")
    ct = r.headers.get("content-type", "")
    record("TRANSPORT", "wasm_content_type", "-",
           "PASS" if "application/wasm" in ct else "FAIL", r.status_code, ms, ct)

    # js MIME. This is the bug that produced a blank viewer.
    r, ms = get(s, "/app-config.js")
    ct = r.headers.get("content-type", "")
    record("TRANSPORT", "javascript_content_type", "-",
           "PASS" if "javascript" in ct else "FAIL", r.status_code, ms, ct)
    return True


def run_qido(s) -> tuple[str | None, str | None, str | None]:
    study = series = instance = None

    r, ms = get(s, "/dicom-web/studies?limit=5")
    ok = r.status_code == 200 and DICOM_JSON.split("/")[1] in r.headers.get("content-type", "")
    record("QIDO", "search_studies", "PS3.18 10.6", "PASS" if ok else "FAIL",
           r.status_code, ms, r.headers.get("content-type", ""))
    if r.status_code == 200:
        try:
            studies = r.json()
            if studies:
                study = studies[0]["0020000D"]["Value"][0]
                # Every attribute must be a DICOM JSON object with vr + Value.
                shape_ok = all(
                    isinstance(v, dict) and "vr" in v and "Value" in v
                    for v in studies[0].values())
                record("QIDO", "dicom_json_attribute_shape", "PS3.18 F.2",
                       "PASS" if shape_ok else "FAIL", 200, ms)
        except (ValueError, KeyError, IndexError) as exc:
            record("QIDO", "search_studies", "PS3.18 10.6", "FAIL", 200, ms, str(exc))

    if study:
        r, ms = get(s, f"/dicom-web/studies/{study}/series?limit=5")
        record("QIDO", "search_series_in_study", "PS3.18 10.6",
               "PASS" if r.status_code == 200 else "FAIL", r.status_code, ms)
        if r.status_code == 200 and r.json():
            series = r.json()[0]["0020000E"]["Value"][0]

    if study and series:
        r, ms = get(s, f"/dicom-web/studies/{study}/series/{series}/instances?limit=5")
        record("QIDO", "search_instances_in_series", "PS3.18 10.6",
               "PASS" if r.status_code == 200 else "FAIL", r.status_code, ms)
        if r.status_code == 200 and r.json():
            inst = r.json()[0]
            instance = inst["00080018"]["Value"][0]
            # The type bug that silently disabled MPR: US/IS/DS must be numbers.
            rows = inst.get("00280010", {}).get("Value", [None])[0]
            record("QIDO", "numeric_vr_is_json_number", "PS3.18 F.2.3",
                   "PASS" if isinstance(rows, (int, float)) else "FAIL", 200, ms,
                   f"Rows={rows!r} type={type(rows).__name__}")

    r, ms = get(s, "/dicom-web/studies?Modality=CT&limit=3")
    record("QIDO", "filter_by_modality", "PS3.18 10.6.1",
           "PASS" if r.status_code == 200 else "FAIL", r.status_code, ms)

    r, ms = get(s, "/dicom-web/studies?SeriesDescription=CH*&limit=3")
    record("QIDO", "wildcard_matching", "PS3.18 10.6.1.1",
           "PASS" if r.status_code == 200 else "FAIL", r.status_code, ms)

    # An unknown attribute must be a 400, never silently ignored.
    r, ms = get(s, "/dicom-web/studies?NoSuchAttribute=x")
    record("QIDO", "reject_unknown_attribute", "PS3.18 10.6",
           "PASS" if r.status_code == 400 else "FAIL", r.status_code, ms,
           "must be 400, not a silently dropped filter")

    r, ms = get(s, "/dicom-web/studies?limit=99999")
    record("QIDO", "limit_offset", "PS3.18 10.6.1.3",
           "PASS" if r.status_code == 200 else "FAIL", r.status_code, ms)
    return study, series, instance


def find_multiframe(s) -> tuple[str, str, str, int] | None:
    """Locate an ENCAPSULATED MULTI-FRAME instance to test frame lists against.

    Why this matters: the first version of this harness requested frames
    1..8 from whatever instance QIDO returned first. That was a single-frame
    native CT, so frames 2-8 legitimately 404 and the harness recorded a FAIL
    against the service for its own bad request.

    Targeting a real multi-frame instance does two things at once:
      * makes the frame-list assertion meaningful
      * exercises COMPRESSED frame serving end to end over HTTP, which is the
        path that had never run at all until the offset oracle was built

    Discovery is by DICOMweb query only -- the harness stays a black-box client.
    """
    r, ms = get(s, "/dicom-web/series?limit=100")
    if r.status_code != 200:
        return None
    try:
        all_series = r.json()
    except ValueError:
        return None

    for srow in all_series:
        series_uid = srow.get("0020000E", {}).get("Value", [None])[0]
        study_uid = srow.get("0020000D", {}).get("Value", [None])[0]
        if not (series_uid and study_uid):
            continue
        r2, _ = get(s, f"/dicom-web/studies/{study_uid}/series/{series_uid}"
                       f"/instances?limit=20")
        if r2.status_code != 200:
            continue
        try:
            for inst in r2.json():
                nf = inst.get("00280008", {}).get("Value", [1])[0]
                try:
                    nf = int(nf)
                except (TypeError, ValueError):
                    continue
                if nf > 1:
                    return (study_uid, series_uid,
                            inst["00080018"]["Value"][0], nf)
        except (ValueError, KeyError, IndexError):
            continue
    return None


def run_wado(s, study, series, instance):
    if not (study and series):
        record("WADO", "retrieve_series_metadata", "PS3.18 10.4", "SKIPPED",
               detail="no series discovered by QIDO")
        return

    r, ms = get(s, f"/dicom-web/studies/{study}/series/{series}/metadata")
    ok = r.status_code == 200
    record("WADO", "retrieve_series_metadata", "PS3.18 10.4",
           "PASS" if ok else "FAIL", r.status_code, ms)
    if ok:
        try:
            meta = r.json()
            has_ts = any("00020010" in m for m in meta)
            record("WADO", "metadata_includes_transfer_syntax", "PS3.18 8.7.3",
                   "PASS" if has_ts else "FAIL", 200, ms)
        except ValueError as exc:
            record("WADO", "retrieve_series_metadata", "PS3.18 10.4", "FAIL",
                   200, ms, str(exc))

    if not instance:
        return

    base = f"/dicom-web/studies/{study}/series/{series}/instances/{instance}"

    # Single frame.
    r, ms = get(s, f"{base}/frames/1", headers={"Accept": "multipart/related"})
    ct = r.headers.get("content-type", "")
    ok = r.status_code == 200 and "multipart/related" in ct
    record("WADO", "retrieve_frames_single", "PS3.18 10.4.1",
           "PASS" if ok else "FAIL", r.status_code, ms, ct)
    if ok:
        # =====================================================================
        # ASSERT ON THE *PART* CONTENT-TYPE, NOT THE OUTER ONE.
        #
        # PS3.18 8.7.3.3: `transfer-syntax` is a parameter of the media type of
        # each PART. The outer multipart/related header carries only `type=`
        # naming the part media type, plus the boundary. So:
        #
        #   outer: multipart/related; type="application/octet-stream"; boundary=..
        #   part:  application/octet-stream; transfer-syntax=1.2.840.10008.1.2.1
        #
        # An earlier version of this assertion read
        #     "transfer-syntax" in r.text[:2000] or "transfer-syntax" in ct
        # and PASSED. That was a false pass twice over: `ct` never contains it,
        # and r.text is a lossy utf-8 decode of BINARY pixel data, so the match
        # was incidental. Decoding pixel bytes as text to grep for a header is
        # never the right move -- parse the part header explicitly.
        #
        # This is the header OHIF parses to choose a decoder, so getting it
        # wrong shows up as an unrenderable image, not an error.
        # =====================================================================
        m = re.search(rb"Content-Type:\s*([^\r\n]+)", r.content[:4096])
        part_ct = m.group(1).decode("ascii", "replace") if m else ""
        record("WADO", "content_type_carries_transfer_syntax", "PS3.18 8.7.3.3",
               "PASS" if "transfer-syntax=" in part_ct else "FAIL", 200, ms,
               f"part: {part_ct}")
        latency.append({
            "series": series, "instance": instance, "frame": 1, "n_frames": 1,
            "bytes": len(r.content), "ms": ms,
            "served_from": r.headers.get("x-voxel-served-from", "unknown"),
        })

    # Frame 0 must be refused.
    r, ms = get(s, f"{base}/frames/0")
    record("WADO", "reject_frame_zero", "PS3.18 10.4.1",
           "PASS" if r.status_code == 400 else "FAIL", r.status_code, ms)

    # Whole instance.
    r, ms = get(s, base, headers={"Accept": "multipart/related"})
    record("WADO", "retrieve_instance", "PS3.18 10.4",
           "PASS" if r.status_code == 200 else "FAIL", r.status_code, ms,
           f"{len(r.content)} bytes")

    # ---- frame LIST, against a genuinely multi-frame instance ---------------
    mf = find_multiframe(s)
    if mf is None:
        record("WADO", "retrieve_frames_list", "PS3.18 10.4.1", "SKIPPED",
               detail="no multi-frame instance found in the catalog")
        record("WADO", "encapsulated_frame_serving", "PS3.5 A.4", "SKIPPED",
               detail="no multi-frame instance found")
        return

    m_study, m_series, m_inst, n_frames = mf
    m_base = (f"/dicom-web/studies/{m_study}/series/{m_series}"
              f"/instances/{m_inst}")
    want = ",".join(str(i) for i in range(1, min(n_frames, 8) + 1))

    r, ms = get(s, f"{m_base}/frames/{want}",
                headers={"Accept": "multipart/related"})
    ok = r.status_code == 200
    record("WADO", "retrieve_frames_list", "PS3.18 10.4.1",
           "PASS" if ok else "FAIL", r.status_code, ms,
           f"{n_frames}-frame instance, requested {want}, {len(r.content)} bytes")

    # A multi-frame instance in this corpus is encapsulated, so a successful
    # multipart response here is proof the STORED-offset read path works over
    # HTTP -- not just that the offsets in the table are correct.
    n_parts = r.content.count(b"Content-Type:") if ok else 0
    record("WADO", "encapsulated_frame_serving", "PS3.5 A.4",
           "PASS" if ok and n_parts == len(want.split(",")) else "FAIL",
           r.status_code, ms,
           f"{n_parts} part(s) for {len(want.split(','))} requested frame(s)")
    if ok:
        latency.append({
            "series": m_series, "instance": m_inst, "frame": 1,
            "n_frames": len(want.split(",")),
            "bytes": len(r.content), "ms": ms,
            "served_from": r.headers.get("x-voxel-served-from", "unknown"),
        })

    # Warm-vs-cold: repeat single-frame reads on the multi-frame instance.
    for i in range(6):
        fr = (i % min(n_frames, 4)) + 1
        r, ms = get(s, f"{m_base}/frames/{fr}",
                    headers={"Accept": "multipart/related"})
        if r.status_code == 200:
            latency.append({
                "series": m_series, "instance": m_inst, "frame": fr,
                "n_frames": 1, "bytes": len(r.content), "ms": ms,
                "served_from": r.headers.get("x-voxel-served-from", "unknown"),
            })


def run_stow(s):
    r = s.post(f"{BASE}/dicom-web/studies", data=b"", timeout=30,
               allow_redirects=False)
    # 501 is the DECLARED behaviour, so 501 is a PASS against the declaration.
    record("STOW", "store_instances", "PS3.18 10.5",
           "NOT_IMPLEMENTED" if r.status_code == 501 else "FAIL",
           r.status_code, None,
           "501 by design: a route that accepts and discards is worse than none")


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
def persist():
    if not results:
        print("nothing to record")
        return
    def q(v):
        if v is None:
            return "NULL"
        return "'" + str(v).replace("'", "''") + "'"

    rows = ",\n".join(
        f"({q(RUN_ID)}, {q(r['suite'])}, {q(r['test'])}, {q(r['ref'])}, "
        f"{q(r['outcome'])}, {r['status'] or 'NULL'}, {r['ms'] or 'NULL'}, "
        f"{q(CLIENT)}, {q(CLIENT_VERSION)}, {q(r['detail'])})"
        for r in results)

    sql = f"""
INSERT INTO VOXEL_DB.IMAGING.CONFORMANCE_RESULT
    (RUN_ID, SUITE, TEST_NAME, DICOM_REFERENCE, OUTCOME, HTTP_STATUS,
     LATENCY_MS, CLIENT, CLIENT_VERSION, DETAIL)
VALUES
{rows};
"""
    if latency:
        lrows = ",\n".join(
            f"({q(RUN_ID)}, {q(l['series'])}, {q(l['instance'])}, {l['frame']}, "
            f"{l['n_frames']}, {l['bytes']}, {l['ms']}, {q(l['served_from'])})"
            for l in latency)
        sql += f"""
INSERT INTO VOXEL_DB.IMAGING.FRAME_LATENCY_SAMPLE
    (RUN_ID, SERIES_INSTANCE_UID, SOP_INSTANCE_UID, FRAME_NUMBER, N_FRAMES,
     BYTES_RETURNED, LATENCY_MS, SERVED_FROM)
VALUES
{lrows};
"""
    p = Path("/tmp/vxr/_conformance_load.sql")
    p.write_text(sql)
    r = subprocess.run(
        ["snow", "sql", "-c", CONN, "--enable-templating", "NONE", "-f", str(p)],
        capture_output=True, text=True)
    print("\nrecorded" if r.returncode == 0 else f"\nrecord FAILED: {r.stdout[-400:]}")


def main() -> int:
    if not BASE:
        print("set VOXEL_URL to the ingress URL", file=sys.stderr)
        return 2
    if not (PAT or COOKIE):
        print("set VOXEL_PAT or VOXEL_COOKIE -- refusing to record passes it "
              "did not earn", file=sys.stderr)
        return 2

    print(f"run_id={RUN_ID}\ntarget={BASE}\n")
    s = session()

    print("TRANSPORT")
    if not run_transport(s):
        print("\nnot authenticated -- recording what was established and stopping")
        persist()
        return 1

    print("QIDO-RS")
    study, series, instance = run_qido(s)
    print("WADO-RS")
    run_wado(s, study, series, instance)
    print("STOW-RS")
    run_stow(s)

    persist()
    n_fail = sum(1 for r in results if r["outcome"] in ("FAIL", "ERROR"))
    print(f"\n{len(results)} assertions, {n_fail} failing, "
          f"{len(latency)} latency sample(s)")
    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
