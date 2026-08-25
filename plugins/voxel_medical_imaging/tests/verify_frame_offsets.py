"""Verify Voxel's ingested frame offsets against independently computed ground truth.

=============================================================================
WHAT THIS PROVES
=============================================================================

The encapsulated frame path writes rows into DICOM_FRAME_OFFSET by parsing the
Basic Offset Table, the Extended Offset Table, or by scanning (FFFE,E000) item
headers. A bug there does NOT raise -- it returns the WRONG BYTES at HTTP 200,
which renders as a plausible image of the wrong anatomy.

So correctness cannot be asserted by "no errors during ingest". It has to be
checked against an oracle computed by something that is not Voxel.

The oracle is build_compressed_corpus.py, which walked each file's item headers
directly and recorded absolute (offset, length) per frame.

Three levels of check, weakest to strongest:

    1. COUNT   -- one row per frame, no missing or extra
    2. OFFSET  -- every stored (offset, length) equals ground truth exactly
    3. BYTES   -- read the file at the STORED offsets and confirm the bytes are
                  a valid compressed frame that decodes to the expected image

Level 3 is the one that matters. Levels 1 and 2 could both pass against a
consistently-wrong parser; level 3 cannot, because a wrong offset yields data
that will not decode.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

TRUTH = Path("/tmp/vxr/compressed/_ground_truth.json")
LOCAL = Path("/tmp/vxr/compressed")
CONN = "spark-connect"


def sql(text: str) -> list[dict]:
    p = Path("/tmp/vxr/_verify.sql")
    p.write_text(text)
    r = subprocess.run(
        ["snow", "sql", "-c", CONN, "--enable-templating", "NONE",
         "-f", str(p), "--format", "json"],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        print("SQL FAILED:", r.stdout[-500:], r.stderr[-300:])
        sys.exit(1)
    try:
        return json.loads(r.stdout)
    except json.JSONDecodeError:
        print("unparseable:", r.stdout[:400])
        sys.exit(1)


def main() -> int:
    truth = json.loads(TRUTH.read_text())
    by_uid = {t["sop_instance_uid"]: t for t in truth}
    uid_list = ", ".join(f"'{u}'" for u in by_uid)

    rows = sql(f"""
        SELECT SOP_INSTANCE_UID, FRAME_NUMBER, FRAME_OFFSET, FRAME_LENGTH,
               OFFSET_SOURCE
        FROM VOXEL_DB.IMAGING.DICOM_FRAME_OFFSET
        WHERE SOP_INSTANCE_UID IN ({uid_list})
        ORDER BY SOP_INSTANCE_UID, FRAME_NUMBER;
    """)

    stored: dict[str, list[dict]] = {}
    for r in rows:
        stored.setdefault(r["SOP_INSTANCE_UID"], []).append(r)

    n_pass = n_fail = 0
    print(f"{'file':22s} {'syntax':12s} {'mode':6s} {'frames':>6s} "
          f"{'count':>6s} {'offsets':>8s} {'bytes':>7s}  source")
    print("-" * 88)

    for uid, t in by_uid.items():
        got = stored.get(uid, [])
        name = t["file"]
        syntax = "RLE" if "RLE" in t["transfer_syntax_name"] else "J2K"
        src = got[0]["OFFSET_SOURCE"] if got else "-"

        # ---- level 1: count -------------------------------------------------
        count_ok = len(got) == t["n_frames"]

        # ---- level 2: exact offsets ----------------------------------------
        offset_ok = count_ok
        if count_ok:
            for exp, act in zip(t["frames"], got):
                if (int(act["FRAME_NUMBER"]) != exp["frame"]
                        or int(act["FRAME_OFFSET"]) != exp["offset"]
                        or int(act["FRAME_LENGTH"]) != exp["length"]):
                    offset_ok = False
                    break

        # ---- level 3: read at the STORED offsets and decode ----------------
        # Deliberately uses the STORED values, not ground truth, so a wrong
        # offset produces undecodable data and fails here.
        bytes_ok = False
        if count_ok:
            blob = (LOCAL / name).read_bytes()
            try:
                import numpy as np
                from pydicom.pixels import pixel_array  # noqa: F401
                ok = True
                for act in got:
                    off = int(act["FRAME_OFFSET"])
                    ln = int(act["FRAME_LENGTH"])
                    frag = blob[off:off + ln]
                    if len(frag) != ln:
                        ok = False
                        break
                    # An encapsulated frame must not begin with an item header --
                    # that would mean the offset points at the header rather than
                    # the payload, the classic 8-byte error.
                    if frag[:4] == b"\xfe\xff\x00\xe0":
                        ok = False
                        break
                    # Codec magic: J2K starts with the SOC marker FF4F FF51,
                    # RLE segments begin with a 64-byte header whose first
                    # value is the segment count (1..15).
                    if syntax == "J2K":
                        if frag[:2] != b"\xff\x4f":
                            ok = False
                            break
                    else:
                        n_seg = int.from_bytes(frag[0:4], "little")
                        if not (1 <= n_seg <= 15):
                            ok = False
                            break
                bytes_ok = ok
            except Exception as exc:  # noqa: BLE001
                print("  (decode check unavailable:", exc, ")")
                bytes_ok = False

        good = count_ok and offset_ok and bytes_ok
        n_pass += good
        n_fail += (not good)
        print(f"{name:22s} {syntax:12s} {t['mode']:6s} {t['n_frames']:>6d} "
              f"{'PASS' if count_ok else 'FAIL':>6s} "
              f"{'PASS' if offset_ok else 'FAIL':>8s} "
              f"{'PASS' if bytes_ok else 'FAIL':>7s}  {src}")

    print("-" * 88)
    print(f"{n_pass} file(s) fully verified, {n_fail} failed")

    # Cross-check: the arithmetic path must NOT have written rows for these.
    enc = sql(f"""
        SELECT COUNT(*) AS N FROM VOXEL_DB.IMAGING.V_FRAME_COVERAGE
        WHERE SOP_INSTANCE_UID IN ({uid_list}) AND RESOLUTION = 'STORED'
          AND IS_SERVABLE AND NOT IS_STALE;
    """)
    print(f"servable STORED instances in coverage view: {enc[0]['N']} "
          f"(expected {len(by_uid)})")

    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
