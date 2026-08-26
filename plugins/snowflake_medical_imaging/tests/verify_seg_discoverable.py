"""Prove a DICOMweb client can discover and retrieve the segmentation.

This is the assertion that separates "we wrote a valid DICOM file" from "the
viewer can overlay it". OHIF finds a segmentation by listing the SERIES in a
study and looking for Modality = SEG, then retrieving its frames. If QIDO does
not return it, the file may as well not exist.
"""
import json
import os
import re
import sys

import requests

PAT = os.environ["VOXEL_PAT"]
BASE = os.environ["VOXEL_URL"].rstrip("/")

s = requests.Session()
s.headers["Authorization"] = f'Snowflake Token="{PAT}"'
s.headers["X-Snowflake-Authorization-Token-Type"] = "PROGRAMMATIC_ACCESS_TOKEN"
DJ = "application/dicom+json"

fails = []


def check(name, ok, detail=""):
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}  {detail}")
    if not ok:
        fails.append(name)


study = os.environ["SEG_STUDY_UID"]
print(f"study {study}\n")

# 1. QIDO must return the SEG series alongside the CT series.
r = s.get(f"{BASE}/dicom-web/studies/{study}/series?limit=50",
          headers={"Accept": DJ}, timeout=120)
check("QIDO returns series for the study", r.status_code == 200, f"HTTP {r.status_code}")
series = r.json() if r.status_code == 200 else []
mods = {}
for row in series:
    m = row.get("00080060", {}).get("Value", [None])[0]
    uid = row.get("0020000E", {}).get("Value", [None])[0]
    mods.setdefault(m, []).append(uid)
print(f"       modalities present: { {k: len(v) for k, v in mods.items()} }")
check("SEG series is discoverable via QIDO", "SEG" in mods,
      f"found {sorted(k for k in mods if k)}")

if "SEG" not in mods:
    print("\ncannot continue without a discoverable SEG series")
    sys.exit(1)

seg_series = mods["SEG"][0]

# 2. Series metadata must declare it as a Segmentation with the right geometry.
r = s.get(f"{BASE}/dicom-web/studies/{study}/series/{seg_series}/metadata",
          headers={"Accept": DJ}, timeout=180)
check("SEG series metadata retrievable", r.status_code == 200, f"HTTP {r.status_code}")
if r.status_code == 200:
    md = r.json()[0]
    sop_class = md.get("00080016", {}).get("Value", [None])[0]
    frames = md.get("00280008", {}).get("Value", [None])[0]
    bits = md.get("00280100", {}).get("Value", [None])[0]
    rows_ = md.get("00280010", {}).get("Value", [None])[0]
    sop_uid = md.get("00080018", {}).get("Value", [None])[0]
    check("SOPClassUID is Segmentation Storage",
          sop_class == "1.2.840.10008.5.1.4.1.1.66.4", str(sop_class))
    check("BitsAllocated is 1 (BINARY segmentation)", int(bits) == 1, str(bits))
    check("NumberOfFrames is a JSON number", isinstance(frames, (int, float)),
          f"{frames!r} ({type(frames).__name__})")
    print(f"       {rows_}x{md.get('00280011',{}).get('Value',[None])[0]}, "
          f"{frames} frames, {bits} bit")

    # 3. THE SEQUENCES THAT DEFINE THE SEGMENTATION MUST BE NON-EMPTY.
    #
    #    This assertion exists because its absence let a broken build pass. The
    #    earlier version of this file checked SOPClassUID, BitsAllocated and
    #    NumberOfFrames -- all scalars, all correct -- and reported 10/10 PASS
    #    while the overlay was impossible to render. The metadata carried
    #    `SegmentSequence: {"vr": "SQ", "Value": []}`: present, well-formed,
    #    empty. A viewer parses that happily and draws nothing.
    #
    #    A segmentation IS its sequences. SegmentSequence holds the labels and
    #    SNOMED codes; PerFrameFunctionalGroupsSequence maps every frame to a
    #    segment and a spatial position. Without them there is no overlay, so
    #    testing the scalars was testing the packaging and not the contents.
    for tag, name, minimum in (
        ("00620002", "SegmentSequence", 1),
        ("52009230", "PerFrameFunctionalGroupsSequence", int(frames)),
        ("52009229", "SharedFunctionalGroupsSequence", 1),
        ("00081115", "ReferencedSeriesSequence", 1),
    ):
        items = md.get(tag, {}).get("Value") or []
        check(f"{name} is non-empty", len(items) >= minimum,
              f"{len(items)} item(s), need >= {minimum}")

    labels = [
        it.get("00620005", {}).get("Value", [None])[0]
        for it in (md.get("00620002", {}).get("Value") or [])
    ]
    check("segment labels survive sequence expansion",
          all(labels) and len(labels) >= 1, str(labels))

    # 4. Retrieve a frame. This is where the // 8 bug would surface: a
    #    zero-length part served with HTTP 200.
    r2 = s.get(f"{BASE}/dicom-web/studies/{study}/series/{seg_series}"
               f"/instances/{sop_uid}/frames/1",
               headers={"Accept": "multipart/related"}, timeout=180)
    check("SEG frame 1 retrievable", r2.status_code == 200, f"HTTP {r2.status_code}")
    if r2.status_code == 200:
        m = re.search(rb"Content-Length:\s*(\d+)", r2.content[:4096])
        n = int(m.group(1)) if m else -1
        expect = int(rows_) * int(md.get("00280011", {}).get("Value", [0])[0]) // 8
        check("SEG frame is one bit per pixel, not zero-length",
              n == expect, f"{n} bytes, expected {expect}")

        # 5. A later frame must be offset correctly, not aliased to frame 1.
        r3 = s.get(f"{BASE}/dicom-web/studies/{study}/series/{seg_series}"
                   f"/instances/{sop_uid}/frames/1000",
                   headers={"Accept": "multipart/related"}, timeout=180)
        check("a deep frame (1000) is retrievable", r3.status_code == 200,
              f"HTTP {r3.status_code}")
        if r3.status_code == 200:
            check("deep frame differs from frame 1 (offsets advance)",
                  r3.content != r2.content,
                  "identical bytes would mean every frame aliases frame 1")

print(f"\n{'ALL PASS' if not fails else 'FAILURES: ' + ', '.join(fails)}")
sys.exit(1 if fails else 0)
