"""All three WADO-RS /metadata levels must exist (PS3.18 10.4).

Study-level returned 404 until now. OHIF prefers the per-series path when study
lazy-load is enabled, so nothing we drove ever asked for it and the gap survived
23 conformance assertions.
"""
import os
import sys
import time

import requests

PAT = os.environ["VOXEL_PAT"]
BASE = os.environ["VOXEL_URL"].rstrip("/")
STUDY = os.environ["SEG_STUDY_UID"]
s = requests.Session()
s.headers["Authorization"] = f'Snowflake Token="{PAT}"'
s.headers["X-Snowflake-Authorization-Token-Type"] = "PROGRAMMATIC_ACCESS_TOKEN"
DJ = "application/dicom+json"

fails = []


def check(name, ok, detail=""):
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}  {detail}")
    if not ok:
        fails.append(name)


r = s.get(f"{BASE}/dicom-web/studies/{STUDY}/series?limit=50",
          headers={"Accept": DJ}, timeout=120)
series = {x["00080060"]["Value"][0]: x["0020000E"]["Value"][0] for x in r.json()}

t = time.perf_counter()
r = s.get(f"{BASE}/dicom-web/studies/{STUDY}/metadata",
          headers={"Accept": DJ}, timeout=900)
el = time.perf_counter() - t
check("study-level /metadata is implemented", r.status_code == 200,
      f"HTTP {r.status_code} in {el:.2f}s, {len(r.content):,} bytes")
if r.status_code == 200:
    md = r.json()
    check("study metadata covers every instance in the study",
          len(md) == 460, f"{len(md)} instance(s), expected 460")
    mods = {x.get("00080060", {}).get("Value", [None])[0] for x in md}
    check("study metadata spans both series (CT and SEG)",
          {"CT", "SEG"} <= mods, str(sorted(m for m in mods if m)))
    # grouped by series, so a client concatenating gets a stable document
    order = [x["0020000E"]["Value"][0] for x in md]
    grouped = all(
        order.index(u) + order.count(u) - 1 == max(
            i for i, v in enumerate(order) if v == u)
        for u in set(order))
    check("instances are grouped by series, not interleaved", grouped)

sop = None
r2 = s.get(f"{BASE}/dicom-web/studies/{STUDY}/series/{series['SEG']}/metadata",
           headers={"Accept": DJ}, timeout=300)
check("series-level /metadata still works", r2.status_code == 200,
      f"HTTP {r2.status_code}")
if r2.status_code == 200:
    sop = r2.json()[0]["00080018"]["Value"][0]

if sop:
    r3 = s.get(f"{BASE}/dicom-web/studies/{STUDY}/series/{series['SEG']}"
               f"/instances/{sop}/metadata", headers={"Accept": DJ}, timeout=300)
    check("instance-level /metadata is implemented", r3.status_code == 200,
          f"HTTP {r3.status_code}")
    if r3.status_code == 200:
        one = r3.json()
        check("instance metadata returns exactly one instance", len(one) == 1,
              f"{len(one)}")
        check("instance metadata carries the expanded SegmentSequence",
              len(one[0].get("00620002", {}).get("Value") or []) == 5,
              f"{len(one[0].get('00620002', {}).get('Value') or [])} segments")

r4 = s.get(f"{BASE}/dicom-web/studies/1.2.3.4.5.6.7.8.9/metadata",
           headers={"Accept": DJ}, timeout=120)
check("unknown study still 404s (not an empty 200)", r4.status_code == 404,
      f"HTTP {r4.status_code}")

print(f"\n{'ALL PASS' if not fails else 'FAILURES: ' + ', '.join(fails)}")
sys.exit(1 if fails else 0)
