"""How long does OHIF's volume load actually take against this gateway?

OHIF builds a CT volume by issuing one WADO-RS frame request PER SLICE. The
Carina-to-Apex series is 459 slices, so the viewport stays black until roughly
459 sequential-ish round trips complete. This measures the per-frame cost and
extrapolates, at OHIF's default concurrency.
"""
import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import requests

PAT = os.environ["VOXEL_PAT"]
BASE = os.environ["VOXEL_URL"].rstrip("/")
STUDY = os.environ["SEG_STUDY_UID"]

s = requests.Session()
s.headers["Authorization"] = f'Snowflake Token="{PAT}"'
s.headers["X-Snowflake-Authorization-Token-Type"] = "PROGRAMMATIC_ACCESS_TOKEN"

r = s.get(f"{BASE}/dicom-web/studies/{STUDY}/series?limit=50",
          headers={"Accept": "application/dicom+json"}, timeout=120)
ct = [row for row in r.json()
      if row.get("00080060", {}).get("Value", [None])[0] == "CT"]
series = ct[0]["0020000E"]["Value"][0]

r = s.get(f"{BASE}/dicom-web/studies/{STUDY}/series/{series}/instances?limit=1000",
          headers={"Accept": "application/dicom+json"}, timeout=300)
sops = [x["00080018"]["Value"][0] for x in r.json()]
print(f"CT series has {len(sops)} instances\n")


def fetch(sop):
    t = time.perf_counter()
    resp = s.get(f"{BASE}/dicom-web/studies/{STUDY}/series/{series}"
                 f"/instances/{sop}/frames/1",
                 headers={"Accept": "multipart/related"}, timeout=120)
    return time.perf_counter() - t, len(resp.content), resp.status_code


sample = sops[:20]
serial = [fetch(x) for x in sample]
lat = [d for d, _, _ in serial]
codes = {c for _, _, c in serial}
print(f"serial, n={len(sample)}   status={codes}")
print(f"  median {statistics.median(lat)*1000:7.1f} ms")
print(f"  mean   {statistics.mean(lat)*1000:7.1f} ms")
print(f"  p90    {sorted(lat)[int(len(lat)*.9)]*1000:7.1f} ms")
print(f"  bytes/frame {serial[0][1]:,}")

for workers in (6, 12):
    t = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(fetch, sops[:60]))
    el = time.perf_counter() - t
    per = el / 60
    print(f"\nconcurrency {workers:2}: 60 frames in {el:5.1f}s "
          f"({per*1000:.0f} ms/frame effective)")
    print(f"  => {len(sops)} slices would take ~{per*len(sops):.0f}s")
