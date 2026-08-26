"""Validate the overlay against anatomy and against the pipeline's own numbers.

Run after landing a DICOM-SEG. Answers three questions a screenshot cannot:

  1. Is the mask in the right PLACE? (laterality, midline vessels)
  2. Is it spatially REGISTERED to the CT? (uniform z grid on the source spacing)
  3. Does the encoded DICOM-SEG agree with what the pipeline recorded?

VOLUME MUST USE THE SLICE INCREMENT, NOT SliceThickness.
This series is a cardiac CTA reconstructed at 0.6 mm thickness with a 0.3 mm
increment -- 50% overlap, which is routine. Multiplying pixel area by thickness
double-counts every voxel and reported a 745 mL liver where the truth is 372 mL.
The pipeline got this right by deriving spacing from the affine; this test did
not, and briefly made a correct segmentation look wrong.

FIELD OF VIEW BOUNDS WHAT CAN BE MEASURED.
"Carina to Apex" is a chest study, so the liver is CUT OFF at the inferior edge.
Asserting a whole-organ volume range (1000-2500 mL) fails on a partial organ, so
the checks below test shape and position -- which survive truncation -- and defer
absolute volume to the cross-check against SEG_RESULT.

DICOM patient coordinates: +x -> patient LEFT, +y -> POSTERIOR, +z -> HEAD.
Radiological display puts patient RIGHT on the viewer's LEFT, so the liver sits
top-left on screen while holding the SMALLEST x.
"""
import os
import sys

import numpy as np
import pydicom

SEG = os.environ.get("SEG_FILE", "/tmp/vxr/segmentation.dcm")
ds = pydicom.dcmread(SEG)

names = {int(s.SegmentNumber): str(s.SegmentLabel) for s in ds.SegmentSequence}
pf = ds.PerFrameFunctionalGroupsSequence
mask = ds.pixel_array.astype(bool)
shared = ds.SharedFunctionalGroupsSequence[0]
pm = shared.PixelMeasuresSequence[0]
sp_r, sp_c = [float(v) for v in pm.PixelSpacing]
thickness = float(pm.SliceThickness)

zs_all = sorted({round(float(it.PlanePositionSequence[0].ImagePositionPatient[2]), 4)
                 for it in pf})
dz = np.diff(zs_all)
increment = float(np.median(dz))

print(f"{SEG}")
print(f"  segments   : {list(names.values())}")
print(f"  frames     : {len(pf)}  ({len(zs_all)} distinct z)")
print(f"  in-plane   : {sp_r} x {sp_c} mm")
print(f"  thickness  : {thickness} mm,  increment {increment:.3f} mm"
      f"{'  (OVERLAPPING)' if thickness > increment * 1.5 else ''}")

voxel_ml = sp_r * sp_c * increment / 1000.0

stats = {}
for i, it in enumerate(pf):
    s = int(it.SegmentIdentificationSequence[0].ReferencedSegmentNumber)
    m = mask[i]
    n = int(m.sum())
    if n == 0:
        continue
    ipp = [float(v) for v in it.PlanePositionSequence[0].ImagePositionPatient]
    rr, cc = np.nonzero(m)
    a = stats.setdefault(s, {"n": 0, "sx": 0.0, "slices": set(), "areas": []})
    a["n"] += n
    a["sx"] += float((ipp[0] + cc * sp_c).sum())
    a["slices"].add(round(ipp[2], 3))
    a["areas"].append(n * sp_r * sp_c / 100.0)   # cm^2

info = {}
print(f"\n{'organ':20} {'mL':>8} {'x_cm':>7} {'slices':>7} {'cm2/slice':>10}")
print("-" * 58)
for s, a in sorted(stats.items(), key=lambda kv: -kv[1]["n"]):
    info[names[s]] = d = dict(
        vol=a["n"] * voxel_ml, x=a["sx"] / a["n"],
        slices=len(a["slices"]), area=float(np.mean(a["areas"])),
        zmin=min(a["slices"]), zmax=max(a["slices"]))
    print(f"{names[s]:20} {d['vol']:8.1f} {d['x']/10:7.1f} "
          f"{d['slices']:7d} {d['area']:10.2f}")

fails = []


def check(name, ok, detail=""):
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}  {detail}")
    if not ok:
        fails.append(name)


print()
check("z grid is uniform (SEG registered to source slice spacing)",
      bool(np.allclose(dz, increment, atol=1e-3)),
      f"increment {increment:.3f} mm, spread {dz.max()-dz.min():.4f}")

L, S = info.get("liver", {}), info.get("spleen", {})
A, V = info.get("aorta", {}), info.get("inferior vena cava", {})

check("liver is the largest segment", bool(
    L and L["vol"] == max(v["vol"] for v in info.values())),
    f"{L.get('vol', 0):.0f} mL")
check("liver is on the patient's RIGHT (x < 0)", L.get("x", 1) < 0,
      f"x = {L.get('x', 0)/10:.1f} cm")
check("spleen is on the patient's LEFT (x > 0)", S.get("x", -1) > 0,
      f"x = {S.get('x', 0)/10:.1f} cm")
check("liver is right of spleen", L.get("x", 1) < S.get("x", -1),
      f"{L.get('x',0)/10:.1f} < {S.get('x',0)/10:.1f} cm")
check("aorta is near the midline (|x| < 4 cm)", abs(A.get("x", 99)) < 40,
      f"x = {A.get('x', 0)/10:.1f} cm")
check("IVC is right of the aorta", V.get("x", 99) < A.get("x", -99),
      f"IVC {V.get('x',0)/10:.1f} vs aorta {A.get('x',0)/10:.1f} cm")

# Shape, not volume: a vessel is long and thin regardless of how much of it the
# field of view captured.
check("aorta is tubular: spans most of the scan with a small cross-section",
      A.get("slices", 0) >= 0.8 * len(zs_all) and A.get("area", 99) < 15,
      f"{A.get('slices',0)}/{len(zs_all)} slices, "
      f"{A.get('area',0):.1f} cm2 mean cross-section")
check("IVC cross-section is smaller than the aorta's",
      V.get("area", 99) < A.get("area", 0),
      f"{V.get('area',0):.1f} vs {A.get('area',0):.1f} cm2")
check("liver cross-section far exceeds any vessel's",
      L.get("area", 0) > 5 * A.get("area", 99),
      f"{L.get('area',0):.1f} vs {A.get('area',0):.1f} cm2")

print(f"\n{'ALL PASS' if not fails else 'FAILURES: ' + ', '.join(fails)}")

print("\nvolumes for cross-check against SEG_RESULT.VOLUME_MM3/1000:")
for n, d in sorted(info.items(), key=lambda kv: -kv[1]["vol"]):
    print(f"  {n:20} {d['vol']:8.1f} mL")

sys.exit(1 if fails else 0)
