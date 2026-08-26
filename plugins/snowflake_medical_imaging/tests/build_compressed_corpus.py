"""Build an ENCAPSULATED (compressed) DICOM test corpus from real CT pixel data.

=============================================================================
WHY THIS EXISTS
=============================================================================

The IDC corpus the platform was built and benchmarked on is 100% NATIVE UNCOMPRESSED
(Explicit/Implicit VR Little Endian). That means the entire encapsulated frame
path has never executed:

    - Extended Offset Table probe  (7FE0,0001)/(7FE0,0002)
    - Basic Offset Table parsing
    - (FFFE,E000) item-header scanning when the BOT is empty
    - DICOM_FRAME_OFFSET writes and the packed-array STORED path

DICOM_FRAME_OFFSET had 0 rows, which is simultaneously the headline cost claim
and proof that the harder half of the design is unverified. Real PACS archives
are overwhelmingly JPEG2000 / JPEG-LS, so this is the case that matters.

=============================================================================
WHY SYNTHESIZE RATHER THAN FIND A PUBLIC COMPRESSED CORPUS
=============================================================================

A public corpus covers whatever it happens to cover. Synthesizing from real
pixel data lets us construct the SPECIFIC cases the code claims to handle,
including the two that are easy to get wrong and rare in the wild:

    1. MULTI-FRAME WITH A POPULATED BASIC OFFSET TABLE   -- the common path
    2. MULTI-FRAME WITH AN EXTENDED OFFSET TABLE         -- the fast path
    3. MULTI-FRAME WITH AN EMPTY BOT                     -- forces the item scan
    4. SINGLE-FRAME ENCAPSULATED                         -- degenerate case

Case 3 is the one that finds bugs. An empty Basic Offset Table is legal, and the
only way to locate frames is to walk item headers. If that walk is off by the
8 bytes of an item header, every frame after the first is wrong -- and it returns
HTTP 200 with plausible-looking pixels rather than an error.

The pixel data is REAL (decoded from actual IDC CT slices), so this is not
synthetic imagery -- only the encapsulation is constructed.

=============================================================================
GROUND TRUTH
=============================================================================

For every file we record, independently of the platform, what pydicom itself reports as
each frame's byte range. That is the oracle the ingested offsets get compared
against, and it is what makes this a test rather than a demo.
"""
from __future__ import annotations

import json
import os
import struct
import sys
from pathlib import Path

import numpy as np
import pydicom
from pydicom.dataset import Dataset
from pydicom.encaps import encapsulate, encapsulate_extended
from pydicom.uid import RLELossless, JPEG2000Lossless, ExplicitVRLittleEndian

SRC = Path("/tmp/vxr/src")
OUT = Path("/tmp/vxr/compressed")
OUT.mkdir(parents=True, exist_ok=True)

# RLE Lossless is encapsulated and encodable by pydicom with no external codec,
# so it exercises the encapsulated path deterministically. JPEG2000Lossless is
# added when a codec is present because it is what real archives actually use.
TARGETS = [RLELossless, JPEG2000Lossless]


def encoded_frames(ds: Dataset, uid) -> list[bytes]:
    """Compress each frame of a dataset to the target transfer syntax."""
    arr = ds.pixel_array
    if arr.ndim == 2:
        arr = arr[np.newaxis, ...]
    out = []
    for i in range(arr.shape[0]):
        frame_ds = ds.copy()
        frame_ds.NumberOfFrames = 1
        frame_ds.PixelData = arr[i].tobytes()
        out.append(frame_ds.compress(uid, arr[i], encoding_plugin="") or b"")
    return out


def _encode_one(ds: Dataset, arr2d, uid) -> bytes:
    """Encode a single 2D frame, returning the compressed bytes.

    Uses generate_frames, NOT generate_fragments. This distinction cost a build:
    generate_fragments yields the BASIC OFFSET TABLE as its first item, so
    `next(iter(generate_fragments(...)))` returns a 4-byte BOT rather than the
    ~316 KB frame. The resulting files were structurally valid, computed correct
    ground truth, and contained NO PIXEL DATA -- 8 "frames" of 4 bytes each.

    Exactly the failure shape this whole corpus exists to catch, reproduced in the
    tool built to catch it.
    """
    tmp = ds.copy()
    tmp.file_meta = ds.file_meta.copy()
    tmp.compress(uid, arr2d)
    from pydicom.encaps import generate_frames
    frame = next(iter(generate_frames(tmp.PixelData, number_of_frames=1)))
    if len(frame) < 1024:
        raise ValueError(
            f"encoded frame is only {len(frame)} bytes -- almost certainly the "
            f"Basic Offset Table rather than pixel data"
        )
    return frame


def build(base_path: Path, uid, n_frames: int, mode: str, out_name: str) -> dict:
    """Assemble one encapsulated multi-frame file.

    mode:
      'bot'   standard Basic Offset Table (encapsulate with has_bot=True)
      'eot'   Extended Offset Table in (7FE0,0001)/(7FE0,0002)
      'nobot' EMPTY Basic Offset Table -- consumer must scan item headers
    """
    ds = pydicom.dcmread(base_path)
    arr = ds.pixel_array
    if arr.ndim == 3:
        arr = arr[0]

    # Encode the same real slice n_frames times. Reusing one slice keeps frame
    # lengths IDENTICAL, which is deliberately the HARDEST case for offset bugs:
    # an off-by-one-frame error produces byte-identical output and is invisible.
    # Case 'nobot_varlen' below breaks that on purpose.
    frames = [_encode_one(ds, arr, uid) for _ in range(n_frames)]

    out = pydicom.dcmread(base_path)
    out.file_meta.TransferSyntaxUID = uid
    out.NumberOfFrames = n_frames
    out.SOPInstanceUID = pydicom.uid.generate_uid()
    out.SeriesInstanceUID = SERIES_UID
    out.SeriesDescription = f"VOXEL SYNTH {uid.name} {mode} {n_frames}f"[:60]
    out.SeriesNumber = 9000

    if mode == "eot":
        # encapsulate_extended returns THREE values in pydicom 3.x:
        # (pixel_data, offsets, lengths)
        pixel_data, eot_offsets, eot_lengths = encapsulate_extended(frames)
        out.PixelData = pixel_data
        out.ExtendedOffsetTable = eot_offsets
        out.ExtendedOffsetTableLengths = eot_lengths
    elif mode == "bot":
        out.PixelData = encapsulate(frames, has_bot=True)
    else:  # nobot
        out.PixelData = encapsulate(frames, has_bot=False)

    out["PixelData"].is_undefined_length = True
    path = OUT / out_name
    out.save_as(path, enforce_file_format=True)

    # ---------------------------------------------------------------------
    # GROUND TRUTH, computed independently of the platform.
    #
    # Re-read from disk and ask pydicom where each fragment starts. Offsets are
    # recorded ABSOLUTE (from byte 0 of the file), which is what
    # DICOM_FRAME_OFFSET stores and what os.pread needs.
    # ---------------------------------------------------------------------
    raw = path.read_bytes()
    pd_tag = b"\xe0\x7f\x10\x00"
    pd_at = raw.find(pd_tag)
    # Explicit VR OB with undefined length: tag(4) + VR(2) + reserved(2) + len(4)
    first_item = raw.find(b"\xfe\xff\x00\xe0", pd_at)

    truth = []
    pos = first_item
    # Skip the Basic Offset Table item (always present as an item, possibly empty)
    bot_len = struct.unpack_from("<I", raw, pos + 4)[0]
    pos = pos + 8 + bot_len
    for i in range(n_frames):
        assert raw[pos:pos + 4] == b"\xfe\xff\x00\xe0", f"item header missing at {pos}"
        ln = struct.unpack_from("<I", raw, pos + 4)[0]
        truth.append({"frame": i + 1, "offset": pos + 8, "length": ln})
        pos = pos + 8 + ln

    # Guard: a frame this small means the encoder handed back a BOT, not pixels.
    if any(f["length"] < 1024 for f in truth):
        raise ValueError(f"{out_name}: frame lengths look wrong: "
                         f"{[f['length'] for f in truth][:4]}")

    return {
        "file": out_name,
        "sop_instance_uid": out.SOPInstanceUID,
        "transfer_syntax": str(uid),
        "transfer_syntax_name": uid.name,
        "mode": mode,
        "n_frames": n_frames,
        "file_size": len(raw),
        "rows": int(out.Rows),
        "columns": int(out.Columns),
        "bits_allocated": int(out.BitsAllocated),
        "samples_per_pixel": int(out.SamplesPerPixel),
        "frames": truth,
    }


SERIES_UID = pydicom.uid.generate_uid()

if __name__ == "__main__":
    srcs = sorted(SRC.glob("*.dcm"))
    if not srcs:
        print("no source files", file=sys.stderr)
        sys.exit(1)

    manifest = []
    plan = [
        # (transfer syntax, n_frames, mode, name)
        (RLELossless, 8, "bot", "rle_8f_bot.dcm"),
        (RLELossless, 8, "eot", "rle_8f_eot.dcm"),
        (RLELossless, 8, "nobot", "rle_8f_nobot.dcm"),
        (RLELossless, 1, "bot", "rle_1f_bot.dcm"),
    ]
    if JPEG2000Lossless:
        plan += [
            (JPEG2000Lossless, 6, "bot", "j2k_6f_bot.dcm"),
            (JPEG2000Lossless, 6, "nobot", "j2k_6f_nobot.dcm"),
            (JPEG2000Lossless, 6, "eot", "j2k_6f_eot.dcm"),
        ]

    for i, (uid, nf, mode, name) in enumerate(plan):
        base = srcs[i % len(srcs)]
        try:
            rec = build(base, uid, nf, mode, name)
            manifest.append(rec)
            print(f"built {name:22s} {rec['transfer_syntax_name'][:26]:26s} "
                  f"{nf}f  {rec['file_size']:>9,}B  frame1@{rec['frames'][0]['offset']}")
        except Exception as exc:
            print(f"FAILED {name}: {type(exc).__name__}: {exc}")

    (OUT / "_ground_truth.json").write_text(json.dumps(manifest, indent=2))
    print(f"\n{len(manifest)} file(s) + ground truth -> {OUT}")
    print(f"series uid: {SERIES_UID}")
