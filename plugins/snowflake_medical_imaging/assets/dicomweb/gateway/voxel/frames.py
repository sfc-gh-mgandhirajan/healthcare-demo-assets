"""Frame offset resolution and byte-range reads.

=============================================================================
THE WHOLE COST ARGUMENT LIVES IN THIS FILE
=============================================================================

The incumbent resolves a frame's byte offset through a 3-tier cache: an
in-memory LRU, a managed Postgres instance holding one row per frame, and live
compute as the fallback. A Spark job repopulates it after every ingest and a
priority scorer decides what to preload at app startup.

Most of that does not need to exist.

    NATIVE uncompressed syntax  ->  the offset is CLOSED FORM:
        offset(n) = pixel_data_start
                  + n * (rows * cols * bits_allocated * samples) // 8

        Every multiplicand except pixel_data_start is already in the QIDO-RS
        metadata the viewer holds. NOTHING IS STORED, NOTHING IS CACHED, there
        is no eviction policy, no priming job, and no staleness question.
        On the IDC corpus this is 100% of instances.

    ENCAPSULATED  ->  offsets were discovered once at ingest (Extended Offset
        Table, else an item-header scan) and persisted in DICOM_FRAME_OFFSET.
        Read ONCE per series into a packed array, not once per frame.

So the "3 GB of cache for 100k CT entries" capacity problem does not arise: a
packed array costs 12 bytes per frame (uint64 offset + uint32 length) against
the incumbent's own documented ~1.7 KB per cache entry.

=============================================================================
READS GO THROUGH A MOUNTED STAGE VOLUME, NOT A WAREHOUSE
=============================================================================

The DICOM files are mounted at VOXEL_PREAD_MOUNT, so serving a frame is
``os.pread(fd, length, offset)`` -- an ordinary positioned read against
Snowflake-managed storage. No SQL, no warehouse, no UDF, no scoped URL.

That deletes the incumbent's entire Tier 3 (documented at 300-500 ms: a SQL warehouse
path lookup plus an object read). It also means the frame path costs compute-pool
seconds only, with no warehouse credits at all.

Two constraints shape the implementation:

  * Stage volumes mount INTERNAL stages only. That is why the pipeline copies a
    subset from the external IDC stage into an internal one.
  * The GA implementation has no local cache and is documented as WEAK at random
    small reads, TUNED for large contiguous ones. Hence ``coalesce`` below, and
    the series-open prefetch in prefetch.py.
"""
from __future__ import annotations

import logging
import os
import struct
import threading
from dataclasses import dataclass

log = logging.getLogger("voxel.frames")

MOUNT = os.environ.get("VOXEL_PREAD_MOUNT", "/mnt/dicom")
HOTRING = os.environ.get("VOXEL_HOTRING", "/dev/shm/voxel")

# Mirrors DICOM_CAPABILITY.IS_ENCAPSULATED = FALSE. Duplicated deliberately: a
# transfer syntax's encapsulation is a fact from the DICOM standard, not a
# tunable, and the read path must not need a SQL round trip to classify a file.
NATIVE_SYNTAXES = {
    "1.2.840.10008.1.2",
    "1.2.840.10008.1.2.1",
    "1.2.840.10008.1.2.2",
}

# Reading more than this in one coalesced span is worse than several reads --
# it pulls bytes nobody asked for. 64 MiB comfortably covers a scroll window of
# 512x512x16-bit frames.
MAX_COALESCE_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True)
class FrameRef:
    """Where one frame's bytes are."""

    offset: int
    length: int


class SeriesManifest:
    """Resolved frame offsets for every instance in one series.

    Keyed by series because that is the ACTUAL UNIT OF ACCESS: a radiologist never
    opens a single frame, they open a series and then scroll. Caching at series
    granularity is what collapses the incumbent's three tiers into one -- the per-frame
    lookup tier stops existing, and with it the eviction policy and the priority
    scorer.
    """

    __slots__ = ("series_uid", "_by_instance", "_paths", "_syntax")

    def __init__(self, series_uid: str):
        self.series_uid = series_uid
        # sop_uid -> packed bytes; 12 bytes per frame, decoded on demand.
        self._by_instance: dict[str, bytes] = {}
        self._paths: dict[str, str] = {}
        self._syntax: dict[str, str] = {}

    # -- construction --------------------------------------------------------
    def add_arithmetic(
        self,
        sop_uid: str,
        path: str,
        syntax: str,
        pixel_data_start: int,
        rows: int,
        cols: int,
        bits_allocated: int,
        samples: int,
        num_frames: int,
    ) -> None:
        """Compute offsets from geometry. No stored rows involved.

        BIT-PACKED PIXEL DATA IS HANDLED IN BITS, NOT BYTES.
        --------------------------------------------------
        This used to compute `rows * cols * (bits_allocated // 8) * samples`. For
        every syntax in the original corpus bits_allocated was 8 or 16 and that was
        fine. For a DICOM-SEG with SegmentationType BINARY, bits_allocated is 1,
        integer division gives 0, and EVERY FRAME BECOMES A ZERO-LENGTH READ. The
        request still returns HTTP 200 with an empty part.
        
        Note the SQL side got this right by accident -- `(BITS_ALLOCATED / 8)` in
        Snowflake is 0.125, not 0 -- so V_FRAME_COVERAGE happily declared these
        instances servable while the gateway could not serve them. Two
        implementations of one formula, disagreeing silently.

        Computing in bits also exposes a real constraint: PS3.3 packs BINARY
        segmentation frames contiguously with no inter-frame padding, so a frame
        only starts on a byte boundary when rows*cols*samples is a multiple of 8.
        os.pread cannot do sub-byte offsets, so the unaligned case is REFUSED
        rather than served shifted by a few bits.
        """
        frame_bits = rows * cols * bits_allocated * samples
        if frame_bits % 8:
            raise ValueError(
                f"{sop_uid}: bit-packed frames are not byte-aligned "
                f"({rows}x{cols}x{samples} at {bits_allocated} bit(s) = "
                f"{frame_bits} bits/frame). Arithmetic offsets cannot address "
                "a sub-byte boundary; this instance needs stored offsets."
            )
        frame_bytes = frame_bits // 8
        packed = bytearray(12 * num_frames)
        for n in range(num_frames):
            struct.pack_into(
                "<QI", packed, 12 * n, pixel_data_start + n * frame_bytes, frame_bytes
            )
        self._by_instance[sop_uid] = bytes(packed)
        self._paths[sop_uid] = path
        self._syntax[sop_uid] = syntax

    def add_stored(
        self, sop_uid: str, path: str, syntax: str, rows: list[tuple[int, int, int]]
    ) -> None:
        """Load offsets discovered at ingest.

        ``rows`` is (frame_number, offset, length), 1-based frame numbers as
        stored. Packed into position frame_number-1 so a gap in the input is a
        zero-length entry rather than a silent shift of every later frame -- which
        would serve the wrong slice at HTTP 200.
        """
        if not rows:
            return
        max_frame = max(r[0] for r in rows)
        packed = bytearray(12 * max_frame)
        for frame_no, offset, length in rows:
            if frame_no < 1:
                continue
            struct.pack_into("<QI", packed, 12 * (frame_no - 1), offset, length)
        self._by_instance[sop_uid] = bytes(packed)
        self._paths[sop_uid] = path
        self._syntax[sop_uid] = syntax

    # -- lookup --------------------------------------------------------------
    def frame(self, sop_uid: str, frame_number: int) -> FrameRef | None:
        """Resolve one frame. Array index, not a query.

        frame_number is 1-BASED, matching DICOMweb (/frames/1 is the first frame)
        and the DICOM standard. An off-by-one here does not error; it returns the
        neighbouring slice, which on a CT is different anatomy.
        """
        packed = self._by_instance.get(sop_uid)
        if packed is None or frame_number < 1:
            return None
        pos = 12 * (frame_number - 1)
        if pos + 12 > len(packed):
            return None
        offset, length = struct.unpack_from("<QI", packed, pos)
        if length == 0:
            # A zero-length entry means the ingest offset extraction was partial.
            # Refuse rather than serving zero bytes as if they were an image.
            return None
        return FrameRef(offset=offset, length=length)

    def frame_count(self, sop_uid: str) -> int:
        packed = self._by_instance.get(sop_uid)
        return 0 if packed is None else len(packed) // 12

    def path(self, sop_uid: str) -> str | None:
        return self._paths.get(sop_uid)

    def syntax(self, sop_uid: str) -> str | None:
        return self._syntax.get(sop_uid)

    def instances(self) -> list[str]:
        return list(self._by_instance)

    def nbytes(self) -> int:
        return sum(len(p) for p in self._by_instance.values())


# ---------------------------------------------------------------------------
# Manifest cache
# ---------------------------------------------------------------------------
class ManifestCache:
    """In-process series manifests.

    Deliberately NOT user-scoped, and deliberately only reachable AFTER a
    policy-enforced lookup.

    The incumbent makes the same choice for its BOT cache and justifies it as "offsets
    are only reachable after a secured path lookup" -- which is
    security-through-sequencing, and breaks the moment a path leaks through a log
    line or an error message.

    The difference here is that a byte offset is USELESS ON ITS OWN. There is no
    route that accepts an offset. Every frame request re-resolves the instance
    through a caller-rights query first, so the offsets are never the
    authorization token. That is a structural property, not a sequencing
    assumption.
    """

    def __init__(self, max_series: int = 256):
        self._max = max_series
        self._lock = threading.Lock()
        self._entries: dict[str, SeriesManifest] = {}
        self._order: list[str] = []

    def get(self, series_uid: str) -> SeriesManifest | None:
        with self._lock:
            m = self._entries.get(series_uid)
            if m is not None:
                # Move to most-recent. A radiologist scrolling one series must not
                # have it evicted by background prefetch of another.
                try:
                    self._order.remove(series_uid)
                except ValueError:
                    pass
                self._order.append(series_uid)
            return m

    def put(self, manifest: SeriesManifest) -> None:
        with self._lock:
            self._entries[manifest.series_uid] = manifest
            self._order.append(manifest.series_uid)
            while len(self._order) > self._max:
                victim = self._order.pop(0)
                self._entries.pop(victim, None)

    def stats(self) -> dict:
        with self._lock:
            return {
                "series_cached": len(self._entries),
                "manifest_bytes": sum(m.nbytes() for m in self._entries.values()),
            }


# ---------------------------------------------------------------------------
# Byte-range reads
# ---------------------------------------------------------------------------
class FileHandles:
    """Open file descriptors, kept per instance.

    Reopening per frame is measurable at scroll rates. Stage-volume I/O has higher
    latency than a local filesystem, so an avoidable open() is an avoidable
    round trip.
    """

    def __init__(self, max_open: int = 128):
        self._max = max_open
        self._lock = threading.Lock()
        self._fds: dict[str, int] = {}
        self._order: list[str] = []

    def fd(self, rel_path: str) -> int:
        with self._lock:
            fd = self._fds.get(rel_path)
            if fd is not None:
                return fd

        full = os.path.join(MOUNT, rel_path.lstrip("/"))
        fd = os.open(full, os.O_RDONLY)

        with self._lock:
            self._fds[rel_path] = fd
            self._order.append(rel_path)
            while len(self._order) > self._max:
                victim = self._order.pop(0)
                vfd = self._fds.pop(victim, None)
                if vfd is not None:
                    try:
                        os.close(vfd)
                    except OSError:
                        pass
            return fd

    def close_all(self) -> None:
        with self._lock:
            for fd in self._fds.values():
                try:
                    os.close(fd)
                except OSError:
                    pass
            self._fds.clear()
            self._order.clear()


_handles = FileHandles()


def close_handles() -> None:
    """Close every cached descriptor. Called from the app's shutdown hook.

    Module-level because _handles is a module singleton and the shutdown hook has
    no reason to know that -- and because a public name here is what makes the
    wiring greppable from app.py.
    """
    _handles.close_all()


def read_frame(rel_path: str, ref: FrameRef) -> bytes:
    """One positioned read. No SQL, no warehouse, no HTTP hop."""
    fd = _handles.fd(rel_path)
    return os.pread(fd, ref.length, ref.offset)


def coalesce(refs: list[FrameRef]) -> tuple[int, int] | None:
    """Span covering every ref, if reading it as one block is worthwhile.

    Stage volumes are documented as weak at random small reads and tuned for
    large contiguous ones, so a frame LIST is served as ONE read over
    min(offset) .. max(offset+length) and then sliced in memory.

    Returns None when the span is mostly padding -- for sparse or widely
    separated frames, reading the gap costs more than separate reads. The 2x
    threshold is a judgement call, not a measurement; it should be revisited once
    the frame path has real latency numbers.
    """
    if not refs:
        return None
    lo = min(r.offset for r in refs)
    hi = max(r.offset + r.length for r in refs)
    span = hi - lo
    useful = sum(r.length for r in refs)
    if span > MAX_COALESCE_BYTES:
        return None
    if span > 2 * useful:
        return None
    return lo, span


def read_frames(rel_path: str, refs: list[FrameRef]) -> list[bytes]:
    """Read several frames, coalescing when the layout makes it worthwhile."""
    if not refs:
        return []
    span = coalesce(refs)
    if span is None:
        return [read_frame(rel_path, r) for r in refs]

    base, length = span
    fd = _handles.fd(rel_path)
    blob = os.pread(fd, length, base)
    out = []
    for r in refs:
        start = r.offset - base
        out.append(blob[start : start + r.length])
    return out


def hotring_path(rel_path: str) -> str:
    """Where prefetch.py stages a warmed copy of a file."""
    return os.path.join(HOTRING, rel_path.lstrip("/").replace("/", "__"))


def read_frames_warm(rel_path: str, refs: list[FrameRef]) -> list[bytes]:
    """Prefer the warmed copy in the shared memory volume, else the stage volume.

    Falling back rather than failing is the point: a cold series is SLOWER, never
    broken. The prefetcher is an optimization, and a viewer must not depend on it
    having finished.
    """
    warm = hotring_path(rel_path)
    try:
        fd = os.open(warm, os.O_RDONLY)
    except OSError:
        return read_frames(rel_path, refs)
    try:
        return [os.pread(fd, r.length, r.offset) for r in refs]
    finally:
        try:
            os.close(fd)
        except OSError:
            pass
