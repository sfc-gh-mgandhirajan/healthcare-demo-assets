"""Series-open prefetcher.

=============================================================================
WHY THIS EXISTS
=============================================================================

The GA SPCS stage-volume implementation has no local disk cache and is documented
as WEAK at random small reads and TUNED for large contiguous ones. Frame serving is
inherently the former: a radiologist scrolling a 500-slice CT generates hundreds of
small positioned reads scattered across hundreds of files.

This process converts that access pattern into the one the platform is good at. On
series open the gateway drops a hint; this sidecar streams each file of that series
SEQUENTIALLY into the shared memory volume, and subsequent frame reads come from
RAM.

The prefetch oracle is the radiologist's own behaviour. Opening a series is a
near-perfect predictor that every frame in it will be requested within seconds --
a far stronger signal than any access-frequency heuristic computed over history,
and it needs no scoring function at all. The incumbent computes
    0.5*exp(-h_since_used/24) + 0.2*exp(-h_since_inserted/168) + 0.3*log10(1+count)
to decide what to preload at startup. Series-open needs none of that.

=============================================================================
THIS IS AN OPTIMIZATION AND MUST BEHAVE LIKE ONE
=============================================================================

Every failure here is swallowed. A file that cannot be warmed is read from the
stage volume instead: slower, correct. If this process dies the service keeps
serving. Nothing in the request path waits on it.

The memory volume is bounded, so admission is oldest-out. A series being actively
scrolled must not be evicted by background warming of another, which is why the
gateway's manifest cache and this ring are ordered independently.
"""
from __future__ import annotations

import logging
import os
import shutil
import sys
import time

logging.basicConfig(
    stream=sys.stdout,
    level=os.environ.get("VOXEL_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("voxel.prefetch")

MOUNT = os.environ.get("VOXEL_PREAD_MOUNT", "/mnt/dicom")
HOTRING = os.environ.get("VOXEL_HOTRING", "/dev/shm/voxel")
BUDGET_BYTES = int(os.environ.get("VOXEL_HOTRING_BUDGET_BYTES", str(24 * 1024**3)))
CHUNK = 8 * 1024 * 1024  # large sequential reads, which is the point


def hot_path(rel_path: str) -> str:
    """Flatten the path so the ring is a single directory.

    A nested tree in tmpfs costs directory inodes and makes the eviction scan a
    walk instead of a listdir, for no benefit.
    """
    return os.path.join(HOTRING, rel_path.lstrip("/").replace("/", "__"))


def ring_bytes() -> int:
    total = 0
    try:
        with os.scandir(HOTRING) as it:
            for e in it:
                try:
                    total += e.stat().st_size
                except OSError:
                    pass
    except OSError:
        pass
    return total


def evict_to(budget: int) -> None:
    """Oldest-out until under budget.

    Uses st_atime so a file being actively read survives, rather than st_mtime
    which only records when it was warmed and would evict a hot series.
    """
    try:
        entries = []
        with os.scandir(HOTRING) as it:
            for e in it:
                try:
                    st = e.stat()
                    entries.append((st.st_atime, st.st_size, e.path))
                except OSError:
                    pass
    except OSError:
        return

    total = sum(sz for _, sz, _ in entries)
    if total <= budget:
        return
    entries.sort(key=lambda t: t[0])
    for _, size, path in entries:
        if total <= budget:
            break
        try:
            os.unlink(path)
            total -= size
        except OSError:
            pass


def warm_file(rel_path: str) -> bool:
    """Copy one file into the ring with large sequential reads.

    Writes to a temp name then renames. A partially-written file at the final name
    would be read as if complete, and every frame past the truncation point would
    return short reads -- which a decoder may render as noise rather than reject.
    The rename makes visibility atomic.
    """
    src = os.path.join(MOUNT, rel_path.lstrip("/"))
    dst = hot_path(rel_path)
    if os.path.exists(dst):
        return True
    tmp = dst + ".part"
    try:
        with open(src, "rb", buffering=0) as fi, open(tmp, "wb", buffering=0) as fo:
            shutil.copyfileobj(fi, fo, CHUNK)
        os.replace(tmp, dst)
        return True
    except OSError as exc:
        log.debug("warm failed for %s: %s", rel_path, exc)
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return False


def warm_series(paths: list[str]) -> tuple[int, int]:
    ok = 0
    for p in paths:
        if warm_file(p):
            ok += 1
    evict_to(BUDGET_BYTES)
    return ok, len(paths)


def main() -> None:
    """Standalone loop: tail the hint file and warm what the gateway asked for.

    The IPC channel is a file in the memory volume that both containers mount.
    The gateway appends one relative path per line to $HOTRING/_hints (see
    app.prefetch_hint) and this process tails it from its last read offset.

    A file rather than a socket because it needs no listener, survives either
    container restarting independently, and the ordering guarantee that matters
    -- O_APPEND writes do not interleave mid-line -- comes free.
    """
    os.makedirs(HOTRING, exist_ok=True)
    log.info("prefetch started: mount=%s ring=%s budget=%.1fGiB",
             MOUNT, HOTRING, BUDGET_BYTES / 1024**3)

    hint_file = os.path.join(HOTRING, "_hints")
    pos = 0
    while True:
        try:
            if os.path.exists(hint_file):
                with open(hint_file) as fh:
                    fh.seek(pos)
                    lines = fh.readlines()
                    pos = fh.tell()
                paths = [ln.strip() for ln in lines if ln.strip()]
                if paths:
                    ok, total = warm_series(paths)
                    log.info("warmed %d/%d hinted file(s); ring=%.1fMiB",
                             ok, total, ring_bytes() / 1024**2)
            time.sleep(2)
        except Exception as exc:  # noqa: BLE001 - a prefetcher must never die
            log.warning("prefetch loop error (continuing): %s", exc)
            time.sleep(5)


if __name__ == "__main__":
    main()
