"""QIDO-RS query compilation and catalog reads.

=============================================================================
EVERY QUERY HERE RUNS UNDER CALLER'S RIGHTS
=============================================================================

These functions take a connection built by auth.caller_session, so row access
policies on DICOM_INSTANCE are evaluated by the query engine for the actual end
user. There is no application-level filtering, and there is no code path that
decides what a user may see.

That is the difference from the incumbent, which hashes the caller's group list into a
cache key and filters in application memory. There, a cache-key collision is a
PHI disclosure and no audit view can see the decision. Here the decision is a
policy the reviewer can read and every access lands in ACCESS_HISTORY.

=============================================================================
THE FILTER COMPILER IS AN ALLOWLIST, NOT AN ESCAPER
=============================================================================

The incumbent compiles QIDO-RS query parameters straight into VARIANT path predicates
and validates table names with a regex ``^[\\w.]+$``. Two problems: the tag PATH
is interpolated as an identifier rather than bound as a value, and a regex on an
identifier is the wrong shape of control -- ``[\\w.]+`` permits any table in the
account the session can read.

Here:

  * Only attributes present in QIDO_ATTRIBUTES can appear in generated SQL. An
    unrecognised attribute is a 400, never a passthrough. So the set of column
    references that can ever be produced is finite and enumerable by reading this
    file.
  * Every VALUE is a bind parameter.
  * Table names are module constants. There is nothing to validate because
    nothing is derived from input.

When the deid phase lands, QIDO_ATTRIBUTES becomes a read of DICOM_TAG_POLICY
where TAG_CLASS in (SAFE, INDIRECT), so one governed table drives both column
security and query-surface safety.
"""
from __future__ import annotations

import logging
import re

log = logging.getLogger("voxel.queries")

# Module constants. NOT derived from request input, so there is no injection
# surface here to validate.
T_INSTANCE = "DICOM_INSTANCE"

# The frame-plane coverage view. Supplies RESOLUTION (ARITHMETIC vs STORED),
# IS_SERVABLE and IS_STALE, which the frame path joins per instance to decide
# whether an offset can be computed closed-form or must be read from
# DICOM_FRAME_OFFSET.
V_COVERAGE = "V_FRAME_COVERAGE"

# =============================================================================
# A MATERIALIZED QIDO INDEX WAS BUILT, MEASURED, AND REMOVED. DO NOT REBUILD IT
# WITHOUT READING THIS.
#
# The reasoning looked sound: QIDO answers study-grain questions from an
# instance-grain table, so every study list aggregated ~30k rows with
# COUNT(DISTINCT ...) and LISTAGG to return ~357. Cost tracks instance count,
# which is the dimension that grows. So DT_STUDY_INDEX / DT_SERIES_INDEX were
# created, entitlement re-attached, and the queries repointed at them.
#
# A controlled A/B -- alternating forms, USE_CACHED_RESULT = FALSE, 5 reps --
# said the indexed form was SLOWER:
#
#                     compile   execute   total
#   GROUP BY form      357 ms     86 ms   455 ms
#   indexed form       487 ms    109 ms   596 ms
#   GROUP BY + filter  131 ms     67 ms   198 ms
#   indexed  + filter  147 ms     86 ms   233 ms
#
# Two reasons, both measurable:
#
#   1. EXECUTION WAS NEVER THE BOTTLENECK. 86 ms to aggregate 30k rows. The
#      table is ~35 MiB, which is a couple of micro-partitions, so there was
#      very little work to remove.
#   2. COMPILATION DOMINATES, and the indexed query is more complex -- an extra
#      join and more projected columns -- so it compiles SLOWER. The change made
#      the actual bottleneck worse.
#
# The index also could not avoid touching DICOM_INSTANCE at all: PATIENT_NAME,
# PATIENT_ID and ACCESSION_NUMBER are masked columns, and materializing them
# would have copied unmasked PHI into a new table. So they had to be read from
# the source through a join, and the instance-table access the index was supposed
# to eliminate remained.
#
# WHAT AN EARLIER MEASUREMENT GOT WRONG: a single un-repeated run showed
# 423 ms -> 285 ms and looked like a 1.5x win. It was compile-cache noise. One
# sample, no alternation, caching on. The harness comparison was worse than
# useless -- an UNCHANGED code path (search_instances_in_series) showed 3.43x in
# the same window, which is what finally exposed the two eras as incomparable.
#
# WHERE THE ~1 s THE HARNESS REPORTS ACTUALLY GOES: ~450 ms compile + execute,
# plus gateway session setup and the ingress round trip. If QIDO latency needs to
# come down, the targets are plan-cache reuse (LIMIT/OFFSET are interpolated into
# the SQL text, so every distinct page defeats it), session reuse, and the round
# trip -- not the data volume.
#
# Revisit the index only when DICOM_INSTANCE spans hundreds of micro-partitions
# AND execution time, measured with an alternating A/B, actually exceeds compile
# time.
# =============================================================================

# ---------------------------------------------------------------------------
# The allowlist. DICOM keyword -> (typed column, is_uid)
#
# Typed columns rather than VARIANT paths, because the catalog already flattens
# these and a typed comparison is both faster and not susceptible to the
# multi-valued-tag ambiguity a VARIANT path has.
#
# UID attributes are matched EXACTLY, never with wildcards, even if the client
# sends one. A wildcard UID search is a catalog enumeration primitive, and DICOM
# does not define wildcard matching for UI value representations.
# ---------------------------------------------------------------------------
QIDO_ATTRIBUTES: dict[str, tuple[str, bool]] = {
    "StudyInstanceUID": ("STUDY_INSTANCE_UID", True),
    "SeriesInstanceUID": ("SERIES_INSTANCE_UID", True),
    "SOPInstanceUID": ("SOP_INSTANCE_UID", True),
    "SOPClassUID": ("SOP_CLASS_UID", True),
    "StudyDate": ("STUDY_DATE", False),
    "StudyTime": ("STUDY_TIME", False),
    "StudyDescription": ("STUDY_DESCRIPTION", False),
    "SeriesDescription": ("SERIES_DESCRIPTION", False),
    "SeriesNumber": ("SERIES_NUMBER", False),
    "InstanceNumber": ("INSTANCE_NUMBER", False),
    "Modality": ("MODALITY", False),
    "BodyPartExamined": ("BODY_PART_EXAMINED", False),
    "Manufacturer": ("MANUFACTURER", False),
    "ManufacturerModelName": ("MANUFACTURER_MODEL_NAME", False),
    "ProtocolName": ("PROTOCOL_NAME", False),
    # PatientID and PatientName are queryable but MASKED. A non-identified role
    # searching by name matches against '***MASKED***' and gets nothing, which is
    # the correct outcome -- the search is refused by the data, not by a branch in
    # application code that could be wrong.
    "PatientID": ("PATIENT_ID", False),
    "PatientName": ("PATIENT_NAME", False),
    "AccessionNumber": ("ACCESSION_NUMBER", False),
}

# Also accept the gggg,eeee hex form, which is what conformance tooling sends.
_HEX_TO_KEYWORD = {
    "0020000D": "StudyInstanceUID",
    "0020000E": "SeriesInstanceUID",
    "00080018": "SOPInstanceUID",
    "00080016": "SOPClassUID",
    "00080020": "StudyDate",
    "00080030": "StudyTime",
    "00081030": "StudyDescription",
    "0008103E": "SeriesDescription",
    "00200011": "SeriesNumber",
    "00200013": "InstanceNumber",
    "00080060": "Modality",
    "00180015": "BodyPartExamined",
    "00080070": "Manufacturer",
    "00081090": "ManufacturerModelName",
    "00181030": "ProtocolName",
    "00100020": "PatientID",
    "00100010": "PatientName",
    "00080050": "AccessionNumber",
}

# Values are bound, but they are also length- and character-capped before
# binding. A 4 KB LIKE pattern full of '%' is a denial-of-service against the
# catalog even when it is perfectly parameterized.
_MAX_VALUE_LEN = 128
_VALUE_OK = re.compile(r"^[A-Za-z0-9 ._\-^*?,:/+()\[\]]{0,128}$")


class BadQuery(ValueError):
    """Maps to HTTP 400. Never 500 -- a malformed client query is not our fault."""


def _normalise_attribute(raw: str) -> str | None:
    """Resolve a client attribute to an allowlisted keyword, or None."""
    if raw in QIDO_ATTRIBUTES:
        return raw
    key = raw.replace(",", "").replace("(", "").replace(")", "").upper()
    return _HEX_TO_KEYWORD.get(key)


def _dicom_wildcard_to_sql(value: str) -> str:
    """Translate DICOM wildcards to SQL, escaping SQL's own metacharacters first.

    ORDER MATTERS. Escape '%' and '_' (SQL wildcards, literal in DICOM) BEFORE
    translating '*' and '?' (DICOM wildcards). Doing it the other way round makes
    a DICOM '*' become '%' and then escapes it back to a literal, so wildcard
    search silently stops working.
    """
    v = value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return v.replace("*", "%").replace("?", "_")


def build_filters(params: dict[str, str]) -> tuple[list[str], list]:
    """Compile QIDO-RS query parameters into predicates plus bind values.

    Unknown attributes are REJECTED rather than ignored. Silently dropping a
    filter is worse than refusing it: the client believes it narrowed the search
    and gets a broader result set it may treat as authoritative.

    This used to take a `spec` argument mapping a column to (kind, expr), with
    "list" and "exists" branches, for the materialized study/series index. That
    index was measured and reverted -- see the block comment above -- and no caller
    ever passed a spec, so both branches were unreachable code carrying the
    semantics of a design that does not exist. They are removed rather than left
    as a hint, because unreachable code that looks load-bearing is how the next
    person concludes the index is still in play.

    If the index is ever revisited, the requirement those branches encoded is
    recorded in the block comment and must be re-derived deliberately: an
    instance-grain filter at study grain has to mean "this study CONTAINS a
    matching instance", not "the representative instance I happened to pick
    matches". The GROUP BY form gets that right by construction, which is a
    further argument for the form that is here now.
    """
    where: list[str] = []
    binds: list = []

    for raw_key, raw_val in params.items():
        low = raw_key.lower()
        if low in {"limit", "offset", "includefield", "fuzzymatching"}:
            continue

        keyword = _normalise_attribute(raw_key)
        if keyword is None:
            raise BadQuery(
                f"attribute '{raw_key}' is not queryable. Allowed: "
                + ", ".join(sorted(QIDO_ATTRIBUTES))
            )

        if raw_val is None or raw_val == "":
            continue
        if len(raw_val) > _MAX_VALUE_LEN or not _VALUE_OK.match(raw_val):
            raise BadQuery(f"value for '{raw_key}' is too long or has illegal characters")

        column, is_uid = QIDO_ATTRIBUTES[keyword]

        if is_uid:
            # Exact match only. See the allowlist note above.
            where.append(f"{column} = %s")
            binds.append(raw_val)
        elif "*" in raw_val or "?" in raw_val:
            where.append(f"UPPER({column}) LIKE UPPER(%s)")
            binds.append(_dicom_wildcard_to_sql(raw_val))
        else:
            where.append(f"UPPER({column}) = UPPER(%s)")
            binds.append(raw_val)

    return where, binds


# QIDO paging bounds.
#
# DEFAULT_LIMIT exists because an unbounded QIDO query is a self-inflicted
# outage: a client that omits `limit` would otherwise ask for every study in the
# catalog and the gateway would try to serialize all of it into one JSON body.
# MAX_LIMIT caps what a client can ask for even deliberately -- catalog
# enumeration should be slow and bounded, not a single request.
DEFAULT_LIMIT = 100
MAX_LIMIT = 1000


def clamp_limit(params: dict[str, str]) -> int:
    raw = params.get("limit") or params.get("Limit")
    if raw is None:
        return DEFAULT_LIMIT
    try:
        n = int(raw)
    except (TypeError, ValueError):
        raise BadQuery("limit must be an integer") from None
    return max(1, min(n, MAX_LIMIT))


def clamp_offset(params: dict[str, str]) -> int:
    raw = params.get("offset") or params.get("Offset")
    if raw is None:
        return 0
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        raise BadQuery("offset must be an integer") from None


# ---------------------------------------------------------------------------
# Catalog reads
# ---------------------------------------------------------------------------
def _rows(conn, sql: str, binds: list) -> list[dict]:
    cur = conn.cursor()
    try:
        cur.execute(sql, binds)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        cur.close()


def search_studies(conn, params: dict[str, str]) -> list[dict]:
    # =====================================================================
    # THIS IS THE AGGREGATE FORM, ON PURPOSE. A materialized study-grain index
    # was built, measured, and REVERTED. See the note above the table constants.
    # =====================================================================
    where, binds = build_filters(params)
    limit, offset = clamp_limit(params), clamp_offset(params)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT STUDY_INSTANCE_UID,
               ANY_VALUE(STUDY_DATE)         AS STUDY_DATE,
               ANY_VALUE(STUDY_TIME)         AS STUDY_TIME,
               ANY_VALUE(STUDY_DESCRIPTION)  AS STUDY_DESCRIPTION,
               ANY_VALUE(PATIENT_ID)         AS PATIENT_ID,
               ANY_VALUE(PATIENT_NAME)       AS PATIENT_NAME,
               ANY_VALUE(ACCESSION_NUMBER)   AS ACCESSION_NUMBER,
               LISTAGG(DISTINCT MODALITY, '\\\\') AS MODALITIES_IN_STUDY,
               COUNT(DISTINCT SERIES_INSTANCE_UID) AS NUM_SERIES,
               COUNT(*)                      AS NUM_INSTANCES
        FROM {T_INSTANCE}
        {clause}
        GROUP BY STUDY_INSTANCE_UID
        -- ORDER BY the output ALIAS, not a repeated ANY_VALUE(...).
        -- Repeating the aggregate here fails with:
        --   Aggregate functions cannot be nested:
        --   [ANY_VALUE(DICOM_INSTANCE.STUDY_DATE_5)] nested in [ANY_VALUE(STUDY_DATE)]
        -- because Snowflake resolves the ORDER BY expression against the already
        -- aggregated projection. The alias refers to the computed column.
        ORDER BY STUDY_DATE DESC NULLS LAST
        LIMIT {limit} OFFSET {offset}
    """
    return _rows(conn, sql, binds)


def search_series(conn, study_uid: str | None, params: dict[str, str]) -> list[dict]:
    where, binds = build_filters(params)
    if study_uid:
        where.append("STUDY_INSTANCE_UID = %s")
        binds.append(study_uid)
    limit, offset = clamp_limit(params), clamp_offset(params)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT SERIES_INSTANCE_UID,
               ANY_VALUE(STUDY_INSTANCE_UID)   AS STUDY_INSTANCE_UID,
               ANY_VALUE(MODALITY)             AS MODALITY,
               ANY_VALUE(SERIES_NUMBER)        AS SERIES_NUMBER,
               ANY_VALUE(SERIES_DESCRIPTION)   AS SERIES_DESCRIPTION,
               ANY_VALUE(BODY_PART_EXAMINED)   AS BODY_PART_EXAMINED,
               COUNT(*)                        AS NUM_INSTANCES
        FROM {T_INSTANCE}
        {clause}
        GROUP BY SERIES_INSTANCE_UID
        -- Alias, not a repeated aggregate. See search_studies.
        ORDER BY SERIES_NUMBER NULLS LAST
        LIMIT {limit} OFFSET {offset}
    """
    return _rows(conn, sql, binds)


def search_instances(
    conn, study_uid: str | None, series_uid: str | None, params: dict[str, str]
) -> list[dict]:
    where, binds = build_filters(params)
    if study_uid:
        where.append("STUDY_INSTANCE_UID = %s")
        binds.append(study_uid)
    if series_uid:
        where.append("SERIES_INSTANCE_UID = %s")
        binds.append(series_uid)
    limit, offset = clamp_limit(params), clamp_offset(params)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT SOP_INSTANCE_UID, SOP_CLASS_UID, SERIES_INSTANCE_UID,
               STUDY_INSTANCE_UID, INSTANCE_NUMBER,
               IMAGE_ROWS, IMAGE_COLUMNS, BITS_ALLOCATED, BITS_STORED,
               SAMPLES_PER_PIXEL, NUMBER_OF_FRAMES,
               PHOTOMETRIC_INTERPRETATION, PIXEL_REPRESENTATION,
               TRANSFER_SYNTAX_UID, WINDOW_CENTER, WINDOW_WIDTH,
               RESCALE_SLOPE, RESCALE_INTERCEPT,
               PIXEL_SPACING, SLICE_THICKNESS, SLICE_LOCATION,
               IMAGE_POSITION_PATIENT, IMAGE_ORIENTATION_PATIENT,
               FILE_SIZE
        FROM {T_INSTANCE}
        {clause}
        ORDER BY INSTANCE_NUMBER NULLS LAST
        LIMIT {limit} OFFSET {offset}
    """
    return _rows(conn, sql, binds)


def series_metadata(conn, series_uid: str) -> list[dict]:
    """Full per-instance metadata for WADO-RS /metadata.

    Returns RAW_METADATA, which is already the DICOM JSON model, so the route
    layer serves it with no translation. Note this VARIANT is NOT masked in
    phase 1 -- see CHECKS PHI_RAW_METADATA_UNMASKED. The deid phase closes it, and
    when it does this route inherits the fix with no change here.
    """
    sql = f"""
        SELECT SOP_INSTANCE_UID, RAW_METADATA, TRANSFER_SYNTAX_UID
        FROM {T_INSTANCE}
        WHERE SERIES_INSTANCE_UID = %s
        ORDER BY INSTANCE_NUMBER NULLS LAST
        LIMIT {MAX_LIMIT}
    """
    return _rows(conn, sql, [series_uid])


def study_metadata(conn, study_uid: str) -> list[dict]:
    """Full per-instance metadata for every series in a study.

    WADO-RS defines /metadata at study, series AND instance level (PS3.18 10.4).
    This one was simply missing, so the route 404'd. OHIF happens to prefer the
    per-series path when study lazy-load is on, which is why the gap survived
    conformance testing: nothing we drove ever asked for it.

    A study can hold thousands of instances, so this is ordered by series then
    instance -- a client concatenating the result gets a stable, grouped document
    rather than an arbitrary interleaving -- and capped, because an unbounded
    response is a denial-of-service surface, not a feature.
    """
    sql = f"""
        SELECT SOP_INSTANCE_UID, RAW_METADATA, TRANSFER_SYNTAX_UID
        FROM {T_INSTANCE}
        WHERE STUDY_INSTANCE_UID = %s
        ORDER BY SERIES_INSTANCE_UID, INSTANCE_NUMBER NULLS LAST
        LIMIT {MAX_LIMIT}
    """
    return _rows(conn, sql, [study_uid])


def instance_metadata(conn, series_uid: str, sop_uid: str) -> list[dict]:
    """Metadata for a single instance. The third level PS3.18 10.4 requires."""
    sql = f"""
        SELECT SOP_INSTANCE_UID, RAW_METADATA, TRANSFER_SYNTAX_UID
        FROM {T_INSTANCE}
        WHERE SERIES_INSTANCE_UID = %s AND SOP_INSTANCE_UID = %s
        LIMIT 1
    """
    return _rows(conn, sql, [series_uid, sop_uid])


def series_frame_plan(conn, series_uid: str) -> list[dict]:
    """Everything needed to build a SeriesManifest, in one query.

    Joins V_FRAME_COVERAGE so unservable instances are visible as such rather than
    absent -- an instance with an unsupported transfer syntax must appear with
    IS_SERVABLE = FALSE, not silently vanish from the series.

    Runs under caller's rights, so an instance the caller may not see is already
    filtered by the row access policy before it reaches the frame path. This query
    is the reason offsets are never the authorization token: they are only ever
    produced as a side effect of a policy-enforced read.
    """
    sql = f"""
        SELECT i.SOP_INSTANCE_UID,
               i.FILE_NAME,
               i.TRANSFER_SYNTAX_UID,
               i.PIXEL_DATA_START,
               i.IMAGE_ROWS,
               i.IMAGE_COLUMNS,
               i.BITS_ALLOCATED,
               i.SAMPLES_PER_PIXEL,
               COALESCE(i.NUMBER_OF_FRAMES, 1) AS NUMBER_OF_FRAMES,
               c.RESOLUTION,
               c.IS_SERVABLE,
               c.IS_STALE
        FROM {T_INSTANCE} i
        JOIN {V_COVERAGE} c ON c.SOP_INSTANCE_UID = i.SOP_INSTANCE_UID
        WHERE i.SERIES_INSTANCE_UID = %s
        ORDER BY i.INSTANCE_NUMBER NULLS LAST
    """
    return _rows(conn, sql, [series_uid])


def stored_offsets(conn, sop_uids: list[str]) -> dict[str, list[tuple[int, int, int]]]:
    """Frame offsets for encapsulated instances only.

    Called with just the SOP UIDs whose RESOLUTION is STORED, so a corpus that is
    entirely native uncompressed never runs this query at all.
    """
    if not sop_uids:
        return {}
    placeholders = ", ".join(["%s"] * len(sop_uids))
    sql = f"""
        SELECT SOP_INSTANCE_UID, FRAME_NUMBER, FRAME_OFFSET, FRAME_LENGTH
        FROM DICOM_FRAME_OFFSET
        WHERE SOP_INSTANCE_UID IN ({placeholders})
        ORDER BY SOP_INSTANCE_UID, FRAME_NUMBER
    """
    out: dict[str, list[tuple[int, int, int]]] = {}
    for r in _rows(conn, sql, list(sop_uids)):
        out.setdefault(r["SOP_INSTANCE_UID"], []).append(
            (int(r["FRAME_NUMBER"]), int(r["FRAME_OFFSET"]), int(r["FRAME_LENGTH"]))
        )
    return out


def instance_locator(conn, sop_uid: str) -> dict | None:
    """Resolve one instance for WADO-RS instance retrieval."""
    sql = f"""
        SELECT i.SOP_INSTANCE_UID, i.FILE_NAME, i.FILE_SIZE, i.TRANSFER_SYNTAX_UID,
               i.SERIES_INSTANCE_UID, c.IS_SERVABLE
        FROM {T_INSTANCE} i
        JOIN {V_COVERAGE} c ON c.SOP_INSTANCE_UID = i.SOP_INSTANCE_UID
        WHERE i.SOP_INSTANCE_UID = %s
        LIMIT 1
    """
    rows = _rows(conn, sql, [sop_uid])
    return rows[0] if rows else None


def whoami(conn) -> dict:
    """The identity assertion. Used by /readyz and the deploy verification.

    If this returns the service owner rather than the radiologist, the
    Sf-Context-* headers are not reaching the gateway and every row access policy
    is passing. That failure is silent, so it needs an explicit probe.
    """
    rows = _rows(
        conn,
        "SELECT CURRENT_USER() AS USR, CURRENT_ROLE() AS ROLE, "
        "CURRENT_WAREHOUSE() AS WH, CURRENT_DATABASE() AS DB",
        [],
    )
    return rows[0] if rows else {}
