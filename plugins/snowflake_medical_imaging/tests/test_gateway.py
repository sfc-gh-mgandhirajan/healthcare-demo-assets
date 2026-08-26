"""Unit tests for the two places a silent wrong answer costs most.

=============================================================================
WHY THESE TWO
=============================================================================

Most bugs in this system announce themselves -- a bad SQL statement fails, a
missing file raises FileNotFoundError. Two areas do not:

  1. FRAME OFFSET ARITHMETIC. An off-by-one returns the NEIGHBOURING SLICE at
     HTTP 200. On a CT that is different anatomy, rendered convincingly. No
     exception, no log line, nothing for a clinician to notice except that the
     image is of the wrong place.

  2. THE QIDO FILTER COMPILER. It decides which column references can ever reach
     generated SQL, and it translates DICOM wildcards. A mistake there either
     widens the query surface (security) or silently drops a filter, which makes
     a client believe it narrowed a search when it did not.

Run:  .venv/bin/python -m pytest tests/test_gateway.py -q
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "assets/dicomweb/gateway"))

# frames.py imports nothing from Snowflake, so it is importable directly.
from voxel import frames  # noqa: E402


# ===========================================================================
# Frame offset arithmetic
# ===========================================================================
class TestArithmeticOffsets:
    """The closed-form path: offset(n) = start + n * rows*cols*(bits/8)*samples."""

    @staticmethod
    def _manifest(num_frames=10, start=1000, rows=512, cols=512, bits=16, samples=1):
        m = frames.SeriesManifest("1.2.3")
        m.add_arithmetic(
            sop_uid="sop1", path="a/b.dcm", syntax="1.2.840.10008.1.2.1",
            pixel_data_start=start, rows=rows, cols=cols,
            bits_allocated=bits, samples=samples, num_frames=num_frames,
        )
        return m

    def test_first_frame_is_at_pixel_data_start(self):
        """Frame 1 must be AT pixel_data_start, not one frame past it."""
        m = self._manifest()
        assert m.frame("sop1", 1).offset == 1000

    def test_frame_numbering_is_one_based(self):
        """DICOMweb /frames/1 is the FIRST frame. Frame 0 does not exist.

        If this ever returns a FrameRef for 0, every frame in the viewer shifts by
        one slice and nothing errors.
        """
        m = self._manifest()
        assert m.frame("sop1", 0) is None
        assert m.frame("sop1", -1) is None

    def test_successive_frames_advance_by_exactly_one_frame(self):
        m = self._manifest(rows=512, cols=512, bits=16, samples=1)
        expected = 512 * 512 * 2 * 1
        f1, f2 = m.frame("sop1", 1), m.frame("sop1", 2)
        assert f2.offset - f1.offset == expected
        assert f1.length == expected

    def test_out_of_range_frame_returns_none_not_garbage(self):
        """Reading past the last frame must refuse, not return a plausible ref."""
        m = self._manifest(num_frames=10)
        assert m.frame("sop1", 10) is not None
        assert m.frame("sop1", 11) is None
        assert m.frame("sop1", 9999) is None

    def test_unknown_instance_returns_none(self):
        m = self._manifest()
        assert m.frame("does-not-exist", 1) is None

    @pytest.mark.parametrize("bits,samples,expected", [
        (8, 1, 512 * 512 * 1),        # 8-bit grayscale
        (16, 1, 512 * 512 * 2),       # 16-bit CT, the common case
        (8, 3, 512 * 512 * 1 * 3),    # RGB
        (16, 3, 512 * 512 * 2 * 3),   # 16-bit colour
    ])
    def test_frame_size_across_pixel_formats(self, bits, samples, expected):
        m = self._manifest(bits=bits, samples=samples)
        assert m.frame("sop1", 1).length == expected

    def test_identity_holds_against_real_geometry(self):
        """The identity asserted continuously in CHECKS as FRAME_ARITHMETIC_IDENTITY.

        Measured on a real IDC file: pixel_data_start 3222, 512x512, 16-bit,
        1 sample, 1 frame -> file size 527510.
        """
        m = self._manifest(num_frames=1, start=3222, rows=512, cols=512,
                           bits=16, samples=1)
        f = m.frame("sop1", 1)
        assert f.offset + f.length == 527510

    def test_frame_count_reported_correctly(self):
        m = self._manifest(num_frames=37)
        assert m.frame_count("sop1") == 37


class TestStoredOffsets:
    """The encapsulated path: offsets discovered at ingest, loaded as a packed array."""

    def test_stored_offsets_round_trip(self):
        m = frames.SeriesManifest("1.2.3")
        rows = [(1, 100, 50), (2, 150, 60), (3, 210, 70)]
        m.add_stored("sop1", "a/b.dcm", "1.2.840.10008.1.2.4.90", rows)
        for frame_no, off, ln in rows:
            ref = m.frame("sop1", frame_no)
            assert (ref.offset, ref.length) == (off, ln)

    def test_gap_in_stored_offsets_refuses_rather_than_shifting(self):
        """A missing frame must NOT shift every later frame.

        Frames are packed at index frame_number-1, so a gap becomes a zero-length
        entry that frame() rejects. Appending in arrival order instead would serve
        frame 3's bytes for frame 2 -- wrong anatomy, HTTP 200.
        """
        m = frames.SeriesManifest("1.2.3")
        m.add_stored("sop1", "a/b.dcm", "1.2.840.10008.1.2.4.90",
                     [(1, 100, 50), (3, 210, 70)])   # frame 2 missing
        assert m.frame("sop1", 1).offset == 100
        assert m.frame("sop1", 2) is None            # refused, not shifted
        assert m.frame("sop1", 3).offset == 210      # still correct

    def test_zero_length_entry_is_refused(self):
        m = frames.SeriesManifest("1.2.3")
        m.add_stored("sop1", "a/b.dcm", "1.2.840.10008.1.2.4.90", [(1, 100, 0)])
        assert m.frame("sop1", 1) is None

    def test_empty_stored_rows_is_not_an_error(self):
        m = frames.SeriesManifest("1.2.3")
        m.add_stored("sop1", "a/b.dcm", "1.2.840.10008.1.2.4.90", [])
        assert m.frame("sop1", 1) is None

    def test_packed_array_is_12_bytes_per_frame(self):
        """The storage claim: 12 bytes/frame vs the incumbent's documented ~1.7 KB/entry."""
        m = frames.SeriesManifest("1.2.3")
        m.add_arithmetic("sop1", "a.dcm", "1.2.840.10008.1.2.1",
                         1000, 512, 512, 16, 1, 100)
        assert m.nbytes() == 12 * 100


class TestCoalesce:
    """Span merging for frame lists. Wrong merging reads the wrong bytes."""

    def test_contiguous_frames_coalesce(self):
        refs = [frames.FrameRef(1000 + i * 100, 100) for i in range(5)]
        span = frames.coalesce(refs)
        assert span == (1000, 500)

    def test_sparse_frames_refuse_to_coalesce(self):
        """Reading a huge gap to serve two small frames is worse than two reads."""
        refs = [frames.FrameRef(1000, 100), frames.FrameRef(9_000_000, 100)]
        assert frames.coalesce(refs) is None

    def test_span_over_max_refuses(self):
        refs = [frames.FrameRef(0, 1), frames.FrameRef(frames.MAX_COALESCE_BYTES + 10, 1)]
        assert frames.coalesce(refs) is None

    def test_empty_list(self):
        assert frames.coalesce([]) is None

    def test_single_frame(self):
        assert frames.coalesce([frames.FrameRef(500, 42)]) == (500, 42)

    def test_unordered_input_still_spans_correctly(self):
        """A client may request frames out of order; the span must still cover all."""
        refs = [frames.FrameRef(1200, 100), frames.FrameRef(1000, 100),
                frames.FrameRef(1100, 100)]
        assert frames.coalesce(refs) == (1000, 300)


class TestManifestCache:
    def test_lru_evicts_oldest(self):
        c = frames.ManifestCache(max_series=2)
        for uid in ("a", "b", "c"):
            m = frames.SeriesManifest(uid)
            m.add_arithmetic("s", "p", "1.2.840.10008.1.2.1", 0, 8, 8, 8, 1, 1)
            c.put(m)
        assert c.get("a") is None      # evicted
        assert c.get("b") is not None
        assert c.get("c") is not None

    def test_get_refreshes_recency(self):
        """An actively scrolled series must not be evicted by background warming."""
        c = frames.ManifestCache(max_series=2)
        for uid in ("a", "b"):
            m = frames.SeriesManifest(uid)
            m.add_arithmetic("s", "p", "1.2.840.10008.1.2.1", 0, 8, 8, 8, 1, 1)
            c.put(m)
        c.get("a")                     # touch 'a' so 'b' is now oldest
        m = frames.SeriesManifest("c")
        m.add_arithmetic("s", "p", "1.2.840.10008.1.2.1", 0, 8, 8, 8, 1, 1)
        c.put(m)
        assert c.get("a") is not None
        assert c.get("b") is None


# ===========================================================================
# QIDO filter compiler
# ===========================================================================
# queries.py imports snowflake.connector only lazily at call time, but the module
# imports it at top level, so skip cleanly if it is absent rather than failing.
queries = pytest.importorskip(
    "voxel.queries", reason="snowflake-connector-python not installed"
)


class TestFilterAllowlist:
    def test_known_attribute_compiles(self):
        where, binds = queries.build_filters({"Modality": "CT"})
        assert len(where) == 1 and binds == ["CT"]

    def test_unknown_attribute_is_rejected_not_ignored(self):
        """Silently dropping a filter is worse than refusing it.

        The client believes it narrowed the search and may treat a broader result
        set as authoritative.
        """
        with pytest.raises(queries.BadQuery):
            queries.build_filters({"PatientWeight": "70"})

    def test_injection_attempt_is_rejected_as_unknown_attribute(self):
        """The defence is the allowlist, not escaping.

        An attribute name is never interpolated, so this fails at the allowlist
        check rather than relying on quoting.
        """
        with pytest.raises(queries.BadQuery):
            queries.build_filters({"Modality; DROP TABLE DICOM_INSTANCE--": "x"})

    def test_hex_tag_form_is_accepted(self):
        """Conformance tooling sends gggg,eeee rather than keywords."""
        for form in ("00080060", "0008,0060", "(0008,0060)"):
            where, binds = queries.build_filters({form: "CT"})
            assert binds == ["CT"], form

    def test_every_generated_column_is_from_the_allowlist(self):
        """The set of column references that can ever be produced is finite."""
        allowed = {c for c, _ in queries.QIDO_ATTRIBUTES.values()}
        for kw in queries.QIDO_ATTRIBUTES:
            where, _ = queries.build_filters({kw: "x"})
            assert any(col in where[0] for col in allowed)

    def test_pagination_params_are_not_treated_as_filters(self):
        where, binds = queries.build_filters(
            {"limit": "10", "offset": "5", "includefield": "all"})
        assert where == [] and binds == []

    def test_empty_value_is_skipped(self):
        where, binds = queries.build_filters({"Modality": ""})
        assert where == [] and binds == []


class TestValueBinding:
    def test_values_are_always_bound_never_interpolated(self):
        where, binds = queries.build_filters({"Modality": "CT"})
        assert "%s" in where[0]
        assert "CT" not in where[0]

    def test_overlong_value_rejected(self):
        with pytest.raises(queries.BadQuery):
            queries.build_filters({"StudyDescription": "A" * 500})

    def test_illegal_characters_rejected(self):
        """Characters with no legitimate use in a DICOM query value are refused.

        The character cap is a DEFENCE IN DEPTH measure, not the injection
        defence -- values are always bound, never interpolated. Its real purpose is
        to stop a pathological LIKE pattern being used as a denial-of-service
        against the catalog.
        """
        for bad in ("a'b", 'a"b', "a;b", "a\\b", "a\x00b", "a\nb"):
            with pytest.raises(queries.BadQuery):
                queries.build_filters({"StudyDescription": bad})

    def test_hyphen_is_allowed_and_that_is_correct(self):
        """'--' is SQL comment syntax but is NOT rejected, deliberately.

        An earlier version of this test asserted that "a--b" should be refused,
        reasoning by analogy with SQL injection filters. That reasoning is wrong
        here on two counts:

          1. The value is a BOUND PARAMETER. It is never parsed as SQL, so comment
             syntax has no meaning in it.
          2. Hyphens are legitimate and common in real DICOM values -- hyphenated
             patient names, formatted dates, and date ranges such as
             '20200101-20201231'.

        Rejecting them would break valid clinical queries in exchange for no
        security benefit. Pinned as a test so nobody "hardens" it back.
        """
        where, binds = queries.build_filters({"StudyDescription": "a--b"})
        assert binds == ["a--b"]
        assert "%s" in where[0]


class TestUidHandling:
    def test_uid_matched_exactly_never_with_like(self):
        """A wildcard UID search is a catalog enumeration primitive.

        DICOM also defines no wildcard matching for UI value representations.
        """
        where, binds = queries.build_filters({"StudyInstanceUID": "1.2.3*"})
        assert "LIKE" not in where[0].upper()
        assert binds == ["1.2.3*"]     # bound literally, matches nothing

    def test_all_uid_attributes_are_exact(self):
        for kw, (_, is_uid) in queries.QIDO_ATTRIBUTES.items():
            if is_uid:
                where, _ = queries.build_filters({kw: "1.2.3*"})
                assert "LIKE" not in where[0].upper(), kw


class TestWildcardTranslation:
    def test_dicom_star_becomes_sql_percent(self):
        assert queries._dicom_wildcard_to_sql("CH*") == "CH%"

    def test_dicom_question_becomes_underscore(self):
        assert queries._dicom_wildcard_to_sql("C?") == "C_"

    def test_sql_metacharacters_escaped_before_translation(self):
        """ORDER MATTERS.

        '%' and '_' are SQL wildcards but LITERAL in DICOM, so they must be
        escaped BEFORE '*' and '?' are translated. Doing it the other way round
        makes a DICOM '*' become '%' and then escapes it back to a literal,
        silently breaking wildcard search.
        """
        assert queries._dicom_wildcard_to_sql("50%") == r"50\%"
        assert queries._dicom_wildcard_to_sql("a_b") == r"a\_b"
        # A literal percent AND a DICOM wildcard in one value.
        assert queries._dicom_wildcard_to_sql("50%*") == r"50\%%"

    def test_wildcard_produces_like_predicate(self):
        where, binds = queries.build_filters({"SeriesDescription": "CHEST*"})
        assert "LIKE" in where[0].upper()
        assert binds == ["CHEST%"]

    def test_no_wildcard_produces_equality(self):
        where, binds = queries.build_filters({"SeriesDescription": "CHEST"})
        assert "LIKE" not in where[0].upper()


class TestLimits:
    def test_limit_clamped_to_ceiling(self):
        assert queries.clamp_limit({"limit": "999999"}) == queries.MAX_LIMIT

    def test_limit_applied_when_client_omits_it(self):
        """An unbounded QIDO query is a self-inflicted outage."""
        assert queries.clamp_limit({}) == queries.DEFAULT_LIMIT

    def test_limit_minimum_is_one(self):
        assert queries.clamp_limit({"limit": "0"}) == 1
        assert queries.clamp_limit({"limit": "-5"}) == 1

    def test_non_integer_limit_rejected(self):
        with pytest.raises(queries.BadQuery):
            queries.clamp_limit({"limit": "abc"})

    def test_negative_offset_clamped(self):
        assert queries.clamp_offset({"offset": "-10"}) == 0


# ===========================================================================
# DICOM JSON shaping
# ===========================================================================
dicomweb = pytest.importorskip(
    "voxel.dicomweb", reason="fastapi not installed"
)


class TestDicomJsonTypes:
    """Numeric VRs must be JSON NUMBERS.

    This is the bug that made OHIF render images but silently disable MPR and 3D:
    samplesPerPixel !== 1 is true when the value is the string "1".
    """

    def test_integer_vr_emitted_as_number(self):
        t = dicomweb._tag("512", "US")
        assert t["Value"] == [512] and isinstance(t["Value"][0], int)

    def test_decimal_vr_emitted_as_number(self):
        t = dicomweb._tag("0.625", "DS")
        assert t["Value"] == [0.625] and isinstance(t["Value"][0], float)

    def test_string_vr_stays_string(self):
        assert dicomweb._tag("CT", "CS")["Value"] == ["CT"]

    def test_absent_value_is_absent_key_not_null(self):
        """A DICOM JSON consumer treats present-but-null differently from absent."""
        assert dicomweb._tag(None, "CS") is None
        assert dicomweb._tag("", "CS") is None

    def test_unparseable_numeric_returns_none_rather_than_a_string(self):
        """Better to omit than to emit a string where a number is required."""
        assert dicomweb._tag("not-a-number", "US") is None

    def test_multivalued_backslash_string_splits(self):
        assert dicomweb._split("0.625\\0.625") == ["0.625", "0.625"]
        assert dicomweb._split(None) is None

    def test_pixel_spacing_round_trips_as_numbers(self):
        row = {"PIXEL_SPACING": "0.625\\0.625"}
        t = dicomweb._tag(dicomweb._split(row["PIXEL_SPACING"]), "DS")
        assert t["Value"] == [0.625, 0.625]


class TestFrameListParsing:
    def test_single_frame(self):
        assert dicomweb._parse_frame_list("1") == [1]

    def test_comma_separated(self):
        assert dicomweb._parse_frame_list("1,2,3") == [1, 2, 3]

    def test_frame_zero_rejected(self):
        """Frame 0 does not exist in DICOM."""
        with pytest.raises(Exception):
            dicomweb._parse_frame_list("0")

    def test_non_numeric_rejected(self):
        with pytest.raises(Exception):
            dicomweb._parse_frame_list("1,abc")

    def test_empty_rejected(self):
        with pytest.raises(Exception):
            dicomweb._parse_frame_list("")

    def test_unbounded_request_rejected(self):
        """An unbounded frame list is how one request outlives the 90s timeout."""
        with pytest.raises(Exception):
            dicomweb._parse_frame_list(",".join(str(i) for i in range(1, 600)))


class TestMultipart:
    def test_parts_carry_transfer_syntax_and_length(self):
        body = b"".join(dicomweb._multipart(
            [(b"abc", "image/jp2; transfer-syntax=1.2.840.10008.1.2.4.90", "/loc")],
            "BOUNDARY"))
        assert b"--BOUNDARY" in body
        assert b"Content-Type: image/jp2; transfer-syntax=1.2.840.10008.1.2.4.90" in body
        assert b"Content-Length: 3" in body
        assert b"Content-Location: /loc" in body
        assert body.endswith(b"--BOUNDARY--\r\n")

    def test_every_part_is_emitted(self):
        parts = [(bytes([i]) * 10, "application/octet-stream", f"/f/{i}")
                 for i in range(5)]
        body = b"".join(dicomweb._multipart(parts, "B"))
        assert body.count(b"--B\r\n") == 5

    def test_generator_yields_incrementally(self):
        """Parts must reach the wire as produced, or the 90s idle timer fires."""
        gen = dicomweb._multipart(
            [(b"x", "application/octet-stream", "")] * 3, "B")
        first = next(gen)
        assert b"--B" in first          # produced without consuming the whole body

    def test_native_syntax_maps_to_octet_stream(self):
        assert dicomweb.TRANSFER_SYNTAX_MIME["1.2.840.10008.1.2.1"] == "application/octet-stream"

    def test_compressed_syntaxes_map_to_real_media_types(self):
        assert dicomweb.TRANSFER_SYNTAX_MIME["1.2.840.10008.1.2.4.90"] == "image/jp2"
        assert dicomweb.TRANSFER_SYNTAX_MIME["1.2.840.10008.1.2.5"] == "image/dicom-rle"


class TestNativeSyntaxSet:
    def test_native_set_matches_capability_table(self):
        """frames.NATIVE_SYNTAXES duplicates DICOM_CAPABILITY.IS_ENCAPSULATED=FALSE.

        Deliberate duplication: encapsulation is a fact of the standard, and the
        read path must not need a SQL round trip to classify a file. But the two
        must agree, so this pins the set.
        """
        assert frames.NATIVE_SYNTAXES == {
            "1.2.840.10008.1.2",      # Implicit VR Little Endian
            "1.2.840.10008.1.2.1",    # Explicit VR Little Endian
            "1.2.840.10008.1.2.2",    # Explicit VR Big Endian
        }

    def test_compressed_syntaxes_are_not_native(self):
        for uid in ("1.2.840.10008.1.2.5", "1.2.840.10008.1.2.4.90",
                    "1.2.840.10008.1.2.4.50"):
            assert uid not in frames.NATIVE_SYNTAXES


class TestNoUndefinedNames:
    """Catch deleted module constants before they reach production.

    WHY THIS EXISTS: refactoring queries.py twice removed module-level constants
    that only one code path referenced -- QIDO_ATTRIBUTES, DEFAULT_LIMIT,
    MAX_LIMIT, then V_COVERAGE. Each survived import, survived every unit test
    that did not happen to touch that path, and failed as a NameError inside a
    live request. V_COVERAGE reached a deployed container and 500'd WADO frame
    retrieval while QIDO passed.

    An f-string SQL builder makes this worse than usual: `f"... {V_COVERAGE} ..."`
    is only evaluated when that query runs, so the reference is invisible to
    import-time checks and to any test that does not exercise the query.

    This walks the AST of every gateway module and asserts that every loaded name
    resolves to a module-level definition, an import, a builtin, or a local. It is
    a cheap substitute for the type checker this project does not run.
    """

    GATEWAY_MODULES = ["queries.py", "dicomweb.py", "auth.py", "frames.py", "app.py"]

    def _undefined(self, path):
        """Names loaded somewhere in the module but bound nowhere in it.

        DELIBERATELY AN OVER-APPROXIMATION OF WHAT IS "BOUND". Every Store target,
        argument, import, except-alias and comprehension variable anywhere in the
        file counts, regardless of scope. So this will NOT catch a name that is
        bound in one function and read in another.

        That imprecision is the point. A scope-accurate version flagged closures --
        `_stream` nested inside `wado_instance`, and the enclosing `boundary`, `fd`,
        `syntax` it captures -- as undefined. A guard with false positives gets
        deleted. This one has none, and it still catches the bug it exists for: a
        module constant that was deleted and is now bound nowhere at all, which is
        how QIDO_ATTRIBUTES, DEFAULT_LIMIT, MAX_LIMIT and V_COVERAGE each escaped.
        """
        import ast
        import builtins

        tree = ast.parse(path.read_text())
        bound = set(dir(builtins))
        for node in ast.walk(tree):
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
                bound.add(node.id)
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                bound.add(node.target.id)
            elif isinstance(node, ast.arg):
                bound.add(node.arg)
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                bound.add(node.name)
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                for alias in node.names:
                    bound.add((alias.asname or alias.name).split(".")[0])
            elif isinstance(node, ast.ExceptHandler) and node.name:
                bound.add(node.name)
            elif isinstance(node, ast.Global):
                bound.update(node.names)

        return {
            n.id
            for n in ast.walk(tree)
            if isinstance(n, ast.Name)
            and isinstance(n.ctx, ast.Load)
            and n.id not in bound
        }

    def test_no_undefined_module_names(self):
        import pathlib

        root = pathlib.Path(__file__).resolve().parents[1] / "assets/dicomweb/gateway/voxel"
        problems = {}
        for mod in self.GATEWAY_MODULES:
            path = root / mod
            if not path.exists():
                continue
            found = self._undefined(path)
            if found:
                problems[mod] = sorted(found)
        assert not problems, (
            "undefined names referenced (a deleted constant or a typo):\n"
            + "\n".join(f"  {m}: {n}" for m, ns in problems.items() for n in ns)
        )


class TestBitPackedFrameArithmetic:
    """DICOM-SEG BINARY is 1 bit per pixel. That broke the offset formula.

    The original arithmetic was `rows * cols * (bits_allocated // 8) * samples`,
    which is correct for the 8- and 16-bit syntaxes in the original corpus and
    computes ZERO for bits_allocated=1. Every DICOM-SEG frame became a zero-length
    read served as HTTP 200 with an empty part.

    Worse, the SQL side of the same formula used `(BITS_ALLOCATED / 8)`, which in
    Snowflake is 0.125 rather than 0. So V_FRAME_COVERAGE declared these instances
    servable while the gateway could not serve them -- two implementations of one
    formula disagreeing, with no error on either side.
    """

    def _manifest(self):
        from voxel.frames import SeriesManifest

        return SeriesManifest("series-under-test")

    def test_binary_seg_frame_is_one_bit_per_pixel(self):
        m = self._manifest()
        m.add_arithmetic("seg", "p", "1.2.840.10008.1.2.1",
                         1000, 512, 512, 1, 1, 2295)
        assert m.frame("seg", 1).length == 512 * 512 // 8 == 32768

    def test_binary_seg_frames_are_contiguous(self):
        """No inter-frame padding for bit-packed data (PS3.3)."""
        m = self._manifest()
        m.add_arithmetic("seg", "p", "1.2.840.10008.1.2.1",
                         1000, 512, 512, 1, 1, 2295)
        assert m.frame("seg", 2).offset == 1000 + 32768
        assert m.frame("seg", 2295).offset == 1000 + 2294 * 32768

    def test_sixteen_bit_ct_unchanged(self):
        """The fix must not perturb the syntaxes that already worked."""
        m = self._manifest()
        m.add_arithmetic("ct", "p", "1.2.840.10008.1.2.1",
                         500, 512, 512, 16, 1, 10)
        assert m.frame("ct", 1).length == 512 * 512 * 2
        assert m.frame("ct", 2).offset == 500 + 512 * 512 * 2

    def test_rgb_eight_bit_unchanged(self):
        m = self._manifest()
        m.add_arithmetic("rgb", "p", "1.2.840.10008.1.2.1",
                         0, 64, 64, 8, 3, 4)
        assert m.frame("rgb", 1).length == 64 * 64 * 3

    def test_unaligned_bit_packing_is_refused_not_shifted(self):
        """os.pread cannot start mid-byte, so refuse rather than serve shifted.

        3x3 at 1 bit is 9 bits per frame, so frame 2 would begin one bit into a
        byte. Serving that would return a mask shifted by a bit -- plausible
        looking, quietly wrong.
        """
        import pytest

        m = self._manifest()
        with pytest.raises(ValueError, match="not byte-aligned"):
            m.add_arithmetic("bad", "p", "1.2.840.10008.1.2.1",
                             0, 3, 3, 1, 1, 2)
