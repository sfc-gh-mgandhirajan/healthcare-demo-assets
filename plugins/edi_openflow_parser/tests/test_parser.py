"""Tests for the X12 EDI parser.

Imports the REAL ParseX12ToJSON processor (with NiFi API stubbed) to ensure
tests validate the actual shipping code, not a fork.
"""
import json
import os
import sys
import types

# Stub the nifiapi module so ParseX12ToJSON can be imported without NiFi
nifiapi = types.ModuleType("nifiapi")
nifiapi_ft = types.ModuleType("nifiapi.flowfiletransform")
nifiapi_props = types.ModuleType("nifiapi.properties")


class _FakeFlowFileTransform:
    def __init__(self, **kwargs):
        pass


class _FakeResult:
    def __init__(self, *a, **kw):
        pass


class _FakePropertyDescriptor:
    def __init__(self, **kwargs):
        self.name = kwargs.get("name", "")
        self.default_value = kwargs.get("default_value", "")


class _FakeValidators:
    NON_EMPTY_VALIDATOR = None


class _FakeELS:
    FLOWFILE_ATTRIBUTES = None


nifiapi_ft.FlowFileTransform = _FakeFlowFileTransform
nifiapi_ft.FlowFileTransformResult = _FakeResult
nifiapi_props.PropertyDescriptor = _FakePropertyDescriptor
nifiapi_props.StandardValidators = _FakeValidators
nifiapi_props.ExpressionLanguageScope = _FakeELS

sys.modules["nifiapi"] = nifiapi
sys.modules["nifiapi.flowfiletransform"] = nifiapi_ft
sys.modules["nifiapi.properties"] = nifiapi_props

# Now import the real parser
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from x12_processors.ParseX12ToJSON import ParseX12ToJSON
from x12_processors.field_maps import FIELD_MAPS, ST_CODE_TO_TRANSACTION

SAMPLE_DIR = os.path.join(os.path.dirname(__file__), "sample_data")


def _parse(raw, allowed_types=None, include_raw=False, include_envelope=True):
    """Call the real parser's internal method."""
    processor = ParseX12ToJSON()
    return processor._parse_x12(raw, allowed_types, include_raw, include_envelope)


# === Core Tests ===

def test_delimiter_detection():
    with open(os.path.join(SAMPLE_DIR, "sample_837p.x12")) as f:
        raw = f.read()
    processor = ParseX12ToJSON()
    d = processor._detect_delimiters(raw)
    assert d is not None
    element_sep, sub_sep, segment_sep = d
    assert element_sep == "*"
    assert sub_sep == ":"
    assert segment_sep == "~"


def test_837p_parse():
    with open(os.path.join(SAMPLE_DIR, "sample_837p.x12")) as f:
        raw = f.read()
    records = _parse(raw)
    assert len(records) == 2, f"Expected 2 claims, got {len(records)}"

    r1 = records[0]
    assert r1["transaction_type"] == "837"
    assert r1["claim_id"] == "CLAIM001"
    assert r1["claim_amount"] == "250.00"
    assert r1["subscriber_last_name"] == "DOE"
    assert r1["billing_provider_npi"] == "1234567890"
    assert r1["interchange_sender_id"] == "PROVIDER123"


def test_834_parse():
    with open(os.path.join(SAMPLE_DIR, "sample_834.x12")) as f:
        raw = f.read()
    records = _parse(raw)
    assert len(records) == 2, f"Expected 2 members, got {len(records)}"
    assert records[0]["member_last_name"] == "DOE"
    assert records[1]["member_last_name"] == "SMITH"


def test_835_parse():
    with open(os.path.join(SAMPLE_DIR, "sample_835.x12")) as f:
        raw = f.read()
    records = _parse(raw)
    assert len(records) == 2, f"Expected 2 remittances, got {len(records)}"
    assert records[0]["claim_id"] == "CLM001"
    assert records[1]["claim_id"] == "CLM002"


def test_transaction_filter():
    with open(os.path.join(SAMPLE_DIR, "sample_837p.x12")) as f:
        raw = f.read()
    records = _parse(raw, allowed_types={"834"})
    assert len(records) == 0
    records = _parse(raw, allowed_types={"837"})
    assert len(records) == 2


def test_include_raw_segments():
    with open(os.path.join(SAMPLE_DIR, "sample_837p.x12")) as f:
        raw = f.read()
    records = _parse(raw, include_raw=True)
    assert "raw_segments" in records[0]
    assert len(records[0]["raw_segments"]) > 0


def test_exclude_envelope():
    with open(os.path.join(SAMPLE_DIR, "sample_837p.x12")) as f:
        raw = f.read()
    records = _parse(raw, include_envelope=False)
    assert "interchange_sender_id" not in records[0]
    assert records[0]["transaction_type"] == "837"


# === Blocker Regression Tests ===

def test_composite_codes_preserved():
    """Blocker 1a regression: composites must be preserved intact, not truncated to qualifier."""
    edi = (
        "ISA*00*          *00*          *ZZ*SENDER         *ZZ*RECEIVER       "
        "*230101*1200*^*00501*000000001*0*P*:~"
        "GS*HC*SENDER*RECEIVER*20230101*1200*1*X*005010X222A1~"
        "ST*837*0001~"
        "CLM*C001*500.00***11:B:1~"
        "HI*ABK:J0600*ABF:E1190~"
        "SV1*HC:99213:25*150.00*UN*1*11~"
        "SE*5*0001~GE*1*1~IEA*1*000000001~"
    )
    records = _parse(edi)
    assert len(records) == 1
    r = records[0]
    # Composite preserved intact — Gold layer extracts via SPLIT_PART
    assert "ABK:J0600" in r.get("diagnosis_code_1", ""), f"Got: {r.get('diagnosis_code_1')}"
    assert "HC:99213" in r.get("procedure_code", ""), f"Got: {r.get('procedure_code')}"


def test_multi_service_line_semicolon_joined():
    """Blocker 3 regression: repeated segments become semicolon-joined strings, not lists."""
    edi = (
        "ISA*00*          *00*          *ZZ*SENDER         *ZZ*RECEIVER       "
        "*230101*1200*^*00501*000000001*0*P*:~"
        "GS*HC*SENDER*RECEIVER*20230101*1200*1*X*005010X222A1~"
        "ST*837*0001~"
        "CLM*C001*500.00***11:B:1~"
        "SV1*HC:99213*150.00*UN*1*11~"
        "SV1*HC:99214*200.00*UN*1*11~"
        "SV1*HC:99215*150.00*UN*1*11~"
        "SE*6*0001~GE*1*1~IEA*1*000000001~"
    )
    records = _parse(edi)
    assert len(records) == 1
    r = records[0]
    # Must be a string (semicolon-joined), NOT a list
    proc = r.get("procedure_code", "")
    assert isinstance(proc, str), f"procedure_code is {type(proc)}, expected str"
    assert ";" in proc, f"Expected semicolons, got: {proc}"
    # All three codes present
    assert "HC:99213" in proc
    assert "HC:99214" in proc
    assert "HC:99215" in proc


def test_multi_subscriber_no_contamination():
    """Blocker 2 regression: claims from different subscribers must not mix patient data."""
    edi = (
        "ISA*00*          *00*          *ZZ*SENDER         *ZZ*RECEIVER       "
        "*230101*1200*^*00501*000000001*0*P*:~"
        "GS*HC*SENDER*RECEIVER*20230101*1200*1*X*005010X222A1~"
        "ST*837*0001~"
        # Subscriber 1
        "SBR*P*18*GRP001*PLAN A~"
        "NM1*IL*1*DOE*JOHN****MI*ABC123~"
        "DMG*D8*19800515*M~"
        "CLM*C001*250.00***11:B:1~"
        "HI*ABK:J0600~"
        # Subscriber 2
        "SBR*P*18*GRP002*PLAN B~"
        "NM1*IL*1*NGUYEN*TRAN****MI*XYZ999~"
        "DMG*D8*19720304*F~"
        "CLM*C002*175.00***11:B:1~"
        "HI*ABK:E1190~"
        "SE*12*0001~GE*1*1~IEA*1*000000001~"
    )
    records = _parse(edi)
    assert len(records) == 2, f"Expected 2 claims, got {len(records)}"

    # Each claim must have exactly one subscriber — no lists, no cross-contamination
    r1 = records[0]
    assert r1["claim_id"] == "C001"
    assert r1["subscriber_last_name"] == "DOE"
    assert isinstance(r1["subscriber_last_name"], str)
    assert "NGUYEN" not in str(r1.get("subscriber_last_name", ""))

    r2 = records[1]
    assert r2["claim_id"] == "C002"
    assert r2["subscriber_last_name"] == "NGUYEN"
    assert isinstance(r2["subscriber_last_name"], str)
    assert "DOE" not in str(r2.get("subscriber_last_name", ""))


def test_no_positional_fallback_for_known_types():
    """Known types should not emit generic positional keys (BHT_01, HL_01, etc.)."""
    with open(os.path.join(SAMPLE_DIR, "sample_837p.x12")) as f:
        raw = f.read()
    records = _parse(raw)
    r = records[0]
    generic_keys = [k for k in r if "_" in k and k.split("_")[-1].isdigit() and len(k.split("_")[-1]) == 2]
    assert len(generic_keys) == 0, f"Found positional fallback keys: {generic_keys}"


def test_malformed_input():
    processor = ParseX12ToJSON()
    result = processor._detect_delimiters("NOT AN X12 FILE")
    assert result is None


if __name__ == "__main__":
    tests = [fn for name, fn in sorted(globals().items()) if name.startswith("test_")]
    passed = failed = 0
    for test in tests:
        try:
            test()
            print(f"  PASS: {test.__name__}")
            passed += 1
        except Exception as e:
            print(f"  FAIL: {test.__name__}: {e}")
            failed += 1
    print(f"\n{passed}/{passed+failed} tests passed")
    if failed:
        sys.exit(1)
