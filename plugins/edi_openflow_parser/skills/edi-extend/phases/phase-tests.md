---
name: phase-tests
description: Generate pytest stubs and sample EDI data for the new transaction type
parent_skill: edi-extend
phase_id: P4
---

# Phase 4: Test Stub Generation

## Purpose
Generate test files that validate the new field map parses correctly. This includes sample EDI data and pytest test cases.

## Input Required (from Phase 1-3)
- Transaction type code and name
- Complete field map
- Record boundary segment
- Delimiter configuration

## Generation Logic

### 1. Sample EDI Data

Generate a minimal but valid EDI file for the new transaction type:

```
ISA*00*          *00*          *ZZ*SENDER         *ZZ*RECEIVER       *230101*1200*^*00501*000000001*0*P*:~
GS*{gs_fic}*SENDER*RECEIVER*20230101*1200*1*X*005010X{version}~
ST*{st_code}*0001~
{boundary_segment}*{sample_data}~
{additional_segments}~
SE*{segment_count}*0001~
GE*1*1~
IEA*1*000000001~
```

Key rules:
- ISA is always exactly 106 characters (fixed width)
- Element separator: `*` (ISA position 3)
- Sub-element separator: `:` (ISA position 104)
- Segment terminator: `~` (ISA position 105)
- Generate at least 2 records (2x boundary segment occurrences)

### 2. Pytest Test File

```python
"""Tests for {tx_code} ({tx_name}) parsing."""
import json
import pytest
from x12_processors.ParseX12ToJSON import ParseX12ToJSON
from x12_processors.field_maps import FIELD_MAPS


# Sample data path
SAMPLE_FILE = "tests/sample_data/sample_{tx_code}.x12"


class Test{TxCode}Parsing:
    """Test {tx_name} field extraction."""

    @pytest.fixture
    def parser(self):
        processor = ParseX12ToJSON()
        return processor

    @pytest.fixture
    def sample_data(self):
        with open(SAMPLE_FILE, "r") as f:
            return f.read()

    def test_field_map_exists(self):
        """Verify {tx_code} field map is registered."""
        assert "{tx_code}" in FIELD_MAPS
        assert FIELD_MAPS["{tx_code}"]["record_boundary_segment"] == "{boundary}"

    def test_record_count(self, parser, sample_data):
        """Verify correct number of records extracted."""
        result = parser._parse_x12(sample_data)
        records = json.loads(result)
        assert len(records) == 2  # sample has 2 records

    def test_primary_fields_populated(self, parser, sample_data):
        """Verify key fields are extracted."""
        result = parser._parse_x12(sample_data)
        records = json.loads(result)
        record = records[0]
        
        # Primary identifier should be present
        assert "{primary_field}" in record
        assert record["{primary_field}"] is not None

    def test_qualifier_aware_mapping(self, parser, sample_data):
        """Verify qualifier-based segment keys resolve correctly."""
        result = parser._parse_x12(sample_data)
        records = json.loads(result)
        record = records[0]
        
        # Qualifier-aware fields should map to correct names
        # e.g., NM1 with qualifier "85" → billing_provider_*
        {qualifier_assertions}

    def test_envelope_fields(self, parser, sample_data):
        """Verify envelope metadata is captured."""
        result = parser._parse_x12(sample_data)
        records = json.loads(result)
        record = records[0]
        
        assert "isa_sender_id" in record
        assert "gs_date" in record
        assert "st_control_number" in record
```

### 3. File Locations

- Sample data: `tests/sample_data/sample_{tx_code}.x12`
- Test file: `tests/test_{tx_code}.py`

## Validation

If in write mode with a working repo:
1. Write the files
2. Run `pytest tests/test_{tx_code}.py -v` to verify tests at least collect (they'll fail until field_maps.py is updated)
3. Report results

## Output

### Write Mode
- Write sample .x12 file to `tests/sample_data/`
- Write test file to `tests/`

### Dry-Run Mode
- Output both files as fenced code blocks

## User Confirmation Required
Present the test file and sample data. Ask: "Does this sample data look realistic for your implementation? Should I adjust any test assertions?"
