---
name: phase-field-map
description: Generate the field_maps.py entry for a new EDI transaction type
parent_skill: edi-extend
phase_id: P1
---

# Phase 1: Field Map Generation

## Purpose
Generate a Python dictionary entry for `field_maps.py` that maps EDI segments/elements to human-readable field names.

## Input Required (from Gate G3)
- `transaction_type.code` — e.g., "278"
- `transaction_type.name` — e.g., "Prior Authorization Request"
- `transaction_type.record_boundary_segment` — e.g., "HL"
- `transaction_type.gs_functional_id` — e.g., "HN"
- `transaction_type.fields` — initial field mappings (may be partial)

## Generation Logic

### 1. Build the FIELD_MAPS entry

```python
"278": {
    "transaction_type_name": "Prior Authorization Request",
    "record_boundary_segment": "HL",
    "fields": {
        "HL": {
            "01": "hierarchy_id",
            "02": "hierarchy_parent_id",
            "03": "hierarchy_level_code",
            "04": "hierarchy_child_code",
        },
        # ... additional segments
    },
},
```

### 2. Naming Conventions for Fields

Follow these patterns (derived from existing field maps):
- Entity names: `{role}_{attribute}` — e.g., `billing_provider_npi`, `subscriber_last_name`
- Dates: `{event}_date` — e.g., `service_date`, `coverage_start_date`
- Codes: `{thing}_code` — e.g., `claim_status_code`, `facility_type_code`
- Amounts: `{thing}_amount` — e.g., `claim_charge_amount`, `allowed_amount`
- Qualifiers: `{thing}_qualifier` — e.g., `provider_id_qualifier`

### 3. Qualifier-Aware Segment Keys

For segments that appear multiple times with different qualifier values:
- `NM1_85` = NM1 where element 01 is "85" (billing provider)
- `NM1_IL` = NM1 where element 01 is "IL" (subscriber/insured)
- `DTP_472` = DTP where element 01 is "472" (service date)
- `REF_EA` = REF where element 01 is "EA" (patient account number)

### 4. Add GS/ST Mappings

Add to `GS_FIC_TO_TRANSACTION`:
```python
"HN": "278",  # GS functional identifier code
```

Add to `ST_CODE_TO_TRANSACTION`:
```python
"278": "278",  # ST transaction set identifier code
```

## AI-Assisted Field Inference

If the user provided an implementation guide or description but incomplete field mappings, use Cortex AI to propose additional fields:

```
Based on the X12 {code} ({name}) standard, the following segments are typically present:
- HL (Hierarchical Level) — defines request/response hierarchy
- UM (Health Care Services Review Information) — authorization details
- HCR (Health Care Services Review) — decision/action codes
- ...

Shall I add mappings for these segments?
```

Mark AI-inferred fields clearly: `# [AI-inferred] verify against your implementation guide`

## Validation

If sample data is available:
1. Parse the sample using the generated field map
2. Report which fields populated vs. empty
3. Flag any unmapped segments found in the data

## Output

### Write Mode
Append the new entry to `field_maps.py` in the backbone repo.

### Dry-Run Mode
Output the complete Python dict as a fenced code block:
```python
# Add this to FIELD_MAPS in src/x12_processors/field_maps.py
"278": {
    ...
}

# Add to GS_FIC_TO_TRANSACTION:
"HN": "278",

# Add to ST_CODE_TO_TRANSACTION:
"278": "278",
```

## User Confirmation Required
Present the generated field map and ask: "Does this look correct? Any fields to add/remove/rename?"
