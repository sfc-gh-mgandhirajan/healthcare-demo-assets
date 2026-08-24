---
name: gate-format-spec
description: Validate the user's EDI format specification is parseable
parent_skill: edi-extend
gate_id: G3
---

# Gate: Format Specification Validation

## Purpose
Determine what EDI format the user wants to add and gather enough information to generate a field map.

## Steps

1. **Ask what format** (if not already stated):
   ```
   What X12 transaction type do you want to add?
   
   Examples:
   - X12 278 (Prior Authorization Request/Response)
   - X12 820 (Payment Order/Remittance)
   - X12 999 (Implementation Acknowledgment)
   - Custom X12 type (describe it)
   ```

2. **Check if already known**: Look in `config/x12_known_types.yaml`
   - If found: inform user this type is already supported, ask if they want to customize/extend it
   - If not found: proceed to specification gathering

3. **Gather specification** — one of three paths:

   **Path A: Implementation Guide (PDF/text)**
   - Ask user to provide or point to an implementation guide
   - Use Cortex AI to extract: segment structure, record boundary, field positions, qualifiers
   - Present extracted spec for validation

   **Path B: AI-Inferred**
   - Use knowledge of X12 standards + Cortex AI to propose a field map
   - Present proposed spec for validation
   - Note: mark generated fields as [AI-inferred] — user must confirm

   **Path C: Manual Entry**
   - Ask user to describe: record boundary segment, key segments and their fields
   - Build spec interactively

4. **Validate minimum requirements**:
   - Record boundary segment identified
   - At least 3 field mappings defined
   - GS Functional Identifier Code known (for X12)
   - ST transaction set code known (for X12)
   - Landing schema name chosen

## Pass Criteria
- Transaction type code identified
- Record boundary segment defined
- Field map has at least one segment with mapped fields
- Landing table target schema chosen

## Output
Produce a structured spec object to pass to phases:
```yaml
transaction_type:
  code: "278"
  name: "Prior Authorization Request"
  family: x12
  gs_functional_id: "HN"
  st_code: "278"
  record_boundary_segment: "HL"
  landing_schema: "AUTHORIZATIONS"
  landing_table: "LANDING_278_AUTHORIZATIONS"
  fields:
    HL:
      "01": hierarchy_id
      # ...
```
