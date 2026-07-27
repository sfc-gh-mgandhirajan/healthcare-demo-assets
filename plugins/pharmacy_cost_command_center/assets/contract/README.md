# Canonical Logical Contract

This directory holds the **accelerator-owned** contract — the reusable asset that makes the
plugin portable across customers. It is the make-or-break piece: the logical elements,
relationships, and vocabulary the solution needs, independent of any one source schema.

## Two-layer model

| Layer | Owner | Where |
|---|---|---|
| Logical contract (what the solution needs) | Accelerator (ships in this plugin) | this directory |
| Physical binding (which of the customer's columns fill it) | Customer (produced at runtime) | the 3 `ACCELERATOR_CORE_*` tables in the customer's target schema |

## Files

- `conformance_tables.sql` — DDL for the 3 conformance tables (parameterized by `{{TARGET_DATABASE}}.{{TARGET_SCHEMA}}`). Elements carry `LOGICAL_OBJECT` + `TRANSFORM_EXPRESSION`; relationships carry `REL_KIND` (equi|expression|temporal); all carry `MAPPING_STATUS` (`PROPOSED` | `APPROVED`). Note: **`ACCELERATOR_CORE_ELEMENTS.ELEMENT_ID` == the contract `LOGICAL_ID`** (copy verbatim).
- `logical_objects.json` — the canonical logical objects the accelerator reasons over, each `required`/optional. Templates gate on presence; the readiness gate enforces required objects.
- `logical_elements.json` — the logical elements (`LOGICAL_ID`, label, `LOGICAL_OBJECT`, tier, data type, `IS_REQUIRED`) — the contract minus physical bindings.
- `logical_relationships.json` — the canonical join graph: each relationship's endpoints as logical objects + logical element keys (`LEFT_ELEMENT`/`RIGHT_ELEMENT`), `CARDINALITY`, `REL_KIND`, and `REQUIRED` (set minimally — only joins whose both endpoints are required objects). Every relationship id the templates reference via `join('...')` is defined here.
- `canonical_vocabulary.json` — the canonical values coded elements crosswalk to (e.g., GENERIC/BRAND, ON_FORMULARY/OFF_FORMULARY), with semantic role.

## Reference instantiation (golden fixture)

The reference source `AA_HC_PAYER_CORE_DATA_PRODUCTS` already has a fully populated, hand-vetted
instantiation of these 3 tables in
`AA_HC_PAYER_DERIVED_DATA_PRODUCTS.PHARMACY_COST_INTELLIGENCE`
(122 elements, 20 relationships, 49 crosswalk rows). During development this serves as:

1. The **input** the Phase 3 build engine binds to (validate the engine before discovery exists).
2. The **golden output** Phase 1 discovery is measured against (can we regenerate ~this from the raw source?).

The logical contract JSON files (M5) are the target-agnostic generalization of that reference instantiation.
