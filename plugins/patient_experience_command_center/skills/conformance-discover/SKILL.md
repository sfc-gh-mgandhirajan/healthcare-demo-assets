---
name: conformance-discover
description: "Phase 1 of the Patient Experience Command Center deployment. Seeds the contract skeleton, then profiles the provider's Snowflake source and proposes mappings for the 3 conformance tables (elements, relationships, code crosswalk incl. the HCAHPS top-box crosswalk). Triggers: patient experience discover, conformance discover, map my source to patient experience accelerator, start patient experience command center, phase 1 patient experience."
---

# Phase 1 - Conformance Discovery

Map the provider's source objects to the accelerator's **canonical logical contract** by proposing rows for the three conformance tables. Assisted, not fully automatic: everything you write is `PROPOSED` and is finalized in Phase 2.

## Paths & connection (set once)
- `PLUGIN_DIR` - the plugin root (the directory containing `.cortex-plugin/`). The contract lives at `$PLUGIN_DIR/assets/contract/`.
- `CONN` - the Snowflake CLI connection name (`snow ... -c "$CONN"`).
- `CONFIG` - the customer's `config.json` (copy from `$PLUGIN_DIR/assets/config.sample.json` and edit).

## Naming seam (read first)
- The contract element id field is `LOGICAL_ID` (in `logical_elements.json`); the conformance column is `ELEMENT_ID`. **`ELEMENT_ID` == the contract `LOGICAL_ID`** - copy verbatim.
- `LOGICAL_OBJECT` on each element = the `objects[].id` it belongs to (`logical_objects.json`).
- Relationship ids + canonical endpoints come from `logical_relationships.json`.

## Inputs to collect (ask once, concisely)
- Target for the derived data product: `TARGET_DATABASE.TARGET_SCHEMA` (created if absent).
- Provider source databases/schemas to search (survey, comments, contact-center, grievances, callbacks, care-team, unit operations, digital, demographics, reference).
- Optional: the call-recordings stage.

## Steps
1. **Create the conformance tables** in `TARGET_DATABASE.TARGET_SCHEMA` (idempotent) using `$PLUGIN_DIR/assets/contract/conformance_tables.sql` with `{{TARGET_DATABASE}}`/`{{TARGET_SCHEMA}}` substituted (via `snow sql -c "$CONN"`): `ACCELERATOR_CORE_ELEMENTS`, `ACCELERATOR_CORE_RELATIONSHIPS`, `ACCELERATOR_CORE_CODE_CROSSWALK`.

2. **Seed the contract skeleton (MANDATORY - do NOT hand-seed).** Run the deterministic seeder so the descriptive columns (`ELEMENT_NAME`, `ELEMENT_TIER`, `LOGICAL_OBJECT`, `DATA_TYPE`, `IS_REQUIRED`, relationship metadata) are populated straight from the contract and can never be left NULL:
   ```bash
   python3 "$PLUGIN_DIR/assets/renderer/seed_contract.py" \
     --contract "$PLUGIN_DIR/assets/contract" \
     --database <TARGET_DATABASE> --schema <TARGET_SCHEMA> --out "$WORKDIR/seed.sql"
   snow sql -c "$CONN" -f "$WORKDIR/seed.sql"
   ```
   The seeder inserts one `PROPOSED` row per contract element + relationship with descriptive columns filled and **all discovery-owned columns (SOURCE_*, STAGE_PATH, FULLY_QUALIFIED_REF, SOURCE_GRAIN, TRANSFORM_*) left NULL**. It is idempotent (re-running only refreshes descriptive columns; never touches SOURCE_*/MAPPING_STATUS).

3. **Profile the provider source (read-only) and match.** Launch the `conformance-mapper` agent (or profile directly). It returns, per logical element, the best physical `SOURCE_OBJECT.SOURCE_COLUMN` (or `STAGE_PATH` for UNSTRUCTURED) with a confidence + notes. Never invent a column profiling did not confirm.

4. **Fill the physical mapping (discovery output).** For each matched element, `UPDATE` its seeded row with `SOURCE_DATABASE/SCHEMA/OBJECT/COLUMN` (or `STAGE_PATH`), `SOURCE_GRAIN`, optional `TRANSFORM_EXPRESSION` (`{a}` = alias) + `TRANSFORM_NOTES` (rationale + confidence), and set `FULLY_QUALIFIED_REF`. Leave genuinely unmatched required elements with NULL source (the gate flags them). Do NOT hand-write descriptive columns - the seeder owns those.

5. **Instantiate relationships.** For each `logical_relationships.json` entry whose BOTH endpoint objects matched, set physical `LEFT_OBJECT/LEFT_COLUMN` + `RIGHT_OBJECT/RIGHT_COLUMN` (from the matched key elements), carry `CARDINALITY`/`REL_KIND`, and set `JOIN_CONDITION`.

6. **Crosswalk coded values.** For each coded element map observed raw values to `canonical_vocabulary.json` (`SOURCE_VALUE -> CANONICAL_VALUE`, `SEMANTIC_ROLE`). **HCAHPS top-box:** for each item ordinal element, profile its distinct values and map top-box value(s) -> `TRUE` and the rest -> `FALSE` (`SEMANTIC_ROLE='TOPBOX'`), per the measure's top-box definition.

7. **Discovery report (do NOT build).** Summarize matched vs unmatched required elements/relationships, low-confidence matches, uncrosswalked coded values, and which OPTIONAL logical objects are absent (so the user knows which panels will `present()`-gate off).

## Hand-off
Tell the user to run `/patient-experience-command-center:conformance-review` to finalize.

> Keep proposals conservative; never invent a column. Everything here is `PROPOSED` - Phase 2 approves. The seeder (step 2) is what guarantees descriptive columns are populated; discovery only fills the physical mapping.
