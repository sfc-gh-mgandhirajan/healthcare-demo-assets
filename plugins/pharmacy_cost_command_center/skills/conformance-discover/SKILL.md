---
name: conformance-discover
description: "Phase 1 of the Pharmacy Cost Command Center deployment. Profiles the customer's Snowflake source and proposes rows for the 3 conformance tables (elements, relationships, code crosswalk) that bind their data to the accelerator's canonical contract. Triggers: pharmacy cost discover, conformance discover, map my source to pharmacy accelerator, start pharmacy cost command center, phase 1 pharmacy."
---

# Phase 1 - Conformance Discovery

Map the customer's source objects to the accelerator's **canonical logical contract** by proposing rows for the three conformance tables. Assisted, not fully automatic: everything you write is `PROPOSED` and is finalized in Phase 2.

## Paths & connection (set once)
Commands use shell variables so this works wherever the plugin is installed:
- `PLUGIN_DIR` - the plugin root (the directory containing `.cortex-plugin/`). The contract lives at `$PLUGIN_DIR/assets/contract/`.
- `CONN` - the Snowflake CLI connection name (`snow ... -c "$CONN"`).
- `CONFIG` - the customer's `config.json` (copy from `$PLUGIN_DIR/assets/config.sample.json` and edit).

## Naming seam (read first)
- The contract's element id field is `LOGICAL_ID` (in `logical_elements.json`); the conformance table column is `ELEMENT_ID`. **`ELEMENT_ID` == the contract `LOGICAL_ID`** - copy it verbatim.
- `LOGICAL_OBJECT` on each element = the `objects[].id` it belongs to (from `logical_objects.json`).
- Relationship ids and their canonical endpoints come from `logical_relationships.json`.

## Inputs to collect (ask once, concisely)
- Target for the derived data product: `TARGET_DATABASE.TARGET_SCHEMA` (created if absent).
- Customer source databases/schemas to search (pharmacy claims, medical claims, membership, clinical, UM, reference).
- Optional: coverage-policy document stage.

## The canonical contract (accelerator-owned, read from `$PLUGIN_DIR/assets/contract/`)
- `logical_elements.json` - business elements (`LOGICAL_ID`, `LABEL`, `TIER`, `LOGICAL_OBJECT`, `DATA_TYPE`, `IS_REQUIRED`).
- `logical_objects.json` - the logical objects + which are `required`.
- `logical_relationships.json` - the canonical join graph (endpoints as logical objects + logical element keys, `CARDINALITY`, `REQUIRED`).
- `canonical_vocabulary.json` - canonical values coded columns must crosswalk to.

## Steps
1. **Create the conformance tables** in `TARGET_DATABASE.TARGET_SCHEMA` (idempotent) using `$PLUGIN_DIR/assets/contract/conformance_tables.sql` with `{{TARGET_DATABASE}}`/`{{TARGET_SCHEMA}}` substituted (via `snow sql -c "$CONN"`):
   `ACCELERATOR_CORE_ELEMENTS`, `ACCELERATOR_CORE_RELATIONSHIPS`, `ACCELERATOR_CORE_CODE_CROSSWALK` (each has `MAPPING_STATUS` = `PROPOSED` | `APPROVED`).

2. **Seed the required skeleton (deterministic gate enabler).** Insert one `PROPOSED` row per **required** contract item so the Phase-2 gate is a pure structural check:
   - one `ACCELERATOR_CORE_ELEMENTS` row per element with `IS_REQUIRED = TRUE` in `logical_elements.json` (`ELEMENT_ID`, `ELEMENT_NAME`, `LOGICAL_OBJECT`, `IS_REQUIRED=TRUE`, `SOURCE_*` left NULL for now);
   - one `ACCELERATOR_CORE_RELATIONSHIPS` row per relationship with `REQUIRED = TRUE` in `logical_relationships.json` (`RELATIONSHIP_ID`, `LEFT_OBJECT`, `RIGHT_OBJECT`, `CARDINALITY`, `REL_KIND`, `IS_REQUIRED=TRUE`, columns NULL for now).
   Optional elements/relationships are added only when matched (step 4-5).

3. **Profile the customer source (read-only) and match.** Launch the `conformance-mapper` agent, or profile directly:
   - `SELECT * FROM <src_db>.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA IN (...)` for table/column names, data types, grains.
   - For coded columns, bounded `SELECT <col>, COUNT(*) ... GROUP BY 1 ORDER BY 2 DESC LIMIT 200` to enumerate raw values.
   The mapper returns, per logical element, the best physical `SOURCE_OBJECT.SOURCE_COLUMN` (or `STAGE_PATH` for UNSTRUCTURED `TIER`) with a confidence and notes. Never invent a column profiling did not confirm.

4. **Fill / add element rows.** For each matched element: UPDATE the seeded required row (or INSERT a new PROPOSED row for optional elements) with `SOURCE_DATABASE/SCHEMA/OBJECT/COLUMN` (or `STAGE_PATH`), `DATA_TYPE`, `SOURCE_GRAIN`, optional `TRANSFORM_EXPRESSION` (Tier-2 escape hatch; `{a}` = table alias), and `TRANSFORM_NOTES` (match rationale + confidence). Leave genuinely unmatched required elements with NULL source (the gate will flag them).

5. **Instantiate relationships.** For each `logical_relationships.json` entry whose BOTH endpoint objects were matched, set the physical `LEFT_OBJECT/LEFT_COLUMN` and `RIGHT_OBJECT/RIGHT_COLUMN` (from the matched elements for `LEFT_ELEMENT`/`RIGHT_ELEMENT`), carry `CARDINALITY`/`REL_KIND`, and set `JOIN_CONDITION`. Add optional relationships only when both endpoints exist.

6. **Crosswalk coded values.** For each coded element, map observed raw values to `canonical_vocabulary.json` (`SOURCE_VALUE -> CANONICAL_VALUE`, `SEMANTIC_ROLE`); flag any raw value with no canonical target.

7. **Discovery report (do NOT build).** Summarize: matched vs unmatched **required** elements/relationships, low-confidence matches, coded values still uncrosswalked, and which OPTIONAL logical objects are absent (so the user knows which arms will be `present()`-gated off).

## Hand-off
Tell the user to run `/pharmacy-cost-command-center:conformance-review` to finalize.

> Keep proposals conservative; never invent a column that does not exist in the customer source. Everything here is `PROPOSED` - Phase 2 approves.
