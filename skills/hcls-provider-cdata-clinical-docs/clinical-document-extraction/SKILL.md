---
name: clinical-document-extraction
parent_skill: hcls-provider-cdata-clinical-docs
description: "Orchestrator for the clinical document extraction pipeline. Delegates to gate micro-skills (Tier 1) for pre-condition confirmation and phase skills (Tier 2) for pipeline execution. No inline gates or pipeline SQL — all execution is in sub-skills."
tools: ["snowflake_sql_execute"]
---

# Clinical Document Extraction — Orchestrator

An interactive, config-driven pipeline for extracting structured intelligence from clinical documents (PDF, DOCX, PNG, JPG, TIFF, TXT) using Snowflake Cortex AI functions (`AI_PARSE_DOCUMENT`, `AI_EXTRACT`, `AI_AGG`).

## Platform Skill Synergy

This industry skill **delegates** to the bundled `document-intelligence` platform skill for generic document processing, while retaining domain-specific clinical logic.

| Capability | Delegated To | Our Addition |
|---|---|---|
| Pricing estimation | `document-intelligence` SKILL.md §"Always Display Pricing" | Clinical-specific cost projections by pipeline step |
| File location & upload | `document-intelligence` SKILL.md §Step 2 | Defaults to `INTERNAL_CLINICAL_DOCS_STAGE` |
| File type validation | `document-intelligence` SKILL.md §Step 3 | Clinical docs are predominantly PDF |
| Test-before-batch | `document-intelligence/references/extraction.md` §Step 5 | Single-doc quality gate before batch classification & extraction |
| Pipeline templates | `document-intelligence/references/pipeline.md` | Domain-specific task with MRN linkage, pivot view joins, presigned URLs |

**Domain-specific** (NOT delegated): Config-driven extraction schemas, classification routing, AI_AGG dual-path, adaptive parse mode, image description injection, identity linkage, pivot views, AI-ready content layer.

## Execution Flow

### Step 0: Query Data Model Knowledge (Auto — Injected by Router)

The clinical-docs router automatically runs this step before loading this skill. The search results from `CLINICAL_DOCS_MODEL_SEARCH_SVC` and `CLINICAL_DOCS_SPECS_SEARCH_SVC` provide the current schema and doc type specs.

**Query extraction config and doc type definitions:**
```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    '{db}.DATA_MODEL_KNOWLEDGE.CLINICAL_DOCS_SPECS_SEARCH_SVC',
    '{"query": "extraction fields for all document types", "columns": ["doc_type", "field_name", "extraction_question", "data_type", "contains_phi", "view_name"]}'
);
```

**Use the results to:**
- Validate configured doc types against the spec CKE (Gate E4-E5)
- Ground extraction prompts in the latest spec definitions
- Build config table INSERTs from spec CKE instead of hardcoded values
- Inform the OTHER onboarding loop (Gate E9) with similar doc type templates

**If search service is unavailable**, fall back to `references/document_type_specs.yaml` on disk.

### Pre-Conditions (Tier 1 Gates — must complete before any pipeline execution)

Each gate is a **separate skill load**. Gates complete sequentially and return confirmed parameters to the orchestrator.

| # | Gate Skill | Gates Covered | Returns |
|---|-----------|--------------|---------|
| 1 | **Load** `gates/confirm-environment/SKILL.md` | E1 + E2 + E3 | `{db}`, `{schema}`, `{stage}`, `{warehouse}`, `{connection}`, `{file_count}` |
| 2 | **Load** `gates/confirm-doc-types/SKILL.md` | E4 + E5 | `{configured_types}`, `{fields_per_type}` |
| 3 | **Load** `gates/confirm-pipeline-config/SKILL.md` | E6 + E6b + E7 | `{mode}`, `{warehouse_size_decision}`, `{estimated_cost}`, `{user_approved_cost}` |

### Pipeline Phases (Tier 2 — router re-enters between each)

Each phase is a **separate skill load**. The router MUST present phase results to the user and get confirmation before loading the next phase.

| # | Phase Skill | Steps Covered | Reactive Gates | Re-entry Question |
|---|------------|--------------|----------------|-------------------|
| 4 | **Load** `phases/classify/SKILL.md` | Preprocess + Classify | E8 (quality) + E9 (unknown type) | "Classification complete. Proceed to extraction?" |
| 5 | **Load** `phases/extract/SKILL.md` | Type-specific extraction | E10 (quality per type) | "Extraction complete. Proceed to parse?" |
| 6 | **Load** `phases/parse-and-refresh/SKILL.md` | Parse + AGG + Refresh + SV + Verify | None | "Pipeline complete! What next?" |

### Post-Pipeline

| # | Gate | Action |
|---|------|--------|
| 7 | GATE E11: What next? | Use `ask_user_question` to present: Search / Agent / Viewer / Add type / Governance / Share |

## Key Cortex AI Patterns

### Config-Driven Extraction (responseFormat from config table)

The `CLINICAL_DOCS_EXTRACTION_CONFIG` table is the runtime config, derived from the authoritative spec layer at `references/document_type_specs.yaml`. The YAML spec defines doc types, fields, prompts, and PHI flags; the config table is seeded from it.

```sql
AI_EXTRACT(
    file => TO_FILE(stage, path),
    responseFormat => {db}.{schema}.BUILD_DOCUMENT_CLASIFICATION_EXTRACTION_JSON()
)
```

### AI_AGG for Split Documents
```sql
AI_AGG(page_content, '{extraction_prompt}')
```
Groups by parent document, extracts across all pages.

### AI_PARSE_DOCUMENT Mode Selection
```sql
CASE
    WHEN complex_tables_flag = 'YES' OR image_flag = 'YES' THEN
        AI_PARSE_DOCUMENT(file, {'mode': 'LAYOUT', 'page_split': true, 'extract_images': true})
    ELSE
        AI_PARSE_DOCUMENT(file, {'mode': 'OCR', 'page_split': true})
END
```

## Execution Notes (CRITICAL for CoCo agents)

### 1. IDENTIFIER() Limitation in DDL
Snowflake's `IDENTIFIER()` does NOT support `||` concatenation in DDL. Use `EXECUTE IMMEDIATE` with string concatenation instead.

### 2. Connection Parameter
`snowflake_sql_execute` may route to IDE's default connection, ignoring the `connection` parameter. Always verify with `snow sql -c {connection} -q "SELECT CURRENT_ACCOUNT();"` first.

### 3. Execution Method
| Method | When to Use |
|--------|------------|
| `snow sql -c {connection} -f <file>` | Default for DDL. Write SQL to temp file. |
| `snow sql -c {connection} -q "..."` | Single simple statements. |
| `snowflake_sql_execute` | Only when default connection matches target. |

### 4. Hardcoded FQN Pattern
Replace ALL `$V_DB`, `$V_SCHEMA` with actual values before execution via CLI. The CLI treats `$VAR` as shell variables.

### 5. Timeout Prevention
Split large DDL batches into 3-4 statements maximum.

## Snowflake Scripting Constraints

| # | Rule | Error Prevented |
|---|------|----------------|
| 1 | No f-strings with `\n` inside `$$` blocks | `unterminated string literal` |
| 2 | No nested `$$` delimiters | `unexpected '$'` |
| 3 | `snow sql -f` cannot execute session variables + `$$` | Empty variable expansion |
| 4 | DECLARE cursors cannot reference variables | `unexpected 'v_fqn'` |
| 5 | INFORMATION_SCHEMA not visible in-transaction | Empty result set |
| 6 | Inline FOR cursors don't support field access | `invalid identifier` |
| 7 | DELETE before INSERT for config seeding | Duplicate rows |
| 8 | COALESCE requires 2+ arguments | `requires at least two arguments` |

## Platform Skill References

| Reference | Path | Used For |
|-----------|------|----------|
| Pricing & constraints | `document-intelligence/SKILL.md` | Cost estimation |
| Extraction workflow | `document-intelligence/references/extraction.md` | Test-before-batch |
| Parsing workflow | `document-intelligence/references/parsing.md` | Mode selection |
| Pipeline templates | `document-intelligence/references/pipeline.md` | Stream + Task patterns |
| Doc type specs | `references/document_type_specs.yaml` | Authoritative field definitions (CKE spec layer) |
| CKE metadata pattern | `references/metadata_as_cke.md` | How specs feed config dynamically |

## Prerequisites

1. Cortex AI features enabled (AI_PARSE_DOCUMENT, AI_EXTRACT, AI_AGG)
2. Pipeline objects created via `scripts/dynamic_pipeline_setup.sql`

## Execution Notes (CRITICAL for CoCo agents)

These notes address known issues when executing the pipeline SQL scripts. Failure to follow them will cause errors.

### 1. IDENTIFIER() Limitation in DDL

Snowflake's `IDENTIFIER()` function does **NOT** support expression concatenation (`||`) inside DDL statements like `CREATE SCHEMA`, `CREATE STAGE`, or `CREATE TABLE`. It only works with a single session variable reference (e.g., `IDENTIFIER($V_DB)`).

**Broken:**
```sql
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($V_DB || '.' || $V_SCHEMA);  -- ERROR: unexpected '||'
```

**Fixed (use EXECUTE IMMEDIATE):**
```sql
EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || $V_DB || '.' || $V_SCHEMA;
```

The setup scripts already use EXECUTE IMMEDIATE for all multi-part DDL.

### 2. Connection Parameter

**Before executing ANY SQL**, verify the target connection works:
```sql
SELECT CURRENT_ACCOUNT(), CURRENT_ROLE(), CURRENT_DATABASE();
```
If this returns the wrong account, ask the user for the correct connection name and pass it via the `connection` parameter of `snowflake_sql_execute`.

### 3. Execution Method: snow sql CLI vs snowflake_sql_execute

| Method | When to Use | Notes |
|--------|------------|-------|
| `snow sql -c {connection} -f <file>` | **DEFAULT for DDL** | Write SQL to temp file first. Respects connection. **Fails on EXECUTE IMMEDIATE with `||` and nested `$$`.** |
| `snow sql -c {connection} -q "..."` | **Single simple statements** | Respects connection. Avoid for `$$` or `$VAR` references (shell mangles them). |
| `snowflake_sql_execute` tool | **DEFAULT for all SQL** | Supports `connection` parameter, handles `$$` and complex quoting natively. |
| Snowflake worksheet (Snowsight) | **Fallback** | Full file works. Best for manual execution. |

**Recommended approach for CoCo agents:**
1. **Always use `snow sql -c {connection}`** — never assume the default connection is correct
2. For complex SQL (EXECUTE IMMEDIATE, `$$`, stored procs): write to a temp file, then `snow sql -c {connection} -f /tmp/step_N.sql`
3. Run each numbered STEP section as a separate file
4. If a section is too large, split at the `-- ===` comment boundaries

### 4. Hardcoded FQN Pattern (Required for CLI Execution)

The setup scripts use session variables (`$V_DB`, `$V_SCHEMA`) which are **incompatible with `snow sql` CLI** because:
- `snow sql -q` treats `$VAR` as shell variables (empty)
- `snow sql -f` loses session state between semicolons

**Required approach**: Generate SQL files with hardcoded FQN values:
1. Read the script template
2. Replace ALL `$V_DB`, `$V_SCHEMA`, `$V_WAREHOUSE` with actual values from user
3. Replace ALL `v_fqn` references with `{db}.{schema}`
4. Replace ALL `IDENTIFIER($V_...)` with hardcoded fully-qualified names
5. Write to a temp file
6. Execute via `snow sql -c {connection} -f <temp_file>`

> **Important**: The `GENERATE_DYNAMIC_OBJECTS()` stored procedure body also uses `v_fqn`. When creating it via CLI, hardcode the FQN inside the proc body as well.

### 5. Timeout Prevention

Large DDL batches (8+ CREATE TABLE statements) can timeout when run as a single call. Split into batches of 3-4 statements maximum. The script's STEP comments provide natural split points.

## Snowflake Scripting Constraints

These rules prevent the 8 most common runtime errors encountered when building Snowflake Scripting procedures for this pipeline. **Every SQL block generated by this skill MUST follow these rules.**

| # | Rule | Error Prevented | Details |
|---|------|----------------|----------|
| 1 | **No f-strings with `\n` inside `$$` blocks** | `SyntaxError: unterminated string literal` | Use `chr(10)` + string concatenation instead of f-strings containing `\n` in Python UDFs wrapped in `$$`. |
| 2 | **No nested `$$` delimiters** | `syntax error: unexpected '$'` | Snowflake does not support `EXECUTE IMMEDIATE $$ ... CREATE PROCEDURE ... AS $$ ... $$ ... $$`. Use `{db}/{schema}` placeholders substituted at creation time instead. |
| 3 | **`snow sql -f` cannot execute session variables + `$$`** | Empty variable expansion / partial execution | The CLI loses session state at `$$` boundaries and treats `$V_DB` as shell variables. Use `snowflake_sql_execute` tool or Snowsight worksheet instead. |
| 4 | **DECLARE cursors cannot reference variables** | `syntax error ... unexpected 'v_fqn'` | Cursors declared in the DECLARE block are compiled before BEGIN runs, so they cannot use variables. Use literal `{db}.{schema}` FQN (substituted at creation time) in DECLARE cursor queries. |
| 5 | **INFORMATION_SCHEMA is not visible in-transaction** | Empty result set / missing rows | Views/tables created earlier in the same procedure are not visible in INFORMATION_SCHEMA until the transaction commits. Read from the config table (`CLINICAL_DOCS_EXTRACTION_CONFIG`) instead. |
| 6 | **Inline FOR cursors don't support field access** | `invalid identifier 'REC.COLUMN_NAME'` | Only named cursors (declared in DECLARE) support `rec.FIELD_NAME` access. Always use named cursors. |
| 7 | **DELETE before INSERT for config seeding** | Duplicate rows / PIVOT column collision | Always use DELETE + INSERT (idempotent pattern) when seeding config tables. Never INSERT without clearing first. |
| 8 | **COALESCE requires 2+ arguments** | `COALESCE requires at least two arguments` | When dynamically building COALESCE from a variable-length list, always append `, NULL` to guarantee the minimum. |
