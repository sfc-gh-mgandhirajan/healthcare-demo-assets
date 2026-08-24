---
name: phase-landing-ddl
description: Generate a typed landing table DDL for the new transaction type
parent_skill: edi-extend
phase_id: P2
---

# Phase 2: Landing Table DDL Generation

## Purpose
Generate the CREATE TABLE statement for the typed landing table that receives parsed records from Snowpipe Streaming (or the Python UDF lite path).

## Input Required (from Phase 1)
- Complete field map with all column names
- `landing_schema` — target schema name
- `landing_table` — target table name

## Generation Logic

### 1. Column Structure

Every landing table follows this pattern:

```sql
CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} (
    -- Envelope columns (always present)
    ISA_SENDER_ID           VARCHAR,
    ISA_RECEIVER_ID         VARCHAR,
    ISA_DATE                VARCHAR,
    ISA_TIME                VARCHAR,
    ISA_CONTROL_NUMBER      VARCHAR,
    GS_FUNCTIONAL_ID        VARCHAR,
    GS_SENDER_CODE          VARCHAR,
    GS_RECEIVER_CODE        VARCHAR,
    GS_DATE                 VARCHAR,
    GS_CONTROL_NUMBER       VARCHAR,
    ST_CONTROL_NUMBER       VARCHAR,
    
    -- Record-specific columns (from field map)
    {field_1}               VARCHAR,
    {field_2}               VARCHAR,
    ...
    
    -- Metadata columns (always present)
    RAW_SEGMENTS            VARCHAR,    -- original segment text (if include_raw=true)
    RECORD_INDEX            VARCHAR,    -- position within the file
    INGESTION_TIMESTAMP     VARCHAR     -- when the record was ingested
);
```

### 2. Critical Constraints

- **ALL columns must be VARCHAR** — Snowpipe Streaming does not support DEFAULT values, IDENTITY, or AUTOINCREMENT
- **No NOT NULL constraints** — streaming inserts may have sparse data
- **No DEFAULT values** — breaks Snowpipe Streaming
- **Column names use UPPER_SNAKE_CASE** — matches Snowflake conventions
- **Map Python field names to SQL columns**: `subscriber_last_name` → `SUBSCRIBER_LAST_NAME`

### 3. Schema Creation (if needed)

```sql
CREATE SCHEMA IF NOT EXISTS {database}.{schema};
```

## Validation

Run the generated DDL with `only_compile=true` to verify it compiles:
```sql
-- Compile check only, does not execute
CREATE TABLE IF NOT EXISTS X12_EDI_AI.{schema}.{table} (...)
```

If compilation fails, diagnose and fix before presenting to user.

## Output

### Write Mode
Write to `sql/` directory in backbone repo as `{nn}_{table_name}.sql` (next available number).

### Dry-Run Mode
Output as fenced SQL code block.

## User Confirmation Required
Present the DDL and ask: "Does this table structure look correct? Any columns to add/remove?"
