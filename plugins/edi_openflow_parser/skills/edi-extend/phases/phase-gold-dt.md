---
name: phase-gold-dt
description: Generate a Gold Dynamic Table with type casting and optional AI enrichment
parent_skill: edi-extend
phase_id: P3
---

# Phase 3: Gold Dynamic Table Generation

## Purpose
Generate a Dynamic Table in the GOLD schema that applies type casting to the VARCHAR landing columns and optionally adds Cortex AI enrichment.

## Input Required (from Phase 2)
- Landing table name and schema
- Field map with semantic understanding of each column's data type
- User's preference on AI enrichment

## Generation Logic

### 1. Type Inference

From field names, infer appropriate casts:
- `*_amount`, `*_charge`, `*_payment` → `TRY_TO_DECIMAL(col, 12, 2)`
- `*_date`, `*_dob` → `TRY_TO_DATE(col)`
- `*_count`, `*_units` → `TRY_TO_NUMBER(col)`
- `*_npi` → `VARCHAR` (keep as-is, it's an identifier)
- `*_code` → `VARCHAR` (keep as-is, codes are categorical)
- Everything else → `VARCHAR`

### 2. Base Dynamic Table

```sql
CREATE OR REPLACE DYNAMIC TABLE X12_EDI_AI.GOLD.GOLD_{tx_code}_{name} 
    TARGET_LAG = '10 minutes'
    WAREHOUSE = APP_WH
AS
SELECT
    -- Identifiers (keep as VARCHAR)
    {id_field} AS {id_field},
    
    -- Amounts (cast to DECIMAL)
    TRY_TO_DECIMAL({amount_field}, 12, 2) AS {amount_field},
    
    -- Dates (cast to DATE)
    TRY_TO_DATE({date_field}) AS {date_field},
    
    -- Counts (cast to NUMBER)
    TRY_TO_NUMBER({count_field}) AS {count_field},
    
    -- Metadata
    INGESTION_TIMESTAMP,
    ST_CONTROL_NUMBER
FROM X12_EDI_AI.{landing_schema}.{landing_table}
WHERE {primary_id_field} IS NOT NULL;
```

### 3. AI Enrichment (Optional)

If the transaction type has fields that benefit from AI interpretation (codes, classifications):

```sql
    -- AI Enrichment: decode ICD-10 diagnosis codes
    AI_COMPLETE(
        'claude-sonnet-4-6',
        CONCAT(
            'Given ICD-10 code: ', DIAGNOSIS_CODE_1,
            '. Return JSON: {"description": "...", "category": "...", "is_chronic": true/false}'
        )
    )::VARIANT:description::VARCHAR AS DIAGNOSIS_DESCRIPTION,
```

**Ask user before adding AI enrichment:**
```
Would you like AI enrichment in the Gold layer?

Options:
- Yes: Add Cortex AI to decode/classify specific fields (costs credits per row)
- No: Just type casting, no AI (zero additional cost)
- Custom: Let me specify which fields to enrich
```

### 4. Important Notes

- Use `AI_COMPLETE('claude-sonnet-4-6', ...)` — NOT `SNOWFLAKE.CORTEX.COMPLETE()`
- AI enrichment on large tables can be expensive — suggest filtering or sampling first
- Dynamic Table target lag of 10 minutes is the default; user can adjust
- Filter on primary ID IS NOT NULL to exclude partial/malformed records

## Validation

Compile-check the generated Dynamic Table DDL with `only_compile=true`.

## Output

### Write Mode
Append to `sql/02_gold_layer.sql` or create new file `sql/03_gold_{tx_code}.sql`.

### Dry-Run Mode
Output as fenced SQL code block.

## User Confirmation Required
Present the Gold DT DDL and ask: "Does this enrichment logic look right? Adjust the AI prompt or type casts?"
