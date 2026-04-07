---
name: normalization-conditions-diagnostics
description: "Normalize extracted conditions, diagnoses, symptoms, and risk factors to standard terminology codes (ICD-10-CM, SNOMED CT) using CONCEPT_DIMENSION lookup with Cortex AI fuzzy matching fallback."
parent_skill: hcls-provider-cdata-clinical-nlp
---

# Conditions & Diagnostics Terminology Normalization

## Scope

| Target Table | Code Fields | Code Systems | Semantic Groups |
|-------------|------------|--------------|-----------------|
| CONDITION | `code`, `code_system` | ICD-10-CM, SNOMED CT | DISEASE, SYMPTOM |

This sub-skill runs **after** extraction (extraction-conditions-diagnostics) and populates terminology codes on rows where `code` IS NULL but `display` is populated.

## Code System Preference (`$NORM_CODE_SYSTEMS`)

This sub-skill receives `$NORM_CODE_SYSTEMS` from the router's Terminology Preference Gate. The preference controls which code systems are targeted:

| `$NORM_CODE_SYSTEMS` | Exact Match Filter | Fuzzy Match Instruction | Result |
|---------------------|-------------------|------------------------|--------|
| `ICD-10-CM` | `cs.name = 'ICD-10-CM'` | "Select only ICD-10-CM codes" | ICD-10-CM only |
| `SNOMED CT` | `cs.name = 'SNOMED CT'` | "Select only SNOMED CT codes" | SNOMED CT only |
| `ICD-10-CM,SNOMED CT` | `cs.name IN ('ICD-10-CM','SNOMED CT')` | "Prefer ICD-10-CM; use SNOMED CT if no ICD-10-CM match" | Dual coding |
| `ALL` | No filter on code system | "Use the most specific code from any system" | Best available |

**If `$NORM_CODE_SYSTEMS` is not set**, prompt the user via the router gate before proceeding.

## Architecture

```
CONDITION (display populated, code NULL)
    |
    v  Step 1: Exact match
CONCEPT_DIMENSION (semantic_group IN ('DISEASE','SYMPTOM'))
    |
    v  Preflight: User prompt — fine-tuned model available?
    |         |                    |
    |     [YES]          [CREATE ONE]          [SKIP]
    |         |                |                  |
    |         |    invoke industry-models skill   |
    |         |    return with model name         |
    |         |                |                  |
    v  Step 1.5: Fine-tuned   |                  |
    |  ICD coding (bare code) |                  |
    |         |               |                  |
    v  Still unmatched? ------|----------------> |
    |                                            v
    |                                     Step 2: Fuzzy match
    |                                     (generic llama3.1-70b)
    v                                            |
    Step 3: UPDATE  <----------------------------+
CONDITION.code, CONDITION.code_system populated
```

## Step 1: Exact Match Lookup

Match extracted `display` text against CONCEPT_DIMENSION entries. Case-insensitive, trimmed. **Filter by `$NORM_CODE_SYSTEMS`**:

```sql
UPDATE CONDITION c
SET c.code = cd.code,
    c.code_system = cs.name
FROM CONCEPT_DIMENSION cd
JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
WHERE UPPER(TRIM(c.display)) = UPPER(TRIM(cd.display))
  AND cd.semantic_group IN ('DISEASE', 'SYMPTOM')
  AND cs.name IN ($NORM_CODE_SYSTEMS)   -- e.g., ('ICD-10-CM') or ('ICD-10-CM','SNOMED CT')
  AND c.code IS NULL
  AND c.display IS NOT NULL;
```

## Preflight: Fine-Tuned Model Gate

Before Step 1.5, ask the user whether a fine-tuned ICD coding model is available:

> Do you have a fine-tuned model for ICD-10-CM coding?
>
> - **Yes** -- provide the model name (e.g., `FINETUNE_llama38b_ICDCODES`)
> - **No, but I'd like to create one** -- invoke `hcls-cross-aiml-industrymodels` to create a fine-tuned ICD-10-CM model, then return here
> - **No, skip** -- skip Step 1.5, proceed to Step 2

| Response | `$FINETUNED_MODEL_NAME` | Behavior |
|----------|------------------------|----------|
| Yes + model name | Set to user-provided name | Proceed with Step 1.5 |
| Create one | Invoke `hcls-cross-aiml-industrymodels` | After model creation, return and set `$FINETUNED_MODEL_NAME`, then proceed with Step 1.5 |
| No, skip | Not set | Skip Step 1.5, go to Step 2 |

## Step 1.5: Fine-Tuned ICD Coding (Conditional)

Runs only when `$FINETUNED_MODEL_NAME` is set. The prompt format **must match the training data** used to fine-tune the model (see `hcls-cross-aiml-industrymodels` Step 2). The model was trained on `evidence_text + instruction suffix` → bare ICD-10-CM code.

Inference and update run in a single statement via subquery UPDATE — the model is called once per row, and results are applied directly with regex validation:

```sql
UPDATE CONDITION c
SET c.code = p.predicted_code,
    c.code_system = 'ICD-10-CM'
FROM (
    SELECT
        u.condition_id,
        TRIM(SNOWFLAKE.CORTEX.COMPLETE(
            $FINETUNED_MODEL_NAME,
            COALESCE(u.evidence_text, u.display)
                || ' Given this clinical text, assign the ICD10-CM diagnosis code in this format ONLY: X##.#. Do not provide explanation '
        )) AS predicted_code
    FROM CONDITION u
    WHERE u.code IS NULL AND u.display IS NOT NULL
) p
WHERE c.condition_id = p.condition_id
  AND p.predicted_code IS NOT NULL
  AND p.predicted_code RLIKE '^[A-Z][0-9]{2}(\.[0-9A-Z]{1,4})?$';
```

After this UPDATE, rows still with `code IS NULL` fall through to Step 2.

## Step 2: Fuzzy Match via Cortex AI

> Step 2 processes rows still unmatched after Step 1 (exact match) and Step 1.5 (fine-tuned model, if available). The existing `WHERE code IS NULL` clause handles this automatically.

For remaining unmatched rows, use Cortex AI with **full clinical context** to select the most specific code. The context fields (category, severity, body site, laterality, clinical status, certainty, evidence text) are critical for ICD-10-CM specificity — e.g., "diabetes" alone is E11.9 but with complication context becomes E11.65.

```sql
WITH unmatched AS (
    SELECT condition_id, display, category, clinical_status, verification_status,
           severity_display, body_site_display, laterality, certainty, evidence_text
    FROM CONDITION
    WHERE code IS NULL AND display IS NOT NULL
),
candidates AS (
    SELECT cd.code, cd.display AS concept_display, cs.name AS code_system_name,
           cd.semantic_group
    FROM CONCEPT_DIMENSION cd
    JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
    WHERE cd.semantic_group IN ('DISEASE', 'SYMPTOM')
      AND cs.name IN ($NORM_CODE_SYSTEMS)   -- filter candidates by user preference
)
SELECT
    u.condition_id,
    u.display AS extracted_text,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        CONCAT(
            'You are a clinical terminology expert. ',
            'Given the extracted condition text AND its clinical context, select the MOST SPECIFIC matching concept from the candidate list. ',
            'IMPORTANT: The user has requested coding in: ', $NORM_CODE_SYSTEMS_DISPLAY, '. Only return codes from the requested system(s).\n',
            'Use the clinical context to drive specificity:\n',
            '- body_site and laterality → anatomical specificity and laterality characters\n',
            '- severity → severity-specific codes (e.g., mild/moderate/severe)\n',
            '- category → distinguish encounter diagnosis vs history vs symptom\n',
            '- clinical_status → active vs resolved affects code choice\n',
            '- evidence_text → the original note citation may contain details not in the display text\n\n',
            'Return ONLY a JSON object: {"code": "<code>", "code_system": "<system>", "confidence": <0.0-1.0>}. ',
            'If no candidate matches with confidence >= 0.7, return {"code": null, "code_system": null, "confidence": 0.0}.\n\n',
            '--- EXTRACTED CONDITION ---\n',
            'Display: "', u.display, '"\n',
            'Category: ', COALESCE(u.category, 'UNKNOWN'), '\n',
            'Clinical Status: ', COALESCE(u.clinical_status, 'UNKNOWN'), '\n',
            'Verification: ', COALESCE(u.verification_status, 'UNKNOWN'), '\n',
            'Severity: ', COALESCE(u.severity_display, 'NOT_SPECIFIED'), '\n',
            'Body Site: ', COALESCE(u.body_site_display, 'NOT_SPECIFIED'), '\n',
            'Laterality: ', COALESCE(u.laterality, 'NOT_SPECIFIED'), '\n',
            'Certainty: ', COALESCE(u.certainty, 'NOT_SPECIFIED'), '\n',
            'Evidence Text: "', COALESCE(u.evidence_text, ''), '"\n\n',
            '--- CANDIDATES (top 20) ---\n',
            (SELECT LISTAGG(CONCAT(c2.code, ' | ', c2.code_system_name, ' | ', c2.concept_display), '\n')
             FROM (SELECT * FROM candidates c2
                   WHERE CONTAINS(UPPER(c2.concept_display), UPPER(SPLIT_PART(u.display, ' ', 1)))
                      OR CONTAINS(UPPER(u.display), UPPER(SPLIT_PART(c2.concept_display, ' ', 1)))
                   LIMIT 20) c2)
        )
    ) AS match_result
FROM unmatched u;
```

## Step 3: Apply Fuzzy Match Results

Parse the LLM response and update CONDITION rows where confidence >= 0.7:

```sql
UPDATE CONDITION c
SET c.code = PARSE_JSON(:match_result):code::VARCHAR,
    c.code_system = PARSE_JSON(:match_result):code_system::VARCHAR
WHERE c.condition_id = :condition_id
  AND PARSE_JSON(:match_result):confidence::NUMBER >= 0.7
  AND PARSE_JSON(:match_result):code IS NOT NULL;
```

## Preferred Code System Priority

**Conditional on `$NORM_CODE_SYSTEMS`**: If the user specified a single code system, that system is used exclusively. If `ALL` or dual-coding was selected, use this priority when multiple systems match the same condition:

| Priority | Code System | Typical Use Case |
|----------|------------|------------------|
| 1 | **ICD-10-CM** | Billing, claims, administrative reporting |
| 2 | **SNOMED CT** | Clinical detail, interoperability, FHIR exchange |

> This table is a **fallback only** — it does NOT override the user's `$NORM_CODE_SYSTEMS` selection.

## Validation

After normalization, verify:

```sql
SELECT
    COUNT(*) AS total_conditions,
    COUNT(code) AS coded_conditions,
    COUNT(code) * 100.0 / NULLIF(COUNT(*), 0) AS coding_rate_pct,
    COUNT(CASE WHEN code_system = 'ICD-10-CM' THEN 1 END) AS icd10_count,
    COUNT(CASE WHEN code_system = 'SNOMED CT' THEN 1 END) AS snomed_count
FROM CONDITION
WHERE display IS NOT NULL;
```

Target: >= 80% coding rate for well-formed condition text.

## Body Site Normalization (Secondary)

CONDITION also has `body_site_code` / `body_site_display`. Apply the same pattern using CONCEPT_DIMENSION with `semantic_group` = 'ANATOMY':

```sql
UPDATE CONDITION c
SET c.body_site_code = cd.code
FROM CONCEPT_DIMENSION cd
JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
WHERE UPPER(TRIM(c.body_site_display)) = UPPER(TRIM(cd.display))
  AND cd.semantic_group = 'ANATOMY'
  AND c.body_site_code IS NULL
  AND c.body_site_display IS NOT NULL;
```

## Severity Normalization (Secondary)

`severity_code` / `severity_display` uses a small fixed enum — no CONCEPT_DIMENSION lookup needed:

```sql
UPDATE CONDITION
SET severity_code = CASE UPPER(TRIM(severity_display))
    WHEN 'MILD' THEN '255604002'
    WHEN 'MODERATE' THEN '6736007'
    WHEN 'SEVERE' THEN '24484000'
    ELSE NULL
END
WHERE severity_code IS NULL AND severity_display IS NOT NULL;
```

## Output

UPDATEd code/code_system on CONDITION table. Returns dict with exact_match and fuzzy_match counts.
