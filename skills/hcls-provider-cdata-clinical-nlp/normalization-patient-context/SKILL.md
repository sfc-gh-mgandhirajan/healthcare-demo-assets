---
name: normalization-patient-context
description: "Normalize extracted social history, SDOH observations, and family history to standard terminology codes (ICD-10-CM Z-codes, SNOMED CT, Gravity Project) using CONCEPT_DIMENSION lookup with Cortex AI fuzzy matching fallback."
parent_skill: hcls-provider-cdata-clinical-nlp
---

# Patient Context Terminology Normalization

## Scope

| Target Table | Code Fields | Code Systems | Semantic Groups |
|-------------|------------|--------------|-----------------|
| SOCIAL_HISTORY_OBSERVATION | `code`, `code_system` | ICD-10-CM (Z-codes), SNOMED CT | SOCIAL |
| FAMILY_MEMBER_HISTORY | `condition_code`, `condition_system` | ICD-10-CM, SNOMED CT | DISEASE, FAMILY |

This sub-skill runs **after** extraction (extraction-patient-context) and populates terminology codes on rows where code fields are NULL but display text is populated.

## Code System Preference (`$NORM_CODE_SYSTEMS`)

This sub-skill receives `$NORM_CODE_SYSTEMS` from the router's Terminology Preference Gate:

| `$NORM_CODE_SYSTEMS` | SOCIAL_HISTORY (SDOH) | SOCIAL_HISTORY (tobacco/alcohol) | FAMILY_MEMBER_HISTORY |
|---------------------|----------------------|--------------------------------|----------------------|
| `ICD-10-CM` | Z-codes (default for SDOH) | ICD-10-CM | ICD-10-CM |
| `SNOMED CT` | SNOMED CT Gravity concepts | SNOMED CT | SNOMED CT |
| `ICD-10-CM,SNOMED CT` | ICD-10-CM Z-codes primary | SNOMED CT primary | ICD-10-CM primary |
| `ALL` | ICD-10-CM Z-codes primary | SNOMED CT primary | ICD-10-CM primary |

> **SDOH Z-codes**: The Step 1 deterministic SDOH domain mapping always targets ICD-10-CM Z-codes (the Gravity Project standard). If the user selected `SNOMED CT` only, Step 1 is skipped and all SDOH observations go through fuzzy matching against SNOMED CT Gravity concepts.

**If `$NORM_CODE_SYSTEMS` is not set**, prompt the user via the router gate before proceeding.

## Special Consideration: SDOH Domain Mapping

SOCIAL_HISTORY_OBSERVATION rows have an `sdoh_domain` field (12 Gravity Project domains). The SDOH domain itself provides a strong signal for code selection — each domain maps to a narrow ICD-10-CM Z-code range.

## Architecture

```
Clinical Table (display populated, code NULL)
    |
    v  Step 1: SDOH domain-guided deterministic mapping
Z-code lookup by sdoh_domain
    |
    v  Step 2: Exact match for non-SDOH social history + family history
CONCEPT_DIMENSION (semantic_group IN ('SOCIAL','FAMILY','DISEASE'))
    |
    v  Step 3: Fuzzy match via Cortex AI (remaining)
LLM candidate selection
    |
    v  Step 4: UPDATE code fields
```

## Step 1: SDOH Domain-Guided Mapping

For SOCIAL_HISTORY_OBSERVATION rows with a populated `sdoh_domain`, use the Gravity Project Z-code mapping:

```sql
UPDATE SOCIAL_HISTORY_OBSERVATION
SET code = CASE sdoh_domain
    WHEN 'FOOD_INSECURITY' THEN 'Z59.41'
    WHEN 'HOUSING' THEN 'Z59.00'
    WHEN 'TRANSPORTATION' THEN 'Z59.82'
    WHEN 'FINANCIAL_STRAIN' THEN 'Z59.70'
    WHEN 'EDUCATION' THEN 'Z55.9'
    WHEN 'SOCIAL_ISOLATION' THEN 'Z60.2'
    WHEN 'SAFETY' THEN 'Z63.0'
    WHEN 'EMPLOYMENT' THEN 'Z56.9'
    WHEN 'SUBSTANCE_USE' THEN 'F19.10'
    WHEN 'TOBACCO' THEN 'Z72.0'
    WHEN 'ALCOHOL' THEN 'Z72.1'
    WHEN 'VETERAN_STATUS' THEN 'Z91.82'
    ELSE NULL
END,
    code_system = CASE
        WHEN sdoh_domain IN ('SUBSTANCE_USE') THEN 'ICD-10-CM'
        ELSE 'ICD-10-CM'
    END
WHERE code IS NULL
  AND sdoh_domain IS NOT NULL;
```

> **Note**: These are "unspecified" Z-codes (e.g., Z59.41 = "Food insecurity"). More specific codes (e.g., Z59.42 = "Inadequate drinking-water supply") require Cortex AI refinement based on `evidence_text` in Step 3.

## Step 2: Exact Match for Non-SDOH Social History

For tobacco, alcohol, and substance use entries without SDOH domain, and for family history conditions:

```sql
UPDATE SOCIAL_HISTORY_OBSERVATION o
SET o.code = cd.code,
    o.code_system = cs.name
FROM CONCEPT_DIMENSION cd
JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
WHERE UPPER(TRIM(o.display)) = UPPER(TRIM(cd.display))
  AND cd.semantic_group = 'SOCIAL'
  AND o.code IS NULL
  AND o.display IS NOT NULL
  AND o.sdoh_domain IS NULL;
```

```sql
UPDATE FAMILY_MEMBER_HISTORY f
SET f.condition_code = cd.code,
    f.condition_system = cs.name
FROM CONCEPT_DIMENSION cd
JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
WHERE UPPER(TRIM(f.condition_display)) = UPPER(TRIM(cd.display))
  AND cd.semantic_group IN ('DISEASE', 'FAMILY')
  AND f.condition_code IS NULL
  AND f.condition_display IS NOT NULL;
```

## Step 3: Fuzzy Match — SDOH Code Refinement

For SDOH observations where the base Z-code from Step 1 could be more specific, use Cortex AI with the `evidence_text`:

```sql
WITH sdoh_refinable AS (
    SELECT social_history_id, sdoh_domain, display, status, screening_instrument,
           evidence_text, code
    FROM SOCIAL_HISTORY_OBSERVATION
    WHERE sdoh_domain IS NOT NULL AND evidence_text IS NOT NULL
)
SELECT
    s.social_history_id,
    s.sdoh_domain,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        CONCAT(
            'You are a clinical coding expert specializing in SDOH and ICD-10-CM Z-codes. ',
            'Given the SDOH domain and evidence text, select the MOST SPECIFIC ICD-10-CM Z-code. ',
            'Return ONLY a JSON object: {"code": "<Z-code>", "code_system": "ICD-10-CM", "confidence": <0.0-1.0>}.\n\n',
            'SDOH Domain: ', s.sdoh_domain, '\n',
            'Display: "', COALESCE(s.display, ''), '"\n',
            'Current code: ', COALESCE(s.code, 'NONE'), '\n',
            'Status: ', COALESCE(s.status, 'NOT_SPECIFIED'), '\n',
            'Screening Instrument: ', COALESCE(s.screening_instrument, 'NOT_SPECIFIED'), '\n',
            'Evidence text: "', s.evidence_text, '"\n\n',
            'Candidate Z-codes for this domain:\n',
            (SELECT LISTAGG(CONCAT(cd2.code, ' | ', cd2.display), '\n')
             FROM CONCEPT_DIMENSION cd2
             JOIN CODE_SYSTEM cs2 ON cd2.code_system_id = cs2.code_system_id
             WHERE cs2.name = 'ICD-10-CM'
             AND cd2.code LIKE CASE s.sdoh_domain
                 WHEN 'FOOD_INSECURITY' THEN 'Z59.4%'
                 WHEN 'HOUSING' THEN 'Z59.0%'
                 WHEN 'TRANSPORTATION' THEN 'Z59.8%'
                 WHEN 'FINANCIAL_STRAIN' THEN 'Z59.%'
                 WHEN 'EDUCATION' THEN 'Z55.%'
                 WHEN 'SOCIAL_ISOLATION' THEN 'Z60.%'
                 WHEN 'SAFETY' THEN 'Z63.%'
                 WHEN 'EMPLOYMENT' THEN 'Z56.%'
                 ELSE 'Z%'
             END
             LIMIT 20)
        )
    ) AS refined_code
FROM sdoh_refinable s;
```

## Step 3: Fuzzy Match — Family History

For unmatched family history conditions, use **full clinical context** — the relationship, onset age, deceased flag, and evidence text all help drive code specificity:

```sql
WITH unmatched_fh AS (
    SELECT family_history_id, condition_display, relationship_display,
           onset_age, deceased_flag, is_negated, evidence_text
    FROM FAMILY_MEMBER_HISTORY
    WHERE condition_code IS NULL AND condition_display IS NOT NULL
)
SELECT
    f.family_history_id,
    f.condition_display,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        CONCAT(
            'You are a clinical terminology expert. Given a family history condition with its context, find the MOST SPECIFIC code. ',
            'IMPORTANT: The user has requested coding in: ', $NORM_CODE_SYSTEMS_DISPLAY, '. Only return codes from the requested system(s).\n',
            'Use the context to drive specificity:\n',
            '- relationship → may affect code choice (e.g., maternal vs paternal for hereditary conditions)\n',
            '- onset_age → early onset may indicate specific subtypes\n',
            '- deceased_flag → cause of death context\n',
            '- is_negated → if TRUE, this is a DENIED family history (no code needed, return confidence 0.0)\n',
            '- evidence_text → original note citation may contain details not in the display\n\n',
            'Return ONLY: {"code": "<code>", "code_system": "<system>", "confidence": <0.0-1.0>}.\n\n',
            '--- FAMILY HISTORY CONDITION ---\n',
            'Condition: "', f.condition_display, '"\n',
            'Relationship: ', COALESCE(f.relationship_display, 'UNKNOWN'), '\n',
            'Onset Age: ', COALESCE(f.onset_age::VARCHAR, 'NOT_SPECIFIED'), '\n',
            'Deceased: ', COALESCE(f.deceased_flag::VARCHAR, 'UNKNOWN'), '\n',
            'Negated: ', f.is_negated::VARCHAR, '\n',
            'Evidence Text: "', COALESCE(f.evidence_text, ''), '"\n\n',
            '--- CANDIDATES ---\n',
            (SELECT LISTAGG(CONCAT(cd2.code, ' | ', cs2.name, ' | ', cd2.display), '\n')
             FROM CONCEPT_DIMENSION cd2
             JOIN CODE_SYSTEM cs2 ON cd2.code_system_id = cs2.code_system_id
             WHERE cd2.semantic_group IN ('DISEASE', 'FAMILY')
             AND cs2.name IN ($NORM_CODE_SYSTEMS)   -- filter by user preference
             AND (CONTAINS(UPPER(cd2.display), UPPER(SPLIT_PART(f.condition_display, ' ', 1))))
             LIMIT 20)
        )
    ) AS match_result
FROM unmatched_fh f;
```

## Preferred Code System Priority

**Conditional on `$NORM_CODE_SYSTEMS`**: This table is a fallback for `ALL` or multi-system selections only. It does NOT override the user's explicit preference.

| Table | Priority 1 | Priority 2 | Rationale |
|-------|-----------|-----------|-----------|
| SOCIAL_HISTORY_OBSERVATION (SDOH) | **ICD-10-CM** (Z-codes) | Gravity Project codes | Z-codes are standard for SDOH screening/documentation |
| SOCIAL_HISTORY_OBSERVATION (tobacco/alcohol) | **SNOMED CT** | ICD-10-CM | More clinical granularity for substance use |
| FAMILY_MEMBER_HISTORY | **ICD-10-CM** | SNOMED CT | Conditions in FH context |

## Validation

```sql
SELECT
    'SOCIAL_HISTORY (SDOH)' AS category,
    COUNT(*) AS total,
    COUNT(code) AS coded,
    ROUND(COUNT(code) * 100.0 / NULLIF(COUNT(*), 0), 1) AS rate_pct
FROM SOCIAL_HISTORY_OBSERVATION WHERE sdoh_domain IS NOT NULL
UNION ALL
SELECT 'SOCIAL_HISTORY (other)', COUNT(*), COUNT(code),
    ROUND(COUNT(code) * 100.0 / NULLIF(COUNT(*), 0), 1)
FROM SOCIAL_HISTORY_OBSERVATION WHERE sdoh_domain IS NULL AND display IS NOT NULL
UNION ALL
SELECT 'FAMILY_MEMBER_HISTORY', COUNT(*), COUNT(condition_code),
    ROUND(COUNT(condition_code) * 100.0 / NULLIF(COUNT(*), 0), 1)
FROM FAMILY_MEMBER_HISTORY WHERE condition_display IS NOT NULL;
```

Target: >= 95% for SDOH (deterministic domain mapping), >= 75% for family history conditions.

## Output

UPDATEd code on SOCIAL_HISTORY_OBSERVATION (Z-codes by sdoh_domain) and condition_code on FAMILY_MEMBER_HISTORY. Returns dict with counts.
