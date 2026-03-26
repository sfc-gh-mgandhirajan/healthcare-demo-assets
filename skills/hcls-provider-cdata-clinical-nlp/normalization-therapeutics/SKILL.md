---
name: normalization-therapeutics
description: "Normalize extracted medications, procedures, and allergies to standard terminology codes (RxNorm, CPT, ICD-10-PCS, SNOMED CT) using CONCEPT_DIMENSION lookup with Cortex AI fuzzy matching fallback."
parent_skill: hcls-provider-cdata-clinical-nlp
---

# Therapeutics Terminology Normalization

## Scope

| Target Table | Code Fields | Code Systems | Semantic Groups |
|-------------|------------|--------------|-----------------|
| MEDICATION_REQUEST | `medication_code`, `medication_system` | RxNorm | MEDICATION |
| PROCEDURE | `code`, `code_system` | CPT, ICD-10-PCS, SNOMED CT | PROCEDURE |
| ALLERGY_INTOLERANCE | `substance_code`, `substance_system` | RxNorm, SNOMED CT | MEDICATION, OTHER |
| ALLERGY_INTOLERANCE | `reaction_code` | SNOMED CT | SYMPTOM |

This sub-skill runs **after** extraction (extraction-therapeutics) and populates terminology codes on rows where code fields are NULL but display text is populated.

## Code System Preference (`$NORM_CODE_SYSTEMS`)

This sub-skill receives `$NORM_CODE_SYSTEMS` from the router's Terminology Preference Gate. For therapeutics, the preference controls procedure coding (the main decision point):

| `$NORM_CODE_SYSTEMS` | MEDICATION_REQUEST | PROCEDURE | ALLERGY_INTOLERANCE |
|---------------------|-------------------|-----------|---------------------|
| `CPT` | RxNorm (always) | CPT only | RxNorm/SNOMED CT |
| `SNOMED CT` | RxNorm (always) | SNOMED CT only | RxNorm/SNOMED CT |
| `CPT,ICD-10-PCS` | RxNorm (always) | CPT primary, ICD-10-PCS fallback | RxNorm/SNOMED CT |
| `ALL` | RxNorm | CPT → ICD-10-PCS → SNOMED CT | RxNorm → SNOMED CT |

> **RxNorm for medications is always used** regardless of preference — there is no alternative drug terminology in common US use.

**If `$NORM_CODE_SYSTEMS` is not set**, prompt the user via the router gate before proceeding.

## Architecture

```
Clinical Table (display populated, code NULL)
    |
    v  Step 1: Exact match per table
CONCEPT_DIMENSION (filtered by semantic_group)
    |
    v  Step 2: Fuzzy match via Cortex AI (unmatched rows)
LLM candidate selection from CONCEPT_DIMENSION
    |
    v  Step 3: UPDATE code fields
```

## Step 1: Exact Match — MEDICATION_REQUEST

```sql
UPDATE MEDICATION_REQUEST m
SET m.medication_code = cd.code,
    m.medication_system = cs.name
FROM CONCEPT_DIMENSION cd
JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
WHERE UPPER(TRIM(m.medication_display)) = UPPER(TRIM(cd.display))
  AND cd.semantic_group = 'MEDICATION'
  AND cs.name = 'RxNorm'
  AND m.medication_code IS NULL
  AND m.medication_display IS NOT NULL;
```

## Step 1: Exact Match — PROCEDURE

```sql
UPDATE PROCEDURE p
SET p.code = cd.code,
    p.code_system = cs.name
FROM CONCEPT_DIMENSION cd
JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
WHERE UPPER(TRIM(p.display)) = UPPER(TRIM(cd.display))
  AND cd.semantic_group = 'PROCEDURE'
  AND cs.name IN ($NORM_CODE_SYSTEMS)   -- respects user preference (CPT, ICD-10-PCS, SNOMED CT)
  AND p.code IS NULL
  AND p.display IS NOT NULL;
```

## Step 1: Exact Match — ALLERGY_INTOLERANCE (substance — drug, RxNorm first)

```sql
UPDATE ALLERGY_INTOLERANCE a
SET a.substance_code = cd.code,
    a.substance_system = cs.name
FROM CONCEPT_DIMENSION cd
JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
WHERE UPPER(TRIM(a.substance_display)) = UPPER(TRIM(cd.display))
  AND cd.semantic_group = 'MEDICATION'
  AND cs.name = 'RxNorm'
  AND a.substance_code IS NULL
  AND a.substance_display IS NOT NULL;
```

## Step 1: Exact Match — ALLERGY_INTOLERANCE (substance — food/environmental, SNOMED CT fallback)

```sql
UPDATE ALLERGY_INTOLERANCE a
SET a.substance_code = cd.code,
    a.substance_system = cs.name
FROM CONCEPT_DIMENSION cd
JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
WHERE UPPER(TRIM(a.substance_display)) = UPPER(TRIM(cd.display))
  AND cd.semantic_group = 'OTHER'
  AND cs.name = 'SNOMED CT'
  AND a.substance_code IS NULL
  AND a.substance_display IS NOT NULL;
```

## Step 1: Exact Match — ALLERGY_INTOLERANCE (reaction)

```sql
UPDATE ALLERGY_INTOLERANCE a
SET a.reaction_code = cd.code
FROM CONCEPT_DIMENSION cd
JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
WHERE UPPER(TRIM(a.reaction_display)) = UPPER(TRIM(cd.display))
  AND cd.semantic_group = 'SYMPTOM'
  AND cs.name = 'SNOMED CT'
  AND a.reaction_code IS NULL
  AND a.reaction_display IS NOT NULL;
```

## Step 2: Fuzzy Match via Cortex AI

For unmatched rows, use Cortex AI with **full clinical context** per table. Each table has different context fields that drive code specificity, so we run separate fuzzy matches.

### Fuzzy Match — MEDICATION_REQUEST

Context: `dosage_text`, `route_display`, `frequency_text`, `intent`, `status`, `is_negated`, `temporality`, `evidence_text` all help disambiguate between drug formulations, strengths, and routes that map to different RxNorm concepts.

```sql
WITH unmatched_meds AS (
    SELECT medication_request_id, medication_display, dosage_text, dose, dose_unit,
           route_display, frequency_text, intent, status, is_negated, temporality, evidence_text
    FROM MEDICATION_REQUEST
    WHERE medication_code IS NULL AND medication_display IS NOT NULL
)
SELECT
    m.medication_request_id,
    m.medication_display,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        CONCAT(
            'You are a clinical pharmacy terminology expert specializing in RxNorm. ',
            'Given the extracted medication AND its clinical context, select the MOST SPECIFIC RxNorm concept. ',
            'Match to the appropriate RxNorm concept type (TTY) based on available context:\n',
            '- If dose AND route are known → target SCD (Semantic Clinical Drug) e.g., "Metformin 500 MG Oral Tablet"\n',
            '- If only dose is known → target SCDC (Clinical Drug Component) e.g., "Metformin 500 MG"\n',
            '- If only drug name is known → target IN (Ingredient) e.g., "Metformin"\n',
            '- If brand name is mentioned in evidence_text → target SBD (Semantic Branded Drug) e.g., "Glucophage 500 MG Oral Tablet"\n',
            'Do NOT guess a strength or dose form that is not supported by the clinical context.\n\n',
            'Brand/generic resolution: If the extracted text uses a brand name (e.g., "Glucophage") but candidates show the generic (e.g., "Metformin"), match on clinical equivalence — they map to the same RxNorm ingredient. Prefer the SCD (generic) form unless the user context specifically requires brand.\n\n',
            'Use context to drive specificity:\n',
            '- dosage/dose/unit → specific strength (e.g., Metformin 500mg vs 1000mg)\n',
            '- route → oral vs injectable vs topical formulation\n',
            '- frequency → may distinguish immediate-release vs extended-release\n',
            '- evidence_text → original note may have brand name, formulation details\n\n',
            'Return ONLY: {"code": "<code>", "code_system": "RxNorm", "confidence": <0.0-1.0>}.\n\n',
            '--- MEDICATION ---\n',
            'Display: "', m.medication_display, '"\n',
            'Dosage Text: ', COALESCE(m.dosage_text, 'NOT_SPECIFIED'), '\n',
            'Dose: ', COALESCE(m.dose::VARCHAR, ''), ' ', COALESCE(m.dose_unit, ''), '\n',
            'Route: ', COALESCE(m.route_display, 'NOT_SPECIFIED'), '\n',
            'Frequency: ', COALESCE(m.frequency_text, 'NOT_SPECIFIED'), '\n',
            'Intent: ', COALESCE(m.intent, 'NOT_SPECIFIED'), '\n',
            'Status: ', COALESCE(m.status, 'NOT_SPECIFIED'), '\n',
            'Negated: ', m.is_negated::VARCHAR, '\n',
            'Temporality: ', COALESCE(m.temporality, 'NOT_SPECIFIED'), '\n',
            'Evidence Text: "', COALESCE(m.evidence_text, ''), '"\n\n',
            '--- CANDIDATES ---\n',
            (SELECT LISTAGG(CONCAT(cd2.code, ' | RxNorm | ', cd2.display), '\n')
             FROM CONCEPT_DIMENSION cd2
             JOIN CODE_SYSTEM cs2 ON cd2.code_system_id = cs2.code_system_id
             WHERE cd2.semantic_group = 'MEDICATION' AND cs2.name = 'RxNorm'
             AND (CONTAINS(UPPER(cd2.display), UPPER(SPLIT_PART(m.medication_display, ' ', 1)))
                  OR CONTAINS(UPPER(m.medication_display), UPPER(SPLIT_PART(cd2.display, ' ', 1))))
             LIMIT 20)
        )
    ) AS match_result
FROM unmatched_meds m;
```

### Fuzzy Match — PROCEDURE

Context: `category`, `body_site_display`, `laterality`, `status`, `is_negated`, `temporality`, `evidence_text` drive CPT/ICD-10-PCS specificity — e.g., laterality and body site determine which CPT modifier or ICD-10-PCS character to use.

```sql
WITH unmatched_procs AS (
    SELECT procedure_id, display, category, status, body_site_display, laterality,
           is_negated, temporality, evidence_text
    FROM PROCEDURE
    WHERE code IS NULL AND display IS NOT NULL
)
SELECT
    p.procedure_id,
    p.display,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        CONCAT(
            'You are a clinical coding expert specializing in procedure terminology. ',
            'Given the extracted procedure AND its clinical context, select the MOST SPECIFIC code. ',
            'IMPORTANT: The user has requested coding in: ', $NORM_CODE_SYSTEMS_DISPLAY, '. Only return codes from the requested system(s).\n',
            CASE WHEN $NORM_CODE_SYSTEMS LIKE '%CPT%'
                THEN CONCAT(
                    'CPT (Current Procedural Terminology): Return a 5-digit numeric code (e.g., 27447 for total knee arthroplasty). ',
                    'CPT is organized by body system and procedure type (Category I = standard procedures, Category III = emerging technology). ',
                    'CPT codes describe the physician service performed — select the code that matches the specific technique, approach, and anatomical extent.\n',
                    'Body site + laterality → CPT modifiers (not part of the base code). Return the base code only.\n')
                ELSE ''
            END,
            CASE WHEN $NORM_CODE_SYSTEMS LIKE '%ICD-10-PCS%'
                THEN CONCAT(
                    'ICD-10-PCS: Return a 7-character alphanumeric code. Each character position has a defined meaning:\n',
                    '- Char 1: Section (0=Medical/Surgical) | Char 2: Body System | Char 3: Root Operation (e.g., Replacement, Excision, Repair)\n',
                    '- Char 4: Body Part | Char 5: Approach (Open, Percutaneous, Endoscopic) | Char 6: Device | Char 7: Qualifier\n',
                    'Use context to determine: body_site → Chars 2+4, evidence_text approach details → Char 5, device mentions → Char 6.\n',
                    'ICD-10-PCS is used for inpatient procedures — if the context suggests outpatient/physician office, prefer CPT if available.\n')
                ELSE ''
            END,
            CASE WHEN $NORM_CODE_SYSTEMS LIKE '%SNOMED CT%'
                THEN 'NOTE: When matching to SNOMED CT procedures, focus on clinical meaning (what was done, to which body part, via what approach) rather than billing specificity. SNOMED CT procedures are hierarchical with is-a relationships — prefer the most specific descendant that matches the clinical description.\n'
                ELSE ''
            END,
            'Use context:\n',
            '- category → SURGICAL vs DIAGNOSTIC vs IMAGING\n',
            '- body_site + laterality → anatomical specificity\n',
            '- evidence_text → may contain approach, technique, device details\n\n',
            'Return ONLY: {"code": "<code>", "code_system": "<CPT or ICD-10-PCS or SNOMED CT>", "confidence": <0.0-1.0>}.\n\n',
            '--- PROCEDURE ---\n',
            'Display: "', p.display, '"\n',
            'Category: ', COALESCE(p.category, 'NOT_SPECIFIED'), '\n',
            'Status: ', COALESCE(p.status, 'NOT_SPECIFIED'), '\n',
            'Body Site: ', COALESCE(p.body_site_display, 'NOT_SPECIFIED'), '\n',
            'Laterality: ', COALESCE(p.laterality, 'NOT_SPECIFIED'), '\n',
            'Negated: ', p.is_negated::VARCHAR, '\n',
            'Temporality: ', COALESCE(p.temporality, 'NOT_SPECIFIED'), '\n',
            'Evidence Text: "', COALESCE(p.evidence_text, ''), '"\n\n',
            '--- CANDIDATES ---\n',
            (SELECT LISTAGG(CONCAT(cd2.code, ' | ', cs2.name, ' | ', cd2.display), '\n')
             FROM CONCEPT_DIMENSION cd2
             JOIN CODE_SYSTEM cs2 ON cd2.code_system_id = cs2.code_system_id
             WHERE cd2.semantic_group = 'PROCEDURE'
             AND cs2.name IN ($NORM_CODE_SYSTEMS)   -- filter by user preference
             AND (CONTAINS(UPPER(cd2.display), UPPER(SPLIT_PART(p.display, ' ', 1)))
                  OR CONTAINS(UPPER(p.display), UPPER(SPLIT_PART(cd2.display, ' ', 1))))
             LIMIT 20)
        )
    ) AS match_result
FROM unmatched_procs p;
```

### Fuzzy Match — ALLERGY_INTOLERANCE

Context: `severity`, `criticality`, `verification_status`, `reaction_display`, `is_negated`, `evidence_text` help distinguish between drug allergy vs intolerance, and drive substance-specific codes.

```sql
WITH unmatched_allergies AS (
    SELECT allergy_id, substance_display, reaction_display, severity, criticality,
           verification_status, is_negated, evidence_text
    FROM ALLERGY_INTOLERANCE
    WHERE substance_code IS NULL AND substance_display IS NOT NULL
)
SELECT
    a.allergy_id,
    a.substance_display,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        CONCAT(
            'You are a clinical terminology expert. Given the extracted allergy/intolerance AND its context, select the MOST SPECIFIC code. ',
            'For drug allergies, prefer RxNorm. For food/environmental, prefer SNOMED CT. ',
            'Use context:\n',
            '- reaction → type of reaction helps confirm substance identification\n',
            '- severity/criticality → may distinguish allergy vs intolerance\n',
            '- is_negated → if TRUE, this is a DENIED allergy (return confidence 0.0)\n',
            '- evidence_text → original note citation\n\n',
            'Return ONLY: {"code": "<code>", "code_system": "<RxNorm or SNOMED CT>", "confidence": <0.0-1.0>}.\n\n',
            '--- ALLERGY ---\n',
            'Substance: "', a.substance_display, '"\n',
            'Reaction: ', COALESCE(a.reaction_display, 'NOT_SPECIFIED'), '\n',
            'Severity: ', COALESCE(a.severity, 'NOT_SPECIFIED'), '\n',
            'Criticality: ', COALESCE(a.criticality, 'NOT_SPECIFIED'), '\n',
            'Verification: ', COALESCE(a.verification_status, 'NOT_SPECIFIED'), '\n',
            'Negated: ', a.is_negated::VARCHAR, '\n',
            'Evidence Text: "', COALESCE(a.evidence_text, ''), '"\n\n',
            '--- CANDIDATES ---\n',
            (SELECT LISTAGG(CONCAT(cd2.code, ' | ', cs2.name, ' | ', cd2.display), '\n')
             FROM CONCEPT_DIMENSION cd2
             JOIN CODE_SYSTEM cs2 ON cd2.code_system_id = cs2.code_system_id
             WHERE cd2.semantic_group IN ('MEDICATION', 'OTHER')
             AND (CONTAINS(UPPER(cd2.display), UPPER(SPLIT_PART(a.substance_display, ' ', 1)))
                  OR CONTAINS(UPPER(a.substance_display), UPPER(SPLIT_PART(cd2.display, ' ', 1))))
             LIMIT 20)
        )
    ) AS match_result
FROM unmatched_allergies a;
```

## Preferred Code System Priority

**Conditional on `$NORM_CODE_SYSTEMS`**: This table is a fallback for `ALL` or multi-system selections only. It does NOT override the user's explicit preference.

| Table | Priority 1 | Priority 2 | Priority 3 |
|-------|-----------|-----------|-----------|
| MEDICATION_REQUEST | **RxNorm** (always) | SNOMED CT | — |
| PROCEDURE | **CPT** | ICD-10-PCS | SNOMED CT |
| ALLERGY_INTOLERANCE (substance) | **RxNorm** (drugs) | SNOMED CT (food/env) | — |
| ALLERGY_INTOLERANCE (reaction) | **SNOMED CT** | — | — |

## Route/Frequency Normalization (Secondary)

MEDICATION_REQUEST has `route_code`/`route_display` and `frequency_norm`. These use small SNOMED CT subsets:

```sql
UPDATE MEDICATION_REQUEST m
SET m.route_code = cd.code
FROM CONCEPT_DIMENSION cd
JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
WHERE UPPER(TRIM(m.route_display)) = UPPER(TRIM(cd.display))
  AND cd.semantic_group = 'ROUTE'
  AND cs.name = 'SNOMED CT'
  AND m.route_code IS NULL
  AND m.route_display IS NOT NULL;
```

## Body Site Normalization (Secondary — PROCEDURE)

```sql
UPDATE PROCEDURE p
SET p.body_site_code = cd.code
FROM CONCEPT_DIMENSION cd
JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
WHERE UPPER(TRIM(p.body_site_display)) = UPPER(TRIM(cd.display))
  AND cd.semantic_group = 'ANATOMY'
  AND cs.name = 'SNOMED CT'
  AND p.body_site_code IS NULL
  AND p.body_site_display IS NOT NULL;
```

## Validation

```sql
SELECT table_name, total, coded, ROUND(coded * 100.0 / NULLIF(total, 0), 1) AS rate_pct
FROM (
    SELECT 'MEDICATION_REQUEST' AS table_name, COUNT(*) AS total,
           COUNT(medication_code) AS coded FROM MEDICATION_REQUEST WHERE medication_display IS NOT NULL
    UNION ALL
    SELECT 'PROCEDURE', COUNT(*), COUNT(code) FROM PROCEDURE WHERE display IS NOT NULL
    UNION ALL
    SELECT 'ALLERGY_INTOLERANCE', COUNT(*), COUNT(substance_code) FROM ALLERGY_INTOLERANCE WHERE substance_display IS NOT NULL
);
```

Target: >= 85% coding rate for medications (RxNorm coverage is high), >= 75% for procedures.

## Output

UPDATEd code columns on MEDICATION_REQUEST, PROCEDURE, and ALLERGY_INTOLERANCE. Returns dict with counts per entity type.
