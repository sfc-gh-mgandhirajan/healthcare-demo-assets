---
name: normalization-observations
description: "Normalize extracted labs, vitals, exam findings, and clinical scores to standard terminology codes (LOINC, SNOMED CT) using CONCEPT_DIMENSION lookup with Cortex AI fuzzy matching fallback."
parent_skill: hcls-provider-cdata-clinical-nlp
---

# Observations Terminology Normalization

## Scope

| Target Table | Code Fields | Code Systems | Semantic Groups |
|-------------|------------|--------------|-----------------|
| OBSERVATION | `code`, `code_system` | LOINC, SNOMED CT | LAB, SCORE (labs/vitals/scores); DISEASE, SYMPTOM (exam/imaging findings) |

This sub-skill runs **after** extraction (extraction-observations) and populates terminology codes on rows where `code` IS NULL but `display` is populated.

## Code System Preference (`$NORM_CODE_SYSTEMS`)

This sub-skill receives `$NORM_CODE_SYSTEMS` from the router's Terminology Preference Gate:

| `$NORM_CODE_SYSTEMS` | Effect |
|---------------------|--------|
| `LOINC` | Labs/vitals → LOINC only; exam findings → skip or LOINC where available |
| `SNOMED CT` | Exam findings → SNOMED CT; labs → SNOMED CT observable entities |
| `LOINC,SNOMED CT` | Labs/vitals → LOINC primary; exam findings → SNOMED CT |
| `ALL` | LOINC for labs/vitals/scores, SNOMED CT for exam findings |

> **LOINC is strongly recommended for labs and vitals** — it is the universal standard and provides axis-level specificity (component, property, timing, system, scale, method) that SNOMED CT cannot match for observations.

**If `$NORM_CODE_SYSTEMS` is not set**, prompt the user via the router gate before proceeding.

## Special Consideration: Observations Have High LOINC Affinity

Unlike conditions (which split between ICD-10-CM and SNOMED CT), observations have a strong natural mapping to LOINC:
- **Lab results** → LOINC (e.g., "Hemoglobin" → 718-7)
- **Vitals** → LOINC vital sign panel (e.g., "Blood Pressure Systolic" → 8480-6)
- **Clinical scores** → LOINC (e.g., "Glasgow Coma Scale" → 9269-2)
- **Exam findings** → SNOMED CT (e.g., "Wheezing" → 56018004)

## Architecture

```
OBSERVATION (display populated, code NULL)
    |
    v  Step 1: Exact match (category-aware semantic groups)
CONCEPT_DIMENSION (LAB/SCORE for labs/vitals/scores; DISEASE/SYMPTOM for exam/imaging)
    |
    v  Step 2: Vital sign shorthand mapping (deterministic)
Known vitals abbreviation table
    |
    v  Step 3: Fuzzy match via Cortex AI (remaining)
LLM candidate selection from CONCEPT_DIMENSION
    |
    v  Step 4: UPDATE
OBSERVATION.code, OBSERVATION.code_system populated
```

## Step 1: Exact Match Lookup

```sql
UPDATE OBSERVATION o
SET o.code = cd.code,
    o.code_system = cs.name
FROM CONCEPT_DIMENSION cd
JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
WHERE UPPER(TRIM(o.display)) = UPPER(TRIM(cd.display))
  AND (
      (o.category IN ('LAB', 'VITAL_SIGNS', 'SCORE') AND cd.semantic_group IN ('LAB', 'SCORE'))
      OR
      (o.category IN ('EXAM', 'IMAGING') AND cd.semantic_group IN ('DISEASE', 'SYMPTOM'))
  )
  AND cs.name IN ($NORM_CODE_SYSTEMS)
  AND o.code IS NULL
  AND o.display IS NOT NULL;
```

## Step 2: Vital Sign Shorthand Mapping

Regex-extracted vitals use standardized display names. Map these deterministically to LOINC.

> **`$NORM_CODE_SYSTEMS` guard**: Only run this step if LOINC is in the user's preference (`$NORM_CODE_SYSTEMS` contains `LOINC` or equals `ALL`). Skip entirely if the user selected `SNOMED CT` only.

```sql
UPDATE OBSERVATION
SET code = CASE UPPER(TRIM(display))
    WHEN 'BLOOD PRESSURE SYSTOLIC' THEN '8480-6'
    WHEN 'BLOOD PRESSURE DIASTOLIC' THEN '8462-4'
    WHEN 'HEART RATE' THEN '8867-4'
    WHEN 'TEMPERATURE' THEN '8310-5'
    WHEN 'RESPIRATORY RATE' THEN '9279-1'
    WHEN 'SPO2' THEN '2708-6'
    WHEN 'OXYGEN SATURATION' THEN '2708-6'
    WHEN 'WEIGHT' THEN '29463-7'
    WHEN 'HEIGHT' THEN '8302-2'
    WHEN 'BMI' THEN '39156-5'
    WHEN 'GLASGOW COMA SCALE' THEN '9269-2'
    WHEN 'PAIN SCORE' THEN '38208-5'
    WHEN 'APGAR SCORE' THEN '9272-6'
    WHEN 'FALLS RISK SCORE' THEN '73830-2'
    WHEN 'BRADEN SCORE' THEN '38227-5'
    WHEN 'PHQ-9' THEN '44261-6'
    WHEN 'GAD-7' THEN '70274-6'
    WHEN 'MMSE' THEN '72106-8'
    WHEN 'MOCA' THEN '72172-0'
    ELSE NULL
END,
    code_system = 'LOINC'
WHERE code IS NULL
  AND display IS NOT NULL
  AND UPPER(TRIM(display)) IN (
    'BLOOD PRESSURE SYSTOLIC','BLOOD PRESSURE DIASTOLIC','HEART RATE','TEMPERATURE',
    'RESPIRATORY RATE','SPO2','OXYGEN SATURATION','WEIGHT','HEIGHT','BMI',
    'GLASGOW COMA SCALE','PAIN SCORE','APGAR SCORE','FALLS RISK SCORE','BRADEN SCORE',
    'PHQ-9','GAD-7','MMSE','MOCA'
  );
```

## Step 3: Fuzzy Match via Cortex AI

For remaining unmatched observations, use **full clinical context**. The prompt adapts based on category:
- **LAB / VITAL_SIGNS / SCORE**: LOINC axis matching (component + property + timing + system + scale + method) using value, unit, method, body site
- **EXAM / IMAGING**: SNOMED CT clinical finding matching using body site, laterality, interpretation, evidence text

```sql
WITH unmatched AS (
    SELECT observation_id, display, category, status, value_quantity, value_unit,
           value_display, interpretation, method, body_site_display, laterality,
           is_negated, temporality, certainty, evidence_text
    FROM OBSERVATION
    WHERE code IS NULL AND display IS NOT NULL
)
SELECT
    u.observation_id,
    u.display AS extracted_text,
    u.category,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        CONCAT(
            'You are a clinical terminology expert specializing in observation coding. ',
            'Given the extracted observation AND its clinical context, select the MOST SPECIFIC concept. ',
            'IMPORTANT: The user has requested coding in: ', $NORM_CODE_SYSTEMS_DISPLAY, '. Only return codes from the requested system(s).\n',
            CASE
                WHEN u.category IN ('EXAM', 'IMAGING') THEN CONCAT(
                    'This is a physical exam / imaging FINDING — match to a SNOMED CT clinical finding concept.\n',
                    'Use context to drive finding specificity:\n',
                    '- body_site + laterality → anatomical qualifier on the finding\n',
                    '- interpretation → confirms the clinical significance (normal vs abnormal)\n',
                    '- is_negated → if TRUE, this finding was ABSENT (return confidence 0.0)\n',
                    '- evidence_text → original note may describe quality, distribution, or severity of finding\n\n'
                )
                ELSE CONCAT(
                    'This is a LAB / VITAL / SCORE — match to a LOINC concept.\n',
                    'LOINC uses a 6-axis model. Use ALL available context to select the correct axis values:\n',
                    '- Component (what is measured): driven by display text\n',
                    '- Property (kind of quantity): driven by value_unit (mass → MCnc, volume → VCnc, ratio → MFr, count → NCnc)\n',
                    '- Timing (point vs duration): look in evidence_text for fasting, random, timed, 24h, peak, trough — e.g., Glucose fasting (1558-6) vs random (2345-7)\n',
                    '- System (specimen): driven by body_site (blood, serum, plasma, urine, CSF, etc.)\n',
                    '- Scale (result type): quantitative (numeric value) vs ordinal (trace/1+/2+/3+) vs narrative (free text) — e.g., Urine protein quantitative (2888-6) vs ordinal (20454-5)\n',
                    '- Method (technique): driven by method field (automated, manual, immunoassay, culture, etc.)\n\n'
                )
            END,
            'Return ONLY: {"code": "<code>", "code_system": "<LOINC or SNOMED CT>", "confidence": <0.0-1.0>}.\n\n',
            '--- OBSERVATION ---\n',
            'Display: "', u.display, '"\n',
            'Category: ', COALESCE(u.category, 'UNKNOWN'), '\n',
            'Status: ', COALESCE(u.status, 'NOT_SPECIFIED'), '\n',
            'Value: ', COALESCE(u.value_quantity::VARCHAR, COALESCE(u.value_display, 'NOT_SPECIFIED')), '\n',
            'Unit: ', COALESCE(u.value_unit, 'NOT_SPECIFIED'), '\n',
            'Interpretation: ', COALESCE(u.interpretation, 'NOT_SPECIFIED'), '\n',
            'Method: ', COALESCE(u.method, 'NOT_SPECIFIED'), '\n',
            'Body Site: ', COALESCE(u.body_site_display, 'NOT_SPECIFIED'), '\n',
            'Laterality: ', COALESCE(u.laterality, 'NOT_SPECIFIED'), '\n',
            'Negated: ', u.is_negated::VARCHAR, '\n',
            'Temporality: ', COALESCE(u.temporality, 'NOT_SPECIFIED'), '\n',
            'Certainty: ', COALESCE(u.certainty, 'NOT_SPECIFIED'), '\n',
            'Evidence Text: "', COALESCE(u.evidence_text, ''), '"\n\n',
            '--- CANDIDATES ---\n',
            (SELECT LISTAGG(CONCAT(cd2.code, ' | ', cs2.name, ' | ', cd2.display), '\n')
             FROM CONCEPT_DIMENSION cd2
             JOIN CODE_SYSTEM cs2 ON cd2.code_system_id = cs2.code_system_id
             WHERE (
                 (u.category IN ('LAB', 'VITAL_SIGNS', 'SCORE') AND cd2.semantic_group IN ('LAB', 'SCORE'))
                 OR
                 (u.category IN ('EXAM', 'IMAGING') AND cd2.semantic_group IN ('DISEASE', 'SYMPTOM'))
             )
             AND cs2.name IN ($NORM_CODE_SYSTEMS)   -- filter by user preference
             AND (CONTAINS(UPPER(cd2.display), UPPER(SPLIT_PART(u.display, ' ', 1)))
                  OR CONTAINS(UPPER(u.display), UPPER(SPLIT_PART(cd2.display, ' ', 1))))
             LIMIT 20)
        )
    ) AS match_result
FROM unmatched u;
```

## Preferred Code System Priority

**Conditional on `$NORM_CODE_SYSTEMS`**: This table is a fallback for `ALL` or multi-system selections only. It does NOT override the user's explicit preference.

| Category | Priority 1 | Priority 2 | Rationale |
|----------|-----------|-----------|-----------|
| LAB | **LOINC** | SNOMED CT | LOINC is the standard for lab orders/results |
| VITAL_SIGNS | **LOINC** | — | LOINC vital sign panel is universal |
| SCORE | **LOINC** | SNOMED CT | Most clinical scores have LOINC codes |
| EXAM | **SNOMED CT** | — | Physical exam findings map to SNOMED CT |
| IMAGING | **SNOMED CT** | LOINC | Imaging observations use SNOMED CT findings |

## Value Code Normalization (Secondary)

OBSERVATION also has `value_code`/`value_code_system` for coded observation values (e.g., "positive"/"negative" results):

```sql
UPDATE OBSERVATION
SET value_code = CASE UPPER(TRIM(value_display))
    WHEN 'POSITIVE' THEN '10828004'
    WHEN 'NEGATIVE' THEN '260385009'
    WHEN 'NORMAL' THEN '17621005'
    WHEN 'ABNORMAL' THEN '263654008'
    WHEN 'HIGH' THEN '75540009'
    WHEN 'LOW' THEN '62482003'
    WHEN 'CRITICAL' THEN '371924009'
    ELSE NULL
END,
    value_code_system = 'SNOMED CT'
WHERE value_code IS NULL AND value_display IS NOT NULL
  AND UPPER(TRIM(value_display)) IN ('POSITIVE','NEGATIVE','NORMAL','ABNORMAL','HIGH','LOW','CRITICAL');
```

## Body Site Normalization (Secondary)

```sql
UPDATE OBSERVATION o
SET o.body_site_code = cd.code
FROM CONCEPT_DIMENSION cd
WHERE UPPER(TRIM(o.body_site_display)) = UPPER(TRIM(cd.display))
  AND cd.semantic_group = 'ANATOMY'
  AND o.body_site_code IS NULL
  AND o.body_site_display IS NOT NULL;
```

## Validation

```sql
SELECT
    category,
    COUNT(*) AS total,
    COUNT(code) AS coded,
    ROUND(COUNT(code) * 100.0 / NULLIF(COUNT(*), 0), 1) AS rate_pct
FROM OBSERVATION
WHERE display IS NOT NULL
GROUP BY category
ORDER BY category;
```

Target: >= 90% coding rate for vitals (deterministic mapping), >= 80% for labs, >= 70% for exam findings.

## Output

UPDATEd code/code_system on OBSERVATION table. Returns dict with vital_deterministic and observation_exact counts.
