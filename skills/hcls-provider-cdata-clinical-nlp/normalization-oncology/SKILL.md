---
name: normalization-oncology
description: "Normalize extracted tumor episodes, histology, and staging data to standard terminology codes (ICD-O-3, SNOMED CT, AJCC) using CONCEPT_DIMENSION lookup with Cortex AI fuzzy matching fallback."
parent_skill: hcls-provider-cdata-clinical-nlp
---

# Oncology Terminology Normalization

## Scope

| Target Table | Code Fields | Code Systems | Semantic Groups |
|-------------|------------|--------------|-----------------|
| TUMOR_EPISODE | `primary_site_code`, `primary_site_system` | ICD-O-3 (topography), SNOMED CT | ANATOMY, TUMOR |
| TUMOR_EPISODE | `histology_code`, `histology_system` | ICD-O-3 (morphology), SNOMED CT | TUMOR |

This sub-skill runs **after** extraction (extraction-oncology) and populates terminology codes on rows where code fields are NULL but display text is populated.

## Code System Preference (`$NORM_CODE_SYSTEMS`)

This sub-skill receives `$NORM_CODE_SYSTEMS` from the router's Terminology Preference Gate:

| `$NORM_CODE_SYSTEMS` | Primary Site | Histology |
|---------------------|-------------|-----------|
| `ICD-O-3` | ICD-O-3 topography (C00-C80) | ICD-O-3 morphology (8000-9999/behavior) |
| `SNOMED CT` | SNOMED CT body structure | SNOMED CT morphologic abnormality |
| `ICD-O-3,SNOMED CT` | ICD-O-3 primary | ICD-O-3 primary |
| `ALL` | ICD-O-3 primary | ICD-O-3 primary |

> **ICD-O-3 is strongly recommended for oncology** — it is the standard for cancer registries and provides morphology behavior codes (/0 benign, /2 in situ, /3 malignant) that SNOMED CT does not encode in the same way.

**If `$NORM_CODE_SYSTEMS` is not set**, prompt the user via the router gate before proceeding.

## Special Considerations

Oncology coding has unique characteristics:
- **Primary site** uses ICD-O-3 topography codes (C00-C80) — these describe anatomical location
- **Histology** uses ICD-O-3 morphology codes (8000-9999/behavior) — these describe cell type and behavior
- **Staging** (TNM, stage group) does NOT need terminology codes — it uses AJCC edition-specific values already captured as text
- **Grade** is a small fixed enum (G1-G4, GX) — no CONCEPT_DIMENSION lookup needed

## Architecture

```
TUMOR_EPISODE (display populated, code NULL)
    |
    v  Step 1: Primary site — exact match
CONCEPT_DIMENSION (semantic_group = 'ANATOMY' or 'TUMOR')
    |
    v  Step 2: Histology — exact match
CONCEPT_DIMENSION (semantic_group = 'TUMOR')
    |
    v  Step 3: Fuzzy match via Cortex AI (unmatched)
LLM with oncology-specific candidates
    |
    v  Step 4: UPDATE code fields
```

## Step 1: Exact Match — Primary Site

```sql
UPDATE TUMOR_EPISODE t
SET t.primary_site_code = cd.code,
    t.primary_site_system = cs.name
FROM CONCEPT_DIMENSION cd
JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
WHERE UPPER(TRIM(t.primary_site_display)) = UPPER(TRIM(cd.display))
  AND cd.semantic_group IN ('ANATOMY', 'TUMOR')
  AND cs.name IN ($NORM_CODE_SYSTEMS)   -- respects user preference
  AND t.primary_site_code IS NULL
  AND t.primary_site_display IS NOT NULL;
```

## Step 2: Exact Match — Histology

```sql
UPDATE TUMOR_EPISODE t
SET t.histology_code = cd.code,
    t.histology_system = cs.name
FROM CONCEPT_DIMENSION cd
JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
WHERE UPPER(TRIM(t.histology_display)) = UPPER(TRIM(cd.display))
  AND cd.semantic_group = 'TUMOR'
  AND cs.name IN ($NORM_CODE_SYSTEMS)   -- respects user preference
  AND t.histology_code IS NULL
  AND t.histology_display IS NOT NULL;
```

## Step 3: Fuzzy Match via Cortex AI

Oncology terminology is highly specialized. The full tumor context (staging, grade, site, histology) is critical — e.g., behavior code (/0 benign, /2 in situ, /3 malignant) depends on grade and staging context.

### Fuzzy Match — Primary Site

```sql
WITH unmatched_sites AS (
    SELECT tumor_episode_id, primary_site_display, histology_display,
           stage_group, grade, tnm_t, tnm_n, tnm_m, certainty, evidence_text
    FROM TUMOR_EPISODE
    WHERE primary_site_code IS NULL AND primary_site_display IS NOT NULL
)
SELECT
    s.tumor_episode_id,
    s.primary_site_display,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        CONCAT(
            'You are an oncology coding expert specializing in tumor site terminology. ',
            'Given a tumor primary site description AND its clinical context, find the MOST SPECIFIC code. ',
            'IMPORTANT: The user has requested coding in: ', $NORM_CODE_SYSTEMS_DISPLAY, '. Only return codes from the requested system(s).\n\n',
            CASE WHEN $NORM_CODE_SYSTEMS LIKE '%ICD-O-3%' OR $NORM_CODE_SYSTEMS = 'ALL'
                THEN CONCAT(
                    'ICD-O-3 topography: Return a code in the range C00.0–C80.9. ',
                    'Use the most specific sub-site (e.g., C50.4 "Upper-outer quadrant of breast" rather than C50.9 "Breast, NOS") when the evidence supports it.\n',
                    'If laterality is specified in the evidence, note it but do NOT change the topography code (ICD-O-3 does not encode laterality in the code itself).\n\n')
                ELSE ''
            END,
            CASE WHEN $NORM_CODE_SYSTEMS LIKE '%SNOMED CT%' AND NOT ($NORM_CODE_SYSTEMS LIKE '%ICD-O-3%')
                THEN 'SNOMED CT body structure: Return the most specific body structure concept matching the anatomical site. Include laterality qualifiers if specified.\n\n'
                ELSE ''
            END,
            'Use context:\n',
            '- histology → helps disambiguate overlapping anatomical sites\n',
            '- staging/TNM → T-stage may indicate specific sub-site\n',
            '- evidence_text → pathology/radiology report may specify exact anatomical sub-location\n\n',
            'Return ONLY: {"code": "<code>", "code_system": "<system>", "confidence": <0.0-1.0>}.\n\n',
            '--- TUMOR CONTEXT ---\n',
            'Primary Site: "', s.primary_site_display, '"\n',
            'Histology: ', COALESCE(s.histology_display, 'NOT_SPECIFIED'), '\n',
            'Stage Group: ', COALESCE(s.stage_group, 'NOT_SPECIFIED'), '\n',
            'TNM: ', COALESCE(s.tnm_t, ''), ' ', COALESCE(s.tnm_n, ''), ' ', COALESCE(s.tnm_m, ''), '\n',
            'Grade: ', COALESCE(s.grade, 'NOT_SPECIFIED'), '\n',
            'Certainty: ', COALESCE(s.certainty, 'NOT_SPECIFIED'), '\n',
            'Evidence Text: "', COALESCE(s.evidence_text, ''), '"\n\n',
            '--- CANDIDATES ---\n',
            (SELECT LISTAGG(CONCAT(cd2.code, ' | ', cs2.name, ' | ', cd2.display), '\n')
             FROM CONCEPT_DIMENSION cd2
             JOIN CODE_SYSTEM cs2 ON cd2.code_system_id = cs2.code_system_id
             WHERE cd2.semantic_group IN ('ANATOMY', 'TUMOR')
             AND cs2.name IN ($NORM_CODE_SYSTEMS)
             AND (CONTAINS(UPPER(cd2.display), UPPER(SPLIT_PART(s.primary_site_display, ' ', 1)))
                  OR CONTAINS(UPPER(s.primary_site_display), UPPER(SPLIT_PART(cd2.display, ' ', 1))))
             LIMIT 20)
        )
    ) AS match_result
FROM unmatched_sites s;
```

### Fuzzy Match — Histology

```sql
WITH unmatched_hist AS (
    SELECT tumor_episode_id, histology_display, primary_site_display,
           stage_group, grade, tnm_t, tnm_n, tnm_m, certainty, evidence_text
    FROM TUMOR_EPISODE
    WHERE histology_code IS NULL AND histology_display IS NOT NULL
)
SELECT
    h.tumor_episode_id,
    h.histology_display,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        CONCAT(
            'You are an oncology coding expert specializing in tumor morphology terminology. ',
            'Given a histology description AND its clinical context, find the MOST SPECIFIC code. ',
            'IMPORTANT: The user has requested coding in: ', $NORM_CODE_SYSTEMS_DISPLAY, '. Only return codes from the requested system(s).\n\n',
            CASE WHEN $NORM_CODE_SYSTEMS LIKE '%ICD-O-3%' OR $NORM_CODE_SYSTEMS = 'ALL'
                THEN CONCAT(
                    'ICD-O-3 morphology: Return a code in the range 8000/0–9989/3.\n',
                    'CRITICAL: Include the behavior code after the slash:\n',
                    '- /0 = benign, /1 = uncertain, /2 = in situ, /3 = malignant primary, /6 = metastatic\n',
                    'Use context to determine behavior:\n',
                    '- grade → higher grade suggests malignant behavior\n',
                    '- stage_group/TNM → staging context confirms invasiveness (any T >T0 or N+/M+ → /3)\n',
                    '- "in situ", "non-invasive", "intraductal" in evidence_text → /2\n',
                    '- "metastatic" in evidence_text → /6\n\n')
                ELSE ''
            END,
            CASE WHEN $NORM_CODE_SYSTEMS LIKE '%SNOMED CT%' AND NOT ($NORM_CODE_SYSTEMS LIKE '%ICD-O-3%')
                THEN 'SNOMED CT morphologic abnormality: Return the most specific morphology concept. SNOMED CT encodes behavior via separate qualifier concepts rather than a slash code.\n\n'
                ELSE ''
            END,
            'Use context:\n',
            '- primary_site → site-specific morphology variants\n',
            '- evidence_text → pathology report details (differentiation, mitotic rate, etc.)\n\n',
            'Return ONLY: {"code": "<code>", "code_system": "<system>", "confidence": <0.0-1.0>}.\n\n',
            '--- TUMOR CONTEXT ---\n',
            'Histology: "', h.histology_display, '"\n',
            'Primary Site: ', COALESCE(h.primary_site_display, 'NOT_SPECIFIED'), '\n',
            'Stage Group: ', COALESCE(h.stage_group, 'NOT_SPECIFIED'), '\n',
            'TNM: ', COALESCE(h.tnm_t, ''), ' ', COALESCE(h.tnm_n, ''), ' ', COALESCE(h.tnm_m, ''), '\n',
            'Grade: ', COALESCE(h.grade, 'NOT_SPECIFIED'), '\n',
            'Certainty: ', COALESCE(h.certainty, 'NOT_SPECIFIED'), '\n',
            'Evidence Text: "', COALESCE(h.evidence_text, ''), '"\n\n',
            '--- CANDIDATES ---\n',
            (SELECT LISTAGG(CONCAT(cd2.code, ' | ', cs2.name, ' | ', cd2.display), '\n')
             FROM CONCEPT_DIMENSION cd2
             JOIN CODE_SYSTEM cs2 ON cd2.code_system_id = cs2.code_system_id
             WHERE cd2.semantic_group = 'TUMOR'
             AND cs2.name IN ($NORM_CODE_SYSTEMS)
             AND (CONTAINS(UPPER(cd2.display), UPPER(SPLIT_PART(h.histology_display, ' ', 1)))
                  OR CONTAINS(UPPER(h.histology_display), UPPER(SPLIT_PART(cd2.display, ' ', 1))))
             LIMIT 20)
        )
    ) AS match_result
FROM unmatched_hist h;
```

## Preferred Code System Priority

**Conditional on `$NORM_CODE_SYSTEMS`**: This table is a fallback for `ALL` or multi-system selections only. It does NOT override the user's explicit preference.

| Field | Priority 1 | Priority 2 | Rationale |
|-------|-----------|-----------|-----------|
| Primary Site | **ICD-O-3** (topography) | SNOMED CT | ICD-O-3 is standard for cancer registries |
| Histology | **ICD-O-3** (morphology) | SNOMED CT | ICD-O-3 morphology includes behavior code |

## Common Oncology Code Patterns

| Primary Site | ICD-O-3 | Display |
|-------------|---------|---------|
| Breast | C50.9 | Breast, NOS |
| Lung | C34.9 | Lung, NOS |
| Colon | C18.9 | Colon, NOS |
| Prostate | C61.9 | Prostate gland |
| Pancreas | C25.9 | Pancreas, NOS |

| Histology | ICD-O-3 | Display |
|-----------|---------|---------|
| Adenocarcinoma | 8140/3 | Adenocarcinoma, NOS |
| Squamous cell | 8070/3 | Squamous cell carcinoma, NOS |
| Small cell | 8041/3 | Small cell carcinoma, NOS |
| Ductal carcinoma in situ | 8500/2 | Intraductal carcinoma, noninfiltrating |
| Invasive ductal | 8500/3 | Infiltrating duct carcinoma |

## Fields That Do NOT Need Normalization

| Field | Reason |
|-------|--------|
| `stage_group` | Free text AJCC value (e.g., "Stage IIIA") — no code system mapping |
| `tnm_t`, `tnm_n`, `tnm_m` | AJCC TNM values — already structured |
| `grade` | Small enum (G1-G4, GX) — no CONCEPT_DIMENSION lookup needed |
| `performance_status_scale` | Known scales (ECOG, Karnofsky) — small enum |
| `response_status` | Free text (CR, PR, SD, PD) — no standard coding |

## Validation

```sql
SELECT
    'Primary Site' AS field,
    COUNT(*) AS total,
    COUNT(primary_site_code) AS coded,
    ROUND(COUNT(primary_site_code) * 100.0 / NULLIF(COUNT(*), 0), 1) AS rate_pct
FROM TUMOR_EPISODE WHERE primary_site_display IS NOT NULL
UNION ALL
SELECT 'Histology', COUNT(*), COUNT(histology_code),
    ROUND(COUNT(histology_code) * 100.0 / NULLIF(COUNT(*), 0), 1)
FROM TUMOR_EPISODE WHERE histology_display IS NOT NULL;
```

Target: >= 85% for primary site, >= 80% for histology (ICD-O-3 morphology can be complex).

## Output

UPDATEd primary_site_code/system and histology_code/system on TUMOR_EPISODE. Returns dict with site and histology counts.
