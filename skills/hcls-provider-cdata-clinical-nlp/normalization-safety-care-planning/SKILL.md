---
name: normalization-safety-care-planning
description: "Normalize extracted adverse events and care plan items to standard terminology codes (MedDRA, SNOMED CT) using CONCEPT_DIMENSION lookup with Cortex AI fuzzy matching fallback."
parent_skill: hcls-provider-cdata-clinical-nlp
---

# Safety & Care Planning Terminology Normalization

## Scope

| Target Table | Code Fields | Code Systems | Semantic Groups |
|-------------|------------|--------------|-----------------|
| ADVERSE_EVENT | `event_code`, `event_system` | MedDRA, SNOMED CT | DISEASE, SYMPTOM |
| CARE_PLAN_ITEM | — | — | — |

> **CARE_PLAN_ITEM** does not have terminology code fields — its `item_type` (GOAL, ACTION, REFERRAL, FOLLOW_UP) and `description` are free text by design. No normalization is needed for this table.

This sub-skill focuses on **ADVERSE_EVENT** normalization only.

## Code System Preference (`$NORM_CODE_SYSTEMS`)

This sub-skill receives `$NORM_CODE_SYSTEMS` from the router's Terminology Preference Gate:

| `$NORM_CODE_SYSTEMS` | Effect |
|---------------------|--------|
| `MedDRA` | MedDRA PT only; skip SNOMED CT fallback |
| `SNOMED CT` | SNOMED CT only; skip MedDRA |
| `MedDRA,SNOMED CT` | MedDRA primary, SNOMED CT fallback (current architecture) |
| `ALL` | MedDRA primary, SNOMED CT fallback |

> **MedDRA is strongly recommended for adverse events** — it is the regulatory standard for FAERS, EMA EudraVigilance, and ICH E2B reporting.

**If `$NORM_CODE_SYSTEMS` is not set**, prompt the user via the router gate before proceeding.

## Special Consideration: MedDRA for Adverse Events

Adverse events in pharmacovigilance use **MedDRA** (Medical Dictionary for Regulatory Activities) as the preferred terminology:
- **Preferred Term (PT)** level for individual event coding
- **System Organ Class (SOC)** for grouping
- MedDRA is the standard for FAERS, EMA EudraVigilance, and ICH E2B reporting
- SNOMED CT can be used as a secondary system for clinical detail

## Architecture

```
ADVERSE_EVENT (event_display populated, event_code NULL)
    |
    v  Step 1: Exact match
CONCEPT_DIMENSION (semantic_group IN ('DISEASE','SYMPTOM'), code_system = 'MedDRA')
    |
    v  Step 2: SNOMED CT fallback exact match
CONCEPT_DIMENSION (code_system = 'SNOMED CT')
    |
    v  Step 3: Fuzzy match via Cortex AI
LLM with MedDRA-specific candidates
    |
    v  Step 4: UPDATE event_code, event_system
```

## Step 1: Exact Match — MedDRA

> **`$NORM_CODE_SYSTEMS` guard**: Only run if MedDRA is in the user's preference (`$NORM_CODE_SYSTEMS` contains `MedDRA` or equals `ALL`). Skip if user selected `SNOMED CT` only.

```sql
UPDATE ADVERSE_EVENT a
SET a.event_code = cd.code,
    a.event_system = cs.name
FROM CONCEPT_DIMENSION cd
JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
WHERE UPPER(TRIM(a.event_display)) = UPPER(TRIM(cd.display))
  AND cs.name = 'MedDRA'
  AND cd.semantic_group IN ('DISEASE', 'SYMPTOM')
  AND a.event_code IS NULL
  AND a.event_display IS NOT NULL;
```

## Step 2: SNOMED CT Fallback

> **`$NORM_CODE_SYSTEMS` guard**: Only run if SNOMED CT is in the user's preference (`$NORM_CODE_SYSTEMS` contains `SNOMED CT` or equals `ALL`). Skip if user selected `MedDRA` only.

For events not found in MedDRA (or if Step 1 was skipped), try SNOMED CT:

```sql
UPDATE ADVERSE_EVENT a
SET a.event_code = cd.code,
    a.event_system = cs.name
FROM CONCEPT_DIMENSION cd
JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
WHERE UPPER(TRIM(a.event_display)) = UPPER(TRIM(cd.display))
  AND cs.name = 'SNOMED CT'
  AND cd.semantic_group IN ('DISEASE', 'SYMPTOM')
  AND a.event_code IS NULL
  AND a.event_display IS NOT NULL;
```

## Step 3: Fuzzy Match via Cortex AI

For unmatched adverse events, use **full clinical context**. Seriousness, severity, outcome, causality linkage, and evidence text are critical for MedDRA PT selection — e.g., "liver injury" could be "Hepatotoxicity" (PT 10019851) or "Drug-induced liver injury" (PT 10072268) depending on causality context.

```sql
WITH unmatched AS (
    SELECT ae.adverse_event_id, ae.event_display, ae.seriousness, ae.severity, ae.outcome,
           ae.onset_datetime, ae.resolution_datetime,
           mr.medication_display AS suspect_medication_display,
           ae.suspect_device_id,
           ae.evidence_text
    FROM ADVERSE_EVENT ae
    LEFT JOIN MEDICATION_REQUEST mr ON ae.suspect_medication_request_id = mr.medication_request_id
    WHERE ae.event_code IS NULL AND ae.event_display IS NOT NULL
)
SELECT
    u.adverse_event_id,
    u.event_display,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        CONCAT(
            'You are a pharmacovigilance coding expert specializing in MedDRA and clinical terminology. ',
            'Given an adverse event description AND its clinical context, find the MOST SPECIFIC code. ',
            'IMPORTANT: The user has requested coding in: ', $NORM_CODE_SYSTEMS_DISPLAY, '. Only return codes from the requested system(s).\n\n',
            'MedDRA hierarchy: Always return a Preferred Term (PT) level code — NOT SOC, HLGT, HLT, or LLT.\n',
            'If the extracted text maps to a Lowest Level Term (LLT), return the parent PT instead.\n',
            'Example: "feeling nauseous" (LLT) → return PT "Nausea" (10028813), NOT SOC "Gastrointestinal disorders".\n\n',
            'Use context to drive PT selection:\n',
            '- seriousness → serious vs non-serious affects PT selection (e.g., "Headache" vs "Intracranial hypertension")\n',
            '- severity → mild/moderate/severe may indicate different PTs\n',
            '- outcome → fatal/resolved/ongoing may shift to more specific PT (e.g., "Death" PT if fatal)\n',
            '- onset/resolution → duration context distinguishes acute vs chronic PTs\n',
            '- suspect medication → drug-induced PTs (e.g., "Drug-induced liver injury" vs "Hepatotoxicity")\n',
            '- suspect device → device-related PTs\n',
            '- evidence_text → original note may have specific clinical details\n\n',
            'Return ONLY: {"code": "<code>", "code_system": "<MedDRA or SNOMED CT>", "confidence": <0.0-1.0>}.\n\n',
            '--- ADVERSE EVENT ---\n',
            'Event: "', u.event_display, '"\n',
            'Seriousness: ', COALESCE(u.seriousness, 'NOT_SPECIFIED'), '\n',
            'Severity: ', COALESCE(u.severity, 'NOT_SPECIFIED'), '\n',
            'Outcome: ', COALESCE(u.outcome, 'NOT_SPECIFIED'), '\n',
            'Onset: ', COALESCE(u.onset_datetime::VARCHAR, 'NOT_SPECIFIED'), '\n',
            'Resolution: ', COALESCE(u.resolution_datetime::VARCHAR, 'NOT_SPECIFIED'), '\n',
            'Suspect Medication: ', COALESCE(u.suspect_medication_display, 'NONE'), '\n',
            'Suspect Device: ', COALESCE(u.suspect_device_id, 'NONE'), '\n',
            'Evidence Text: "', COALESCE(u.evidence_text, ''), '"\n\n',
            CASE WHEN $NORM_CODE_SYSTEMS LIKE '%MedDRA%' OR $NORM_CODE_SYSTEMS = 'ALL'
                THEN CONCAT(
                    '--- MedDRA CANDIDATES (PT level) ---\n',
                    (SELECT LISTAGG(CONCAT(cd2.code, ' | MedDRA PT | ', cd2.display), '\n')
                     FROM CONCEPT_DIMENSION cd2
                     JOIN CODE_SYSTEM cs2 ON cd2.code_system_id = cs2.code_system_id
                     WHERE cs2.name = 'MedDRA'
                     AND (CONTAINS(UPPER(cd2.display), UPPER(SPLIT_PART(u.event_display, ' ', 1)))
                          OR CONTAINS(UPPER(u.event_display), UPPER(SPLIT_PART(cd2.display, ' ', 1))))
                     LIMIT 15),
                    '\n\n')
                ELSE ''
            END,
            CASE WHEN $NORM_CODE_SYSTEMS LIKE '%SNOMED CT%' OR $NORM_CODE_SYSTEMS = 'ALL'
                THEN CONCAT(
                    '--- SNOMED CT CANDIDATES ---\n',
                    (SELECT LISTAGG(CONCAT(cd3.code, ' | SNOMED CT | ', cd3.display), '\n')
                     FROM CONCEPT_DIMENSION cd3
                     JOIN CODE_SYSTEM cs3 ON cd3.code_system_id = cs3.code_system_id
                     WHERE cs3.name = 'SNOMED CT'
                     AND cd3.semantic_group IN ('DISEASE', 'SYMPTOM')
                     AND (CONTAINS(UPPER(cd3.display), UPPER(SPLIT_PART(u.event_display, ' ', 1))))
                     LIMIT 10))
                ELSE ''
            END
        )
    ) AS match_result
FROM unmatched u;
```

## Preferred Code System Priority

**Conditional on `$NORM_CODE_SYSTEMS`**: This table is a fallback for `ALL` or multi-system selections only. It does NOT override the user's explicit preference.

| Priority | Code System | Typical Use Case |
|----------|------------|------------------|
| 1 | **MedDRA** | Pharmacovigilance, regulatory reporting (FAERS, EMA, ICH E2B) |
| 2 | **SNOMED CT** | Clinical detail, interoperability, when MedDRA has no match |

## Common Adverse Event Mappings

| Event | MedDRA PT Code | MedDRA PT |
|-------|---------------|-----------|
| Nausea | 10028813 | Nausea |
| Rash | 10037844 | Rash |
| Dizziness | 10013573 | Dizziness |
| Headache | 10019211 | Headache |
| Anaphylaxis | 10002198 | Anaphylactic reaction |
| Thrombocytopenia | 10043554 | Thrombocytopenia |
| Hepatotoxicity | 10019851 | Hepatotoxicity |
| QT prolongation | 10014387 | Electrocardiogram QT prolonged |

## Validation

```sql
SELECT
    COUNT(*) AS total_events,
    COUNT(event_code) AS coded_events,
    ROUND(COUNT(event_code) * 100.0 / NULLIF(COUNT(*), 0), 1) AS coding_rate_pct,
    COUNT(CASE WHEN event_system = 'MedDRA' THEN 1 END) AS meddra_count,
    COUNT(CASE WHEN event_system = 'SNOMED CT' THEN 1 END) AS snomed_count
FROM ADVERSE_EVENT
WHERE event_display IS NOT NULL;
```

Target: >= 80% coding rate for well-formed adverse event descriptions.

## Output

UPDATEd event_code/event_system on ADVERSE_EVENT. Returns dict with adverse_event_exact count.
