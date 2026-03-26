---
name: pipeline-implementation
description: "Production-grade Snowflake pipeline implementation pattern for clinical NLP. Covers Dynamic Tables for medical concept extraction, Snowpark Stored Procedure for terminology normalization, MERGE tasks, streams, and serving layer. Use when deploying clinical NLP as an automated pipeline."
parent_skill: hcls-provider-cdata-clinical-nlp
platform_skills_required:
  - dynamic-tables
  - snowpark-python
  - cortex-ai-functions
  - data-governance
  - developing-with-streamlit
  - search-optimization
---

# Production Pipeline Implementation

## Assumptions

1. **NOTE_DOCUMENT table is already populated** — the `hcls-provider-cdata-clinical-docs` skill handles document ingestion, parsing, and raw text extraction. This pipeline reads from `NOTE_DOCUMENT.raw_text`.
2. **Data model DDL is already deployed** — the `data-model-knowledge` sub-skill provides setup scripts for all 17 tables.
3. **CODE_SYSTEM and CONCEPT_DIMENSION tables are seeded** — terminology reference data is loaded before normalization runs.

## Architecture

```
NOTE_DOCUMENT (raw_text populated by clinical-docs skill)
        |
        v
┌──────────────────────────────────────────────────┐
│  EXTRACTION LAYER — Dynamic Tables               │
│  6 DTs, one per concept category                 │
│  Cortex COMPLETE → FLATTEN → typed columns       │
│  TARGET_LAG = DOWNSTREAM                         │
│  $dynamic-tables skill for creation/monitoring   │
└──────────────────────────────────────────────────┘
        |
        v
┌──────────────────────────────────────────────────┐
│  MERGE LAYER — Task                              │
│  MERGE DT output → 10 clinical tables            │
│  Deduplicates, handles EHR_STRUCTURED coexistence│
│  Triggered by Stream on extraction DTs           │
└──────────────────────────────────────────────────┘
        |
        v
┌──────────────────────────────────────────────────┐
│  NORMALIZATION LAYER — Snowpark Stored Procedure │
│  Reads un-coded rows (code IS NULL)              │
│  Step 1: Bulk exact match UPDATE (SQL)           │
│  Step 2: Bulk deterministic mapping UPDATE (SQL) │
│  Step 3: Bulk fuzzy match UPDATE (Cortex         │
│           COMPLETE inline in SQL — warehouse     │
│           parallelizes across rows)              │
│  Triggered by Stream on clinical tables          │
│  $snowpark-python skill for SP creation          │
└──────────────────────────────────────────────────┘
        |
        v
┌──────────────────────────────────────────────────┐
│  SERVING LAYER                                   │
│  Clinical tables (now with codes populated)      │
│  Cortex Search Services (semantic search)        │
│  Streamlit review/annotation app                 │
│  $search-optimization + $developing-with-        │
│  streamlit skills                                │
└──────────────────────────────────────────────────┘
```

## Layer 1: Extraction — Dynamic Tables

> **Platform skill**: Invoke `$dynamic-tables` for creation, monitoring, and troubleshooting.

Each concept category gets one Dynamic Table that reads NOTE_DOCUMENT, calls Cortex COMPLETE, and FLATTENs the JSON response into typed columns.

### Why Dynamic Tables for Extraction

| Concern | DT Advantage |
|---------|-------------|
| Orchestration | Zero-code — DT dependency graph handles refresh order |
| Incremental refresh | Only processes new/changed NOTE_DOCUMENT rows |
| Parallelism | Warehouse nodes parallelize Cortex COMPLETE calls across documents |
| Monitoring | `DYNAMIC_TABLE_REFRESH_HISTORY` built-in |
| Freshness SLA | `TARGET_LAG` controls how stale extraction results can be |

### DT Naming Convention

```
DT_EXTRACT_CONDITIONS
DT_EXTRACT_THERAPEUTICS
DT_EXTRACT_OBSERVATIONS
DT_EXTRACT_PATIENT_CONTEXT
DT_EXTRACT_ONCOLOGY
DT_EXTRACT_SAFETY_CARE_PLANNING
```

### Example: DT_EXTRACT_CONDITIONS

```sql
CREATE OR REPLACE DYNAMIC TABLE DT_EXTRACT_CONDITIONS
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = $WAREHOUSE
AS
WITH extracted AS (
    SELECT
        d.document_id,
        d.patient_id,
        d.encounter_id,
        SNOWFLAKE.CORTEX.COMPLETE(
            'llama3.1-70b',
            CONCAT(
                'You are a clinical NLP system. Extract ALL conditions, diagnoses, symptoms, and risk factors from this clinical note.\n\n',
                'RULES:\n',
                '- Extract every condition mentioned, including negated ones (mark is_negated=true)\n',
                '- Distinguish between current vs historical conditions\n',
                '- Assign category: PROBLEM_LIST_ITEM, ENCOUNTER_DIAGNOSIS, SYMPTOM, RISK_FACTOR, HISTORY_OF\n',
                '- Capture severity if stated (mild, moderate, severe)\n',
                '- Capture body site and laterality if stated\n',
                '- Assess certainty: CONFIRMED, PROBABLE, POSSIBLE, UNLIKELY, RULED_OUT\n',
                '- Do NOT assign ICD-10 or SNOMED codes\n\n',
                'Return ONLY valid JSON: {"conditions": [{"display": "", "clinical_status": "active", ',
                '"verification_status": "confirmed", "category": "PROBLEM_LIST_ITEM", ',
                '"severity_display": null, "body_site_display": null, "laterality": null, ',
                '"onset_description": null, "is_negated": false, "temporality": "CURRENT", ',
                '"certainty": "CONFIRMED", "evidence_text": ""}]}\n\n',
                'Clinical Note:\n', d.raw_text
            )
        ) AS llm_response
    FROM NOTE_DOCUMENT d
)
SELECT
    UUID_STRING() AS condition_id,
    e.document_id AS provenance_document_id,
    e.patient_id,
    e.encounter_id,
    c.value:display::VARCHAR AS display,
    c.value:clinical_status::VARCHAR AS clinical_status,
    c.value:verification_status::VARCHAR AS verification_status,
    c.value:category::VARCHAR AS category,
    c.value:severity_display::VARCHAR AS severity_display,
    c.value:body_site_display::VARCHAR AS body_site_display,
    c.value:laterality::VARCHAR AS laterality,
    TRY_TO_TIMESTAMP(c.value:onset_description::VARCHAR) AS onset_datetime,
    COALESCE(c.value:is_negated::BOOLEAN, FALSE) AS is_negated,
    c.value:temporality::VARCHAR AS temporality,
    c.value:certainty::VARCHAR AS certainty,
    c.value:evidence_text::VARCHAR AS evidence_text,
    'GENAI_NLP_NOTE' AS source,
    CURRENT_TIMESTAMP() AS recorded_date,
    NULL::VARCHAR AS code,
    NULL::VARCHAR AS code_system
FROM extracted e,
    LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(e.llm_response):conditions) c
WHERE TRY_PARSE_JSON(e.llm_response) IS NOT NULL;
```

> **Pattern**: Each of the 6 extraction sub-skills (SKILL.md) contains the full prompt and schema. Translate each into a DT using this same pattern: CTE with Cortex COMPLETE → LATERAL FLATTEN → typed columns.

### DT Dependencies

```
NOTE_DOCUMENT
    ├── DT_EXTRACT_CONDITIONS
    ├── DT_EXTRACT_THERAPEUTICS          (produces: medications, procedures, allergies)
    ├── DT_EXTRACT_OBSERVATIONS
    ├── DT_EXTRACT_PATIENT_CONTEXT       (produces: social_history, family_history)
    ├── DT_EXTRACT_ONCOLOGY
    └── DT_EXTRACT_SAFETY_CARE_PLANNING  (produces: adverse_events, care_plan_items)
```

All 6 DTs read from the same source (NOTE_DOCUMENT) with no inter-DT dependencies — they can refresh in parallel.

## Layer 2: MERGE — Task

> **Platform skill**: Standard Snowflake Tasks. No special skill needed.

A scheduled Task reads from extraction DTs and MERGEs into the canonical clinical tables. This layer exists because:

1. Clinical tables are **mutable** (normalization UPDATEs them later)
2. Clinical tables may also contain **EHR_STRUCTURED** rows from non-NLP sources
3. Deduplication logic: don't re-insert if the same document was already processed

### MERGE Task Pattern

```sql
CREATE OR REPLACE TASK TASK_MERGE_EXTRACTED_CONDITIONS
    WAREHOUSE = $WAREHOUSE
    SCHEDULE = 'USING CRON 0 * * * * America/New_York'  -- hourly, or triggered by stream
AS
MERGE INTO CONDITION tgt
USING DT_EXTRACT_CONDITIONS src
ON tgt.provenance_document_id = src.provenance_document_id
   AND tgt.display = src.display
   AND tgt.source = 'GENAI_NLP_NOTE'
WHEN NOT MATCHED THEN INSERT (
    condition_id, patient_id, encounter_id, display, clinical_status,
    verification_status, category, onset_datetime, severity_display,
    body_site_display, laterality, recorded_date, source,
    provenance_document_id, code, code_system,
    is_negated, temporality, certainty, evidence_text
) VALUES (
    src.condition_id, src.patient_id, src.encounter_id, src.display,
    src.clinical_status, src.verification_status, src.category,
    src.onset_datetime, src.severity_display, src.body_site_display,
    src.laterality, src.recorded_date, src.source,
    src.provenance_document_id, src.code, src.code_system,
    src.is_negated, src.temporality, src.certainty, src.evidence_text
);
```

> **One MERGE task per clinical table** (10 total). Tasks can run in a DAG with a root task or independently on schedule.

### Stream-Triggered Alternative

Instead of CRON schedule, use a Stream on each extraction DT to trigger MERGE only when new rows appear:

```sql
CREATE OR REPLACE STREAM STREAM_EXTRACT_CONDITIONS
    ON DYNAMIC TABLE DT_EXTRACT_CONDITIONS
    APPEND_ONLY = TRUE;

CREATE OR REPLACE TASK TASK_MERGE_EXTRACTED_CONDITIONS
    WAREHOUSE = $WAREHOUSE
    WHEN SYSTEM$STREAM_HAS_DATA('STREAM_EXTRACT_CONDITIONS')
AS
MERGE INTO CONDITION tgt
USING STREAM_EXTRACT_CONDITIONS src
ON ...
```

## Layer 3: Normalization — Snowpark Stored Procedure

> **Platform skill**: Invoke `$snowpark-python` for SP creation, deployment, and debugging.

### Design Principles

1. **Thin orchestrator** — the SP coordinates which SQL statements run and in what order
2. **All heavy lifting stays in SQL** — exact match, deterministic mapping, AND fuzzy match are all SQL UPDATE statements that the warehouse parallelizes
3. **No row-by-row Python loops** — Cortex COMPLETE calls are inline in SQL, executed in parallel by warehouse nodes
4. **Batch processing** — Steps 1 and 2 reduce the volume before the expensive Step 3 (LLM calls)

### SP Signature

```sql
CREATE OR REPLACE PROCEDURE NORMALIZE_CLINICAL_ENTITIES(
    CATEGORY VARCHAR DEFAULT 'ALL',
    CODE_SYSTEMS VARCHAR DEFAULT 'ALL',
    CONFIDENCE_THRESHOLD FLOAT DEFAULT 0.7,
    REPROCESS BOOLEAN DEFAULT FALSE
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
-- See Snowpark SP body below
$$;
```

### Parameters

| Parameter | Type | Purpose |
|-----------|------|---------|
| `CATEGORY` | VARCHAR | Which concept category to normalize: `CONDITIONS`, `THERAPEUTICS`, `OBSERVATIONS`, `PATIENT_CONTEXT`, `ONCOLOGY`, `SAFETY`, or `ALL` |
| `CODE_SYSTEMS` | VARCHAR | Code system preference: `ICD-10-CM`, `SNOMED CT`, `ALL`, or JSON override per category |
| `CONFIDENCE_THRESHOLD` | FLOAT | Minimum confidence from fuzzy match to accept (default 0.7) |
| `REPROCESS` | BOOLEAN | If TRUE, re-normalize rows that already have codes (when new terminology loaded) |

### SP Body — Internal Dispatch Pattern

```python
from snowflake.snowpark import Session
import json

def main(session: Session, category: str, code_systems: str, confidence_threshold: float, reprocess: bool) -> dict:
    results = {}

    cs_config = parse_code_systems(code_systems)

    dispatchers = {
        'CONDITIONS': normalize_conditions,
        'THERAPEUTICS': normalize_therapeutics,
        'OBSERVATIONS': normalize_observations,
        'PATIENT_CONTEXT': normalize_patient_context,
        'ONCOLOGY': normalize_oncology,
        'SAFETY': normalize_safety,
    }

    targets = dispatchers.keys() if category == 'ALL' else [category.upper()]

    for cat in targets:
        if cat in dispatchers:
            cat_cs = cs_config.get(cat, cs_config.get('DEFAULT', 'ALL'))
            results[cat] = dispatchers[cat](session, cat_cs, confidence_threshold, reprocess)

    return results


def parse_code_systems(code_systems: str) -> dict:
    try:
        return json.loads(code_systems)
    except (json.JSONDecodeError, TypeError):
        return {'DEFAULT': code_systems}


def normalize_conditions(session: Session, code_systems: str, threshold: float, reprocess: bool) -> dict:
    where_clause = "display IS NOT NULL" if reprocess else "code IS NULL AND display IS NOT NULL"

    step1 = session.sql(f"""
        UPDATE CONDITION c
        SET c.code = cd.code, c.code_system = cs.name
        FROM CONCEPT_DIMENSION cd
        JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
        WHERE UPPER(TRIM(c.display)) = UPPER(TRIM(cd.display))
          AND cd.semantic_group IN ('DISEASE', 'SYMPTOM')
          AND cs.name IN ({format_in_clause(code_systems)})
          AND c.{where_clause}
    """).collect()
    step1_count = step1[0][0] if step1 else 0

    step3 = session.sql(f"""
        UPDATE CONDITION c
        SET c.code = parsed.code, c.code_system = parsed.code_system
        FROM (
            SELECT
                condition_id,
                TRY_PARSE_JSON(
                    SNOWFLAKE.CORTEX.COMPLETE(
                        'llama3.1-70b',
                        CONCAT(
                            'You are a clinical terminology expert. Map this condition to the MOST SPECIFIC code.\n',
                            'Target code systems: {code_systems_display(code_systems)}\n',
                            'Return ONLY: {{"code": "<code>", "code_system": "<system>", "confidence": <0.0-1.0>}}\n\n',
                            'Condition: "', display, '"\n',
                            'Category: ', COALESCE(category, 'UNKNOWN'), '\n',
                            'Severity: ', COALESCE(severity_display, 'NOT_SPECIFIED'), '\n',
                            'Body Site: ', COALESCE(body_site_display, 'NOT_SPECIFIED'), '\n',
                            'Laterality: ', COALESCE(laterality, 'NOT_SPECIFIED'), '\n',
                            'Certainty: ', COALESCE(certainty, 'NOT_SPECIFIED'), '\n',
                            'Negated: ', is_negated::VARCHAR, '\n',
                            'Temporality: ', COALESCE(temporality, 'NOT_SPECIFIED'), '\n',
                            'Evidence: "', COALESCE(evidence_text, ''), '"\n\n',
                            '--- CANDIDATES ---\n',
                            (SELECT LISTAGG(CONCAT(cd2.code, ' | ', cs2.name, ' | ', cd2.display), '\n')
                             FROM CONCEPT_DIMENSION cd2
                             JOIN CODE_SYSTEM cs2 ON cd2.code_system_id = cs2.code_system_id
                             WHERE cd2.semantic_group IN ('DISEASE', 'SYMPTOM')
                               AND cs2.name IN ({format_in_clause(code_systems)})
                               AND CONTAINS(UPPER(cd2.display), UPPER(SPLIT_PART(c.display, ' ', 1)))
                             LIMIT 20)
                        )
                    )
                ) AS parsed
            FROM CONDITION c
            WHERE c.code IS NULL AND c.display IS NOT NULL
        ) fuzzy
        WHERE fuzzy.condition_id = c.condition_id
          AND fuzzy.parsed:confidence::FLOAT >= {threshold}
    """).collect()
    step3_count = step3[0][0] if step3 else 0

    return {
        'exact_match': step1_count,
        'fuzzy_match': step3_count,
        'total_normalized': step1_count + step3_count
    }


def format_in_clause(code_systems: str) -> str:
    if code_systems == 'ALL':
        return "'ICD-10-CM','SNOMED CT','RxNorm','LOINC','MedDRA','ICD-O-3','CPT','ICD-10-PCS'"
    return ','.join(f"'{cs.strip()}'" for cs in code_systems.split(','))


def code_systems_display(code_systems: str) -> str:
    if code_systems == 'ALL':
        return 'all available code systems'
    return code_systems


def normalize_therapeutics(session: Session, code_systems: str, threshold: float, reprocess: bool) -> dict:
    results = {}

    where_clause = "medication_display IS NOT NULL" if reprocess else "medication_code IS NULL AND medication_display IS NOT NULL"
    step1 = session.sql(f"""
        UPDATE MEDICATION_REQUEST m
        SET m.medication_code = cd.code, m.medication_system = cs.name
        FROM CONCEPT_DIMENSION cd
        JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
        WHERE UPPER(TRIM(m.medication_display)) = UPPER(TRIM(cd.display))
          AND cd.semantic_group = 'MEDICATION'
          AND cs.name IN ({format_in_clause(code_systems)})
          AND m.{where_clause}
    """).collect()
    results['medication_exact'] = step1[0][0] if step1 else 0

    where_clause = "display IS NOT NULL" if reprocess else "code IS NULL AND display IS NOT NULL"
    step1_proc = session.sql(f"""
        UPDATE PROCEDURE p
        SET p.code = cd.code, p.code_system = cs.name
        FROM CONCEPT_DIMENSION cd
        JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
        WHERE UPPER(TRIM(p.display)) = UPPER(TRIM(cd.display))
          AND cd.semantic_group = 'PROCEDURE'
          AND cs.name IN ({format_in_clause(code_systems)})
          AND p.{where_clause}
    """).collect()
    results['procedure_exact'] = step1_proc[0][0] if step1_proc else 0

    where_clause = "substance_display IS NOT NULL" if reprocess else "substance_code IS NULL AND substance_display IS NOT NULL"
    step1_allergy = session.sql(f"""
        UPDATE ALLERGY_INTOLERANCE a
        SET a.substance_code = cd.code, a.substance_system = cs.name
        FROM CONCEPT_DIMENSION cd
        JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
        WHERE UPPER(TRIM(a.substance_display)) = UPPER(TRIM(cd.display))
          AND cd.semantic_group IN ('MEDICATION', 'OTHER')
          AND cs.name IN ({format_in_clause(code_systems)})
          AND a.{where_clause}
    """).collect()
    results['allergy_exact'] = step1_allergy[0][0] if step1_allergy else 0

    return results


def normalize_observations(session: Session, code_systems: str, threshold: float, reprocess: bool) -> dict:
    results = {}

    if 'LOINC' in code_systems or code_systems == 'ALL':
        vital_map = session.sql("""
            UPDATE OBSERVATION
            SET code = CASE display
                WHEN 'BLOOD PRESSURE SYSTOLIC' THEN '8480-6'
                WHEN 'BLOOD PRESSURE DIASTOLIC' THEN '8462-4'
                WHEN 'HEART RATE' THEN '8867-4'
                WHEN 'BODY TEMPERATURE' THEN '8310-5'
                WHEN 'RESPIRATORY RATE' THEN '9279-1'
                WHEN 'OXYGEN SATURATION' THEN '2708-6'
                WHEN 'BODY WEIGHT' THEN '29463-7'
                WHEN 'BODY HEIGHT' THEN '8302-2'
                WHEN 'BMI' THEN '39156-5'
                ELSE code
            END,
                code_system = 'LOINC'
            WHERE category = 'VITAL_SIGNS'
              AND code IS NULL
              AND display IS NOT NULL
        """).collect()
        results['vital_deterministic'] = vital_map[0][0] if vital_map else 0

    where_clause = "display IS NOT NULL" if reprocess else "code IS NULL AND display IS NOT NULL"
    step1 = session.sql(f"""
        UPDATE OBSERVATION o
        SET o.code = cd.code, o.code_system = cs.name
        FROM CONCEPT_DIMENSION cd
        JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
        WHERE UPPER(TRIM(o.display)) = UPPER(TRIM(cd.display))
          AND cd.semantic_group IN ('LAB', 'SCORE')
          AND cs.name IN ({format_in_clause(code_systems)})
          AND o.category IN ('LAB', 'SCORE', 'VITAL_SIGNS')
          AND o.{where_clause}
    """).collect()
    results['observation_exact'] = step1[0][0] if step1 else 0

    return results


def normalize_patient_context(session: Session, code_systems: str, threshold: float, reprocess: bool) -> dict:
    results = {}

    if 'ICD-10-CM' in code_systems or code_systems == 'ALL':
        sdoh = session.sql("""
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
                code_system = 'ICD-10-CM'
            WHERE code IS NULL AND sdoh_domain IS NOT NULL
        """).collect()
        results['sdoh_deterministic'] = sdoh[0][0] if sdoh else 0

    where_clause = "condition_display IS NOT NULL" if reprocess else "condition_code IS NULL AND condition_display IS NOT NULL"
    fh = session.sql(f"""
        UPDATE FAMILY_MEMBER_HISTORY f
        SET f.condition_code = cd.code, f.condition_system = cs.name
        FROM CONCEPT_DIMENSION cd
        JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
        WHERE UPPER(TRIM(f.condition_display)) = UPPER(TRIM(cd.display))
          AND cd.semantic_group IN ('DISEASE', 'FAMILY')
          AND cs.name IN ({format_in_clause(code_systems)})
          AND f.{where_clause}
    """).collect()
    results['family_history_exact'] = fh[0][0] if fh else 0

    return results


def normalize_oncology(session: Session, code_systems: str, threshold: float, reprocess: bool) -> dict:
    results = {}

    where_clause = "primary_site_display IS NOT NULL" if reprocess else "primary_site_code IS NULL AND primary_site_display IS NOT NULL"
    site = session.sql(f"""
        UPDATE TUMOR_EPISODE t
        SET t.primary_site_code = cd.code, t.primary_site_system = cs.name
        FROM CONCEPT_DIMENSION cd
        JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
        WHERE UPPER(TRIM(t.primary_site_display)) = UPPER(TRIM(cd.display))
          AND cd.semantic_group IN ('ANATOMY', 'TUMOR')
          AND cs.name IN ({format_in_clause(code_systems)})
          AND t.{where_clause}
    """).collect()
    results['site_exact'] = site[0][0] if site else 0

    where_clause = "histology_display IS NOT NULL" if reprocess else "histology_code IS NULL AND histology_display IS NOT NULL"
    hist = session.sql(f"""
        UPDATE TUMOR_EPISODE t
        SET t.histology_code = cd.code, t.histology_system = cs.name
        FROM CONCEPT_DIMENSION cd
        JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
        WHERE UPPER(TRIM(t.histology_display)) = UPPER(TRIM(cd.display))
          AND cd.semantic_group = 'TUMOR'
          AND cs.name IN ({format_in_clause(code_systems)})
          AND t.{where_clause}
    """).collect()
    results['histology_exact'] = hist[0][0] if hist else 0

    return results


def normalize_safety(session: Session, code_systems: str, threshold: float, reprocess: bool) -> dict:
    results = {}

    where_clause = "event_display IS NOT NULL" if reprocess else "event_code IS NULL AND event_display IS NOT NULL"
    ae = session.sql(f"""
        UPDATE ADVERSE_EVENT ae
        SET ae.event_code = cd.code, ae.event_system = cs.name
        FROM CONCEPT_DIMENSION cd
        JOIN CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
        WHERE UPPER(TRIM(ae.event_display)) = UPPER(TRIM(cd.display))
          AND cd.semantic_group IN ('DISEASE', 'SYMPTOM')
          AND cs.name IN ({format_in_clause(code_systems)})
          AND ae.{where_clause}
    """).collect()
    results['adverse_event_exact'] = ae[0][0] if ae else 0

    return results
$$;
```

### Triggering the SP

**Option A: Stream-triggered Task (recommended for production)**

```sql
CREATE OR REPLACE STREAM STREAM_UNCODED_CONDITIONS
    ON TABLE CONDITION
    SHOW_INITIAL_ROWS = FALSE;

CREATE OR REPLACE TASK TASK_NORMALIZE_CONDITIONS
    WAREHOUSE = $WAREHOUSE
    WHEN SYSTEM$STREAM_HAS_DATA('STREAM_UNCODED_CONDITIONS')
AS
    CALL NORMALIZE_CLINICAL_ENTITIES('CONDITIONS', 'ICD-10-CM,SNOMED CT', 0.7, FALSE);
```

**Option B: Scheduled Task (simpler)**

```sql
CREATE OR REPLACE TASK TASK_NORMALIZE_ALL
    WAREHOUSE = $WAREHOUSE
    SCHEDULE = 'USING CRON 0 */2 * * * America/New_York'  -- every 2 hours
AS
    CALL NORMALIZE_CLINICAL_ENTITIES('ALL', 'ALL', 0.7, FALSE);
```

**Option C: On-demand (ad hoc or after bulk load)**

```sql
CALL NORMALIZE_CLINICAL_ENTITIES('ALL', 'ALL', 0.7, FALSE);

CALL NORMALIZE_CLINICAL_ENTITIES('ONCOLOGY', 'ICD-O-3', 0.8, FALSE);

CALL NORMALIZE_CLINICAL_ENTITIES('ALL', 'ALL', 0.7, TRUE);
```

### Task DAG (Full Pipeline)

```sql
CREATE OR REPLACE TASK TASK_ROOT_CLINICAL_NLP_PIPELINE
    WAREHOUSE = $WAREHOUSE
    SCHEDULE = 'USING CRON 0 * * * * America/New_York'
AS
    SELECT 1;  -- root task, triggers children

CREATE OR REPLACE TASK TASK_MERGE_CONDITIONS
    WAREHOUSE = $WAREHOUSE
    AFTER TASK_ROOT_CLINICAL_NLP_PIPELINE
AS
    MERGE INTO CONDITION ... USING DT_EXTRACT_CONDITIONS ...;

-- ... (10 MERGE tasks, one per clinical table) ...

CREATE OR REPLACE TASK TASK_NORMALIZE_ALL
    WAREHOUSE = $WAREHOUSE
    AFTER TASK_MERGE_CONDITIONS, TASK_MERGE_THERAPEUTICS,
          TASK_MERGE_OBSERVATIONS, TASK_MERGE_PATIENT_CONTEXT,
          TASK_MERGE_ONCOLOGY, TASK_MERGE_SAFETY
AS
    CALL NORMALIZE_CLINICAL_ENTITIES('ALL', $NORM_CODE_SYSTEMS, 0.7, FALSE);
```

## Layer 4: Serving

### Cortex Search Services

> **Platform skill**: Invoke `$search-optimization` for creation and tuning.

```sql
CREATE OR REPLACE CORTEX SEARCH SERVICE CLINICAL_ENTITY_SEARCH_SVC
    ON CONDITION
    WAREHOUSE = $WAREHOUSE
    TARGET_LAG = '1 day'
    SEARCH_COLUMN = display
    COLUMNS = (condition_id, patient_id, display, code, code_system,
               clinical_status, category, certainty, evidence_text);
```

### Streamlit Review App

> **Platform skill**: Invoke `$developing-with-streamlit` for app creation.

Key screens:
- **Extraction QA**: Review extracted entities per document, flag incorrect extractions
- **Normalization QA**: Review fuzzy match results, override codes, adjust confidence thresholds
- **Pipeline Dashboard**: DT refresh status, normalization rates per category, unmatched entity queue
- **Terminology Browser**: Search CONCEPT_DIMENSION, view code system coverage

## Monitoring

### Extraction Health

```sql
SELECT
    dt_name,
    state,
    last_completed_time,
    data_timestamp,
    DATEDIFF('minute', data_timestamp, CURRENT_TIMESTAMP()) AS staleness_minutes
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE dt_name LIKE 'DT_EXTRACT_%'
ORDER BY last_completed_time DESC;
```

### Normalization Coverage

```sql
SELECT 'CONDITION' AS entity, COUNT(*) AS total,
    COUNT(code) AS coded, ROUND(COUNT(code)*100.0/NULLIF(COUNT(*),0),1) AS pct
FROM CONDITION WHERE source = 'GENAI_NLP_NOTE'
UNION ALL
SELECT 'MEDICATION_REQUEST', COUNT(*), COUNT(medication_code),
    ROUND(COUNT(medication_code)*100.0/NULLIF(COUNT(*),0),1)
FROM MEDICATION_REQUEST WHERE source = 'GENAI_NLP_NOTE'
UNION ALL
SELECT 'PROCEDURE', COUNT(*), COUNT(code),
    ROUND(COUNT(code)*100.0/NULLIF(COUNT(*),0),1)
FROM PROCEDURE WHERE source = 'GENAI_NLP_NOTE'
UNION ALL
SELECT 'OBSERVATION', COUNT(*), COUNT(code),
    ROUND(COUNT(code)*100.0/NULLIF(COUNT(*),0),1)
FROM OBSERVATION WHERE source = 'GENAI_NLP_NOTE'
UNION ALL
SELECT 'ALLERGY_INTOLERANCE', COUNT(*), COUNT(substance_code),
    ROUND(COUNT(substance_code)*100.0/NULLIF(COUNT(*),0),1)
FROM ALLERGY_INTOLERANCE WHERE source = 'GENAI_NLP_NOTE'
UNION ALL
SELECT 'ADVERSE_EVENT', COUNT(*), COUNT(event_code),
    ROUND(COUNT(event_code)*100.0/NULLIF(COUNT(*),0),1)
FROM ADVERSE_EVENT WHERE source = 'GENAI_NLP_NOTE'
UNION ALL
SELECT 'SOCIAL_HISTORY', COUNT(*), COUNT(code),
    ROUND(COUNT(code)*100.0/NULLIF(COUNT(*),0),1)
FROM SOCIAL_HISTORY_OBSERVATION WHERE source = 'GENAI_NLP_NOTE'
UNION ALL
SELECT 'FAMILY_HISTORY', COUNT(*), COUNT(condition_code),
    ROUND(COUNT(condition_code)*100.0/NULLIF(COUNT(*),0),1)
FROM FAMILY_MEMBER_HISTORY WHERE source = 'GENAI_NLP_NOTE'
UNION ALL
SELECT 'TUMOR_EPISODE', COUNT(*), COUNT(primary_site_code),
    ROUND(COUNT(primary_site_code)*100.0/NULLIF(COUNT(*),0),1)
FROM TUMOR_EPISODE WHERE source = 'GENAI_NLP_NOTE';
```

## Warehouse Sizing Guidance

| Layer | Warehouse Size | Rationale |
|-------|---------------|-----------|
| Extraction DTs | **MEDIUM to LARGE** | Cortex COMPLETE calls are the bottleneck; larger warehouse = more parallel LLM calls per document |
| MERGE Tasks | **SMALL** | Simple INSERT/MERGE operations, low compute |
| Normalization SP | **MEDIUM** | Steps 1-2 are fast SQL; Step 3 (fuzzy) needs parallel Cortex COMPLETE capacity |
| Cortex Search refresh | **SMALL** | Background index maintenance |

> **Cost tip**: Use separate warehouses for extraction (bursty, expensive) vs MERGE/monitoring (steady, cheap). Auto-suspend extraction warehouse aggressively.

## Output

Production pipeline: 6 extraction DTs, 10 MERGE tasks, 1 normalization SP, Cortex Search Services, optional Streamlit app. Task DAG orchestrated.
