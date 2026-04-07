# Clinical NLP Pipeline

End-to-end clinical NLP pipeline on Snowflake using Cortex AI to extract, normalize, and serve structured clinical entities from unstructured medical transcription notes.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  SOURCE: DOC_CATEGORIZED_AND_CHUNKED_2A (12,333 rows)       │
│  └─ Dedup View: NOTE_DOCUMENT_SOURCE (4,969 docs)           │
└──────────────────┬───────────────────────────────────────────┘
                   │
┌──────────────────▼───────────────────────────────────────────┐
│  EXTRACTION (6 Stored Procedures × Cortex COMPLETE)          │
│  ├─ SP_EXTRACT_CONDITIONS      → CONDITION                   │
│  ├─ SP_EXTRACT_THERAPEUTICS    → MEDICATION_REQUEST,          │
│  │                               PROCEDURE,                  │
│  │                               ALLERGY_INTOLERANCE          │
│  ├─ SP_EXTRACT_OBSERVATIONS    → OBSERVATION                 │
│  ├─ SP_EXTRACT_PATIENT_CONTEXT → SOCIAL_HISTORY_OBSERVATION, │
│  │                               FAMILY_MEMBER_HISTORY        │
│  ├─ SP_EXTRACT_ONCOLOGY        → TUMOR_EPISODE               │
│  └─ SP_EXTRACT_SAFETY_CARE     → ADVERSE_EVENT,              │
│                                  CARE_PLAN_ITEM               │
└──────────────────┬───────────────────────────────────────────┘
                   │
┌──────────────────▼───────────────────────────────────────────┐
│  NORMALIZATION (SP_NORMALIZE_ENTITIES)                        │
│  ├─ Exact match vs CONCEPT_DIMENSION (154K codes)            │
│  ├─ Deterministic vital sign → LOINC mapping                 │
│  └─ LLM-assisted fuzzy match (ICD-10, RxNorm, LOINC)        │
└──────────────────┬───────────────────────────────────────────┘
                   │
┌──────────────────▼───────────────────────────────────────────┐
│  GOVERNANCE                                                   │
│  ├─ PHI tagging (16 columns, 12 tables)                      │
│  ├─ Tag-based masking policy                                  │
│  ├─ 3-tier role hierarchy (READER < ANALYST < ADMIN)         │
│  └─ De-identified secure views for Cortex AI                 │
└──────────────────┬───────────────────────────────────────────┘
                   │
┌──────────────────▼───────────────────────────────────────────┐
│  SERVING                                                      │
│  ├─ Cortex Search Service (natural language queries)          │
│  └─ ENTITY_COUNTS + PIPELINE_PROGRESS views                  │
└──────────────────────────────────────────────────────────────┘
```

## Quick Start

```bash
# 1. Deploy all objects (schema, tables, SPs, views)
SNOWFLAKE_CONNECTION_NAME=polaris1 python scripts/run_pipeline.py deploy

# 2. Validate on 20-doc sample
SNOWFLAKE_CONNECTION_NAME=polaris1 python scripts/run_pipeline.py sample

# 3. Run full pipeline (4,969 docs, 500-doc batches)
SNOWFLAKE_CONNECTION_NAME=polaris1 python scripts/run_pipeline.py run 500

# 4. Check progress
SNOWFLAKE_CONNECTION_NAME=polaris1 python scripts/run_pipeline.py progress
```

## Directory Structure

```
clinical-nlp-pipeline/
├── sql/
│   ├── 01-setup/              # Schema, tables, dedup view
│   ├── 02-extraction/         # 6 extraction stored procedures
│   ├── 04-normalization/      # Terminology normalization SP
│   ├── 05-governance/         # PHI tags, masking, roles
│   ├── 06-serving/            # Cortex Search Service
│   └── 07-orchestration/      # Master pipeline SP + progress views
├── scripts/
│   └── run_pipeline.py        # Python runner with progress reporting
└── docs/
```

## Data Model

| Table | Description | Key Fields |
|-------|-------------|------------|
| NOTE_DOCUMENT | Source clinical notes (1 per doc) | document_id, patient_id, raw_text |
| CONDITION | Diagnoses, symptoms, risk factors | display, code (ICD-10), certainty |
| MEDICATION_REQUEST | Medications with dosing | medication_display, dose, route |
| PROCEDURE | Surgical, diagnostic, imaging | display, code (CPT/SNOMED) |
| OBSERVATION | Vitals, labs, exam findings | display, code (LOINC), value |
| ALLERGY_INTOLERANCE | Drug/food/environmental | substance_display, severity |
| ADVERSE_EVENT | Drug reactions, complications | event_display, seriousness |
| SOCIAL_HISTORY_OBSERVATION | SDOH, tobacco, alcohol | sdoh_domain, status |
| FAMILY_MEMBER_HISTORY | Family conditions | relationship, condition |
| CARE_PLAN_ITEM | Goals, referrals, follow-ups | item_type, description |
| TUMOR_EPISODE | Cancer staging/grading | site, histology, TNM, stage |

## Pipeline Progress Tracking

All steps log to `PIPELINE_RUN_LOG`. Query progress:

```sql
SELECT * FROM CLINICAL_NLP.PIPELINE_PROGRESS;
SELECT * FROM CLINICAL_NLP.ENTITY_COUNTS;
```

## Prerequisites

- Snowflake account with Cortex AI enabled
- Warehouse: BI_WH (or modify in scripts)
- Source table: `UNSTRUCTURED_HEALTHDATA.MED_TRANSCRIPTS.DOC_CATEGORIZED_AND_CHUNKED_2A`
- Terminology: `UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CONCEPT_DIMENSION` (154K codes)
