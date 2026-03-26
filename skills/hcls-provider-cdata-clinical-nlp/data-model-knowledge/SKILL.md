---
name: data-model-knowledge
description: "Query the Clinical NLP data model knowledge repository (Cortex Search Service) to retrieve FHIR-aligned table definitions, column specifications, PHI indicators, terminology mappings, and relationship metadata at runtime. Use when building schemas, generating DDL, validating data models, or answering data model questions for clinical NLP pipelines."
parent_skill: hcls-provider-cdata-clinical-nlp
---

# Clinical NLP Data Model Knowledge Repository

## Preflight Check (REQUIRED -- Run Before Any Query)

Before executing any data model search, verify both the reference table and Cortex Search Service exist:

### Check 1: Reference Table

```sql
SELECT COUNT(*) FROM UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CLINICAL_NLP_MODEL_REFERENCE LIMIT 1;
```

### Check 2: Cortex Search Service

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CLINICAL_NLP_MODEL_SEARCH_SVC',
    '{"query": "test", "columns": ["SEARCH_TEXT"], "limit": 1}'
);
```

| Check 1 | Check 2 | Status | Action |
|---------|---------|--------|--------|
| OK | OK | READY | Proceed with dynamic data model queries |
| OK | FAIL | PARTIAL | Table exists but search service is missing -- guide user to recreate search service (see Setup) |
| FAIL | FAIL | MISSING | Guide user through full Setup below |
| ERROR | ERROR | ERROR | Show error, check permissions on `UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE` |

### Fallback (When MISSING or PARTIAL)

If the Cortex Search Service is not available:
- **Use hardcoded clinical NLP schema definitions** from the PDF reference documents in the skill folder
- **Inform the user**: "Clinical NLP data model search service is not available -- using hardcoded schema definitions. Results may not reflect the latest data model updates."
- **Never block the parent skill** -- the clinical-nlp router and sub-skills must continue to work with hardcoded fallbacks

### Auto-Detection for Clinical NLP Router

The clinical-nlp router runs this preflight as part of its Step 0 (Data Model Knowledge pre-step):
1. Run both probes above
2. If READY -- use dynamic search results to ground DDL/pipeline generation
3. If MISSING -- fall back to hardcoded schemas, note the fallback in output
4. Set `$DMK_AVAILABLE` context flag for sub-skills to check

## When to Load

Clinical-nlp router or any skill that needs to reference the GenAI-powered clinical NLP concept schema at runtime instead of relying on hardcoded schema definitions.

## Architecture

```
PDF (GenAI-powered clinical NLP concept schema)
    |
    v  manual extraction
CSV (clinical_nlp_model_search_corpus.csv)
    |
    v  COPY INTO
Snowflake Table (CLINICAL_NLP_MODEL_REFERENCE)
    |
    v  Cortex Search Service
CLINICAL_NLP_MODEL_SEARCH_SVC  <-- Skills query this at runtime
```

## Data Model Overview

The reference contains **17 tables** across **4 layers** with **245 columns**:

| Layer | Tables | Purpose |
|-------|--------|---------|
| **CONTEXT** (2) | NOTE_DOCUMENT, NOTE_SECTION | Note-level context and section segmentation |
| **CLINICAL** (10) | CONDITION, OBSERVATION, PROCEDURE, MEDICATION_REQUEST, ALLERGY_INTOLERANCE, ADVERSE_EVENT, SOCIAL_HISTORY_OBSERVATION, FAMILY_MEMBER_HISTORY, CARE_PLAN_ITEM, TUMOR_EPISODE | FHIR-aligned clinical concept tables |
| **NLP** (3) | NLP_NOTE_ENTITY_MENTION, NLP_NOTE_ENTITY_ATTRIBUTE, NLP_NOTE_ENTITY_RELATION | Optional deep-audit trail for NLP extraction provenance (most-queried NLP fields are promoted onto Clinical tables) |
| **TERMINOLOGY** (2) | CODE_SYSTEM, CONCEPT_DIMENSION | Optional terminology normalization dimension |

### FHIR Resource Mapping

| Table | FHIR Resource | Key Use Case |
|-------|--------------|--------------|
| CONDITION | Condition | Diagnoses, problems, symptoms, risk states |
| OBSERVATION | Observation | Labs, vitals, exam findings, scores |
| PROCEDURE | Procedure | Surgical, diagnostic, imaging procedures |
| MEDICATION_REQUEST | MedicationRequest | Medication orders, prescriptions |
| ALLERGY_INTOLERANCE | AllergyIntolerance | Drug/food/environmental allergies |
| ADVERSE_EVENT | AdverseEvent | Unintended harmful clinical events |
| SOCIAL_HISTORY_OBSERVATION | Observation | SDOH (12 Gravity Project domains), tobacco, alcohol, substance use, family/occupation |
| FAMILY_MEMBER_HISTORY | FamilyMemberHistory | Conditions affecting relatives |
| CARE_PLAN_ITEM | CarePlan | Goals, actions, referrals, follow-ups |
| TUMOR_EPISODE | Condition (extension) | Oncology: stage, grade, histology, response |

### Partially Flattened Design

Clinical tables carry **promoted NLP provenance fields** directly, eliminating the need to join through NLP layer tables for common queries:

| Promoted Field | Type | Description | Tables That Carry It |
|---------------|------|-------------|---------------------|
| `is_negated` | BOOLEAN | Whether the entity was negated in the source text | CONDITION, OBSERVATION, PROCEDURE, MEDICATION_REQUEST, ALLERGY_INTOLERANCE, FAMILY_MEMBER_HISTORY |
| `temporality` | VARCHAR | CURRENT, HISTORICAL, or FUTURE context | CONDITION, OBSERVATION, PROCEDURE, MEDICATION_REQUEST |
| `certainty` | VARCHAR | CONFIRMED, PROBABLE, POSSIBLE, UNLIKELY, RULED_OUT | CONDITION, OBSERVATION, TUMOR_EPISODE |
| `evidence_text` | VARCHAR | Exact citation from the source note | All 10 clinical tables |
| `extraction_confidence` | NUMBER(5,4) | NLP engine confidence score (0.0–1.0) | All 10 clinical tables |

**Query without joins:**
```sql
SELECT display, clinical_status, severity_display, evidence_text, extraction_confidence
FROM CONDITION
WHERE patient_id = :patient_id AND is_negated = FALSE AND certainty IN ('CONFIRMED','PROBABLE');
```

**NLP Layer Tables** (`NLP_NOTE_ENTITY_MENTION`, `NLP_NOTE_ENTITY_ATTRIBUTE`, `NLP_NOTE_ENTITY_RELATION`) remain available as an **optional deep-audit trail** — they store span offsets, additional attributes, and inter-entity relationships for provenance tracing, but are not required for typical clinical queries.

## Prerequisites

- Cortex Search Service `CLINICAL_NLP_MODEL_SEARCH_SVC` created in `UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE`
- Run `scripts/setup_clinical_nlp_model_knowledge_repo.sql` for initial setup
- Warehouse with `SNOWFLAKE.CORTEX_USER` database role granted

## Setup (One-Time)

### 1. Upload CSV and Create Reference Table

```sql
-- Run the setup script
-- scripts/setup_clinical_nlp_model_knowledge_repo.sql
```

Or manually:
```sql
USE DATABASE UNSTRUCTURED_HEALTHDATA;
USE SCHEMA DATA_MODEL_KNOWLEDGE;

CREATE STAGE IF NOT EXISTS clinical_nlp_model_stage;

PUT file://references/clinical_nlp_model_search_corpus.csv @clinical_nlp_model_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

CREATE TABLE IF NOT EXISTS CLINICAL_NLP_MODEL_REFERENCE (
    TABLE_NAME VARCHAR NOT NULL, COLUMN_NAME VARCHAR NOT NULL,
    DATA_TYPE VARCHAR NOT NULL, CONSTRAINTS VARCHAR,
    DESCRIPTION VARCHAR, FHIR_RESOURCE VARCHAR,
    FHIR_FIELD VARCHAR, CATEGORY VARCHAR NOT NULL,
    CONTAINS_PHI VARCHAR(1) NOT NULL, RELATIONSHIPS VARCHAR,
    ENUM_VALUES VARCHAR, SEARCH_TEXT VARCHAR NOT NULL
);

COPY INTO CLINICAL_NLP_MODEL_REFERENCE
FROM @clinical_nlp_model_stage/clinical_nlp_model_search_corpus.csv
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE);

CREATE OR REPLACE CORTEX SEARCH SERVICE CLINICAL_NLP_MODEL_SEARCH_SVC
    ON search_text
    ATTRIBUTES table_name, column_name, data_type, category, contains_phi, fhir_resource
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 day'
AS (
    SELECT search_text, table_name, column_name, data_type, fhir_resource,
           fhir_field, category, description, constraints, contains_phi,
           relationships, enum_values
    FROM CLINICAL_NLP_MODEL_REFERENCE
);
```

## Runtime Query Patterns

### Pattern 1: Find Tables for a Clinical Concept

When a user describes what they need to extract, search the model to find relevant tables/columns:

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CLINICAL_NLP_MODEL_SEARCH_SVC',
    '{"query": "medication dosage route frequency prescription", "columns": ["table_name", "column_name", "data_type", "description", "fhir_resource"]}'
);
```

### Pattern 2: Generate DDL from Search Results

Search for columns of a specific table, then generate CREATE TABLE:

```sql
WITH model_knowledge AS (
    SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CLINICAL_NLP_MODEL_SEARCH_SVC',
        '{"query": "CONDITION diagnosis problem severity laterality", "columns": ["table_name", "column_name", "data_type", "constraints", "description", "enum_values"]}'
    ) AS context
)
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'llama3.1-70b',
    'Based on this data model reference, generate a Snowflake CREATE TABLE DDL statement for the CONDITION table. Use the exact column names, data types, and constraints from the reference. Add COMMENT ON COLUMN for each column using the description. Reference: ' || context::STRING
) AS generated_ddl
FROM model_knowledge;
```

### Pattern 3: Identify PHI Columns

Find all columns containing Protected Health Information for governance:

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CLINICAL_NLP_MODEL_SEARCH_SVC',
    '{"query": "PHI protected health information patient name identifier", "columns": ["table_name", "column_name", "description", "contains_phi"]}'
);
```

### Pattern 4: Explore Foreign Key Relationships

Find relationships and entity hierarchy across the schema:

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CLINICAL_NLP_MODEL_SEARCH_SVC',
    '{"query": "foreign key relationship provenance document condition indication", "columns": ["table_name", "column_name", "constraints", "relationships", "description"]}'
);
```

### Pattern 5: FHIR Resource Mapping Lookup

Find how clinical NLP tables map to FHIR resources:

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CLINICAL_NLP_MODEL_SEARCH_SVC',
    '{"query": "AllergyIntolerance reaction severity criticality substance", "columns": ["table_name", "column_name", "fhir_resource", "fhir_field", "description"]}'
);
```

### Pattern 6: NLP Layer Schema for Deep Audit

Find NLP-specific tables for provenance tracing and deep audit (most-queried NLP fields are already on clinical tables):

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CLINICAL_NLP_MODEL_SEARCH_SVC',
    '{"query": "NLP entity mention attribute negation temporality confidence engine", "columns": ["table_name", "column_name", "data_type", "enum_values", "description"]}'
);
```

### Pattern 7: Enum Values for Validation

Retrieve valid enum values for a column to build validation rules:

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CLINICAL_NLP_MODEL_SEARCH_SVC',
    '{"query": "clinical_status verification_status category enum values", "columns": ["table_name", "column_name", "enum_values", "description"]}'
);
```

### Pattern 8: Find Promoted NLP Fields on Clinical Tables

Search for flattened NLP provenance columns available directly on clinical tables:

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CLINICAL_NLP_MODEL_SEARCH_SVC',
    '{"query": "is_negated temporality certainty evidence_text extraction_confidence promoted NLP", "columns": ["table_name", "column_name", "data_type", "enum_values", "description"]}'
);
```

## Integration with Other Sub-Skills

### Extraction Sub-Skills (Conditions, Medications, Observations, etc.)
Before generating extraction prompts or output schemas, query this service to get the latest model definition:
```
1. User asks to extract conditions from discharge summary
2. Search CLINICAL_NLP_MODEL_SEARCH_SVC for CONDITION table columns
3. Use search results to construct the LLM extraction prompt JSON schema
4. Map extracted entities to the exact column definitions from the model
```

### Governance Sub-Skill
Query PHI indicators to auto-generate masking policies:
```
1. Search for contains_phi = "Y" columns across all 17 tables
2. Generate masking policies for each PHI column found
3. Apply row-access policies based on table relationships
```

### Terminology Normalization Sub-Skill
Query terminology layer to understand code system requirements:
```
1. Search for code_system columns across clinical tables
2. Identify which terminology systems are expected (SNOMED, ICD-10, RxNorm, LOINC, MedDRA)
3. Build normalization mappings per entity type
```

## Updating the Data Model

When the clinical NLP data model changes:

1. Edit `references/clinical_nlp_model_search_corpus.csv`
2. Reload to Snowflake:
```sql
TRUNCATE TABLE CLINICAL_NLP_MODEL_REFERENCE;
PUT file://references/clinical_nlp_model_search_corpus.csv @clinical_nlp_model_stage OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
COPY INTO CLINICAL_NLP_MODEL_REFERENCE FROM @clinical_nlp_model_stage/clinical_nlp_model_search_corpus.csv
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE);
```
3. Cortex Search Service auto-refreshes based on TARGET_LAG (1 day default)

## Terminology Seed Data (CODE_SYSTEM & CONCEPT_DIMENSION)

The TERMINOLOGY layer tables (`CODE_SYSTEM` and `CONCEPT_DIMENSION`) are pre-seeded with representative terminology data for normalization lookups:

| Code System | Codes | Source | Coverage |
|-------------|-------|--------|----------|
| ICD-10-CM | 74,719 | CDC FTP (FY2026 order file) | **Complete** — all billable diagnosis codes |
| ICD-10-PCS | 79,115 | CMS.gov (FY2026 order file) | **Complete** — all billable procedure codes |
| ICD-O-3 | 325 | Curated (topography + morphology) | Representative — 137 sites, 188 histologies |
| SNOMED CT | 131 | Curated (top clinical concepts) | Representative — diseases, symptoms, procedures, anatomy |
| MedDRA | 104 | Curated (common adverse event PTs) | Representative — top pharmacovigilance terms |
| RxNorm | 100 | Curated (top prescribed medications) | Representative — clinical drugs + ingredients |
| LOINC | 87 | Curated (vitals, CBC, CMP, scores) | Representative — common lab/vital/assessment codes |
| HCPCS | 45 | Curated (E/M, J-codes, G-codes) | Representative — common billing codes |

**Total: 154,626 codes across 8 code systems.**

> **BRING YOUR OWN CODESET DISCLAIMER**: ICD-10-CM and ICD-10-PCS are loaded in full from official government sources. All other code systems contain curated representative subsets sufficient for demonstration, development, and testing. **For production use, organizations should load their own complete, licensed terminology sets** (especially SNOMED CT, LOINC, RxNorm, and MedDRA which require licenses for full distribution). The curated subsets cover the most clinically common concepts but are not exhaustive.

### Setup Instructions

All seed data files and setup scripts are in `data-model-knowledge/seed-data/`:

| File | Contents |
|------|----------|
| `code_system.csv` | 8 code system definitions |
| `concept_dimension_curated.csv` | 792 curated concepts (6 systems: SNOMED CT, LOINC, RxNorm, MedDRA, ICD-O-3, HCPCS) |
| `setup_seed_data.sql` | Complete setup script (tables, staging, COPY INTO, ICD-10 loaders) |

**Quick start:**
1. Run `setup_seed_data.sql` in Snowflake (creates tables, stage, network rules, SPs)
2. Upload CSVs via PUT:
   ```sql
   PUT file:///path/to/seed-data/code_system.csv @seed_data_stage AUTO_COMPRESS=FALSE;
   PUT file:///path/to/seed-data/concept_dimension_curated.csv @seed_data_stage AUTO_COMPRESS=FALSE;
   ```
3. Run the COPY INTO statements in the setup script
4. Run `CALL LOAD_ICD10CM();` and `CALL LOAD_ICD10PCS();` for complete ICD-10 (requires ACCOUNTADMIN for external access integrations)

## Extending to Other Data Models

This pattern is reusable — see also:

| Domain | Reference Table | Search Service |
|--------|----------------|----------------|
| DICOM Imaging | DICOM_MODEL_REFERENCE | DICOM_MODEL_SEARCH_SVC |
| **Clinical NLP** | **CLINICAL_NLP_MODEL_REFERENCE** | **CLINICAL_NLP_MODEL_SEARCH_SVC** |
| FHIR R4 | fhir_r4_resource_model (future) | FHIR_MODEL_SEARCH_SVC |
| OMOP CDM v5.4 | omop_cdm_v54_model (future) | OMOP_MODEL_SEARCH_SVC |

## Output

17-table DDL, CLINICAL_NLP_MODEL_REFERENCE (245 columns), Cortex Search Service, terminology seed (154,626 concepts across 8 code systems).
