---
name: governance
description: "Apply data governance to Clinical NLP pipeline outputs: PHI column masking, row-access policies, role hierarchy, Cortex AI guardrails, ML feature views, audit logging, and tag-based auto-classification. Personas: clinical managers (masked PHI), population health analysts (de-identified aggregates), AI/talk-to-my-data (semantic layer with guardrails), ML models (governed feature serving)."
parent_skill: hcls-provider-cdata-clinical-nlp
---

# Clinical NLP Governance

## When to Load

Router dispatches here on GOVERNANCE intent: "PHI masking", "de-identification", "clinical data governance", "audit", "role setup", "feature view", "Cortex guardrails".

## Prerequisites

- Clinical NLP tables created (via pipeline-implementation or extraction sub-skills)
- Data Model Knowledge preflight READY (router runs Step 0 before loading this sub-skill)
- ACCOUNTADMIN or SECURITYADMIN for policy creation; SYSADMIN for role/grant setup

## Architecture Overview

```
                    ┌─────────────────────────────────────────┐
                    │           GOVERNANCE LAYERS              │
                    ├─────────────────────────────────────────┤
                    │                                         │
                    │  L1  Tag-Based Classification            │
                    │      (SNOWFLAKE.CORE tags on PHI cols)   │
                    │              │                           │
                    │              v                           │
                    │  L2  Column Masking Policies              │
                    │      (tag-based, one policy per type)    │
                    │              │                           │
                    │              v                           │
                    │  L3  Row-Access Policies                  │
                    │      (patient attribution, department)   │
                    │              │                           │
                    │              v                           │
                    │  L4  Role Hierarchy                       │
                    │      READER → ANALYST → ADMIN            │
                    │              │                           │
                    │              v                           │
                    │  L5  Cortex AI Guardrails                 │
                    │      (semantic view column restrictions) │
                    │              │                           │
                    │              v                           │
                    │  L6  ML Feature Views                     │
                    │      (de-identified, typed, versioned)   │
                    │              │                           │
                    │              v                           │
                    │  L7  Audit & Compliance                   │
                    │      (ACCESS_HISTORY + QUERY_HISTORY)    │
                    └─────────────────────────────────────────┘
```

## PHI Column Inventory

16 PHI columns across 12 tables (from Data Model Knowledge `contains_phi = 'Y'`):

| Table | PHI Column | Type | PHI Category |
|-------|-----------|------|-------------|
| NOTE_DOCUMENT | patient_id | VARCHAR | Direct identifier |
| NOTE_DOCUMENT | author | VARCHAR | Workforce identifier |
| NOTE_DOCUMENT | raw_text | TEXT | Free-text PHI (names, dates, MRNs embedded) |
| CONDITION | patient_id | VARCHAR | Direct identifier |
| OBSERVATION | patient_id | VARCHAR | Direct identifier |
| PROCEDURE | patient_id | VARCHAR | Direct identifier |
| MEDICATION_REQUEST | patient_id | VARCHAR | Direct identifier |
| MEDICATION_REQUEST | requester | VARCHAR | Workforce identifier |
| ALLERGY_INTOLERANCE | patient_id | VARCHAR | Direct identifier |
| ADVERSE_EVENT | patient_id | VARCHAR | Direct identifier |
| SOCIAL_HISTORY_OBSERVATION | patient_id | VARCHAR | Direct identifier |
| FAMILY_MEMBER_HISTORY | patient_id | VARCHAR | Direct identifier |
| CARE_PLAN_ITEM | patient_id | VARCHAR | Direct identifier |
| TUMOR_EPISODE | patient_id | VARCHAR | Direct identifier |
| NLP_NOTE_ENTITY_MENTION | patient_id | VARCHAR | Direct identifier |
| NLP_NOTE_ENTITY_MENTION | text | VARCHAR | Free-text span (may contain PHI) |

**Non-PHI tables** (no masking needed): CODE_SYSTEM, CONCEPT_DIMENSION, NOTE_SECTION, NLP_NOTE_ENTITY_ATTRIBUTE, NLP_NOTE_ENTITY_RELATION.

---

## L1: Tag-Based Auto-Classification

Apply Snowflake object tags to PHI columns so masking policies follow the data through clones, shares, and lineage.

### Step 1: Create Tags

```sql
USE DATABASE UNSTRUCTURED_HEALTHDATA;
USE SCHEMA DATA_MODEL_KNOWLEDGE;

CREATE TAG IF NOT EXISTS PHI_CATEGORY
  ALLOWED_VALUES 'DIRECT_IDENTIFIER', 'WORKFORCE_IDENTIFIER', 'FREE_TEXT_PHI', 'NONE'
  COMMENT = 'HIPAA PHI classification for Clinical NLP columns';
```

### Step 2: Apply Tags to PHI Columns

```sql
-- Direct identifiers (patient_id across 11 tables)
ALTER TABLE CONDITION MODIFY COLUMN patient_id SET TAG PHI_CATEGORY = 'DIRECT_IDENTIFIER';
ALTER TABLE OBSERVATION MODIFY COLUMN patient_id SET TAG PHI_CATEGORY = 'DIRECT_IDENTIFIER';
ALTER TABLE PROCEDURE MODIFY COLUMN patient_id SET TAG PHI_CATEGORY = 'DIRECT_IDENTIFIER';
ALTER TABLE MEDICATION_REQUEST MODIFY COLUMN patient_id SET TAG PHI_CATEGORY = 'DIRECT_IDENTIFIER';
ALTER TABLE ALLERGY_INTOLERANCE MODIFY COLUMN patient_id SET TAG PHI_CATEGORY = 'DIRECT_IDENTIFIER';
ALTER TABLE ADVERSE_EVENT MODIFY COLUMN patient_id SET TAG PHI_CATEGORY = 'DIRECT_IDENTIFIER';
ALTER TABLE SOCIAL_HISTORY_OBSERVATION MODIFY COLUMN patient_id SET TAG PHI_CATEGORY = 'DIRECT_IDENTIFIER';
ALTER TABLE FAMILY_MEMBER_HISTORY MODIFY COLUMN patient_id SET TAG PHI_CATEGORY = 'DIRECT_IDENTIFIER';
ALTER TABLE CARE_PLAN_ITEM MODIFY COLUMN patient_id SET TAG PHI_CATEGORY = 'DIRECT_IDENTIFIER';
ALTER TABLE TUMOR_EPISODE MODIFY COLUMN patient_id SET TAG PHI_CATEGORY = 'DIRECT_IDENTIFIER';
ALTER TABLE NOTE_DOCUMENT MODIFY COLUMN patient_id SET TAG PHI_CATEGORY = 'DIRECT_IDENTIFIER';
ALTER TABLE NLP_NOTE_ENTITY_MENTION MODIFY COLUMN patient_id SET TAG PHI_CATEGORY = 'DIRECT_IDENTIFIER';

-- Workforce identifiers
ALTER TABLE NOTE_DOCUMENT MODIFY COLUMN author SET TAG PHI_CATEGORY = 'WORKFORCE_IDENTIFIER';
ALTER TABLE MEDICATION_REQUEST MODIFY COLUMN requester SET TAG PHI_CATEGORY = 'WORKFORCE_IDENTIFIER';

-- Free-text PHI (may contain embedded names, dates, MRNs)
ALTER TABLE NOTE_DOCUMENT MODIFY COLUMN raw_text SET TAG PHI_CATEGORY = 'FREE_TEXT_PHI';
ALTER TABLE NLP_NOTE_ENTITY_MENTION MODIFY COLUMN text SET TAG PHI_CATEGORY = 'FREE_TEXT_PHI';
```

### Step 3: Verify Tags

```sql
SELECT tag_name, tag_value, object_name AS table_name, column_name
FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
  'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CONDITION', 'TABLE'))
WHERE tag_name = 'PHI_CATEGORY';
```

---

## L2: Column Masking Policies

Tag-based masking: one policy per data type, applied via tag. When new PHI columns are added, just tag them — masking follows automatically.

### Strategy

| PHI Category | Masking Behavior (READER) | Masking Behavior (ANALYST) |
|---|---|---|
| DIRECT_IDENTIFIER | `'***MASKED***'` | Full value |
| WORKFORCE_IDENTIFIER | `'***MASKED***'` | Full value |
| FREE_TEXT_PHI | `'[PHI REDACTED — request ANALYST access]'` | Full value |

### Step 1: Create Masking Policies

```sql
CREATE OR REPLACE MASKING POLICY clinical_nlp_mask_varchar AS (val VARCHAR)
  RETURNS VARCHAR ->
  CASE
    WHEN IS_ROLE_IN_SESSION('CLINICAL_NLP_ANALYST') THEN val
    WHEN IS_ROLE_IN_SESSION('CLINICAL_NLP_ADMIN') THEN val
    ELSE '***MASKED***'
  END
  COMMENT = 'Masks VARCHAR PHI columns for non-ANALYST roles';

CREATE OR REPLACE MASKING POLICY clinical_nlp_mask_text AS (val TEXT)
  RETURNS TEXT ->
  CASE
    WHEN IS_ROLE_IN_SESSION('CLINICAL_NLP_ANALYST') THEN val
    WHEN IS_ROLE_IN_SESSION('CLINICAL_NLP_ADMIN') THEN val
    ELSE '[PHI REDACTED — request ANALYST access]'
  END
  COMMENT = 'Masks TEXT PHI columns (raw clinical notes) for non-ANALYST roles';
```

### Step 2: Apply via Tag-Based Masking

```sql
ALTER TAG PHI_CATEGORY SET
  MASKING POLICY clinical_nlp_mask_varchar ON 'DIRECT_IDENTIFIER',
  MASKING POLICY clinical_nlp_mask_varchar ON 'WORKFORCE_IDENTIFIER',
  MASKING POLICY clinical_nlp_mask_text ON 'FREE_TEXT_PHI';
```

> **Why tag-based?** When new tables are added to the clinical NLP schema, just tag the PHI columns. The masking policy automatically applies — no per-table ALTER TABLE needed.

### Step 3: Verify Masking

```sql
-- As CLINICAL_NLP_READER (should see masked values):
USE ROLE CLINICAL_NLP_READER;
SELECT patient_id, raw_text FROM NOTE_DOCUMENT LIMIT 3;
-- Expected: patient_id = '***MASKED***', raw_text = '[PHI REDACTED — request ANALYST access]'

-- As CLINICAL_NLP_ANALYST (should see full values):
USE ROLE CLINICAL_NLP_ANALYST;
SELECT patient_id, raw_text FROM NOTE_DOCUMENT LIMIT 3;
-- Expected: full values visible
```

---

## L3: Row-Access Policies

Control which patients/records each role can see. Two patterns for clinical NLP:

### Pattern A: Department-Based (Clinical Managers)

Clinical managers see only patients from their department. Requires a mapping table.

```sql
CREATE TABLE IF NOT EXISTS CLINICAL_NLP_DEPT_ACCESS (
  role_name VARCHAR NOT NULL,
  department VARCHAR NOT NULL,
  UNIQUE(role_name, department)
);

-- Example: Cardiology manager sees only cardiology notes
INSERT INTO CLINICAL_NLP_DEPT_ACCESS VALUES
  ('CARDIOLOGY_MANAGER', 'Cardiology'),
  ('ONCOLOGY_MANAGER', 'Oncology'),
  ('ED_MANAGER', 'Emergency');
```

```sql
CREATE OR REPLACE ROW ACCESS POLICY clinical_nlp_dept_rap
  AS (note_type VARCHAR)
  RETURNS BOOLEAN ->
  CASE
    WHEN IS_ROLE_IN_SESSION('CLINICAL_NLP_ADMIN') THEN TRUE
    WHEN IS_ROLE_IN_SESSION('CLINICAL_NLP_ANALYST') THEN TRUE
    WHEN EXISTS (
      SELECT 1 FROM CLINICAL_NLP_DEPT_ACCESS
      WHERE role_name = CURRENT_ROLE() AND department = note_type
    ) THEN TRUE
    ELSE FALSE
  END;

-- Apply to NOTE_DOCUMENT (filters cascade through JOINs to clinical tables)
ALTER TABLE NOTE_DOCUMENT ADD ROW ACCESS POLICY clinical_nlp_dept_rap ON (note_type);
```

### Pattern B: Patient-Attribution (Population Health)

Pop health analysts see only their attributed patient panel.

```sql
CREATE TABLE IF NOT EXISTS CLINICAL_NLP_PATIENT_ATTRIBUTION (
  role_name VARCHAR NOT NULL,
  patient_id VARCHAR NOT NULL,
  attribution_start DATE,
  attribution_end DATE,
  UNIQUE(role_name, patient_id)
);
```

```sql
CREATE OR REPLACE ROW ACCESS POLICY clinical_nlp_patient_rap
  AS (patient_id_col VARCHAR)
  RETURNS BOOLEAN ->
  CASE
    WHEN IS_ROLE_IN_SESSION('CLINICAL_NLP_ADMIN') THEN TRUE
    WHEN IS_ROLE_IN_SESSION('CLINICAL_NLP_ANALYST') THEN TRUE
    WHEN EXISTS (
      SELECT 1 FROM CLINICAL_NLP_PATIENT_ATTRIBUTION
      WHERE role_name = CURRENT_ROLE()
        AND patient_id = patient_id_col
        AND CURRENT_DATE() BETWEEN COALESCE(attribution_start, '1900-01-01') AND COALESCE(attribution_end, '9999-12-31')
    ) THEN TRUE
    ELSE FALSE
  END;

-- Apply to all 10 clinical tables
ALTER TABLE CONDITION ADD ROW ACCESS POLICY clinical_nlp_patient_rap ON (patient_id);
ALTER TABLE OBSERVATION ADD ROW ACCESS POLICY clinical_nlp_patient_rap ON (patient_id);
ALTER TABLE PROCEDURE ADD ROW ACCESS POLICY clinical_nlp_patient_rap ON (patient_id);
ALTER TABLE MEDICATION_REQUEST ADD ROW ACCESS POLICY clinical_nlp_patient_rap ON (patient_id);
ALTER TABLE ALLERGY_INTOLERANCE ADD ROW ACCESS POLICY clinical_nlp_patient_rap ON (patient_id);
ALTER TABLE ADVERSE_EVENT ADD ROW ACCESS POLICY clinical_nlp_patient_rap ON (patient_id);
ALTER TABLE SOCIAL_HISTORY_OBSERVATION ADD ROW ACCESS POLICY clinical_nlp_patient_rap ON (patient_id);
ALTER TABLE FAMILY_MEMBER_HISTORY ADD ROW ACCESS POLICY clinical_nlp_patient_rap ON (patient_id);
ALTER TABLE CARE_PLAN_ITEM ADD ROW ACCESS POLICY clinical_nlp_patient_rap ON (patient_id);
ALTER TABLE TUMOR_EPISODE ADD ROW ACCESS POLICY clinical_nlp_patient_rap ON (patient_id);
```

> **Choose Pattern A, B, or both** depending on the use case. ADMIN and ANALYST roles always see all rows.

---

## L4: Role Hierarchy

Three-tier database role pattern for clinical NLP data:

```
CLINICAL_NLP_ADMIN          (full access, policy management, schema changes)
      │
      v
CLINICAL_NLP_ANALYST        (unmasked PHI, all rows, no schema changes)
      │
      v
CLINICAL_NLP_READER         (masked PHI, row-filtered, read-only)
```

### Create Roles

```sql
USE ROLE SECURITYADMIN;

CREATE DATABASE ROLE IF NOT EXISTS UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP_READER;
CREATE DATABASE ROLE IF NOT EXISTS UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP_ANALYST;
CREATE DATABASE ROLE IF NOT EXISTS UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP_ADMIN;

-- Hierarchy: READER < ANALYST < ADMIN
GRANT DATABASE ROLE UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP_READER
  TO DATABASE ROLE UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP_ANALYST;
GRANT DATABASE ROLE UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP_ANALYST
  TO DATABASE ROLE UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP_ADMIN;
```

### Grant Permissions

```sql
USE ROLE SECURITYADMIN;

-- READER: SELECT on all clinical NLP tables
GRANT USAGE ON DATABASE UNSTRUCTURED_HEALTHDATA
  TO DATABASE ROLE UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP_READER;
GRANT USAGE ON SCHEMA UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE
  TO DATABASE ROLE UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP_READER;
GRANT SELECT ON ALL TABLES IN SCHEMA UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE
  TO DATABASE ROLE UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP_READER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE
  TO DATABASE ROLE UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP_READER;

-- ANALYST: inherits READER + nothing extra (unmasked via masking policy logic)
-- ADMIN: inherits ANALYST + schema management
GRANT CREATE TABLE, CREATE VIEW, CREATE FUNCTION, CREATE PROCEDURE
  ON SCHEMA UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE
  TO DATABASE ROLE UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP_ADMIN;
```

### Assign to Account Roles

```sql
-- Example: clinical managers get READER, data scientists get ANALYST, pipeline admins get ADMIN
GRANT DATABASE ROLE UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP_READER TO ROLE CLINICAL_MANAGER;
GRANT DATABASE ROLE UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP_ANALYST TO ROLE DATA_SCIENTIST;
GRANT DATABASE ROLE UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP_ADMIN TO ROLE SYSADMIN;
```

### Persona Mapping

| Persona | Database Role | PHI Access | Row Access | Use Case |
|---------|-------------|-----------|-----------|----------|
| Clinical Manager | CLINICAL_NLP_READER + dept RAP | Masked | Department-filtered | Review NLP extractions for their department |
| Pop Health Analyst | CLINICAL_NLP_READER + patient RAP | Masked | Panel-filtered | Aggregate metrics over attributed patients |
| Data Scientist / AI | CLINICAL_NLP_ANALYST | Unmasked | All rows | Model training, feature engineering, validation |
| Cortex Analyst / Talk-to-Data | CLINICAL_NLP_READER | Masked | Via semantic view | Natural language queries, dashboards |
| Pipeline Admin | CLINICAL_NLP_ADMIN | Unmasked | All rows | Schema changes, reprocessing, normalization |

---

## L5: Cortex AI Guardrails

When Cortex Analyst or Cortex Agent queries clinical NLP tables, PHI columns must be excluded or masked in the semantic layer.

### Semantic View Restrictions

Create a semantic view that exposes only non-PHI and aggregatable columns to the AI layer:

```sql
CREATE OR REPLACE SECURE VIEW CLINICAL_NLP_AI_VIEW AS
SELECT
    -- CONDITION (no patient_id, no evidence_text with potential PHI)
    c.condition_id,
    c.display,
    c.code,
    c.code_system,
    c.clinical_status,
    c.verification_status,
    c.category,
    c.severity_display,
    c.body_site_display,
    c.laterality,
    c.onset_datetime,
    c.is_negated,
    c.temporality,
    c.certainty,
    c.extraction_confidence,
    -- NOTE context (no raw_text, no author, no patient_id)
    n.note_type,
    n.encounter_type,
    n.note_datetime
FROM CONDITION c
JOIN NOTE_DOCUMENT n ON c.note_document_id = n.note_document_id;
```

> **Pattern**: For each clinical table the AI layer needs, create a secure view that:
> 1. Excludes `patient_id`, `author`, `requester`, `raw_text`, `text`
> 2. Excludes `evidence_text` if it may contain embedded patient names
> 3. Keeps clinical columns (codes, displays, statuses, dates, confidence scores)
> 4. Joins to NOTE_DOCUMENT for note context but drops PHI columns

### Cortex Analyst Semantic Model

When building a Cortex Analyst semantic model (YAML) over clinical NLP data, reference the secure views — not base tables:

```yaml
tables:
  - name: CLINICAL_NLP_AI_VIEW
    description: "De-identified clinical NLP extractions for natural language querying"
    columns:
      - name: DISPLAY
        description: "Clinical concept display name (e.g., 'Type 2 diabetes mellitus')"
      - name: CODE
        description: "Standardized code (ICD-10-CM, SNOMED CT)"
      - name: CLINICAL_STATUS
        description: "active, resolved, inactive, remission"
      # ... (no patient_id, no raw_text)
```

### Evidence Text Decision

`evidence_text` is promoted onto all 10 clinical tables. It contains the exact citation from the source note.

| Scenario | Include evidence_text in AI view? |
|---|---|
| Notes are already de-identified (Safe Harbor / Expert Determination) | Yes — valuable for AI grounding |
| Notes contain raw PHI (names, dates, MRNs in text) | **No** — exclude from AI view |
| Partial de-identification (dates shifted, names removed) | Case-by-case — consider masking policy on evidence_text |

Add a masking policy to `evidence_text` if needed:

```sql
-- Optional: mask evidence_text for READER role
ALTER TABLE CONDITION MODIFY COLUMN evidence_text
  SET MASKING POLICY clinical_nlp_mask_varchar;
-- Repeat for all 10 clinical tables if evidence_text carries PHI risk
```

---

## L6: ML Feature Views

Serve governed, de-identified, pre-joined feature sets for model training and inference.

### Design Principles

1. **No PHI in feature views** — join to patient demographics via surrogate key, never expose real patient_id
2. **Typed and versioned** — use `_v1`, `_v2` suffix for breaking schema changes
3. **Pre-aggregated where possible** — reduce row-level PHI exposure
4. **Consistent** — same view used for training and inference (no train/serve skew)

### Example: Condition Feature View

```sql
CREATE OR REPLACE SECURE VIEW ML_FEATURES_CONDITION_V1 AS
SELECT
    MD5(c.patient_id) AS patient_hash,
    c.display AS condition_display,
    c.code,
    c.code_system,
    cd.semantic_group,
    c.clinical_status,
    c.severity_display,
    c.is_negated,
    c.certainty,
    c.extraction_confidence,
    c.onset_datetime,
    DATEDIFF('day', c.onset_datetime, CURRENT_DATE()) AS days_since_onset
FROM CONDITION c
LEFT JOIN CONCEPT_DIMENSION cd
  ON c.code = cd.code AND c.code_system = cd.code_system_id
WHERE c.is_negated = FALSE
  AND c.certainty IN ('CONFIRMED', 'PROBABLE')
  AND c.extraction_confidence >= 0.7;
```

### Example: Medication Feature View

```sql
CREATE OR REPLACE SECURE VIEW ML_FEATURES_MEDICATION_V1 AS
SELECT
    MD5(m.patient_id) AS patient_hash,
    m.medication_display,
    m.code,
    m.code_system,
    m.dosage_value,
    m.dosage_unit,
    m.route_display,
    m.frequency_display,
    m.status,
    m.extraction_confidence,
    m.authored_on
FROM MEDICATION_REQUEST m
WHERE m.is_negated = FALSE
  AND m.extraction_confidence >= 0.7;
```

### Example: Patient-Level Aggregated Features

```sql
CREATE OR REPLACE SECURE VIEW ML_FEATURES_PATIENT_SUMMARY_V1 AS
SELECT
    MD5(patient_id) AS patient_hash,
    COUNT(DISTINCT CASE WHEN code_system = 'ICD10CM' THEN code END) AS unique_icd10_conditions,
    COUNT(DISTINCT CASE WHEN code_system = 'RXNORM' THEN code END) AS unique_medications,
    SUM(CASE WHEN display ILIKE '%diabetes%' THEN 1 ELSE 0 END) AS diabetes_mention_count,
    SUM(CASE WHEN display ILIKE '%hypertension%' THEN 1 ELSE 0 END) AS htn_mention_count,
    MAX(extraction_confidence) AS max_confidence,
    MIN(extraction_confidence) AS min_confidence
FROM (
    SELECT patient_id, code, code_system, display, extraction_confidence FROM CONDITION
    UNION ALL
    SELECT patient_id, code, code_system, medication_display, extraction_confidence FROM MEDICATION_REQUEST
) combined
GROUP BY patient_id;
```

### Grant Feature Views to ML Role

```sql
GRANT SELECT ON VIEW ML_FEATURES_CONDITION_V1 TO DATABASE ROLE CLINICAL_NLP_READER;
GRANT SELECT ON VIEW ML_FEATURES_MEDICATION_V1 TO DATABASE ROLE CLINICAL_NLP_READER;
GRANT SELECT ON VIEW ML_FEATURES_PATIENT_SUMMARY_V1 TO DATABASE ROLE CLINICAL_NLP_READER;
```

> **ML models consuming features via READER role** get: de-identified (MD5 hash), confidence-filtered, negation-excluded, coded data — no raw PHI exposure.

---

## L7: Audit & Compliance

### Access History Query

Track who accessed PHI columns:

```sql
SELECT
    qh.user_name,
    qh.role_name,
    ah.direct_objects_accessed,
    ah.base_objects_accessed,
    ah.columns_accessed,
    qh.start_time,
    qh.query_text
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah
JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
  ON ah.query_id = qh.query_id
WHERE EXISTS (
    SELECT 1 FROM TABLE(FLATTEN(ah.direct_objects_accessed)) dao
    WHERE dao.value:objectName::STRING ILIKE '%CONDITION%'
       OR dao.value:objectName::STRING ILIKE '%NOTE_DOCUMENT%'
       OR dao.value:objectName::STRING ILIKE '%MEDICATION_REQUEST%'
)
AND qh.start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY qh.start_time DESC
LIMIT 100;
```

### PHI Column Access Report

Identify which users accessed specific PHI columns:

```sql
SELECT
    qh.user_name,
    qh.role_name,
    col.value:columnName::STRING AS column_accessed,
    dao.value:objectName::STRING AS table_accessed,
    COUNT(*) AS access_count,
    MIN(qh.start_time) AS first_access,
    MAX(qh.start_time) AS last_access
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah
JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON ah.query_id = qh.query_id,
LATERAL FLATTEN(ah.direct_objects_accessed) dao,
LATERAL FLATTEN(dao.value:columns) col
WHERE col.value:columnName::STRING IN ('PATIENT_ID', 'RAW_TEXT', 'AUTHOR', 'REQUESTER', 'TEXT')
AND qh.start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2, 3, 4
ORDER BY access_count DESC;
```

### Masking Policy Effectiveness Check

Verify masking is working — compare query results across roles:

```sql
-- Run as ACCOUNTADMIN to check policy assignments
SELECT
    policy_name, policy_kind, ref_entity_name AS table_name, ref_column_name AS column_name
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    ref_entity_name => 'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CONDITION',
    ref_entity_domain => 'TABLE'
));
```

### Compliance Summary View

```sql
CREATE OR REPLACE VIEW CLINICAL_NLP_GOVERNANCE_SUMMARY AS
SELECT
    r.TABLE_NAME,
    r.COLUMN_NAME,
    r.CONTAINS_PHI,
    CASE WHEN r.CONTAINS_PHI = 'Y' THEN 'REQUIRED' ELSE 'NOT_REQUIRED' END AS masking_status,
    r.DATA_TYPE,
    r.DESCRIPTION
FROM UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CLINICAL_NLP_MODEL_REFERENCE r
WHERE r.CONTAINS_PHI = 'Y'
ORDER BY r.TABLE_NAME, r.COLUMN_NAME;
```

---

## Quick-Start Deployment Sequence

For users who want to apply all governance in one pass:

```
Step 1: Create database roles (L4)
Step 2: Create PHI_CATEGORY tag (L1)
Step 3: Tag all PHI columns (L1)
Step 4: Create masking policies (L2)
Step 5: Apply tag-based masking (L2)
Step 6: Choose and apply row-access policy pattern (L3)
Step 7: Create AI secure views (L5) — if using Cortex Analyst / Agent
Step 8: Create ML feature views (L6) — if serving features to models
Step 9: Assign database roles to account roles
Step 10: Verify masking + row access with test queries
Step 11: Set up audit queries as scheduled tasks (L7)
```

## Platform Affinities

When implementing governance, CoCo should also load:
- **data-governance** skill (for masking/RAP/tagging syntax reference and best practices)
- **data-quality** skill (for DMF-based monitoring of governance compliance)

## Stopping Points

- **⚠️ MANDATORY STOPPING POINT** after L1 (tagging): Confirm tag values with user before proceeding to masking
- **⚠️ MANDATORY STOPPING POINT** after L2 (masking): Test masking with a SELECT before applying row-access policies
- **⚠️ MANDATORY STOPPING POINT** after L3 (row access): Confirm RAP pattern choice (department vs. patient attribution vs. both)
- **⚠️ MANDATORY STOPPING POINT** after L5 (AI guardrails): Confirm which columns to expose to the AI layer
- **⚠️ MANDATORY STOPPING POINT** after L6 (ML views): Confirm feature view schema with the ML team

## Output

7-layer governance framework: PHI tags, masking policies, row-access policies, database roles, AI secure views, ML feature views, audit queries.
