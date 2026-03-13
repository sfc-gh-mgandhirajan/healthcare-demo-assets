---
name: dicom-analytics
description: "DICOM metadata analytics, radiology report NLP, and imaging search using Cortex AI functions and Cortex Search on Snowflake."
parent_skill: healthcare-imaging
---

# DICOM Analytics & Metadata Intelligence

## When to Load

Healthcare-imaging router: After user intent matches ANALYTICS.

## Prerequisites

- DICOM metadata ingested (run `dicom-ingestion` skill first if needed)
- Cortex AI functions available (COMPLETE, EXTRACT, SUMMARIZE)
- Cortex Search service available for semantic search

## Workflow

### Step 1: Understand Analytics Goals

**Ask** user:
```
What imaging analytics do you need?
1. Study-level dashboards (volume, modality mix, turnaround times)
2. Radiology report NLP (extract findings, impressions, diagnoses)
3. Semantic search across imaging metadata and reports
4. Population-level imaging trends
5. Anomaly detection (missing metadata, duplicates, quality issues)
```

### Step 2: Build Analytical Views

**Goal:** Create curated analytical layers.

**Study Volume Analytics:**
```sql
CREATE OR REPLACE DYNAMIC TABLE imaging_study_metrics
  TARGET_LAG = '30 minutes'
  WAREHOUSE = analytics_wh
AS
SELECT
  DATE_TRUNC('day', TRY_TO_DATE(study_date, 'YYYYMMDD')) AS study_day,
  modality,
  body_part,
  institution,
  COUNT(DISTINCT study_uid) AS study_count,
  COUNT(DISTINCT patient_id) AS patient_count,
  COUNT(DISTINCT series_uid) AS series_count,
  COUNT(*) AS image_count
FROM dicom_studies
GROUP BY 1, 2, 3, 4;
```

### Step 3: Radiology Report NLP with Cortex AI

**Goal:** Extract structured findings from unstructured radiology reports.

**Extract clinical entities:**
```sql
CREATE OR REPLACE DYNAMIC TABLE radiology_findings
  TARGET_LAG = '1 hour'
  WAREHOUSE = analytics_wh
AS
SELECT
  study_uid,
  patient_id,
  report_text,
  SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
    report_text,
    'What are the key findings?'
  ) AS key_findings,
  SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
    report_text,
    'What is the impression or diagnosis?'
  ) AS impression,
  SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
    report_text,
    'Are there any critical or urgent findings?'
  ) AS critical_findings,
  SNOWFLAKE.CORTEX.SENTIMENT(report_text) AS report_sentiment
FROM radiology_reports;
```

**Summarize lengthy reports:**
```sql
SELECT
  study_uid,
  SNOWFLAKE.CORTEX.SUMMARIZE(report_text) AS report_summary
FROM radiology_reports
WHERE LENGTH(report_text) > 500;
```

### Step 4: Cortex Search for Imaging Metadata

**Goal:** Enable semantic search across imaging studies and reports.

**Create Cortex Search Service:**
```sql
CREATE OR REPLACE CORTEX SEARCH SERVICE imaging_search_svc
  ON imaging_search_corpus
  WAREHOUSE = analytics_wh
  TARGET_LAG = '1 hour'
AS (
  SELECT
    study_uid,
    patient_id,
    modality,
    study_description,
    body_part,
    report_text,
    CONCAT(
      'Study: ', study_description,
      ' Modality: ', modality,
      ' Body Part: ', body_part,
      ' Report: ', COALESCE(report_text, '')
    ) AS search_text
  FROM dicom_studies_with_reports
);
```

**Query the search service (via Cortex Agent or API):**
```sql
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'imaging_search_svc',
    '{"query": "chest CT with pulmonary nodule", "columns": ["study_uid", "modality", "study_description"], "limit": 10}'
  )
);
```

### Step 5: Data Quality & Anomaly Detection

**Goal:** Identify imaging data quality issues.

```sql
SELECT
  'Missing Patient ID' AS issue,
  COUNT(*) AS count
FROM dicom_studies WHERE patient_id IS NULL
UNION ALL
SELECT
  'Missing Modality',
  COUNT(*)
FROM dicom_studies WHERE modality IS NULL
UNION ALL
SELECT
  'Duplicate Study UID',
  COUNT(*)
FROM (
  SELECT study_uid FROM dicom_studies
  GROUP BY study_uid HAVING COUNT(*) > 1
);
```

## Stopping Points

- After Step 1 to confirm analytics scope
- After Step 3 before creating Cortex AI pipelines (cost implications)
- After Step 4 before creating Search service

## Output

- Analytical Dynamic Tables for study metrics
- NLP-enriched radiology findings table
- Cortex Search service for semantic imaging search
- Data quality summary

## Cortex Knowledge Extension: PubMed CKE

**PubMed Biomedical Research Corpus** from Snowflake Marketplace (listing `GZSTZ67BY9OQW`) provides RAG-based search across biomedical literature for radiology research context.

**Setup:** Install from Marketplace → shared Cortex Search Service appears in your account.

**When to use in imaging analytics:**
- **Radiology research context:** Search for imaging biomarkers, modality-specific diagnostic criteria, and evidence-based imaging guidelines
- **Report enrichment:** Augment Cortex AI extraction with published radiology evidence
- **Population benchmarking:** Compare institutional imaging patterns against published utilization studies

**Query Pattern:**
```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  '<CKE_DB>.SHARED.CKE_PUBMED_SERVICE',
  '{"query": "pulmonary nodule CT screening Lung-RADS classification", "columns": ["chunk", "document_title", "source_url"]}'
);
```

**Integration with imaging analytics:**
```sql
SELECT
  r.study_uid,
  r.key_findings,
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    '<CKE_DB>.SHARED.CKE_PUBMED_SERVICE',
    '{"query": "' || r.key_findings::STRING || ' radiology evidence", "columns": ["chunk", "document_title", "source_url"]}'
  ) AS literature_context
FROM radiology_findings r
WHERE r.critical_findings IS NOT NULL
LIMIT 10;
```
