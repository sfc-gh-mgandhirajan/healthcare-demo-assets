---
name: dicom-analytics
description: "DICOM metadata analytics, cross-modality cohort analysis, equipment utilization, temporal trends, imaging quality scoring, radiology NLP, and Cortex Search/Agent integration on Snowflake. Use when: imaging analytics, cohort analysis, equipment utilization, temporal trends, quality scoring, radiology NLP, Cortex Search imaging, Cortex Agent imaging, Semantic View imaging."
parent_skill: hcls-provider-imaging
---

# DICOM Analytics & Metadata Intelligence

## When to Load

Parent router (`hcls-provider-imaging`) routes here on **ANALYTICS** intent: imaging analytics, metadata extraction, imaging search, study analytics, radiology NLP, report extraction, cohort analysis, equipment utilization, temporal trends, quality scoring, turnaround time.

## Prerequisites

- DICOM metadata ingested into `{database}.{schema}` (run `dicom-ingestion` first if needed)
- Core tables: `DICOM_PATIENT`, `DICOM_STUDY`, `DICOM_SERIES`, `DICOM_INSTANCE`, `DICOM_EQUIPMENT`, `RADIOLOGY_REPORTS`
- Cortex AI functions available (`SNOWFLAKE.CORTEX.COMPLETE`), Cortex Search available
- Target warehouse with sufficient compute for Dynamic Tables and Cortex AI calls

## SQL References

All DDLs and query templates are in the `references/` directory:
- **`references/dynamic_tables.sql`** — 5 Dynamic Table DDLs (cohorts, equipment, timeline, quality, NLP findings)
- **`references/cortex_services.sql`** — Search corpus, Cortex Search Service, Semantic View, Cortex Agent
- **`references/query_templates.sql`** — Ad-hoc query templates (cohort, downtime, trends, anomalies, TAT, search)

All SQL functionally tested against POLARIS1 (2026-04-13).

## Workflow

### Step 1: Understand Analytics Goals

**Ask** user:
```
What imaging analytics do you need?
1. Cross-modality cohort analysis (patients by modality combos, demographics, findings)
2. Equipment utilization & operational analytics (throughput, peak hours, downtime)
3. Temporal imaging trends & longitudinal analysis (timelines, follow-up gaps, repeats)
4. Imaging quality & completeness scoring (metadata completeness, anomalies)
5. Radiology report NLP (structured extraction of findings, impressions, recommendations)
6. Enhanced Cortex Search with faceted filtering (semantic + modality/body part filters)
7. Semantic View for natural-language analytics (text-to-SQL over imaging data)
8. Cortex Agent integration (conversational analytics: search + structured queries)
9. All of the above
```

**MANDATORY STOPPING POINT:** Confirm scope before creating objects — each step creates Dynamic Tables/services with cost implications.

### Step 2: Cross-Modality Cohort Analysis

**Goal:** Segment patients by modality combinations, demographics, body parts, and findings.

Use ad-hoc queries from `references/query_templates.sql` (cross-modality cohort, frequency-based cohort).

**Materialized cohort:** Create `DT_IMAGING_COHORTS` from `references/dynamic_tables.sql`.

**MANDATORY STOPPING POINT:** Confirm cohort DT creation — consumes warehouse credits on refresh.

### Step 3: Equipment Utilization & Operational Analytics

**Goal:** Scanner throughput, peak hours, downtime detection, and comparative equipment performance.

Use queries from `references/query_templates.sql` (downtime detection, comparative performance).

**Materialized utilization:** Create `DT_EQUIPMENT_UTILIZATION_DEEP` from `references/dynamic_tables.sql`.

**MANDATORY STOPPING POINT:** Confirm equipment utilization DT creation.

### Step 4: Temporal Imaging Trends & Longitudinal Analysis

**Goal:** Patient-level timelines, follow-up gaps, repeat study detection, population-level volume trends.

Use queries from `references/query_templates.sql` (repeat detection, monthly trends).

**Materialized timeline:** Create `DT_PATIENT_IMAGING_TIMELINE` from `references/dynamic_tables.sql`.

**MANDATORY STOPPING POINT:** Confirm temporal DT creation.

### Step 5: Imaging Quality & Completeness Scoring

**Goal:** Score metadata completeness, detect anomalies, flag missing data.

Use anomaly detection query from `references/query_templates.sql`.

**Materialized scorecard:** Create `DT_IMAGING_QUALITY_SCORECARD` from `references/dynamic_tables.sql`.

**MANDATORY STOPPING POINT:** Confirm quality scorecard DT creation.

### Step 6: Radiology Report NLP

**Goal:** Extract structured findings from radiology reports using `SNOWFLAKE.CORTEX.COMPLETE` with JSON output.

**Materialized findings:** Create `DT_RADIOLOGY_FINDINGS` from `references/dynamic_tables.sql`.

Use turnaround-time query from `references/query_templates.sql`.

**MANDATORY STOPPING POINT:** NLP DT has per-token Cortex AI cost.

### Step 7: Enhanced Cortex Search with Faceted Filtering

**Goal:** Cortex Search Service with ATTRIBUTES for combined semantic + structured filtering.

Create `IMAGING_SEARCH_CORPUS` and `IMAGING_SEARCH_SVC` from `references/cortex_services.sql`.

Use search queries from `references/query_templates.sql` (semantic, single-filter, combined-filter).

**MANDATORY STOPPING POINT:** Cortex Search Service runs continuously.

### Step 8: Semantic View Integration

**Goal:** Create a Semantic View over core DICOM tables for natural-language analytics via Cortex Analyst.

Create `DICOM_ANALYTICS_SV` from `references/cortex_services.sql`.

**Verify:** `DESCRIBE SEMANTIC VIEW {database}.{schema}.DICOM_ANALYTICS_SV;`

**MANDATORY STOPPING POINT:** Confirm Semantic View creation before Cortex Agent setup.

### Step 9: Cortex Agent Integration

**Goal:** Cortex Agent combining Semantic View (structured queries) + Cortex Search (report search) for conversational imaging analytics.

Create `IMAGING_ANALYTICS_AGENT` from `references/cortex_services.sql`.

**Verify:** `DESCRIBE AGENT {database}.{schema}.IMAGING_ANALYTICS_AGENT;`

Use example agent prompts from `references/query_templates.sql`.

**MANDATORY STOPPING POINT:** Cortex Agent is the capstone integration object.

## Key Schema Notes (Normalized 19-Table Model)

- **MODALITY, BODY_PART_EXAMINED** are on `DICOM_SERIES` (NOT `DICOM_STUDY`)
- **INSTITUTION_NAME, MANUFACTURER** are on `DICOM_EQUIPMENT`, joined via `SERIES_KEY` (NOT `STUDY_KEY`)
- **STUDY_DATE** is `DATE` type, **STUDY_TIME** is `TIME` type — use `TIMESTAMP_FROM_PARTS(STUDY_DATE, STUDY_TIME)` for timestamps
- All joins: PATIENT→STUDY via `PATIENT_KEY`, STUDY→SERIES via `STUDY_KEY`, SERIES→EQUIPMENT via `SERIES_KEY`

## Output

| Object | Type | Purpose |
|--------|------|---------|
| `DT_IMAGING_COHORTS` | Dynamic Table | Cross-modality patient cohort profiles |
| `DT_EQUIPMENT_UTILIZATION_DEEP` | Dynamic Table | Scanner throughput, peak hours, utilization |
| `DT_PATIENT_IMAGING_TIMELINE` | Dynamic Table | Longitudinal timelines with follow-up detection |
| `DT_IMAGING_QUALITY_SCORECARD` | Dynamic Table | Metadata completeness and anomaly flags |
| `DT_RADIOLOGY_FINDINGS` | Dynamic Table | NLP-extracted findings, impressions, recommendations |
| `IMAGING_SEARCH_CORPUS` | Table | Denormalized search corpus |
| `IMAGING_SEARCH_SVC` | Cortex Search Service | Semantic search with faceted filtering |
| `DICOM_ANALYTICS_SV` | Semantic View | Natural-language analytics over DICOM tables |
| `IMAGING_ANALYTICS_AGENT` | Cortex Agent | Conversational analytics (Semantic View + Search) |

All objects in `{database}.{schema}`.

## Evidence Grounding: PubMed CKE (Optional)

If the `$cke-pubmed` skill is available in your environment, invoke it when radiology research context enriches imaging analytics. If unavailable, skip this section — the imaging analytics workflow is fully functional without it.

- Imaging biomarkers, modality-specific diagnostic criteria, evidence-based guidelines
- Augment Cortex AI extraction prompts with published radiology evidence
- Compare institutional patterns against published utilization benchmarks
- Reference ACR Appropriateness Criteria for follow-up interval validation

See `$cke-pubmed` for setup, query patterns, and imaging research context SQL pattern.
