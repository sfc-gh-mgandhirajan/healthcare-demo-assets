---
name: healthcare-imaging
description: "**[REQUIRED]** Use for ALL DICOM medical imaging tasks on Snowflake. This is the entry point for healthcare imaging solutions combining platform skills with clinical imaging workflows. Triggers: DICOM, medical imaging, radiology, imaging pipeline, PACS, imaging viewer, imaging AI, imaging governance, HIPAA imaging, clinical images, pathology images, imaging metadata, imaging ML, imaging model, imaging analytics, healthcare imaging, imaging data lake, imaging FHIR, imaging study, imaging series."
---

# Healthcare Imaging Solutions on Snowflake

## Setup

1. **Load** `references/dicom-standards.md` for DICOM domain context
2. **Verify** Snowflake connection is active and target database/schema exist

## Intent Detection

| Intent | Triggers | Load |
|--------|----------|------|
| PARSE | "parse DICOM", "extract DICOM tags", "DICOM schema", "DICOM data model", "pydicom", "DICOM to Snowflake", "build DICOM tables" | `dicom-parser/SKILL.md` |
| INGEST | "ingest DICOM", "imaging pipeline", "load images", "PACS integration", "stage DICOM", "stream images", "dynamic table imaging" | `dicom-ingestion/SKILL.md` |
| ANALYTICS | "imaging analytics", "metadata extraction", "imaging search", "Cortex search imaging", "study analytics", "radiology NLP", "report extraction" | `dicom-analytics/SKILL.md` |
| VIEWER | "imaging viewer", "Streamlit imaging", "DICOM viewer", "imaging app", "imaging dashboard", "radiology UI", "deploy viewer" | `imaging-viewer/SKILL.md` |
| GOVERNANCE | "imaging governance", "HIPAA", "PHI masking", "imaging audit", "imaging classification", "imaging access policy", "de-identification" | `imaging-governance/SKILL.md` |
| ML | "imaging model", "train imaging", "imaging classification ML", "pathology model", "radiology AI", "deploy imaging model", "imaging inference" | `imaging-ml/SKILL.md` |

## Workflow

```
Start
  |
  v
Detect Intent from table above
  |
  +---> PARSE -----> Load dicom-parser/SKILL.md (has parse_dicom.py script + 18-table data model)
  |
  +---> INGEST ----> Load dicom-ingestion/SKILL.md
  |
  +---> ANALYTICS -> Load dicom-analytics/SKILL.md
  |
  +---> VIEWER ----> Load imaging-viewer/SKILL.md
  |
  +---> GOVERNANCE > Load imaging-governance/SKILL.md
  |
  +---> ML --------> Load imaging-ml/SKILL.md
```

## Cross-Cutting Concerns

All sub-skills should apply these platform patterns:

- **DICOM Parsing**: The `dicom-parser` sub-skill contains a comprehensive 18-table DICOM data model and a pydicom-based parser script. Use it as the foundation before ingestion or analytics.
- **Data Engineering**: Dynamic Tables for incremental refresh, Streams/Tasks for event-driven pipelines
- **AI/ML**: Cortex AI functions (COMPLETE, EXTRACT, SENTIMENT), Cortex Search for imaging metadata, ML Registry for models
- **Apps**: Streamlit in Snowflake for dashboards, SPCS for compute-heavy imaging workloads
- **Security**: Data masking for PHI, row-access policies per role, SYSTEM$CLASSIFY for PII detection, audit trails via ACCESS_HISTORY

## Stopping Points

- After intent detection if ambiguous
- Before creating any database objects
- Before deploying apps or models
