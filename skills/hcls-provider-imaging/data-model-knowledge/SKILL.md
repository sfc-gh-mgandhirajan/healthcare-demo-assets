---
name: data-model-knowledge
description: "DICOM 19-table data model reference. Directs sub-skills to read the authoritative DDL file instead of generating schema from LLM memory."
parent_skill: hcls-provider-imaging
---

# DICOM Data Model Knowledge

## Purpose

This sub-skill provides **grounded schema knowledge** for the 19-table DICOM data model. Instead of generating DDL from LLM memory (which causes hallucinated columns), all schema queries are resolved by reading the authoritative DDL reference file.

## Authoritative Source

The single source of truth for the DICOM data model is:

```
dicom-parser/references/data_model_ddl.sql
```

This file contains all 19 `CREATE TABLE` DDLs with exact column names, data types, constraints, foreign keys, and defaults.

## When to Use

The imaging router directs sub-skills to read the DDL reference file as a **pre-step** for any intent that needs schema knowledge:
- **PARSE**: Table creation, DDL execution
- **INGEST**: Column mappings for COPY INTO / Dynamic Tables
- **ANALYTICS**: Semantic View dimensions/measures, query templates
- **GOVERNANCE**: PHI column identification

## Grounding Rule

**CRITICAL:** Sub-skills MUST read `dicom-parser/references/data_model_ddl.sql` for column definitions. Do NOT generate DDL from memory. The DDL file is the contract — if a column is not in the file, it does not exist.

## How Sub-Skills Use This

### For DDL generation (PARSE)
Read and execute the DDL file directly:
```sql
-- Execute DDLs from dicom-parser/references/data_model_ddl.sql
-- Substitute {database}.{schema} placeholders before execution
```

### For column mappings (INGEST)
Read the DDL file to extract column names and types for COPY INTO or Dynamic Table definitions.

### For Semantic View / query construction (ANALYTICS)
Read the DDL file to identify dimensions (VARCHAR columns), measures (INTEGER/FLOAT columns), and relationships (REFERENCES clauses).

### For PHI identification (GOVERNANCE)
The following columns contain PHI and require masking policies:

| Table | PHI Columns |
|-------|-------------|
| `dicom_patient` | `patient_name`, `patient_id`, `patient_birth_date`, `patient_age`, `patient_sex`, `other_patient_ids`, `other_patient_names` |
| `dicom_study` | `referring_physician`, `admitting_diagnosis` |
| `dicom_procedure_step` | `performing_physician` |
| `dicom_equipment` | `institution_name`, `institution_address` |
| `radiology_reports` | `patient_id`, `radiologist_name`, `report_text` |

## 19-Table Summary

| # | Table | FK Parent | Category |
|---|-------|-----------|----------|
| 1 | `dicom_patient` | — | CORE |
| 2 | `dicom_study` | `dicom_patient` | CORE |
| 3 | `dicom_series` | `dicom_study` | CORE |
| 4 | `dicom_instance` | `dicom_series` | CORE |
| 5 | `dicom_frame` | `dicom_instance` | CORE |
| 6 | `dicom_equipment` | `dicom_series` | EQUIPMENT |
| 7 | `dicom_image_pixel` | `dicom_instance` | IMAGE |
| 8 | `dicom_image_plane` | `dicom_instance` | IMAGE |
| 9 | `dicom_procedure_step` | `dicom_study` | PROCEDURE |
| 10 | `dicom_dose_summary` | `dicom_series` | DOSE |
| 11 | `dicom_segmentation_metadata` | `dicom_instance` | SEGMENTATION |
| 12 | `dicom_structured_report_header` | `dicom_instance` | STRUCTURED_REPORT |
| 13 | `dicom_file_location` | `dicom_instance` | FILE_MANAGEMENT |
| 14 | `dicom_element` | `dicom_instance` | TAG_ELEMENTS |
| 15 | `dicom_sequence_item` | `dicom_element` | TAG_ELEMENTS |
| 16 | `image_embedding` | `dicom_instance` | AI_ML |
| 17 | `embedding_model` | — | AI_ML |
| 18 | `embedding_evaluation` | `embedding_model` | AI_ML |
| 19 | `radiology_reports` | `dicom_study` | CLINICAL_REPORTS |

## Output

Provides schema grounding context to other sub-skills. No database objects created — this is a read-only reference layer.
