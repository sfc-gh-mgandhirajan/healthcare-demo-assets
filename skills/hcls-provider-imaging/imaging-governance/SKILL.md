---
name: imaging-governance
description: "HIPAA-compliant governance for DICOM imaging data on Snowflake. Full 19-table PHI masking, VARIANT masking, object tagging, row-access policies, aggregation policies, de-identification, audit trails, and data retention. Use when: imaging governance, HIPAA, PHI masking, imaging audit, imaging classification, imaging access policy, de-identification, DICOM governance, imaging data retention, consent management."
parent_skill: hcls-provider-imaging
---

# Imaging Data Governance & HIPAA Compliance

## When to Load

Parent router (`hcls-provider-imaging`) routes here on **GOVERNANCE** intent: imaging governance, HIPAA, PHI masking, imaging audit, classification, access policy, de-identification, consent management, data retention.

## Prerequisites

- DICOM imaging tables deployed in Snowflake (19-table model or subset)
- ACCOUNTADMIN or SECURITYADMIN role for policy/tag creation
- Active warehouse (e.g., `{warehouse}`)

## SQL References

All DDLs are in the `references/` directory:
- **`references/policies_and_tags.sql`** — Tags (PHI_TYPE, SENSITIVITY_LEVEL, HIPAA_SAFE_HARBOR), masking policies (STRING/DATE/NUMBER/VARIANT), tag application, row access policies, aggregation policy, consent registry
- **`references/audit_deidentification_retention.sql`** — De-identification DT, audit view, anomalous access alert, retention registry, archival task

## Workflow

### PHI Column Inventory

The following tables/columns contain Protected Health Information (PHI) per the DICOM data model:

| Table | PHI Column | Type | HIPAA ID |
|-------|-----------|------|----------|
| DICOM_PATIENT | patient_name | STRING | Name |
| DICOM_PATIENT | patient_id | STRING | MRN |
| DICOM_PATIENT | patient_birth_date | DATE | DOB |
| DICOM_STUDY | referring_physician | STRING | Name |
| DICOM_STUDY | accession_number | STRING | Unique ID |
| DICOM_EQUIPMENT | device_serial_number | STRING | Device Serial |
| DICOM_EQUIPMENT | station_name | STRING | Device ID |
| DICOM_PROCEDURE_STEP | performing_physician | STRING | Name |
| DICOM_FILE_LOCATION | storage_uri | STRING | May embed patient ID in path |
| DICOM_RAW | metadata | VARIANT | Embedded PHI tags |
| RADIOLOGY_REPORTS | patient_id | STRING | MRN |
| RADIOLOGY_REPORTS | radiologist_name | STRING | Name |
| RADIOLOGY_REPORTS | report_text | STRING | Free-text PHI |

MANDATORY STOPPING POINT: Present PHI column inventory to user. Confirm scope before proceeding.

### Step 1: PHI Discovery & Classification

**Goal:** Auto-detect PHI columns not in the inventory above.

Run `SYSTEM$CLASSIFY` on each PHI-containing table with `{'auto_tag': true}`. Cross-reference results with the PHI column inventory. Verify coverage against **HIPAA Safe Harbor 18 identifiers**.

MANDATORY STOPPING POINT: Present classification results and coverage gaps before creating tags.

### Step 2: Object Tagging for PHI Classification

**Goal:** Create tag taxonomy and apply to all PHI columns.

Create tags and apply from `references/policies_and_tags.sql`.

Verify:
```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    '{database}.{schema}.DICOM_PATIENT', 'TABLE'))
WHERE TAG_NAME IN ('PHI_TYPE', 'SENSITIVITY_LEVEL', 'HIPAA_SAFE_HARBOR');
```

MANDATORY STOPPING POINT: Confirm all PHI columns tagged before creating masking policies.

### Step 3: Dynamic Data Masking — All Data Types

**Goal:** Create and apply masking policies for STRING, DATE, NUMBER columns.

Create and apply policies from `references/policies_and_tags.sql`. Authorized roles: `PHI_AUTHORIZED`, `IMAGING_ADMIN`.

MANDATORY STOPPING POINT: Verify masking with a test query before proceeding to VARIANT masking.

### Step 4: VARIANT Column Masking for DICOM Raw Metadata

**Goal:** Mask PHI DICOM tags inside VARIANT while preserving non-PHI metadata.

PHI DICOM tags to redact: PatientName, PatientID, PatientBirthDate, ReferringPhysicianName, InstitutionName, InstitutionAddress, PerformingPhysicianName, OperatorsName, PatientAddress, PatientTelephoneNumbers, DeviceSerialNumber, StationName.

Apply `PHI_VARIANT_MASK` from `references/policies_and_tags.sql`. If VARIANT keys use tag notation (e.g., `00100010`), adapt OBJECT_DELETE keys accordingly.

### Step 5: Row Access Policies

**Goal:** Restrict imaging data by institution, department, and patient consent.

Create institution RAP, department RAP, and consent-based RAP from `references/policies_and_tags.sql`.

MANDATORY STOPPING POINT: Test RAPs with multiple roles before proceeding.

### Step 6: Aggregation Policies

**Goal:** Enforce k-anonymity (minimum cell size) for research analytics.

Create and apply `IMAGING_MIN_CELL_SIZE` aggregation policy from `references/policies_and_tags.sql`. Research analysts must use GROUP BY with aggregates; groups smaller than 5 are suppressed.

### Step 7: De-Identification Pipeline — HIPAA Safe Harbor

**Goal:** Create de-identified Dynamic Table covering all 18 HIPAA Safe Harbor identifiers.

Create `DICOM_STUDIES_DEIDENTIFIED` from `references/audit_deidentification_retention.sql`.

MANDATORY STOPPING POINT: Validate de-identification — query the Dynamic Table and confirm no PHI leaks.

### Step 8: Audit Trail & Monitoring

**Goal:** Monitor PHI access, detect anomalies, create compliance dashboards.

Create audit view and alert from `references/audit_deidentification_retention.sql`.

### Step 9: Data Retention & Lifecycle

**Goal:** Configure retention, time-travel, and archival for HIPAA-mandated periods (6 years).

Apply retention settings and create archival task from `references/audit_deidentification_retention.sql`.

MANDATORY STOPPING POINT: Review retention settings and archival schedule before enabling the task.

## Stopping Points

- After PHI Column Inventory: Confirm scope before proceeding
- After Step 1: Review classification results and coverage gaps
- After Step 2: Verify all PHI columns are tagged before masking
- After Step 3: Test masking with authorized and unauthorized roles
- After Step 5: Test RAPs with multiple roles before aggregation setup
- After Step 7: Validate de-identified Dynamic Table contains no PHI
- After Step 9: Review retention settings and archival schedule

## Output

- PHI column inventory covering all 18 HIPAA identifiers across imaging tables
- Object tags (PHI_TYPE, SENSITIVITY_LEVEL, HIPAA_SAFE_HARBOR) on all PHI columns
- Masking policies for STRING, DATE, NUMBER, and VARIANT data types
- VARIANT masking that redacts PHI DICOM tags while preserving clinical metadata
- Row access policies: institution-based, department-based, and consent-based
- Aggregation policy enforcing k-anonymity (min cell size = 5)
- De-identified Dynamic Table for research (HIPAA Safe Harbor compliant)
- PHI access audit view and anomalous access alerting
- Data retention registry and automated archival task
- Consent management registry for research data governance

## Next

Return to parent router for next workflow. Common follow-ups:
- ANALYTICS: Build analytics over de-identified data
- VIEWER: Role-aware dashboard respecting masking policies
- AGENT: Conversational access with governance-enforced data visibility
