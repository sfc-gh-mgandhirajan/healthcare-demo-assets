---
name: "medicare-prepayment-edits-poc"
created: "2026-03-17T16:43:38.298Z"
status: pending
---

# Plan: Medicare Part A Institutional Claims Pre-Payment Edit Simulation (NRT)

## Context

This POC simulates the CMS pre-payment edit process for Medicare Part A institutional claims (837I format). The architecture mirrors the Claims Adjudication POC diagram where data flows from external sources through an S3 bucket (Bronze Interface), gets processed through a Data Analytics Engine (Silver), and feeds into a Validation Engine (Gold) that produces adjudication decisions (Accept/Deny/Reject).

**NRT Requirement**: End-to-end latency from S3 landing to Gold adjudication must be **under 15 minutes**.

**DQ Requirement**: Data must pass SNIP-framework-aligned quality checks (DMFs) in the Silver layer **before** being passed to the Gold layer edit rules engine. A DQ dashboard provides visibility into data quality metrics.

**SNIP Alignment**: DMFs in Silver cover SNIP Types 1-6 (EDI/Syntax, Balancing/Situational, Code Set/Clinical). Payer-specific Medicare edits (MUE, NCCI, Frequency) live in the Gold layer as business rules.

## Architecture Overview

```
flowchart LR
  subgraph external [External - S3 Bucket]
    SyntheticGen["Synthetic Data Generator\n(Python Script)"]
    S3["S3 Stage\n(Bronze Interface)"]
  end

  subgraph snowflake [Snowflake - MEDICARE_CLAIMS_POC]
    subgraph bronze [BRONZE Schema]
      Snowpipe["Snowpipe (auto_ingest)\n< 1 min latency"]
      RawClaims["RAW_CLAIM_HEADER"]
      RawLines["RAW_SERVICE_LINES"]
      RawProvider["RAW_PROVIDER"]
      RawBeneficiary["RAW_BENEFICIARY"]
    end

    subgraph silver [SILVER Schema - Dynamic Tables]
      DTClaimHeader["DT: CLAIM_HEADER\nTARGET_LAG = DOWNSTREAM"]
      DTServiceLines["DT: SERVICE_LINES\nTARGET_LAG = DOWNSTREAM"]
      DTProvider["DT: PROVIDER\nTARGET_LAG = DOWNSTREAM"]
      DTBeneficiary["DT: BENEFICIARY\nTARGET_LAG = DOWNSTREAM"]
      DMF["Data Metric Functions\nTRIGGER_ON_CHANGES\n(16 built-in + 11 custom)\nSNIP Types 1-6"]
    end

    subgraph dq [DQ Schema - Quality Gate]
      DQDashboard["DQ Dashboard Views\n(SNOWFLAKE.LOCAL.\nDATA_QUALITY_MONITORING_RESULTS)"]
      DQScorecard["DQ.V_DQ_SCORECARD"]
      DQClaimGate["DQ.CLAIMS_QUALITY_GATE\n(Pass/Fail per claim)"]
    end

    subgraph config [CONFIG Schema - Reference Data]
      MUELimits["CONFIG.MUE_LIMITS\n(HCPCS → max units)"]
      NCCIPairs["CONFIG.NCCI_CODE_PAIRS\n(code1/code2 bundles)"]
      FreqLimits["CONFIG.FREQUENCY_LIMITS\n(HCPCS → max per period)"]
      EditRules["CONFIG.EDIT_RULES"]
    end

    subgraph gold [GOLD Schema - Edit Rules (only DQ-passed claims)]
      DTEditDuplicate["DT: EDIT_DUPLICATE_CHECK"]
      DTEditProvider["DT: EDIT_PROVIDER_ENROLLMENT"]
      DTEditEligibility["DT: EDIT_BENEFICIARY_ELIGIBILITY"]
      DTEditRevCode["DT: EDIT_REVENUE_CODE"]
      DTEditDateLogic["DT: EDIT_DATE_LOGIC"]
      DTEditDRG["DT: EDIT_DRG_CONSISTENCY"]
      DTEditNPI["DT: EDIT_NPI_VALIDATION"]
      DTEditTimely["DT: EDIT_TIMELY_FILING"]
      DTEditMUE["DT: EDIT_MUE_CHECK"]
      DTEditNCCI["DT: EDIT_NCCI_UNBUNDLING"]
      DTEditFreq["DT: EDIT_FREQUENCY_CHECK"]
      DTEditSummary["DT: EDIT_RESULTS_SUMMARY\nTARGET_LAG = DOWNSTREAM"]
      DTAdjudication["DT: CLAIM_ADJUDICATION\nTARGET_LAG = '5 MINUTES'"]
    end
  end

  SyntheticGen --> S3
  S3 -->|SQS event| Snowpipe
  Snowpipe --> RawClaims & RawLines & RawProvider & RawBeneficiary
  RawClaims --> DTClaimHeader
  RawLines --> DTServiceLines
  RawProvider --> DTProvider
  RawBeneficiary --> DTBeneficiary
  DTClaimHeader --> DMF
  DTServiceLines --> DMF
  DTProvider --> DMF
  DTBeneficiary --> DMF
  DMF --> DQDashboard & DQScorecard & DQClaimGate
  DQClaimGate --> DTEditDuplicate & DTEditProvider & DTEditEligibility & DTEditDateLogic & DTEditDRG & DTEditNPI & DTEditTimely & DTEditMUE & DTEditNCCI & DTEditFreq
  DTServiceLines --> DTEditRevCode & DTEditDRG & DTEditMUE & DTEditNCCI & DTEditFreq
  DTProvider --> DTEditProvider
  DTBeneficiary --> DTEditEligibility
  MUELimits --> DTEditMUE
  NCCIPairs --> DTEditNCCI
  FreqLimits --> DTEditFreq
  DTEditDuplicate & DTEditProvider & DTEditEligibility & DTEditRevCode & DTEditDateLogic & DTEditDRG & DTEditNPI & DTEditTimely & DTEditMUE & DTEditNCCI & DTEditFreq --> DTEditSummary
  DTEditSummary --> DTAdjudication
```

### NRT Latency Budget (Target: < 15 minutes)

| Stage                            | Mechanism                                 | Expected Latency         |
| -------------------------------- | ----------------------------------------- | ------------------------ |
| S3 → Bronze                      | Snowpipe (SQS auto\_ingest)               | 1-2 minutes              |
| Bronze → Silver                  | Dynamic Tables (TARGET\_LAG = DOWNSTREAM) | Cascading, \~1-2 minutes |
| Silver → Gold Edit Rules         | Dynamic Tables (TARGET\_LAG = DOWNSTREAM) | Cascading, \~1-2 minutes |
| Gold Edit Summary → Adjudication | Dynamic Table (TARGET\_LAG = '5 MINUTES') | ≤ 5 minutes              |
| **Total**                        |                                           | **\~5-11 minutes**       |

---

## Key Design Decisions

1. **Snowpipe replaces COPY INTO Task** as primary ingestion — sub-minute latency via SQS event notifications.

2. **TARGET\_LAG = 'DOWNSTREAM'** on all Silver and intermediate Gold DTs — single terminal DT (`CLAIM_ADJUDICATION`, 5 min) drives the entire cascade.

3. **All edit rules are pure-SQL Dynamic Tables** — no Snowpark SP in the hot path.

4. **SNIP 1-6 checks as DMFs in Silver** — structural/syntax/balancing/code-set validation happens before claims enter the Gold edit rules engine.

5. **Payer-specific edits (MUE/NCCI/Frequency) as Gold edit rules** — these are business logic, not data quality, and require reference lookup tables.

---

## Task 1: Snowflake Infrastructure Setup

### Database and Schemas

```
CREATE DATABASE IF NOT EXISTS MEDICARE_CLAIMS_POC;

CREATE SCHEMA IF NOT EXISTS MEDICARE_CLAIMS_POC.BRONZE;
CREATE SCHEMA IF NOT EXISTS MEDICARE_CLAIMS_POC.SILVER;
CREATE SCHEMA IF NOT EXISTS MEDICARE_CLAIMS_POC.GOLD;
CREATE SCHEMA IF NOT EXISTS MEDICARE_CLAIMS_POC.DQ;
CREATE SCHEMA IF NOT EXISTS MEDICARE_CLAIMS_POC.CONFIG;
```

### External Stage to S3

```
CREATE OR REPLACE STAGE MEDICARE_CLAIMS_POC.BRONZE.S3_CLAIMS_STAGE
  URL = 's3://<bucket>/medicare-claims-poc/'
  STORAGE_INTEGRATION = <integration_name>
  FILE_FORMAT = (TYPE = 'JSON');
```

### Warehouses

- `CLAIMS_INGEST_WH` (X-Small) — Snowpipe uses its own compute, so this is for manual catch-up only
- `CLAIMS_TRANSFORM_WH` (Small) — Dynamic Tables (Silver + Gold) and Snowpark
- `CLAIMS_ANALYTICS_WH` (Small) — Analyst queries on Gold views and DQ dashboard

### RBAC Roles

- `CLAIMS_ADMIN` — full ownership of all POC objects
- `CLAIMS_ENGINEER` — read/write on BRONZE/SILVER, execute on GOLD procedures
- `CLAIMS_ANALYST` — read-only on SILVER/GOLD/DQ (includes DQ dashboard access)
- Future grants on all schemas for automatic permission propagation

---

## Task 2: Synthetic Medicare Part A Claims Data Generation

Generate realistic synthetic data using a Python script that writes JSON files to S3.

### Claim Header (RAW\_CLAIM\_HEADER)

| Field                       | Description                           | Example                    | SNIP Relevance                  |
| --------------------------- | ------------------------------------- | -------------------------- | ------------------------------- |
| CLAIM\_ID                   | Unique claim identifier               | CLM-2026-000001            | —                               |
| DCN                         | Document Control Number               | 1234567890123              | —                               |
| BENEFICIARY\_HIC            | Medicare Beneficiary Identifier (MBI) | 1EG4-TE5-MK72              | SNIP 1-2: MBI format            |
| PROVIDER\_NPI               | Billing provider NPI (10-digit)       | 1234567890                 | SNIP 1-2: NPI format            |
| FACILITY\_TYPE\_CODE        | Type of bill - facility               | 01 (Hospital)              | —                               |
| CLAIM\_FREQUENCY\_CODE      | Original/replacement/void             | 1                          | —                               |
| ADMISSION\_DATE             | Date of admission                     | 2026-01-15                 | SNIP 1-2: Date format           |
| DISCHARGE\_DATE             | Date of discharge                     | 2026-01-20                 | SNIP 1-2: Date format           |
| PATIENT\_STATUS\_CODE       | Discharge status                      | 01                         | —                               |
| DRG\_CODE                   | Diagnosis Related Group               | 470                        | SNIP 5-6: Code validity         |
| PRINCIPAL\_DIAGNOSIS\_CODE  | ICD-10 primary                        | J18.9                      | SNIP 5-6: ICD-10 format         |
| TOTAL\_CHARGES              | Total billed amount                   | 45230.00                   | SNIP 3-4: Financial balance     |
| CLAIM\_SUBMISSION\_DATE     | Date submitted to MAC                 | 2026-02-01                 | —                               |
| TYPE\_OF\_BILL              | 3-digit type of bill code             | 111                        | SNIP 1-2: Format                |
| **ADMISSION\_SOURCE\_CODE** | **Admission source (new)**            | **1 (physician referral)** | **SNIP 3-4: Conditional field** |
| **PATIENT\_SEX**            | **M/F/U (new)**                       | **F**                      | **SNIP 5-6: MCE age/sex check** |

### Service Lines (RAW\_SERVICE\_LINES)

| Field         | Description                                    |
| ------------- | ---------------------------------------------- |
| CLAIM\_ID     | FK to claim header                             |
| LINE\_NUMBER  | Sequence number                                |
| REVENUE\_CODE | 4-digit revenue center code (e.g., 0120, 0250) |
| HCPCS\_CODE   | Procedure code                                 |
| SERVICE\_DATE | Date of service                                |
| UNITS         | Units of service                               |
| LINE\_CHARGES | Charge amount                                  |

### Provider Reference (RAW\_PROVIDER)

| Field                     | Description               |
| ------------------------- | ------------------------- |
| PROVIDER\_NPI             | 10-digit NPI              |
| PROVIDER\_NAME            | Facility name             |
| PROVIDER\_TYPE            | Hospital/SNF/HHA          |
| PECOS\_ENROLLMENT\_STATUS | Active/Inactive/Revoked   |
| PECOS\_EFFECTIVE\_DATE    | Enrollment effective date |
| MAC\_JURISDICTION         | NGS/Noridian/etc.         |
| CCN                       | CMS Certification Number  |

### Beneficiary Reference (RAW\_BENEFICIARY)

| Field                      | Description                          |
| -------------------------- | ------------------------------------ |
| BENEFICIARY\_HIC           | MBI number                           |
| MEDICARE\_PART\_A\_STATUS  | Active/Terminated                    |
| PART\_A\_EFFECTIVE\_DATE   | Coverage start                       |
| PART\_A\_TERMINATION\_DATE | Coverage end (null if active)        |
| DATE\_OF\_BIRTH            | For age validation                   |
| **PATIENT\_SEX**           | **M/F (new — for MCE age/sex edit)** |
| MSP\_INDICATOR             | Medicare Secondary Payer flag        |

### Reference/Config Tables (new for payer-specific edits)

**CONFIG.MUE\_LIMITS** — Medically Unlikely Edit thresholds

| Field                | Description             | Example    |
| -------------------- | ----------------------- | ---------- |
| HCPCS\_CODE          | Procedure code          | 99213      |
| MAX\_UNITS\_PER\_DAY | MUE unit limit          | 1          |
| EFFECTIVE\_DATE      | Start date              | 2025-01-01 |
| TERMINATION\_DATE    | End date (null=current) | NULL       |

**CONFIG.NCCI\_CODE\_PAIRS** — National Correct Coding Initiative bundling pairs

| Field               | Description                                | Example    |
| ------------------- | ------------------------------------------ | ---------- |
| COLUMN\_1\_CODE     | Comprehensive code                         | 99214      |
| COLUMN\_2\_CODE     | Component code (cannot bill with column 1) | 99211      |
| MODIFIER\_INDICATOR | 0=never, 1=modifier allowed                | 0          |
| EFFECTIVE\_DATE     | Start date                                 | 2025-01-01 |

**CONFIG.FREQUENCY\_LIMITS** — Service frequency limits

| Field                   | Description             | Example    |
| ----------------------- | ----------------------- | ---------- |
| HCPCS\_CODE             | Procedure code          | 77067      |
| MAX\_FREQUENCY          | Max allowed occurrences | 1          |
| FREQUENCY\_PERIOD\_DAYS | Rolling period in days  | 365        |
| EFFECTIVE\_DATE         | Start date              | 2025-01-01 |

Data volumes: \~50,000 claim headers, \~200,000 service lines, 500 providers, 30,000 beneficiaries, \~200 MUE limits, \~500 NCCI pairs, \~100 frequency limits. Seed \~15% of claims with errors spanning all SNIP categories + payer edits.

Output to S3 as partitioned JSON files (batches of \~5,000 claims spaced 1-2 minutes apart).

---

## Task 3: Bronze Layer — Snowpipe Ingestion (NRT)

### Raw Landing Tables

```
CREATE OR REPLACE TABLE BRONZE.RAW_CLAIM_HEADER (
    RAW            VARIANT,
    _LOAD_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE   STRING DEFAULT METADATA$FILENAME
);
```

Similarly for RAW\_SERVICE\_LINES, RAW\_PROVIDER, RAW\_BENEFICIARY.

### Snowpipe (Primary — NRT)

```
CREATE OR REPLACE PIPE BRONZE.CLAIM_HEADER_PIPE
  AUTO_INGEST = TRUE
AS
  COPY INTO BRONZE.RAW_CLAIM_HEADER (RAW, _LOAD_TIMESTAMP, _SOURCE_FILE)
  FROM (
    SELECT $1, CURRENT_TIMESTAMP(), METADATA$FILENAME
    FROM @BRONZE.S3_CLAIMS_STAGE/claim_headers/
  )
  FILE_FORMAT = (TYPE = 'JSON');
```

Similarly for service\_lines, provider, beneficiary pipes. **SQS notification ARN** from `SHOW PIPES` must be configured on the S3 bucket.

### Manual Catch-Up Task (suspended cold standby)

```
CREATE OR REPLACE TASK BRONZE.CATCHUP_INGEST_TASK
  WAREHOUSE = CLAIMS_INGEST_WH
  SCHEDULE = '5 MINUTE'
  SUSPEND_TASK_AFTER_NUM_FAILURES = 3
  WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.CLAIM_HEADER_CATCHUP_STREAM')
AS
  COPY INTO BRONZE.RAW_CLAIM_HEADER
  FROM @BRONZE.S3_CLAIMS_STAGE/claim_headers/
  FILE_FORMAT = (TYPE = 'JSON')
  ON_ERROR = 'CONTINUE';
```

---

## Task 4: Silver Layer — Dynamic Tables + SNIP-Aligned DMFs (27 Total)

### Dynamic Tables with TARGET\_LAG = 'DOWNSTREAM'

```
CREATE OR REPLACE DYNAMIC TABLE SILVER.CLAIM_HEADER
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = CLAIMS_TRANSFORM_WH
AS
SELECT
  raw:CLAIM_ID::STRING              AS claim_id,
  raw:DCN::STRING                   AS dcn,
  raw:BENEFICIARY_HIC::STRING       AS beneficiary_hic,
  raw:PROVIDER_NPI::STRING          AS provider_npi,
  raw:FACILITY_TYPE_CODE::STRING    AS facility_type_code,
  raw:CLAIM_FREQUENCY_CODE::STRING  AS claim_frequency_code,
  raw:ADMISSION_DATE::DATE          AS admission_date,
  raw:DISCHARGE_DATE::DATE          AS discharge_date,
  raw:PATIENT_STATUS_CODE::STRING   AS patient_status_code,
  raw:DRG_CODE::STRING              AS drg_code,
  raw:PRINCIPAL_DIAGNOSIS_CODE::STRING AS principal_diagnosis_code,
  raw:TOTAL_CHARGES::NUMBER(12,2)   AS total_charges,
  raw:TYPE_OF_BILL::STRING          AS type_of_bill,
  raw:CLAIM_SUBMISSION_DATE::DATE   AS claim_submission_date,
  raw:ADMISSION_SOURCE_CODE::STRING AS admission_source_code,
  raw:PATIENT_SEX::STRING           AS patient_sex,
  _LOAD_TIMESTAMP,
  _SOURCE_FILE
FROM BRONZE.RAW_CLAIM_HEADER;
```

Similarly for SILVER.SERVICE\_LINES (unchanged), SILVER.PROVIDER (unchanged), SILVER.BENEFICIARY (add `patient_sex`).

---

### Data Metric Functions (DMFs) — SNIP Framework Alignment

DMFs fire on `TRIGGER_ON_CHANGES` and results flow to `SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS`.

```
ALTER TABLE SILVER.CLAIM_HEADER SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
ALTER TABLE SILVER.SERVICE_LINES SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
ALTER TABLE SILVER.PROVIDER SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
ALTER TABLE SILVER.BENEFICIARY SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
```

---

#### SNIP Type 1-2: EDI & Syntax Validation (DMFs #1-14)

These DMFs validate that fields are present, properly formatted, and follow CMS-mandated patterns.

**A. SILVER.CLAIM\_HEADER — Built-in DMFs**

| # | DMF                              | Column(s)         | SNIP Check      | What It Measures                |
| - | -------------------------------- | ----------------- | --------------- | ------------------------------- |
| 1 | `SNOWFLAKE.CORE.NULL_COUNT`      | `claim_id`        | Syntax          | NULL primary keys               |
| 2 | `SNOWFLAKE.CORE.NULL_COUNT`      | `provider_npi`    | Syntax          | Missing NPI                     |
| 3 | `SNOWFLAKE.CORE.NULL_COUNT`      | `beneficiary_hic` | Syntax          | Missing MBI                     |
| 4 | `SNOWFLAKE.CORE.NULL_COUNT`      | `admission_date`  | Syntax          | Missing admission date          |
| 5 | `SNOWFLAKE.CORE.DUPLICATE_COUNT` | `claim_id`        | Syntax          | Duplicate claim IDs at Silver   |
| 6 | `SNOWFLAKE.CORE.BLANK_COUNT`     | `drg_code`        | Syntax          | Empty DRG                       |
| 7 | `SNOWFLAKE.CORE.FRESHNESS`       | `_LOAD_TIMESTAMP` | Pipeline health | Data staleness                  |
| 8 | `SNOWFLAKE.CORE.ROW_COUNT`       | `claim_id`        | Volume          | Row count for anomaly detection |

```
ALTER TABLE SILVER.CLAIM_HEADER ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (claim_id);
ALTER TABLE SILVER.CLAIM_HEADER ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (provider_npi);
ALTER TABLE SILVER.CLAIM_HEADER ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (beneficiary_hic);
ALTER TABLE SILVER.CLAIM_HEADER ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (admission_date);
ALTER TABLE SILVER.CLAIM_HEADER ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (claim_id);
ALTER TABLE SILVER.CLAIM_HEADER ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.BLANK_COUNT ON (drg_code);
ALTER TABLE SILVER.CLAIM_HEADER ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON (_LOAD_TIMESTAMP);
ALTER TABLE SILVER.CLAIM_HEADER ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON (claim_id);
```

**B. SILVER.CLAIM\_HEADER — Custom DMFs (SNIP 1-2: Format Validation)**

| #  | DMF Name                       | Column(s)                          | SNIP Check                     | What It Measures                                                                          |
| -- | ------------------------------ | ---------------------------------- | ------------------------------ | ----------------------------------------------------------------------------------------- |
| 9  | `DQ.CHECK_NPI_FORMAT`          | `provider_npi`                     | SNIP 1-2: Provider Credentials | NPIs not exactly 10 digits                                                                |
| 10 | `DQ.CHECK_MBI_FORMAT`          | `beneficiary_hic`                  | SNIP 1-2: Demographic Accuracy | MBIs not matching 11-char CMS pattern (e.g., `[1-9][AC-HJ-NP-RT-Y][AC-HJ-NP-RT-Y0-9]...`) |
| 11 | `DQ.CHECK_TYPE_OF_BILL_FORMAT` | `type_of_bill`                     | SNIP 1-2: Field Formatting     | TOBs not matching 3-digit pattern                                                         |
| 12 | `DQ.CHECK_DATE_FORMAT_VALID`   | `admission_date`, `discharge_date` | SNIP 1-2: Field Formatting     | Dates that are NULL or future-dated beyond claim submission                               |

```
-- DMF #9: NPI must be exactly 10 digits
CREATE OR REPLACE DATA METRIC FUNCTION DQ.CHECK_NPI_FORMAT(
  ARG_T TABLE(provider_npi STRING)
)
RETURNS NUMBER AS
'SELECT COUNT(*) FROM ARG_T WHERE NOT REGEXP_LIKE(provider_npi, ''^[0-9]{10}$'')';

ALTER TABLE SILVER.CLAIM_HEADER ADD DATA METRIC FUNCTION DQ.CHECK_NPI_FORMAT ON (provider_npi);

-- DMF #10: MBI must follow CMS 11-character format
-- Pattern: Position 1=numeric(1-9), Position 2=alpha(non-SLOIBZ), etc.
-- Simplified: 11 alphanumeric characters, specific positions alpha vs numeric
CREATE OR REPLACE DATA METRIC FUNCTION DQ.CHECK_MBI_FORMAT(
  ARG_T TABLE(beneficiary_hic STRING)
)
RETURNS NUMBER AS
'SELECT COUNT(*) FROM ARG_T WHERE NOT REGEXP_LIKE(beneficiary_hic, ''^[1-9][A-HJ-NP-RT-Y][A-HJ-NP-RT-Y0-9][0-9][A-HJ-NP-RT-Y][A-HJ-NP-RT-Y0-9][0-9][A-HJ-NP-RT-Y]{2}[0-9]{2}$'')';

ALTER TABLE SILVER.CLAIM_HEADER ADD DATA METRIC FUNCTION DQ.CHECK_MBI_FORMAT ON (beneficiary_hic);

-- DMF #11: Type of Bill must be 3-digit numeric
CREATE OR REPLACE DATA METRIC FUNCTION DQ.CHECK_TYPE_OF_BILL_FORMAT(
  ARG_T TABLE(type_of_bill STRING)
)
RETURNS NUMBER AS
'SELECT COUNT(*) FROM ARG_T WHERE NOT REGEXP_LIKE(type_of_bill, ''^[0-9]{3}$'')';

ALTER TABLE SILVER.CLAIM_HEADER ADD DATA METRIC FUNCTION DQ.CHECK_TYPE_OF_BILL_FORMAT ON (type_of_bill);

-- DMF #12: Dates must not be future-dated beyond reasonable window
CREATE OR REPLACE DATA METRIC FUNCTION DQ.CHECK_DATE_FORMAT_VALID(
  ARG_T TABLE(admission_date DATE, discharge_date DATE)
)
RETURNS NUMBER AS
'SELECT COUNT(*) FROM ARG_T WHERE admission_date > CURRENT_DATE() OR discharge_date > DATEADD(day, 1, CURRENT_DATE())';

ALTER TABLE SILVER.CLAIM_HEADER ADD DATA METRIC FUNCTION DQ.CHECK_DATE_FORMAT_VALID ON (admission_date, discharge_date);
```

---

#### SNIP Type 3-4: Balancing & Situational Logic (DMFs #13-16)

These DMFs verify inter-field and cross-table logical consistency.

| #  | DMF Name                              | Column(s) / Tables                          | SNIP Check                    | What It Measures                                                 |
| -- | ------------------------------------- | ------------------------------------------- | ----------------------------- | ---------------------------------------------------------------- |
| 13 | `DQ.CHECK_DATE_CONSISTENCY`           | `admission_date`, `discharge_date`          | SNIP 3-4: Inter-segment       | Discharge date before admission date                             |
| 14 | `DQ.CHECK_TOTAL_CHARGES_RANGE`        | `total_charges`                             | SNIP 3-4: Financial           | Charges ≤ 0 or > $10M                                            |
| 15 | `DQ.CHECK_FINANCIAL_BALANCE`          | Multi-table: SERVICE\_LINES → CLAIM\_HEADER | SNIP 3-4: Financial Balancing | Claims where SUM(line\_charges) ≠ total\_charges                 |
| 16 | `DQ.CHECK_INPATIENT_ADMISSION_SOURCE` | `type_of_bill`, `admission_source_code`     | SNIP 3-4: Situational Logic   | Inpatient claims (TOB starts with '11') missing admission source |

```
-- DMF #13: Discharge must not precede admission
CREATE OR REPLACE DATA METRIC FUNCTION DQ.CHECK_DATE_CONSISTENCY(
  ARG_T TABLE(admission_date DATE, discharge_date DATE)
)
RETURNS NUMBER AS
'SELECT COUNT(*) FROM ARG_T WHERE discharge_date < admission_date';

ALTER TABLE SILVER.CLAIM_HEADER ADD DATA METRIC FUNCTION DQ.CHECK_DATE_CONSISTENCY ON (admission_date, discharge_date);

-- DMF #14: Charges must be positive and reasonable
CREATE OR REPLACE DATA METRIC FUNCTION DQ.CHECK_TOTAL_CHARGES_RANGE(
  ARG_T TABLE(total_charges NUMBER)
)
RETURNS NUMBER AS
'SELECT COUNT(*) FROM ARG_T WHERE total_charges <= 0 OR total_charges > 10000000';

ALTER TABLE SILVER.CLAIM_HEADER ADD DATA METRIC FUNCTION DQ.CHECK_TOTAL_CHARGES_RANGE ON (total_charges);

-- DMF #15: Sum of line charges must equal claim total charges
-- Multi-table DMF: ARG_T1 = CLAIM_HEADER, ARG_T2 = SERVICE_LINES (aggregated)
CREATE OR REPLACE DATA METRIC FUNCTION DQ.CHECK_FINANCIAL_BALANCE(
  ARG_T1 TABLE(claim_id STRING, total_charges NUMBER),
  ARG_T2 TABLE(claim_id STRING, line_charges NUMBER)
)
RETURNS NUMBER AS
'SELECT COUNT(*) FROM (
  SELECT h.claim_id
  FROM ARG_T1 h
  LEFT JOIN (SELECT claim_id, SUM(line_charges) AS sum_line_charges FROM ARG_T2 GROUP BY claim_id) sl
    ON h.claim_id = sl.claim_id
  WHERE ABS(h.total_charges - COALESCE(sl.sum_line_charges, 0)) > 0.01
)';

ALTER TABLE SILVER.CLAIM_HEADER ADD DATA METRIC FUNCTION DQ.CHECK_FINANCIAL_BALANCE
  ON (claim_id, total_charges, TABLE(MEDICARE_CLAIMS_POC.SILVER.SERVICE_LINES(claim_id, line_charges)));

-- DMF #16: Inpatient TOB (11x) must have admission source code
CREATE OR REPLACE DATA METRIC FUNCTION DQ.CHECK_INPATIENT_ADMISSION_SOURCE(
  ARG_T TABLE(type_of_bill STRING, admission_source_code STRING)
)
RETURNS NUMBER AS
'SELECT COUNT(*) FROM ARG_T WHERE LEFT(type_of_bill, 2) = ''11'' AND (admission_source_code IS NULL OR admission_source_code = '''')';

ALTER TABLE SILVER.CLAIM_HEADER ADD DATA METRIC FUNCTION DQ.CHECK_INPATIENT_ADMISSION_SOURCE ON (type_of_bill, admission_source_code);
```

---

#### SNIP Type 5-6: Code Set & Clinical Integrity (DMFs #17-20)

These DMFs validate that medical codes are properly formatted and clinically reasonable.

| #  | DMF Name                       | Column(s)                                | SNIP Check                 | What It Measures                                                          |
| -- | ------------------------------ | ---------------------------------------- | -------------------------- | ------------------------------------------------------------------------- |
| 17 | `DQ.CHECK_ICD10_FORMAT`        | `principal_diagnosis_code`               | SNIP 5-6: HIPAA Code Sets  | ICD-10-CM codes not matching valid format (3-7 chars, alpha start)        |
| 18 | `DQ.CHECK_REVENUE_CODE_FORMAT` | `revenue_code` (SERVICE\_LINES)          | SNIP 5-6: HIPAA Code Sets  | Revenue codes not 4-digit numeric                                         |
| 19 | `DQ.CHECK_HCPCS_FORMAT`        | `hcpcs_code` (SERVICE\_LINES)            | SNIP 5-6: Service Category | HCPCS codes not matching valid format (5 chars, alpha start for Level II) |
| 20 | `DQ.CHECK_AGE_SEX_CONSISTENCY` | Multi-table: CLAIM\_HEADER + BENEFICIARY | SNIP 5-6: MCE Age/Sex      | Claims where diagnosis is clinically inconsistent with patient age or sex |

```
-- DMF #17: ICD-10-CM format: starts with alpha, 3-7 characters
CREATE OR REPLACE DATA METRIC FUNCTION DQ.CHECK_ICD10_FORMAT(
  ARG_T TABLE(principal_diagnosis_code STRING)
)
RETURNS NUMBER AS
'SELECT COUNT(*) FROM ARG_T WHERE NOT REGEXP_LIKE(principal_diagnosis_code, ''^[A-Z][0-9]{2}(\.[0-9A-Z]{0,4})?$'')';

ALTER TABLE SILVER.CLAIM_HEADER ADD DATA METRIC FUNCTION DQ.CHECK_ICD10_FORMAT ON (principal_diagnosis_code);

-- DMF #18: Revenue code format (4-digit numeric 0100-0999)
CREATE OR REPLACE DATA METRIC FUNCTION DQ.CHECK_REVENUE_CODE_FORMAT(
  ARG_T TABLE(revenue_code STRING)
)
RETURNS NUMBER AS
'SELECT COUNT(*) FROM ARG_T WHERE NOT REGEXP_LIKE(revenue_code, ''^[0-9]{4}$'')';

ALTER TABLE SILVER.SERVICE_LINES ADD DATA METRIC FUNCTION DQ.CHECK_REVENUE_CODE_FORMAT ON (revenue_code);

-- DMF #19: HCPCS format: 5 characters (Level I = 5 digits; Level II = alpha + 4 digits)
CREATE OR REPLACE DATA METRIC FUNCTION DQ.CHECK_HCPCS_FORMAT(
  ARG_T TABLE(hcpcs_code STRING)
)
RETURNS NUMBER AS
'SELECT COUNT(*) FROM ARG_T WHERE hcpcs_code IS NOT NULL AND NOT REGEXP_LIKE(hcpcs_code, ''^[A-Z0-9][0-9]{4}$'')';

ALTER TABLE SILVER.SERVICE_LINES ADD DATA METRIC FUNCTION DQ.CHECK_HCPCS_FORMAT ON (hcpcs_code);

-- DMF #20: MCE Age/Sex — simplified: maternity DRGs (765-768, 774-775) require female sex
-- and pediatric DRGs (789-795) require age < 18
-- Full MCE requires extensive lookup; this is a representative subset
CREATE OR REPLACE DATA METRIC FUNCTION DQ.CHECK_AGE_SEX_CONSISTENCY(
  ARG_T1 TABLE(claim_id STRING, drg_code STRING, patient_sex STRING),
  ARG_T2 TABLE(beneficiary_hic STRING, date_of_birth DATE, patient_sex STRING)
)
RETURNS NUMBER AS
'SELECT COUNT(*) FROM (
  SELECT h.claim_id
  FROM ARG_T1 h
  LEFT JOIN ARG_T2 b ON h.claim_id IS NOT NULL
  WHERE
    (h.drg_code IN (''765'',''766'',''767'',''768'',''774'',''775'') AND COALESCE(h.patient_sex, b.patient_sex) != ''F'')
    OR
    (h.drg_code IN (''789'',''790'',''791'',''793'',''794'',''795'')
     AND DATEDIFF(''year'', b.date_of_birth, CURRENT_DATE()) >= 18)
)';

-- Note: MCE age/sex check attaches to CLAIM_HEADER with BENEFICIARY as second table arg
ALTER TABLE SILVER.CLAIM_HEADER ADD DATA METRIC FUNCTION DQ.CHECK_AGE_SEX_CONSISTENCY
  ON (claim_id, drg_code, patient_sex, TABLE(MEDICARE_CLAIMS_POC.SILVER.BENEFICIARY(beneficiary_hic, date_of_birth, patient_sex)));
```

---

#### Referential Integrity & Completeness (DMFs #21-27)

| #  | DMF                                     | Table                          | Column(s)         | What It Measures                     |
| -- | --------------------------------------- | ------------------------------ | ----------------- | ------------------------------------ |
| 21 | `SNOWFLAKE.CORE.NULL_COUNT`             | SERVICE\_LINES                 | `claim_id`        | Orphaned lines (null FK)             |
| 22 | `SNOWFLAKE.CORE.NULL_COUNT`             | SERVICE\_LINES                 | `revenue_code`    | Missing revenue code                 |
| 23 | `DQ.CHECK_REFERENTIAL_INTEGRITY_CLAIMS` | SERVICE\_LINES → CLAIM\_HEADER | `claim_id`        | Service lines with no matching claim |
| 24 | `SNOWFLAKE.CORE.NULL_COUNT`             | PROVIDER                       | `provider_npi`    | NULL provider PK                     |
| 25 | `SNOWFLAKE.CORE.DUPLICATE_COUNT`        | PROVIDER                       | `provider_npi`    | Duplicate providers                  |
| 26 | `SNOWFLAKE.CORE.NULL_COUNT`             | BENEFICIARY                    | `beneficiary_hic` | NULL beneficiary PK                  |
| 27 | `SNOWFLAKE.CORE.DUPLICATE_COUNT`        | BENEFICIARY                    | `beneficiary_hic` | Duplicate beneficiaries              |

```
-- SERVICE_LINES
ALTER TABLE SILVER.SERVICE_LINES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (claim_id);
ALTER TABLE SILVER.SERVICE_LINES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (revenue_code);

CREATE OR REPLACE DATA METRIC FUNCTION DQ.CHECK_REFERENTIAL_INTEGRITY_CLAIMS(
  ARG_T1 TABLE(claim_id STRING),
  ARG_T2 TABLE(claim_id STRING)
)
RETURNS NUMBER AS
'SELECT COUNT(*) FROM ARG_T1 WHERE claim_id NOT IN (SELECT claim_id FROM ARG_T2)';

ALTER TABLE SILVER.SERVICE_LINES ADD DATA METRIC FUNCTION DQ.CHECK_REFERENTIAL_INTEGRITY_CLAIMS
  ON (claim_id, TABLE(MEDICARE_CLAIMS_POC.SILVER.CLAIM_HEADER(claim_id)));

-- PROVIDER
ALTER TABLE SILVER.PROVIDER ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (provider_npi);
ALTER TABLE SILVER.PROVIDER ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (provider_npi);

-- BENEFICIARY
ALTER TABLE SILVER.BENEFICIARY ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (beneficiary_hic);
ALTER TABLE SILVER.BENEFICIARY ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (beneficiary_hic);
```

---

### DMF Summary — SNIP Alignment

| SNIP Category                         | DMFs                                                                                                                   | Built-in | Custom | Total       |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- | -------- | ------ | ----------- |
| **SNIP 1-2: EDI & Syntax**            | NULL\_COUNT ×4, DUPLICATE\_COUNT, BLANK\_COUNT, FRESHNESS, ROW\_COUNT, NPI format, MBI format, TOB format, Date format | 8        | 4      | **12**      |
| **SNIP 3-4: Balancing & Situational** | Date consistency, Charges range, Financial balance (multi-table), Inpatient admission source                           | 0        | 4      | **4**       |
| **SNIP 5-6: Code Set & Clinical**     | ICD-10 format, Revenue code format, HCPCS format, MCE Age/Sex (multi-table)                                            | 0        | 4      | **4**       |
| **Referential Integrity**             | NULL\_COUNT ×3, DUPLICATE\_COUNT ×2, Referential integrity (multi-table)                                               | 5        | 1      | **6**       |
| **Pipeline Health**                   | FRESHNESS                                                                                                              | 1        | 0      | **1**       |
| **Total**                             |                                                                                                                        | **14**   | **13** | **27 DMFs** |

---

### DQ Gate — SNIP-Aligned Per-Claim Quality Filter

The DQ gate now includes all SNIP 1-6 checks:

```
CREATE OR REPLACE DYNAMIC TABLE DQ.CLAIMS_QUALITY_GATE
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = CLAIMS_TRANSFORM_WH
AS
SELECT
  ch.claim_id,
  ch.provider_npi,
  ch.beneficiary_hic,
  ch.admission_date,
  ch.discharge_date,
  ch.total_charges,
  ch.type_of_bill,
  ch.admission_source_code,
  ch.patient_sex,
  ch.principal_diagnosis_code,

  -- SNIP 1-2: EDI & Syntax
  CASE WHEN ch.claim_id IS NULL THEN 1 ELSE 0 END AS dq_null_claim_id,
  CASE WHEN ch.provider_npi IS NULL OR NOT REGEXP_LIKE(ch.provider_npi, '^[0-9]{10}$') THEN 1 ELSE 0 END AS dq_invalid_npi,
  CASE WHEN ch.beneficiary_hic IS NULL OR NOT REGEXP_LIKE(ch.beneficiary_hic, '^[1-9][A-HJ-NP-RT-Y][A-HJ-NP-RT-Y0-9][0-9][A-HJ-NP-RT-Y][A-HJ-NP-RT-Y0-9][0-9][A-HJ-NP-RT-Y]{2}[0-9]{2}$') THEN 1 ELSE 0 END AS dq_invalid_mbi,
  CASE WHEN NOT REGEXP_LIKE(COALESCE(ch.type_of_bill, ''), '^[0-9]{3}$') THEN 1 ELSE 0 END AS dq_invalid_type_of_bill,
  CASE WHEN ch.admission_date IS NULL OR ch.admission_date > CURRENT_DATE() THEN 1 ELSE 0 END AS dq_invalid_admission_date,

  -- SNIP 3-4: Balancing & Situational
  CASE WHEN ch.discharge_date < ch.admission_date THEN 1 ELSE 0 END AS dq_date_inconsistency,
  CASE WHEN ch.total_charges <= 0 OR ch.total_charges > 10000000 THEN 1 ELSE 0 END AS dq_charges_out_of_range,
  CASE WHEN LEFT(ch.type_of_bill, 2) = '11' AND (ch.admission_source_code IS NULL OR ch.admission_source_code = '') THEN 1 ELSE 0 END AS dq_missing_admission_source,

  -- SNIP 5-6: Code Set & Clinical
  CASE WHEN NOT REGEXP_LIKE(COALESCE(ch.principal_diagnosis_code, ''), '^[A-Z][0-9]{2}(\\.[0-9A-Z]{0,4})?$') THEN 1 ELSE 0 END AS dq_invalid_icd10,

  -- Overall DQ verdict
  CASE
    WHEN ch.claim_id IS NULL
      OR NOT REGEXP_LIKE(COALESCE(ch.provider_npi, ''), '^[0-9]{10}$')
      OR NOT REGEXP_LIKE(COALESCE(ch.beneficiary_hic, ''), '^[1-9][A-HJ-NP-RT-Y][A-HJ-NP-RT-Y0-9][0-9][A-HJ-NP-RT-Y][A-HJ-NP-RT-Y0-9][0-9][A-HJ-NP-RT-Y]{2}[0-9]{2}$')
      OR NOT REGEXP_LIKE(COALESCE(ch.type_of_bill, ''), '^[0-9]{3}$')
      OR ch.admission_date IS NULL OR ch.admission_date > CURRENT_DATE()
      OR ch.discharge_date < ch.admission_date
      OR ch.total_charges <= 0 OR ch.total_charges > 10000000
      OR (LEFT(ch.type_of_bill, 2) = '11' AND (ch.admission_source_code IS NULL OR ch.admission_source_code = ''))
      OR NOT REGEXP_LIKE(COALESCE(ch.principal_diagnosis_code, ''), '^[A-Z][0-9]{2}(\\.[0-9A-Z]{0,4})?$')
    THEN 'DQ_FAIL'
    ELSE 'DQ_PASS'
  END AS dq_status,

  -- Issue count
  (CASE WHEN ch.claim_id IS NULL THEN 1 ELSE 0 END
   + CASE WHEN NOT REGEXP_LIKE(COALESCE(ch.provider_npi, ''), '^[0-9]{10}$') THEN 1 ELSE 0 END
   + CASE WHEN NOT REGEXP_LIKE(COALESCE(ch.beneficiary_hic, ''), '^[1-9][A-HJ-NP-RT-Y][A-HJ-NP-RT-Y0-9][0-9][A-HJ-NP-RT-Y][A-HJ-NP-RT-Y0-9][0-9][A-HJ-NP-RT-Y]{2}[0-9]{2}$') THEN 1 ELSE 0 END
   + CASE WHEN NOT REGEXP_LIKE(COALESCE(ch.type_of_bill, ''), '^[0-9]{3}$') THEN 1 ELSE 0 END
   + CASE WHEN ch.admission_date IS NULL OR ch.admission_date > CURRENT_DATE() THEN 1 ELSE 0 END
   + CASE WHEN ch.discharge_date < ch.admission_date THEN 1 ELSE 0 END
   + CASE WHEN ch.total_charges <= 0 OR ch.total_charges > 10000000 THEN 1 ELSE 0 END
   + CASE WHEN LEFT(ch.type_of_bill, 2) = '11' AND (ch.admission_source_code IS NULL OR ch.admission_source_code = '') THEN 1 ELSE 0 END
   + CASE WHEN NOT REGEXP_LIKE(COALESCE(ch.principal_diagnosis_code, ''), '^[A-Z][0-9]{2}(\\.[0-9A-Z]{0,4})?$') THEN 1 ELSE 0 END
  ) AS dq_issue_count,

  -- SNIP category of worst failure
  CASE
    WHEN ch.claim_id IS NULL OR NOT REGEXP_LIKE(COALESCE(ch.provider_npi, ''), '^[0-9]{10}$') OR NOT REGEXP_LIKE(COALESCE(ch.beneficiary_hic, ''), '^.{11}$') THEN 'SNIP_1_2'
    WHEN ch.discharge_date < ch.admission_date OR ch.total_charges <= 0 THEN 'SNIP_3_4'
    WHEN NOT REGEXP_LIKE(COALESCE(ch.principal_diagnosis_code, ''), '^[A-Z]') THEN 'SNIP_5_6'
    ELSE NULL
  END AS dq_fail_snip_category

FROM SILVER.CLAIM_HEADER ch;
```

---

### DQ Dashboard — Views Powered by DMF Results

5 views in the `DQ` schema querying `SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS`:

1. **`DQ.V_DQ_SCORECARD`** — Per-table, per-metric pass/fail with SNIP category and DQ dimension (completeness, validity, consistency, integrity, timeliness)
2. **`DQ.V_DQ_TREND`** — Time-series of metric values for trend analysis
3. **`DQ.V_DQ_GATE_SUMMARY`** — Aggregate DQ\_PASS/DQ\_FAIL counts, percentages, SNIP category breakdown
4. **`DQ.V_DQ_QUARANTINE`** — Detailed view of DQ-failed claims with per-check flags for investigation
5. **`DQ.V_DQ_DIMENSION_HEATMAP`** — Pass rates grouped by SNIP category and DQ dimension per table

*(View SQL same structure as previous plan, updated to include SNIP category mapping in the CASE statements)*

---

## Task 5: Gold Layer — Edit Rules Engine (11 Rules, NRT Dynamic Tables)

**All Gold edit rule DTs join `DQ.CLAIMS_QUALITY_GATE` filtering on `dq_status = 'DQ_PASS'`.**

### Original 8 Edit Rules (unchanged from previous plan)

| Edit Code | Edit Name                      | Category    | Severity | Layer |
| --------- | ------------------------------ | ----------- | -------- | ----- |
| PPE-001   | Duplicate Claim Check          | DUPLICATE   | REJECT   | Gold  |
| PPE-002   | Provider Enrollment Validation | PROVIDER    | DENY     | Gold  |
| PPE-003   | Beneficiary Eligibility Check  | ELIGIBILITY | DENY     | Gold  |
| PPE-004   | Revenue Code Validation        | CODING      | REJECT   | Gold  |
| PPE-005   | Admission/Discharge Date Logic | DATE        | REJECT   | Gold  |
| PPE-006   | DRG Consistency Check          | CODING      | FLAG     | Gold  |
| PPE-007   | Billing NPI Format Validation  | BILLING     | REJECT   | Gold  |
| PPE-008   | Timely Filing Check            | DATE        | DENY     | Gold  |

### NEW: 3 Payer-Specific Medicare Edit Rules

| Edit Code   | Edit Name                         | Category | Severity | Reference Table          |
| ----------- | --------------------------------- | -------- | -------- | ------------------------ |
| **PPE-009** | **Medically Unlikely Edit (MUE)** | PAYER    | DENY     | CONFIG.MUE\_LIMITS       |
| **PPE-010** | **NCCI Unbundling Check**         | PAYER    | DENY     | CONFIG.NCCI\_CODE\_PAIRS |
| **PPE-011** | **Frequency Limit Check**         | PAYER    | DENY     | CONFIG.FREQUENCY\_LIMITS |

**PPE-009: Medically Unlikely Edit (MUE)**

```
CREATE OR REPLACE DYNAMIC TABLE GOLD.EDIT_MUE_CHECK
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = CLAIMS_TRANSFORM_WH
AS
SELECT
  sl.claim_id,
  'PPE-009' AS edit_code,
  'Medically Unlikely Edit (MUE)' AS edit_name,
  'DENY' AS severity,
  sl.hcpcs_code || ': ' || sl.units || ' units (max ' || mue.max_units_per_day || ')' AS detail
FROM DQ.CLAIMS_QUALITY_GATE dqg
INNER JOIN SILVER.SERVICE_LINES sl ON dqg.claim_id = sl.claim_id AND dqg.dq_status = 'DQ_PASS'
INNER JOIN CONFIG.MUE_LIMITS mue
  ON sl.hcpcs_code = mue.hcpcs_code
  AND sl.service_date BETWEEN mue.effective_date AND COALESCE(mue.termination_date, '9999-12-31')
WHERE sl.units > mue.max_units_per_day;
```

**PPE-010: NCCI Unbundling Check**

```
CREATE OR REPLACE DYNAMIC TABLE GOLD.EDIT_NCCI_UNBUNDLING
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = CLAIMS_TRANSFORM_WH
AS
SELECT
  sl1.claim_id,
  'PPE-010' AS edit_code,
  'NCCI Unbundling Check' AS edit_name,
  'DENY' AS severity,
  sl1.hcpcs_code || ' + ' || sl2.hcpcs_code || ' (bundled pair)' AS detail
FROM DQ.CLAIMS_QUALITY_GATE dqg
INNER JOIN SILVER.SERVICE_LINES sl1 ON dqg.claim_id = sl1.claim_id AND dqg.dq_status = 'DQ_PASS'
INNER JOIN SILVER.SERVICE_LINES sl2
  ON sl1.claim_id = sl2.claim_id
  AND sl1.service_date = sl2.service_date
  AND sl1.line_number < sl2.line_number
INNER JOIN CONFIG.NCCI_CODE_PAIRS ncci
  ON sl1.hcpcs_code = ncci.column_1_code
  AND sl2.hcpcs_code = ncci.column_2_code
  AND ncci.modifier_indicator = 0
  AND sl1.service_date BETWEEN ncci.effective_date AND COALESCE(ncci.termination_date, '9999-12-31');
```

**PPE-011: Frequency Limit Check**

```
CREATE OR REPLACE DYNAMIC TABLE GOLD.EDIT_FREQUENCY_CHECK
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = CLAIMS_TRANSFORM_WH
AS
WITH service_counts AS (
  SELECT
    dqg.claim_id,
    sl.hcpcs_code,
    sl.service_date,
    ch.beneficiary_hic,
    COUNT(*) OVER (
      PARTITION BY ch.beneficiary_hic, sl.hcpcs_code
      ORDER BY sl.service_date
      RANGE BETWEEN INTERVAL '365 DAY' PRECEDING AND CURRENT ROW
    ) AS rolling_count,
    fl.max_frequency,
    fl.frequency_period_days
  FROM DQ.CLAIMS_QUALITY_GATE dqg
  INNER JOIN SILVER.SERVICE_LINES sl ON dqg.claim_id = sl.claim_id AND dqg.dq_status = 'DQ_PASS'
  INNER JOIN SILVER.CLAIM_HEADER ch ON sl.claim_id = ch.claim_id
  INNER JOIN CONFIG.FREQUENCY_LIMITS fl
    ON sl.hcpcs_code = fl.hcpcs_code
    AND sl.service_date BETWEEN fl.effective_date AND COALESCE(fl.termination_date, '9999-12-31')
)
SELECT
  claim_id,
  'PPE-011' AS edit_code,
  'Frequency Limit Check' AS edit_name,
  'DENY' AS severity,
  hcpcs_code || ': ' || rolling_count || ' occurrences in ' || frequency_period_days || ' days (max ' || max_frequency || ')' AS detail
FROM service_counts
WHERE rolling_count > max_frequency;
```

### Edit Results Summary (UNION ALL — now 11 edits)

```
CREATE OR REPLACE DYNAMIC TABLE GOLD.EDIT_RESULTS_SUMMARY
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = CLAIMS_TRANSFORM_WH
AS
WITH all_edits AS (
  SELECT claim_id, edit_code, edit_name, severity FROM GOLD.EDIT_DUPLICATE_CHECK
  UNION ALL SELECT claim_id, edit_code, edit_name, severity FROM GOLD.EDIT_PROVIDER_ENROLLMENT
  UNION ALL SELECT claim_id, edit_code, edit_name, severity FROM GOLD.EDIT_BENEFICIARY_ELIGIBILITY
  UNION ALL SELECT claim_id, edit_code, edit_name, severity FROM GOLD.EDIT_REVENUE_CODE
  UNION ALL SELECT claim_id, edit_code, edit_name, severity FROM GOLD.EDIT_DATE_LOGIC
  UNION ALL SELECT claim_id, edit_code, edit_name, severity FROM GOLD.EDIT_DRG_CONSISTENCY
  UNION ALL SELECT claim_id, edit_code, edit_name, severity FROM GOLD.EDIT_NPI_VALIDATION
  UNION ALL SELECT claim_id, edit_code, edit_name, severity FROM GOLD.EDIT_TIMELY_FILING
  UNION ALL SELECT claim_id, edit_code, edit_name, severity FROM GOLD.EDIT_MUE_CHECK
  UNION ALL SELECT claim_id, edit_code, edit_name, severity FROM GOLD.EDIT_NCCI_UNBUNDLING
  UNION ALL SELECT claim_id, edit_code, edit_name, severity FROM GOLD.EDIT_FREQUENCY_CHECK
)
SELECT
  claim_id,
  ARRAY_AGG(DISTINCT edit_code) AS triggered_edits,
  COUNT(*) AS edit_count,
  CASE
    WHEN MAX(CASE severity WHEN 'DENY' THEN 3 WHEN 'REJECT' THEN 2 WHEN 'FLAG' THEN 1 ELSE 0 END) = 3 THEN 'DENY'
    WHEN MAX(CASE severity WHEN 'DENY' THEN 3 WHEN 'REJECT' THEN 2 WHEN 'FLAG' THEN 1 ELSE 0 END) = 2 THEN 'REJECT'
    WHEN MAX(CASE severity WHEN 'DENY' THEN 3 WHEN 'REJECT' THEN 2 WHEN 'FLAG' THEN 1 ELSE 0 END) = 1 THEN 'FLAG'
    ELSE 'ACCEPT'
  END AS highest_severity
FROM all_edits
GROUP BY claim_id;
```

### Terminal Dynamic Table: Adjudication

```
CREATE OR REPLACE DYNAMIC TABLE GOLD.CLAIM_ADJUDICATION
  TARGET_LAG = '5 MINUTES'
  WAREHOUSE = CLAIMS_TRANSFORM_WH
AS
-- DQ-passed claims through edit rules
SELECT
  ch.claim_id, ch.dcn, ch.provider_npi, ch.beneficiary_hic,
  ch.admission_date, ch.discharge_date, ch.drg_code,
  ch.total_charges, ch.type_of_bill,
  COALESCE(er.highest_severity, 'ACCEPT') AS disposition,
  COALESCE(er.triggered_edits, ARRAY_CONSTRUCT()) AS triggered_edits,
  COALESCE(er.edit_count, 0) AS edit_count,
  'DQ_PASS' AS dq_status,
  CURRENT_TIMESTAMP() AS adjudication_timestamp
FROM DQ.CLAIMS_QUALITY_GATE dqg
INNER JOIN SILVER.CLAIM_HEADER ch ON dqg.claim_id = ch.claim_id AND dqg.dq_status = 'DQ_PASS'
LEFT JOIN GOLD.EDIT_RESULTS_SUMMARY er ON ch.claim_id = er.claim_id

UNION ALL

-- DQ-failed claims get DQ_REJECT (never entered edit rules)
SELECT
  ch.claim_id, ch.dcn, ch.provider_npi, ch.beneficiary_hic,
  ch.admission_date, ch.discharge_date, ch.drg_code,
  ch.total_charges, ch.type_of_bill,
  'DQ_REJECT' AS disposition,
  ARRAY_CONSTRUCT() AS triggered_edits,
  0 AS edit_count,
  'DQ_FAIL' AS dq_status,
  CURRENT_TIMESTAMP() AS adjudication_timestamp
FROM DQ.CLAIMS_QUALITY_GATE dqg
INNER JOIN SILVER.CLAIM_HEADER ch ON dqg.claim_id = ch.claim_id AND dqg.dq_status = 'DQ_FAIL';
```

---

## Task 6: DQ Dashboard Views

5 views powered by `SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS`:

1. **`DQ.V_DQ_SCORECARD`** — Per-table, per-metric, with SNIP category and DQ dimension
2. **`DQ.V_DQ_TREND`** — Time-series for trend analysis
3. **`DQ.V_DQ_GATE_SUMMARY`** — DQ\_PASS/DQ\_FAIL counts, SNIP breakdown
4. **`DQ.V_DQ_QUARANTINE`** — DQ-failed claims with per-check flags
5. **`DQ.V_DQ_DIMENSION_HEATMAP`** — Pass rates by SNIP category per table

---

## Task 7: Gold Analytics Views

- **GOLD.V\_EDIT\_HIT\_RATE** — % claims triggering each of the 11 edits
- **GOLD.V\_DISPOSITION\_SUMMARY** — Accept/Deny/Reject/Flag/DQ\_Reject distribution
- **GOLD.V\_PROVIDER\_EDIT\_SUMMARY** — Edits grouped by provider for fraud signals
- **GOLD.V\_PAYER\_EDIT\_SUMMARY** — MUE/NCCI/Frequency edit breakdown by HCPCS code

---

## Task 8: End-to-End Testing

1. Upload first batch (5,000 claims) to S3

2. Verify Snowpipe ingests into Bronze within \~1 minute

3. Confirm Dynamic Tables cascade through Silver and Gold within \~5-10 minutes

4. **Check DQ dashboard** — verify all 27 DMF results populate in scorecard, SNIP categories are accurate

5. Confirm DQ-failed claims appear in quarantine with correct SNIP failure category

6. Validate Gold adjudication results (DQ-passed claims):

   - \~80% Accept
   - \~10% Deny (eligibility/enrollment/MUE/NCCI/frequency failures)
   - \~5% Reject (coding/format errors)
   - \~5% DQ\_Reject (SNIP 1-6 structural failures)

7. Upload second batch to simulate trickle feed

8. Confirm incremental processing within 15-minute SLA

9. Verify payer-specific edits (MUE/NCCI/frequency) trigger correctly against reference tables

---

## NRT Technology Mapping

| Requirement                          | Snowflake Feature                                                | NRT Impact                                  |
| ------------------------------------ | ---------------------------------------------------------------- | ------------------------------------------- |
| S3 → Bronze Ingestion                | **Snowpipe** (auto\_ingest + SQS)                                | \~1-2 min latency                           |
| Bronze → Silver Transform            | **Dynamic Tables** (TARGET\_LAG = DOWNSTREAM)                    | Cascading, on-demand                        |
| SNIP 1-6 DQ Checks                   | **27 DMFs** (14 built-in + 13 custom, TRIGGER\_ON\_CHANGES)      | Auto-evaluates on data change               |
| DQ Gate (pre-edit filter)            | **Dynamic Table** (DQ.CLAIMS\_QUALITY\_GATE)                     | SNIP-aligned per-claim pass/fail            |
| DQ Dashboard                         | **SNOWFLAKE.LOCAL.DATA\_QUALITY\_MONITORING\_RESULTS** + 5 views | Real-time DQ visibility by SNIP category    |
| Silver → Gold Edit Rules             | **11 Dynamic Tables** (TARGET\_LAG = DOWNSTREAM)                 | Pure SQL, DQ-gated, includes payer-specific |
| Payer-Specific Edits (MUE/NCCI/Freq) | **Reference tables** in CONFIG + Gold DTs                        | Lookup-driven, configurable                 |
| Final Adjudication                   | **Dynamic Table** (TARGET\_LAG = 5 MINUTES)                      | Terminal DT drives the pipeline             |
| Orchestration                        | **Fully declarative** — no Tasks in hot path                     | Zero orchestration overhead                 |
