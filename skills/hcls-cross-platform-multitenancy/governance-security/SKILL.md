---
name: governance-security
description: "Enterprise governance for multi-tenant platforms using Snowflake Horizon: data classification, tag-based masking, row access policies, column-level security, lineage, audit, object tagging, data discovery, network policies, Trust Center. Triggers: governance, Horizon, classification, masking, tagging, lineage, audit, data discovery, network policy, Trust Center, compliance, object tagging, tag-based masking, data catalog, access history, sensitive data."
platform_affinities:
  produces:
    - classification tags
    - masking policies
    - lineage metadata
    - audit views
  benefits_from:
    - skill: data-governance
      when: "always — Snowflake Horizon classification, lineage, and policy management are the core platform capability here"
    - skill: trust-center
      when: "customer wants CIS Benchmark or Security Essentials scanning to validate governance posture"
    - skill: network-security
      when: "network policies, private link, or IP allow-listing are part of the governance posture"
---

# Enterprise Governance & Security (Snowflake Horizon)

## When to Use

**Load** this skill when the customer needs enterprise governance setup using Snowflake Horizon features — data classification, masking, tagging, lineage, discovery, and audit.

---

## Step 1: Understand Governance Requirements

Gather:

| Input | Description |
|-------|-------------|
| Compliance frameworks | HIPAA, SOC2, PCI-DSS, GDPR, FedRAMP, industry-specific |
| Sensitive data types | PII, PHI, financial data, trade secrets, custom categories |
| Governance team structure | Centralized governance team, distributed per-tenant, hybrid |
| Existing governance setup | Tags, policies, classification already in place? |
| Audit requirements | Access logs, query history, data lineage for compliance |
| Data discovery needs | Auto-classify new data, catalog for self-service |

---

## Step 2: Snowflake Horizon — Governance Framework

### 2A: Object Tagging

Create a tag taxonomy for governance tracking:

```sql
CREATE TAG IF NOT EXISTS {{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.DATA_CLASSIFICATION
    ALLOWED_VALUES 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED'
    COMMENT = 'Data classification level';

CREATE TAG IF NOT EXISTS {{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.DATA_DOMAIN
    COMMENT = 'Business domain for the data (e.g., finance, operations, customer)';

CREATE TAG IF NOT EXISTS {{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.DATA_OWNER
    COMMENT = 'Team or individual responsible for data quality';

CREATE TAG IF NOT EXISTS {{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.RETENTION_PERIOD
    ALLOWED_VALUES '30_DAYS', '90_DAYS', '1_YEAR', '3_YEARS', '7_YEARS', 'INDEFINITE'
    COMMENT = 'Data retention policy';

CREATE TAG IF NOT EXISTS {{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.TENANT_ID
    COMMENT = 'Tenant identifier for multi-tenant governance';
```

Apply tags to objects:

```sql
ALTER TABLE {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}
    SET TAG {{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.DATA_CLASSIFICATION = '{{LEVEL}}';
ALTER TABLE {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}
    SET TAG {{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.DATA_DOMAIN = '{{DOMAIN}}';
ALTER TABLE {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}
    SET TAG {{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.DATA_OWNER = '{{OWNER}}';
```

### 2B: Data Classification (Automatic)

Use Snowflake's built-in classification to auto-detect sensitive data:

```sql
SELECT SYSTEM$CLASSIFY('{{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}', {'auto_tag': true});
```

This automatically:
- Scans columns for PII, PHI, financial data patterns
- Assigns semantic and privacy category tags
- Tags are SYSTEM$CLASSIFICATION_TAG_BASED

Review classification results:

```sql
SELECT *
FROM TABLE(
    {{DATA_DB}}.INFORMATION_SCHEMA.TAG_REFERENCES(
        '{{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}', 'TABLE'
    )
);
```

### 2C: Tag-Based Masking Policies

Automatically apply masking based on classification tags:

```sql
CREATE OR REPLACE MASKING POLICY {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.AUTO_MASK_SENSITIVE
AS (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'PLATFORM_ADMIN_ROLE', 'GOVERNANCE_ADMIN_ROLE')
        THEN val
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('{{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.DATA_CLASSIFICATION') = 'RESTRICTED'
        THEN '***REDACTED***'
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('{{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.DATA_CLASSIFICATION') = 'CONFIDENTIAL'
        THEN CONCAT('***', RIGHT(val, 4))
        ELSE val
    END;

ALTER TAG {{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.DATA_CLASSIFICATION
    SET MASKING POLICY {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.AUTO_MASK_SENSITIVE;
```

### 2D: Column-Level Security

For granular column access beyond masking:

```sql
CREATE OR REPLACE MASKING POLICY {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.COLUMN_RESTRICT_{{COLUMN_TYPE}}
AS (val {{DATA_TYPE}}) RETURNS {{DATA_TYPE}} ->
    CASE
        WHEN CURRENT_ROLE() IN ({{AUTHORIZED_ROLES}})
        THEN val
        ELSE {{MASKED_VALUE}}
    END;

ALTER TABLE {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}
    MODIFY COLUMN {{COLUMN_NAME}}
    SET MASKING POLICY {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.COLUMN_RESTRICT_{{COLUMN_TYPE}};
```

---

## Step 3: Data Lineage & Discovery

### Lineage Tracking

Snowflake automatically tracks lineage through ACCOUNT_USAGE views:

```sql
SELECT
    DOWNSTREAM_OBJECT_NAME,
    DOWNSTREAM_OBJECT_DOMAIN,
    UPSTREAM_OBJECT_NAME,
    UPSTREAM_OBJECT_DOMAIN
FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
WHERE REFERENCED_OBJECT_NAME = '{{TABLE_NAME}}'
    AND REFERENCED_DATABASE = '{{DATA_DB}}'
    AND REFERENCED_SCHEMA = '{{SCHEMA}}';
```

### Column-Level Lineage

```sql
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
WHERE query_start_time >= DATEADD('DAY', -30, CURRENT_TIMESTAMP)
ORDER BY query_start_time DESC;
```

### Data Discovery via Catalog

Use Snowflake's Horizon Catalog for self-service data discovery:
- Auto-classification discovers sensitive data
- Tags provide business context
- Lineage shows data flow
- Access history shows who uses what

---

## Step 4: Audit & Compliance

### Access Audit

```sql
SELECT
    query_id,
    user_name,
    role_name,
    warehouse_name,
    start_time,
    total_elapsed_time / 1000 AS elapsed_sec,
    rows_produced,
    bytes_scanned,
    query_text
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_text ILIKE '%{{TABLE_OR_KEYWORD}}%'
    AND start_time >= DATEADD('DAY', -{{DAYS}}, CURRENT_TIMESTAMP)
ORDER BY start_time DESC;
```

### Column-Level Access Tracking

```sql
SELECT
    ah.query_id,
    ah.user_name,
    ah.role_name,
    ah.query_start_time,
    obj.value:objectName::VARCHAR AS object_name,
    col.value:columnName::VARCHAR AS column_name
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
    LATERAL FLATTEN(base_objects_accessed) obj,
    LATERAL FLATTEN(obj.value:columns) col
WHERE ah.query_start_time >= DATEADD('DAY', -{{DAYS}}, CURRENT_TIMESTAMP)
    AND obj.value:objectName::VARCHAR ILIKE '%{{TABLE_NAME}}%'
ORDER BY ah.query_start_time DESC;
```

### Login Anomaly Detection

```sql
SELECT
    user_name,
    client_ip,
    reported_client_type,
    error_code,
    error_message,
    event_timestamp,
    is_success
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE event_timestamp >= DATEADD('DAY', -30, CURRENT_TIMESTAMP)
    AND is_success = 'NO'
ORDER BY event_timestamp DESC;
```

### Trust Center Integration

Recommend enabling **Snowflake Trust Center** scanners:
- **Security Essentials** — baseline security posture
- **CIS Benchmarks** — industry-standard security controls
- **Threat Intelligence** — anomaly detection

---

## Step 5: Network Security

### Network Policies for Tenant Isolation

**Recommended approach (Network Rules)**:

```sql
CREATE NETWORK RULE IF NOT EXISTS {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.NR_{{TENANT_ID}}_INGRESS
    TYPE = IPV4
    VALUE_LIST = ({{TENANT_IP_RANGES}})
    MODE = INGRESS
    COMMENT = 'Ingress rule for {{TENANT_NAME}}';

CREATE OR REPLACE NETWORK POLICY NP_{{TENANT_ID}}
    ALLOWED_NETWORK_RULE_LIST = ('{{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.NR_{{TENANT_ID}}_INGRESS')
    COMMENT = 'Network policy for {{TENANT_NAME}}';

ALTER USER {{TENANT_USER}} SET NETWORK_POLICY = NP_{{TENANT_ID}};
```

**Legacy approach (IP lists)** — still supported but network rules are preferred for new deployments:

```sql
CREATE OR REPLACE NETWORK POLICY NP_{{TENANT_ID}}
    ALLOWED_IP_LIST = ({{TENANT_IP_RANGES}})
    BLOCKED_IP_LIST = ()
    COMMENT = 'Network policy for {{TENANT_NAME}}';

ALTER USER {{TENANT_USER}} SET NETWORK_POLICY = NP_{{TENANT_ID}};
```

---

## Step 6: Industry Compliance Frameworks

### HIPAA (Healthcare)

If the customer mentions HIPAA, PHI, healthcare, clinical, patient, or health plan data, apply this framework:

**HIPAA Safe Harbor De-Identification — 18 Identifiers to Mask or Remove:**

| # | Identifier | Snowflake Classification Category | Masking Action |
|---|-----------|----------------------------------|----------------|
| 1 | Names | NAME (IDENTIFIER) | Full mask |
| 2 | Geographic data (below state) | STREET_ADDRESS, POSTAL_CODE, CITY | Full mask or generalize to state |
| 3 | Dates (except year) related to individual | DATE_OF_BIRTH | Truncate to year: `DATE_TRUNC('YEAR', val)` |
| 4 | Phone numbers | PHONE_NUMBER (IDENTIFIER) | Full mask |
| 5 | Fax numbers | (custom classifier) | Full mask |
| 6 | Email addresses | EMAIL (IDENTIFIER) | Full mask |
| 7 | Social Security Numbers | NATIONAL_IDENTIFIER / US_SSN | Full mask |
| 8 | Medical Record Numbers (MRN) | (custom classifier) | Full mask or hash: `SHA2(val, 256)` |
| 9 | Health plan beneficiary numbers | (custom classifier) | Full mask |
| 10 | Account numbers | BANK_ACCOUNT (IDENTIFIER) | Full mask |
| 11 | Certificate/license numbers | (custom classifier) | Full mask |
| 12 | Vehicle identifiers and serial numbers | VIN (IDENTIFIER) | Full mask |
| 13 | Device identifiers | (custom classifier) | Full mask |
| 14 | Web URLs | URL (IDENTIFIER) | Full mask |
| 15 | IP addresses | IP_ADDRESS (IDENTIFIER) | Full mask |
| 16 | Biometric identifiers | (custom classifier) | Full mask |
| 17 | Full-face photographs | N/A (unstructured) | Remove from data pipeline |
| 18 | Any other unique identifying number | (custom classifier per domain) | Full mask |

**Custom classifiers for healthcare-specific identifiers not covered by Snowflake native categories:**

```sql
CREATE OR REPLACE CUSTOM CLASSIFIER {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.HEALTHCARE_PHI_CLASSIFIER();

ALTER CUSTOM CLASSIFIER {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.HEALTHCARE_PHI_CLASSIFIER
    ADD CATEGORY 'MEDICAL_RECORD_NUMBER'
    REGEX '(MRN|mrn|Medical.?Record)[:\s\-]?\d{6,12}'
    SEMANTIC_CATEGORY 'MEDICAL_DATA'
    PRIVACY_CATEGORY 'IDENTIFIER'
    DESCRIPTION 'Medical Record Number (HIPAA identifier #8)';

ALTER CUSTOM CLASSIFIER {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.HEALTHCARE_PHI_CLASSIFIER
    ADD CATEGORY 'HEALTH_PLAN_ID'
    REGEX '(HICN|MBI|Member.?ID)[:\s\-]?[A-Z0-9]{8,12}'
    SEMANTIC_CATEGORY 'MEDICAL_DATA'
    PRIVACY_CATEGORY 'IDENTIFIER'
    DESCRIPTION 'Health plan beneficiary number (HIPAA identifier #9)';
```

**HIPAA audit requirements — use these queries for compliance evidence:**

```sql
SELECT
    ah.query_id,
    ah.user_name,
    ah.role_name,
    ah.query_start_time,
    obj.value:objectName::VARCHAR AS phi_table_accessed,
    ARRAY_AGG(DISTINCT col.value:columnName::VARCHAR) AS phi_columns_accessed
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
    LATERAL FLATTEN(base_objects_accessed) obj,
    LATERAL FLATTEN(obj.value:columns) col
WHERE ah.query_start_time >= DATEADD('DAY', -{{AUDIT_DAYS}}, CURRENT_TIMESTAMP)
GROUP BY ah.query_id, ah.user_name, ah.role_name, ah.query_start_time, phi_table_accessed
ORDER BY ah.query_start_time DESC;
```

### 21 CFR Part 11 (Life Sciences / Clinical Trials)

If the customer mentions clinical trials, CRO, pharma, FDA, or 21 CFR Part 11:

| Requirement | Snowflake Implementation |
|------------|-------------------------|
| **Audit trails** | `ACCESS_HISTORY` + `QUERY_HISTORY` — immutable, system-managed |
| **Electronic signatures** | Application-layer responsibility; Snowflake provides user authentication + MFA |
| **Data integrity** | Time Travel + Fail-safe ensure data cannot be silently modified |
| **Access controls** | RBAC + RLAP + masking policies |
| **System validation** | Snowflake SOC 2 Type II + SOC 1 reports; customer validates application layer |

```sql
SELECT
    qh.query_id,
    qh.user_name,
    qh.role_name,
    qh.start_time,
    qh.query_type,
    qh.query_text
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
WHERE qh.query_type IN ('INSERT', 'UPDATE', 'DELETE', 'MERGE', 'COPY')
    AND qh.start_time >= DATEADD('DAY', -{{AUDIT_DAYS}}, CURRENT_TIMESTAMP)
    AND qh.database_name = '{{DATA_DB}}'
ORDER BY qh.start_time DESC;
```

### GINA (Genetic Information Nondiscrimination Act)

If the customer handles genomic or genetic data:

| Requirement | Snowflake Implementation |
|------------|-------------------------|
| Prevent use of genetic info in employment/insurance decisions | RLAP restricting genetic tables to authorized research roles only |
| Re-identification risk for rare variants | Aggregate variant data with minimum group sizes (k-anonymity); never expose individual-level variants in shares |
| IRB protocol enforcement | Tag tables with `IRB_PROTOCOL_ID`; RLAP references an IRB_APPROVALS entitlements table |

### GxP (Good Practice — Manufacturing / Quality)

If the customer mentions manufacturing, batch records, quality, GMP, or GxP:

| Requirement | Snowflake Implementation |
|------------|-------------------------|
| Data integrity (ALCOA+) | Time Travel preserves original values; `QUERY_HISTORY` shows all modifications |
| Change control | Use `OBJECT_DEPENDENCIES` lineage + `TAG_REFERENCES` for change tracking |
| Validation | Document Snowflake IQ/OQ/PQ via SOC reports + customer-side validation scripts |

---

## Step 7: De-Identification & Safe Data Sharing

### HIPAA Safe Harbor Method

For creating de-identified datasets (e.g., for research, Marketplace, data clean rooms):

```sql
CREATE OR REPLACE SECURE VIEW {{DATA_DB}}.{{SCHEMA}}.V_DEIDENTIFIED_{{TABLE_NAME}} AS
SELECT
    SHA2({{PATIENT_ID_COLUMN}}, 256) AS PATIENT_HASH,
    DATE_TRUNC('YEAR', {{DOB_COLUMN}}) AS BIRTH_YEAR,
    LEFT({{ZIP_COLUMN}}, 3) AS ZIP3,
    {{NON_PHI_COLUMNS}}
FROM {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}
WHERE LEFT({{ZIP_COLUMN}}, 3) NOT IN
    ('036','059','063','102','203','556','692','790','821','823','830','831','878','879','884','890','893');
```

**Note**: The WHERE clause excludes 3-digit ZIP codes with populations under 20,000 per HIPAA Safe Harbor.

### Genomic Data Re-Identification Risk

For variant-level data, apply k-anonymity:

```sql
CREATE OR REPLACE SECURE VIEW {{DATA_DB}}.{{SCHEMA}}.V_AGGREGATE_VARIANTS AS
SELECT
    CHROMOSOME, POSITION, REF, ALT, GENE,
    COUNT(DISTINCT {{AMC_ID_COLUMN}}) AS CONTRIBUTING_SITES,
    COUNT(*) AS TOTAL_OBSERVATIONS,
    COUNT(*) / SUM(COUNT(*)) OVER () AS ALLELE_FREQUENCY
FROM {{DATA_DB}}.{{SCHEMA}}.{{VARIANT_TABLE}}
GROUP BY CHROMOSOME, POSITION, REF, ALT, GENE
HAVING COUNT(DISTINCT {{AMC_ID_COLUMN}}) >= 3
    AND COUNT(*) >= 5;
```

---

## Edition Requirements

| Feature | Minimum Edition |
|---------|----------------|
| Automatic data classification (`SYSTEM$CLASSIFY`) | **Enterprise** |
| Custom classifiers | **Enterprise** |
| Tag-based masking policies | **Enterprise** |
| Row access policies | **Enterprise** |
| Column-level masking policies | All editions (Standard+) |
| Object tagging | All editions |
| ACCESS_HISTORY view | **Enterprise** |
| Network policies | All editions |
| Network rules | All editions |
| Trust Center scanners | **Enterprise** |
| Time Travel (up to 90 days) | **Enterprise** |
| Tri-Secret Secure encryption | **Business Critical** |
| HIPAA BAA support | **Business Critical** |
| PCI-DSS compliance | **Business Critical** |

---

## Output

**MANDATORY STOPPING POINT**: Present the governance framework for customer approval before generating scripts.

Deliver:
1. **Tag taxonomy** — classification, domain, owner, retention tags
2. **Classification plan** — auto-classification schedule, custom classifiers for industry-specific data
3. **Masking policy set** — tag-based and column-level masking policies
4. **Compliance checklist** — HIPAA 18 identifiers, 21 CFR Part 11, GINA, or GxP as applicable
5. **De-identification templates** — Safe Harbor views for research/sharing
6. **Audit queries** — ready-to-run compliance monitoring queries
7. **Lineage documentation** — data flow from source to data products
8. **Network policy setup** — if tenant-level IP restriction needed
9. **Trust Center recommendations** — scanners to enable
10. **Edition requirements** — flag all features requiring Enterprise or Business Critical

After approval, route to:
- `implementation` for executable scripts
- `ai-governance` if AI features are used
