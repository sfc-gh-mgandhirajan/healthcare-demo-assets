/*
 * Clinical NLP Terminology Seed Data Setup
 * =========================================
 * Loads CODE_SYSTEM (8 rows) and CONCEPT_DIMENSION (154,626 rows) tables.
 *
 * APPROACH:
 *   - 6 curated code systems (792 rows) loaded from CSV via PUT + COPY INTO
 *   - ICD-10-CM (74,719 rows) loaded from CDC FTP via Snowflake stored procedure
 *   - ICD-10-PCS (79,115 rows) loaded from CMS.gov via Snowflake stored procedure
 *
 * PREREQUISITES:
 *   - Database UNSTRUCTURED_HEALTHDATA with schema DATA_MODEL_KNOWLEDGE
 *   - ACCOUNTADMIN role (for external access integrations)
 *   - Warehouse with USAGE grant
 *
 * USAGE:
 *   1. Run this script in Snowflake
 *   2. Upload CSVs:  PUT file:///path/to/seed-data/code_system.csv @seed_data_stage AUTO_COMPRESS=FALSE;
 *                    PUT file:///path/to/seed-data/concept_dimension_curated.csv @seed_data_stage AUTO_COMPRESS=FALSE;
 *   3. Run the COPY INTO statements (Step 2)
 *   4. Run the stored procedures for ICD-10-CM and ICD-10-PCS (Step 3)
 */

USE DATABASE UNSTRUCTURED_HEALTHDATA;
USE SCHEMA DATA_MODEL_KNOWLEDGE;

-- ============================================================
-- STEP 1: Create tables and staging area
-- ============================================================

CREATE TABLE IF NOT EXISTS CODE_SYSTEM (
    code_system_id VARCHAR NOT NULL PRIMARY KEY,
    name           VARCHAR NOT NULL,
    uri            VARCHAR
);

CREATE TABLE IF NOT EXISTS CONCEPT_DIMENSION (
    concept_id     VARCHAR NOT NULL PRIMARY KEY,
    code           VARCHAR NOT NULL,
    code_system_id VARCHAR NOT NULL,
    display        VARCHAR NOT NULL,
    semantic_group VARCHAR
);

CREATE OR REPLACE STAGE seed_data_stage
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- ============================================================
-- STEP 2: Load CSVs (run after PUT)
-- ============================================================
-- First upload the CSVs:
--   PUT file:///path/to/seed-data/code_system.csv @seed_data_stage AUTO_COMPRESS=FALSE;
--   PUT file:///path/to/seed-data/concept_dimension_curated.csv @seed_data_stage AUTO_COMPRESS=FALSE;

COPY INTO CODE_SYSTEM (code_system_id, name, uri)
FROM @seed_data_stage/code_system.csv
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'SKIP_FILE';

COPY INTO CONCEPT_DIMENSION (concept_id, code, code_system_id, display, semantic_group)
FROM @seed_data_stage/concept_dimension_curated.csv
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'SKIP_FILE';

-- ============================================================
-- STEP 3: Load ICD-10-CM and ICD-10-PCS from government sources
-- ============================================================
-- These require external access integrations (ACCOUNTADMIN).
-- Each SP is idempotent — safe to re-run.

-- 3a. Network rules
CREATE OR REPLACE NETWORK RULE cdc_ftp_rule
    MODE = EGRESS TYPE = HOST_PORT VALUE_LIST = ('ftp.cdc.gov:443');

CREATE OR REPLACE NETWORK RULE cms_gov_rule
    MODE = EGRESS TYPE = HOST_PORT VALUE_LIST = ('www.cms.gov:443');

-- 3b. External access integrations
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION cdc_ftp_access
    ALLOWED_NETWORK_RULES = (cdc_ftp_rule)
    ENABLED = TRUE;

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION cms_gov_access
    ALLOWED_NETWORK_RULES = (cdc_ftp_rule, cms_gov_rule)
    ENABLED = TRUE;

-- 3c. ICD-10-CM loader (downloads from CDC FTP, parses fixed-width order file)
CREATE OR REPLACE PROCEDURE LOAD_ICD10CM()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
EXTERNAL_ACCESS_INTEGRATIONS = (cdc_ftp_access)
HANDLER = 'main'
EXECUTE AS CALLER
AS $$
import _snowflake
import urllib.request
import zipfile
import io

def main(session):
    ct = session.sql("SELECT COUNT(*) AS C FROM CONCEPT_DIMENSION WHERE code_system_id='ICD10CM'").collect()[0]['C']
    if ct > 0:
        return {"status": "SKIPPED", "message": f"ICD-10-CM already loaded ({ct} rows)"}

    url = "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Publications/ICD10CM/2025/icd10cm-order-2025.zip"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    resp = urllib.request.urlopen(req, timeout=120)
    zdata = resp.read()

    zf = zipfile.ZipFile(io.BytesIO(zdata))
    order_file = [n for n in zf.namelist() if 'order' in n.lower() and n.endswith('.txt')][0]
    lines = zf.read(order_file).decode('utf-8', errors='replace').splitlines()

    rows = []
    for line in lines:
        if len(line) < 17:
            continue
        billable = line[14:15].strip()
        if billable != '1':
            continue
        code = line[6:13].strip()
        short_desc = line[16:77].strip()
        long_desc = line[77:].strip() if len(line) > 77 else short_desc
        display = long_desc if long_desc else short_desc
        concept_id = f"ICD10CM-{code}"
        sem = 'DISEASE'
        if code.startswith('R'):
            sem = 'SYMPTOM'
        elif code.startswith('Z'):
            sem = 'SOCIAL'
        elif any(code.startswith(p) for p in ['0','1','2','3','4','5','6','7','8','9']):
            sem = 'PROCEDURE'
        rows.append((concept_id, code, 'ICD10CM', display, sem))

    batch = 2000
    total = 0
    for i in range(0, len(rows), batch):
        chunk = rows[i:i+batch]
        vals = ", ".join([f"('{r[0]}','{r[1]}','ICD10CM','{r[3].replace(chr(39), chr(39)+chr(39))}','{r[4]}')" for r in chunk])
        session.sql(f"INSERT INTO CONCEPT_DIMENSION (concept_id,code,code_system_id,display,semantic_group) VALUES {vals}").collect()
        total += len(chunk)

    return {"status": "SUCCESS", "rows_loaded": total}
$$;

-- 3d. ICD-10-PCS loader (downloads from CMS.gov, parses fixed-width order file)
CREATE OR REPLACE PROCEDURE LOAD_ICD10PCS()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
EXTERNAL_ACCESS_INTEGRATIONS = (cms_gov_access)
HANDLER = 'main'
EXECUTE AS CALLER
AS $$
import _snowflake
import urllib.request
import zipfile
import io

def main(session):
    ct = session.sql("SELECT COUNT(*) AS C FROM CONCEPT_DIMENSION WHERE code_system_id='ICD10PCS'").collect()[0]['C']
    if ct > 0:
        return {"status": "SKIPPED", "message": f"ICD-10-PCS already loaded ({ct} rows)"}

    url = "https://www.cms.gov/files/zip/2025-icd-10-pcs-order-file-long-and-abbreviated-titles-updated-12/20/2024.zip"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    resp = urllib.request.urlopen(req, timeout=120)
    zdata = resp.read()

    zf = zipfile.ZipFile(io.BytesIO(zdata))
    order_file = [n for n in zf.namelist() if 'order' in n.lower() and n.endswith('.txt')][0]
    lines = zf.read(order_file).decode('utf-8', errors='replace').splitlines()

    rows = []
    for line in lines:
        if len(line) < 17:
            continue
        billable = line[14:15].strip()
        if billable != '1':
            continue
        code = line[6:13].strip()
        short_desc = line[16:77].strip()
        long_desc = line[77:].strip() if len(line) > 77 else short_desc
        display = long_desc if long_desc else short_desc
        concept_id = f"ICD10PCS-{code}"
        rows.append((concept_id, code, 'ICD10PCS', display, 'PROCEDURE'))

    batch = 2000
    total = 0
    for i in range(0, len(rows), batch):
        chunk = rows[i:i+batch]
        vals = ", ".join([f"('{r[0]}','{r[1]}','ICD10PCS','{r[3].replace(chr(39), chr(39)+chr(39))}','PROCEDURE')" for r in chunk])
        session.sql(f"INSERT INTO CONCEPT_DIMENSION (concept_id,code,code_system_id,display,semantic_group) VALUES {vals}").collect()
        total += len(chunk)

    return {"status": "SUCCESS", "rows_loaded": total}
$$;

-- 3e. Execute the loaders (idempotent — safe to re-run)
CALL LOAD_ICD10CM();
CALL LOAD_ICD10PCS();

-- ============================================================
-- STEP 4: Verify
-- ============================================================
SELECT code_system_id, COUNT(*) AS concept_count
FROM CONCEPT_DIMENSION
GROUP BY code_system_id
ORDER BY concept_count DESC;
