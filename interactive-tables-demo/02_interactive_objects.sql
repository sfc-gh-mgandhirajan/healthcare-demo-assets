----------------------------------------------------------------------
-- ER Patient Admissions: Interactive Tables & Warehouses Demo
-- Step 2: Create Interactive Table + Interactive Warehouse
----------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ER_STANDARD_WH;
USE SCHEMA ER_INTERACTIVE_DEMO.ER_DATA;

----------------------------------------------------------------------
-- Dynamic Interactive Table with auto-refresh (TARGET_LAG)
-- Clustered by key dimension + time columns so the interactive
-- warehouse can serve GROUP BY analytics at sub-second latency
-- Refreshes from source table every 1 minute
----------------------------------------------------------------------
CREATE OR REPLACE INTERACTIVE TABLE ER_ADMISSIONS_IT
  CLUSTER BY (ADMISSION_TIME, STATUS, TRIAGE_LEVEL, TRIAGE_LABEL, CHIEF_COMPLAINT, FACILITY, ARRIVAL_MODE, DEPARTMENT)
  TARGET_LAG = '1 minute'
  WAREHOUSE = ER_STANDARD_WH
AS
SELECT
    ADMISSION_ID,
    PATIENT_ID,
    PATIENT_AGE,
    PATIENT_GENDER,
    ADMISSION_TIME,
    TRIAGE_LEVEL,
    TRIAGE_LABEL,
    CHIEF_COMPLAINT,
    DEPARTMENT,
    ATTENDING_PHYSICIAN,
    BED_NUMBER,
    HEART_RATE,
    SYSTOLIC_BP,
    DIASTOLIC_BP,
    TEMPERATURE_F,
    O2_SATURATION,
    ARRIVAL_MODE,
    STATUS,
    WAIT_TIME_MINUTES,
    FACILITY
FROM ER_ADMISSIONS_SOURCE;

SHOW INTERACTIVE TABLES IN SCHEMA ER_INTERACTIVE_DEMO.ER_DATA;

----------------------------------------------------------------------
-- Interactive Warehouse — dedicated low-latency compute
----------------------------------------------------------------------
CREATE OR REPLACE INTERACTIVE WAREHOUSE ER_INTERACTIVE_WH
  WAREHOUSE_SIZE = 'XSMALL';

----------------------------------------------------------------------
-- Add the interactive table to the interactive warehouse
----------------------------------------------------------------------
ALTER WAREHOUSE ER_INTERACTIVE_WH ADD TABLES (ER_INTERACTIVE_DEMO.ER_DATA.ER_ADMISSIONS_IT);

----------------------------------------------------------------------
-- Resume the interactive warehouse (created in suspended state)
----------------------------------------------------------------------
ALTER WAREHOUSE ER_INTERACTIVE_WH RESUME;

----------------------------------------------------------------------
-- Verify: row count through interactive warehouse
-- NOTE: first queries may be slower while cache warms (~1-2 min)
----------------------------------------------------------------------
USE WAREHOUSE ER_INTERACTIVE_WH;

SELECT COUNT(*) AS total_admissions FROM ER_ADMISSIONS_IT;
