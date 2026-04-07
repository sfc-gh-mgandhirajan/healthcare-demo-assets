----------------------------------------------------------------------
-- ER Patient Admissions: Interactive Tables & Warehouses Demo
-- Step 3: Demo Queries — Sub-second analytics on Interactive Warehouse
----------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ER_INTERACTIVE_WH;
USE SCHEMA ER_INTERACTIVE_DEMO.ER_DATA;

----------------------------------------------------------------------
-- Query 1: Live ER Census — current patients by status
----------------------------------------------------------------------
SELECT
    STATUS,
    COUNT(*) AS patient_count
FROM ER_ADMISSIONS_IT
GROUP BY STATUS
ORDER BY patient_count DESC;

----------------------------------------------------------------------
-- Query 2: Triage Distribution — volume by acuity level
----------------------------------------------------------------------
SELECT
    TRIAGE_LEVEL,
    TRIAGE_LABEL,
    COUNT(*) AS patient_count,
    ROUND(AVG(WAIT_TIME_MINUTES), 1) AS avg_wait_min
FROM ER_ADMISSIONS_IT
GROUP BY TRIAGE_LEVEL, TRIAGE_LABEL
ORDER BY TRIAGE_LEVEL;

----------------------------------------------------------------------
-- Query 3: Average Wait Times by Facility
----------------------------------------------------------------------
SELECT
    FACILITY,
    COUNT(*) AS admissions,
    ROUND(AVG(WAIT_TIME_MINUTES), 1) AS avg_wait_min,
    MAX(WAIT_TIME_MINUTES) AS max_wait_min
FROM ER_ADMISSIONS_IT
GROUP BY FACILITY
ORDER BY avg_wait_min DESC;

----------------------------------------------------------------------
-- Query 4: Top Chief Complaints (last 12 hours)
----------------------------------------------------------------------
SELECT
    CHIEF_COMPLAINT,
    COUNT(*) AS occurrences,
    ROUND(AVG(WAIT_TIME_MINUTES), 1) AS avg_wait_min
FROM ER_ADMISSIONS_IT
WHERE ADMISSION_TIME >= DATEADD('hour', -12, CURRENT_TIMESTAMP())
ORDER BY occurrences DESC
LIMIT 10;

----------------------------------------------------------------------
-- Query 5: Physician Workload
----------------------------------------------------------------------
SELECT
    ATTENDING_PHYSICIAN,
    COUNT(*) AS active_patients,
    ROUND(AVG(TRIAGE_LEVEL), 1) AS avg_acuity
FROM ER_ADMISSIONS_IT
WHERE STATUS IN ('Waiting', 'In Treatment')
GROUP BY ATTENDING_PHYSICIAN
ORDER BY active_patients DESC;

----------------------------------------------------------------------
-- Query 6: Arrival Mode Breakdown
----------------------------------------------------------------------
SELECT
    ARRIVAL_MODE,
    COUNT(*) AS count,
    ROUND(AVG(TRIAGE_LEVEL), 1) AS avg_triage,
    ROUND(AVG(WAIT_TIME_MINUTES), 1) AS avg_wait_min
FROM ER_ADMISSIONS_IT
GROUP BY ARRIVAL_MODE
ORDER BY count DESC;

----------------------------------------------------------------------
-- Query 7: Critical Patients (Triage 1-2) — point lookup style
----------------------------------------------------------------------
SELECT
    ADMISSION_TIME,
    PATIENT_AGE,
    PATIENT_GENDER,
    CHIEF_COMPLAINT,
    TRIAGE_LABEL,
    HEART_RATE,
    O2_SATURATION,
    STATUS,
    ATTENDING_PHYSICIAN,
    FACILITY
FROM ER_ADMISSIONS_IT
WHERE TRIAGE_LEVEL <= 2
ORDER BY ADMISSION_TIME DESC
LIMIT 20;

----------------------------------------------------------------------
-- Query 8: Hourly admission trend (last 24 hours)
----------------------------------------------------------------------
SELECT
    DATE_TRUNC('hour', ADMISSION_TIME) AS hour,
    COUNT(*) AS admissions
FROM ER_ADMISSIONS_IT
WHERE ADMISSION_TIME >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY hour
ORDER BY hour;
