----------------------------------------------------------------------
-- ER Patient Admissions: Interactive Tables & Warehouses Demo
-- Step 4: Live Refresh Demo — Show near-real-time data flow
----------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE SCHEMA ER_INTERACTIVE_DEMO.ER_DATA;

----------------------------------------------------------------------
-- BEFORE: Check current state through Interactive Warehouse
----------------------------------------------------------------------
USE WAREHOUSE ER_INTERACTIVE_WH;

SELECT 'BEFORE' AS phase, COUNT(*) AS total_rows FROM ER_ADMISSIONS_IT;

SELECT 'BEFORE' AS phase, MAX(ADMISSION_TIME) AS most_recent_admission FROM ER_ADMISSIONS_IT;

----------------------------------------------------------------------
-- INSERT new ER admissions into SOURCE table (use standard warehouse)
----------------------------------------------------------------------
USE WAREHOUSE ER_STANDARD_WH;

INSERT INTO ER_ADMISSIONS_SOURCE
SELECT
    UUID_STRING()                                                        AS ADMISSION_ID,
    UUID_STRING()                                                        AS PATIENT_ID,
    v.age                                                                AS PATIENT_AGE,
    v.gender                                                             AS PATIENT_GENDER,
    CURRENT_TIMESTAMP()                                                  AS ADMISSION_TIME,
    v.triage                                                             AS TRIAGE_LEVEL,
    CASE v.triage
        WHEN 1 THEN 'Resuscitation'
        WHEN 2 THEN 'Emergent'
        WHEN 3 THEN 'Urgent'
        WHEN 4 THEN 'Less Urgent'
        WHEN 5 THEN 'Non-Urgent'
    END                                                                  AS TRIAGE_LABEL,
    v.complaint                                                          AS CHIEF_COMPLAINT,
    v.dept                                                               AS DEPARTMENT,
    v.physician                                                          AS ATTENDING_PHYSICIAN,
    'ER-' || LPAD(UNIFORM(1,60,RANDOM())::VARCHAR, 3, '0')              AS BED_NUMBER,
    v.hr                                                                 AS HEART_RATE,
    v.sbp                                                                AS SYSTOLIC_BP,
    v.dbp                                                                AS DIASTOLIC_BP,
    v.temp                                                               AS TEMPERATURE_F,
    v.o2                                                                 AS O2_SATURATION,
    v.arrival                                                            AS ARRIVAL_MODE,
    'Waiting'                                                            AS STATUS,
    v.wait_min                                                           AS WAIT_TIME_MINUTES,
    v.facility                                                           AS FACILITY
FROM (
    SELECT
        column1 AS complaint, column2 AS dept, column3 AS triage,
        column4 AS physician, column5 AS age, column6 AS gender,
        column7 AS hr, column8 AS sbp, column9 AS dbp,
        column10 AS temp, column11 AS o2, column12 AS arrival,
        column13 AS wait_min, column14 AS facility
    FROM VALUES
        ('Chest Pain','Cardiology',1,'Dr. Sarah Chen',67,'Male',110,180,95,99.1,94.2,'Ambulance',2,'Metro General Hospital'),
        ('Stroke Symptoms','Neurology',1,'Dr. Aisha Patel',72,'Female',95,210,110,98.6,96.0,'Helicopter',0,'University Health System ER'),
        ('Shortness of Breath','Pulmonology',2,'Dr. Michael Kim',55,'Male',105,150,88,100.2,89.5,'Ambulance',8,'St. Mary Regional Medical Center'),
        ('Allergic Reaction','General',2,'Dr. Emily Washington',28,'Female',120,100,65,99.8,95.0,'Walk-in',12,'Coastal Community Hospital'),
        ('Abdominal Pain','General',3,'Dr. David Okafor',45,'Male',88,135,82,98.9,97.5,'Walk-in',35,'Metro General Hospital'),
        ('Fracture','Orthopedics',3,'Dr. Robert Singh',34,'Female',82,128,78,98.4,98.2,'Walk-in',42,'St. Mary Regional Medical Center'),
        ('Laceration','Trauma',4,'Dr. Lisa Yamamoto',19,'Male',75,122,76,98.6,99.0,'Walk-in',65,'University Health System ER'),
        ('Fever / Infection','General',3,'Dr. Maria Gonzalez',8,'Female',130,95,60,102.8,97.0,'Walk-in',28,'Coastal Community Hospital'),
        ('Diabetic Emergency','Endocrinology',2,'Dr. Thomas Adebayo',61,'Male',100,145,90,97.8,95.5,'Ambulance',5,'Metro General Hospital'),
        ('Overdose','Toxicology',1,'Dr. Rachel Hoffman',23,'Female',55,85,50,96.2,91.0,'Ambulance',0,'University Health System ER')
) v;

SELECT 'INSERTED' AS phase, COUNT(*) AS source_rows FROM ER_ADMISSIONS_SOURCE;

----------------------------------------------------------------------
-- Force immediate refresh of the interactive table
----------------------------------------------------------------------
ALTER INTERACTIVE TABLE ER_ADMISSIONS_IT REFRESH;

----------------------------------------------------------------------
-- AFTER: Check updated state through Interactive Warehouse
-- Wait ~10 seconds after the REFRESH command, then run these
----------------------------------------------------------------------
USE WAREHOUSE ER_INTERACTIVE_WH;

SELECT 'AFTER' AS phase, COUNT(*) AS total_rows FROM ER_ADMISSIONS_IT;

SELECT 'AFTER' AS phase, MAX(ADMISSION_TIME) AS most_recent_admission FROM ER_ADMISSIONS_IT;

----------------------------------------------------------------------
-- Show the freshly inserted patients (should show current timestamp)
----------------------------------------------------------------------
SELECT
    ADMISSION_TIME,
    PATIENT_AGE,
    PATIENT_GENDER,
    CHIEF_COMPLAINT,
    TRIAGE_LABEL,
    STATUS,
    FACILITY
FROM ER_ADMISSIONS_IT
WHERE ADMISSION_TIME >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
ORDER BY ADMISSION_TIME DESC;

----------------------------------------------------------------------
-- OPTIONAL: Insert another batch to simulate ongoing ER flow
-- Run this multiple times to keep adding new patients
----------------------------------------------------------------------
/*
USE WAREHOUSE ER_STANDARD_WH;

INSERT INTO ER_ADMISSIONS_SOURCE
SELECT
    UUID_STRING(),
    UUID_STRING(),
    UNIFORM(5, 90, RANDOM()),
    CASE WHEN UNIFORM(0,1,RANDOM())=0 THEN 'Male' ELSE 'Female' END,
    CURRENT_TIMESTAMP(),
    UNIFORM(1, 5, RANDOM()),
    CASE UNIFORM(1,5,RANDOM())
        WHEN 1 THEN 'Resuscitation' WHEN 2 THEN 'Emergent'
        WHEN 3 THEN 'Urgent' WHEN 4 THEN 'Less Urgent' ELSE 'Non-Urgent'
    END,
    c.complaint, c.dept,
    p.name,
    'ER-' || LPAD(UNIFORM(1,60,RANDOM())::VARCHAR, 3, '0'),
    UNIFORM(50,160,RANDOM()), UNIFORM(80,200,RANDOM()), UNIFORM(50,120,RANDOM()),
    ROUND(UNIFORM(966,1040,RANDOM())/10.0,1),
    ROUND(UNIFORM(850,1000,RANDOM())/10.0,1),
    am.mode,
    'Waiting',
    UNIFORM(0, 120, RANDOM()),
    f.facility
FROM (SELECT column1 AS complaint, column2 AS dept FROM VALUES
    ('Chest Pain','Cardiology'),('Fracture','Orthopedics'),
    ('Fever / Infection','General'),('Head Injury','Trauma')) c,
    (SELECT column1 AS name FROM VALUES ('Dr. Sarah Chen'),('Dr. Kevin Chu')) p,
    (SELECT column1 AS mode FROM VALUES ('Walk-in'),('Ambulance')) am,
    (SELECT column1 AS facility FROM VALUES ('Metro General Hospital'),('Coastal Community Hospital')) f
QUALIFY ROW_NUMBER() OVER (ORDER BY RANDOM()) <= 5;

ALTER INTERACTIVE TABLE ER_ADMISSIONS_IT REFRESH;
*/
