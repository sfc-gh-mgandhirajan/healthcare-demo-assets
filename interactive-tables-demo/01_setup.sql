----------------------------------------------------------------------
-- ER Patient Admissions: Interactive Tables & Warehouses Demo
-- Step 1: Setup — Database, Schema, Standard Warehouse, Source Table
----------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS ER_INTERACTIVE_DEMO;
CREATE SCHEMA IF NOT EXISTS ER_INTERACTIVE_DEMO.ER_DATA;

CREATE WAREHOUSE IF NOT EXISTS ER_STANDARD_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  COMMENT = 'Standard warehouse for data loading and interactive table creation';

USE WAREHOUSE ER_STANDARD_WH;
USE SCHEMA ER_INTERACTIVE_DEMO.ER_DATA;

----------------------------------------------------------------------
-- Source table: simulates an operational ER admissions feed
----------------------------------------------------------------------
CREATE OR REPLACE TABLE ER_ADMISSIONS_SOURCE (
    ADMISSION_ID        VARCHAR(36),
    PATIENT_ID          VARCHAR(36),
    PATIENT_AGE         NUMBER(3,0),
    PATIENT_GENDER      VARCHAR(10),
    ADMISSION_TIME      TIMESTAMP_NTZ,
    TRIAGE_LEVEL        NUMBER(1,0),
    TRIAGE_LABEL        VARCHAR(20),
    CHIEF_COMPLAINT     VARCHAR(100),
    DEPARTMENT          VARCHAR(50),
    ATTENDING_PHYSICIAN VARCHAR(80),
    BED_NUMBER          VARCHAR(10),
    HEART_RATE          NUMBER(3,0),
    SYSTOLIC_BP         NUMBER(3,0),
    DIASTOLIC_BP        NUMBER(3,0),
    TEMPERATURE_F       FLOAT,
    O2_SATURATION       FLOAT,
    ARRIVAL_MODE        VARCHAR(20),
    STATUS              VARCHAR(20),
    WAIT_TIME_MINUTES   NUMBER(4,0),
    FACILITY            VARCHAR(60)
);

----------------------------------------------------------------------
-- Seed the source table with 500 realistic ER admissions
----------------------------------------------------------------------
INSERT INTO ER_ADMISSIONS_SOURCE
WITH complaints AS (
    SELECT column1 AS idx, column2 AS complaint, column3 AS dept, column4 AS typical_triage FROM VALUES
        (0,'Chest Pain','Cardiology',2),
        (1,'Shortness of Breath','Pulmonology',2),
        (2,'Abdominal Pain','General',3),
        (3,'Laceration','Trauma',4),
        (4,'Fracture','Orthopedics',3),
        (5,'Fever / Infection','General',3),
        (6,'Stroke Symptoms','Neurology',1),
        (7,'Allergic Reaction','General',2),
        (8,'Back Pain','Orthopedics',4),
        (9,'Head Injury','Trauma',2),
        (10,'Seizure','Neurology',2),
        (11,'Difficulty Breathing','Pulmonology',2),
        (12,'Nausea / Vomiting','General',4),
        (13,'Burn Injury','Trauma',3),
        (14,'Cardiac Arrest','Cardiology',1),
        (15,'Minor Cut / Bruise','General',5),
        (16,'Sprained Ankle','Orthopedics',5),
        (17,'High Blood Pressure','Cardiology',3),
        (18,'Diabetic Emergency','Endocrinology',2),
        (19,'Overdose','Toxicology',1)
),
physicians AS (
    SELECT column1 AS idx, column2 AS name FROM VALUES
        (0,'Dr. Sarah Chen'),(1,'Dr. James Rodriguez'),(2,'Dr. Aisha Patel'),
        (3,'Dr. Michael Kim'),(4,'Dr. Emily Washington'),(5,'Dr. David Okafor'),
        (6,'Dr. Lisa Yamamoto'),(7,'Dr. Robert Singh'),(8,'Dr. Maria Gonzalez'),
        (9,'Dr. Thomas Adebayo'),(10,'Dr. Rachel Hoffman'),(11,'Dr. Kevin Chu')
),
facilities AS (
    SELECT column1 AS idx, column2 AS facility FROM VALUES
        (0,'Metro General Hospital'),
        (1,'St. Mary Regional Medical Center'),
        (2,'University Health System ER'),
        (3,'Coastal Community Hospital')
),
row_gen AS (
    SELECT SEQ4() AS rn FROM TABLE(GENERATOR(ROWCOUNT => 500))
)
SELECT
    UUID_STRING(),
    UUID_STRING(),
    UNIFORM(1, 95, RANDOM()),
    CASE WHEN UNIFORM(0,1,RANDOM()) = 0 THEN 'Male' ELSE 'Female' END,
    DATEADD('minute', -1 * UNIFORM(0, 1440, RANDOM()), CURRENT_TIMESTAMP()),
    c.typical_triage AS TRIAGE_LEVEL,
    CASE c.typical_triage
        WHEN 1 THEN 'Resuscitation' WHEN 2 THEN 'Emergent' WHEN 3 THEN 'Urgent'
        WHEN 4 THEN 'Less Urgent' ELSE 'Non-Urgent' END AS TRIAGE_LABEL,
    c.complaint,
    c.dept,
    p.name,
    'ER-' || LPAD(UNIFORM(1,60,RANDOM())::VARCHAR, 3, '0'),
    UNIFORM(50, 160, RANDOM()),
    UNIFORM(80, 200, RANDOM()),
    UNIFORM(50, 120, RANDOM()),
    ROUND(UNIFORM(966, 1040, RANDOM()) / 10.0, 1),
    ROUND(UNIFORM(850, 1000, RANDOM()) / 10.0, 1),
    CASE MOD(UNIFORM(0,9,RANDOM()),4)
        WHEN 0 THEN 'Walk-in' WHEN 1 THEN 'Ambulance'
        WHEN 2 THEN 'Transfer' ELSE 'Walk-in' END,
    CASE MOD(UNIFORM(0,9,RANDOM()),4)
        WHEN 0 THEN 'Waiting' WHEN 1 THEN 'In Treatment'
        WHEN 2 THEN 'Admitted' ELSE 'Discharged' END,
    CASE
        WHEN c.typical_triage <= 1 THEN UNIFORM(0, 5, RANDOM())
        WHEN c.typical_triage  = 2 THEN UNIFORM(5, 30, RANDOM())
        WHEN c.typical_triage  = 3 THEN UNIFORM(15, 90, RANDOM())
        WHEN c.typical_triage  = 4 THEN UNIFORM(30, 180, RANDOM())
        ELSE UNIFORM(60, 300, RANDOM())
    END,
    f.facility
FROM row_gen rg
JOIN complaints c ON MOD(rg.rn, 20) = c.idx
JOIN physicians p ON MOD(rg.rn, 12) = p.idx
JOIN facilities f ON MOD(rg.rn, 4) = f.idx;

SELECT COUNT(*) AS total_rows, MIN(ADMISSION_TIME) AS earliest, MAX(ADMISSION_TIME) AS latest
FROM ER_ADMISSIONS_SOURCE;
