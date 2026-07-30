-- =============================================================================
-- 08: Clinical Data Model (Synthetic)
-- Star schema simulating TAVR clinical workflow.
-- Joins to real DICOM_CARDIAC_METADATA via PATIENT_ID and STUDY_INSTANCE_UID.
-- 20 patients (10 cardiac + 10 chest CT), 1,058 total rows across 8 tables.
-- =============================================================================

USE DATABASE EW_IMAGING_DB;
USE SCHEMA EXPLORER;

-- ---------------------------------------------------------------------------
-- DIM_PATIENT
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_PATIENT (
    PATIENT_ID          VARCHAR PRIMARY KEY,
    FULL_NAME           VARCHAR,
    DATE_OF_BIRTH       DATE,
    SEX                 VARCHAR(1),
    ENROLLMENT_DATE     DATE,
    HYPERTENSION        BOOLEAN,
    DIABETES            BOOLEAN,
    NYHA_CLASS          VARCHAR(3),
    LVEF_PERCENT        NUMBER(5,1),
    STS_RISK_SCORE      NUMBER(5,2)
);

INSERT INTO DIM_PATIENT VALUES
('AMC-015',           'James Whitfield',    '1934-02-04', 'M', '2024-01-15', TRUE,  FALSE, 'III', 42.5, 5.80),
('AMC-027',           'Patricia Nakamura',  '1926-04-28', 'F', '2024-02-20', TRUE,  TRUE,  'IV',  28.0, 9.20),
('AP-26JK',           'Robert Andersen',    '1956-11-20', 'M', '2024-03-10', FALSE, FALSE, 'II',  55.0, 3.10),
('RIDER-2019259100',  'Michael Chen',       '1961-12-09', 'M', '2024-04-05', TRUE,  TRUE,  'III', 35.0, 7.40),
('RIDER-2266952716',  'Susan Okafor',       '1954-03-21', 'F', '2024-05-12', FALSE, TRUE,  'II',  50.0, 4.20),
('A130302',           'David Petrov',       '1983-10-24', 'M', '2024-06-01', FALSE, FALSE, 'II',  58.0, 2.50),
('A095019',           'Maria Gonzalez',     '1950-11-14', 'M', '2024-07-15', TRUE,  FALSE, 'III', 38.0, 6.80),
('202207',            'Linda Johansson',    '1940-01-02', 'F', '2024-08-20', TRUE,  TRUE,  'III', 32.0, 8.10),
('133417',            'Thomas Wanjiku',     '1942-01-02', 'M', '2024-09-05', FALSE, FALSE, 'II',  52.0, 3.90),
('200119',            'Karen Dubois',       '1944-01-02', 'F', '2024-10-10', TRUE,  FALSE, 'III', 40.0, 5.50),
('RIDER-2416820556',  'Angela Freeman',     '1948-06-15', 'F', '2025-01-10', TRUE,  FALSE, 'III', 38.0, 6.20),
('RIDER-2796673129',  'Harold Bergstrom',   '1952-09-03', 'M', '2025-01-25', TRUE,  TRUE,  'III', 33.0, 7.80),
('RIDER-7701645091',  'George Yamamoto',    '1946-07-22', 'M', '2025-02-14', FALSE, FALSE, 'II',  54.0, 3.40),
('RIDER-1822442188',  'Frank Novak',        '1959-03-18', 'M', '2025-03-01', TRUE,  TRUE,  'IV',  26.0, 9.50),
('RIDER-2522924559',  'Dorothy Reeves',     '1955-12-01', 'F', '2025-03-20', FALSE, TRUE,  'II',  48.0, 4.60),
('RIDER-1836251657',  'Beatrice Lindgren',  '1943-04-11', 'F', '2025-04-05', TRUE,  FALSE, 'III', 40.0, 5.90),
('TCGA-QQ-A5V2',     'Raymond Choi',       '1960-08-29', 'M', '2025-04-22', FALSE, FALSE, 'II',  56.0, 2.80),
('RIDER-2991299498',  'Evelyn Sato',        '1951-01-17', 'F', '2025-05-10', TRUE,  TRUE,  'III', 34.0, 7.10),
('RIDER-2857227961',  'Walter Kowalczyk',   '1957-11-05', 'M', '2025-06-01', FALSE, FALSE, 'II',  52.0, 3.70),
('RIDER-4767464492',  'Irene Delacroix',    '1949-02-20', 'F', '2025-06-18', TRUE,  FALSE, 'III', 41.0, 5.30);

ALTER TABLE DIM_PATIENT MODIFY COLUMN FULL_NAME SET MASKING POLICY PHI_MASK;

-- ---------------------------------------------------------------------------
-- DIM_DEVICE
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_DEVICE (
    DEVICE_ID           VARCHAR PRIMARY KEY,
    MODEL_NAME          VARCHAR,
    VALVE_SIZE_MM       NUMBER(3,0),
    LOT_NUMBER          VARCHAR,
    MANUFACTURING_DATE  DATE,
    EXPIRY_DATE         DATE
);

INSERT INTO DIM_DEVICE VALUES
('DEV-001', 'SAPIEN 3',            23, 'LOT-2024-A001', '2024-01-10', '2027-01-10'),
('DEV-002', 'SAPIEN 3',            26, 'LOT-2024-A002', '2024-01-15', '2027-01-15'),
('DEV-003', 'SAPIEN 3 Ultra',      23, 'LOT-2024-B001', '2024-02-01', '2027-02-01'),
('DEV-004', 'SAPIEN 3 Ultra',      26, 'LOT-2024-B002', '2024-02-10', '2027-02-10'),
('DEV-005', 'SAPIEN 3 Ultra',      29, 'LOT-2024-B003', '2024-03-01', '2027-03-01'),
('DEV-006', 'SAPIEN 3 Ultra PLUS', 20, 'LOT-2024-C001', '2024-03-15', '2027-03-15'),
('DEV-007', 'SAPIEN 3 Ultra PLUS', 23, 'LOT-2024-C002', '2024-04-01', '2027-04-01'),
('DEV-008', 'SAPIEN 3 Ultra PLUS', 26, 'LOT-2024-C003', '2024-04-15', '2027-04-15'),
('DEV-009', 'SAPIEN 3 Ultra PLUS', 29, 'LOT-2024-C004', '2024-05-01', '2027-05-01'),
('DEV-010', 'SAPIEN 3',            20, 'LOT-2024-A003', '2024-05-15', '2027-05-15'),
('DEV-011', 'SAPIEN 3 Ultra',      20, 'LOT-2024-B004', '2024-06-01', '2027-06-01'),
('DEV-012', 'SAPIEN 3 Ultra PLUS', 23, 'LOT-2024-C005', '2024-06-15', '2027-06-15'),
('DEV-013', 'SAPIEN 3',            29, 'LOT-2024-A004', '2024-07-01', '2027-07-01'),
('DEV-014', 'SAPIEN 3 Ultra',      29, 'LOT-2024-B005', '2024-07-15', '2027-07-15'),
('DEV-015', 'SAPIEN 3 Ultra PLUS', 26, 'LOT-2024-C006', '2024-08-01', '2027-08-01');

-- ---------------------------------------------------------------------------
-- DIM_SITE
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_SITE (
    SITE_ID               VARCHAR PRIMARY KEY,
    SITE_NAME             VARCHAR,
    CITY                  VARCHAR,
    STATE                 VARCHAR,
    COUNTRY               VARCHAR,
    IMPLANT_VOLUME_TIER   VARCHAR,
    ACCREDITATION         VARCHAR
);

INSERT INTO DIM_SITE VALUES
('SITE-001', 'Cedars-Sinai Heart Institute',      'Los Angeles',   'CA', 'USA', 'High',   'TVT Registry Center of Excellence'),
('SITE-002', 'Cleveland Clinic',                   'Cleveland',     'OH', 'USA', 'High',   'TVT Registry Center of Excellence'),
('SITE-003', 'Mayo Clinic',                        'Rochester',     'MN', 'USA', 'High',   'TVT Registry Center of Excellence'),
('SITE-004', 'Johns Hopkins Heart Center',         'Baltimore',     'MD', 'USA', 'Medium', 'ACC Chest Pain Center'),
('SITE-005', 'Massachusetts General Hospital',     'Boston',        'MA', 'USA', 'Medium', 'ACC Chest Pain Center'),
('SITE-006', 'Duke Heart Center',                  'Durham',        'NC', 'USA', 'Medium', 'TVT Registry Participant'),
('SITE-007', 'Stony Brook University Hospital',    'Stony Brook',   'NY', 'USA', 'Low',    'TVT Registry Participant'),
('SITE-008', 'Amsterdam UMC',                      'Amsterdam',     NULL, 'Netherlands', 'Medium', 'EAPCI Center');

-- ---------------------------------------------------------------------------
-- DIM_PHYSICIAN
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_PHYSICIAN (
    PHYSICIAN_ID      VARCHAR PRIMARY KEY,
    PHYSICIAN_NAME    VARCHAR,
    SPECIALIZATION    VARCHAR,
    EXPERIENCE_YEARS  NUMBER(3,0),
    SITE_ID           VARCHAR REFERENCES DIM_SITE(SITE_ID)
);

INSERT INTO DIM_PHYSICIAN VALUES
('PHY-001', 'Dr. Sarah Mitchell',    'Interventional Cardiology', 18, 'SITE-001'),
('PHY-002', 'Dr. James Park',        'Interventional Cardiology', 22, 'SITE-002'),
('PHY-003', 'Dr. Emily Rosenberg',   'Structural Heart',         15, 'SITE-003'),
('PHY-004', 'Dr. Raj Patel',         'Interventional Cardiology', 12, 'SITE-004'),
('PHY-005', 'Dr. Lisa Chang',        'Structural Heart',         20, 'SITE-005'),
('PHY-006', 'Dr. Marcus Johnson',    'Interventional Cardiology', 10, 'SITE-006'),
('PHY-007', 'Dr. Anna Kowalski',     'Structural Heart',          8, 'SITE-007'),
('PHY-008', 'Dr. Henrik van Dijk',   'Interventional Cardiology', 25, 'SITE-008'),
('PHY-009', 'Dr. Yuki Tanaka',       'Structural Heart',         14, 'SITE-001'),
('PHY-010', 'Dr. Omar Hassan',       'Interventional Cardiology', 16, 'SITE-002'),
('PHY-011', 'Dr. Catherine Moreau',  'Structural Heart',         11, 'SITE-003'),
('PHY-012', 'Dr. William Torres',    'Interventional Cardiology',  9, 'SITE-004');

ALTER TABLE DIM_PHYSICIAN MODIFY COLUMN PHYSICIAN_NAME SET MASKING POLICY PHI_MASK;

-- ---------------------------------------------------------------------------
-- FACT_PROCEDURE (one TAVR per patient, linked to pre-procedure imaging)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE FACT_PROCEDURE (
    PROCEDURE_ID          VARCHAR PRIMARY KEY,
    PATIENT_ID            VARCHAR REFERENCES DIM_PATIENT(PATIENT_ID),
    STUDY_INSTANCE_UID    VARCHAR,
    DEVICE_ID             VARCHAR REFERENCES DIM_DEVICE(DEVICE_ID),
    SITE_ID               VARCHAR REFERENCES DIM_SITE(SITE_ID),
    PHYSICIAN_ID          VARCHAR REFERENCES DIM_PHYSICIAN(PHYSICIAN_ID),
    PROCEDURE_DATE        DATE,
    PROCEDURE_TYPE        VARCHAR,
    ACCESS_ROUTE          VARCHAR,
    PROCEDURE_DURATION_MIN NUMBER(5,0),
    FLUOROSCOPY_TIME_MIN  NUMBER(5,1),
    OUTCOME               VARCHAR,
    DISCHARGE_DATE        DATE,
    LOS_DAYS              NUMBER(3,0)
);

INSERT INTO FACT_PROCEDURE VALUES
('PROC-001', 'AMC-015',          '1.3.6.1.4.1.14519.5.2.1.4334.1501.119531128953610472040332469413', 'DEV-007', 'SITE-008', 'PHY-008', '2024-03-15', 'TAVR', 'Transfemoral',  85, 12.3, 'Successful',  '2024-03-19', 4),
('PROC-002', 'AMC-027',          '1.3.6.1.4.1.14519.5.2.1.4334.1501.124191703163061365051974545663', 'DEV-004', 'SITE-001', 'PHY-001', '2024-04-22', 'TAVR', 'Transfemoral', 110, 18.7, 'Complication', '2024-04-29', 7),
('PROC-003', 'AP-26JK',          '1.3.6.1.4.1.14519.5.2.1.312962097772585042533562177679382394774',  'DEV-008', 'SITE-002', 'PHY-002', '2024-05-10', 'TAVR', 'Transfemoral',  72, 10.1, 'Successful',  '2024-05-13', 3),
('PROC-004', 'RIDER-2019259100', '1.3.6.1.4.1.9328.50.17.176980532084133607879538329005269847241',   'DEV-003', 'SITE-003', 'PHY-003', '2024-06-05', 'TAVR', 'Transfemoral',  95, 14.5, 'Complication', '2024-06-12', 7),
('PROC-005', 'RIDER-2266952716', '1.3.6.1.4.1.9328.50.17.199036456548697345934535648816437802445',   'DEV-009', 'SITE-004', 'PHY-004', '2024-07-18', 'TAVR', 'Transapical',   130, 22.0, 'Successful',  '2024-07-23', 5),
('PROC-006', 'A130302',          '1.3.6.1.4.1.14519.5.2.1.99.1071.18502340383544479609937077864159',  'DEV-006', 'SITE-005', 'PHY-005', '2024-08-02', 'TAVR', 'Transfemoral',  78, 11.0, 'Successful',  '2024-08-05', 3),
('PROC-007', 'A095019',          '1.3.6.1.4.1.14519.5.2.1.99.1071.24959495743882045369807958755791',  'DEV-014', 'SITE-006', 'PHY-006', '2024-09-14', 'TAVR', 'Transfemoral',  88, 13.2, 'Complication', '2024-09-21', 7),
('PROC-008', '202207',           '1.3.6.1.4.1.14519.5.2.1.7009.9004.260246832117421987607246525149',  'DEV-012', 'SITE-007', 'PHY-007', '2024-10-25', 'TAVR', 'Transapical',   145, 25.0, 'Successful',  '2024-10-31', 6),
('PROC-009', '133417',           '1.2.840.113654.2.55.41488091813007895142306681258807080711',         'DEV-001', 'SITE-001', 'PHY-009', '2024-11-08', 'TAVR', 'Transfemoral',  70, 9.8,  'Successful',  '2024-11-11', 3),
('PROC-010', '200119',           '1.3.6.1.4.1.14519.5.2.1.7009.9004.606545977234644424118044434831',  'DEV-005', 'SITE-003', 'PHY-011', '2024-12-03', 'TAVR', 'Transfemoral',  92, 15.1, 'Successful',  '2024-12-07', 4),
('PROC-011', 'RIDER-2416820556', '1.3.6.1.4.1.9328.50.17.160615776127383877801997638127387345147', 'DEV-002', 'SITE-001', 'PHY-001', '2025-02-12', 'TAVR', 'Transfemoral',  80, 11.5, 'Successful',  '2025-02-15', 3),
('PROC-012', 'RIDER-2796673129', '1.3.6.1.4.1.9328.50.17.222977663000802088113199851616615413305', 'DEV-008', 'SITE-002', 'PHY-010', '2025-02-28', 'TAVR', 'Transfemoral', 105, 16.8, 'Complication', '2025-03-06', 6),
('PROC-013', 'RIDER-7701645091', '1.3.6.1.4.1.9328.50.17.116294505685643189132968343930901708714', 'DEV-005', 'SITE-003', 'PHY-003', '2025-03-18', 'TAVR', 'Transfemoral',  68, 9.2,  'Successful',  '2025-03-21', 3),
('PROC-014', 'RIDER-1822442188', '1.3.6.1.4.1.9328.50.17.143762991526078790342095275146292315148', 'DEV-009', 'SITE-004', 'PHY-004', '2025-04-02', 'TAVR', 'Transapical',  140, 24.3, 'Complication', '2025-04-10', 8),
('PROC-015', 'RIDER-2522924559', '1.3.6.1.4.1.9328.50.17.152749821065072200852832402799384967030', 'DEV-006', 'SITE-005', 'PHY-005', '2025-04-20', 'TAVR', 'Transfemoral',  75, 10.8, 'Successful',  '2025-04-23', 3),
('PROC-016', 'RIDER-1836251657', '1.3.6.1.4.1.9328.50.17.295609911680419974621153095585688074293', 'DEV-015', 'SITE-006', 'PHY-006', '2025-05-08', 'TAVR', 'Transfemoral',  90, 13.7, 'Successful',  '2025-05-12', 4),
('PROC-017', 'TCGA-QQ-A5V2',    '1.3.6.1.4.1.14519.5.2.1.3023.4024.194945893650500105451818109994', 'DEV-011', 'SITE-007', 'PHY-007', '2025-05-25', 'TAVR', 'Transfemoral',  82, 11.9, 'Successful',  '2025-05-28', 3),
('PROC-018', 'RIDER-2991299498', '1.3.6.1.4.1.9328.50.17.43984287026108946231425463342480208442',  'DEV-003', 'SITE-001', 'PHY-009', '2025-06-12', 'TAVR', 'Transfemoral',  98, 15.0, 'Complication', '2025-06-19', 7),
('PROC-019', 'RIDER-2857227961', '1.3.6.1.4.1.9328.50.17.194711301017391069744468415967714763022', 'DEV-013', 'SITE-002', 'PHY-002', '2025-07-01', 'TAVR', 'Transfemoral',  72, 10.0, 'Successful',  '2025-07-04', 3),
('PROC-020', 'RIDER-4767464492', '1.3.6.1.4.1.9328.50.17.34002176343373687463009592400896850334',  'DEV-007', 'SITE-003', 'PHY-011', '2025-07-18', 'TAVR', 'Transfemoral',  88, 12.6, 'Successful',  '2025-07-21', 3);

-- ---------------------------------------------------------------------------
-- FACT_DEVICE_TELEMETRY (post-implant hemodynamic readings)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE FACT_DEVICE_TELEMETRY (
    TELEMETRY_ID              VARCHAR PRIMARY KEY,
    PATIENT_ID                VARCHAR REFERENCES DIM_PATIENT(PATIENT_ID),
    DEVICE_ID                 VARCHAR REFERENCES DIM_DEVICE(DEVICE_ID),
    READING_DATE              DATE,
    MEAN_GRADIENT_MMHG        NUMBER(5,1),
    PEAK_GRADIENT_MMHG        NUMBER(5,1),
    VALVE_AREA_CM2            NUMBER(4,2),
    HEART_RATE_BPM            NUMBER(4,0),
    PARAVALVULAR_LEAK_GRADE   VARCHAR
);

-- Generate ~50 readings per patient (20 patients x 50 = 1000 rows)
-- Monthly readings for 12 months plus some weekly readings early post-op
INSERT INTO FACT_DEVICE_TELEMETRY
WITH patients AS (
    SELECT p.PATIENT_ID, fp.DEVICE_ID, fp.PROCEDURE_DATE,
           ROW_NUMBER() OVER (ORDER BY p.PATIENT_ID) as pn
    FROM DIM_PATIENT p JOIN FACT_PROCEDURE fp ON p.PATIENT_ID = fp.PATIENT_ID
),
time_series AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) as rn
    FROM TABLE(GENERATOR(ROWCOUNT => 50))
)
SELECT
    'TEL-' || LPAD(((p.pn - 1) * 50 + t.rn)::VARCHAR, 4, '0'),
    p.PATIENT_ID,
    p.DEVICE_ID,
    DATEADD('day', CASE WHEN t.rn <= 14 THEN t.rn * 2 ELSE 28 + (t.rn - 14) * 7 END, p.PROCEDURE_DATE),
    ROUND(8.0 + UNIFORM(0::FLOAT, 6::FLOAT, RANDOM()) + CASE WHEN t.rn <= 7 THEN 4.0 ELSE 0 END, 1),
    ROUND(14.0 + UNIFORM(0::FLOAT, 10::FLOAT, RANDOM()) + CASE WHEN t.rn <= 7 THEN 6.0 ELSE 0 END, 1),
    ROUND(1.6 + UNIFORM(0::FLOAT, 0.8::FLOAT, RANDOM()) - CASE WHEN t.rn <= 7 THEN 0.3 ELSE 0 END, 2),
    ROUND(65 + UNIFORM(0::FLOAT, 25::FLOAT, RANDOM())),
    CASE
        WHEN p.pn IN (2, 4, 7) AND t.rn > 10 THEN
            CASE WHEN UNIFORM(0::FLOAT, 1::FLOAT, RANDOM()) > 0.7 THEN 'Mild' ELSE 'Trace' END
        WHEN p.pn IN (2, 4, 7) AND t.rn <= 10 THEN 'None'
        ELSE 'None'
    END
FROM patients p CROSS JOIN time_series t
WHERE p.pn <= 10;

INSERT INTO FACT_DEVICE_TELEMETRY
WITH patients AS (
    SELECT p.PATIENT_ID, fp.DEVICE_ID, fp.PROCEDURE_DATE,
           ROW_NUMBER() OVER (ORDER BY p.PATIENT_ID) + 10 as pn
    FROM DIM_PATIENT p JOIN FACT_PROCEDURE fp ON p.PATIENT_ID = fp.PATIENT_ID
    WHERE p.PATIENT_ID IN ('RIDER-2416820556','RIDER-2796673129','RIDER-7701645091','RIDER-1822442188','RIDER-2522924559','RIDER-1836251657','TCGA-QQ-A5V2','RIDER-2991299498','RIDER-2857227961','RIDER-4767464492')
),
time_series AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) as rn
    FROM TABLE(GENERATOR(ROWCOUNT => 50))
)
SELECT
    'TEL-' || LPAD(((p.pn - 1) * 50 + t.rn)::VARCHAR, 4, '0'),
    p.PATIENT_ID,
    p.DEVICE_ID,
    DATEADD('day', CASE WHEN t.rn <= 14 THEN t.rn * 2 ELSE 28 + (t.rn - 14) * 7 END, p.PROCEDURE_DATE),
    ROUND(8.0 + UNIFORM(0::FLOAT, 6::FLOAT, RANDOM()) + CASE WHEN t.rn <= 7 THEN 4.0 ELSE 0 END, 1),
    ROUND(14.0 + UNIFORM(0::FLOAT, 10::FLOAT, RANDOM()) + CASE WHEN t.rn <= 7 THEN 6.0 ELSE 0 END, 1),
    ROUND(1.6 + UNIFORM(0::FLOAT, 0.8::FLOAT, RANDOM()) - CASE WHEN t.rn <= 7 THEN 0.3 ELSE 0 END, 2),
    ROUND(65 + UNIFORM(0::FLOAT, 25::FLOAT, RANDOM())),
    CASE
        WHEN p.pn IN (12, 14, 18) AND t.rn > 10 THEN
            CASE WHEN UNIFORM(0::FLOAT, 1::FLOAT, RANDOM()) > 0.7 THEN 'Mild' ELSE 'Trace' END
        WHEN p.pn IN (12, 14, 18) AND t.rn <= 10 THEN 'None'
        ELSE 'None'
    END
FROM patients p CROSS JOIN time_series t;

-- ---------------------------------------------------------------------------
-- FACT_ADVERSE_EVENT
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE FACT_ADVERSE_EVENT (
    EVENT_ID          VARCHAR PRIMARY KEY,
    PATIENT_ID        VARCHAR REFERENCES DIM_PATIENT(PATIENT_ID),
    DEVICE_ID         VARCHAR REFERENCES DIM_DEVICE(DEVICE_ID),
    EVENT_DATE        DATE,
    EVENT_TYPE        VARCHAR,
    SEVERITY          VARCHAR,
    MDR_REPORTABLE    BOOLEAN,
    RESOLUTION        VARCHAR
);

INSERT INTO FACT_ADVERSE_EVENT VALUES
('AE-001', 'AMC-027',          'DEV-004', '2024-04-23', 'Paravalvular Leak',       'Major',            TRUE,  'Managed medically, stable at 6 months'),
('AE-002', 'AMC-027',          'DEV-004', '2024-04-25', 'Conduction Disturbance',  'Major',            TRUE,  'Permanent pacemaker implanted day 3'),
('AE-003', 'RIDER-2019259100', 'DEV-003', '2024-06-06', 'Vascular Complication',   'Minor',            FALSE, 'Access site hematoma, resolved conservatively'),
('AE-004', 'RIDER-2019259100', 'DEV-003', '2024-06-08', 'Paravalvular Leak',       'Minor',            FALSE, 'Trace PVL, hemodynamically insignificant'),
('AE-005', 'A095019',          'DEV-014', '2024-09-15', 'Conduction Disturbance',  'Major',            TRUE,  'New LBBB, monitoring with temporary pacemaker'),
('AE-006', 'A095019',          'DEV-014', '2024-09-18', 'Stroke',                  'Life-Threatening', TRUE,  'Minor ischemic event, full neurological recovery'),
('AE-007', '202207',           'DEV-012', '2024-11-15', 'Endocarditis',            'Life-Threatening', TRUE,  'IV antibiotics 6 weeks, valve function preserved'),
('AE-008', 'RIDER-2266952716', 'DEV-009', '2024-08-02', 'Vascular Complication',   'Minor',            FALSE, 'Femoral pseudoaneurysm, thrombin injection'),
('AE-009', 'RIDER-2796673129', 'DEV-008', '2025-03-01', 'Conduction Disturbance',  'Major',            TRUE,  'Complete heart block, permanent pacemaker implanted day 2'),
('AE-010', 'RIDER-1822442188', 'DEV-009', '2025-04-03', 'Vascular Complication',   'Major',            TRUE,  'Retroperitoneal hemorrhage, surgical repair required'),
('AE-011', 'RIDER-1822442188', 'DEV-009', '2025-04-05', 'Acute Kidney Injury',     'Minor',            FALSE, 'Stage 1 AKI from contrast, resolved with hydration by day 5'),
('AE-012', 'RIDER-2991299498', 'DEV-003', '2025-06-13', 'Paravalvular Leak',       'Minor',            FALSE, 'Trace PVL on echo, hemodynamically insignificant'),
('AE-013', 'RIDER-2991299498', 'DEV-003', '2025-06-15', 'Atrial Fibrillation',     'Minor',            FALSE, 'New-onset AF, rate-controlled with beta blocker');

-- ---------------------------------------------------------------------------
-- FACT_CAPA
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE FACT_CAPA (
    CAPA_ID           VARCHAR PRIMARY KEY,
    ADVERSE_EVENT_ID  VARCHAR REFERENCES FACT_ADVERSE_EVENT(EVENT_ID),
    CAPA_TYPE         VARCHAR,
    DESCRIPTION       VARCHAR,
    STATUS            VARCHAR,
    INITIATED_DATE    DATE,
    CLOSED_DATE       DATE
);

INSERT INTO FACT_CAPA VALUES
('CAPA-001', 'AE-001', 'Corrective',  'Investigate sizing protocol deviation at SITE-001 for SAPIEN 3 Ultra 26mm. Root cause: CT annulus measurement used non-gated phase. Action: mandate gated CT for all pre-TAVR sizing.', 'Closed', '2024-05-01', '2024-07-15'),
('CAPA-002', 'AE-006', 'Preventive',  'Review anticoagulation protocol at SITE-006 for transapical access cases. Finding: INR was subtherapeutic at time of procedure. Action: standardize pre-procedure INR check within 4 hours.', 'Closed', '2024-10-01', '2024-12-20'),
('CAPA-003', 'AE-007', 'Corrective',  'Investigate early endocarditis case at SITE-007 for SAPIEN 3 Ultra PLUS 23mm. Suspected contamination during implant. Action: review sterile field protocols and antibiotic prophylaxis timing.', 'Open', '2024-12-01', NULL),
('CAPA-004', 'AE-010', 'Corrective',  'Investigate retroperitoneal hemorrhage at SITE-004 for transapical TAVR with SAPIEN 3 Ultra PLUS 29mm. Root cause: vessel calcification underestimated on pre-procedure CT. Action: mandate CT calcium scoring of iliac/femoral vessels for all transapical candidates.', 'Open', '2025-04-15', NULL),
('CAPA-005', 'AE-009', 'Preventive',  'Review conduction disturbance rate at SITE-002 for SAPIEN 3 Ultra PLUS 26mm. Third pacemaker implant in 12 months at this site. Action: revise implant depth protocol and mandate intraoperative rapid pacing threshold assessment.', 'Open', '2025-03-15', NULL);
