-- ============================================================
-- IMPORTANT: Run sql/deploy_medgemma.sql FIRST to create the
-- compute pool, secrets, EAI, and deploy MedGemma to SPCS.
-- Then update the variables below with your environment values.
-- ============================================================

-- >>> CONFIGURE THESE VARIABLES FOR YOUR ENVIRONMENT <<<
SET MEDGEMMA_ENDPOINT = 'https://<your-spcs-endpoint>.snowflakecomputing.app/__call__';  -- From: SHOW ENDPOINTS IN SERVICE <your_service>
SET MY_DB = 'DEMO_DB';             -- Your database name
SET MY_SCHEMA = 'HIMSS_DEMO';      -- Your schema name
SET MY_WAREHOUSE = 'DEMO_BUILD_WH';  -- Your warehouse name
SET IMAGE_STAGE = 'MEDGEMMA_DEMO.PUBLIC.ECG_STAGE';  -- Fully-qualified stage for medical images

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE IDENTIFIER($MY_WAREHOUSE);
USE DATABASE IDENTIFIER($MY_DB);

CREATE SCHEMA IF NOT EXISTS IDENTIFIER($MY_SCHEMA);
USE SCHEMA IDENTIFIER($MY_SCHEMA);

CREATE STAGE IF NOT EXISTS SEMANTIC_STAGE;

-- ============================================================
-- PATIENTS (3 curated cardiology patients)
-- ============================================================
CREATE OR REPLACE TABLE PATIENTS (
    PATIENT_ID        VARCHAR(36) PRIMARY KEY,
    FIRST_NAME        VARCHAR(50),
    LAST_NAME         VARCHAR(50),
    DATE_OF_BIRTH     DATE,
    GENDER            VARCHAR(1),
    RACE              VARCHAR(30),
    ETHNICITY         VARCHAR(30),
    ADDRESS           VARCHAR(100),
    CITY              VARCHAR(50),
    STATE             VARCHAR(30),
    ZIP               VARCHAR(10),
    PHONE             VARCHAR(20),
    EMERGENCY_CONTACT VARCHAR(100),
    INSURANCE_PROVIDER VARCHAR(50),
    INSURANCE_ID      VARCHAR(30),
    PRIMARY_PHYSICIAN VARCHAR(80),
    BLOOD_TYPE        VARCHAR(5),
    HEIGHT_CM         NUMBER(5,1),
    WEIGHT_KG         NUMBER(5,1),
    BMI               NUMBER(4,1)
);

INSERT INTO PATIENTS VALUES
('P-1001', 'Maria', 'Santos', '1957-03-15', 'F', 'Hispanic', 'Hispanic',
 '742 Elm Street', 'Chicago', 'Illinois', '60614', '312-555-0142',
 'Carlos Santos (Husband) 312-555-0143', 'Blue Cross Blue Shield', 'BCBS-88421',
 'Dr. Sarah Chen', 'A+', 162.5, 78.2, 29.6),

('P-1002', 'James', 'Wilson', '1953-11-28', 'M', 'White', 'Non-Hispanic',
 '1580 Oak Avenue', 'Naperville', 'Illinois', '60540', '630-555-0278',
 'Linda Wilson (Wife) 630-555-0279', 'Aetna', 'AET-55903',
 'Dr. Sarah Chen', 'O+', 180.3, 102.1, 31.4),

('P-1003', 'Aisha', 'Rahman', '1968-07-22', 'F', 'Asian', 'Non-Hispanic',
 '309 Maple Drive', 'Evanston', 'Illinois', '60201', '847-555-0391',
 'Tariq Rahman (Son) 847-555-0392', 'UnitedHealthcare', 'UHC-77210',
 'Dr. Michael Torres', 'B+', 157.0, 62.8, 25.5);

-- ============================================================
-- CONDITIONS
-- Patient P-1001 (Maria Santos): Post-MI, Hypertension, Diabetes
-- Patient P-1002 (James Wilson): Active MI event, Afib, Hyperlipidemia
-- Patient P-1003 (Aisha Rahman): Abnormal heartbeat (SVT), Hypertension
-- ============================================================
CREATE OR REPLACE TABLE CONDITIONS (
    CONDITION_ID       VARCHAR(36) PRIMARY KEY,
    PATIENT_ID         VARCHAR(36) REFERENCES PATIENTS(PATIENT_ID),
    CONDITION_CODE     VARCHAR(10),
    CONDITION_NAME     VARCHAR(200),
    ONSET_DATE         DATE,
    RESOLUTION_DATE    DATE,
    STATUS             VARCHAR(20),
    SEVERITY           VARCHAR(20),
    TREATING_PHYSICIAN VARCHAR(80)
);

INSERT INTO CONDITIONS VALUES
-- Maria Santos: Post-MI recovery, chronic conditions
('C-2001', 'P-1001', 'I21.0',  'ST elevation myocardial infarction involving left anterior descending coronary artery', '2025-06-12', '2025-07-01', 'resolved', 'severe', 'Dr. Sarah Chen'),
('C-2002', 'P-1001', 'I25.10', 'Atherosclerotic heart disease of native coronary artery', '2025-06-12', NULL, 'chronic', 'moderate', 'Dr. Sarah Chen'),
('C-2003', 'P-1001', 'I10',    'Essential hypertension', '2018-03-10', NULL, 'chronic', 'moderate', 'Dr. Sarah Chen'),
('C-2004', 'P-1001', 'E11.9',  'Type 2 diabetes mellitus without complications', '2019-09-22', NULL, 'chronic', 'mild', 'Dr. Sarah Chen'),
('C-2005', 'P-1001', 'E78.5',  'Hyperlipidemia, unspecified', '2018-03-10', NULL, 'chronic', 'mild', 'Dr. Sarah Chen'),
('C-2006', 'P-1001', 'I25.2',  'Old myocardial infarction', '2025-07-01', NULL, 'chronic', 'moderate', 'Dr. Sarah Chen'),

-- James Wilson: Acute MI presentation, Afib
('C-2010', 'P-1002', 'I21.1',  'ST elevation myocardial infarction involving right coronary artery', '2026-02-25', NULL, 'active', 'severe', 'Dr. Sarah Chen'),
('C-2011', 'P-1002', 'I48.0',  'Paroxysmal atrial fibrillation', '2024-04-15', NULL, 'chronic', 'moderate', 'Dr. Sarah Chen'),
('C-2012', 'P-1002', 'I10',    'Essential hypertension', '2015-08-20', NULL, 'chronic', 'moderate', 'Dr. Sarah Chen'),
('C-2013', 'P-1002', 'E78.0',  'Pure hypercholesterolemia', '2016-01-12', NULL, 'chronic', 'moderate', 'Dr. Sarah Chen'),
('C-2014', 'P-1002', 'E11.65', 'Type 2 diabetes mellitus with hyperglycemia', '2020-03-05', NULL, 'chronic', 'moderate', 'Dr. Sarah Chen'),
('C-2015', 'P-1002', 'I50.9',  'Heart failure, unspecified', '2026-02-25', NULL, 'active', 'moderate', 'Dr. Sarah Chen'),

-- Aisha Rahman: SVT / abnormal heartbeat, well controlled
('C-2020', 'P-1003', 'I47.1',  'Supraventricular tachycardia', '2024-11-03', NULL, 'chronic', 'moderate', 'Dr. Michael Torres'),
('C-2021', 'P-1003', 'I10',    'Essential hypertension', '2021-05-14', NULL, 'chronic', 'mild', 'Dr. Michael Torres'),
('C-2022', 'P-1003', 'I49.9',  'Cardiac arrhythmia, unspecified', '2024-11-03', NULL, 'chronic', 'moderate', 'Dr. Michael Torres'),
('C-2023', 'P-1003', 'R00.0',  'Tachycardia, unspecified', '2024-10-15', '2024-11-03', 'resolved', 'moderate', 'Dr. Michael Torres');

-- ============================================================
-- MEDICATIONS
-- ============================================================
CREATE OR REPLACE TABLE MEDICATIONS (
    MEDICATION_ID        VARCHAR(36) PRIMARY KEY,
    PATIENT_ID           VARCHAR(36) REFERENCES PATIENTS(PATIENT_ID),
    MEDICATION_NAME      VARCHAR(100),
    DOSAGE               VARCHAR(50),
    FREQUENCY            VARCHAR(50),
    ROUTE                VARCHAR(20),
    PRESCRIBING_PHYSICIAN VARCHAR(80),
    START_DATE           DATE,
    END_DATE             DATE,
    STATUS               VARCHAR(20),
    MEDICATION_CLASS     VARCHAR(50)
);

INSERT INTO MEDICATIONS VALUES
-- Maria Santos: Post-MI regimen
('M-3001', 'P-1001', 'Aspirin', '81 mg', 'Once daily', 'Oral', 'Dr. Sarah Chen', '2025-06-12', NULL, 'active', 'Antiplatelet'),
('M-3002', 'P-1001', 'Clopidogrel (Plavix)', '75 mg', 'Once daily', 'Oral', 'Dr. Sarah Chen', '2025-06-12', NULL, 'active', 'Antiplatelet'),
('M-3003', 'P-1001', 'Metoprolol Succinate', '50 mg', 'Once daily', 'Oral', 'Dr. Sarah Chen', '2025-06-15', NULL, 'active', 'Beta Blocker'),
('M-3004', 'P-1001', 'Atorvastatin (Lipitor)', '80 mg', 'Once daily at bedtime', 'Oral', 'Dr. Sarah Chen', '2025-06-12', NULL, 'active', 'Statin'),
('M-3005', 'P-1001', 'Lisinopril', '20 mg', 'Once daily', 'Oral', 'Dr. Sarah Chen', '2018-03-10', NULL, 'active', 'ACE Inhibitor'),
('M-3006', 'P-1001', 'Metformin', '1000 mg', 'Twice daily', 'Oral', 'Dr. Sarah Chen', '2019-09-22', NULL, 'active', 'Antidiabetic'),
('M-3007', 'P-1001', 'Nitroglycerin SL', '0.4 mg', 'As needed for chest pain', 'Sublingual', 'Dr. Sarah Chen', '2025-06-12', NULL, 'active', 'Nitrate'),

-- James Wilson: Acute MI + Afib management
('M-3010', 'P-1002', 'Heparin IV', '18 units/kg/hr', 'Continuous infusion', 'IV', 'Dr. Sarah Chen', '2026-02-25', NULL, 'active', 'Anticoagulant'),
('M-3011', 'P-1002', 'Aspirin', '325 mg', 'Once (loading)', 'Oral', 'Dr. Sarah Chen', '2026-02-25', NULL, 'active', 'Antiplatelet'),
('M-3012', 'P-1002', 'Ticagrelor (Brilinta)', '180 mg', 'Once (loading)', 'Oral', 'Dr. Sarah Chen', '2026-02-25', NULL, 'active', 'Antiplatelet'),
('M-3013', 'P-1002', 'Metoprolol Tartrate', '25 mg', 'Every 6 hours', 'IV', 'Dr. Sarah Chen', '2026-02-25', NULL, 'active', 'Beta Blocker'),
('M-3014', 'P-1002', 'Atorvastatin (Lipitor)', '80 mg', 'Once daily', 'Oral', 'Dr. Sarah Chen', '2016-01-12', NULL, 'active', 'Statin'),
('M-3015', 'P-1002', 'Warfarin', '5 mg', 'Once daily', 'Oral', 'Dr. Sarah Chen', '2024-04-20', NULL, 'active', 'Anticoagulant'),
('M-3016', 'P-1002', 'Morphine', '4 mg', 'Every 4 hours PRN', 'IV', 'Dr. Sarah Chen', '2026-02-25', NULL, 'active', 'Opioid Analgesic'),
('M-3017', 'P-1002', 'Lisinopril', '10 mg', 'Once daily', 'Oral', 'Dr. Sarah Chen', '2015-08-20', NULL, 'active', 'ACE Inhibitor'),

-- Aisha Rahman: SVT / arrhythmia management
('M-3020', 'P-1003', 'Metoprolol Succinate', '100 mg', 'Once daily', 'Oral', 'Dr. Michael Torres', '2024-11-03', NULL, 'active', 'Beta Blocker'),
('M-3021', 'P-1003', 'Diltiazem ER', '180 mg', 'Once daily', 'Oral', 'Dr. Michael Torres', '2024-11-10', NULL, 'active', 'Calcium Channel Blocker'),
('M-3022', 'P-1003', 'Lisinopril', '10 mg', 'Once daily', 'Oral', 'Dr. Michael Torres', '2021-05-14', NULL, 'active', 'ACE Inhibitor'),
('M-3023', 'P-1003', 'Flecainide', '100 mg', 'Twice daily', 'Oral', 'Dr. Michael Torres', '2024-12-01', NULL, 'active', 'Antiarrhythmic');

-- ============================================================
-- VITALS (time series, ~5-8 readings per patient)
-- ============================================================
CREATE OR REPLACE TABLE VITALS (
    VITAL_ID       VARCHAR(36) PRIMARY KEY,
    PATIENT_ID     VARCHAR(36) REFERENCES PATIENTS(PATIENT_ID),
    RECORDED_DATE  TIMESTAMP_NTZ,
    HEART_RATE     NUMBER(3),
    SYSTOLIC_BP    NUMBER(3),
    DIASTOLIC_BP   NUMBER(3),
    RESPIRATORY_RATE NUMBER(2),
    TEMPERATURE_F  NUMBER(4,1),
    SPO2           NUMBER(3),
    NOTES          VARCHAR(200)
);

INSERT INTO VITALS VALUES
-- Maria Santos: Stable post-MI recovery
('V-4001', 'P-1001', '2025-06-12 08:15:00', 110, 168, 95, 22, 98.8, 94, 'Presenting with chest pain - acute MI'),
('V-4002', 'P-1001', '2025-06-13 06:00:00', 88, 140, 82, 18, 98.2, 97, 'Post-PCI day 1, hemodynamically stable'),
('V-4003', 'P-1001', '2025-06-15 10:30:00', 78, 132, 78, 16, 98.4, 98, 'Discharge vitals'),
('V-4004', 'P-1001', '2025-09-10 14:00:00', 72, 128, 76, 16, 98.6, 98, '3-month follow-up, well controlled'),
('V-4005', 'P-1001', '2025-12-15 09:30:00', 70, 126, 74, 16, 98.4, 99, '6-month follow-up, excellent progress'),
('V-4006', 'P-1001', '2026-02-20 11:00:00', 68, 124, 72, 15, 98.5, 99, 'Routine cardiology visit, stable'),

-- James Wilson: Acute MI presentation, deteriorating
('V-4010', 'P-1002', '2026-02-25 02:30:00', 112, 92, 58, 24, 99.1, 91, 'ER arrival - crushing chest pain, diaphoretic'),
('V-4011', 'P-1002', '2026-02-25 03:15:00', 105, 98, 62, 22, 98.9, 93, 'Post initial stabilization'),
('V-4012', 'P-1002', '2026-02-25 06:00:00', 96, 108, 68, 20, 98.6, 95, 'Post-cath, stent placed in RCA'),
('V-4013', 'P-1002', '2026-02-25 14:00:00', 88, 112, 70, 18, 98.4, 96, 'ICU - improving on pressors'),
('V-4014', 'P-1002', '2026-02-26 06:00:00', 82, 118, 72, 17, 98.2, 97, 'ICU day 2, weaning pressors'),
('V-4015', 'P-1002', '2026-02-27 08:00:00', 78, 122, 74, 16, 98.4, 97, 'Transfer to telemetry floor'),
('V-4016', 'P-1002', '2026-02-28 07:00:00', 76, 124, 76, 16, 98.6, 98, 'Current vitals - stable'),

-- Aisha Rahman: SVT episodes, mostly controlled
('V-4020', 'P-1003', '2024-10-15 16:45:00', 168, 150, 90, 20, 98.8, 97, 'ER visit - palpitations, SVT on monitor'),
('V-4021', 'P-1003', '2024-10-15 18:00:00', 88, 128, 78, 16, 98.4, 99, 'Post-adenosine, converted to NSR'),
('V-4022', 'P-1003', '2024-11-03 10:00:00', 92, 130, 80, 16, 98.6, 98, 'Cardiology consult - mild tachycardia'),
('V-4023', 'P-1003', '2025-02-12 09:00:00', 76, 122, 76, 15, 98.4, 99, 'Follow-up - well controlled on meds'),
('V-4024', 'P-1003', '2025-08-20 10:30:00', 78, 120, 74, 15, 98.5, 99, 'Routine follow-up'),
('V-4025', 'P-1003', '2026-01-15 14:00:00', 82, 124, 76, 16, 98.4, 99, 'Mild tachycardia, otherwise stable'),
('V-4026', 'P-1003', '2026-02-26 09:00:00', 74, 118, 72, 15, 98.6, 99, 'Current visit - excellent control');

-- ============================================================
-- ENCOUNTERS
-- ============================================================
CREATE OR REPLACE TABLE ENCOUNTERS (
    ENCOUNTER_ID          VARCHAR(36) PRIMARY KEY,
    PATIENT_ID            VARCHAR(36) REFERENCES PATIENTS(PATIENT_ID),
    ENCOUNTER_DATE        DATE,
    ENCOUNTER_TYPE        VARCHAR(30),
    DEPARTMENT            VARCHAR(50),
    PROVIDER_NAME         VARCHAR(80),
    CHIEF_COMPLAINT       VARCHAR(200),
    DIAGNOSIS_CODE        VARCHAR(10),
    DIAGNOSIS_NAME        VARCHAR(200),
    DISCHARGE_DISPOSITION VARCHAR(50),
    NOTES                 VARCHAR(500)
);

INSERT INTO ENCOUNTERS VALUES
-- Maria Santos
('E-5001', 'P-1001', '2025-06-12', 'Emergency', 'Emergency Department', 'Dr. Sarah Chen',
 'Crushing substernal chest pain radiating to left arm x 2 hours',
 'I21.0', 'STEMI - LAD', 'Admitted to CCU',
 'ECG showed ST elevation V1-V4. Troponin elevated at 2.8. Emergent cath with DES to LAD.'),
('E-5002', 'P-1001', '2025-06-15', 'Inpatient Discharge', 'Cardiac Care Unit', 'Dr. Sarah Chen',
 'Post-STEMI recovery', 'I25.10', 'Atherosclerotic heart disease',
 'Discharged home with cardiac rehab referral',
 'Uneventful recovery. EF 45% on echo. Dual antiplatelet therapy initiated.'),
('E-5003', 'P-1001', '2025-09-10', 'Outpatient', 'Cardiology Clinic', 'Dr. Sarah Chen',
 '3-month post-MI follow-up', 'I25.2', 'Old myocardial infarction',
 NULL, 'Doing well in cardiac rehab. EF improved to 50%. Continue current meds.'),
('E-5004', 'P-1001', '2026-02-20', 'Outpatient', 'Cardiology Clinic', 'Dr. Sarah Chen',
 'Routine cardiology follow-up', 'I25.2', 'Old myocardial infarction',
 NULL, 'Stable. ECG shows old MI changes. Continue current regimen. Next visit in 6 months.'),

-- James Wilson
('E-5010', 'P-1002', '2026-02-25', 'Emergency', 'Emergency Department', 'Dr. Sarah Chen',
 'Severe chest pain, onset 1 hour ago, associated with nausea and diaphoresis',
 'I21.1', 'STEMI - RCA', 'Admitted to CCU',
 'ECG showed ST elevation in II, III, aVF. Troponin 5.2. Emergent PCI with DES to RCA. Complicated by transient hypotension.'),
('E-5011', 'P-1002', '2026-02-26', 'Inpatient', 'Cardiac ICU', 'Dr. Sarah Chen',
 'Post-STEMI monitoring', 'I21.1', 'STEMI - RCA', NULL,
 'Hemodynamically improving. Afib with RVR overnight, rate controlled with IV metoprolol. Echo shows EF 35%.'),
('E-5012', 'P-1002', '2026-02-27', 'Inpatient', 'Telemetry', 'Dr. Sarah Chen',
 'Step down from ICU', 'I21.1', 'STEMI - RCA', NULL,
 'Transferred to telemetry. Tolerating oral meds. PT/OT consult placed. ECG pending review.'),

-- Aisha Rahman
('E-5020', 'P-1003', '2024-10-15', 'Emergency', 'Emergency Department', 'Dr. Michael Torres',
 'Sudden onset palpitations, dizziness, near-syncope',
 'I47.1', 'Supraventricular tachycardia', 'Discharged with follow-up',
 'SVT at rate 168. Converted with adenosine 6mg IV push. Observation x 4 hours then discharged.'),
('E-5021', 'P-1003', '2024-11-03', 'Outpatient', 'Cardiology Clinic', 'Dr. Michael Torres',
 'Follow-up after SVT episode', 'I47.1', 'Supraventricular tachycardia', NULL,
 'Holter monitor showed intermittent SVT runs. Started metoprolol. Discussed ablation as future option.'),
('E-5022', 'P-1003', '2025-02-12', 'Outpatient', 'Cardiology Clinic', 'Dr. Michael Torres',
 'SVT follow-up', 'I47.1', 'Supraventricular tachycardia', NULL,
 'Significant improvement on beta blocker + CCB. No symptomatic episodes in 3 months.'),
('E-5023', 'P-1003', '2026-02-26', 'Outpatient', 'Cardiology Clinic', 'Dr. Michael Torres',
 'Routine cardiology follow-up with ECG', 'I47.1', 'Supraventricular tachycardia', NULL,
 'Well controlled on current regimen. ECG today shows abnormal rhythm pattern. Review pending.');

-- ============================================================
-- MEDICAL_IMAGES (9 images across 3 patients)
-- ============================================================
CREATE OR REPLACE TABLE MEDICAL_IMAGES (
    IMAGE_ID            VARCHAR(20),
    PATIENT_ID          VARCHAR(36) REFERENCES PATIENTS(PATIENT_ID),
    MODALITY            VARCHAR(50),
    BODY_PART           VARCHAR(100),
    IMAGE_DATE          TIMESTAMP_NTZ,
    IMAGE_FILE          VARCHAR(500),
    STAGE_PATH          VARCHAR(500),
    DESCRIPTION         VARCHAR(1000),
    ORDERING_PHYSICIAN  VARCHAR(100),
    CLINICAL_INDICATION VARCHAR(500),
    STATUS              VARCHAR(20),
    FINDINGS            VARCHAR(2000),
    ENCOUNTER_ID        VARCHAR(20)
);

INSERT INTO MEDICAL_IMAGES VALUES
-- P-1001 (Maria Santos): ECG, Chest X-Ray, Echo
('IMG-7001', 'P-1001', 'ECG', 'Heart', '2025-06-10 14:30:00', 'dummy/ecg_p1001_stemi_lad.png',
 'ecg_data_new_version/ecg data new version/myocardial_infarction_ecg_images/MI(1).jpg',
 '12-lead ECG showing post-MI changes with ST elevation in leads V1-V4', 'Dr. Sarah Chen',
 'Post-MI monitoring, LAD STEMI day 3', 'reviewed',
 'ST elevation in V1-V4, Q waves in V2-V3, consistent with anterior STEMI', 'E-2001'),

('IMG-7004', 'P-1001', 'Chest X-Ray', 'Chest', '2025-06-10 08:15:00', 'dummy/chest_xray_p1001_admission.png',
 'dummy/chest_xray_p1001_admission.png',
 'PA chest radiograph on admission', 'Dr. Sarah Chen',
 'Dyspnea, rule out pulmonary edema post-MI', 'reviewed',
 'Mild cardiomegaly. Clear lung fields bilaterally. No pleural effusion. Mediastinal contours normal.', 'E-2001'),

('IMG-7005', 'P-1001', 'Echocardiogram', 'Heart', '2025-06-11 10:00:00', 'dummy/echo_p1001_post_pci.png',
 'dummy/echo_p1001_post_pci.png',
 'Transthoracic echocardiogram, apical 4-chamber view post PCI', 'Dr. Sarah Chen',
 'LV function assessment post LAD PCI', 'reviewed',
 'LVEF 42%. Anterior wall hypokinesis. Mild mitral regurgitation. No pericardial effusion.', 'E-2001'),

-- P-1002 (James Wilson): ECG, Chest X-Ray, Angiogram
('IMG-7010', 'P-1002', 'ECG', 'Heart', '2025-06-12 02:45:00', 'dummy/ecg_p1002_stemi_rca.png',
 'ecg_data_new_version/ecg data new version/myocardial_infarction_ecg_images/MI(10).jpg',
 '12-lead ECG showing acute inferior STEMI with RCA involvement', 'Dr. Sarah Chen',
 'Acute chest pain, troponin elevated, RCA occlusion', 'reviewed',
 'ST elevation in II, III, aVF. Reciprocal depression in I, aVL. Acute inferior STEMI.', 'E-2004'),

('IMG-7013', 'P-1002', 'Chest X-Ray', 'Chest', '2025-06-12 03:00:00', 'dummy/chest_xray_p1002_er.png',
 'dummy/chest_xray_p1002_er.png',
 'PA chest radiograph, ER admission', 'Dr. Sarah Chen',
 'Acute STEMI workup, assess for heart failure', 'reviewed',
 'Cardiomegaly. Bilateral small pleural effusions. Cephalization of pulmonary vessels.', 'E-2004'),

('IMG-7015', 'P-1002', 'Coronary Angiogram', 'Heart', '2025-06-12 05:30:00', 'dummy/angiogram_p1002_rca.png',
 'dummy/angiogram_p1002_rca.png',
 'Coronary angiogram, RAO 30 degree projection showing RCA', 'Dr. Sarah Chen',
 'Acute STEMI, emergent cardiac catheterization', 'reviewed',
 '99% proximal RCA occlusion. TIMI 0 flow pre-intervention. Successful PCI with DES placement. TIMI 3 flow restored.', 'E-2004'),

-- P-1003 (Aisha Rahman): ECG, Holter, Cardiac MRI
('IMG-7020', 'P-1003', 'ECG', 'Heart', '2025-06-08 16:20:00', 'dummy/ecg_p1003_svt.png',
 'ecg_data_new_version/ecg data new version/abnormal_heartbeat_ecg_images/HB(1).jpg',
 '12-lead ECG during SVT episode showing narrow complex tachycardia', 'Dr. Michael Torres',
 'Palpitations, dizziness, recurrent SVT episodes', 'reviewed',
 'Narrow complex tachycardia at 178 bpm. No P waves visible. Regular R-R intervals. Consistent with AVNRT.', 'E-2007'),

('IMG-7023', 'P-1003', 'Holter Monitor', 'Heart', '2025-06-09 00:00:00', 'dummy/holter_p1003_24hr.png',
 'dummy/holter_p1003_24hr.png',
 '24-hour Holter monitor, 3-lead continuous recording', 'Dr. Michael Torres',
 'Quantify SVT episodes, assess rate control', 'reviewed',
 'Multiple SVT episodes recorded (12 episodes, longest 4 min 32 sec). Max HR 188 bpm. Average HR 92 bpm. No significant pauses.', 'E-2007'),

('IMG-7025', 'P-1003', 'Cardiac MRI', 'Heart', '2025-06-09 14:00:00', 'dummy/cardiac_mri_p1003.png',
 'dummy/cardiac_mri_p1003.png',
 'Cardiac MRI short-axis SSFP cine, end-diastolic frame', 'Dr. Michael Torres',
 'Evaluate structural heart disease, pre-ablation workup', 'reviewed',
 'Normal LV size and function. LVEF 58%. No late gadolinium enhancement. No structural abnormality to account for SVT.', 'E-2007');

-- ============================================================
-- MEDGEMMA_MEDICAL_INTERPRETER stored procedure
-- ============================================================
CREATE OR REPLACE PROCEDURE MEDGEMMA_MEDICAL_INTERPRETER(
    P_MODE VARCHAR DEFAULT 'text',
    P_IMAGE_ID VARCHAR DEFAULT NULL,
    P_TEXT VARCHAR DEFAULT NULL,
    P_QUESTION VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'medgemma_medical_interpreter'
EXTERNAL_ACCESS_INTEGRATIONS = (MEDGEMMA_SPCS_EAI)
SECRETS = ('pat_token' = MEDGEMMA_PAT_SECRET)  -- Resolved from current DB.SCHEMA context
EXECUTE AS CALLER
AS
$$
import json
import time
import base64
import requests
from snowflake.snowpark.files import SnowflakeFile
import _snowflake

MEDGEMMA_REST_URL = None  # Set dynamically from session variable
IMAGE_STAGE_REF = None    # Set dynamically from session variable

def _get_endpoint(session):
    """Retrieve the SPCS endpoint URL from session variable or fallback."""
    global MEDGEMMA_REST_URL
    if MEDGEMMA_REST_URL is None:
        try:
            rows = session.sql("SELECT $MEDGEMMA_ENDPOINT").collect()
            MEDGEMMA_REST_URL = rows[0][0]
        except Exception:
            raise Exception(
                'MEDGEMMA_REST_URL not configured. '
                'Run: SET MEDGEMMA_ENDPOINT = \'https://<your-spcs-endpoint>.snowflakecomputing.app/__call__\'; '
                'Or see sql/deploy_medgemma.sql Step 7 to find your endpoint URL.'
            )
    return MEDGEMMA_REST_URL

def _get_image_stage(session):
    """Retrieve the image stage reference from session variable or fallback."""
    global IMAGE_STAGE_REF
    if IMAGE_STAGE_REF is None:
        try:
            rows = session.sql("SELECT $IMAGE_STAGE").collect()
            IMAGE_STAGE_REF = rows[0][0]
        except Exception:
            db = session.get_current_database().replace('"', '')
            schema = session.get_current_schema().replace('"', '')
            IMAGE_STAGE_REF = f"{db}.{schema}.ECG_STAGE"
    return IMAGE_STAGE_REF

_http = requests.Session()

def _esc(val):
    if val is None:
        return 'NULL'
    return "'" + str(val).replace('\\', '\\\\').replace("'", "\\'") + "'"

def _call_medgemma(prompt_text, image_data_url=None, max_tokens=512, url_override=None):
    pat = _snowflake.get_generic_secret_string('pat_token')

    if image_data_url:
        content = [
            {"type": "image_url", "image_url": {"url": image_data_url}},
            {"type": "text", "text": prompt_text}
        ]
    else:
        content = [{"type": "text", "text": prompt_text}]

    payload = {
        "dataframe_split": {
            "index": [0],
            "columns": ["MESSAGES", "TEMPERATURE", "MAX_COMPLETION_TOKENS", "STOP", "N", "STREAM", "TOP_P", "FREQUENCY_PENALTY", "PRESENCE_PENALTY"],
            "data": [[
                [{"role": "user", "content": content}],
                0.3, max_tokens, [], 1, False, 0.95, 0.0, 0.0
            ]]
        }
    }

    endpoint = url_override or MEDGEMMA_REST_URL
    if endpoint is None:
        raise Exception('MEDGEMMA_REST_URL not set. See deploy_medgemma.sql Step 7.')
    resp = _http.post(
        endpoint,
        json=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f'Snowflake Token="{pat}"'
        },
        timeout=90
    )

    if resp.status_code != 200:
        raise Exception(f'MedGemma REST returned {resp.status_code}: {resp.text[:500]}')

    result = resp.json()
    data_arr = result.get('data', [[]])[0]
    if len(data_arr) > 1 and isinstance(data_arr[1], dict):
        choices = data_arr[1].get('choices', [])
        if choices:
            return choices[0].get('message', {}).get('content', '')
    return ''


def _interpret_image(session, timings, p_image_id, prompt_text):
    t0 = time.time()

    db = session.get_current_database().replace('"', '')
    schema = session.get_current_schema().replace('"', '')
    img_stage = _get_image_stage(session)

    rows = session.sql(f"""
        SELECT m.IMAGE_ID, m.PATIENT_ID, m.MODALITY, m.BODY_PART,
               m.IMAGE_DATE, m.STAGE_PATH,
               m.DESCRIPTION, m.CLINICAL_INDICATION, m.FINDINGS,
               p.FIRST_NAME, p.LAST_NAME,
               BUILD_SCOPED_FILE_URL(@{img_stage}, m.STAGE_PATH) AS SCOPED_URL,
               GET_PRESIGNED_URL(@{img_stage}, m.STAGE_PATH, 3600) AS PRESIGNED_URL
        FROM {db}.{schema}.MEDICAL_IMAGES_IT m
        JOIN {db}.{schema}.PATIENTS_IT p ON m.PATIENT_ID = p.PATIENT_ID
        WHERE m.IMAGE_ID = {_esc(p_image_id)}
    """).collect()

    timings['metadata_query'] = round(time.time() - t0, 2)

    if not rows:
        return {'status': 'error', 'message': f'No medical image found for IMAGE_ID={p_image_id}'}

    img = rows[0]
    stage_path = img['STAGE_PATH']
    patient_name = f"{img['FIRST_NAME']} {img['LAST_NAME']}"

    if not stage_path or stage_path == 'None':
        return {
            'status': 'error',
            'message': f'No stage path for image {img["IMAGE_ID"]}.',
            'image_id': img['IMAGE_ID'], 'patient_name': patient_name, 'modality': img['MODALITY']
        }

    t1 = time.time()
    try:
        with SnowflakeFile.open(img['SCOPED_URL'], 'rb') as f:
            b64_str = base64.b64encode(f.read()).decode('utf-8')
    except Exception as e:
        return {'status': 'error', 'message': f'Could not read image from stage: {str(e)}'}

    ext = stage_path.rsplit('.', 1)[-1].lower() if '.' in stage_path else 'png'
    mime = 'image/jpeg' if ext in ('jpg', 'jpeg') else f'image/{ext}'
    timings['image_prep'] = round(time.time() - t1, 2)

    if not prompt_text:
        modality = img['MODALITY']
        parts = [f'Analyze this {modality} image for {patient_name}. Body part: {img["BODY_PART"]}. {img["DESCRIPTION"]}.']
        if img['CLINICAL_INDICATION']:
            parts.append(f'Indication: {img["CLINICAL_INDICATION"]}.')
        if img['FINDINGS']:
            parts.append(f'Prior findings: {img["FINDINGS"]}.')
        modality_instr = {
            'ECG': 'Interpret: Rate/Rhythm, ST-Segment, Clinical impression, Recommendations.',
            'Chest X-Ray': 'Interpret: Heart, Lungs, Mediastinum, Bones, Impression, Recommendations.',
            'Echocardiogram': 'Interpret: Chambers, Wall motion, Valves, EF, Impression, Recommendations.',
            'Coronary Angiogram': 'Interpret: Anatomy, Stenosis, Collaterals, Impression, Recommendations.',
            'Cardiac MRI': 'Interpret: Morphology, Tissue, Function, Impression, Recommendations.',
            'Holter Monitor': 'Interpret: Rhythm, Arrhythmias, HRV, Impression, Recommendations.',
        }
        parts.append(modality_instr.get(modality, 'Provide structured clinical interpretation with findings and recommendations.'))
        prompt_text = ' '.join(parts)

    t2 = time.time()
    interpretation = _call_medgemma(prompt_text, image_data_url=f'data:{mime};base64,{b64_str}', max_tokens=512)
    timings['medgemma_inference'] = round(time.time() - t2, 2)

    return {
        'status': 'success',
        'mode': 'image',
        'image_id': img['IMAGE_ID'],
        'patient_name': patient_name,
        'patient_id': img['PATIENT_ID'],
        'modality': img['MODALITY'],
        'body_part': img['BODY_PART'],
        'image_date': str(img['IMAGE_DATE']),
        'description': img['DESCRIPTION'],
        'clinical_indication': img['CLINICAL_INDICATION'],
        'image_url': img['PRESIGNED_URL'] or '',
        'medgemma_interpretation': interpretation,
    }


def medgemma_medical_interpreter(session, p_mode='text', p_image_id=None,
                                  p_text=None, p_question=None):
    timings = {}
    t0 = time.time()
    mode = (p_mode or 'text').strip().lower()

    try:
        _get_endpoint(session)

        if mode == 'image':
            if not p_image_id:
                return json.dumps({'status': 'error', 'message': 'IMAGE mode requires P_IMAGE_ID'})
            result = _interpret_image(session, timings, p_image_id, p_question)
            if result.get('status') == 'error':
                result['timings'] = timings
                return json.dumps(result)
            timings['total'] = round(time.time() - t0, 2)
            result['timings'] = timings
            return json.dumps(result)

        if not p_text:
            return json.dumps({'status': 'error', 'message': 'TEXT mode requires P_TEXT (clinical data to interpret)'})

        prompt = p_text
        if p_question:
            prompt = f"{p_text}\n\nQuestion: {p_question}"

        t2 = time.time()
        interpretation = _call_medgemma(prompt, image_data_url=None, max_tokens=512)
        timings['medgemma_inference'] = round(time.time() - t2, 2)
        timings['total'] = round(time.time() - t0, 2)

        return json.dumps({
            'status': 'success',
            'mode': 'text',
            'medgemma_interpretation': interpretation,
            'timings': timings
        })

    except Exception as e:
        timings['total'] = round(time.time() - t0, 2)
        return json.dumps({
            'status': 'error',
            'message': f'MedGemma interpretation failed: {str(e)}',
            'mode': mode,
            'timings': timings
        })
$$;
