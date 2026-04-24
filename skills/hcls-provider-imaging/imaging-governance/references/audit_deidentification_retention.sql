-- =============================================================================
-- Imaging Governance: De-identification, Audit, Retention, and Archival
-- Database: {database}.{schema}
-- =============================================================================

-- ===== DE-IDENTIFICATION DYNAMIC TABLE =====

CREATE OR REPLACE DYNAMIC TABLE {database}.{schema}.DICOM_STUDIES_DEIDENTIFIED
    TARGET_LAG = '1 hour' WAREHOUSE = {warehouse}
AS
SELECT
    SHA2(s.study_instance_uid, 256) AS study_uid_hash,
    SHA2(p.patient_id, 256) AS patient_id_hash,
    '***REDACTED***' AS patient_name_redacted,
    DATE_FROM_PARTS(YEAR(p.patient_birth_date), 1, 1) AS birth_year_only,
    DATE_FROM_PARTS(YEAR(s.study_date), 1, 1) AS study_year_only,
    ser.modality, ser.body_part_examined,
    SHA2(s.referring_physician, 256) AS referring_physician_hash,
    SHA2(s.accession_number, 256) AS accession_hash,
    SHA2(e.device_serial_number, 256) AS device_serial_hash,
    SHA2(ps.performing_physician, 256) AS performing_physician_hash,
    REGEXP_REPLACE(fl.storage_uri, '/patients/[^/]+/', '/patients/REDACTED/') AS file_uri_sanitized,
    s.number_of_series, s.number_of_instances
FROM {database}.{schema}.DICOM_STUDY s
    LEFT JOIN {database}.{schema}.DICOM_PATIENT p ON s.patient_key = p.patient_key
    LEFT JOIN {database}.{schema}.DICOM_SERIES ser ON s.study_key = ser.study_key
    LEFT JOIN {database}.{schema}.DICOM_EQUIPMENT e ON ser.series_key = e.series_key
    LEFT JOIN {database}.{schema}.DICOM_PROCEDURE_STEP ps ON s.study_key = ps.study_key
    LEFT JOIN {database}.{schema}.DICOM_INSTANCE inst ON ser.series_key = inst.series_key
    LEFT JOIN {database}.{schema}.DICOM_FILE_LOCATION fl ON inst.instance_key = fl.instance_key
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.study_key ORDER BY ser.series_key) = 1;

GRANT SELECT ON DYNAMIC TABLE {database}.{schema}.DICOM_STUDIES_DEIDENTIFIED TO ROLE RESEARCH_ANALYST;

-- ===== AUDIT VIEW =====

CREATE OR REPLACE VIEW {database}.{schema}.V_PHI_ACCESS_AUDIT AS
SELECT ah.user_name, ah.role_name, ah.query_start_time, ah.query_id,
    do.value:objectName::STRING AS object_name,
    col.value:columnName::STRING AS column_name
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
    LATERAL FLATTEN(input => ah.direct_objects_accessed) do,
    LATERAL FLATTEN(input => do.value:columns) col
WHERE do.value:objectName::STRING IN ('DICOM_PATIENT','DICOM_STUDY','DICOM_EQUIPMENT',
    'DICOM_PROCEDURE_STEP','DICOM_FILE_LOCATION','DICOM_RAW','RADIOLOGY_REPORTS')
AND col.value:columnName::STRING IN ('PATIENT_NAME','PATIENT_ID','PATIENT_BIRTH_DATE',
    'REFERRING_PHYSICIAN','ACCESSION_NUMBER','DEVICE_SERIAL_NUMBER','STATION_NAME',
    'PERFORMING_PHYSICIAN','STORAGE_URI','METADATA','RADIOLOGIST_NAME','REPORT_TEXT')
AND ah.query_start_time >= DATEADD('day', -90, CURRENT_TIMESTAMP());

-- Anomalous access alert
CREATE OR REPLACE ALERT {database}.{schema}.PHI_ANOMALOUS_ACCESS_ALERT
    WAREHOUSE = {warehouse}
    SCHEDULE = 'USING CRON 0 */4 * * * America/New_York'
    IF (EXISTS (
        SELECT user_name, COUNT(*) AS cnt
        FROM {database}.{schema}.V_PHI_ACCESS_AUDIT
        WHERE query_start_time >= DATEADD('hour', -4, CURRENT_TIMESTAMP())
        GROUP BY user_name HAVING cnt > 100))
    THEN CALL SYSTEM$SEND_EMAIL('imaging_governance_notifications',
        'security-team@hospital.org','ALERT: Anomalous PHI Access Detected',
        'A user exceeded 100 PHI column accesses in 4 hours. Review V_PHI_ACCESS_AUDIT.');

ALTER ALERT {database}.{schema}.PHI_ANOMALOUS_ACCESS_ALERT RESUME;

-- Compliance summary
SELECT user_name, role_name, object_name, COUNT(DISTINCT query_id) AS query_count,
    COUNT(DISTINCT column_name) AS phi_columns_accessed,
    MIN(query_start_time) AS first_access, MAX(query_start_time) AS last_access
FROM {database}.{schema}.V_PHI_ACCESS_AUDIT
GROUP BY user_name, role_name, object_name ORDER BY query_count DESC;

-- ===== RETENTION & LIFECYCLE =====

ALTER TABLE {database}.{schema}.DICOM_PATIENT SET DATA_RETENTION_TIME_IN_DAYS = 90;
ALTER TABLE {database}.{schema}.DICOM_STUDY SET DATA_RETENTION_TIME_IN_DAYS = 90;
ALTER TABLE {database}.{schema}.RADIOLOGY_REPORTS SET DATA_RETENTION_TIME_IN_DAYS = 90;

CREATE TABLE IF NOT EXISTS {database}.{schema}.DATA_RETENTION_REGISTRY (
    table_name STRING, retention_policy STRING, retention_years NUMBER,
    last_review_date DATE, next_review_date DATE, responsible_role STRING);

INSERT INTO {database}.{schema}.DATA_RETENTION_REGISTRY VALUES
    ('DICOM_PATIENT','HIPAA_6_YEAR',6,CURRENT_DATE(),DATEADD('year',1,CURRENT_DATE()),'IMAGING_ADMIN'),
    ('DICOM_STUDY','HIPAA_6_YEAR',6,CURRENT_DATE(),DATEADD('year',1,CURRENT_DATE()),'IMAGING_ADMIN'),
    ('RADIOLOGY_REPORTS','HIPAA_6_YEAR',6,CURRENT_DATE(),DATEADD('year',1,CURRENT_DATE()),'IMAGING_ADMIN'),
    ('DICOM_RAW','HIPAA_6_YEAR',6,CURRENT_DATE(),DATEADD('year',1,CURRENT_DATE()),'IMAGING_ADMIN'),
    ('DICOM_STUDIES_DEIDENTIFIED','RESEARCH_INDEFINITE',99,CURRENT_DATE(),DATEADD('year',1,CURRENT_DATE()),'RESEARCH_ADMIN');

-- Automated archival task
CREATE OR REPLACE TASK {database}.{schema}.ARCHIVE_EXPIRED_STUDIES
    WAREHOUSE = {warehouse}
    SCHEDULE = 'USING CRON 0 2 1 * * America/New_York'
AS
INSERT INTO {database}.{schema}.DICOM_STUDY_ARCHIVE
    SELECT *, CURRENT_TIMESTAMP() AS archived_at
    FROM {database}.{schema}.DICOM_STUDY
    WHERE study_date < DATEADD('year', -6, CURRENT_DATE())
    AND study_instance_uid NOT IN (SELECT study_instance_uid FROM {database}.{schema}.DICOM_STUDY_ARCHIVE);

ALTER TASK {database}.{schema}.ARCHIVE_EXPIRED_STUDIES RESUME;

-- Retention compliance check
SELECT r.table_name, r.retention_policy, r.retention_years, r.next_review_date,
    t.row_count, t.bytes / (1024*1024*1024) AS size_gb
FROM {database}.{schema}.DATA_RETENTION_REGISTRY r
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS t
    ON t.table_name = r.table_name AND t.table_catalog = '{database}' AND t.table_dropped IS NULL
ORDER BY r.next_review_date;
