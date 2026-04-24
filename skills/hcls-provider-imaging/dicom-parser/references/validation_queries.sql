-- =============================================================================
-- DICOM Parser: Validation and Quality Scoring Queries
-- Database: {database}.{schema}
-- =============================================================================

-- Referential integrity checks
SELECT 'study_orphans' AS check_name, COUNT(*) AS violations
FROM {database}.{schema}.dicom_study WHERE patient_key NOT IN (SELECT patient_key FROM {database}.{schema}.dicom_patient)
UNION ALL
SELECT 'series_orphans', COUNT(*)
FROM {database}.{schema}.dicom_series WHERE study_key NOT IN (SELECT study_key FROM {database}.{schema}.dicom_study)
UNION ALL
SELECT 'instance_orphans', COUNT(*)
FROM {database}.{schema}.dicom_instance WHERE series_key NOT IN (SELECT series_key FROM {database}.{schema}.dicom_series);

-- Quality score (per study)
SELECT
    s.study_instance_uid,
    s.study_date,
    ROUND(100.0 * (
        IFF(s.study_description IS NOT NULL, 1, 0) +
        IFF(s.referring_physician IS NOT NULL, 1, 0) +
        IFF(s.accession_number IS NOT NULL, 1, 0) +
        IFF(p.patient_name IS NOT NULL, 1, 0) +
        IFF(p.patient_birth_date IS NOT NULL, 1, 0) +
        IFF(EXISTS(SELECT 1 FROM {database}.{schema}.dicom_equipment e JOIN {database}.{schema}.dicom_series ser ON e.series_key = ser.series_key WHERE ser.study_key = s.study_key), 1, 0) +
        IFF(EXISTS(SELECT 1 FROM {database}.{schema}.dicom_dose_summary d WHERE d.study_key = s.study_key), 1, 0)
    ) / 7.0, 1) AS quality_score
FROM {database}.{schema}.dicom_study s
LEFT JOIN {database}.{schema}.dicom_patient p ON s.patient_key = p.patient_key
ORDER BY quality_score ASC;
