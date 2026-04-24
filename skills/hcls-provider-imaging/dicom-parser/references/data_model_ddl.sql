-- =============================================================================
-- DICOM Parser: 19-Table Normalized Data Model DDLs
-- Database: {database}.{schema}
-- =============================================================================

-- ===== CORE HIERARCHY =====

-- 1. DICOM_PATIENT
CREATE OR REPLACE TABLE dicom_patient (
    patient_key INTEGER AUTOINCREMENT PRIMARY KEY,
    source_system VARCHAR,
    patient_id VARCHAR NOT NULL,
    issuer_of_patient_id VARCHAR,
    patient_name VARCHAR,
    patient_sex VARCHAR(16),
    patient_birth_date DATE,
    patient_age VARCHAR(16),
    other_patient_ids ARRAY,
    other_patient_names ARRAY,
    comments VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (patient_id, issuer_of_patient_id)
);

-- 2. DICOM_STUDY
CREATE OR REPLACE TABLE dicom_study (
    study_key INTEGER AUTOINCREMENT PRIMARY KEY,
    patient_key INTEGER REFERENCES dicom_patient(patient_key),
    study_instance_uid VARCHAR NOT NULL UNIQUE,
    accession_number VARCHAR,
    study_id VARCHAR,
    study_datetime TIMESTAMP_NTZ,
    study_date DATE,
    study_time TIME,
    study_description VARCHAR,
    referring_physician VARCHAR,
    admitting_diagnosis VARCHAR,
    study_instance_uid_root VARCHAR,
    number_of_series INTEGER,
    number_of_instances INTEGER,
    modalities_in_study ARRAY,
    _source_file VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 3. DICOM_SERIES
CREATE OR REPLACE TABLE dicom_series (
    series_key INTEGER AUTOINCREMENT PRIMARY KEY,
    study_key INTEGER REFERENCES dicom_study(study_key),
    series_instance_uid VARCHAR NOT NULL UNIQUE,
    series_number INTEGER,
    modality VARCHAR(16) NOT NULL,
    body_part_examined VARCHAR,
    laterality VARCHAR(16),
    series_description VARCHAR,
    frame_of_reference_uid VARCHAR,
    patient_position VARCHAR(16),
    performed_station_name VARCHAR,
    performed_location VARCHAR,
    series_date DATE,
    series_time TIME,
    protocol_name VARCHAR,
    number_of_instances INTEGER,
    _source_file VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 4. DICOM_INSTANCE
CREATE OR REPLACE TABLE dicom_instance (
    instance_key INTEGER AUTOINCREMENT PRIMARY KEY,
    series_key INTEGER REFERENCES dicom_series(series_key),
    sop_instance_uid VARCHAR NOT NULL UNIQUE,
    sop_class_uid VARCHAR NOT NULL,
    instance_number INTEGER,
    image_type ARRAY,
    acquisition_datetime TIMESTAMP_NTZ,
    content_datetime TIMESTAMP_NTZ,
    acquisition_date DATE,
    acquisition_time TIME,
    content_date DATE,
    content_time TIME,
    number_of_frames INTEGER DEFAULT 1,
    specific_character_set VARCHAR,
    burned_in_annotation VARCHAR,
    presentation_intent VARCHAR,
    file_path VARCHAR,
    file_size_bytes INTEGER,
    transfer_syntax_uid VARCHAR,
    _source_file VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 5. DICOM_FRAME
CREATE OR REPLACE TABLE dicom_frame (
    frame_key INTEGER AUTOINCREMENT PRIMARY KEY,
    instance_key INTEGER REFERENCES dicom_instance(instance_key),
    frame_number INTEGER NOT NULL,
    frame_content_datetime TIMESTAMP_NTZ,
    image_position_patient ARRAY,
    image_orientation_patient ARRAY,
    slice_location FLOAT,
    temporal_position_index INTEGER,
    cardiac_cycle_position FLOAT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (instance_key, frame_number)
);

-- ===== TECHNICAL AND ACQUISITION CONTEXT =====

-- 6. DICOM_EQUIPMENT
CREATE OR REPLACE TABLE dicom_equipment (
    equipment_key INTEGER AUTOINCREMENT PRIMARY KEY,
    series_key INTEGER REFERENCES dicom_series(series_key),
    manufacturer VARCHAR,
    manufacturer_model_name VARCHAR,
    device_serial_number VARCHAR,
    software_versions VARCHAR,
    institution_name VARCHAR,
    institution_address VARCHAR,
    station_name VARCHAR,
    institutional_dept_name VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 7. DICOM_IMAGE_PIXEL
CREATE OR REPLACE TABLE dicom_image_pixel (
    image_pixel_key INTEGER AUTOINCREMENT PRIMARY KEY,
    instance_key INTEGER REFERENCES dicom_instance(instance_key),
    image_rows INTEGER,
    image_columns INTEGER,
    number_of_frames INTEGER,
    samples_per_pixel INTEGER,
    photometric_interpretation VARCHAR,
    bits_allocated INTEGER,
    bits_stored INTEGER,
    high_bit INTEGER,
    pixel_representation INTEGER,
    planar_configuration INTEGER,
    rescale_intercept FLOAT,
    rescale_slope FLOAT,
    window_center ARRAY,
    window_width ARRAY,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 8. DICOM_IMAGE_PLANE
CREATE OR REPLACE TABLE dicom_image_plane (
    image_plane_key INTEGER AUTOINCREMENT PRIMARY KEY,
    instance_key INTEGER REFERENCES dicom_instance(instance_key),
    frame_key INTEGER REFERENCES dicom_frame(frame_key),
    pixel_spacing ARRAY,
    slice_thickness FLOAT,
    image_position_patient ARRAY,
    image_orientation_patient ARRAY,
    spacing_between_slices FLOAT,
    position_reference_indicator VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ===== WORKFLOW AND PROCEDURE CONTEXT =====

-- 9. DICOM_PROCEDURE_STEP
CREATE OR REPLACE TABLE dicom_procedure_step (
    procedure_key INTEGER AUTOINCREMENT PRIMARY KEY,
    study_key INTEGER REFERENCES dicom_study(study_key),
    requested_procedure_id VARCHAR,
    requested_procedure_description VARCHAR,
    requested_procedure_code_seq VARIANT,
    performed_procedure_step_id VARCHAR,
    performed_procedure_description VARCHAR,
    performed_procedure_type VARCHAR,
    performed_procedure_code_seq VARIANT,
    performing_physician VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ===== DOSE AND EXPOSURE =====

-- 10. DICOM_DOSE_SUMMARY
CREATE OR REPLACE TABLE dicom_dose_summary (
    dose_key INTEGER AUTOINCREMENT PRIMARY KEY,
    series_key INTEGER REFERENCES dicom_series(series_key),
    study_key INTEGER REFERENCES dicom_study(study_key),
    ctdi_vol FLOAT,
    dose_length_product FLOAT,
    exposure_time FLOAT,
    kvp FLOAT,
    xray_tube_current FLOAT,
    exposure FLOAT,
    acquisition_protocol VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ===== DERIVED OBJECT METADATA =====

-- 11. DICOM_SEGMENTATION_METADATA
CREATE OR REPLACE TABLE dicom_segmentation_metadata (
    segmentation_key INTEGER AUTOINCREMENT PRIMARY KEY,
    instance_key INTEGER REFERENCES dicom_instance(instance_key),
    referenced_series_key INTEGER REFERENCES dicom_series(series_key),
    segment_number INTEGER,
    segment_label VARCHAR,
    segment_description VARCHAR,
    segmentation_type VARCHAR,
    segmentation_fractional_type VARCHAR,
    recommended_display_grayscale VARIANT,
    anatomic_region_code_seq VARIANT,
    property_category_code_seq VARIANT,
    property_type_code_seq VARIANT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 12. DICOM_STRUCTURED_REPORT_HEADER
CREATE OR REPLACE TABLE dicom_structured_report_header (
    sr_header_key INTEGER AUTOINCREMENT PRIMARY KEY,
    instance_key INTEGER REFERENCES dicom_instance(instance_key),
    completion_flag VARCHAR,
    verification_flag VARCHAR,
    document_title VARCHAR,
    coding_scheme_identification VARIANT,
    referenced_instance_keys ARRAY,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ===== PHYSICAL STORAGE =====

-- 13. DICOM_FILE_LOCATION
CREATE OR REPLACE TABLE dicom_file_location (
    location_key INTEGER AUTOINCREMENT PRIMARY KEY,
    instance_key INTEGER REFERENCES dicom_instance(instance_key),
    storage_uri VARCHAR,
    storage_provider VARCHAR,
    storage_container VARCHAR,
    object_key VARCHAR,
    transfer_syntax_uid VARCHAR,
    checksum VARCHAR,
    ingestion_source VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ===== GENERIC ELEMENT & SEQUENCE STORE =====

-- 14. DICOM_ELEMENT
CREATE OR REPLACE TABLE dicom_element (
    element_key INTEGER AUTOINCREMENT PRIMARY KEY,
    instance_key INTEGER REFERENCES dicom_instance(instance_key),
    frame_number INTEGER,
    tag_group INTEGER,
    tag_element INTEGER,
    tag_hex VARCHAR(8),
    name VARCHAR,
    vr VARCHAR(4),
    vm INTEGER,
    value_string VARCHAR,
    value_number FLOAT,
    value_datetime TIMESTAMP_NTZ,
    value_binary_ref VARCHAR,
    is_private BOOLEAN,
    private_creator VARCHAR,
    sequence_item_key INTEGER,
    sequence_path VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 15. DICOM_SEQUENCE_ITEM
CREATE OR REPLACE TABLE dicom_sequence_item (
    sequence_item_key INTEGER AUTOINCREMENT PRIMARY KEY,
    instance_key INTEGER REFERENCES dicom_instance(instance_key),
    parent_element_key INTEGER REFERENCES dicom_element(element_key),
    item_index INTEGER,
    sequence_path VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ===== IMAGE EMBEDDINGS =====

-- 16. IMAGE_EMBEDDING
CREATE OR REPLACE TABLE image_embedding (
    embedding_key INTEGER AUTOINCREMENT PRIMARY KEY,
    instance_key INTEGER REFERENCES dicom_instance(instance_key),
    frame_key INTEGER REFERENCES dicom_frame(frame_key),
    segmentation_key INTEGER REFERENCES dicom_segmentation_metadata(segmentation_key),
    embedding_vector ARRAY,
    model_key INTEGER REFERENCES embedding_model(model_key),
    representation_scope VARCHAR,
    representation_version VARCHAR,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_image_uri VARCHAR
);

-- 17. EMBEDDING_MODEL
CREATE OR REPLACE TABLE embedding_model (
    model_key INTEGER AUTOINCREMENT PRIMARY KEY,
    model_name VARCHAR NOT NULL,
    model_version VARCHAR,
    modality_scope ARRAY,
    task_scope ARRAY,
    dimensionality INTEGER,
    training_data_summary VARCHAR,
    preprocessing_spec VARIANT,
    postprocessing_notes VARCHAR,
    owning_team VARCHAR,
    regulatory_notes VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 18. EMBEDDING_EVALUATION
CREATE OR REPLACE TABLE embedding_evaluation (
    evaluation_key INTEGER AUTOINCREMENT PRIMARY KEY,
    model_key INTEGER REFERENCES embedding_model(model_key),
    dataset_name VARCHAR,
    dataset_description VARCHAR,
    metric_name VARCHAR,
    metric_value FLOAT,
    metric_details VARIANT,
    evaluated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ===== CLINICAL REPORTS =====

-- 19. RADIOLOGY_REPORTS
CREATE OR REPLACE TABLE radiology_reports (
    report_key INTEGER AUTOINCREMENT PRIMARY KEY,
    study_instance_uid VARCHAR REFERENCES dicom_study(study_instance_uid),
    patient_id VARCHAR,
    radiologist_name VARCHAR,
    report_datetime TIMESTAMP_NTZ,
    report_date DATE,
    report_text VARCHAR,
    report_status VARCHAR,
    _source_file VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
