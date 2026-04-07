"""
Snowpark Ingestion Pipeline for DICOM files from Snowflake Stage
Reads DICOM files from internal/external stage and loads to IMAGING_STUDIES tables
"""
import os
import json
from typing import Dict, List, Any
from datetime import datetime

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, parse_json, current_timestamp
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, ArrayType, VariantType

from parse_dicom_snowpark import parse_dicom_file, serialize_for_snowflake


DATABASE = 'UNSTRUCTURED_HEALTHDATA'
SCHEMA = 'IMAGING_STUDIES'


def get_session(connection_name: str = None) -> Session:
    """Create Snowpark session."""
    import snowflake.connector
    
    conn_name = connection_name or os.getenv('SNOWFLAKE_CONNECTION_NAME') or 'default_connection_name'
    conn = snowflake.connector.connect(connection_name=conn_name)
    
    return Session.builder.configs({
        "connection": conn
    }).create()


def upsert_patient(session: Session, patient_data: Dict[str, Any]) -> int:
    """Upsert patient and return patient_key."""
    patient_id = patient_data.get('patient_id', '')
    issuer = patient_data.get('issuer_of_patient_id') or ''
    
    existing = session.sql(f"""
        SELECT patient_key FROM {DATABASE}.{SCHEMA}.dicom_patient 
        WHERE patient_id = '{patient_id}' 
        AND COALESCE(issuer_of_patient_id, '') = '{issuer}'
    """).collect()
    
    if existing:
        return existing[0]['PATIENT_KEY']
    
    birth_date = patient_data.get('patient_birth_date')
    birth_date_str = f"'{birth_date}'" if birth_date else 'NULL'
    
    other_ids = json.dumps(patient_data.get('other_patient_ids') or [])
    other_names = json.dumps(patient_data.get('other_patient_names') or [])
    
    session.sql(f"""
        INSERT INTO {DATABASE}.{SCHEMA}.dicom_patient 
        (source_system, patient_id, issuer_of_patient_id, patient_name, patient_sex, 
         patient_birth_date, patient_age, other_patient_ids, other_patient_names, comments)
        VALUES (
            {_sql_str(patient_data.get('source_system'))},
            {_sql_str(patient_id)},
            {_sql_str(patient_data.get('issuer_of_patient_id'))},
            {_sql_str(patient_data.get('patient_name'))},
            {_sql_str(patient_data.get('patient_sex'))},
            {birth_date_str},
            {_sql_str(patient_data.get('patient_age'))},
            PARSE_JSON('{other_ids}'),
            PARSE_JSON('{other_names}'),
            {_sql_str(patient_data.get('comments'))}
        )
    """).collect()
    
    result = session.sql(f"""
        SELECT patient_key FROM {DATABASE}.{SCHEMA}.dicom_patient 
        WHERE patient_id = '{patient_id}' 
        AND COALESCE(issuer_of_patient_id, '') = '{issuer}'
    """).collect()
    
    return result[0]['PATIENT_KEY']


def upsert_study(session: Session, study_data: Dict[str, Any], patient_key: int) -> int:
    """Upsert study and return study_key."""
    study_uid = study_data.get('study_instance_uid', '')
    
    existing = session.sql(f"""
        SELECT study_key FROM {DATABASE}.{SCHEMA}.dicom_study 
        WHERE study_instance_uid = '{study_uid}'
    """).collect()
    
    if existing:
        return existing[0]['STUDY_KEY']
    
    study_datetime = study_data.get('study_datetime')
    study_datetime_str = f"'{study_datetime}'" if study_datetime else 'NULL'
    study_date = study_data.get('study_date')
    study_date_str = f"'{study_date}'" if study_date else 'NULL'
    study_time = study_data.get('study_time')
    study_time_str = f"'{study_time}'" if study_time else 'NULL'
    
    session.sql(f"""
        INSERT INTO {DATABASE}.{SCHEMA}.dicom_study 
        (patient_key, study_instance_uid, accession_number, study_id, study_datetime,
         study_date, study_time, study_description, referring_physician, admitting_diagnosis)
        VALUES (
            {patient_key},
            {_sql_str(study_uid)},
            {_sql_str(study_data.get('accession_number'))},
            {_sql_str(study_data.get('study_id'))},
            {study_datetime_str},
            {study_date_str},
            {study_time_str},
            {_sql_str(study_data.get('study_description'))},
            {_sql_str(study_data.get('referring_physician'))},
            {_sql_str(study_data.get('admitting_diagnosis'))}
        )
    """).collect()
    
    result = session.sql(f"""
        SELECT study_key FROM {DATABASE}.{SCHEMA}.dicom_study 
        WHERE study_instance_uid = '{study_uid}'
    """).collect()
    
    return result[0]['STUDY_KEY']


def upsert_series(session: Session, series_data: Dict[str, Any], study_key: int) -> int:
    """Upsert series and return series_key."""
    series_uid = series_data.get('series_instance_uid', '')
    
    existing = session.sql(f"""
        SELECT series_key FROM {DATABASE}.{SCHEMA}.dicom_series 
        WHERE series_instance_uid = '{series_uid}'
    """).collect()
    
    if existing:
        return existing[0]['SERIES_KEY']
    
    series_date = series_data.get('series_date')
    series_date_str = f"'{series_date}'" if series_date else 'NULL'
    series_time = series_data.get('series_time')
    series_time_str = f"'{series_time}'" if series_time else 'NULL'
    
    session.sql(f"""
        INSERT INTO {DATABASE}.{SCHEMA}.dicom_series 
        (study_key, series_instance_uid, series_number, modality, body_part_examined,
         laterality, series_description, frame_of_reference_uid, patient_position,
         performed_station_name, performed_location, series_date, series_time, protocol_name)
        VALUES (
            {study_key},
            {_sql_str(series_uid)},
            {series_data.get('series_number') or 'NULL'},
            {_sql_str(series_data.get('modality', 'OT'))},
            {_sql_str(series_data.get('body_part_examined'))},
            {_sql_str(series_data.get('laterality'))},
            {_sql_str(series_data.get('series_description'))},
            {_sql_str(series_data.get('frame_of_reference_uid'))},
            {_sql_str(series_data.get('patient_position'))},
            {_sql_str(series_data.get('performed_station_name'))},
            {_sql_str(series_data.get('performed_location'))},
            {series_date_str},
            {series_time_str},
            {_sql_str(series_data.get('protocol_name'))}
        )
    """).collect()
    
    result = session.sql(f"""
        SELECT series_key FROM {DATABASE}.{SCHEMA}.dicom_series 
        WHERE series_instance_uid = '{series_uid}'
    """).collect()
    
    return result[0]['SERIES_KEY']


def upsert_instance(session: Session, instance_data: Dict[str, Any], series_key: int) -> int:
    """Upsert instance and return instance_key."""
    sop_uid = instance_data.get('sop_instance_uid', '')
    
    existing = session.sql(f"""
        SELECT instance_key FROM {DATABASE}.{SCHEMA}.dicom_instance 
        WHERE sop_instance_uid = '{sop_uid}'
    """).collect()
    
    if existing:
        return existing[0]['INSTANCE_KEY']
    
    acq_datetime = instance_data.get('acquisition_datetime')
    acq_datetime_str = f"'{acq_datetime}'" if acq_datetime else 'NULL'
    content_datetime = instance_data.get('content_datetime')
    content_datetime_str = f"'{content_datetime}'" if content_datetime else 'NULL'
    acq_date = instance_data.get('acquisition_date')
    acq_date_str = f"'{acq_date}'" if acq_date else 'NULL'
    acq_time = instance_data.get('acquisition_time')
    acq_time_str = f"'{acq_time}'" if acq_time else 'NULL'
    content_date = instance_data.get('content_date')
    content_date_str = f"'{content_date}'" if content_date else 'NULL'
    content_time = instance_data.get('content_time')
    content_time_str = f"'{content_time}'" if content_time else 'NULL'
    
    image_type = json.dumps(instance_data.get('image_type') or [])
    
    session.sql(f"""
        INSERT INTO {DATABASE}.{SCHEMA}.dicom_instance 
        (series_key, sop_instance_uid, sop_class_uid, instance_number, image_type,
         acquisition_datetime, content_datetime, acquisition_date, acquisition_time,
         content_date, content_time, number_of_frames, specific_character_set,
         burned_in_annotation, presentation_intent, file_path, file_size_bytes, transfer_syntax_uid)
        VALUES (
            {series_key},
            {_sql_str(sop_uid)},
            {_sql_str(instance_data.get('sop_class_uid'))},
            {instance_data.get('instance_number') or 'NULL'},
            PARSE_JSON('{image_type}'),
            {acq_datetime_str},
            {content_datetime_str},
            {acq_date_str},
            {acq_time_str},
            {content_date_str},
            {content_time_str},
            {instance_data.get('number_of_frames', 1) or 1},
            {_sql_str(instance_data.get('specific_character_set'))},
            {_sql_str(instance_data.get('burned_in_annotation'))},
            {_sql_str(instance_data.get('presentation_intent'))},
            {_sql_str(instance_data.get('file_path'))},
            {instance_data.get('file_size_bytes') or 'NULL'},
            {_sql_str(instance_data.get('transfer_syntax_uid'))}
        )
    """).collect()
    
    result = session.sql(f"""
        SELECT instance_key FROM {DATABASE}.{SCHEMA}.dicom_instance 
        WHERE sop_instance_uid = '{sop_uid}'
    """).collect()
    
    return result[0]['INSTANCE_KEY']


def insert_equipment(session: Session, equipment_data: Dict[str, Any], series_key: int) -> int:
    """Insert equipment metadata."""
    session.sql(f"""
        INSERT INTO {DATABASE}.{SCHEMA}.dicom_equipment 
        (series_key, manufacturer, manufacturer_model_name, device_serial_number,
         software_versions, institution_name, institution_address, station_name, institutional_dept_name)
        VALUES (
            {series_key},
            {_sql_str(equipment_data.get('manufacturer'))},
            {_sql_str(equipment_data.get('manufacturer_model_name'))},
            {_sql_str(equipment_data.get('device_serial_number'))},
            {_sql_str(equipment_data.get('software_versions'))},
            {_sql_str(equipment_data.get('institution_name'))},
            {_sql_str(equipment_data.get('institution_address'))},
            {_sql_str(equipment_data.get('station_name'))},
            {_sql_str(equipment_data.get('institutional_dept_name'))}
        )
    """).collect()
    
    result = session.sql(f"SELECT MAX(equipment_key) as key FROM {DATABASE}.{SCHEMA}.dicom_equipment").collect()
    return result[0]['KEY']


def insert_image_pixel(session: Session, pixel_data: Dict[str, Any], instance_key: int) -> int:
    """Insert image pixel metadata."""
    window_center = json.dumps(pixel_data.get('window_center') or [])
    window_width = json.dumps(pixel_data.get('window_width') or [])
    
    session.sql(f"""
        INSERT INTO {DATABASE}.{SCHEMA}.dicom_image_pixel 
        (instance_key, image_rows, image_columns, number_of_frames, samples_per_pixel,
         photometric_interpretation, bits_allocated, bits_stored, high_bit,
         pixel_representation, planar_configuration, rescale_intercept, rescale_slope,
         window_center, window_width)
        VALUES (
            {instance_key},
            {pixel_data.get('image_rows') or 'NULL'},
            {pixel_data.get('image_columns') or 'NULL'},
            {pixel_data.get('number_of_frames') or 'NULL'},
            {pixel_data.get('samples_per_pixel') or 'NULL'},
            {_sql_str(pixel_data.get('photometric_interpretation'))},
            {pixel_data.get('bits_allocated') or 'NULL'},
            {pixel_data.get('bits_stored') or 'NULL'},
            {pixel_data.get('high_bit') or 'NULL'},
            {pixel_data.get('pixel_representation') or 'NULL'},
            {pixel_data.get('planar_configuration') or 'NULL'},
            {pixel_data.get('rescale_intercept') or 'NULL'},
            {pixel_data.get('rescale_slope') or 'NULL'},
            PARSE_JSON('{window_center}'),
            PARSE_JSON('{window_width}')
        )
    """).collect()
    
    result = session.sql(f"SELECT MAX(image_pixel_key) as key FROM {DATABASE}.{SCHEMA}.dicom_image_pixel").collect()
    return result[0]['KEY']


def insert_file_location(session: Session, instance_key: int, storage_uri: str, 
                         storage_provider: str = 'S3', checksum: str = None) -> int:
    """Insert file location metadata."""
    parts = storage_uri.replace('s3://', '').replace('@', '').split('/', 1)
    container = parts[0] if parts else ''
    object_key = parts[1] if len(parts) > 1 else ''
    
    session.sql(f"""
        INSERT INTO {DATABASE}.{SCHEMA}.dicom_file_location 
        (instance_key, storage_uri, storage_provider, storage_container, object_key,
         checksum, ingestion_source)
        VALUES (
            {instance_key},
            {_sql_str(storage_uri)},
            {_sql_str(storage_provider)},
            {_sql_str(container)},
            {_sql_str(object_key)},
            {_sql_str(checksum)},
            'SNOWPARK_PIPELINE'
        )
    """).collect()
    
    result = session.sql(f"SELECT MAX(location_key) as key FROM {DATABASE}.{SCHEMA}.dicom_file_location").collect()
    return result[0]['KEY']


def _sql_str(value) -> str:
    """Convert value to SQL string literal."""
    if value is None:
        return 'NULL'
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def ingest_dicom_from_stage(session: Session, stage_path: str, source_system: str = None) -> Dict[str, int]:
    """
    Ingest DICOM files from a Snowflake stage into IMAGING_STUDIES tables.
    
    Args:
        session: Snowpark session
        stage_path: Full stage path (e.g., @UNSTRUCTURED_HEALTHDATA.DICOM_RAW.DICOM_STAGE/folder/)
        source_system: Source system identifier
    
    Returns:
        Dictionary with counts of ingested entities
    """
    files = session.sql(f"LIST {stage_path}").collect()
    
    counts = {'patients': 0, 'studies': 0, 'series': 0, 'instances': 0, 'files': 0}
    seen_patients = set()
    seen_studies = set()
    seen_series = set()
    
    for file_row in files:
        file_name = file_row['name']
        if not file_name.lower().endswith('.dcm'):
            continue
        
        try:
            file_content = session.file.get_stream(f"@{file_name}", decompress=False).read()
            metadata = parse_dicom_file(file_name, file_content, source_system)
            
            patient_id = metadata['patient']['patient_id']
            if patient_id not in seen_patients:
                patient_key = upsert_patient(session, metadata['patient'])
                seen_patients.add(patient_id)
                counts['patients'] += 1
            else:
                patient_key = session.sql(f"""
                    SELECT patient_key FROM {DATABASE}.{SCHEMA}.dicom_patient 
                    WHERE patient_id = '{patient_id}'
                """).collect()[0]['PATIENT_KEY']
            
            study_uid = metadata['study']['study_instance_uid']
            if study_uid not in seen_studies:
                study_key = upsert_study(session, metadata['study'], patient_key)
                seen_studies.add(study_uid)
                counts['studies'] += 1
            else:
                study_key = session.sql(f"""
                    SELECT study_key FROM {DATABASE}.{SCHEMA}.dicom_study 
                    WHERE study_instance_uid = '{study_uid}'
                """).collect()[0]['STUDY_KEY']
            
            series_uid = metadata['series']['series_instance_uid']
            if series_uid not in seen_series:
                series_key = upsert_series(session, metadata['series'], study_key)
                insert_equipment(session, metadata['equipment'], series_key)
                seen_series.add(series_uid)
                counts['series'] += 1
            else:
                series_key = session.sql(f"""
                    SELECT series_key FROM {DATABASE}.{SCHEMA}.dicom_series 
                    WHERE series_instance_uid = '{series_uid}'
                """).collect()[0]['SERIES_KEY']
            
            instance_key = upsert_instance(session, metadata['instance'], series_key)
            insert_image_pixel(session, metadata['image_pixel'], instance_key)
            insert_file_location(session, instance_key, f"@{file_name}", 'SNOWFLAKE_STAGE')
            counts['instances'] += 1
            counts['files'] += 1
            
            print(f"Ingested: {file_name}")
            
        except Exception as e:
            print(f"Error processing {file_name}: {e}")
            continue
    
    return counts


def update_study_counts(session: Session):
    """Update computed columns on study table (series count, instance count, modalities)."""
    session.sql(f"""
        MERGE INTO {DATABASE}.{SCHEMA}.dicom_study t
        USING (
            SELECT 
                st.study_key,
                COUNT(DISTINCT ser.series_key) as num_series,
                COUNT(DISTINCT i.instance_key) as num_instances,
                ARRAY_AGG(DISTINCT ser.modality) as modalities
            FROM {DATABASE}.{SCHEMA}.dicom_study st
            LEFT JOIN {DATABASE}.{SCHEMA}.dicom_series ser ON st.study_key = ser.study_key
            LEFT JOIN {DATABASE}.{SCHEMA}.dicom_instance i ON ser.series_key = i.series_key
            GROUP BY st.study_key
        ) s ON t.study_key = s.study_key
        WHEN MATCHED THEN UPDATE SET
            t.number_of_series = s.num_series,
            t.number_of_instances = s.num_instances,
            t.modalities_in_study = s.modalities
    """).collect()


def update_series_counts(session: Session):
    """Update computed columns on series table (instance count)."""
    session.sql(f"""
        MERGE INTO {DATABASE}.{SCHEMA}.dicom_series t
        USING (
            SELECT 
                ser.series_key,
                COUNT(i.instance_key) as num_instances
            FROM {DATABASE}.{SCHEMA}.dicom_series ser
            LEFT JOIN {DATABASE}.{SCHEMA}.dicom_instance i ON ser.series_key = i.series_key
            GROUP BY ser.series_key
        ) s ON t.series_key = s.series_key
        WHEN MATCHED THEN UPDATE SET
            t.number_of_instances = s.num_instances
    """).collect()


if __name__ == '__main__':
    import sys
    
    connection_name = os.getenv('SNOWFLAKE_CONNECTION_NAME', 'default_connection_name')
    session = get_session(connection_name)
    
    if len(sys.argv) > 1:
        stage_path = sys.argv[1]
        source_system = sys.argv[2] if len(sys.argv) > 2 else 'DICOM_INGESTION'
        
        print(f"Ingesting DICOM files from: {stage_path}")
        counts = ingest_dicom_from_stage(session, stage_path, source_system)
        
        print("\nUpdating computed counts...")
        update_study_counts(session)
        update_series_counts(session)
        
        print(f"\nIngestion complete:")
        for entity, count in counts.items():
            print(f"  {entity}: {count}")
    else:
        print("Usage: python ingest_pipeline.py <stage_path> [source_system]")
        print("Example: python ingest_pipeline.py @UNSTRUCTURED_HEALTHDATA.DICOM_RAW.DICOM_STAGE/studies/")
