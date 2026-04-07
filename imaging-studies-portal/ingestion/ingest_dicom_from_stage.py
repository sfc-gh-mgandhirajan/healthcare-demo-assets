"""
DICOM Ingestion Pipeline - Load DICOM files from Snowflake Stage to Repository Tables
Reads DICOM files from S3 stage, extracts metadata, and loads into normalized tables.
"""
import os
import io
import json
import hashlib
from datetime import datetime, date
from typing import Dict, Any, Optional, List

# Snowflake connection
CONNECTION_NAME = os.getenv('SNOWFLAKE_CONNECTION_NAME', 'Murali-AWS-US_WEST')
DATABASE = 'UNSTRUCTURED_HEALTHDATA'
SCHEMA = 'IMAGING_STUDIES'
STAGE = f'@{DATABASE}.{SCHEMA}.DICOM_FILES_STAGE'

def execute_sql(sql: str, params: list = None) -> List[Dict]:
    """Execute SQL using snow CLI."""
    import subprocess
    result = subprocess.run(
        ['snow', 'sql', '-q', sql, '-c', CONNECTION_NAME, '--format', 'json'],
        capture_output=True, text=True, timeout=300
    )
    if result.returncode != 0:
        print(f"SQL Error: {result.stderr}")
        return []
    if not result.stdout.strip():
        return []
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return []

def execute_sql_no_result(sql: str) -> bool:
    """Execute SQL that doesn't return results."""
    import subprocess
    result = subprocess.run(
        ['snow', 'sql', '-q', sql, '-c', CONNECTION_NAME],
        capture_output=True, text=True, timeout=300
    )
    return result.returncode == 0

def parse_dicom_date(date_str: str) -> Optional[str]:
    """Parse DICOM date format (YYYYMMDD) to ISO format."""
    if not date_str or len(date_str) < 8:
        return None
    try:
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    except:
        return None

def parse_dicom_time(time_str: str) -> Optional[str]:
    """Parse DICOM time format to HH:MM:SS."""
    if not time_str or len(time_str) < 6:
        return None
    try:
        return f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
    except:
        return None

def safe_str(val: Any, max_len: int = 255) -> Optional[str]:
    """Safely convert value to string, handling None and truncating."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    return s[:max_len] if len(s) > max_len else s

def parse_dicom_bytes(dicom_bytes: bytes, file_path: str) -> Optional[Dict[str, Any]]:
    """Parse DICOM file bytes and extract metadata."""
    try:
        import pydicom
        from pydicom.errors import InvalidDicomError
        
        ds = pydicom.dcmread(io.BytesIO(dicom_bytes), force=True)
        
        # Patient level
        patient_id = safe_str(getattr(ds, 'PatientID', None)) or 'UNKNOWN'
        patient_name = safe_str(getattr(ds, 'PatientName', None))
        patient_birth_date = parse_dicom_date(safe_str(getattr(ds, 'PatientBirthDate', None)))
        patient_sex = safe_str(getattr(ds, 'PatientSex', None), 16)
        patient_age = safe_str(getattr(ds, 'PatientAge', None), 16)
        
        # Study level
        study_instance_uid = safe_str(getattr(ds, 'StudyInstanceUID', None)) or hashlib.md5(dicom_bytes[:1000]).hexdigest()
        study_id = safe_str(getattr(ds, 'StudyID', None))
        study_date = parse_dicom_date(safe_str(getattr(ds, 'StudyDate', None)))
        study_time = parse_dicom_time(safe_str(getattr(ds, 'StudyTime', None)))
        study_description = safe_str(getattr(ds, 'StudyDescription', None), 500)
        accession_number = safe_str(getattr(ds, 'AccessionNumber', None))
        referring_physician = safe_str(getattr(ds, 'ReferringPhysicianName', None))
        
        # Series level
        series_instance_uid = safe_str(getattr(ds, 'SeriesInstanceUID', None)) or f"{study_instance_uid}_series"
        series_number = getattr(ds, 'SeriesNumber', None)
        series_description = safe_str(getattr(ds, 'SeriesDescription', None), 500)
        modality = safe_str(getattr(ds, 'Modality', None), 16)
        body_part = safe_str(getattr(ds, 'BodyPartExamined', None))
        
        # Instance level
        sop_instance_uid = safe_str(getattr(ds, 'SOPInstanceUID', None)) or hashlib.md5(dicom_bytes).hexdigest()
        sop_class_uid = safe_str(getattr(ds, 'SOPClassUID', None))
        instance_number = getattr(ds, 'InstanceNumber', None)
        
        # Equipment
        manufacturer = safe_str(getattr(ds, 'Manufacturer', None))
        manufacturer_model = safe_str(getattr(ds, 'ManufacturerModelName', None))
        station_name = safe_str(getattr(ds, 'StationName', None))
        institution_name = safe_str(getattr(ds, 'InstitutionName', None))
        software_version = safe_str(getattr(ds, 'SoftwareVersions', None))
        
        # Image parameters
        rows = getattr(ds, 'Rows', None)
        columns = getattr(ds, 'Columns', None)
        bits_allocated = getattr(ds, 'BitsAllocated', None)
        bits_stored = getattr(ds, 'BitsStored', None)
        pixel_spacing = safe_str(getattr(ds, 'PixelSpacing', None))
        slice_thickness = getattr(ds, 'SliceThickness', None)
        window_center = getattr(ds, 'WindowCenter', None)
        window_width = getattr(ds, 'WindowWidth', None)
        photometric = safe_str(getattr(ds, 'PhotometricInterpretation', None))
        transfer_syntax = safe_str(getattr(ds, 'file_meta', {}).get('TransferSyntaxUID', None) if hasattr(ds, 'file_meta') else None)
        
        return {
            'patient': {
                'patient_id': patient_id,
                'patient_name': patient_name,
                'patient_birth_date': patient_birth_date,
                'patient_sex': patient_sex,
                'patient_age': patient_age,
            },
            'study': {
                'study_instance_uid': study_instance_uid,
                'study_id': study_id,
                'study_date': study_date,
                'study_time': study_time,
                'study_description': study_description,
                'accession_number': accession_number,
                'referring_physician': referring_physician,
            },
            'series': {
                'series_instance_uid': series_instance_uid,
                'series_number': series_number,
                'series_description': series_description,
                'modality': modality,
                'body_part_examined': body_part,
            },
            'instance': {
                'sop_instance_uid': sop_instance_uid,
                'sop_class_uid': sop_class_uid,
                'instance_number': instance_number,
                'file_path': file_path,
                'file_size': len(dicom_bytes),
            },
            'equipment': {
                'manufacturer': manufacturer,
                'manufacturer_model': manufacturer_model,
                'station_name': station_name,
                'institution_name': institution_name,
                'software_version': software_version,
            },
            'image_params': {
                'rows': rows,
                'columns': columns,
                'bits_allocated': bits_allocated,
                'bits_stored': bits_stored,
                'pixel_spacing': pixel_spacing,
                'slice_thickness': slice_thickness,
                'window_center': window_center[0] if isinstance(window_center, (list, tuple)) else window_center,
                'window_width': window_width[0] if isinstance(window_width, (list, tuple)) else window_width,
                'photometric_interpretation': photometric,
                'transfer_syntax_uid': transfer_syntax,
            }
        }
    except Exception as e:
        print(f"Error parsing {file_path}: {e}")
        return None

def sql_value(val: Any) -> str:
    """Format value for SQL insertion."""
    if val is None:
        return 'NULL'
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, bool):
        return 'TRUE' if val else 'FALSE'
    # Escape single quotes
    s = str(val).replace("'", "''")
    return f"'{s}'"

def insert_patient(data: Dict) -> int:
    """Insert or get patient, return patient_key."""
    patient_id = data['patient_id']
    
    # Check if exists
    result = execute_sql(f"""
        SELECT PATIENT_KEY FROM {DATABASE}.{SCHEMA}.DICOM_PATIENT 
        WHERE PATIENT_ID = {sql_value(patient_id)}
    """)
    if result:
        return result[0]['PATIENT_KEY']
    
    # Insert new
    execute_sql_no_result(f"""
        INSERT INTO {DATABASE}.{SCHEMA}.DICOM_PATIENT 
        (PATIENT_ID, PATIENT_NAME, PATIENT_BIRTH_DATE, PATIENT_SEX, PATIENT_AGE)
        VALUES (
            {sql_value(patient_id)},
            {sql_value(data.get('patient_name'))},
            {sql_value(data.get('patient_birth_date'))},
            {sql_value(data.get('patient_sex'))},
            {sql_value(data.get('patient_age'))}
        )
    """)
    
    result = execute_sql(f"""
        SELECT PATIENT_KEY FROM {DATABASE}.{SCHEMA}.DICOM_PATIENT 
        WHERE PATIENT_ID = {sql_value(patient_id)}
    """)
    return result[0]['PATIENT_KEY'] if result else None

def insert_study(data: Dict, patient_key: int) -> int:
    """Insert or get study, return study_key."""
    study_uid = data['study_instance_uid']
    
    result = execute_sql(f"""
        SELECT STUDY_KEY FROM {DATABASE}.{SCHEMA}.DICOM_STUDY 
        WHERE STUDY_INSTANCE_UID = {sql_value(study_uid)}
    """)
    if result:
        return result[0]['STUDY_KEY']
    
    execute_sql_no_result(f"""
        INSERT INTO {DATABASE}.{SCHEMA}.DICOM_STUDY 
        (PATIENT_KEY, STUDY_INSTANCE_UID, STUDY_ID, STUDY_DATE, STUDY_TIME, 
         STUDY_DESCRIPTION, ACCESSION_NUMBER, REFERRING_PHYSICIAN_NAME)
        VALUES (
            {patient_key},
            {sql_value(study_uid)},
            {sql_value(data.get('study_id'))},
            {sql_value(data.get('study_date'))},
            {sql_value(data.get('study_time'))},
            {sql_value(data.get('study_description'))},
            {sql_value(data.get('accession_number'))},
            {sql_value(data.get('referring_physician'))}
        )
    """)
    
    result = execute_sql(f"""
        SELECT STUDY_KEY FROM {DATABASE}.{SCHEMA}.DICOM_STUDY 
        WHERE STUDY_INSTANCE_UID = {sql_value(study_uid)}
    """)
    return result[0]['STUDY_KEY'] if result else None

def insert_series(data: Dict, study_key: int) -> int:
    """Insert or get series, return series_key."""
    series_uid = data['series_instance_uid']
    
    result = execute_sql(f"""
        SELECT SERIES_KEY FROM {DATABASE}.{SCHEMA}.DICOM_SERIES 
        WHERE SERIES_INSTANCE_UID = {sql_value(series_uid)}
    """)
    if result:
        return result[0]['SERIES_KEY']
    
    execute_sql_no_result(f"""
        INSERT INTO {DATABASE}.{SCHEMA}.DICOM_SERIES 
        (STUDY_KEY, SERIES_INSTANCE_UID, SERIES_NUMBER, SERIES_DESCRIPTION, 
         MODALITY, BODY_PART_EXAMINED)
        VALUES (
            {study_key},
            {sql_value(series_uid)},
            {sql_value(data.get('series_number'))},
            {sql_value(data.get('series_description'))},
            {sql_value(data.get('modality'))},
            {sql_value(data.get('body_part_examined'))}
        )
    """)
    
    result = execute_sql(f"""
        SELECT SERIES_KEY FROM {DATABASE}.{SCHEMA}.DICOM_SERIES 
        WHERE SERIES_INSTANCE_UID = {sql_value(series_uid)}
    """)
    return result[0]['SERIES_KEY'] if result else None

def insert_instance(data: Dict, series_key: int) -> int:
    """Insert instance, return instance_key."""
    sop_uid = data['sop_instance_uid']
    
    # Check if exists
    result = execute_sql(f"""
        SELECT INSTANCE_KEY FROM {DATABASE}.{SCHEMA}.DICOM_INSTANCE 
        WHERE SOP_INSTANCE_UID = {sql_value(sop_uid)}
    """)
    if result:
        return result[0]['INSTANCE_KEY']
    
    execute_sql_no_result(f"""
        INSERT INTO {DATABASE}.{SCHEMA}.DICOM_INSTANCE 
        (SERIES_KEY, SOP_INSTANCE_UID, SOP_CLASS_UID, INSTANCE_NUMBER, 
         FILE_PATH, FILE_SIZE_BYTES)
        VALUES (
            {series_key},
            {sql_value(sop_uid)},
            {sql_value(data.get('sop_class_uid'))},
            {sql_value(data.get('instance_number'))},
            {sql_value(data.get('file_path'))},
            {sql_value(data.get('file_size'))}
        )
    """)
    
    result = execute_sql(f"""
        SELECT INSTANCE_KEY FROM {DATABASE}.{SCHEMA}.DICOM_INSTANCE 
        WHERE SOP_INSTANCE_UID = {sql_value(sop_uid)}
    """)
    return result[0]['INSTANCE_KEY'] if result else None

def insert_equipment(data: Dict, series_key: int):
    """Insert equipment info."""
    # Check if exists for this series
    result = execute_sql(f"""
        SELECT 1 FROM {DATABASE}.{SCHEMA}.DICOM_EQUIPMENT 
        WHERE SERIES_KEY = {series_key}
    """)
    if result:
        return
    
    execute_sql_no_result(f"""
        INSERT INTO {DATABASE}.{SCHEMA}.DICOM_EQUIPMENT 
        (SERIES_KEY, MANUFACTURER, MANUFACTURER_MODEL_NAME, STATION_NAME, 
         INSTITUTION_NAME, SOFTWARE_VERSIONS)
        VALUES (
            {series_key},
            {sql_value(data.get('manufacturer'))},
            {sql_value(data.get('manufacturer_model'))},
            {sql_value(data.get('station_name'))},
            {sql_value(data.get('institution_name'))},
            {sql_value(data.get('software_version'))}
        )
    """)

def insert_image_params(data: Dict, instance_key: int):
    """Insert image parameters."""
    result = execute_sql(f"""
        SELECT 1 FROM {DATABASE}.{SCHEMA}.DICOM_IMAGE_PARAMS 
        WHERE INSTANCE_KEY = {instance_key}
    """)
    if result:
        return
    
    execute_sql_no_result(f"""
        INSERT INTO {DATABASE}.{SCHEMA}.DICOM_IMAGE_PARAMS 
        (INSTANCE_KEY, ROWS, COLUMNS, BITS_ALLOCATED, BITS_STORED, 
         PIXEL_SPACING, SLICE_THICKNESS, WINDOW_CENTER, WINDOW_WIDTH,
         PHOTOMETRIC_INTERPRETATION, TRANSFER_SYNTAX_UID)
        VALUES (
            {instance_key},
            {sql_value(data.get('rows'))},
            {sql_value(data.get('columns'))},
            {sql_value(data.get('bits_allocated'))},
            {sql_value(data.get('bits_stored'))},
            {sql_value(data.get('pixel_spacing'))},
            {sql_value(data.get('slice_thickness'))},
            {sql_value(data.get('window_center'))},
            {sql_value(data.get('window_width'))},
            {sql_value(data.get('photometric_interpretation'))},
            {sql_value(data.get('transfer_syntax_uid'))}
        )
    """)

def download_file_from_stage(file_path: str) -> Optional[bytes]:
    """Download file from Snowflake stage using GET."""
    import subprocess
    import tempfile
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Use snow stage copy to download
        result = subprocess.run(
            ['snow', 'stage', 'copy', f'{STAGE}/{file_path}', tmpdir, '-c', CONNECTION_NAME],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode != 0:
            print(f"Download error for {file_path}: {result.stderr}")
            return None
        
        # Read the downloaded file
        local_path = os.path.join(tmpdir, os.path.basename(file_path))
        if os.path.exists(local_path):
            with open(local_path, 'rb') as f:
                return f.read()
    return None

def ingest_dicom_files():
    """Main ingestion function."""
    print("=" * 60)
    print("DICOM Ingestion Pipeline")
    print("=" * 60)
    
    # Get list of DICOM files from stage
    print("\nFetching file list from stage...")
    files = execute_sql(f"""
        SELECT RELATIVE_PATH, SIZE 
        FROM DIRECTORY({STAGE})
        WHERE RELATIVE_PATH ILIKE '%.dcm'
        ORDER BY SIZE ASC
    """)
    
    print(f"Found {len(files)} DICOM files to process")
    
    processed = 0
    errors = 0
    
    for i, file_info in enumerate(files):
        file_path = file_info['RELATIVE_PATH']
        file_size = file_info['SIZE']
        
        # Skip very large files for now (>100MB)
        if file_size > 100 * 1024 * 1024:
            print(f"[{i+1}/{len(files)}] Skipping large file: {file_path} ({file_size/1024/1024:.1f} MB)")
            continue
        
        print(f"[{i+1}/{len(files)}] Processing: {file_path} ({file_size/1024:.1f} KB)")
        
        try:
            # Download file
            dicom_bytes = download_file_from_stage(file_path)
            if not dicom_bytes:
                print(f"  ERROR: Could not download file")
                errors += 1
                continue
            
            # Parse DICOM
            metadata = parse_dicom_bytes(dicom_bytes, f"{STAGE}/{file_path}")
            if not metadata:
                print(f"  ERROR: Could not parse DICOM")
                errors += 1
                continue
            
            # Insert into tables
            patient_key = insert_patient(metadata['patient'])
            if not patient_key:
                print(f"  ERROR: Could not insert patient")
                errors += 1
                continue
            
            study_key = insert_study(metadata['study'], patient_key)
            if not study_key:
                print(f"  ERROR: Could not insert study")
                errors += 1
                continue
            
            series_key = insert_series(metadata['series'], study_key)
            if not series_key:
                print(f"  ERROR: Could not insert series")
                errors += 1
                continue
            
            instance_key = insert_instance(metadata['instance'], series_key)
            if not instance_key:
                print(f"  ERROR: Could not insert instance")
                errors += 1
                continue
            
            # Insert related tables
            insert_equipment(metadata['equipment'], series_key)
            insert_image_params(metadata['image_params'], instance_key)
            
            processed += 1
            print(f"  SUCCESS: Patient={patient_key}, Study={study_key}, Series={series_key}, Instance={instance_key}")
            
        except Exception as e:
            print(f"  ERROR: {e}")
            errors += 1
    
    print("\n" + "=" * 60)
    print(f"Ingestion Complete: {processed} processed, {errors} errors")
    print("=" * 60)
    
    # Show summary
    summary = execute_sql(f"""
        SELECT 
            (SELECT COUNT(*) FROM {DATABASE}.{SCHEMA}.DICOM_PATIENT) as patients,
            (SELECT COUNT(*) FROM {DATABASE}.{SCHEMA}.DICOM_STUDY) as studies,
            (SELECT COUNT(*) FROM {DATABASE}.{SCHEMA}.DICOM_SERIES) as series,
            (SELECT COUNT(*) FROM {DATABASE}.{SCHEMA}.DICOM_INSTANCE) as instances
    """)
    if summary:
        print(f"\nRepository Summary:")
        print(f"  Patients:  {summary[0]['PATIENTS']}")
        print(f"  Studies:   {summary[0]['STUDIES']}")
        print(f"  Series:    {summary[0]['SERIES']}")
        print(f"  Instances: {summary[0]['INSTANCES']}")

if __name__ == '__main__':
    ingest_dicom_files()
