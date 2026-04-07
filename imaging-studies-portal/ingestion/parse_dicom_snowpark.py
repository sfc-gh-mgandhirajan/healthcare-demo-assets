"""
DICOM Ingestion Pipeline for Medical Imaging Studies Portal
Snowpark Python UDF + pydicom for parsing DICOM files from S3/stage
"""
import os
import json
import hashlib
from datetime import datetime, date, time
from typing import Dict, List, Any, Optional, Tuple

import pydicom
from pydicom.dataset import Dataset
from pydicom.sequence import Sequence


def safe_get(ds: Dataset, tag: str, default=None) -> Any:
    """Safely retrieve a DICOM tag value."""
    try:
        if hasattr(ds, tag):
            val = getattr(ds, tag)
            if val is None:
                return default
            if isinstance(val, pydicom.valuerep.PersonName):
                return str(val)
            if isinstance(val, (list, pydicom.multival.MultiValue)):
                return [str(v) if isinstance(v, pydicom.valuerep.PersonName) else v for v in val]
            return val
        return default
    except Exception:
        return default


def parse_dicom_date(date_str: str) -> Optional[date]:
    """Parse DICOM date (DA) format YYYYMMDD."""
    if not date_str:
        return None
    try:
        return datetime.strptime(str(date_str).strip()[:8], '%Y%m%d').date()
    except Exception:
        return None


def parse_dicom_time(time_str: str) -> Optional[time]:
    """Parse DICOM time (TM) format HHMMSS.FFFFFF."""
    if not time_str:
        return None
    try:
        time_str = str(time_str).strip()
        if '.' in time_str:
            time_str = time_str.split('.')[0]
        time_str = time_str[:6].ljust(6, '0')
        return datetime.strptime(time_str, '%H%M%S').time()
    except Exception:
        return None


def parse_dicom_datetime(date_str: str, time_str: str) -> Optional[datetime]:
    """Combine DICOM date and time into datetime."""
    d = parse_dicom_date(date_str)
    t = parse_dicom_time(time_str)
    if d is None:
        return None
    if t is None:
        return datetime.combine(d, time(0, 0, 0))
    return datetime.combine(d, t)


def extract_patient(ds: Dataset, source_system: str = None) -> Dict[str, Any]:
    """Extract patient-level metadata."""
    return {
        'source_system': source_system,
        'patient_id': safe_get(ds, 'PatientID', ''),
        'issuer_of_patient_id': safe_get(ds, 'IssuerOfPatientID'),
        'patient_name': safe_get(ds, 'PatientName'),
        'patient_sex': safe_get(ds, 'PatientSex'),
        'patient_birth_date': parse_dicom_date(safe_get(ds, 'PatientBirthDate')),
        'patient_age': safe_get(ds, 'PatientAge'),
        'other_patient_ids': safe_get(ds, 'OtherPatientIDs'),
        'other_patient_names': safe_get(ds, 'OtherPatientNames'),
        'comments': safe_get(ds, 'PatientComments'),
    }


def extract_study(ds: Dataset) -> Dict[str, Any]:
    """Extract study-level metadata."""
    study_date = safe_get(ds, 'StudyDate')
    study_time = safe_get(ds, 'StudyTime')
    return {
        'study_instance_uid': safe_get(ds, 'StudyInstanceUID'),
        'accession_number': safe_get(ds, 'AccessionNumber'),
        'study_id': safe_get(ds, 'StudyID'),
        'study_datetime': parse_dicom_datetime(study_date, study_time),
        'study_date': parse_dicom_date(study_date),
        'study_time': parse_dicom_time(study_time),
        'study_description': safe_get(ds, 'StudyDescription'),
        'referring_physician': safe_get(ds, 'ReferringPhysicianName'),
        'admitting_diagnosis': safe_get(ds, 'AdmittingDiagnosesDescription'),
    }


def extract_series(ds: Dataset) -> Dict[str, Any]:
    """Extract series-level metadata."""
    series_date = safe_get(ds, 'SeriesDate')
    series_time = safe_get(ds, 'SeriesTime')
    return {
        'series_instance_uid': safe_get(ds, 'SeriesInstanceUID'),
        'series_number': safe_get(ds, 'SeriesNumber'),
        'modality': safe_get(ds, 'Modality', 'OT'),
        'body_part_examined': safe_get(ds, 'BodyPartExamined'),
        'laterality': safe_get(ds, 'Laterality'),
        'series_description': safe_get(ds, 'SeriesDescription'),
        'frame_of_reference_uid': safe_get(ds, 'FrameOfReferenceUID'),
        'patient_position': safe_get(ds, 'PatientPosition'),
        'performed_station_name': safe_get(ds, 'PerformedStationName'),
        'performed_location': safe_get(ds, 'PerformedLocation'),
        'series_date': parse_dicom_date(series_date),
        'series_time': parse_dicom_time(series_time),
        'protocol_name': safe_get(ds, 'ProtocolName'),
    }


def extract_instance(ds: Dataset, file_path: str = None, file_size: int = None) -> Dict[str, Any]:
    """Extract instance-level metadata."""
    acq_date = safe_get(ds, 'AcquisitionDate')
    acq_time = safe_get(ds, 'AcquisitionTime')
    content_date = safe_get(ds, 'ContentDate')
    content_time = safe_get(ds, 'ContentTime')
    
    image_type = safe_get(ds, 'ImageType')
    if image_type and not isinstance(image_type, list):
        image_type = [image_type]
    
    return {
        'sop_instance_uid': safe_get(ds, 'SOPInstanceUID'),
        'sop_class_uid': safe_get(ds, 'SOPClassUID'),
        'instance_number': safe_get(ds, 'InstanceNumber'),
        'image_type': image_type,
        'acquisition_datetime': parse_dicom_datetime(acq_date, acq_time),
        'content_datetime': parse_dicom_datetime(content_date, content_time),
        'acquisition_date': parse_dicom_date(acq_date),
        'acquisition_time': parse_dicom_time(acq_time),
        'content_date': parse_dicom_date(content_date),
        'content_time': parse_dicom_time(content_time),
        'number_of_frames': safe_get(ds, 'NumberOfFrames', 1),
        'specific_character_set': safe_get(ds, 'SpecificCharacterSet'),
        'burned_in_annotation': safe_get(ds, 'BurnedInAnnotation'),
        'presentation_intent': safe_get(ds, 'PresentationIntentType'),
        'file_path': file_path,
        'file_size_bytes': file_size,
        'transfer_syntax_uid': safe_get(ds.file_meta, 'TransferSyntaxUID') if hasattr(ds, 'file_meta') else None,
    }


def extract_equipment(ds: Dataset) -> Dict[str, Any]:
    """Extract equipment metadata."""
    return {
        'manufacturer': safe_get(ds, 'Manufacturer'),
        'manufacturer_model_name': safe_get(ds, 'ManufacturerModelName'),
        'device_serial_number': safe_get(ds, 'DeviceSerialNumber'),
        'software_versions': safe_get(ds, 'SoftwareVersions'),
        'institution_name': safe_get(ds, 'InstitutionName'),
        'institution_address': safe_get(ds, 'InstitutionAddress'),
        'station_name': safe_get(ds, 'StationName'),
        'institutional_dept_name': safe_get(ds, 'InstitutionalDepartmentName'),
    }


def extract_image_pixel(ds: Dataset) -> Dict[str, Any]:
    """Extract image pixel module metadata."""
    window_center = safe_get(ds, 'WindowCenter')
    window_width = safe_get(ds, 'WindowWidth')
    if window_center and not isinstance(window_center, list):
        window_center = [window_center]
    if window_width and not isinstance(window_width, list):
        window_width = [window_width]
    
    return {
        'image_rows': safe_get(ds, 'Rows'),
        'image_columns': safe_get(ds, 'Columns'),
        'number_of_frames': safe_get(ds, 'NumberOfFrames'),
        'samples_per_pixel': safe_get(ds, 'SamplesPerPixel'),
        'photometric_interpretation': safe_get(ds, 'PhotometricInterpretation'),
        'bits_allocated': safe_get(ds, 'BitsAllocated'),
        'bits_stored': safe_get(ds, 'BitsStored'),
        'high_bit': safe_get(ds, 'HighBit'),
        'pixel_representation': safe_get(ds, 'PixelRepresentation'),
        'planar_configuration': safe_get(ds, 'PlanarConfiguration'),
        'rescale_intercept': safe_get(ds, 'RescaleIntercept'),
        'rescale_slope': safe_get(ds, 'RescaleSlope'),
        'window_center': window_center,
        'window_width': window_width,
    }


def extract_image_plane(ds: Dataset) -> Dict[str, Any]:
    """Extract image plane module metadata."""
    pixel_spacing = safe_get(ds, 'PixelSpacing')
    if pixel_spacing and not isinstance(pixel_spacing, list):
        pixel_spacing = [pixel_spacing]
    
    ipp = safe_get(ds, 'ImagePositionPatient')
    if ipp and not isinstance(ipp, list):
        ipp = list(ipp)
    
    iop = safe_get(ds, 'ImageOrientationPatient')
    if iop and not isinstance(iop, list):
        iop = list(iop)
    
    return {
        'pixel_spacing': pixel_spacing,
        'slice_thickness': safe_get(ds, 'SliceThickness'),
        'image_position_patient': ipp,
        'image_orientation_patient': iop,
        'spacing_between_slices': safe_get(ds, 'SpacingBetweenSlices'),
        'position_reference_indicator': safe_get(ds, 'PositionReferenceIndicator'),
    }


def extract_dose_summary(ds: Dataset) -> Dict[str, Any]:
    """Extract dose/exposure metadata for CT/Radiography."""
    return {
        'ctdi_vol': safe_get(ds, 'CTDIvol'),
        'dose_length_product': safe_get(ds, 'DoseLengthProduct'),
        'exposure_time': safe_get(ds, 'ExposureTime'),
        'kvp': safe_get(ds, 'KVP'),
        'xray_tube_current': safe_get(ds, 'XRayTubeCurrent'),
        'exposure': safe_get(ds, 'Exposure'),
        'acquisition_protocol': safe_get(ds, 'AcquisitionProtocol'),
    }


def extract_file_location(storage_uri: str, storage_provider: str = 'S3') -> Dict[str, Any]:
    """Create file location entry for storage tracking."""
    parts = storage_uri.replace('s3://', '').split('/', 1)
    bucket = parts[0] if parts else ''
    key = parts[1] if len(parts) > 1 else ''
    
    return {
        'storage_uri': storage_uri,
        'storage_provider': storage_provider,
        'storage_container': bucket,
        'object_key': key,
        'ingestion_source': 'SNOWPARK_PIPELINE',
    }


def parse_dicom_file(file_path: str, file_content: bytes = None, source_system: str = None) -> Dict[str, Any]:
    """
    Parse a single DICOM file and return all extracted metadata.
    
    Args:
        file_path: Path to the DICOM file or storage URI
        file_content: Optional bytes content of the file (for stage files)
        source_system: Source system identifier
    
    Returns:
        Dictionary containing all extracted metadata organized by entity
    """
    if file_content:
        import io
        ds = pydicom.dcmread(io.BytesIO(file_content), force=True)
        file_size = len(file_content)
    else:
        ds = pydicom.dcmread(file_path, force=True)
        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else None
    
    result = {
        'patient': extract_patient(ds, source_system),
        'study': extract_study(ds),
        'series': extract_series(ds),
        'instance': extract_instance(ds, file_path, file_size),
        'equipment': extract_equipment(ds),
        'image_pixel': extract_image_pixel(ds),
        'image_plane': extract_image_plane(ds),
        'dose_summary': extract_dose_summary(ds),
        '_file_path': file_path,
        '_modality': safe_get(ds, 'Modality', 'OT'),
    }
    
    return result


def serialize_for_snowflake(data: Dict[str, Any]) -> str:
    """Serialize extracted metadata to JSON for Snowflake insertion."""
    def json_serializer(obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, time):
            return obj.isoformat()
        if hasattr(obj, '__str__'):
            return str(obj)
        return None
    
    return json.dumps(data, default=json_serializer)


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        result = parse_dicom_file(sys.argv[1])
        print(serialize_for_snowflake(result))
