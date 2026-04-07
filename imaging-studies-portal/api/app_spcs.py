"""
Medical Imaging Studies Portal - REST API (SPCS Version)
Flask API for Cohort Builder, Study Explorer, and DICOM Viewer integration
Uses Snowflake connector with token-based authentication for SPCS deployment
"""
import os
import json
from datetime import datetime, date
from typing import Dict, List, Any, Optional

from flask import Flask, request, jsonify
from flask_cors import CORS
import snowflake.connector

app = Flask(__name__)
CORS(app)

DATABASE = 'UNSTRUCTURED_HEALTHDATA'
SCHEMA = 'IMAGING_STUDIES'


def get_connection():
    """Get Snowflake connection using SPCS token authentication."""
    return snowflake.connector.connect(
        host=os.environ.get('SNOWFLAKE_HOST'),
        account=os.environ.get('SNOWFLAKE_ACCOUNT'),
        authenticator='oauth',
        token=open('/snowflake/session/token').read(),
        database=DATABASE,
        schema=SCHEMA
    )


def execute_sql(sql: str) -> List[Dict[str, Any]]:
    """Execute SQL and return results as list of dicts."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        raise Exception(f"SQL error: {str(e)}")


@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'timestamp': datetime.utcnow().isoformat()})


@app.route('/api/filters/options', methods=['GET'])
def get_filter_options():
    try:
        modalities = execute_sql(f"""
            SELECT DISTINCT modality, COUNT(*) as count 
            FROM {DATABASE}.{SCHEMA}.dicom_series 
            WHERE modality IS NOT NULL
            GROUP BY modality ORDER BY count DESC
        """)
        modalities = [{'value': r.get('MODALITY'), 'count': r.get('COUNT')} for r in modalities if r.get('MODALITY')]
        
        body_parts = execute_sql(f"""
            SELECT DISTINCT body_part_examined, COUNT(*) as count 
            FROM {DATABASE}.{SCHEMA}.dicom_series 
            WHERE body_part_examined IS NOT NULL
            GROUP BY body_part_examined ORDER BY count DESC LIMIT 50
        """)
        body_parts = [{'value': r.get('BODY_PART_EXAMINED'), 'count': r.get('COUNT')} for r in body_parts if r.get('BODY_PART_EXAMINED')]
        
        manufacturers = execute_sql(f"""
            SELECT DISTINCT manufacturer, COUNT(*) as count 
            FROM {DATABASE}.{SCHEMA}.dicom_equipment 
            WHERE manufacturer IS NOT NULL
            GROUP BY manufacturer ORDER BY count DESC
        """)
        manufacturers = [{'value': r.get('MANUFACTURER'), 'count': r.get('COUNT')} for r in manufacturers if r.get('MANUFACTURER')]
        
        institutions = execute_sql(f"""
            SELECT DISTINCT institution_name, COUNT(*) as count 
            FROM {DATABASE}.{SCHEMA}.dicom_equipment 
            WHERE institution_name IS NOT NULL
            GROUP BY institution_name ORDER BY count DESC LIMIT 50
        """)
        institutions = [{'value': r.get('INSTITUTION_NAME'), 'count': r.get('COUNT')} for r in institutions if r.get('INSTITUTION_NAME')]
        
        date_range = {'min': None, 'max': None}
        date_result = execute_sql(f"SELECT MIN(study_date) as min_date, MAX(study_date) as max_date FROM {DATABASE}.{SCHEMA}.dicom_study")
        if date_result:
            min_val = date_result[0].get('MIN_DATE')
            max_val = date_result[0].get('MAX_DATE')
            date_range = {
                'min': min_val.isoformat() if hasattr(min_val, 'isoformat') else min_val,
                'max': max_val.isoformat() if hasattr(max_val, 'isoformat') else max_val
            }
        
        patient_sex = execute_sql(f"""
            SELECT DISTINCT patient_sex, COUNT(*) as count 
            FROM {DATABASE}.{SCHEMA}.dicom_patient 
            WHERE patient_sex IS NOT NULL GROUP BY patient_sex
        """)
        patient_sex = [{'value': r.get('PATIENT_SEX'), 'count': r.get('COUNT')} for r in patient_sex if r.get('PATIENT_SEX')]
        
        return jsonify({
            'modalities': modalities, 'bodyParts': body_parts, 'manufacturers': manufacturers,
            'institutions': institutions, 'studyDateRange': date_range, 'patientSex': patient_sex
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/cohorts/preview', methods=['POST'])
def preview_cohort():
    filters = request.get_json() or {}
    try:
        where_sql = build_where_sql(filters)
        count_result = execute_sql(f"""
            SELECT COUNT(DISTINCT p.patient_key) as patient_count, COUNT(DISTINCT st.study_key) as study_count,
                   COUNT(DISTINCT ser.series_key) as series_count, COUNT(DISTINCT i.instance_key) as instance_count
            FROM {DATABASE}.{SCHEMA}.dicom_study st
            JOIN {DATABASE}.{SCHEMA}.dicom_patient p ON st.patient_key = p.patient_key
            JOIN {DATABASE}.{SCHEMA}.dicom_series ser ON st.study_key = ser.study_key
            LEFT JOIN {DATABASE}.{SCHEMA}.dicom_instance i ON ser.series_key = i.series_key
            LEFT JOIN {DATABASE}.{SCHEMA}.dicom_equipment eq ON ser.series_key = eq.series_key
            WHERE {where_sql}
        """)
        counts = {
            'patients': count_result[0].get('PATIENT_COUNT', 0) if count_result else 0,
            'studies': count_result[0].get('STUDY_COUNT', 0) if count_result else 0,
            'series': count_result[0].get('SERIES_COUNT', 0) if count_result else 0,
            'instances': count_result[0].get('INSTANCE_COUNT', 0) if count_result else 0
        }
        
        modality_result = execute_sql(f"""
            SELECT ser.modality, COUNT(DISTINCT st.study_key) as count
            FROM {DATABASE}.{SCHEMA}.dicom_study st
            JOIN {DATABASE}.{SCHEMA}.dicom_patient p ON st.patient_key = p.patient_key
            JOIN {DATABASE}.{SCHEMA}.dicom_series ser ON st.study_key = ser.study_key
            LEFT JOIN {DATABASE}.{SCHEMA}.dicom_equipment eq ON ser.series_key = eq.series_key
            WHERE {where_sql} GROUP BY ser.modality ORDER BY count DESC
        """)
        modality_breakdown = [{'modality': r.get('MODALITY'), 'count': r.get('COUNT')} for r in modality_result]
        
        monthly_result = execute_sql(f"""
            SELECT DATE_TRUNC('month', st.study_date) as month, COUNT(DISTINCT st.study_key) as count
            FROM {DATABASE}.{SCHEMA}.dicom_study st
            JOIN {DATABASE}.{SCHEMA}.dicom_patient p ON st.patient_key = p.patient_key
            JOIN {DATABASE}.{SCHEMA}.dicom_series ser ON st.study_key = ser.study_key
            LEFT JOIN {DATABASE}.{SCHEMA}.dicom_equipment eq ON ser.series_key = eq.series_key
            WHERE {where_sql} AND st.study_date IS NOT NULL GROUP BY month ORDER BY month
        """)
        monthly_trend = [{'month': r.get('MONTH').isoformat() if hasattr(r.get('MONTH'), 'isoformat') else r.get('MONTH'), 'count': r.get('COUNT')} for r in monthly_result]
        
        return jsonify({'counts': counts, 'modalityBreakdown': modality_breakdown, 'monthlyTrend': monthly_trend})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/cohorts/studies', methods=['POST'])
def get_cohort_studies():
    body = request.get_json() or {}
    filters = body.get('filters', {})
    page = body.get('page', 1)
    page_size = min(body.get('pageSize', 25), 100)
    sort_by = body.get('sortBy', 'study_date')
    sort_order = body.get('sortOrder', 'desc').upper()
    if sort_order not in ('ASC', 'DESC'):
        sort_order = 'DESC'
    allowed_sort = ['study_date', 'patient_name', 'modality', 'study_description', 'accession_number']
    if sort_by not in allowed_sort:
        sort_by = 'study_date'
    
    try:
        where_sql = build_where_sql(filters)
        offset = (page - 1) * page_size
        
        count_result = execute_sql(f"""
            SELECT COUNT(DISTINCT st.study_key) as total
            FROM {DATABASE}.{SCHEMA}.dicom_study st
            JOIN {DATABASE}.{SCHEMA}.dicom_patient p ON st.patient_key = p.patient_key
            JOIN {DATABASE}.{SCHEMA}.dicom_series ser ON st.study_key = ser.study_key
            LEFT JOIN {DATABASE}.{SCHEMA}.dicom_equipment eq ON ser.series_key = eq.series_key
            WHERE {where_sql}
        """)
        total_count = count_result[0].get('TOTAL', 0) if count_result else 0
        
        sort_column_map = {'study_date': 'st.study_date', 'patient_name': 'p.patient_name', 'modality': 'modalities', 'study_description': 'st.study_description', 'accession_number': 'st.accession_number'}
        sort_col = sort_column_map.get(sort_by, 'st.study_date')
        
        studies_result = execute_sql(f"""
            SELECT st.study_key, st.study_instance_uid, st.accession_number, st.study_date, st.study_description,
                   p.patient_key, p.patient_id, p.patient_name, p.patient_sex, p.patient_birth_date,
                   ARRAY_AGG(DISTINCT ser.modality) as modalities, COUNT(DISTINCT ser.series_key) as series_count,
                   COUNT(DISTINCT i.instance_key) as instance_count
            FROM {DATABASE}.{SCHEMA}.dicom_study st
            JOIN {DATABASE}.{SCHEMA}.dicom_patient p ON st.patient_key = p.patient_key
            JOIN {DATABASE}.{SCHEMA}.dicom_series ser ON st.study_key = ser.study_key
            LEFT JOIN {DATABASE}.{SCHEMA}.dicom_instance i ON ser.series_key = i.series_key
            LEFT JOIN {DATABASE}.{SCHEMA}.dicom_equipment eq ON ser.series_key = eq.series_key
            WHERE {where_sql}
            GROUP BY st.study_key, st.study_instance_uid, st.accession_number, st.study_date, st.study_description,
                     p.patient_key, p.patient_id, p.patient_name, p.patient_sex, p.patient_birth_date
            ORDER BY {sort_col} {sort_order} NULLS LAST LIMIT {page_size} OFFSET {offset}
        """)
        
        studies = []
        for row in studies_result:
            modalities = row.get('MODALITIES', [])
            if isinstance(modalities, str):
                try:
                    modalities = json.loads(modalities)
                except:
                    modalities = [modalities]
            study_date = row.get('STUDY_DATE')
            birth_date = row.get('PATIENT_BIRTH_DATE')
            studies.append({
                'studyKey': row.get('STUDY_KEY'), 'studyInstanceUID': row.get('STUDY_INSTANCE_UID'),
                'accessionNumber': row.get('ACCESSION_NUMBER'),
                'studyDate': study_date.isoformat() if hasattr(study_date, 'isoformat') else study_date,
                'studyDescription': row.get('STUDY_DESCRIPTION'), 'patientKey': row.get('PATIENT_KEY'),
                'patientId': row.get('PATIENT_ID'), 'patientName': row.get('PATIENT_NAME'),
                'patientSex': row.get('PATIENT_SEX'),
                'patientBirthDate': birth_date.isoformat() if hasattr(birth_date, 'isoformat') else birth_date,
                'modalities': modalities, 'seriesCount': row.get('SERIES_COUNT'), 'instanceCount': row.get('INSTANCE_COUNT')
            })
        
        return jsonify({
            'studies': studies,
            'pagination': {'page': page, 'pageSize': page_size, 'totalCount': total_count,
                          'totalPages': (total_count + page_size - 1) // page_size if total_count > 0 else 0}
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/studies/<study_instance_uid>', methods=['GET'])
def get_study_details(study_instance_uid: str):
    try:
        study_result = execute_sql(f"""
            SELECT st.study_key, st.study_instance_uid, st.accession_number, st.study_id, st.study_datetime,
                   st.study_date, st.study_description, st.referring_physician, st.number_of_series, st.number_of_instances,
                   p.patient_key, p.patient_id, p.patient_name, p.patient_sex, p.patient_birth_date, p.patient_age
            FROM {DATABASE}.{SCHEMA}.dicom_study st
            JOIN {DATABASE}.{SCHEMA}.dicom_patient p ON st.patient_key = p.patient_key
            WHERE st.study_instance_uid = '{study_instance_uid}'
        """)
        if not study_result:
            return jsonify({'error': 'Study not found'}), 404
        
        row = study_result[0]
        study_key = row.get('STUDY_KEY')
        study_date = row.get('STUDY_DATE')
        study_dt = row.get('STUDY_DATETIME')
        birth_date = row.get('PATIENT_BIRTH_DATE')
        
        study = {
            'studyKey': study_key, 'studyInstanceUID': row.get('STUDY_INSTANCE_UID'),
            'accessionNumber': row.get('ACCESSION_NUMBER'), 'studyId': row.get('STUDY_ID'),
            'studyDatetime': study_dt.isoformat() if hasattr(study_dt, 'isoformat') else study_dt,
            'studyDate': study_date.isoformat() if hasattr(study_date, 'isoformat') else study_date,
            'studyDescription': row.get('STUDY_DESCRIPTION'), 'referringPhysician': row.get('REFERRING_PHYSICIAN'),
            'numberOfSeries': row.get('NUMBER_OF_SERIES'), 'numberOfInstances': row.get('NUMBER_OF_INSTANCES'),
            'patient': {
                'patientKey': row.get('PATIENT_KEY'), 'patientId': row.get('PATIENT_ID'),
                'patientName': row.get('PATIENT_NAME'), 'patientSex': row.get('PATIENT_SEX'),
                'patientBirthDate': birth_date.isoformat() if hasattr(birth_date, 'isoformat') else birth_date,
                'patientAge': row.get('PATIENT_AGE')
            }
        }
        
        series_result = execute_sql(f"""
            SELECT ser.series_key, ser.series_instance_uid, ser.series_number, ser.modality, ser.body_part_examined,
                   ser.series_description, ser.protocol_name, ser.number_of_instances,
                   eq.manufacturer, eq.manufacturer_model_name, eq.station_name
            FROM {DATABASE}.{SCHEMA}.dicom_series ser
            LEFT JOIN {DATABASE}.{SCHEMA}.dicom_equipment eq ON ser.series_key = eq.series_key
            WHERE ser.study_key = {study_key} ORDER BY ser.series_number
        """)
        
        series_list = []
        for sr in series_result:
            series_list.append({
                'seriesKey': sr.get('SERIES_KEY'), 'seriesInstanceUID': sr.get('SERIES_INSTANCE_UID'),
                'seriesNumber': sr.get('SERIES_NUMBER'), 'modality': sr.get('MODALITY'),
                'bodyPartExamined': sr.get('BODY_PART_EXAMINED'), 'seriesDescription': sr.get('SERIES_DESCRIPTION'),
                'protocolName': sr.get('PROTOCOL_NAME'), 'numberOfInstances': sr.get('NUMBER_OF_INSTANCES'),
                'equipment': {'manufacturer': sr.get('MANUFACTURER'), 'modelName': sr.get('MANUFACTURER_MODEL_NAME'), 'stationName': sr.get('STATION_NAME')}
            })
        study['series'] = series_list
        return jsonify(study)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/series/<series_instance_uid>/instances', methods=['GET'])
def get_series_instances(series_instance_uid: str):
    try:
        instances_result = execute_sql(f"""
            SELECT i.instance_key, i.sop_instance_uid, i.sop_class_uid, i.instance_number, i.image_type, i.number_of_frames, i.file_path
            FROM {DATABASE}.{SCHEMA}.dicom_instance i
            JOIN {DATABASE}.{SCHEMA}.dicom_series ser ON i.series_key = ser.series_key
            WHERE ser.series_instance_uid = '{series_instance_uid}' ORDER BY i.instance_number
        """)
        
        instances = []
        for row in instances_result:
            image_type = row.get('IMAGE_TYPE', [])
            if isinstance(image_type, str):
                try:
                    image_type = json.loads(image_type)
                except:
                    image_type = [image_type]
            instances.append({
                'instanceKey': row.get('INSTANCE_KEY'), 'sopInstanceUID': row.get('SOP_INSTANCE_UID'),
                'sopClassUID': row.get('SOP_CLASS_UID'), 'instanceNumber': row.get('INSTANCE_NUMBER'),
                'imageType': image_type, 'numberOfFrames': row.get('NUMBER_OF_FRAMES'), 'filePath': row.get('FILE_PATH')
            })
        return jsonify({'seriesInstanceUID': series_instance_uid, 'instances': instances, 'count': len(instances)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/instances/<sop_instance_uid>/view', methods=['GET'])
def get_viewer_config(sop_instance_uid: str):
    try:
        result = execute_sql(f"""
            SELECT i.instance_key, i.sop_instance_uid, i.sop_class_uid, i.number_of_frames,
                   ser.series_instance_uid, ser.modality, st.study_instance_uid
            FROM {DATABASE}.{SCHEMA}.dicom_instance i
            JOIN {DATABASE}.{SCHEMA}.dicom_series ser ON i.series_key = ser.series_key
            JOIN {DATABASE}.{SCHEMA}.dicom_study st ON ser.study_key = st.study_key
            WHERE i.sop_instance_uid = '{sop_instance_uid}'
        """)
        if not result:
            return jsonify({'error': 'Instance not found'}), 404
        
        row = result[0]
        study_uid = row.get('STUDY_INSTANCE_UID')
        series_uid = row.get('SERIES_INSTANCE_UID')
        instance_uid = row.get('SOP_INSTANCE_UID')
        
        return jsonify({
            'instanceKey': row.get('INSTANCE_KEY'), 'sopInstanceUID': instance_uid,
            'sopClassUID': row.get('SOP_CLASS_UID'), 'numberOfFrames': row.get('NUMBER_OF_FRAMES'),
            'seriesInstanceUID': series_uid, 'modality': row.get('MODALITY'), 'studyInstanceUID': study_uid,
            'viewerConfig': {
                'wadoUriRoot': os.getenv('WADO_URI_ROOT', '/wado'),
                'wadoUri': f"/wado/studies/{study_uid}/series/{series_uid}/instances/{instance_uid}",
                'ohifViewerUrl': f"/ohif/viewer?StudyInstanceUIDs={study_uid}"
            }
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/cohorts', methods=['GET'])
def list_cohorts():
    try:
        result = execute_sql(f"""
            SELECT cohort_key, cohort_name, cohort_description, created_by, filter_criteria, is_active, created_at, updated_at
            FROM {DATABASE}.{SCHEMA}.cohort_definition WHERE is_active = TRUE ORDER BY created_at DESC
        """)
        cohorts = []
        for row in result:
            filter_criteria = row.get('FILTER_CRITERIA', {})
            if isinstance(filter_criteria, str):
                try:
                    filter_criteria = json.loads(filter_criteria)
                except:
                    filter_criteria = {}
            created_at = row.get('CREATED_AT')
            updated_at = row.get('UPDATED_AT')
            cohorts.append({
                'cohortKey': row.get('COHORT_KEY'), 'cohortName': row.get('COHORT_NAME'),
                'cohortDescription': row.get('COHORT_DESCRIPTION'), 'createdBy': row.get('CREATED_BY'),
                'filterCriteria': filter_criteria, 'isActive': row.get('IS_ACTIVE'),
                'createdAt': created_at.isoformat() if hasattr(created_at, 'isoformat') else created_at,
                'updatedAt': updated_at.isoformat() if hasattr(updated_at, 'isoformat') else updated_at
            })
        return jsonify({'cohorts': cohorts})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/cohorts', methods=['POST'])
def save_cohort():
    body = request.get_json() or {}
    cohort_name = body.get('cohortName')
    cohort_description = body.get('cohortDescription', '')
    filter_criteria = body.get('filterCriteria', {})
    created_by = body.get('createdBy', 'API_USER')
    if not cohort_name:
        return jsonify({'error': 'cohortName is required'}), 400
    try:
        filter_json = json.dumps(filter_criteria).replace("'", "''")
        execute_sql(f"""
            INSERT INTO {DATABASE}.{SCHEMA}.cohort_definition (cohort_name, cohort_description, created_by, filter_criteria)
            VALUES ('{cohort_name}', '{cohort_description}', '{created_by}', PARSE_JSON('{filter_json}'))
        """)
        result = execute_sql(f"SELECT MAX(cohort_key) as cohort_key FROM {DATABASE}.{SCHEMA}.cohort_definition")
        cohort_key = result[0].get('COHORT_KEY') if result else None
        return jsonify({'message': 'Cohort saved successfully', 'cohortKey': cohort_key}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def build_where_sql(filters: Dict[str, Any]) -> str:
    clauses = []
    if filters.get('modalities'):
        modalities = filters['modalities']
        if isinstance(modalities, str):
            modalities = [modalities]
        mod_list = "', '".join(modalities)
        clauses.append(f"ser.modality IN ('{mod_list}')")
    if filters.get('bodyParts'):
        body_parts = filters['bodyParts']
        if isinstance(body_parts, str):
            body_parts = [body_parts]
        bp_list = "', '".join(body_parts)
        clauses.append(f"ser.body_part_examined IN ('{bp_list}')")
    if filters.get('studyDateFrom'):
        clauses.append(f"st.study_date >= '{filters['studyDateFrom']}'")
    if filters.get('studyDateTo'):
        clauses.append(f"st.study_date <= '{filters['studyDateTo']}'")
    if filters.get('manufacturers'):
        manufacturers = filters['manufacturers']
        if isinstance(manufacturers, str):
            manufacturers = [manufacturers]
        mfg_list = "', '".join(manufacturers)
        clauses.append(f"eq.manufacturer IN ('{mfg_list}')")
    if filters.get('institutions'):
        institutions = filters['institutions']
        if isinstance(institutions, str):
            institutions = [institutions]
        inst_list = "', '".join(institutions)
        clauses.append(f"eq.institution_name IN ('{inst_list}')")
    if filters.get('patientSex'):
        patient_sex = filters['patientSex']
        if isinstance(patient_sex, str):
            patient_sex = [patient_sex]
        sex_list = "', '".join(patient_sex)
        clauses.append(f"p.patient_sex IN ('{sex_list}')")
    if filters.get('patientId'):
        clauses.append(f"p.patient_id ILIKE '%{filters['patientId']}%'")
    if filters.get('accessionNumber'):
        clauses.append(f"st.accession_number ILIKE '%{filters['accessionNumber']}%'")
    return ' AND '.join(clauses) if clauses else '1=1'


if __name__ == '__main__':
    port = int(os.getenv('API_PORT', 5000))
    app.run(host='0.0.0.0', port=port)
