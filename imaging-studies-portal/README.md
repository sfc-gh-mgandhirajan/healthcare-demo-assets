# Medical Imaging Studies Portal

A clinical research platform for imaging cohort analytics, built on Snowflake with DICOM metadata fidelity.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Medical Imaging Studies Portal                │
├─────────────────────────────────────────────────────────────────┤
│  React Frontend (Cohort Builder → Study Explorer → OHIF Viewer) │
├─────────────────────────────────────────────────────────────────┤
│                    Flask REST API Layer                          │
├─────────────────────────────────────────────────────────────────┤
│              Snowflake (IMAGING_STUDIES schema)                  │
│  20 DICOM metadata tables + Cohort definitions                   │
├─────────────────────────────────────────────────────────────────┤
│         Snowpark + pydicom Ingestion Pipeline                    │
└─────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Snowflake Schema (`UNSTRUCTURED_HEALTHDATA.IMAGING_STUDIES`)

20 tables following DICOM hierarchy:
- **Core**: `dicom_patient`, `dicom_study`, `dicom_series`, `dicom_instance`, `dicom_frame`
- **Technical**: `dicom_equipment`, `dicom_image_pixel`, `dicom_image_plane`, `dicom_dose_summary`
- **Workflow**: `dicom_procedure_step`
- **Derived**: `dicom_segmentation_metadata`, `dicom_structured_report_header`
- **Storage**: `dicom_file_location`
- **Generic**: `dicom_element`, `dicom_sequence_item`
- **ML/AI**: `embedding_model`, `image_embedding`, `embedding_evaluation`
- **Cohorts**: `cohort_definition`, `cohort_membership`

### 2. Ingestion Pipeline (`ingestion/`)

- `parse_dicom_snowpark.py` - DICOM metadata extraction using pydicom
- `ingest_pipeline.py` - Snowpark-based ingestion from stages

### 3. REST API (`api/`)

Flask API with endpoints:
- `GET /api/filters/options` - Available filter values
- `POST /api/cohorts/preview` - Cohort summary counts
- `POST /api/cohorts/studies` - Paginated study list
- `GET /api/studies/{studyInstanceUID}` - Study details
- `GET /api/series/{seriesInstanceUID}/instances` - Instance list
- `GET /api/instances/{sopInstanceUID}/view` - Viewer config
- `GET/POST /api/cohorts` - Saved cohorts

### 4. React Frontend (`frontend/`)

- **Cohort Builder**: Multi-select filters (modality, body part, manufacturer, etc.) with live preview
- **Study Explorer**: Sortable/paginated table of matching studies
- **Study Detail View**: Full metadata with series/instance drill-down
- **DICOM Viewer**: OHIF integration via iframe

## Quick Start

### 1. Start API Server

```bash
cd api
pip install -r requirements.txt
export SNOWFLAKE_CONNECTION_NAME=your_connection
python app.py
```

### 2. Start Frontend

```bash
cd frontend
npm install
cp .env.example .env
npm run dev
```

### 3. Ingest DICOM Files

```bash
cd ingestion
python ingest_pipeline.py @UNSTRUCTURED_HEALTHDATA.DICOM_RAW.DICOM_STAGE/studies/
```

## Environment Variables

### API
- `SNOWFLAKE_CONNECTION_NAME` - Snowflake connection name
- `API_PORT` - Server port (default: 5000)
- `WADO_URI_ROOT` - DICOMweb WADO-RS endpoint
- `OHIF_VIEWER_URL` - OHIF viewer base URL

### Frontend
- `VITE_API_URL` - Backend API URL
- `VITE_OHIF_URL` - OHIF viewer URL

## Database Info

- **Database**: `UNSTRUCTURED_HEALTHDATA`
- **Schema**: `IMAGING_STUDIES`
- **Tables**: 20 (see above)

## Related

- `dicom-parser` skill - Core DICOM parsing functionality
- OHIF Viewer - Open source DICOM viewer (https://ohif.org/)
