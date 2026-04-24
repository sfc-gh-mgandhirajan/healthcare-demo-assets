---
name: dicom-parser
description: "Parse DICOM medical image metadata and create standardized 19-table data model on Snowflake. Full entity extraction (patient, study, series, instance, frame, equipment, image pixel/plane, procedure step, dose, segmentation, structured reports), validation, quality scoring, and Snowflake direct-load."
parent_skill: hcls-provider-imaging
---

# DICOM Metadata Parser

Parse DICOM file metadata and load into Snowflake's 19-table normalized data model covering the full DICOM hierarchy plus dose, segmentation, structured reports, and embeddings.

## When to Load

- Extracting metadata from DICOM files (.dcm)
- Building radiology/imaging metadata data models on Snowflake
- Loading imaging study information into Snowflake tables
- Creating PACS analytics foundations
- Dose tracking and radiation exposure analysis
- Validating DICOM metadata completeness and quality

## SQL References

All DDLs and queries are in the `references/` directory:
- **`references/data_model_ddl.sql`** — All 19 CREATE TABLE DDLs (core hierarchy, equipment, pixel, plane, procedure, dose, segmentation, SR, file location, elements, embeddings)
- **`references/validation_queries.sql`** — Referential integrity checks, quality scoring queries

## Quick Start

```bash
python scripts/parse_dicom.py ./dicom_folder --recursive --output-dir ./output
python scripts/parse_dicom.py ./dicom_folder --recursive --snowflake --database {database} --schema {schema}
python scripts/parse_dicom.py ./dicom_folder --recursive --snowflake --database {database} --schema {schema} --validate --quality-score
```

| Flag | Behavior |
|------|----------|
| `--snowflake` | Creates tables (if not exists), parses DICOM, loads via `write_pandas()` — skips CSV |
| `--validate` | Runs mandatory tag checks, data type conformance, referential integrity |
| `--quality-score` | Computes per-study completeness score (0-100) |
| `--analyze-only` | Inventory scan without parsing — counts by modality, date range, equipment |

## Comprehensive DICOM Data Model

Patient -> Study -> Series -> Instance -> Frame, with supporting entities for equipment, procedure context, dose tracking, and derived objects.

### Entity Relationship Diagram

```
DicomPatient (1) --> (many) DicomStudy
DicomStudy (1) --> (many) DicomSeries
DicomSeries (1) --> (many) DicomInstance
DicomInstance (1) --> (0..many) DicomFrame
DicomSeries (1) --> (0..many) DicomEquipment
DicomStudy (1) --> (0..many) DicomProcedureStep
DicomSeries/Study (1) --> (0..many) DicomDoseSummary
DicomInstance (1) --> (0..1) DicomImagePixel
DicomInstance/DicomFrame (1) --> (0..1) DicomImagePlane
DicomInstance (1) --> (0..many) DicomElement
DicomElement (SQ) (1) --> (0..many) DicomSequenceItem
DicomInstance (1) --> (0..many) DicomFileLocation
DicomInstance (SEG) (1) --> (0..many) DicomSegmentationMetadata
DicomInstance (SR) (1) --> (0..1) DicomStructuredReportHeader
DicomInstance (1) --> (0..many) ImageEmbedding
```

### Table Summary (19 Tables)

| # | Table | FK Parent | Purpose |
|---|-------|-----------|---------|
| 1 | `dicom_patient` | — | Patient demographics (PatientID, Name, Sex, DOB) |
| 2 | `dicom_study` | `dicom_patient` | Study-level (StudyInstanceUID, Date, Description) |
| 3 | `dicom_series` | `dicom_study` | Series-level (Modality, BodyPart, Protocol) |
| 4 | `dicom_instance` | `dicom_series` | Instance/image-level (SOPInstanceUID, SOPClassUID) |
| 5 | `dicom_frame` | `dicom_instance` | Multi-frame per-frame data (position, slice) |
| 6 | `dicom_equipment` | `dicom_series` | Scanner/equipment info (Manufacturer, Station) |
| 7 | `dicom_image_pixel` | `dicom_instance` | Pixel data params (Rows, Cols, Bits, Rescale) |
| 8 | `dicom_image_plane` | `dicom_instance` | Spatial geometry (PixelSpacing, SliceThickness) |
| 9 | `dicom_procedure_step` | `dicom_study` | Requested/performed procedure context |
| 10 | `dicom_dose_summary` | `dicom_series`, `dicom_study` | Radiation dose (CTDIvol, DLP, kVp) |
| 11 | `dicom_segmentation_metadata` | `dicom_instance` | SEG object segments and labels |
| 12 | `dicom_structured_report_header` | `dicom_instance` | SR document metadata |
| 13 | `dicom_file_location` | `dicom_instance` | Physical file storage references |
| 14 | `dicom_element` | `dicom_instance` | Generic DICOM tag/value store |
| 15 | `dicom_sequence_item` | `dicom_element` | Nested sequence items |
| 16 | `image_embedding` | `dicom_instance` | Vector embeddings for similarity search |
| 17 | `embedding_model` | — | Embedding model registry |
| 18 | `embedding_evaluation` | `embedding_model` | Model evaluation metrics |
| 19 | `radiology_reports` | `dicom_study` | Radiology report text and metadata |

Full DDLs: `references/data_model_ddl.sql`

## Enhanced Parser Capabilities

The parser extracts all 19 entities using pydicom. Key tags per entity:

**Core Hierarchy** — Patient: (0010,0010/0020/0021/0030/0040), Study: (0020,000D), (0008,0020/0030/0050/0090/1030), Series: (0020,000E/0011), (0008,0060/103E), (0018,0015/1030), Instance: (0008,0016/0018), (0020,0013), (0008,0008), (0028,0008)

**Frame-Level** — From Per-Frame Functional Groups (5200,9230): frame number, content datetime, per-frame ImagePositionPatient/OrientationPatient, SliceLocation, TemporalPositionIndex

**Equipment** — (0008,0070/0080/1010/1090), (0018,1000/1020) including SoftwareVersions for fleet drift detection

**Image Pixel** — (0028,xxxx): Rows, Columns, BitsAllocated/Stored, HighBit, PixelRepresentation, PhotometricInterpretation, SamplesPerPixel, RescaleSlope/Intercept, WindowCenter/Width

**Image Plane** — (0028,0030) PixelSpacing, (0018,0050) SliceThickness, (0020,0032) ImagePositionPatient, (0020,0037) ImageOrientationPatient, (0018,0088) SpacingBetweenSlices

**Procedure Step** — (0040,xxxx): requested/performed procedure IDs, descriptions, coded sequences

**Dose Summary** — From CT Dose Report SR or acquisition tags: CTDIvol, DLP, kVp, XRayTubeCurrent, ExposureTime/Exposure. Falls back to series-level tags when dose SR unavailable.

**Segmentation** — When SOPClassUID = 1.2.840.10008.5.1.4.1.1.66.4: Segment Sequence for number/label/description/type, Anatomic Region and Property code sequences, Referenced Series UID

**SR Headers** — CompletionFlag, VerificationFlag, DocumentTitle, Coding Scheme, referenced instance UIDs

**File Location** — Source file path, file size (os.stat), transfer syntax UID, SHA-256 checksum

**Element & Sequence Item** — Generic extraction for all DataElements: tag group/element, VR, VM, typed value. For SQ elements, recursive nested extraction with full sequence_path tracking.

## Validation & Quality Scoring

Run with `--validate` and `--quality-score` flags, or post-load using queries from `references/validation_queries.sql`.

### Mandatory Tag Checks

| Entity | Required Tags | Check |
|--------|--------------|-------|
| Patient | PatientID | NOT NULL, non-empty |
| Study | StudyInstanceUID, StudyDate | Valid UID format, valid date |
| Series | SeriesInstanceUID, Modality | Modality in known code set |
| Instance | SOPInstanceUID, SOPClassUID | Valid UID format |

### Data Type Conformance

| VR | Rule | VR | Rule |
|----|------|-----|------|
| DA | YYYYMMDD, valid calendar date | TM | HHMMSS.FFFFFF |
| UI | Max 64 chars, digits+dots only | IS | Parseable as integer |
| DS | Parseable as decimal | CS | Max 16 chars, uppercase+digits+space |

MANDATORY STOPPING POINT: Review validation results before proceeding. If quality_score < 50 for >20% of studies, investigate source data quality with user.

## Modality-Specific Tag Extraction

### CT — Dose & Acquisition
| Tag | Name | Tag | Name |
|-----|------|-----|------|
| (0018,0060) | KVP | (0018,1151) | XRayTubeCurrent |
| (0018,1150) | ExposureTime | (0018,1152) | Exposure (mAs) |
| (0018,9345) | CTDIvol | (0018,9311) | DLP |

### MR — Sequence Parameters
| Tag | Name | Tag | Name |
|-----|------|-----|------|
| (0018,0080) | RepetitionTime (TR) | (0018,0081) | EchoTime (TE) |
| (0018,0082) | InversionTime (TI) | (0018,0087) | MagneticFieldStrength |

### Modality Codes
| Code | Modality | Code | Modality | Code | Modality |
|------|----------|------|----------|------|----------|
| CT | Computed Tomography | MR | Magnetic Resonance | US | Ultrasound |
| CR | Computed Radiography | DX | Digital Radiography | MG | Mammography |
| NM | Nuclear Medicine | PT | PET | SEG | Segmentation |

## Workflow

### Step 1: Analyze DICOM Files
```bash
python scripts/parse_dicom.py ./dicom_folder --recursive --analyze-only
```
Review: file count by modality, study/series/instance counts, date ranges, equipment, missing tags.

MANDATORY STOPPING POINT: Confirm file inventory and target schema with user before parsing.

### Step 2: Create Snowflake Schema
Run DDL from `references/data_model_ddl.sql`.

MANDATORY STOPPING POINT: Confirm tables created. Show `SHOW TABLES` output.

### Step 3: Parse and Load
```bash
python scripts/parse_dicom.py ./dicom_folder --recursive \
    --snowflake --database {database} --schema {schema} \
    --validate --quality-score
```

### Step 4: Validate Results
Run queries from `references/validation_queries.sql`.

MANDATORY STOPPING POINT: Present validation results and quality score distribution.

## Dependencies

**Required:** pydicom>=2.4.0, pandas>=2.0.0, snowflake-connector-python>=3.0.0, python-gdcm>=3.0.0

**PHI Warning:** DICOM files contain PHI. De-identify before loading or apply masking post-load via `imaging-governance`. Apply dynamic data masking on PHI columns (patient_name, patient_id, patient_birth_date, referring_physician). Enable ACCESS_HISTORY.

## Stopping Points

- After Step 1 analysis — confirm file inventory and modality breakdown
- After Step 2 schema creation — confirm tables exist
- After Step 4 validation — review quality scores before downstream use
- If quality_score < 50 for >20% of studies — investigate data source

## Output

- 19 Snowflake tables populated with DICOM metadata
- Validation report (mandatory tag completeness, data type conformance, referential integrity)
- Quality score per study (0-100)
- Parse summary: files processed, entities extracted, errors encountered
