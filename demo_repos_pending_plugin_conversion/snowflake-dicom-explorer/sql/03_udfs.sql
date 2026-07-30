-- =============================================================================
-- 03: Python UDFs
-- These UDFs read binary DICOM files directly from an external stage,
-- demonstrating Snowflake's native unstructured data processing.
-- No external ETL tools, no data movement, no infrastructure to manage.
-- =============================================================================

USE DATABASE EW_IMAGING_DB;
USE SCHEMA EXPLORER;

-- -----------------------------------------------------------------------------
-- UDF 1: EXTRACT_DICOM_METADATA
-- Reads a binary DICOM file from stage, parses all metadata tags via pydicom,
-- and returns a VARIANT (JSON) with ~100+ clinical and technical attributes.
-- Skips pixel data and sequence types for efficiency.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION EXTRACT_DICOM_METADATA(FILE_PATH STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pydicom')
HANDLER = 'extract_metadata'
AS $$
import pydicom
import io
from snowflake.snowpark.files import SnowflakeFile

def extract_metadata(file_path):
    try:
        with SnowflakeFile.open(file_path, 'rb') as f:
            dcm_bytes = f.read()
        ds = pydicom.dcmread(io.BytesIO(dcm_bytes))

        result = {}
        for elem in ds:
            if elem.VR in ('OW', 'OB', 'OF', 'OD', 'UN'):
                continue
            if elem.VR == 'SQ':
                continue
            keyword = elem.keyword if elem.keyword else f"TAG_{elem.tag.group:04X}_{elem.tag.element:04X}"
            if not keyword:
                continue
            try:
                result[keyword.upper()] = str(elem.value)
            except:
                pass

        if hasattr(ds, 'file_meta'):
            for elem in ds.file_meta:
                keyword = elem.keyword if elem.keyword else f"META_{elem.tag.group:04X}_{elem.tag.element:04X}"
                if not keyword:
                    continue
                try:
                    result[keyword.upper()] = str(elem.value)
                except:
                    pass

        return result
    except Exception as e:
        return {'ERROR': str(e)}
$$;


-- -----------------------------------------------------------------------------
-- UDF 2: RENDER_DICOM_SLICE
-- Reads a binary DICOM file, applies Hounsfield unit rescaling (slope/intercept),
-- applies DICOM windowing (window center/width), and returns a base64-encoded PNG.
-- This enables real-time medical image rendering directly from Snowflake.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION RENDER_DICOM_SLICE(
    FILE_PATH STRING,
    WINDOW_CENTER FLOAT,
    WINDOW_WIDTH FLOAT
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pydicom', 'Pillow', 'numpy')
HANDLER = 'render_slice'
AS $$
import pydicom
import numpy as np
from PIL import Image
import io
import base64
from snowflake.snowpark.files import SnowflakeFile

def render_slice(file_path, window_center, window_width):
    try:
        with SnowflakeFile.open(file_path, 'rb') as f:
            dcm_bytes = f.read()
        ds = pydicom.dcmread(io.BytesIO(dcm_bytes))
        pixel_array = ds.pixel_array.astype(np.float64)

        slope = float(getattr(ds, 'RescaleSlope', 1))
        intercept = float(getattr(ds, 'RescaleIntercept', 0))
        pixel_array = pixel_array * slope + intercept

        wc = float(window_center)
        ww = float(window_width)
        lower = wc - ww / 2.0
        upper = wc + ww / 2.0
        pixel_array = np.clip(pixel_array, lower, upper)
        pixel_array = ((pixel_array - lower) / (upper - lower) * 255.0).astype(np.uint8)

        img = Image.fromarray(pixel_array, mode='L')
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        return base64.b64encode(buf.getvalue()).decode('utf-8')
    except Exception as e:
        return f'ERROR:{str(e)}'
$$;
