---
name: phase-udf-lite
description: Deploy the Python UDF lite path — stored procedure + task + Snowpipe
parent_skill: edi-deploy
phase_id: DEPLOY_P_LITE
---

# Phase: Python UDF Lite Deployment

## Purpose
Deploy the EDI parser as a Snowflake Python stored procedure with a scheduled task. No Openflow dependency.

## Architecture

```
Internal Stage (@edi_raw) → Snowpipe → RAW_EDI table → Task (every 5 min) → parse_edi() proc → Typed Landing Tables
```

## Steps

### 1. Create Infrastructure

```sql
-- Stage for incoming EDI files
CREATE STAGE IF NOT EXISTS X12_EDI_AI.PUBLIC.EDI_RAW
    DIRECTORY = (ENABLE = TRUE);

-- Raw staging table
CREATE TABLE IF NOT EXISTS X12_EDI_AI.PUBLIC.RAW_EDI (
    FILE_NAME       VARCHAR,
    FILE_CONTENT    VARCHAR,
    FILE_SIZE       NUMBER,
    LOADED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PROCESSED       BOOLEAN DEFAULT FALSE
);

-- Snowpipe for auto-ingest (optional — user can also COPY INTO manually)
CREATE PIPE IF NOT EXISTS X12_EDI_AI.PUBLIC.EDI_PIPE
    AUTO_INGEST = FALSE  -- set TRUE if S3 event notifications configured
AS
COPY INTO X12_EDI_AI.PUBLIC.RAW_EDI (FILE_NAME, FILE_CONTENT, FILE_SIZE)
FROM (
    SELECT 
        METADATA$FILENAME,
        $1,
        METADATA$FILE_ROW_NUMBER
    FROM @X12_EDI_AI.PUBLIC.EDI_RAW
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = NONE RECORD_DELIMITER = NONE);
```

### 2. Deploy Parse Procedure

The stored procedure uses the same parsing logic as the NAR processor but runs in Snowflake:

```sql
CREATE OR REPLACE PROCEDURE X12_EDI_AI.PUBLIC.PARSE_EDI(
    P_FILE_NAME VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import json

def detect_delimiters(content):
    """Detect X12 delimiters from ISA segment."""
    if len(content) < 106:
        return None
    return {
        'element': content[3],
        'sub_element': content[104],
        'segment': content[105]
    }

def parse_x12(content, field_maps, tx_filter=None):
    """Parse X12 content into flat JSON records."""
    delims = detect_delimiters(content)
    if not delims:
        return []
    
    segments = content.split(delims['segment'])
    records = []
    current_record = {}
    current_tx_type = None
    envelope = {}
    
    for seg in segments:
        seg = seg.strip()
        if not seg:
            continue
        elements = seg.split(delims['element'])
        seg_id = elements[0]
        
        # Envelope handling
        if seg_id == 'ISA' and len(elements) >= 13:
            envelope['isa_sender_id'] = elements[6].strip()
            envelope['isa_receiver_id'] = elements[8].strip()
            envelope['isa_date'] = elements[9]
            envelope['isa_time'] = elements[10]
            envelope['isa_control_number'] = elements[13] if len(elements) > 13 else ''
            continue
        elif seg_id == 'GS' and len(elements) >= 6:
            envelope['gs_functional_id'] = elements[1]
            envelope['gs_sender_code'] = elements[2]
            envelope['gs_receiver_code'] = elements[3]
            envelope['gs_date'] = elements[4]
            envelope['gs_control_number'] = elements[6] if len(elements) > 6 else ''
            continue
        elif seg_id == 'ST' and len(elements) >= 2:
            current_tx_type = elements[1]
            envelope['st_control_number'] = elements[2] if len(elements) > 2 else ''
            continue
        elif seg_id in ('SE', 'GE', 'IEA'):
            continue
        
        if current_tx_type and current_tx_type in field_maps:
            fm = field_maps[current_tx_type]
            boundary = fm['record_boundary_segment']
            
            # Check for record boundary
            if seg_id == boundary:
                if current_record:
                    current_record.update(envelope)
                    records.append(current_record)
                current_record = {}
            
            # Map fields
            qualifier = elements[1] if len(elements) > 1 else ''
            seg_key = f"{seg_id}_{qualifier}" if f"{seg_id}_{qualifier}" in fm['fields'] else seg_id
            
            if seg_key in fm['fields']:
                for pos, field_name in fm['fields'][seg_key].items():
                    idx = int(pos)
                    if idx < len(elements):
                        val = elements[idx]
                        if delims['sub_element'] in val:
                            val = val.split(delims['sub_element'])[0]
                        if val:
                            if field_name in current_record:
                                current_record[field_name] = current_record[field_name] + ';' + val
                            else:
                                current_record[field_name] = val
    
    # Don't forget the last record
    if current_record:
        current_record.update(envelope)
        records.append(current_record)
    
    return records

def main(session, p_file_name):
    # Load field maps from config table (or hardcode for PoC)
    # For production: store field_maps in a Snowflake table or stage
    from x12_field_maps import FIELD_MAPS  # imported from stage
    
    # Get unprocessed files
    if p_file_name:
        query = f"SELECT FILE_NAME, FILE_CONTENT FROM X12_EDI_AI.PUBLIC.RAW_EDI WHERE FILE_NAME = '{p_file_name}' AND NOT PROCESSED"
    else:
        query = "SELECT FILE_NAME, FILE_CONTENT FROM X12_EDI_AI.PUBLIC.RAW_EDI WHERE NOT PROCESSED LIMIT 100"
    
    df = session.sql(query).collect()
    total_records = 0
    
    for row in df:
        records = parse_x12(row['FILE_CONTENT'], FIELD_MAPS)
        
        if records:
            # Route to appropriate landing table based on transaction type
            # Group by GS functional ID → transaction type → target table
            for record in records:
                gs_fic = record.get('gs_functional_id', '')
                # Determine target table and insert
                # (simplified — production version routes per type)
                pass
            total_records += len(records)
        
        # Mark as processed
        session.sql(f"UPDATE X12_EDI_AI.PUBLIC.RAW_EDI SET PROCESSED = TRUE WHERE FILE_NAME = '{row['FILE_NAME']}'").collect()
    
    return f"Processed {len(df)} files, extracted {total_records} records"
$$;
```

### 3. Create Scheduled Task

```sql
CREATE OR REPLACE TASK X12_EDI_AI.PUBLIC.PARSE_EDI_TASK
    WAREHOUSE = APP_WH
    SCHEDULE = '5 MINUTE'
AS
CALL X12_EDI_AI.PUBLIC.PARSE_EDI(NULL);

-- Resume task
ALTER TASK X12_EDI_AI.PUBLIC.PARSE_EDI_TASK RESUME;
```

### 4. Usage

```sql
-- Manual: upload file and parse
PUT file:///path/to/file.edi @X12_EDI_AI.PUBLIC.EDI_RAW;
COPY INTO X12_EDI_AI.PUBLIC.RAW_EDI ...;
CALL X12_EDI_AI.PUBLIC.PARSE_EDI('file.edi');

-- Automatic: task picks up new files every 5 minutes
```

## Output
- SQL script with all infrastructure DDL
- Stored procedure code
- Task definition
- Usage examples
