# Demo Walkthrough: Extending for X12 278 (Prior Authorization)

This walkthrough demonstrates the "dynamic path" — how a customer uses `/edi:extend` to add support for a new transaction type.

## Scenario

A healthcare payer wants to process 278 Prior Authorization Request/Response transactions. They already have the 837/835/834 pipeline running via the backbone. Now they need to add 278.

## Step-by-Step

### 1. User Invokes `/edi:extend`

```
User: /edi:extend
```

Router loads the extension workflow.

### 2. Gate G1: Connection

Skill verifies Snowflake connection:
```sql
SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE();
-- <your-org>-<your-account>, <YOUR_USER>, SYSADMIN

SHOW DATABASES LIKE 'X12_EDI_AI';
-- Found. Schemas: CLAIMS, ENROLLMENTS, REMITTANCES, GOLD
```

### 3. Gate G2: Repository

Skill detects backbone repo at `/workspace/x12repo/`:
```
Found x12-openflow-quickstart at /workspace/x12repo/
  - ParseX12ToJSON.py ✓
  - field_maps.py ✓
  - Output mode: WRITE
```

### 4. Gate G3: Format Specification

```
Skill: What EDI transaction type do you want to add?

User: X12 278 — Prior Authorization. I don't have an implementation guide,
      but I know it uses HL hierarchy levels and has UM/HCR segments for
      the review request and decision.

Skill: I'll use AI to propose field mappings for 278 based on the X12 standard.
       Here's what I found:
       
       278 Prior Authorization Request/Response:
       - GS Functional ID: HN
       - ST Code: 278
       - Record Boundary: HL (each hierarchy level = one request/response)
       - Key Segments: HL, UM, HCR, HSD, DTP, NM1, REF, TRN
       
       Shall I generate the field map with these segments?

User: Yes, go for it.
```

### 5. Phase 1: Field Map Generation

Skill generates:
```python
"278": {
    "transaction_type_name": "Health Care Services Review - Prior Authorization",
    "record_boundary_segment": "HL",
    "fields": {
        "HL": {
            "01": "hierarchy_id",
            "02": "hierarchy_parent_id",
            "03": "hierarchy_level_code",
            "04": "hierarchy_child_code",
        },
        "TRN": {
            "02": "trace_number",
            "03": "trace_originating_company_id",
            "04": "trace_reference_id",
        },
        "UM": {
            "01": "request_category_code",      # [AI-inferred]
            "02": "certification_type_code",     # [AI-inferred]
            "03": "service_type_code",           # [AI-inferred]
            "04": "health_care_service_location",# [AI-inferred]
            "06": "level_of_care_code",          # [AI-inferred]
        },
        "HCR": {
            "01": "action_code",                 # [AI-inferred]
            "02": "review_identification_number",# [AI-inferred]
            "03": "review_decision_reason",      # [AI-inferred]
        },
        "HSD": {
            "01": "quantity_qualifier",          # [AI-inferred]
            "02": "authorized_quantity",         # [AI-inferred]
            "03": "unit_basis",                  # [AI-inferred]
            "05": "period_count",                # [AI-inferred]
        },
        "NM1_IL": {
            "03": "subscriber_last_name",
            "04": "subscriber_first_name",
            "08": "subscriber_id_qualifier",
            "09": "subscriber_id",
        },
        "NM1_1P": {
            "03": "provider_last_name",
            "04": "provider_first_name",
            "09": "provider_npi",
        },
        "NM1_PR": {
            "03": "payer_name",
            "09": "payer_id",
        },
        "DTP_472": {
            "03": "service_date",
        },
        "DTP_435": {
            "03": "admission_date",
        },
        "DTP_096": {
            "03": "discharge_date",
        },
        "REF_BB": {
            "02": "authorization_number",        # [AI-inferred]
        },
        "SV1": {
            "01": "procedure_code",
            "02": "service_charge_amount",
            "04": "service_unit_count",
        },
    },
},
```

```
Skill: Does this look correct? Fields marked [AI-inferred] are from my 
       knowledge of the 278 standard — verify against your payer's guide.

User: Looks good. Add it.
```

Skill writes the entry to `field_maps.py` and adds GS/ST mappings.

### 6. Phase 2: Landing DDL

```sql
CREATE SCHEMA IF NOT EXISTS X12_EDI_AI.AUTHORIZATIONS;

CREATE TABLE IF NOT EXISTS X12_EDI_AI.AUTHORIZATIONS.LANDING_278_AUTHORIZATIONS (
    -- Envelope
    ISA_SENDER_ID           VARCHAR,
    ISA_RECEIVER_ID         VARCHAR,
    ISA_DATE                VARCHAR,
    ISA_TIME                VARCHAR,
    ISA_CONTROL_NUMBER      VARCHAR,
    GS_FUNCTIONAL_ID        VARCHAR,
    GS_SENDER_CODE          VARCHAR,
    GS_RECEIVER_CODE        VARCHAR,
    GS_DATE                 VARCHAR,
    GS_CONTROL_NUMBER       VARCHAR,
    ST_CONTROL_NUMBER       VARCHAR,
    
    -- HL hierarchy
    HIERARCHY_ID            VARCHAR,
    HIERARCHY_PARENT_ID     VARCHAR,
    HIERARCHY_LEVEL_CODE    VARCHAR,
    HIERARCHY_CHILD_CODE    VARCHAR,
    
    -- TRN trace
    TRACE_NUMBER            VARCHAR,
    TRACE_ORIGINATING_COMPANY_ID VARCHAR,
    TRACE_REFERENCE_ID      VARCHAR,
    
    -- UM review request
    REQUEST_CATEGORY_CODE   VARCHAR,
    CERTIFICATION_TYPE_CODE VARCHAR,
    SERVICE_TYPE_CODE       VARCHAR,
    HEALTH_CARE_SERVICE_LOCATION VARCHAR,
    LEVEL_OF_CARE_CODE      VARCHAR,
    
    -- HCR decision
    ACTION_CODE             VARCHAR,
    REVIEW_IDENTIFICATION_NUMBER VARCHAR,
    REVIEW_DECISION_REASON  VARCHAR,
    
    -- HSD quantity
    QUANTITY_QUALIFIER      VARCHAR,
    AUTHORIZED_QUANTITY     VARCHAR,
    UNIT_BASIS              VARCHAR,
    PERIOD_COUNT            VARCHAR,
    
    -- Subscriber
    SUBSCRIBER_LAST_NAME    VARCHAR,
    SUBSCRIBER_FIRST_NAME   VARCHAR,
    SUBSCRIBER_ID_QUALIFIER VARCHAR,
    SUBSCRIBER_ID           VARCHAR,
    
    -- Provider
    PROVIDER_LAST_NAME      VARCHAR,
    PROVIDER_FIRST_NAME     VARCHAR,
    PROVIDER_NPI            VARCHAR,
    
    -- Payer
    PAYER_NAME              VARCHAR,
    PAYER_ID                VARCHAR,
    
    -- Dates
    SERVICE_DATE            VARCHAR,
    ADMISSION_DATE          VARCHAR,
    DISCHARGE_DATE          VARCHAR,
    
    -- Reference
    AUTHORIZATION_NUMBER    VARCHAR,
    
    -- Service line
    PROCEDURE_CODE          VARCHAR,
    SERVICE_CHARGE_AMOUNT   VARCHAR,
    SERVICE_UNIT_COUNT      VARCHAR,
    
    -- Metadata
    RAW_SEGMENTS            VARCHAR,
    RECORD_INDEX            VARCHAR,
    INGESTION_TIMESTAMP     VARCHAR
);
```

### 7. Phase 3: Gold Dynamic Table

```sql
CREATE OR REPLACE DYNAMIC TABLE X12_EDI_AI.GOLD.GOLD_278_AUTHORIZATIONS
    TARGET_LAG = '10 minutes'
    WAREHOUSE = APP_WH
AS
SELECT
    TRACE_NUMBER,
    SUBSCRIBER_ID,
    SUBSCRIBER_LAST_NAME || ', ' || SUBSCRIBER_FIRST_NAME AS SUBSCRIBER_NAME,
    PROVIDER_NPI,
    PROVIDER_LAST_NAME || ', ' || PROVIDER_FIRST_NAME AS PROVIDER_NAME,
    PAYER_NAME,
    REQUEST_CATEGORY_CODE,
    CERTIFICATION_TYPE_CODE,
    SERVICE_TYPE_CODE,
    ACTION_CODE,
    REVIEW_IDENTIFICATION_NUMBER,
    REVIEW_DECISION_REASON,
    TRY_TO_NUMBER(AUTHORIZED_QUANTITY) AS AUTHORIZED_QUANTITY,
    PROCEDURE_CODE,
    TRY_TO_DECIMAL(SERVICE_CHARGE_AMOUNT, 12, 2) AS SERVICE_CHARGE_AMOUNT,
    TRY_TO_DATE(SERVICE_DATE) AS SERVICE_DATE,
    TRY_TO_DATE(ADMISSION_DATE) AS ADMISSION_DATE,
    TRY_TO_DATE(DISCHARGE_DATE) AS DISCHARGE_DATE,
    AUTHORIZATION_NUMBER,
    -- AI Enrichment: decode procedure codes and classify urgency
    AI_COMPLETE(
        'claude-sonnet-4-6',
        CONCAT(
            'Given prior authorization: procedure_code=', COALESCE(PROCEDURE_CODE, 'N/A'),
            ', service_type=', COALESCE(SERVICE_TYPE_CODE, 'N/A'),
            ', action_code=', COALESCE(ACTION_CODE, 'N/A'),
            '. Return JSON: {"procedure_description": "...", "urgency_level": "routine|urgent|emergent", "approval_likelihood": "high|medium|low"}'
        )
    ) AS AI_ENRICHMENT,
    ST_CONTROL_NUMBER,
    INGESTION_TIMESTAMP
FROM X12_EDI_AI.AUTHORIZATIONS.LANDING_278_AUTHORIZATIONS
WHERE TRACE_NUMBER IS NOT NULL;
```

### 8. Phase 4: Tests

Skill generates `tests/test_278.py` and `tests/sample_data/sample_278.x12`.

### 9. Summary

```
Extension Complete!
===================
Transaction type: 278 (Health Care Services Review - Prior Authorization)
Files generated:
  - field_maps.py (updated with 278 entry)
  - sql/03_landing_278.sql
  - sql/04_gold_278.sql
  - tests/test_278.py
  - tests/sample_data/sample_278.x12
  - config/x12_known_types.yaml (updated)

Next steps:
  - Run tests: pytest tests/test_278.py -v
  - Deploy: /edi:deploy (build NAR + wire Openflow route for 278)
```

## Demo Value

This walkthrough takes ~5 minutes interactively. Compare to Databricks where the customer would need to:
1. Understand X12 278 segment structure deeply
2. Write a custom Python parser module from scratch
3. Manually create Delta tables
4. Wire Spark Structured Streaming
5. No AI enrichment available natively

**Our message**: "What took your team weeks of EDI expertise, we do in minutes with an AI-guided skill."
