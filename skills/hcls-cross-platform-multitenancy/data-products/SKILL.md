---
name: data-products
description: "Data product lifecycle for multi-tenant platforms: ingestion (Snowpipe, Streaming, Kafka), transformation (dbt, Dynamic Tables, Tasks), curation, data quality, catalog. Industry-agnostic. Triggers: data product, ingestion, Snowpipe, streaming, Kafka, transformation, Dynamic Table, dbt, curation, data pipeline, data quality, data catalog, ETL, ELT."
platform_affinities:
  produces:
    - tables
    - views
    - dynamic tables
    - streams
    - tasks
  benefits_from:
    - skill: dynamic-tables
      when: "continuous or incremental data transformation pipelines are needed"
    - skill: dbt-projects-on-snowflake
      when: "dbt is the customer's preferred transformation framework"
    - skill: data-quality
      when: "data quality checks or anomaly detection are required on ingested data"
---

# Data Products & Lifecycle

## When to Use

**Load** this skill when the customer needs guidance on building data products in a multi-tenant platform — from ingestion through transformation to curation.

---

## Step 1: Understand the Customer's Data Products

Gather:

| Input | Description |
|-------|-------------|
| Source systems | What systems feed data into Snowflake (APIs, files, databases, event streams)? |
| Data entities | What are the key tables/datasets? (use customer's exact names) |
| Ingestion pattern | Batch (hourly/daily), micro-batch, streaming, event-driven? |
| Transformation needs | Simple aggregations, complex joins, ML feature engineering? |
| Consumption patterns | Dashboards, APIs, data shares, ML models, Cortex Agents? |
| Data quality requirements | SLAs, freshness, completeness, uniqueness constraints? |
| Tenant isolation in data | Is there a `TENANT_ID` column? Per-tenant source files/topics? |

---

## Step 2: Ingestion Layer

### Ingestion Methods by Pattern

| Method | Best For | Tenant Isolation Approach |
|--------|----------|--------------------------|
| **Snowpipe** | Continuous file-based ingestion | Per-tenant stage directories or `TENANT_ID` in filename/payload |
| **Snowpipe Streaming** | Low-latency event streams | `TENANT_ID` in message payload |
| **Kafka Connector** | Enterprise event pipelines | Topic-per-tenant or `TENANT_ID` field in messages |
| **External Tables** | Data lake integration | Per-tenant prefix in external location |
| **COPY INTO** | Scheduled batch loads | Per-tenant files or filtered loads |

### MTT Ingestion Template

```sql
CREATE OR REPLACE PIPE {{DATA_DB}}.{{SCHEMA}}.PIPE_{{TABLE_NAME}} AS
COPY INTO {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}_RAW
FROM (
    SELECT
        $1:tenant_id::VARCHAR AS TENANT_ID,
        $1:{{field_1}}::{{TYPE_1}} AS {{COLUMN_1}},
        $1:{{field_2}}::{{TYPE_2}} AS {{COLUMN_2}},
        CURRENT_TIMESTAMP() AS INGESTED_AT
    FROM @{{DATA_DB}}.{{SCHEMA}}.{{STAGE_NAME}}
)
FILE_FORMAT = (TYPE = '{{FORMAT}}');
```

### OPT Ingestion Template

```sql
CREATE OR REPLACE PIPE {{DATA_DB}}.{{TENANT_SCHEMA}}.PIPE_{{TABLE_NAME}} AS
COPY INTO {{DATA_DB}}.{{TENANT_SCHEMA}}.{{TABLE_NAME}}_RAW
FROM @{{DATA_DB}}.{{TENANT_SCHEMA}}.{{STAGE_NAME}}
FILE_FORMAT = (TYPE = '{{FORMAT}}');
```

---

## Step 3: Transformation Layer

### Option A: Dynamic Tables (Recommended for Continuous)

```sql
CREATE OR REPLACE DYNAMIC TABLE {{DATA_DB}}.{{SCHEMA}}.DT_{{TABLE_NAME}}_CURATED
    TARGET_LAG = '{{LAG}}'
    WAREHOUSE = {{WAREHOUSE}}
AS
SELECT
    {{TENANT_ID_COLUMN}},
    {{COLUMN_1}},
    {{COLUMN_2}},
    {{DERIVED_COLUMN_EXPRESSION}} AS {{DERIVED_COLUMN_NAME}},
    {{DATE_COLUMN}}
FROM {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}_RAW
WHERE {{FILTER_CONDITION}};
```

### Option B: dbt Models (Recommended for Complex Transformations)

Use dbt with Snowflake adapter for version-controlled transformation pipelines:

- **Staging models**: Clean and standardize raw data
- **Intermediate models**: Business logic and joins
- **Mart models**: Curated data products for consumption

Ensure `TENANT_ID` propagation through all model layers for MTT.

### Option C: Snowflake Tasks (Scheduled SQL)

```sql
CREATE OR REPLACE TASK {{DATA_DB}}.{{SCHEMA}}.TASK_{{TABLE_NAME}}_TRANSFORM
    WAREHOUSE = {{WAREHOUSE}}
    SCHEDULE = 'USING CRON {{CRON_EXPRESSION}} America/New_York'
AS
INSERT INTO {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}_CURATED
SELECT {{COLUMNS}} FROM {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}_RAW
WHERE INGESTED_AT > (SELECT MAX(INGESTED_AT) FROM {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}_CURATED);
```

---

## Step 4: Curation Layer

Design curated data products based on consumption needs:

| Data Product Pattern | Implementation | Best For |
|---------------------|----------------|----------|
| **Secure Views** | `CREATE SECURE VIEW` with RLAP | MTT — tenant sees only their data |
| **Materialized Tables** | Dynamic Tables or scheduled INSERT | Performance-critical queries |
| **Aggregated Datasets** | Pre-computed metrics by dimension | Dashboards and reporting |
| **Feature Tables** | ML-ready feature sets | Cortex ML, model training |
| **API-Ready Views** | Flattened, denormalized views | External API consumption |

### Secure View Template (MTT)

```sql
CREATE OR REPLACE SECURE VIEW {{DATA_DB}}.{{SCHEMA}}.V_{{DATA_PRODUCT_NAME}}
AS
SELECT
    TENANT_ID,
    {{COLUMNS}},
    {{AGGREGATION_EXPRESSIONS}}
FROM {{DATA_DB}}.{{SCHEMA}}.{{SOURCE_TABLE}}
{{GROUP_BY_CLAUSE}};
```

---

## Step 5: Data Quality

### Snowflake Data Metric Functions (DMFs)

Use DMFs to monitor data quality at the table level:

```sql
ALTER TABLE {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

ALTER TABLE {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON ({{COLUMN_NAME}});

ALTER TABLE {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUE_COUNT ON ({{COLUMN_NAME}});

ALTER TABLE {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ({{TIMESTAMP_COLUMN}});
```

### Custom DMFs

```sql
CREATE OR REPLACE DATA METRIC FUNCTION {{DATA_DB}}.{{SCHEMA}}.DMF_{{CHECK_NAME}}(
    ARG_T TABLE({{COLUMN_NAME}} {{COLUMN_TYPE}})
)
RETURNS NUMBER AS
'SELECT COUNT(*) FROM ARG_T WHERE {{CONDITION}}';
```

---

## Output

**MANDATORY STOPPING POINT**: Present the data product design for customer approval before generating scripts.

Deliver:
1. **Ingestion pipeline templates** — configured for the customer's source systems and tenant model
2. **Transformation pattern** — Dynamic Tables, dbt, or Tasks based on customer needs
3. **Curated data product catalog** — list of data products with schema and consumption target
4. **Data quality setup** — DMFs configured for the customer's key tables
5. **Pipeline diagram** — RAW → STAGED → CURATED → DATA_PRODUCT flow
6. **Edition requirements** — see table below

### Edition Requirements

| Feature | Minimum Edition |
|---------|----------------|
| Dynamic Tables | Standard+ |
| Snowpipe | Standard+ |
| Snowpipe Streaming | Standard+ |
| Data Metric Functions (DMFs) | **Enterprise** |
| Search Optimization Service | **Enterprise** |
| Query Acceleration Service | **Enterprise** |
| Materialized Views | **Enterprise** |

After approval, route to:
- `secure-sharing` for sharing data products externally
- `implementation` for executable scripts
