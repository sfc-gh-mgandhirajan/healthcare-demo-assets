-- ============================================================
-- MULTI-TENANT PLATFORM: COST ATTRIBUTION VIEWS
-- Replace {{VARIABLES}} before execution
-- ============================================================

-- =========================
-- OPT: DEDICATED WAREHOUSE COSTS
-- =========================
CREATE OR REPLACE VIEW {{COST_DB}}.{{ANALYTICS_SCHEMA}}.V_OPT_WAREHOUSE_COSTS AS
SELECT
    wmh.warehouse_name,
    wtm.tenant_id,
    wmh.start_time::DATE AS usage_date,
    SUM(wmh.credits_used_compute) AS compute_credits,
    SUM(wmh.credits_used_cloud_services) AS cloud_credits,
    SUM(wmh.credits_used_compute + wmh.credits_used_cloud_services) AS total_credits,
    total_credits * COALESCE(tr.credit_rate, 3.00) AS cost_estimate
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY wmh
JOIN {{COST_DB}}.{{FINOPS_SCHEMA}}.WAREHOUSE_TENANT_MAP wtm
    ON wmh.warehouse_name = wtm.warehouse_name
    AND wtm.warehouse_type = 'DEDICATED'
LEFT JOIN {{COST_DB}}.{{FINOPS_SCHEMA}}.TENANT_REGISTRY tr
    ON wtm.tenant_id = tr.tenant_id
GROUP BY wmh.warehouse_name, wtm.tenant_id, usage_date, tr.credit_rate;

-- =========================
-- MTT: QUERY TAG ATTRIBUTION
-- =========================
CREATE OR REPLACE VIEW {{COST_DB}}.{{ANALYTICS_SCHEMA}}.V_MTT_QUERY_TAG_COSTS AS
SELECT
    PARSE_JSON(query_tag):tenant_id::VARCHAR AS tenant_id,
    PARSE_JSON(query_tag):cost_center::VARCHAR AS cost_center,
    warehouse_name,
    start_time::DATE AS usage_date,
    COUNT(*) AS query_count,
    SUM(total_elapsed_time) / 1000 AS total_elapsed_sec,
    SUM(credits_used_cloud_services) AS cloud_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag IS NOT NULL
    AND TRY_PARSE_JSON(query_tag) IS NOT NULL
    AND TRY_PARSE_JSON(query_tag):tenant_id IS NOT NULL
GROUP BY tenant_id, cost_center, warehouse_name, usage_date;

-- =========================
-- MTT: ROW-RATIO STORAGE ALLOCATION
-- =========================
CREATE OR REPLACE VIEW {{COST_DB}}.{{ANALYTICS_SCHEMA}}.V_MTT_STORAGE_ALLOCATION AS
SELECT
    r.tenant_id,
    r.ratio_date,
    r.fully_qualified_name,
    ts.active_bytes * r.row_ratio / POWER(1024, 4) AS allocated_tb,
    allocated_tb * 23.00 AS storage_cost_usd
FROM {{COST_DB}}.{{FINOPS_SCHEMA}}.TABLE_TENANT_RATIOS r
JOIN SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS ts
    ON r.fully_qualified_name = CONCAT(ts.table_catalog, '.', ts.table_schema, '.', ts.table_name)
WHERE r.ratio_date = CURRENT_DATE;

-- =========================
-- SERVERLESS COSTS
-- =========================
CREATE OR REPLACE VIEW {{COST_DB}}.{{ANALYTICS_SCHEMA}}.V_SERVERLESS_COSTS AS
SELECT
    start_time::DATE AS usage_date,
    DATABASE_NAME,
    SCHEMA_NAME,
    TASK_NAME AS resource_name,
    SUM(credits_used) AS serverless_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.SERVERLESS_TASK_HISTORY
GROUP BY usage_date, DATABASE_NAME, SCHEMA_NAME, TASK_NAME;

-- =========================
-- UNIFIED COST SUMMARY
-- =========================
CREATE OR REPLACE VIEW {{COST_DB}}.{{ANALYTICS_SCHEMA}}.V_UNIFIED_COST_SUMMARY AS
SELECT usage_date, tenant_id, 'DEDICATED_COMPUTE' AS cost_category,
    total_credits AS credits, cost_estimate AS cost_usd
FROM {{COST_DB}}.{{ANALYTICS_SCHEMA}}.V_OPT_WAREHOUSE_COSTS

UNION ALL

SELECT usage_date, tenant_id, 'SHARED_COMPUTE',
    cloud_credits, cloud_credits * 3.00 AS cost_usd
FROM {{COST_DB}}.{{ANALYTICS_SCHEMA}}.V_MTT_QUERY_TAG_COSTS

UNION ALL

SELECT ratio_date, tenant_id, 'STORAGE',
    allocated_tb, storage_cost_usd
FROM {{COST_DB}}.{{ANALYTICS_SCHEMA}}.V_MTT_STORAGE_ALLOCATION;
