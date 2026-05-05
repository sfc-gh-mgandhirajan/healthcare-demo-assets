-- ============================================================
-- MULTI-TENANT PLATFORM: AUDIT & MONITORING QUERIES
-- Ready-to-run compliance and operational monitoring
-- Replace {{VARIABLES}} with your values
-- ============================================================

-- =========================
-- DATA ACCESS AUDIT
-- =========================

-- Who accessed sensitive data in the last N days?
SELECT
    query_id,
    user_name,
    role_name,
    warehouse_name,
    start_time,
    end_time,
    total_elapsed_time / 1000 AS elapsed_sec,
    rows_produced,
    bytes_scanned,
    query_text
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_text ILIKE '%{{TABLE_OR_KEYWORD}}%'
    AND start_time >= DATEADD('DAY', -{{DAYS}}, CURRENT_TIMESTAMP)
ORDER BY start_time DESC;

-- Column-level access tracking
SELECT
    ah.query_id,
    ah.user_name,
    ah.role_name,
    ah.query_start_time,
    obj.value:objectName::VARCHAR AS object_name,
    col.value:columnName::VARCHAR AS column_name
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
    LATERAL FLATTEN(base_objects_accessed) obj,
    LATERAL FLATTEN(obj.value:columns) col
WHERE ah.query_start_time >= DATEADD('DAY', -{{DAYS}}, CURRENT_TIMESTAMP)
    AND obj.value:objectName::VARCHAR ILIKE '%{{TABLE_NAME}}%'
ORDER BY ah.query_start_time DESC;

-- =========================
-- SECURITY: LOGIN ANOMALIES
-- =========================

-- Failed logins in last 30 days
SELECT
    user_name,
    client_ip,
    reported_client_type,
    first_authentication_factor,
    error_code,
    error_message,
    event_timestamp,
    is_success
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE event_timestamp >= DATEADD('DAY', -30, CURRENT_TIMESTAMP)
    AND is_success = 'NO'
ORDER BY event_timestamp DESC;

-- Users with logins from multiple IPs in same hour (potential credential sharing)
SELECT
    user_name,
    DATE_TRUNC('HOUR', event_timestamp) AS login_hour,
    COUNT(DISTINCT client_ip) AS distinct_ips,
    ARRAY_AGG(DISTINCT client_ip) AS ip_list
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE event_timestamp >= DATEADD('DAY', -30, CURRENT_TIMESTAMP)
    AND is_success = 'YES'
GROUP BY user_name, login_hour
HAVING distinct_ips > 2
ORDER BY login_hour DESC;

-- =========================
-- RLAP: POLICY EFFECTIVENESS
-- =========================

-- Verify row access policy is attached to shared tables
SELECT
    policy_name,
    policy_kind,
    ref_database_name,
    ref_schema_name,
    ref_entity_name,
    ref_column_name
FROM SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES
WHERE policy_kind = 'ROW_ACCESS_POLICY'
ORDER BY ref_entity_name;

-- =========================
-- OPERATIONS: WAREHOUSE HEALTH
-- =========================

-- Warehouse utilization and queuing
-- Uses WAREHOUSE_LOAD_HISTORY for concurrency/queuing metrics
-- and WAREHOUSE_METERING_HISTORY for credit consumption
SELECT
    warehouse_name,
    DATE_TRUNC('HOUR', start_time) AS usage_hour,
    AVG(avg_running) AS avg_concurrent,
    AVG(avg_queued_load) AS avg_queued,
    MAX(avg_queued_load) AS max_queued
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
WHERE start_time >= DATEADD('DAY', -7, CURRENT_TIMESTAMP)
GROUP BY warehouse_name, usage_hour
HAVING avg_queued > 0
ORDER BY avg_queued DESC;

-- Long-running queries by tenant
SELECT
    PARSE_JSON(query_tag):tenant_id::VARCHAR AS tenant_id,
    query_id,
    warehouse_name,
    total_elapsed_time / 1000 AS elapsed_sec,
    bytes_scanned / POWER(1024, 3) AS gb_scanned,
    rows_produced,
    SUBSTR(query_text, 1, 200) AS query_preview,
    start_time
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE total_elapsed_time > 300000
    AND start_time >= DATEADD('DAY', -7, CURRENT_TIMESTAMP)
    AND execution_status = 'SUCCESS'
ORDER BY total_elapsed_time DESC
LIMIT 50;

-- =========================
-- COST: DAILY SPEND BY TENANT
-- =========================

-- Quick daily spend check
SELECT
    usage_date,
    tenant_id,
    cost_category,
    SUM(credits) AS total_credits,
    SUM(cost_usd) AS total_cost_usd
FROM {{COST_DB}}.{{ANALYTICS_SCHEMA}}.V_UNIFIED_COST_SUMMARY
WHERE usage_date >= DATEADD('DAY', -30, CURRENT_DATE)
GROUP BY usage_date, tenant_id, cost_category
ORDER BY usage_date DESC, total_cost_usd DESC;

-- Month-over-month cost trend by tenant
SELECT
    tenant_id,
    DATE_TRUNC('MONTH', usage_date) AS cost_month,
    SUM(credits) AS monthly_credits,
    SUM(cost_usd) AS monthly_cost_usd
FROM {{COST_DB}}.{{ANALYTICS_SCHEMA}}.V_UNIFIED_COST_SUMMARY
WHERE usage_date >= DATEADD('MONTH', -6, CURRENT_DATE)
GROUP BY tenant_id, cost_month
ORDER BY tenant_id, cost_month;
