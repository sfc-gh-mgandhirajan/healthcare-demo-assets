-- ============================================================
-- COST VIEW VALIDATION QUERY
-- Run against {{COST_DB}} to validate all 5 cost views
-- Expected: all views compile, non-null key columns, status PASS
--
-- NOTE: V_OPT_WAREHOUSE_COSTS and V_MTT_QUERY_TAG_COSTS depend
-- on SNOWFLAKE.ACCOUNT_USAGE views which have ingestion latency:
--   WAREHOUSE_METERING_HISTORY: up to 6 hours
--   QUERY_HISTORY: up to 45 minutes
-- Empty results for these views are expected immediately after setup.
-- ============================================================

SELECT
    'V_OPT_WAREHOUSE_COSTS' AS view_name,
    COUNT(*) AS row_count,
    COUNT(tenant_id) AS non_null_tenant,
    COUNT(usage_date) AS non_null_date,
    COUNT(total_credits) AS non_null_credits,
    CASE
        WHEN COUNT(*) > 0 AND COUNT(tenant_id) = COUNT(*) THEN 'PASS'
        WHEN COUNT(*) = 0 THEN 'EMPTY — ACCOUNT_USAGE latency (up to 6hr)'
        ELSE 'FAIL — null values detected'
    END AS status
FROM {{COST_DB}}.ANALYTICS.V_OPT_WAREHOUSE_COSTS

UNION ALL

SELECT
    'V_MTT_QUERY_TAG_COSTS',
    COUNT(*),
    COUNT(tenant_id),
    COUNT(usage_date),
    COUNT(cloud_credits),
    CASE
        WHEN COUNT(*) > 0 AND COUNT(tenant_id) = COUNT(*) THEN 'PASS'
        WHEN COUNT(*) = 0 THEN 'EMPTY — ACCOUNT_USAGE latency (up to 45min)'
        ELSE 'FAIL — null values detected'
    END
FROM {{COST_DB}}.ANALYTICS.V_MTT_QUERY_TAG_COSTS

UNION ALL

SELECT
    'V_MTT_STORAGE_ALLOCATION',
    COUNT(*),
    COUNT(tenant_id),
    COUNT(ratio_date),
    COUNT(allocated_tb),
    CASE
        WHEN COUNT(*) > 0 AND COUNT(tenant_id) = COUNT(*) THEN 'PASS'
        WHEN COUNT(*) = 0 THEN 'EMPTY — no ratio data for today'
        ELSE 'FAIL — null values detected'
    END
FROM {{COST_DB}}.ANALYTICS.V_MTT_STORAGE_ALLOCATION

UNION ALL

SELECT
    'V_SERVERLESS_COSTS',
    COUNT(*),
    COUNT(resource_name),
    COUNT(usage_date),
    COUNT(serverless_credits),
    CASE
        WHEN COUNT(*) > 0 AND COUNT(resource_name) = COUNT(*) THEN 'PASS'
        WHEN COUNT(*) = 0 THEN 'EMPTY — no serverless tasks in account'
        ELSE 'FAIL — null values detected'
    END
FROM {{COST_DB}}.ANALYTICS.V_SERVERLESS_COSTS

UNION ALL

SELECT
    'V_UNIFIED_COST_SUMMARY',
    COUNT(*),
    COUNT(tenant_id),
    COUNT(usage_date),
    COUNT(credits),
    CASE
        WHEN COUNT(*) > 0 AND COUNT(tenant_id) = COUNT(*) THEN 'PASS'
        WHEN COUNT(*) = 0 THEN 'EMPTY — upstream views still populating'
        ELSE 'FAIL — null values detected'
    END
FROM {{COST_DB}}.ANALYTICS.V_UNIFIED_COST_SUMMARY

ORDER BY view_name;
