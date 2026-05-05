---
name: cost-attribution
description: "FinOps and cost attribution for multi-tenant platforms: tag-based cost tracking, resource monitors, chargeback models, showback, query tag attribution, warehouse-tenant mapping, credit budgeting, cost optimization. Industry-agnostic. Triggers: cost attribution, FinOps, chargeback, showback, resource monitor, credit quota, cost center, cost allocation, billing, tenant cost, warehouse cost, query tag, cost optimization, budget."
platform_affinities:
  produces:
    - cost attribution views
    - resource monitors
    - chargeback reports
  benefits_from:
    - skill: cost-intelligence
      when: "always — Snowflake cost intelligence provides the account-level metering data and optimization recommendations that underpin chargeback models"
    - skill: warehouse
      when: "warehouse sizing, auto-suspend, and multi-cluster configuration are part of the cost optimization strategy"
---

# Cost Attribution & FinOps

## When to Use

**Load** this skill when the customer needs to attribute Snowflake costs to individual tenants, set up resource monitors, or design chargeback/showback models.

---

## Step 1: Understand Cost Model Requirements

Gather:

| Input | Description |
|-------|-------------|
| Billing model | Chargeback (tenant pays), showback (visibility only), shared pool |
| Tenancy pattern | MTT (shared resources), OPT (dedicated), Hybrid |
| Cost components to track | Compute, storage, serverless, AI/ML, data transfer |
| Service tiers | Do different tenants pay different rates? |
| Budget controls | Hard limits (suspend) vs soft limits (notify) |
| Reporting cadence | Daily, weekly, monthly |
| Currency | Credits only or USD conversion needed |

### Healthcare & Regulated Industry Considerations

If the customer is in healthcare, life sciences, or another regulated industry, also ask:

| Input | Description |
|-------|-------------|
| Are there clinical SLA requirements? | Clinical reporting (e.g., lab results, ADT alerts) may require dedicated warehouses that cannot be suspended, even if idle |
| Which workloads are compliance-critical? | HIPAA audit queries, regulatory reporting, and real-time alerting must not be impacted by resource monitor suspensions |
| Are there data residency cost implications? | Cross-region replication for data residency adds cost that must be attributed |
| Do AI/ML workloads process PHI? | AI warehouses processing clinical data need separate cost tracking AND governance tagging |

**Warning**: In healthcare, auto-suspend and resource monitor `SUSPEND` triggers can disrupt clinical reporting SLAs. Use `NOTIFY` triggers at lower thresholds and `SUSPEND` only on non-clinical warehouses.

---

## Step 2: Cost Attribution Tags

### Create FinOps Tags

```sql
CREATE DATABASE IF NOT EXISTS {{COST_DB}};
CREATE SCHEMA IF NOT EXISTS {{COST_DB}}.{{FINOPS_SCHEMA}};

CREATE TAG IF NOT EXISTS {{COST_DB}}.{{FINOPS_SCHEMA}}.TENANT_ID
    COMMENT = 'Tenant identifier for cost attribution';

CREATE TAG IF NOT EXISTS {{COST_DB}}.{{FINOPS_SCHEMA}}.COST_CENTER
    COMMENT = 'Cost center for financial attribution';

CREATE TAG IF NOT EXISTS {{COST_DB}}.{{FINOPS_SCHEMA}}.SERVICE_TIER
    ALLOWED_VALUES 'BASIC', 'STANDARD', 'PREMIUM', 'ENTERPRISE'
    COMMENT = 'Tenant service tier for pricing';

CREATE TAG IF NOT EXISTS {{COST_DB}}.{{FINOPS_SCHEMA}}.ENVIRONMENT
    ALLOWED_VALUES 'PRODUCTION', 'STAGING', 'DEVELOPMENT'
    COMMENT = 'Environment classification';
```

### Apply Tags to Resources

```sql
ALTER WAREHOUSE {{WAREHOUSE_NAME}}
    SET TAG {{COST_DB}}.{{FINOPS_SCHEMA}}.TENANT_ID = '{{TENANT_ID}}';
ALTER WAREHOUSE {{WAREHOUSE_NAME}}
    SET TAG {{COST_DB}}.{{FINOPS_SCHEMA}}.COST_CENTER = '{{COST_CENTER}}';

ALTER SCHEMA {{DATA_DB}}.{{SCHEMA}}
    SET TAG {{COST_DB}}.{{FINOPS_SCHEMA}}.TENANT_ID = '{{TENANT_ID}}';

ALTER USER {{TENANT_USER}}
    SET TAG {{COST_DB}}.{{FINOPS_SCHEMA}}.TENANT_ID = '{{TENANT_ID}}';
```

---

## Step 3: FinOps Configuration Tables

### Tenant Registry

```sql
CREATE TABLE IF NOT EXISTS {{COST_DB}}.{{FINOPS_SCHEMA}}.TENANT_REGISTRY (
    TENANT_ID           VARCHAR PRIMARY KEY,
    TENANT_NAME         VARCHAR NOT NULL,
    TENANT_TYPE         VARCHAR,
    CONTRACT_VALUE      NUMBER(12,2),
    CREDIT_RATE         NUMBER(6,4) DEFAULT 3.00,
    TENANCY_PATTERN     VARCHAR,
    SERVICE_TIER        VARCHAR DEFAULT 'STANDARD',
    ACTIVE              BOOLEAN DEFAULT TRUE,
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Warehouse-Tenant Mapping

```sql
CREATE TABLE IF NOT EXISTS {{COST_DB}}.{{FINOPS_SCHEMA}}.WAREHOUSE_TENANT_MAP (
    WAREHOUSE_NAME  VARCHAR NOT NULL,
    TENANT_ID       VARCHAR,
    WAREHOUSE_TYPE  VARCHAR NOT NULL,
    CONSTRAINT pk_wh_map PRIMARY KEY (WAREHOUSE_NAME)
);
```

### MTT Row Ratio Table (for shared table cost allocation)

```sql
CREATE TABLE IF NOT EXISTS {{COST_DB}}.{{FINOPS_SCHEMA}}.TABLE_TENANT_RATIOS (
    RATIO_DATE           DATE NOT NULL,
    FULLY_QUALIFIED_NAME VARCHAR NOT NULL,
    TENANT_ID            VARCHAR NOT NULL,
    ROW_RATIO            FLOAT NOT NULL,
    ROW_COUNT            NUMBER,
    CONSTRAINT pk_ratios PRIMARY KEY (RATIO_DATE, FULLY_QUALIFIED_NAME, TENANT_ID)
);
```

---

## Step 4: Cost Attribution Strategies

### OPT: Dedicated Warehouse Attribution

```sql
CREATE OR REPLACE VIEW {{COST_DB}}.{{ANALYTICS_SCHEMA}}.V_OPT_WAREHOUSE_COSTS AS
SELECT
    wmh.warehouse_name,
    wtm.tenant_id,
    wmh.start_time::DATE AS usage_date,
    SUM(wmh.credits_used_compute) AS compute_credits,
    SUM(wmh.credits_used_cloud_services) AS cloud_credits,
    SUM(wmh.credits_used_compute + wmh.credits_used_cloud_services) AS total_credits,
    total_credits * tr.credit_rate AS cost_estimate
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY wmh
JOIN {{COST_DB}}.{{FINOPS_SCHEMA}}.WAREHOUSE_TENANT_MAP wtm
    ON wmh.warehouse_name = wtm.warehouse_name
LEFT JOIN {{COST_DB}}.{{FINOPS_SCHEMA}}.TENANT_REGISTRY tr
    ON wtm.tenant_id = tr.tenant_id
GROUP BY wmh.warehouse_name, wtm.tenant_id, usage_date, tr.credit_rate;
```

### MTT: Query Tag Attribution

For shared warehouses, use **query tags** to attribute costs:

```sql
ALTER SESSION SET QUERY_TAG = '{"tenant_id": "{{TENANT_ID}}", "cost_center": "{{COST_CENTER}}"}';
```

Attribution view:

```sql
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
    AND TRY_PARSE_JSON(query_tag):tenant_id IS NOT NULL
    AND start_time >= DATEADD('DAY', -{{DAYS}}, CURRENT_TIMESTAMP)
GROUP BY tenant_id, cost_center, warehouse_name, usage_date;
```

### MTT: Row Ratio Attribution (Storage & Shared Compute)

```sql
CREATE OR REPLACE VIEW {{COST_DB}}.{{ANALYTICS_SCHEMA}}.V_MTT_STORAGE_ALLOCATION AS
SELECT
    r.tenant_id,
    r.ratio_date,
    ts.active_bytes * r.row_ratio / POWER(1024, 4) AS allocated_tb,
    allocated_tb * {{STORAGE_COST_PER_TB}} AS storage_cost
FROM {{COST_DB}}.{{FINOPS_SCHEMA}}.TABLE_TENANT_RATIOS r
JOIN SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS ts
    ON r.fully_qualified_name = CONCAT(ts.table_catalog, '.', ts.table_schema, '.', ts.table_name)
WHERE r.ratio_date = CURRENT_DATE;
```

### Unified Cost Summary

```sql
CREATE OR REPLACE VIEW {{COST_DB}}.{{ANALYTICS_SCHEMA}}.V_UNIFIED_COST_SUMMARY AS
SELECT usage_date, tenant_id, 'DEDICATED_COMPUTE' AS cost_category, total_credits AS credits, cost_estimate AS cost_usd
FROM {{COST_DB}}.{{ANALYTICS_SCHEMA}}.V_OPT_WAREHOUSE_COSTS
UNION ALL
SELECT usage_date, tenant_id, 'SHARED_COMPUTE', cloud_credits, cloud_credits * {{CREDIT_RATE}} AS cost_usd
FROM {{COST_DB}}.{{ANALYTICS_SCHEMA}}.V_MTT_QUERY_TAG_COSTS
UNION ALL
SELECT ratio_date, tenant_id, 'STORAGE', allocated_tb, storage_cost
FROM {{COST_DB}}.{{ANALYTICS_SCHEMA}}.V_MTT_STORAGE_ALLOCATION;
```

---

## Step 5: Resource Monitors & Budget Controls

### Per-Tenant Resource Monitors (OPT)

```sql
CREATE RESOURCE MONITOR IF NOT EXISTS RM_{{TENANT_ID}}
    WITH CREDIT_QUOTA = {{CREDIT_QUOTA}}
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 50 PERCENT DO NOTIFY
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE WH_{{TENANT_ID}} SET RESOURCE_MONITOR = RM_{{TENANT_ID}};
```

### Account-Level Resource Monitor

```sql
CREATE RESOURCE MONITOR IF NOT EXISTS RM_ACCOUNT_LEVEL
    WITH CREDIT_QUOTA = {{ACCOUNT_QUOTA}}
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 80 PERCENT DO NOTIFY
        ON 95 PERCENT DO SUSPEND_IMMEDIATE;
```

---

## Step 6: Cost Reporting

### Daily Spend by Tenant

```sql
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
```

### Month-over-Month Trend

```sql
SELECT
    tenant_id,
    DATE_TRUNC('MONTH', usage_date) AS cost_month,
    SUM(credits) AS monthly_credits,
    SUM(cost_usd) AS monthly_cost_usd
FROM {{COST_DB}}.{{ANALYTICS_SCHEMA}}.V_UNIFIED_COST_SUMMARY
WHERE usage_date >= DATEADD('MONTH', -6, CURRENT_DATE)
GROUP BY tenant_id, cost_month
ORDER BY tenant_id, cost_month;
```

---

## Output

**MANDATORY STOPPING POINT**: Present the cost attribution model for customer approval.

Deliver:
1. **Tag strategy** — which tags to create and where to apply them
2. **Attribution views** — per tenancy pattern (OPT dedicated, MTT query tag, MTT row ratio)
3. **Unified cost summary** — single view combining all cost components
4. **Resource monitors** — per-tenant and account-level
5. **Reporting queries** — daily, monthly, trend analysis
6. **Service tier pricing** — if tiered billing is needed
7. **Clinical SLA warnings** — if healthcare, flag warehouses that must not be auto-suspended
8. **Edition requirements** — see table below

### Edition Requirements

| Feature | Minimum Edition |
|---------|----------------|
| Object tagging (CREATE TAG) | Standard+ |
| ACCOUNT_USAGE views (WAREHOUSE_METERING_HISTORY, QUERY_HISTORY) | **Enterprise** |
| Resource monitors | Standard+ |
| Multi-cluster warehouses | **Enterprise** |
| Query Acceleration Service | **Enterprise** |

After approval, route to:
- `implementation` for executable scripts
