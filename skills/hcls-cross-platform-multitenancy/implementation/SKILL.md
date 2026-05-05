---
name: implementation
description: "Generate parameterized implementation scripts for multi-tenant platform setup: foundation databases, roles, policies, tenant onboarding, cost attribution views, audit monitoring. All scripts use {{VARIABLES}} for customer customization. Triggers: implementation, setup, scripts, deploy, foundation, onboard, execute, generate scripts, create, bootstrap."
platform_affinities:
  produces:
    - SQL setup scripts
    - foundation DDL
    - tenant onboarding scripts
  benefits_from:
    - skill: snowpark-python
      when: "automation or stored procedure logic needs to be implemented in Python via Snowpark"
---

# Implementation Scripts

## When to Use

**Load** this skill when the customer has approved a design from another sub-skill and is ready to generate executable, parameterized SQL scripts.

---

## Step 1: Confirm Design Decisions

Before generating scripts, confirm the customer has approved:

| Decision | Source Sub-Skill |
|----------|-----------------|
| Tenancy pattern (MTT/OPT/Hybrid) | tenancy-decision |
| Role hierarchy and RLAP design | rbac-tenancy |
| Data product pipeline design | data-products |
| Cost attribution model | cost-attribution |
| Governance framework | governance-security |
| BCDR plan (if needed) | bcdr-operations |

---

## Step 2: Variable Collection

Collect all variables needed for script generation. Use the customer's **exact entity names**.

### Foundation Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `{{DATA_DB}}` | Primary data database | `ACME_DATA` |
| `{{GOVERNANCE_DB}}` | Governance database | `ACME_GOVERNANCE` |
| `{{COST_DB}}` | Cost attribution database | `ACME_COST_MART` |
| `{{POLICY_SCHEMA}}` | Schema for policies | `POLICIES` |
| `{{TAG_SCHEMA}}` | Schema for tags | `TAGS` |
| `{{AUDIT_SCHEMA}}` | Schema for audit objects | `AUDIT` |
| `{{FINOPS_SCHEMA}}` | Schema for FinOps config | `FINOPS` |
| `{{ANALYTICS_SCHEMA}}` | Schema for cost analytics views | `ANALYTICS` |

### Tenant Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `{{TENANT_ID}}` | Unique tenant identifier | `TENANT_ACME` |
| `{{TENANT_NAME}}` | Human-readable tenant name | `Acme Corporation` |
| `{{TENANT_TYPE}}` | Tenant classification | `ENTERPRISE` |
| `{{TENANT_SCHEMA}}` | OPT: dedicated schema name | `SCHEMA_ACME` |
| `{{SHARED_SCHEMA}}` | MTT: shared schema name | `SHARED` |
| `{{WH_SIZE}}` | Warehouse size | `SMALL` |
| `{{CREDIT_QUOTA}}` | Monthly credit limit | `500` |
| `{{SERVICE_TIER}}` | Service tier | `PREMIUM` |

---

## Step 3: Script Templates

### Script 1: Foundation Setup (Run Once)

```sql
-- Foundation databases
CREATE DATABASE IF NOT EXISTS {{GOVERNANCE_DB}};
CREATE SCHEMA IF NOT EXISTS {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}};
CREATE SCHEMA IF NOT EXISTS {{GOVERNANCE_DB}}.{{TAG_SCHEMA}};
CREATE SCHEMA IF NOT EXISTS {{GOVERNANCE_DB}}.{{AUDIT_SCHEMA}};

CREATE DATABASE IF NOT EXISTS {{COST_DB}};
CREATE SCHEMA IF NOT EXISTS {{COST_DB}}.{{FINOPS_SCHEMA}};
CREATE SCHEMA IF NOT EXISTS {{COST_DB}}.{{ANALYTICS_SCHEMA}};

-- Platform roles
CREATE ROLE IF NOT EXISTS PLATFORM_ADMIN_ROLE
    COMMENT = 'Top-level admin for multi-tenant platform';
GRANT ROLE PLATFORM_ADMIN_ROLE TO ROLE SYSADMIN;

CREATE ROLE IF NOT EXISTS FINOPS_ADMIN_ROLE
    COMMENT = 'FinOps team — cost attribution and budgeting';
GRANT ROLE FINOPS_ADMIN_ROLE TO ROLE SYSADMIN;

CREATE ROLE IF NOT EXISTS DATA_ENGINEER_ROLE
    COMMENT = 'Platform data engineering team';
GRANT ROLE DATA_ENGINEER_ROLE TO ROLE PLATFORM_ADMIN_ROLE;

CREATE ROLE IF NOT EXISTS GOVERNANCE_ADMIN_ROLE
    COMMENT = 'Governance team — policies, tags, classification';
GRANT ROLE GOVERNANCE_ADMIN_ROLE TO ROLE PLATFORM_ADMIN_ROLE;

-- Governance grants
GRANT ALL ON DATABASE {{GOVERNANCE_DB}} TO ROLE GOVERNANCE_ADMIN_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE {{GOVERNANCE_DB}} TO ROLE GOVERNANCE_ADMIN_ROLE;
GRANT ALL ON DATABASE {{COST_DB}} TO ROLE FINOPS_ADMIN_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE {{COST_DB}} TO ROLE FINOPS_ADMIN_ROLE;

-- Tenant entitlements table (for RLAP)
CREATE TABLE IF NOT EXISTS {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.TENANT_ENTITLEMENTS (
    ROLE_NAME       VARCHAR NOT NULL,
    TENANT_ID       VARCHAR NOT NULL,
    ACCESS_LEVEL    VARCHAR DEFAULT 'READ',
    GRANTED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    GRANTED_BY      VARCHAR DEFAULT CURRENT_USER(),
    CONSTRAINT pk_entitlements PRIMARY KEY (ROLE_NAME, TENANT_ID)
);

-- Row Access Policy
CREATE OR REPLACE ROW ACCESS POLICY {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.TENANT_ISOLATION_RAP
AS (tenant_id_col VARCHAR) RETURNS BOOLEAN ->
    CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'PLATFORM_ADMIN_ROLE')
    OR tenant_id_col IN (
        SELECT TENANT_ID
        FROM {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.TENANT_ENTITLEMENTS
        WHERE ROLE_NAME = CURRENT_ROLE()
    );

-- Governance tags
CREATE TAG IF NOT EXISTS {{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.SENSITIVITY_LEVEL
    ALLOWED_VALUES 'FULL_MASK', 'PARTIAL_MASK', 'DATE_MASK', 'NONE'
    COMMENT = 'Data sensitivity level for tag-based masking';

CREATE TAG IF NOT EXISTS {{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.DATA_CLASSIFICATION
    ALLOWED_VALUES 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED'
    COMMENT = 'Data classification level';

-- Cost attribution tags
CREATE TAG IF NOT EXISTS {{COST_DB}}.{{FINOPS_SCHEMA}}.TENANT_ID
    COMMENT = 'Tenant identifier for cost attribution';

CREATE TAG IF NOT EXISTS {{COST_DB}}.{{FINOPS_SCHEMA}}.COST_CENTER
    COMMENT = 'Cost center for financial attribution';

CREATE TAG IF NOT EXISTS {{COST_DB}}.{{FINOPS_SCHEMA}}.SERVICE_TIER
    ALLOWED_VALUES 'BASIC', 'STANDARD', 'PREMIUM', 'ENTERPRISE'
    COMMENT = 'Tenant service tier for pricing';

-- FinOps configuration tables
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

CREATE TABLE IF NOT EXISTS {{COST_DB}}.{{FINOPS_SCHEMA}}.WAREHOUSE_TENANT_MAP (
    WAREHOUSE_NAME  VARCHAR NOT NULL,
    TENANT_ID       VARCHAR,
    WAREHOUSE_TYPE  VARCHAR NOT NULL,
    CONSTRAINT pk_wh_map PRIMARY KEY (WAREHOUSE_NAME)
);
```

### Script 2: MTT Tenant Onboarding

```sql
-- 1. Tenant role
CREATE ROLE IF NOT EXISTS {{TENANT_ID}}_ROLE
    COMMENT = 'Tenant role for {{TENANT_NAME}}';
GRANT ROLE {{TENANT_ID}}_ROLE TO ROLE PLATFORM_ADMIN_ROLE;

-- 2. Access grants
GRANT USAGE ON DATABASE {{DATA_DB}} TO ROLE {{TENANT_ID}}_ROLE;
GRANT USAGE ON SCHEMA {{DATA_DB}}.{{SHARED_SCHEMA}} TO ROLE {{TENANT_ID}}_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA {{DATA_DB}}.{{SHARED_SCHEMA}} TO ROLE {{TENANT_ID}}_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA {{DATA_DB}}.{{SHARED_SCHEMA}} TO ROLE {{TENANT_ID}}_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA {{DATA_DB}}.{{SHARED_SCHEMA}} TO ROLE {{TENANT_ID}}_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA {{DATA_DB}}.{{SHARED_SCHEMA}} TO ROLE {{TENANT_ID}}_ROLE;

-- 3. Warehouse access
GRANT USAGE ON WAREHOUSE {{SHARED_WH}} TO ROLE {{TENANT_ID}}_ROLE;

-- 4. RLAP entitlement
INSERT INTO {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.TENANT_ENTITLEMENTS (ROLE_NAME, TENANT_ID, ACCESS_LEVEL)
VALUES ('{{TENANT_ID}}_ROLE', '{{TENANT_ID}}', 'READ');

-- 5. FinOps registration
INSERT INTO {{COST_DB}}.{{FINOPS_SCHEMA}}.TENANT_REGISTRY
    (TENANT_ID, TENANT_NAME, TENANT_TYPE, TENANCY_PATTERN, SERVICE_TIER)
VALUES ('{{TENANT_ID}}', '{{TENANT_NAME}}', '{{TENANT_TYPE}}', 'MTT', '{{SERVICE_TIER}}');

-- 6. Cost tagging
ALTER USER {{TENANT_USER}} SET TAG {{COST_DB}}.{{FINOPS_SCHEMA}}.TENANT_ID = '{{TENANT_ID}}';
```

### Script 3: OPT Tenant Onboarding

```sql
-- 1. Dedicated schema
CREATE SCHEMA IF NOT EXISTS {{DATA_DB}}.{{TENANT_SCHEMA}}
    COMMENT = 'Dedicated schema for {{TENANT_NAME}}';

-- 2. Dedicated warehouse
CREATE WAREHOUSE IF NOT EXISTS WH_{{TENANT_ID}}
    WAREHOUSE_SIZE = '{{WH_SIZE}}'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'STANDARD'
    ENABLE_QUERY_ACCELERATION = TRUE
    COMMENT = 'Dedicated warehouse for {{TENANT_NAME}}';

-- 3. Role hierarchy
CREATE ROLE IF NOT EXISTS {{TENANT_ID}}_ADMIN_ROLE;
CREATE ROLE IF NOT EXISTS {{TENANT_ID}}_ANALYST_ROLE;
CREATE ROLE IF NOT EXISTS {{TENANT_ID}}_VIEWER_ROLE;
GRANT ROLE {{TENANT_ID}}_VIEWER_ROLE TO ROLE {{TENANT_ID}}_ANALYST_ROLE;
GRANT ROLE {{TENANT_ID}}_ANALYST_ROLE TO ROLE {{TENANT_ID}}_ADMIN_ROLE;
GRANT ROLE {{TENANT_ID}}_ADMIN_ROLE TO ROLE PLATFORM_ADMIN_ROLE;

-- 4. Permissions
GRANT USAGE ON DATABASE {{DATA_DB}} TO ROLE {{TENANT_ID}}_VIEWER_ROLE;
GRANT USAGE ON SCHEMA {{DATA_DB}}.{{TENANT_SCHEMA}} TO ROLE {{TENANT_ID}}_VIEWER_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA {{DATA_DB}}.{{TENANT_SCHEMA}} TO ROLE {{TENANT_ID}}_VIEWER_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA {{DATA_DB}}.{{TENANT_SCHEMA}} TO ROLE {{TENANT_ID}}_VIEWER_ROLE;
GRANT ALL ON SCHEMA {{DATA_DB}}.{{TENANT_SCHEMA}} TO ROLE {{TENANT_ID}}_ADMIN_ROLE;
GRANT USAGE ON WAREHOUSE WH_{{TENANT_ID}} TO ROLE {{TENANT_ID}}_VIEWER_ROLE;

-- 5. Resource monitor
CREATE RESOURCE MONITOR IF NOT EXISTS RM_{{TENANT_ID}}
    WITH CREDIT_QUOTA = {{CREDIT_QUOTA}}
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;
ALTER WAREHOUSE WH_{{TENANT_ID}} SET RESOURCE_MONITOR = RM_{{TENANT_ID}};

-- 6. Cost tagging
ALTER WAREHOUSE WH_{{TENANT_ID}} SET TAG {{COST_DB}}.{{FINOPS_SCHEMA}}.TENANT_ID = '{{TENANT_ID}}';
ALTER SCHEMA {{DATA_DB}}.{{TENANT_SCHEMA}} SET TAG {{COST_DB}}.{{FINOPS_SCHEMA}}.TENANT_ID = '{{TENANT_ID}}';

-- 7. FinOps registration
INSERT INTO {{COST_DB}}.{{FINOPS_SCHEMA}}.TENANT_REGISTRY
    (TENANT_ID, TENANT_NAME, TENANT_TYPE, TENANCY_PATTERN, SERVICE_TIER)
VALUES ('{{TENANT_ID}}', '{{TENANT_NAME}}', '{{TENANT_TYPE}}', 'OPT', '{{SERVICE_TIER}}');

INSERT INTO {{COST_DB}}.{{FINOPS_SCHEMA}}.WAREHOUSE_TENANT_MAP (WAREHOUSE_NAME, TENANT_ID, WAREHOUSE_TYPE)
VALUES ('WH_{{TENANT_ID}}', '{{TENANT_ID}}', 'DEDICATED');
```

### Script 4: Audit & Monitoring Queries

```sql
-- Access audit (last N days)
SELECT
    query_id, user_name, role_name, warehouse_name,
    start_time, total_elapsed_time / 1000 AS elapsed_sec,
    rows_produced, bytes_scanned, query_text
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_text ILIKE '%{{TABLE_OR_KEYWORD}}%'
    AND start_time >= DATEADD('DAY', -{{DAYS}}, CURRENT_TIMESTAMP)
ORDER BY start_time DESC;

-- Column-level access tracking
SELECT
    ah.query_id, ah.user_name, ah.role_name, ah.query_start_time,
    obj.value:objectName::VARCHAR AS object_name,
    col.value:columnName::VARCHAR AS column_name
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
    LATERAL FLATTEN(base_objects_accessed) obj,
    LATERAL FLATTEN(obj.value:columns) col
WHERE ah.query_start_time >= DATEADD('DAY', -{{DAYS}}, CURRENT_TIMESTAMP)
    AND obj.value:objectName::VARCHAR ILIKE '%{{TABLE_NAME}}%'
ORDER BY ah.query_start_time DESC;

-- Failed logins
SELECT
    user_name, client_ip, reported_client_type,
    error_code, error_message, event_timestamp, is_success
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE event_timestamp >= DATEADD('DAY', -30, CURRENT_TIMESTAMP)
    AND is_success = 'NO'
ORDER BY event_timestamp DESC;

-- Warehouse utilization and queuing
SELECT
    warehouse_name,
    DATE_TRUNC('HOUR', start_time) AS usage_hour,
    SUM(credits_used_compute) AS compute_credits,
    AVG(avg_running) AS avg_concurrent,
    AVG(avg_queued_load) AS avg_queued
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('DAY', -7, CURRENT_TIMESTAMP)
GROUP BY warehouse_name, usage_hour
HAVING avg_queued > 0
ORDER BY avg_queued DESC;

-- Long-running queries by tenant
SELECT
    PARSE_JSON(query_tag):tenant_id::VARCHAR AS tenant_id,
    query_id, warehouse_name,
    total_elapsed_time / 1000 AS elapsed_sec,
    bytes_scanned / POWER(1024, 3) AS gb_scanned,
    SUBSTR(query_text, 1, 200) AS query_preview, start_time
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE total_elapsed_time > 300000
    AND start_time >= DATEADD('DAY', -7, CURRENT_TIMESTAMP)
    AND execution_status = 'SUCCESS'
ORDER BY total_elapsed_time DESC
LIMIT 50;

-- Daily cost by tenant
SELECT
    usage_date, tenant_id, cost_category,
    SUM(credits) AS total_credits,
    SUM(cost_usd) AS total_cost_usd
FROM {{COST_DB}}.{{ANALYTICS_SCHEMA}}.V_UNIFIED_COST_SUMMARY
WHERE usage_date >= DATEADD('DAY', -30, CURRENT_DATE)
GROUP BY usage_date, tenant_id, cost_category
ORDER BY usage_date DESC, total_cost_usd DESC;
```

---

## Output

Deliver:
1. **Foundation setup script** — run once per account
2. **Tenant onboarding script** — per tenancy pattern (MTT or OPT)
3. **Audit & monitoring queries** — ready-to-run compliance queries
4. **Variable reference sheet** — all `{{VARIABLES}}` with descriptions

All scripts use `{{VARIABLES}}` — instruct the customer to replace with their actual values before execution.
