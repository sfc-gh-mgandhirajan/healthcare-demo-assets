---
name: modernization-assessment
description: "Assess and modernize existing Snowflake multi-tenant deployments: footprint review, optimization recommendations, governance maturity, cost efficiency, revenue generation from data products, migration paths, performance tuning. Triggers: modernization, assessment, review, optimize, existing setup, footprint, migration, efficiency, revenue, data monetization, platform maturity, optimization, consolidation, audit existing."
platform_affinities:
  produces: []
  benefits_from:
    - skill: workload-performance-analysis
      when: "query performance and warehouse utilization analysis are needed to baseline the existing footprint"
    - skill: cost-intelligence
      when: "cost efficiency and credit consumption analysis are part of the modernization assessment"
---

# Modernization & Platform Assessment

## When to Use

**Load** this skill when the customer has an existing Snowflake deployment and wants to:
- Review and optimize their current multi-tenant architecture
- Identify governance gaps and remediate them
- Reduce costs and improve efficiency
- Unlock revenue generation from existing data products
- Migrate to a better tenancy model

---

## Step 1: Platform Discovery

### Automated Discovery Queries

Run these queries to understand the customer's current Snowflake footprint:

#### Databases & Schemas
```sql
SELECT
    database_name,
    schema_name,
    created,
    last_altered,
    schema_owner
FROM {{ACCOUNT}}.INFORMATION_SCHEMA.SCHEMATA
ORDER BY database_name, schema_name;
```

#### Warehouse Inventory
```sql
SELECT
    name AS warehouse_name,
    type,
    size,
    min_cluster_count,
    max_cluster_count,
    auto_suspend,
    auto_resume,
    enable_query_acceleration,
    resource_monitor,
    comment
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSES
WHERE deleted IS NULL
ORDER BY name;
```

#### Role Hierarchy
```sql
SELECT
    created_on,
    name AS role_name,
    comment,
    owner
FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES
WHERE deleted_on IS NULL
ORDER BY name;
```

#### Role Grants
```sql
SELECT
    role,
    grantee_name,
    granted_to,
    granted_by
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE privilege = 'USAGE'
    AND deleted_on IS NULL
ORDER BY role, grantee_name;
```

#### Existing Tags
```sql
SELECT
    tag_database,
    tag_schema,
    tag_name,
    allowed_values,
    comment
FROM SNOWFLAKE.ACCOUNT_USAGE.TAGS
WHERE deleted IS NULL;
```

#### Existing Policies
```sql
SELECT
    policy_catalog AS database_name,
    policy_schema AS schema_name,
    policy_name,
    policy_kind,
    policy_owner
FROM SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES
GROUP BY policy_catalog, policy_schema, policy_name, policy_kind, policy_owner
ORDER BY policy_kind, policy_name;
```

#### Share Inventory
```sql
SHOW SHARES;
```

---

## Step 2: Cost Efficiency Assessment

### Credit Consumption Trend

```sql
SELECT
    DATE_TRUNC('MONTH', start_time) AS month,
    warehouse_name,
    SUM(credits_used_compute) AS compute_credits,
    SUM(credits_used_cloud_services) AS cloud_credits,
    SUM(credits_used_compute + credits_used_cloud_services) AS total_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('MONTH', -6, CURRENT_TIMESTAMP)
GROUP BY month, warehouse_name
ORDER BY month DESC, total_credits DESC;
```

### Idle Warehouse Detection

```sql
SELECT
    warehouse_name,
    MAX(end_time) AS last_used,
    DATEDIFF('DAY', MAX(end_time), CURRENT_TIMESTAMP) AS days_idle
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('MONTH', -3, CURRENT_TIMESTAMP)
GROUP BY warehouse_name
HAVING days_idle > 14
ORDER BY days_idle DESC;
```

### Auto-Suspend Optimization

```sql
SELECT
    name AS warehouse_name,
    auto_suspend,
    CASE
        WHEN auto_suspend > 600 THEN 'REDUCE to 300s or less'
        WHEN auto_suspend = 0 THEN 'ENABLE auto-suspend'
        ELSE 'OK'
    END AS recommendation
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSES
WHERE deleted IS NULL;
```

### Underutilized Warehouses

```sql
SELECT
    warehouse_name,
    DATE_TRUNC('WEEK', start_time) AS week,
    AVG(avg_running) AS avg_concurrent_queries,
    AVG(avg_queued_load) AS avg_queued,
    SUM(credits_used_compute) AS weekly_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('MONTH', -1, CURRENT_TIMESTAMP)
GROUP BY warehouse_name, week
HAVING avg_concurrent_queries < 1
ORDER BY weekly_credits DESC;
```

---

## Step 3: Governance Maturity Assessment

### Governance Scorecard

| Dimension | Check | Query/Method |
|-----------|-------|-------------|
| **Classification** | Are tables classified? | Check TAGS on sensitive tables |
| **Masking** | Are masking policies applied to sensitive columns? | Check POLICY_REFERENCES |
| **Row Access** | Are row access policies in place for shared tables? | Check POLICY_REFERENCES |
| **Tagging** | Is there a consistent tag taxonomy? | Review TAGS view |
| **Lineage** | Is ACCESS_HISTORY being leveraged? | Check query patterns |
| **Network** | Are network policies configured? | SHOW NETWORK POLICIES |
| **MFA** | Is MFA enforced? | Check user settings |
| **Trust Center** | Are security scanners enabled? | Trust Center dashboard |

### Classification Coverage

```sql
SELECT
    t.table_catalog AS database_name,
    t.table_schema AS schema_name,
    t.table_name,
    COUNT(tr.tag_name) AS tag_count,
    CASE WHEN COUNT(tr.tag_name) = 0 THEN 'UNCLASSIFIED' ELSE 'CLASSIFIED' END AS status
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES t
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES tr
    ON t.table_catalog = tr.object_database
    AND t.table_schema = tr.object_schema
    AND t.table_name = tr.object_name
    AND tr.domain = 'TABLE'
WHERE t.deleted IS NULL
    AND t.table_schema != 'INFORMATION_SCHEMA'
GROUP BY t.table_catalog, t.table_schema, t.table_name
ORDER BY tag_count ASC;
```

---

## Step 4: Revenue Generation from Data Products

### Identify Monetizable Data Assets

| Question | Assessment Method |
|----------|------------------|
| Which datasets are most queried? | Access history — most popular tables |
| Which datasets serve external consumers? | Existing shares |
| What aggregated/anonymized views exist? | View inventory |
| Are there unique datasets competitors don't have? | Business assessment |
| Can data products be listed on Marketplace? | Compliance + de-identification check |

### Most Queried Tables (Demand Signal)

```sql
SELECT
    obj.value:objectName::VARCHAR AS table_name,
    COUNT(DISTINCT ah.query_id) AS query_count,
    COUNT(DISTINCT ah.user_name) AS distinct_users,
    MIN(ah.query_start_time) AS first_accessed,
    MAX(ah.query_start_time) AS last_accessed
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
    LATERAL FLATTEN(base_objects_accessed) obj
WHERE ah.query_start_time >= DATEADD('MONTH', -3, CURRENT_TIMESTAMP)
GROUP BY table_name
ORDER BY query_count DESC
LIMIT 50;
```

### Revenue Strategy Framework

| Data Product Type | Monetization Model | Snowflake Feature |
|------------------|-------------------|-------------------|
| Raw data feeds | Subscription per tenant | Direct Share |
| Curated analytics | Tiered pricing | Private Listing |
| Aggregated benchmarks | Pay-per-query or flat fee | Public Listing |
| ML model outputs | API-based pricing | Snowpark Container Services |
| Enriched datasets | Premium tier only | Application Package |

---

## Step 5: Modernization Recommendations

Based on assessment findings, generate a prioritized roadmap:

| Priority | Category | Example Recommendations |
|----------|----------|------------------------|
| P0 — Critical | Security | Enable MFA, apply masking to exposed PII, network policies |
| P1 — High | Cost | Right-size warehouses, enable auto-suspend, consolidate idle WHs |
| P1 — High | Governance | Classify sensitive tables, set up tag taxonomy, enable Trust Center |
| P2 — Medium | Architecture | Migrate to proper tenancy model, implement RLAP, set up cost attribution |
| P3 — Low | Revenue | Identify marketplace-ready datasets, create data product catalog |
| P3 — Low | Operational | Implement BCDR, automate tenant onboarding, set up monitoring |

---

## Step 6: Migration Patterns

### View-Based Isolation → RLAP Migration

For platforms currently using WHERE clauses in views for tenant isolation:

```sql
-- Step 1: Add RLAP to the base table
ALTER TABLE {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}
    ADD ROW ACCESS POLICY {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.TENANT_ISOLATION_RAP
    ON ({{TENANT_ID_COLUMN}});

-- Step 2: Replace filtered views with direct table access (RLAP handles filtering)
-- Old: CREATE VIEW V_TENANT_DATA AS SELECT * FROM TABLE WHERE TENANT_ID = 'X';
-- New: Grant SELECT on base table; RLAP enforces filtering automatically.

-- Step 3: Validate — as tenant role, confirm only tenant rows visible
USE ROLE {{TENANT_ROLE}};
SELECT COUNT(*) FROM {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}};
-- Should return only the tenant's row count

-- Step 4: Drop old filtered views after validation
-- DROP VIEW {{DATA_DB}}.{{SCHEMA}}.V_{{OLD_FILTERED_VIEW}};
```

### Schema-Per-Tenant → MTT Consolidation

For platforms with hundreds of identical schemas (one per tenant):

```sql
-- Step 1: Create the consolidated MTT table with TENANT_ID
CREATE TABLE {{DATA_DB}}.{{SHARED_SCHEMA}}.{{TABLE_NAME}} AS
SELECT '{{TENANT_1_ID}}' AS TENANT_ID, * FROM {{DATA_DB}}.{{TENANT_1_SCHEMA}}.{{TABLE_NAME}}
WHERE 1=0;

-- Step 2: Migrate data tenant-by-tenant (non-blocking)
INSERT INTO {{DATA_DB}}.{{SHARED_SCHEMA}}.{{TABLE_NAME}}
SELECT '{{TENANT_ID}}' AS TENANT_ID, * FROM {{DATA_DB}}.{{TENANT_SCHEMA}}.{{TABLE_NAME}};

-- Step 3: Attach RLAP and verify isolation
ALTER TABLE {{DATA_DB}}.{{SHARED_SCHEMA}}.{{TABLE_NAME}}
    ADD ROW ACCESS POLICY {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.TENANT_ISOLATION_RAP
    ON (TENANT_ID);

-- Step 4: Parallel-run validation — both old schema and new MTT table serve queries
-- Compare row counts per tenant between old and new
SELECT '{{TENANT_ID}}' AS tenant, 'OLD' AS source,
    COUNT(*) AS rows FROM {{DATA_DB}}.{{TENANT_SCHEMA}}.{{TABLE_NAME}}
UNION ALL
SELECT '{{TENANT_ID}}', 'NEW',
    COUNT(*) FROM {{DATA_DB}}.{{SHARED_SCHEMA}}.{{TABLE_NAME}} WHERE TENANT_ID = '{{TENANT_ID}}';

-- Step 5: Cut over — update grants, drop old schemas after validation period
```

### Multi-Account Consolidation

For customers with multiple Snowflake accounts to merge into one:

```sql
-- Step 1: Enable replication from source account
-- Run on SOURCE account:
ALTER DATABASE {{SOURCE_DB}} ENABLE REPLICATION TO ACCOUNTS {{TARGET_ACCOUNT}};

-- Step 2: Create replica on TARGET account
-- Run on TARGET account:
CREATE DATABASE {{SOURCE_DB}}_REPLICA AS REPLICA OF {{SOURCE_ACCOUNT}}.{{SOURCE_DB}};
ALTER DATABASE {{SOURCE_DB}}_REPLICA REFRESH;

-- Step 3: Rename and restructure in target account
ALTER DATABASE {{SOURCE_DB}}_REPLICA RENAME TO {{TARGET_DB}};

-- Step 4: Apply tenant tags and RLAP on target
ALTER TABLE {{TARGET_DB}}.{{SCHEMA}}.{{TABLE_NAME}}
    SET TAG {{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.DATA_DOMAIN = '{{DEPARTMENT}}';
ALTER TABLE {{TARGET_DB}}.{{SCHEMA}}.{{TABLE_NAME}}
    ADD ROW ACCESS POLICY {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.TENANT_ISOLATION_RAP
    ON ({{DEPARTMENT_ID_COLUMN}});

-- Step 5: Parallel-run — both accounts serve traffic during validation
-- Step 6: DNS/connection cutover to target account
-- Step 7: Decommission source account after validation period
```

### Migration Validation Checklist

| Check | Query/Method |
|-------|-------------|
| Row count parity (old vs new) | `SELECT COUNT(*) FROM old UNION ALL SELECT COUNT(*) FROM new` |
| Data checksum validation | `SELECT HASH_AGG(*) FROM old` vs `SELECT HASH_AGG(*) FROM new WHERE TENANT_ID = X` |
| RLAP isolation test | Switch to tenant role, confirm only tenant rows visible |
| Masking verification | Switch to tenant role, confirm PHI columns are masked |
| Performance baseline | Compare p50/p95 query times on same workload before/after |
| Grant completeness | `SHOW GRANTS ON SCHEMA` — verify all tenant roles have correct access |

---

## Output

**MANDATORY STOPPING POINT**: Present the assessment report for customer review.

Deliver:
1. **Platform inventory** — databases, schemas, warehouses, roles, shares
2. **Cost efficiency report** — idle resources, optimization opportunities, projected savings
3. **Governance maturity scorecard** — current state vs recommended state
4. **Revenue opportunity assessment** — monetizable data products and strategy
5. **Prioritized modernization roadmap** — P0 through P3 recommendations
6. **Migration plan** — step-by-step with parallel-run validation and rollback
7. **Migration validation checklist** — row counts, checksums, RLAP tests, performance baselines
8. **Edition requirements** — flag features requiring Enterprise or Business Critical

### Edition Requirements

| Feature | Minimum Edition |
|---------|----------------|
| ACCOUNT_USAGE views for discovery | **Enterprise** |
| Row Access Policies (migration target) | **Enterprise** |
| Auto-classification (SYSTEM$CLASSIFY) | **Enterprise** |
| Database Replication (multi-account consolidation) | **Business Critical** for failover groups; **Enterprise** for database replication |
| Trust Center scanners | **Enterprise** |

After approval, route to relevant sub-skills for implementation:
- `tenancy-decision` if architecture change needed
- `governance-security` for governance gaps
- `cost-attribution` for FinOps setup
- `secure-sharing` for data monetization
