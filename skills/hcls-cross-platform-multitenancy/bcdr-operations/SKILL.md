---
name: bcdr-operations
description: "BCDR for multi-tenant platforms: database replication, failover groups, client redirect, account replication, DR drill procedures, RTO/RPO targets, cross-region/cross-cloud replication. Triggers: BCDR, disaster recovery, business continuity, failover, failback, replication, failover group, client redirect, cross-region, DR drill, RTO, RPO, secondary account."
platform_affinities:
  produces:
    - replication groups
    - failover group configurations
    - DR runbooks
  benefits_from:
    - skill: network-security
      when: "private link or network policy configurations must be replicated alongside data for full DR parity"
---

# Business Continuity & Disaster Recovery (BCDR)

## When to Use

**Load** this skill when the customer needs to design or implement BCDR for a multi-tenant Snowflake platform — including replication, failover, and recovery procedures.

---

## Step 1: Understand BCDR Requirements

Gather:

| Input | Description |
|-------|-------------|
| RTO target | How fast must the platform recover? (minutes, hours) |
| RPO target | How much data loss is acceptable? (near-zero, minutes, hours) |
| Primary region/cloud | Where the production account runs |
| Secondary region/cloud | Where the DR account runs |
| What must be replicated? | Databases, shares, users/roles, warehouses, resource monitors |
| Tenant SLA tiers | Do premium tenants get better DR guarantees? |
| DR testing frequency | Quarterly, semi-annual, annual |
| Edition | Business Critical or Enterprise (required for failover groups) |

---

## Step 2: Replication Architecture

### Failover Groups (Recommended)

Failover groups replicate databases, shares, roles, users, and warehouses as a unit:

```sql
-- On PRIMARY account
CREATE FAILOVER GROUP IF NOT EXISTS FG_{{PLATFORM_NAME}}
    OBJECT_TYPES = DATABASES, SHARES, ROLES, USERS, WAREHOUSES, RESOURCE MONITORS, INTEGRATIONS
    ALLOWED_DATABASES = {{DATA_DB}}, {{GOVERNANCE_DB}}, {{COST_DB}}
    ALLOWED_SHARES = {{SHARE_LIST}}
    ALLOWED_INTEGRATION_TYPES = SECURITY INTEGRATIONS
    REPLICATION_SCHEDULE = '{{REPLICATION_INTERVAL}} MINUTE'
    ALLOWED_ACCOUNTS = {{SECONDARY_ACCOUNT}};
```

```sql
-- On SECONDARY account
CREATE FAILOVER GROUP IF NOT EXISTS FG_{{PLATFORM_NAME}}
    AS REPLICA OF {{PRIMARY_ACCOUNT}}.FG_{{PLATFORM_NAME}};
```

### Database Replication (Simpler Alternative)

For replicating specific databases without full failover group:

```sql
ALTER DATABASE {{DATA_DB}} ENABLE REPLICATION TO ACCOUNTS {{SECONDARY_ACCOUNT}};

-- On SECONDARY account
CREATE DATABASE {{DATA_DB}} AS REPLICA OF {{PRIMARY_ACCOUNT}}.{{DATA_DB}};
ALTER DATABASE {{DATA_DB}} REFRESH;
```

---

## Step 3: Replication Strategy by Tenancy Pattern

### MTT (Shared Tables)

| Component | Replication Approach |
|-----------|---------------------|
| Data databases | Include in failover group |
| Governance database | Include in failover group (policies, entitlements) |
| Cost database | Include in failover group |
| Shares | Include in failover group |
| Roles & Users | Include in failover group |

### OPT (Dedicated Per-Tenant)

| Component | Replication Approach |
|-----------|---------------------|
| All tenant databases/schemas | Include in failover group |
| Per-tenant warehouses | Include in failover group |
| Per-tenant resource monitors | Include in failover group |
| Per-tenant shares | Include in failover group |

### Tiered DR (Premium Tenants Only)

For hybrid platforms where only premium tenants get DR:

```sql
CREATE FAILOVER GROUP IF NOT EXISTS FG_PREMIUM
    OBJECT_TYPES = DATABASES, SHARES, ROLES
    ALLOWED_DATABASES = {{PREMIUM_TENANT_DBS}}
    REPLICATION_SCHEDULE = '10 MINUTE'
    ALLOWED_ACCOUNTS = {{SECONDARY_ACCOUNT}};
```

---

## Step 4: Client Redirect

Enable automatic client redirect so applications reconnect to the secondary after failover:

```sql
-- On PRIMARY account
CREATE CONNECTION IF NOT EXISTS CONN_{{PLATFORM_NAME}}
    AS PRIMARY;
ALTER CONNECTION CONN_{{PLATFORM_NAME}} ENABLE FAILOVER TO ACCOUNTS {{SECONDARY_ACCOUNT}};

-- On SECONDARY account
CREATE CONNECTION IF NOT EXISTS CONN_{{PLATFORM_NAME}}
    AS REPLICA OF {{PRIMARY_ACCOUNT}}.CONN_{{PLATFORM_NAME}};
```

Applications connect using the **connection URL** instead of the account URL, enabling automatic redirect.

---

## Step 5: Failover & Failback Procedures

### Planned Failover (DR Drill)

```sql
-- On SECONDARY account (promote to primary)
ALTER FAILOVER GROUP FG_{{PLATFORM_NAME}} PRIMARY;

-- Verify
SHOW FAILOVER GROUPS;
```

### Planned Failback (Return to Original Primary)

```sql
-- On ORIGINAL PRIMARY account (now secondary)
ALTER FAILOVER GROUP FG_{{PLATFORM_NAME}} PRIMARY;
```

### Monitoring Replication Lag

```sql
SELECT
    PHASE,
    START_TIME,
    END_TIME,
    TOTAL_BYTES_REPLICATED,
    OBJECT_COUNT,
    ERROR
FROM TABLE(INFORMATION_SCHEMA.REPLICATION_GROUP_REFRESH_PROGRESS('FG_{{PLATFORM_NAME}}'));
```

### Replication History

```sql
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_USAGE_HISTORY
WHERE GROUP_NAME = 'FG_{{PLATFORM_NAME}}'
    AND START_TIME >= DATEADD('DAY', -30, CURRENT_TIMESTAMP)
ORDER BY START_TIME DESC;
```

---

## Step 6: DR Runbook Template

| Step | Action | Owner | RTO Impact |
|------|--------|-------|-----------|
| 1 | Detect outage (monitoring alert or Snowflake status) | Platform Ops | 0 min |
| 2 | Assess severity — is failover warranted? | Platform Lead | 5 min |
| 3 | Notify stakeholders (tenants, internal teams) | Comms | 10 min |
| 4 | Execute failover: `ALTER FAILOVER GROUP ... PRIMARY` | Platform Ops | 15 min |
| 5 | Verify client redirect is working | Platform Ops | 20 min |
| 6 | Validate data integrity on secondary | Data Engineering | 30 min |
| 7 | Confirm tenant access and operations | Tenant Support | 45 min |
| 8 | Monitor secondary for stability | Platform Ops | Ongoing |
| 9 | Plan failback when primary is restored | Platform Lead | Post-recovery |

---

## Output

**MANDATORY STOPPING POINT**: Present the BCDR plan for customer approval.

Deliver:
1. **Replication architecture** — failover group configuration
2. **RTO/RPO analysis** — achievable targets based on replication schedule
3. **Client redirect setup** — connection configuration
4. **Failover/failback procedures** — step-by-step runbook
5. **Monitoring queries** — replication lag and history
6. **DR drill plan** — schedule and procedure for testing
7. **Edition requirements** — flag Business Critical requirements

After approval, route to:
- `implementation` for executable scripts
