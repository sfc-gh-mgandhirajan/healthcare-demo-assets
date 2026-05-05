---
name: rbac-tenancy
description: "RBAC design for multi-tenant platforms: role hierarchies, database roles, row access policies, masking policies, tenant entitlements, grant management, least-privilege access. Triggers: RBAC, role hierarchy, database role, row access policy, RLAP, masking policy, tenant role, grant, privilege, access control, least privilege, role-based access."
platform_affinities:
  produces:
    - row access policies
    - masking policies
    - object tags
    - database roles
  benefits_from:
    - skill: data-governance
      when: "always — tag-based masking and Horizon classification extend RBAC with automated policy enforcement"
---

# RBAC & Tenant Access Control

## When to Use

**Load** this skill when the customer needs to design or implement RBAC (Role-Based Access Control) for multi-tenant platforms, including role hierarchies, row access policies, and masking policies.

---

## Step 1: Understand the Customer's Access Model

Gather:

| Input | Description |
|-------|-------------|
| Tenancy pattern chosen | MTT, OPT, or Hybrid (from tenancy-decision skill) |
| Number of distinct access levels per tenant | e.g., Admin, Analyst, Viewer |
| Cross-tenant access needs | Platform admins, FinOps, governance teams |
| Compliance-driven access controls | PHI masking, PII redaction, data classification tagging |
| Integration service accounts | ETL tools, BI tools, Cortex Agents, external apps |
| Existing roles (brownfield) | Current role hierarchy to assess |

---

## Step 2: Role Hierarchy Design

### Platform-Level Roles (Always Required)

```
ACCOUNTADMIN
  └── SYSADMIN
        ├── PLATFORM_ADMIN_ROLE        — Full platform management
        │     ├── GOVERNANCE_ADMIN_ROLE — Tags, policies, classification
        │     ├── DATA_ENGINEER_ROLE    — Pipelines, transformations
        │     └── FINOPS_ADMIN_ROLE     — Cost attribution, budgets
        ├── {{TENANT_A}}_ADMIN_ROLE     — Tenant A administration
        │     ├── {{TENANT_A}}_ANALYST_ROLE
        │     └── {{TENANT_A}}_VIEWER_ROLE
        └── {{TENANT_B}}_ADMIN_ROLE
              ├── {{TENANT_B}}_ANALYST_ROLE
              └── {{TENANT_B}}_VIEWER_ROLE
```

### MTT Role Pattern

For shared-table tenancy, use a **flat tenant role** + **Row Access Policy**:

```sql
CREATE ROLE IF NOT EXISTS {{TENANT_ID}}_ROLE
    COMMENT = 'Tenant role for {{TENANT_NAME}}';
GRANT ROLE {{TENANT_ID}}_ROLE TO ROLE PLATFORM_ADMIN_ROLE;

GRANT USAGE ON DATABASE {{DATA_DB}} TO ROLE {{TENANT_ID}}_ROLE;
GRANT USAGE ON SCHEMA {{DATA_DB}}.{{SHARED_SCHEMA}} TO ROLE {{TENANT_ID}}_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA {{DATA_DB}}.{{SHARED_SCHEMA}} TO ROLE {{TENANT_ID}}_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA {{DATA_DB}}.{{SHARED_SCHEMA}} TO ROLE {{TENANT_ID}}_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA {{DATA_DB}}.{{SHARED_SCHEMA}} TO ROLE {{TENANT_ID}}_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA {{DATA_DB}}.{{SHARED_SCHEMA}} TO ROLE {{TENANT_ID}}_ROLE;
GRANT USAGE ON WAREHOUSE {{SHARED_WH}} TO ROLE {{TENANT_ID}}_ROLE;
```

### OPT Role Pattern

For dedicated-schema tenancy, use a **tiered role hierarchy** per tenant:

```sql
CREATE ROLE IF NOT EXISTS {{TENANT_ID}}_ADMIN_ROLE;
CREATE ROLE IF NOT EXISTS {{TENANT_ID}}_ANALYST_ROLE;
CREATE ROLE IF NOT EXISTS {{TENANT_ID}}_VIEWER_ROLE;

GRANT ROLE {{TENANT_ID}}_VIEWER_ROLE TO ROLE {{TENANT_ID}}_ANALYST_ROLE;
GRANT ROLE {{TENANT_ID}}_ANALYST_ROLE TO ROLE {{TENANT_ID}}_ADMIN_ROLE;
GRANT ROLE {{TENANT_ID}}_ADMIN_ROLE TO ROLE PLATFORM_ADMIN_ROLE;

GRANT USAGE ON DATABASE {{DATA_DB}} TO ROLE {{TENANT_ID}}_VIEWER_ROLE;
GRANT USAGE ON SCHEMA {{DATA_DB}}.{{TENANT_SCHEMA}} TO ROLE {{TENANT_ID}}_VIEWER_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA {{DATA_DB}}.{{TENANT_SCHEMA}} TO ROLE {{TENANT_ID}}_VIEWER_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA {{DATA_DB}}.{{TENANT_SCHEMA}} TO ROLE {{TENANT_ID}}_VIEWER_ROLE;
GRANT ALL ON SCHEMA {{DATA_DB}}.{{TENANT_SCHEMA}} TO ROLE {{TENANT_ID}}_ADMIN_ROLE;
GRANT USAGE ON WAREHOUSE WH_{{TENANT_ID}} TO ROLE {{TENANT_ID}}_VIEWER_ROLE;
```

### Database Roles (Recommended for Sharing)

Use **database roles** when sharing data across accounts, as they can be granted via shares:

```sql
CREATE DATABASE ROLE IF NOT EXISTS {{DATA_DB}}.DR_{{TENANT_ID}}_READ;
GRANT SELECT ON ALL TABLES IN SCHEMA {{DATA_DB}}.{{SCHEMA}} TO DATABASE ROLE {{DATA_DB}}.DR_{{TENANT_ID}}_READ;
GRANT DATABASE ROLE {{DATA_DB}}.DR_{{TENANT_ID}}_READ TO SHARE {{SHARE_NAME}};
```

---

## Step 3: Row Access Policies (MTT & Hybrid)

### Entitlements-Based RLAP

```sql
CREATE TABLE IF NOT EXISTS {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.TENANT_ENTITLEMENTS (
    ROLE_NAME       VARCHAR NOT NULL,
    TENANT_ID       VARCHAR NOT NULL,
    ACCESS_LEVEL    VARCHAR DEFAULT 'READ',
    GRANTED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    GRANTED_BY      VARCHAR DEFAULT CURRENT_USER(),
    CONSTRAINT pk_entitlements PRIMARY KEY (ROLE_NAME, TENANT_ID)
);

CREATE OR REPLACE ROW ACCESS POLICY {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.TENANT_ISOLATION_RAP
AS (tenant_id_col VARCHAR) RETURNS BOOLEAN ->
    CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'PLATFORM_ADMIN_ROLE')
    OR tenant_id_col IN (
        SELECT TENANT_ID
        FROM {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.TENANT_ENTITLEMENTS
        WHERE ROLE_NAME = CURRENT_ROLE()
    );
```

### Attach RLAP to Tables

```sql
ALTER TABLE {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}
    ADD ROW ACCESS POLICY {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.TENANT_ISOLATION_RAP
    ON (TENANT_ID);
```

---

## Step 4: Masking Policies

### Generic Masking Policies

```sql
CREATE OR REPLACE MASKING POLICY {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.SENSITIVE_FULL_MASK
AS (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'PLATFORM_ADMIN_ROLE')
        THEN val
        ELSE '***MASKED***'
    END;

CREATE OR REPLACE MASKING POLICY {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.SENSITIVE_PARTIAL_MASK
AS (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'PLATFORM_ADMIN_ROLE')
        THEN val
        ELSE CONCAT('***', RIGHT(val, 4))
    END;

CREATE OR REPLACE MASKING POLICY {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.DATE_MASK
AS (val DATE) RETURNS DATE ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'PLATFORM_ADMIN_ROLE')
        THEN val
        ELSE DATE_TRUNC('YEAR', val)
    END;
```

### Tag-Based Masking (Recommended)

Use Snowflake's **tag-based masking** to automatically apply masking policies based on classification tags:

```sql
ALTER TAG {{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.SENSITIVITY_LEVEL
    SET MASKING POLICY {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.SENSITIVE_FULL_MASK;
```

This ensures that any column tagged with the sensitivity tag automatically gets the masking policy applied — no per-column grants needed.

---

## Step 5: Governance Tags

```sql
CREATE TAG IF NOT EXISTS {{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.SENSITIVITY_LEVEL
    ALLOWED_VALUES 'FULL_MASK', 'PARTIAL_MASK', 'DATE_MASK', 'NONE'
    COMMENT = 'Data sensitivity level for tag-based masking';

CREATE TAG IF NOT EXISTS {{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.DATA_CLASSIFICATION
    ALLOWED_VALUES 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED'
    COMMENT = 'Data classification level';

CREATE TAG IF NOT EXISTS {{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.TENANT_ID
    COMMENT = 'Tenant identifier for lineage and governance tracking';
```

---

## Output

**MANDATORY STOPPING POINT**: Present the RBAC design for customer approval before generating scripts.

Deliver:
1. **Role hierarchy diagram** using the customer's tenant and role names
2. **Row Access Policy definition** tailored to the customer's tenancy pattern
3. **Masking policy set** based on the customer's data sensitivity requirements
4. **Grant scripts** — parameterized with `{{VARIABLES}}` for the customer to fill in
5. **Entitlements table** design for dynamic RLAP
6. **Database roles** if secure sharing is needed
7. **Edition requirements** — see table below

### Edition Requirements

| Feature | Minimum Edition |
|---------|----------------|
| Row Access Policies | **Enterprise** |
| Tag-based masking policies | **Enterprise** |
| Column-level masking policies | Standard+ |
| Database roles | Standard+ |
| Object tagging (CREATE TAG) | Standard+ |

After approval, route to:
- `implementation` to generate executable scripts
- `governance-security` for Horizon governance setup
