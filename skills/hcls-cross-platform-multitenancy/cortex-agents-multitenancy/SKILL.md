---
name: cortex-agents-multitenancy
description: "Cortex Agents multi-tenancy configuration: session attributes, immutable variables, row access policies with SYS_CONTEXT, agent:run API tenant isolation, per-tenant Cortex Search/Analyst. Follows Snowflake documentation at docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-multi-tenancy. Triggers: Cortex Agent multi-tenant, agent multi-tenancy, agent tenant isolation, session attribute, immutable session attribute, agent RBAC, per-tenant agent, Cortex Search tenant, Cortex Analyst tenant."
platform_affinities:
  produces:
    - Cortex Agent configurations
    - session attribute policies
    - per-tenant search services
  benefits_from:
    - skill: cortex-agent
      when: "always — Cortex Agent setup, tool configuration, and API patterns are the core platform capability here"
    - skill: rbac-tenancy
      when: "session-attribute-based row access policies need to be coordinated with the platform RBAC hierarchy"
---

# Cortex Agents for Multi-Tenancy

## When to Use

**Load** this skill when the customer uses or plans to use Snowflake Cortex Agents in a multi-tenant platform and needs tenant-isolated AI-powered applications.

**Reference**: [Snowflake Docs — Multi-tenancy for Cortex Agents](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-multi-tenancy)

---

## Step 1: Understand the Pattern

Cortex Agents support multi-tenancy through **immutable session attributes** paired with **row access policies**. This is a shared responsibility model:

- **Snowflake provides**: session attributes and row access policies
- **Customer configures**: correct tenant boundaries using these tools

### How It Works

1. Application calls `agent:run` API with a `variables` block containing tenant-specific values
2. Before executing any generated SQL, Snowflake sets the session attributes (immutable)
3. Generated SQL runs in the session where variables are active
4. Row access policies on tables evaluate session attributes and filter rows per tenant

---

## Step 2: Configure Session Attributes

### In the agent:run API Call

Pass tenant context as immutable session attributes:

```json
PUT /api/v2/databases/{{DATABASE}}/schemas/{{SCHEMA}}/agents/{{AGENT_NAME}}:run

{
    "variables": {
        "tenant_id": {
            "value": "{{TENANT_ID_VALUE}}",
            "type": "string",
            "is_immutable_session_attribute": true
        }
    }
}
```

Key properties:
- `is_immutable_session_attribute: true` — ensures the value cannot be modified by generated SQL, code execution, or tool invocation during the session
- Session attributes persist for the duration of the interaction
- Cannot be overwritten once set

### Multiple Attributes

You can pass multiple tenant-scoping attributes:

```json
{
    "variables": {
        "tenant_id": {
            "value": "{{TENANT_ID_VALUE}}",
            "type": "string",
            "is_immutable_session_attribute": true
        },
        "region": {
            "value": "{{REGION_VALUE}}",
            "type": "string",
            "is_immutable_session_attribute": true
        },
        "access_tier": {
            "value": "{{TIER_VALUE}}",
            "type": "string",
            "is_immutable_session_attribute": true
        }
    }
}
```

---

## Step 3: Row Access Policies for Agents

### Create RAP Using SYS_CONTEXT

The row access policy must reference `SYS_CONTEXT('SNOWFLAKE$SESSION_ATTRIBUTES', ...)` to read the immutable session attribute:

```sql
CREATE OR REPLACE ROW ACCESS POLICY {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.RAP_AGENT_TENANT_FILTER
  AS (tenant_id_col VARCHAR) RETURNS BOOLEAN ->
    tenant_id_col = SYS_CONTEXT('SNOWFLAKE$SESSION_ATTRIBUTES', 'tenant_id');
```

### Apply to Tables

```sql
ALTER TABLE {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}
  ADD ROW ACCESS POLICY {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.RAP_AGENT_TENANT_FILTER
  ON ({{TENANT_ID_COLUMN}});
```

### Combined RAP (Agent + Direct Access)

If both agents and direct SQL access need tenant isolation:

```sql
CREATE OR REPLACE ROW ACCESS POLICY {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.RAP_COMBINED_TENANT_FILTER
  AS (tenant_id_col VARCHAR) RETURNS BOOLEAN ->
    CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'PLATFORM_ADMIN_ROLE')
    OR tenant_id_col = SYS_CONTEXT('SNOWFLAKE$SESSION_ATTRIBUTES', 'tenant_id')
    OR tenant_id_col IN (
        SELECT TENANT_ID
        FROM {{GOVERNANCE_DB}}.{{POLICY_SCHEMA}}.TENANT_ENTITLEMENTS
        WHERE ROLE_NAME = CURRENT_ROLE()
    );
```

---

## Step 4: Agent Configuration

### Agent Creation

Use the `FROM SPECIFICATION` syntax with a YAML specification block:

```sql
CREATE OR REPLACE AGENT {{DATA_DB}}.{{SCHEMA}}.{{AGENT_NAME}}
  COMMENT = '{{AGENT_DESCRIPTION}}'
  FROM SPECIFICATION
  $$
  models:
    orchestration: auto

  instructions:
    system: '{{SYSTEM_INSTRUCTIONS}}'
    orchestration: '{{ORCHESTRATION_INSTRUCTIONS}}'
    response: '{{RESPONSE_INSTRUCTIONS}}'

  tools:
    - tool_spec:
        type: "cortex_analyst_text_to_sql"
        name: "{{ANALYST_TOOL_NAME}}"
        description: "{{ANALYST_DESCRIPTION}}"
    - tool_spec:
        type: "cortex_search"
        name: "{{SEARCH_TOOL_NAME}}"
        description: "{{SEARCH_DESCRIPTION}}"

  tool_resources:
    {{ANALYST_TOOL_NAME}}:
      semantic_view: "{{DB}}.{{SCHEMA}}.{{SEMANTIC_VIEW}}"
    {{SEARCH_TOOL_NAME}}:
      name: "{{DB}}.{{SCHEMA}}.{{SEARCH_SERVICE}}"
      max_results: "5"
  $$;
```

### Access Control for Agents

```sql
GRANT USAGE ON DATABASE {{DATA_DB}} TO ROLE {{TENANT_ROLE}};
GRANT USAGE ON SCHEMA {{DATA_DB}}.{{SCHEMA}} TO ROLE {{TENANT_ROLE}};
GRANT USAGE ON AGENT {{DATA_DB}}.{{SCHEMA}}.{{AGENT_NAME}} TO ROLE {{TENANT_ROLE}};
```

### Selective Agent Access (Restrict to Specific Roles)

If not all users should access Cortex Agents, grant agent usage only to authorized roles:

```sql
CREATE ROLE IF NOT EXISTS CORTEX_AGENT_USER_ROLE;
GRANT USAGE ON AGENT {{DATA_DB}}.{{SCHEMA}}.{{AGENT_NAME}} TO ROLE CORTEX_AGENT_USER_ROLE;
GRANT ROLE CORTEX_AGENT_USER_ROLE TO ROLE {{AUTHORIZED_ROLE}};
```

To allow a role to create new agents:

```sql
GRANT CREATE AGENT ON SCHEMA {{DATA_DB}}.{{SCHEMA}} TO ROLE {{AUTHORIZED_ROLE}};
```

---

## Step 5: Tool Configuration for Multi-Tenancy

### Cortex Analyst (Structured Data)

The semantic model referenced by Cortex Analyst must include tables with RAPs attached. The RAP automatically filters data based on the session attribute.

### Cortex Search (Unstructured Data)

For Cortex Search services, tenant filtering can be applied via:
- **Filter columns** in the search service definition
- The agent dynamically adjusts filter conditions based on the session attributes

### Custom Tools (Stored Procedures / UDFs)

Custom tools must respect tenant context:

```sql
CREATE OR REPLACE PROCEDURE {{DATA_DB}}.{{SCHEMA}}.SP_TENANT_ACTION(action VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    LET tenant := SYS_CONTEXT('SNOWFLAKE$SESSION_ATTRIBUTES', 'tenant_id');
    -- Use tenant variable in all queries
    ...
$$;
```

---

## Step 6: Best Practices (Per Snowflake Documentation)

1. **Always use `is_immutable_session_attribute: true`** for any variable referenced by a row access policy — prevents generated SQL or tool execution from modifying tenant context
2. **Scope variables to the narrowest level** — only pass what the agent needs for the current request
3. **Test RAPs independently** — verify row access policies filter correctly using `SET` statements outside the agent before deploying
4. **Use the DEFAULT_ROLE of the querying user** — Cortex Agents use the default role; ensure it has the correct grants
5. **Single agent, multiple tenants** — one agent definition can serve all tenants; tenant context is set per-invocation via the API

---

## Step 7: Application Integration Pattern

### Tenant-Aware Application Layer

The application calling the agent:run API is responsible for:

1. **Authenticating the end user** and determining their tenant
2. **Passing the correct tenant_id** as an immutable session attribute
3. **Never exposing the agent:run API directly** to end users without tenant context enforcement

### Thread Management

Use threads to maintain conversation context per tenant:

```json
POST /api/v2/databases/{{DATABASE}}/schemas/{{SCHEMA}}/agents/{{AGENT_NAME}}:run

{
    "thread_id": "{{THREAD_ID}}",
    "variables": {
        "tenant_id": {
            "value": "{{TENANT_ID_VALUE}}",
            "type": "string",
            "is_immutable_session_attribute": true
        }
    },
    "messages": [
        {"role": "user", "content": "{{USER_QUERY}}"}
    ]
}
```

---

## Output

**MANDATORY STOPPING POINT**: Present the Cortex Agent multi-tenancy design for customer approval.

Deliver:
1. **Agent configuration** — agent creation SQL with tools and instructions
2. **Session attribute design** — which variables to pass per-tenant
3. **Row access policies** — using SYS_CONTEXT for agent-compatible filtering
4. **RBAC grants** — agent access per tenant role
5. **Application integration guidance** — how to call agent:run with tenant context
6. **Testing plan** — how to verify tenant isolation independently

After approval, route to:
- `ai-governance` for Cortex Guard and AI safety
- `rbac-tenancy` for broader RBAC setup
- `implementation` for executable scripts
