---
name: tenancy-decision
description: "Multi-tenancy pattern selection: Multi-Tenant Table (MTT), One-Per-Tenant (OPT), Hybrid. Decision framework based on tenant count, isolation needs, compliance, cost model, and operational complexity. Triggers: tenancy decision, tenancy pattern, MTT, OPT, hybrid tenancy, shared vs dedicated, tenant isolation model, schema per tenant, database per tenant."
platform_affinities:
  produces: []
  benefits_from:
    - skill: cost-intelligence
      when: "customer needs to understand cost implications of each tenancy pattern before deciding"
---

# Tenancy Pattern Decision Framework

## When to Use

**Load** this skill when the customer needs to decide between Multi-Tenant Table (MTT), One-Per-Tenant (OPT), or Hybrid tenancy patterns.

---

## Step 1: Collect Decision Inputs

Gather the following from the customer before making a recommendation:

| Input | Options/Range |
|-------|--------------|
| Number of tenants (current) | 1–10, 10–100, 100–1000, 1000+ |
| Expected tenant growth (12 months) | Low (<20%), Medium (20–100%), High (>100%) |
| Data isolation requirement | Logical (row-level), Schema-level, Database-level, Account-level |
| Compliance requirements | None, SOC2, HIPAA, PCI-DSS, GDPR, FedRAMP, industry-specific |
| Tenant size variance | Uniform, moderate variance, extreme variance (whales + long tail) |
| Customization per tenant | None, minor (config), major (schema differences) |
| Cost model | Shared cost pool, per-tenant chargeback, tiered pricing |
| Data sharing needs | No sharing, provider-to-tenant, tenant-to-tenant, external marketplace |
| Operational team size | Small (1–3), Medium (4–10), Large (10+) |

---

## Step 2: Decision Matrix

| Criterion | MTT (Multi-Tenant Table) | OPT (One-Per-Tenant) | Hybrid |
|-----------|-------------------------|---------------------|--------|
| **Tenant count** | Best for 50+ tenants | Best for <20 tenants | Mixed portfolio |
| **Isolation** | Logical via Row Access Policy | Physical (schema/database) | Logical for most, physical for premium |
| **Compliance** | Meets most with proper RLAP + masking | Strongest isolation for strict regs | Tiered by compliance need |
| **Schema uniformity** | Required — all tenants same schema | Flexible per tenant | Uniform base + tenant extensions |
| **Cost attribution** | Complex — requires tag-based allocation | Simple — dedicated warehouse per tenant | Mixed |
| **Operational overhead** | Low — single set of objects to manage | High — N copies of everything | Medium |
| **Query performance** | Excellent with clustering on TENANT_ID | Excellent — no cross-tenant scan | Varies |
| **Data sharing** | Easy — secure views with RLAP | Easy — per-tenant shares | Both |
| **Onboarding speed** | Fast — insert entitlement row | Slower — create schema/database/roles | Fast for MTT tier, slower for OPT |

---

## Step 3: Pattern Definitions

### MTT — Multi-Tenant Table

All tenants share the same tables, schemas, and warehouses. Tenant isolation is enforced by **Row Access Policies (RLAP)** on a `TENANT_ID` column.

**Architecture:**
```
{{DATA_DB}}
  └── {{SHARED_SCHEMA}}
        ├── TABLE_1 (TENANT_ID, ...)  ← RLAP attached
        ├── TABLE_2 (TENANT_ID, ...)  ← RLAP attached
        └── V_TABLE_1_SECURE          ← Secure view with RLAP
```

**Best for:** SaaS platforms with many uniform tenants, rapid onboarding, centralized operations.

**Snowflake features used:** Row Access Policies, Object Tagging, Shared Warehouses, Query Tags for cost attribution.

### OPT — One-Per-Tenant

Each tenant gets dedicated schema (or database) and optionally a dedicated warehouse. Isolation is physical.

**Architecture:**
```
{{DATA_DB}}
  ├── {{TENANT_A_SCHEMA}}
  │     ├── TABLE_1 (...)
  │     └── TABLE_2 (...)
  ├── {{TENANT_B_SCHEMA}}
  │     ├── TABLE_1 (...)
  │     └── TABLE_2 (...)
  └── SHARED_REFERENCE
        └── LOOKUP_TABLES
```

**Best for:** Highly regulated industries, tenants with different schemas, tenants requiring dedicated compute, strict data residency.

**Snowflake features used:** Schema-level grants, Dedicated Warehouses, Resource Monitors, Per-tenant shares.

### Hybrid

Combine MTT for the majority of tenants with OPT for premium/enterprise tenants.

**Architecture:**
```
{{DATA_DB}}
  ├── SHARED_SCHEMA          ← MTT tenants (RLAP on TENANT_ID)
  ├── {{PREMIUM_TENANT_A}}   ← OPT dedicated schema
  └── {{PREMIUM_TENANT_B}}   ← OPT dedicated schema
```

**Best for:** Platforms with tiered service offerings (Basic/Premium/Enterprise), mixed compliance needs.

---

## Step 4: Recommendation Output

**MANDATORY STOPPING POINT**: Present the recommendation for customer approval before proceeding.

Deliver:

1. **Recommended pattern** with justification based on the customer's inputs
2. **Architecture diagram** showing database/schema/warehouse layout using the customer's entity names
3. **Trade-offs acknowledged** — what the customer gains and gives up with this choice
4. **Migration considerations** — if brownfield, how to transition from current state
5. **Edition requirements** — flag features requiring Enterprise or Business Critical edition

### Edition Requirements

| Feature Used in Tenancy | Minimum Edition |
|------------------------|----------------|
| Row Access Policies (RLAP) for MTT | **Enterprise** |
| Dynamic Data Masking | Standard (basic), **Enterprise** (tag-based) |
| Multi-cluster Warehouses for OPT | **Enterprise** |
| Query Acceleration Service | **Enterprise** |
| HIPAA BAA / PCI-DSS compliance | **Business Critical** |
| Tri-Secret Secure (customer-managed keys) | **Business Critical** |

After approval, route to:
- `rbac-tenancy` for RBAC setup
- `implementation` for foundation scripts
- `cost-attribution` for FinOps design
