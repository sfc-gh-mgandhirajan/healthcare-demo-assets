---
name: best-practices
description: "Well-architected framework and best practices for multi-tenant Snowflake platforms: design patterns, discovery, interoperability, performance, scalability, security, operational excellence, reliability. Triggers: best practices, well-architected, design pattern, discovery, interoperability, performance, scalability, anti-pattern, operational excellence, reliability, framework, guidance."
platform_affinities:
  produces: []
  benefits_from: []
---

# Best Practices & Well-Architected Framework

## When to Use

**Load** this skill when the customer needs overarching design guidance, well-architected review, or best practices for multi-tenant Snowflake platforms.

---

## Pillar 1: Security & Governance

### Best Practices

| Practice | Description |
|----------|-------------|
| **Least-privilege RBAC** | Grant minimum permissions needed; use role hierarchies; avoid ACCOUNTADMIN for operations |
| **Row Access Policies for MTT** | Every shared table must have RLAP on TENANT_ID; test isolation before onboarding |
| **Tag-based masking** | Use classification tags to auto-apply masking; avoid per-column manual policy attachment |
| **Auto-classification** | Run `SYSTEM$CLASSIFY` on new tables to detect PII automatically |
| **Network policies** | Restrict access by tenant IP range where applicable |
| **MFA enforcement** | Require MFA for all interactive users, especially admin roles |
| **Trust Center** | Enable Security Essentials and CIS Benchmark scanners |
| **Cortex Guard** | Apply input/output safety filters for all tenant-facing LLM interactions |

### Anti-Patterns to Avoid

| Anti-Pattern | Risk | Remedy |
|-------------|------|--------|
| Using ACCOUNTADMIN for tenant operations | Full account exposure | Create PLATFORM_ADMIN_ROLE |
| No RLAP on shared tables | Cross-tenant data leakage | Attach RLAP to every MTT table |
| Hardcoded role checks in views | Maintenance nightmare | Use entitlements table + RLAP |
| No masking on sensitive columns | Compliance violation | Tag-based masking framework |
| Sharing raw data without secure views | Data exposure | Always use SECURE VIEW for shares |

---

## Pillar 2: Cost Efficiency

### Best Practices

| Practice | Description |
|----------|-------------|
| **Right-size warehouses** | Start small, monitor queuing, scale up only when needed |
| **Auto-suspend at 300s or less** | Prevent idle credit consumption |
| **Query acceleration** | Enable for ad-hoc workloads; avoid for predictable batch |
| **Multi-cluster for concurrency** | Use scaling policies instead of oversized single warehouses |
| **Tag-based cost attribution** | Tag all warehouses, schemas, and users with TENANT_ID |
| **Resource monitors** | Set per-tenant credit quotas with suspend triggers |
| **Query tags for MTT** | Use session QUERY_TAG to attribute shared warehouse costs |
| **Serverless features** | Use Dynamic Tables, Snowpipe, serverless tasks where possible to avoid idle compute |

### Anti-Patterns to Avoid

| Anti-Pattern | Risk | Remedy |
|-------------|------|--------|
| One XL warehouse for everything | No cost attribution, waste | Per-tenant or per-workload warehouses |
| No auto-suspend | Continuous credit burn | Set AUTO_SUSPEND = 300 |
| No resource monitors | Runaway costs | Resource monitors with suspend triggers |
| No cost visibility per tenant | Cannot bill back | Tag-based attribution model |

---

## Pillar 3: Reliability & Performance

### Best Practices

| Practice | Description |
|----------|-------------|
| **Cluster on TENANT_ID** | For MTT tables, add TENANT_ID as first clustering key |
| **Search optimization** | Enable for tables with point-lookup queries |
| **Dynamic Tables over Tasks** | Prefer for continuous transformations (auto-managed dependencies) |
| **Materialized views** | For expensive aggregations queried frequently |
| **BCDR with failover groups** | Replicate databases, roles, and shares as a unit |
| **Client redirect** | Use connection objects for automatic failover |
| **Monitor replication lag** | Alert when lag exceeds RPO target |

### Performance Optimization for MTT

```sql
ALTER TABLE {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}
    CLUSTER BY (TENANT_ID, {{DATE_COLUMN}});

ALTER TABLE {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}
    ADD SEARCH OPTIMIZATION ON EQUALITY(TENANT_ID);
```

### Anti-Patterns to Avoid

| Anti-Pattern | Risk | Remedy |
|-------------|------|--------|
| No clustering on TENANT_ID | Full table scans for tenant queries | Cluster on (TENANT_ID, date) |
| Using Tasks for everything | Complex dependency management | Dynamic Tables for continuous pipelines |
| No BCDR plan | Single point of failure | Failover groups + client redirect |

---

## Pillar 4: Operational Excellence

### Best Practices

| Practice | Description |
|----------|-------------|
| **Parameterized scripts** | Use `{{VARIABLES}}` in all templates; never hardcode tenant names |
| **Automated onboarding** | Script tenant creation end-to-end (role, grants, entitlements, tags) |
| **Infrastructure as code** | Use Terraform, Snowflake CLI, or stored procedures for repeatable setup |
| **Monitoring dashboards** | Cost, performance, security — per tenant and platform-wide |
| **Tenant lifecycle management** | Onboard, suspend, offboard procedures documented and scripted |
| **Data quality DMFs** | Attach freshness, null count, unique count to critical tables |
| **Audit logging** | Leverage ACCESS_HISTORY and QUERY_HISTORY for compliance |

### Tenant Lifecycle Stages

```
PROSPECT → ONBOARDING → ACTIVE → SUSPENDED → OFFBOARDED
```

| Stage | Actions |
|-------|---------|
| Prospect | Gather requirements, select tenancy pattern |
| Onboarding | Create roles, grants, warehouses, entitlements, tags |
| Active | Monitor costs, performance, governance compliance |
| Suspended | Revoke warehouse access, retain data |
| Offboarded | Archive data, drop roles, remove entitlements |

---

## Pillar 5: Data Product Excellence

### Best Practices

| Practice | Description |
|----------|-------------|
| **Layered architecture** | RAW → STAGED → CURATED → DATA_PRODUCT |
| **Schema-on-read for RAW** | Use VARIANT columns for flexibility |
| **Schema-on-write for CURATED** | Strong typing, constraints, documentation |
| **Secure views for sharing** | Never share base tables directly |
| **Declarative sharing** | Prefer Application Packages for versioned data products |
| **Data clean rooms** | For cross-tenant analytics without raw data exposure |
| **Marketplace strategy** | Private listings for partners, public for monetization |

---

## Pillar 6: AI & Innovation

### Best Practices

| Practice | Description |
|----------|-------------|
| **Cortex Guard on all tenant-facing LLMs** | Filter input and output for safety |
| **Immutable session attributes for Agents** | Use `is_immutable_session_attribute: true` |
| **RAP on Agent data sources** | Ensure Cortex Agents respect tenant boundaries |
| **AI cost tracking** | Tag AI warehouses and use query tags for attribution |
| **Model access by tier** | Restrict expensive models to premium tenants |
| **Data residency for AI** | Process data in the same region as the Snowflake account |

---

## Pillar 7: Interoperability

### Best Practices

| Practice | Description |
|----------|-------------|
| **Iceberg Tables** | For open-format interoperability with non-Snowflake tools |
| **External Tables** | For reading data lake files without copying |
| **Snowpark Container Services** | For custom APIs, non-SQL workloads |
| **REST API** | For programmatic access from external applications |
| **Kafka Connector** | For real-time event integration |
| **dbt** | For version-controlled, tested transformations |

---

## Discovery Checklist

When starting any multi-tenant engagement, use this checklist:

- [ ] Industry and compliance requirements identified
- [ ] Tenant count and growth projection documented
- [ ] Tenancy pattern selected (MTT/OPT/Hybrid)
- [ ] Data products and entities cataloged
- [ ] RBAC hierarchy designed
- [ ] Cost attribution model defined
- [ ] Governance framework planned
- [ ] AI usage and governance needs assessed
- [ ] Sharing and distribution strategy defined
- [ ] BCDR requirements documented
- [ ] Snowflake edition confirmed (Standard/Enterprise/Business Critical)
- [ ] Implementation scripts parameterized and reviewed

### Healthcare & Life Sciences Discovery Additions

If the customer is in HLS, also confirm:
- [ ] Compliance framework identified (HIPAA, 21 CFR Part 11, GINA, GxP, FERPA)
- [ ] PHI/PII columns cataloged and mapped to HIPAA 18 identifiers
- [ ] De-identification method chosen (Safe Harbor vs Expert Determination)
- [ ] Patient consent model documented (opt-in, opt-out, per-purpose)
- [ ] Clinical data standards identified (FHIR, HL7v2, CDISC, OMOP)
- [ ] Research data access governed by IRB protocol entitlements
- [ ] Genomic data re-identification risk assessed (if applicable)
- [ ] Business Critical edition confirmed (required for HIPAA BAA)

---

## Pillar 8: Industry Compliance (Healthcare & Life Sciences)

### Best Practices

| Practice | Description |
|----------|-------------|
| **HIPAA 18-identifier masking** | Map every PHI column to one of the 18 HIPAA Safe Harbor identifiers; apply tag-based masking |
| **Custom classifiers for clinical data** | Build custom classifiers for MRN, health plan IDs, NPI — not covered by native classification |
| **Consent-aware RLAP** | For HIEs and patient portals, combine RLAP with a PATIENT_CONSENT lookup table |
| **Safe Harbor de-identification views** | Create secure views that strip all 18 identifiers for research and Marketplace |
| **21 CFR Part 11 audit trail** | Use ACCESS_HISTORY + QUERY_HISTORY; ensure no DELETE without audit record |
| **IRB protocol entitlements** | Use an IRB_APPROVALS table in RLAP so researchers only access approved study data |
| **Genomic k-anonymity** | Never expose individual variant calls in shares; aggregate with minimum group sizes |
| **Business Critical for BAA** | HIPAA Business Associate Agreement requires Business Critical edition |
| **Cortex Guard for clinical AI** | Always filter LLM inputs/outputs when processing clinical text to prevent PHI leakage |
| **Data residency tags** | Tag tables with DATA_RESIDENCY to enforce regional processing requirements |

### HLS Anti-Patterns to Avoid

| Anti-Pattern | Risk | Remedy |
|-------------|------|--------|
| PHI in plaintext without masking | HIPAA violation, breach liability | Auto-classify + tag-based masking for all 18 identifiers |
| WHERE-clause tenant isolation (no RLAP) | Application bug exposes cross-tenant PHI | Replace all filtered views with RLAP on base tables |
| Consent enforced only in app layer | Data access bypasses consent via direct SQL | Consent-aware RLAP that joins PATIENT_CONSENT table |
| Sharing raw PHI to Marketplace | Regulatory violation | Only share Safe Harbor de-identified secure views |
| No audit trail for PHI access | HIPAA audit failure | Enable ACCESS_HISTORY (Enterprise+), query PHI access patterns |
| Genomic variants shared at individual level | Re-identification via rare variants | Aggregate with k-anonymity (min group size ≥ 5) |
| Same warehouse for clinical AI and reporting | AI cost attribution impossible; AI spikes impact dashboards | Dedicated AI warehouse with cost tags |
| MRN/NPI used as join keys in shared views | Exposes identifiers to unauthorized roles | Hash MRN/NPI in shared views: `SHA2(MRN, 256)` |
| No Time Travel on clinical tables | Cannot prove data integrity for 21 CFR Part 11 | Set `DATA_RETENTION_TIME_IN_DAYS = 90` (Enterprise) |
| Standard edition for HIPAA workloads | No BAA, no Tri-Secret Secure | Upgrade to Business Critical |

---

## Output

Deliver:
1. **Well-architected review** — scored against all 8 pillars (7 general + 1 HLS if applicable)
2. **Best practices checklist** — tailored to customer's chosen patterns and industry
3. **Anti-pattern audit** — flag any current practices that should be remediated
4. **Discovery checklist** — ensure all decisions are captured before implementation
5. **Compliance checklist** — HIPAA/21 CFR/GINA/GxP specific items if HLS customer
