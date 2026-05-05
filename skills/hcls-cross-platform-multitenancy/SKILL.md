---
name: hcls-cross-platform-multitenancy
description: "Multi-tenant data platform on Snowflake for health sciences and beyond: tenancy decisions, RBAC, data products, secure sharing, governance, AI infusion, cost attribution, BCDR, modernization assessment. Triggers: multi-tenant, multi-tenancy, tenancy, data product, SaaS platform, platform architecture, tenant isolation, shared database, dedicated database, row access policy, RLAP, OPT, MTT, hybrid tenancy, tenant onboarding, data residency, cross-tenant, tenant segregation, chargeback, cost attribution, BCDR, failover, data sharing, marketplace, reader account, AI governance, Cortex Guard, model access, platform modernization, footprint optimization, healthtech platform, health data platform, multi-tenant EHR, multi-tenant FHIR, health SaaS, clinical data platform, life sciences SaaS."
platform_affinities:
  produces: []
  benefits_from:
    - skill: data-governance
      when: "PHI/PII present — masking policies, tagging, and row-access policies are required for tenant data"
    - skill: cost-intelligence
      when: "chargeback or showback cost model is needed across tenants"
    - skill: security-investigation
      when: "tenant security incidents or anomalous access patterns need investigation"
    - skill: dynamic-tables
      when: "continuous ingestion pipelines are part of the data product design"
    - skill: network-security
      when: "network policies or private link are required for tenant isolation at the network layer"
---

# Multi-Tenant Data Products Platform — Orchestrator

## Purpose

This is the **root orchestrator skill** for building, operating, and modernizing multi-tenant data platforms on Snowflake. It is **industry-agnostic** — it works for healthtech, fintech, medtech, adtech, SaaS, or any customer building data products for multiple tenants.

The skill guides customers through every phase: greenfield platform builds, tenant onboarding acceleration, and brownfield modernization of existing Snowflake deployments.

---

## How This Skill Works

1. **Gather context** from the customer (intake questionnaire below)
2. **Route to the appropriate sub-skill(s)** based on what the customer needs
3. **Produce deliverables**: design blueprints, SQL scripts, RBAC hierarchies, cost models, and guidance documents — all parameterized with the customer's specific entities (databases, tables, schemas, roles, use cases)

---

## Step 1: Customer Intake Questionnaire

**MANDATORY**: Before routing to any sub-skill, collect the following from the customer. Use their exact entity names (databases, schemas, tables, roles) throughout all outputs.

### Greenfield (New Platform)
| Question | Why It Matters |
|----------|---------------|
| What industry/domain are you in? | Drives compliance and data sensitivity defaults |
| How many tenants do you expect (current and 12-month)? | Drives tenancy pattern recommendation |
| Describe your data products (tables, views, datasets) | Drives data product lifecycle design |
| Do tenants share the same schema or need full isolation? | Drives MTT vs OPT vs Hybrid decision |
| What compliance requirements apply (HIPAA, SOC2, PCI-DSS, GDPR, etc.)? | Drives governance and data residency |
| Do you need to share data across tenants or with external consumers? | Drives secure sharing strategy |
| Do you plan to use AI/ML features (Cortex, LLMs, embeddings)? | Drives AI governance sub-skill |
| What is your budget model (chargeback, showback, shared cost)? | Drives cost attribution design |
| Do you need BCDR (failover to another region)? | Drives BCDR sub-skill |

### Industry-Specific Deep-Dive Questions

**Ask these ONLY when the customer's industry is identified.** They unlock higher-quality, compliance-aware outputs.

#### Healthcare / Life Sciences
| Question | Why It Matters |
|----------|---------------|
| What clinical data standards do you use (FHIR R4, HL7v2, CDISC/SDTM, OMOP)? | Drives ingestion pipeline design and interoperability |
| What EHR/source systems feed your platform (Epic, Cerner, Meditech, custom)? | Drives connector and parsing choices |
| Do you have a patient consent/opt-out model? How is it enforced today? | Drives consent-aware RLAP design |
| What are your PHI de-identification requirements (Safe Harbor, Expert Determination)? | Drives masking and de-identification framework |
| Are you subject to 21 CFR Part 11 (clinical trials), GINA (genomics), or GxP (manufacturing)? | Drives compliance framework selection beyond HIPAA |
| Do you need IRB protocol-level access controls for research data? | Drives research-specific RBAC with entitlements per protocol |
| What data types require special handling (genomic variants, medical images, CDA/XML documents)? | Drives unstructured data governance and re-identification risk |

#### Financial Services
| Question | Why It Matters |
|----------|---------------|
| What regulatory frameworks apply (SOX, PCI-DSS, GLBA, Basel III)? | Drives compliance governance |
| Do you process cardholder data (PAN, CVV)? | Drives PCI-DSS scope and tokenization |
| What are your data retention requirements? | Drives Time Travel and retention policies |

#### General (All Industries)
| Question | Why It Matters |
|----------|---------------|
| What is your Snowflake edition (Standard, Enterprise, Business Critical)? | Gates which features can be recommended |
| Do you have an existing SIEM or security monitoring tool to integrate? | Drives audit log export strategy |
| What is your operational team size for platform management? | Drives automation vs manual admin recommendations |

### Brownfield (Existing Platform Modernization)
| Question | Why It Matters |
|----------|---------------|
| Describe your current Snowflake setup (databases, schemas, warehouses, roles) | Baseline for assessment |
| What are your current pain points (cost, performance, governance gaps)? | Prioritizes recommendations |
| How is tenant isolation currently implemented? | Identifies gaps and migration paths |
| What data products do you currently offer? | Identifies monetization opportunities |
| Are you using Snowflake Horizon features (tags, classification, lineage)? | Identifies governance maturity |

---

## Step 2: Route to Sub-Skills

Based on the intake, load one or more of these sub-skills:

| Sub-Skill | When to Load |
|-----------|-------------|
| **[tenancy-decision](./tenancy-decision/SKILL.md)** | Customer needs to choose MTT, OPT, or Hybrid tenancy pattern |
| **[rbac-tenancy](./rbac-tenancy/SKILL.md)** | Customer needs RBAC hierarchy, row access policies, database roles for multi-tenancy |
| **[data-products](./data-products/SKILL.md)** | Customer needs data product lifecycle: ingestion, transformation, curation |
| **[secure-sharing](./secure-sharing/SKILL.md)** | Customer needs secure sharing, Marketplace, reader accounts, data clean rooms, declarative sharing |
| **[governance-security](./governance-security/SKILL.md)** | Customer needs Snowflake Horizon governance: classification, masking, tagging, lineage, enterprise governance |
| **[ai-governance](./ai-governance/SKILL.md)** | Customer needs AI infusion with governance: Cortex Guard, AI Guardrails, model access controls, data residency |
| **[cortex-agents-multitenancy](./cortex-agents-multitenancy/SKILL.md)** | Customer needs Cortex Agents configured for multi-tenant access |
| **[cost-attribution](./cost-attribution/SKILL.md)** | Customer needs FinOps: cost attribution, chargeback, resource monitors, budgeting |
| **[bcdr-operations](./bcdr-operations/SKILL.md)** | Customer needs BCDR: replication, failover groups, client redirect, DR drills |
| **[modernization-assessment](./modernization-assessment/SKILL.md)** | Customer wants to review and optimize existing Snowflake footprint |
| **[implementation](./implementation/SKILL.md)** | Customer is ready to generate parameterized setup scripts |
| **[best-practices](./best-practices/SKILL.md)** | Customer needs well-architected framework guidance, design patterns, discovery |

---

## Step 3: Deliverables

Every engagement should produce a subset of these deliverables tailored to the customer:

1. **Architecture Blueprint** — Tenancy pattern, database layout, schema design, warehouse topology
2. **RBAC Hierarchy** — Role tree diagram, row access policies, masking policies, grants
3. **Data Product Catalog** — Ingestion pipelines, transformation layers, curated data products
4. **Secure Sharing Plan** — Shares, listings, reader accounts, data clean rooms
5. **Governance Framework** — Tags, classification profiles, masking policies, lineage setup
6. **AI Governance Plan** — Model access controls, Cortex Guard configuration, AI cost tracking
7. **Cost Attribution Model** — Tag-based cost tracking, resource monitors, chargeback views
8. **BCDR Runbook** — Replication groups, failover procedures, RTO/RPO targets
9. **Modernization Roadmap** — Gap analysis, optimization recommendations, revenue generation guidance
10. **Implementation Scripts** — Parameterized SQL ready to execute with customer's entity names

---

## Key Principles

- **Never hardcode industry-specific examples** — Always use the customer's actual entity names, tables, schemas, and use cases
- **Always ground guidance in Snowflake documentation** — Reference `snowflake_product_docs` for every recommendation
- **Always present designs for approval** before generating scripts
- **Use `{{VARIABLE}}` placeholders** in templates that the customer fills in
- **Consider all Snowflake editions** — Flag features that require Enterprise or Business Critical
