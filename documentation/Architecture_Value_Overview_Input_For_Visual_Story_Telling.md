# Industry Solutions Architect for Health Sciences
## Architecture & Value Overview

---

> **Document purpose and audience**
> This document is written for healthcare data leaders, enterprise architects, Snowflake field engineers, and executive stakeholders seeking to understand the value and architectural design of the Industry Solutions Architect for Health Sciences framework. It is a stable reference — covering the framework's business rationale, design philosophy, and architectural model. It is **not** a skills inventory or a technical configuration guide. This document should only be updated when the framework's architecture or design principles change, not when individual skills are added, updated, or removed.
>
> **For Google NotebookLM users:** Upload this file as a source to generate briefing documents, audio overviews, or presentation outlines. Key concepts are bolded throughout to aid concept indexing.
>
> **Source files this document is grounded on:** [`README.md`](../README.md), [`agents/health-sciences-incubator.md`](../agents/health-sciences-incubator.md), [`templates/skills_incubator.yaml`](../templates/skills_incubator.yaml)

---

## 1. What Is This Framework?

The **Industry Solutions Architect for Health Sciences** is a composable skill-based **framework** built on [Snowflake Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code) — Snowflake's AI coding assistant — that enables end-to-end healthcare and life sciences data and AI pipelines to be delivered from a single natural language conversation.

Instead of writing boilerplate pipelines from scratch, a user describes the business problem. An orchestrator agent understands the healthcare domain, selects the right combination of industry skills and Snowflake platform capabilities, and composes them into a working solution — spanning data ingestion, interoperability, analytics, AI, governance, and applications. 

> **Example:** A user asks: *"Design a Phase III clinical trial for a novel GLP-1 receptor agonist targeting Type 2 Diabetes with cardiovascular outcome endpoints."* The orchestrator automatically chains together research problem validation → competitor trial search (ClinicalTrials.gov) → literature review (PubMed) → protocol generation → survival endpoint design → claims-based feasibility analysis. No skill names are required. No pipeline code is written from scratch.

### What Industry Skills Add to Cortex Code

**Cortex Code (CoCo)** is Snowflake's AI coding assistant. It is general-purpose by design — capable of writing code, building data pipelines, and creating AI applications across any domain. General-purpose also means context-free: without domain expertise loaded at runtime, CoCo approaches every problem the same way. It can write a FHIR ingestion pipeline — but without the FHIR R4 resource model, the OMOP vocabulary mapping requirements, the HIPAA masking obligations, or the data quality checks that make the result clinically valid, the output is technically functional but clinically incomplete.

**Skills** are the mechanism that closes this gap. A skill is a loadable module that injects expertise — domain workflows, guardrails, best practices, regulatory constraints — into CoCo at runtime. Snowflake ships Cortex Code **pre-bundled with Platform Skills** — modules that encode Snowflake feature expertise out of the box: how to build an incremental pipeline with Dynamic Tables, how to configure Cortex Search, how to apply tag-based masking policies. Platform Skills are what make CoCo produce best-practice Snowflake implementations, not just working ones.

**Industry Skills apply the same mechanism to domain knowledge.** Where a Platform Skill encodes how to use a Snowflake feature correctly, an Industry Skill encodes how to act on a specific healthcare or life sciences problem correctly — bringing in the clinical data models, regulatory constraints, vocabulary systems, and governance requirements that turn technically functional output into clinically valid output.

The orchestrator's **Skill-First Rule** captures this precisely: *"Skills encode domain expertise, gated workflows, guardrails, and best practices that raw tool usage does not."* Both skill types load through the same mechanism — the architecture makes no distinction. The distinction is in what expertise is encoded: Snowflake feature knowledge versus healthcare domain knowledge.

The **Industry Solutions Architect for Health Sciences** is the framework that enables this capability. It provides the orchestrator design, skill patterns, governance model, and reference implementations that give CoCo the structure to act as a domain-expert partner on healthcare and life sciences problems. It is **not an exhaustive catalog** of every possible HCLS skill — it is an extensible architecture. Snowflake partners and customers can adopt this framework to develop and contribute their own domain-focused skills, accelerating time to outcome in their specific area of healthcare or life sciences without rebuilding the underlying architecture from scratch.

---

## 2. What Was Missing Before

The README's "What makes this different" section describes the framework by explicit contrast with the prior state. Each point names what the framework is; the contrast reveals what was missing:

**"Composable skills, not monolithic scripts."** Delivering healthcare and life sciences solutions has historically meant writing monolithic, custom-built scripts for each problem — tightly coupled, difficult to reuse, impossible to extend without rewriting. Each skill in this framework encodes one unit of domain expertise (DICOM imaging, FHIR interoperability, FAERS pharmacovigilance, genomics pipelines, claims analytics) as a reusable, independently loadable building block.

**"Instead of writing boilerplate pipelines from scratch."** Teams starting a new healthcare or life sciences workflow face the same overhead every time: staging configuration, schema creation, transformation logic, search service setup, governance wiring. This framework compresses that overhead by encoding the infrastructure as skills the orchestrator assembles on demand.

**"Governance by default. HIPAA guardrails — PHI masking, row-access policies, audit trails, de-identification — are enforced as cross-cutting concerns across every workflow."** The prior paradigm treats HIPAA compliance as something layered on at the end of a project. This framework builds it into the architecture as a default cross-cutting concern, enforced at the skill level, the schema level, and the platform level simultaneously.

**"Scales like lego blocks. New skills snap into the framework as independent building blocks. Router skills cluster related capabilities under a business function. Adding a new domain or business function is just adding another skill directory — the orchestrator picks it up automatically."** Existing approaches require central coordination to extend — every new capability means modifying shared infrastructure. The framework's extensibility is structural: a new skill is a new directory, and the system detects it without any changes to existing components.

---

## 3. Architectural Philosophy

The README describes the framework through five design properties that, taken together, constitute its architectural philosophy:

**Composable over monolithic.** Skills are independently loadable units of expertise. They do not depend on each other directly. The orchestrator composes them at plan-build time. This means any skill can be updated, replaced, or extended without touching any other part of the system.

**Outcome-first.** Users describe business outcomes in natural language. The routing and composition logic lives inside the orchestrator and the skills — not in the user's prompt. The orchestrator detects the domain from trigger keywords and context, selects and sequences the appropriate skills, and delivers a plan before executing.

**Knowledge-grounded.** Cortex Knowledge Extensions (CKEs) provide on-demand RAG search over external knowledge sources. A Data Model Knowledge repository grounds schema generation in live reference models via Cortex Search. Knowledge is queried at runtime — not hardcoded in skill instructions.

**Governance by default.** HIPAA guardrails are enforced as cross-cutting concerns across every workflow. They are not added on at the end; they are built into the architecture as a mandatory concern at the skill level, the schema level, and the Snowflake platform level.

**Open by design.** New skills snap into the framework as independent building blocks. Adding a new domain or business function is just adding another skill directory — the orchestrator picks it up automatically. There is no central gating on contribution; the framework is designed for continuous field contribution.

---

## 4. The Four-Layer Architecture

The framework is organized into four distinct layers. The README's architecture diagram defines these layers explicitly:

```
┌────────────────────────────────────────────────────────────────┐
│  LAYER 1: ORCHESTRATOR AGENT                                   │
│  Intent Detection → Domain Routing → Skill Composition         │
│  Plan-then-Execute Gate → Cross-cutting Governance             │
└────────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌────────────────────────────────────────────────────────────────┐
│  LAYER 2: INDUSTRY DOMAIN SKILLS                               │
│  Composable units of domain expertise, organized by            │
│  sub-industry (Provider, Pharma, Payer) and business function  │
│  Router Skills  |  Standalone Skills  |  CKE Skills            │
└────────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌────────────────────────────────────────────────────────────────┐
│  LAYER 3: KNOWLEDGE CONTEXT                                    │
│  CKE: On-demand literature and trial evidence (Cortex Search)  │
│  DMK: Live schema metadata grounding (Cortex Search)           │
└────────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌────────────────────────────────────────────────────────────────┐
│  LAYER 4: SNOWFLAKE PLATFORM SKILLS                            │
│  Sequenced into plans based on each domain skill's declared    │
│  platform affinities                                           │
│  Dynamic Tables | Cortex AI | Streamlit | SPCS | ML Registry  │
│  Cortex Search | Cortex Agent | Data Governance | Semantic View│
└────────────────────────────────────────────────────────────────┘
```

**Layer 1 — Orchestrator Agent.** The single entry point for every request. It classifies the user's intent by sub-industry (Provider, Pharma, Payer) and task type, routes to matching domain skills, evaluates each skill's **platform affinities** to determine which platform capabilities belong in the plan, and assembles a structured execution plan. The orchestrator follows a mandatory **Plan-then-Execute** protocol: the complete plan is presented to the user for approval before any skill is invoked or any data operation begins.

**Layer 2 — Industry Domain Skills.** The core intellectual property of the framework. Each skill encodes the domain expertise for one healthcare use case: the right data models, the right clinical standards, the right vocabulary systems, and the right governance considerations for that specific problem. Skills are organized by sub-industry and business function in a five-level taxonomy (Industry → Sub-Industry → Business Function → Use Case Skill → Sub-Skill), encoded in a flat directory structure using a structured naming prefix.

**Layer 3 — Knowledge Context.** Makes skills adaptive rather than static. Instead of hardcoding schema definitions, document type specifications, or external evidence inside skill instructions, skills query live knowledge sources at runtime via Cortex Search. Two patterns: **CKE** for external literature and trial evidence, and **DMK** for live schema metadata. Both are queried at runtime — knowledge is never compile-time.

**Layer 4 — Snowflake Platform Skills.** The bundled capabilities of the Snowflake platform itself, sequenced into plans intelligently. As the README describes: *"Platform skills are added based on each skill's declared platform affinities (e.g., `data-governance` when PHI is present, `dynamic-tables` for ongoing feeds)."* These are not custom-built components — they are Snowflake's production platform capabilities, composed into plans automatically based on what each domain skill declares it benefits from.

---

## 5. The Orchestrator: Plan-then-Execute Protocol

The orchestrator is designed around a two-phase protocol that the `agents/health-sciences-incubator.md` describes as *"MANDATORY"* and *"non-negotiable"*: **Phase 1 (Plan) MUST complete before Phase 2 (Execute) can begin.**

### Phase 1: Plan (Mandatory Gate)

The orchestrator builds a complete solution plan before invoking any skill:

1. **Identify the sub-industry** (Provider, Pharma, Payer) from routing rules. When the customer straddles sub-industries, route by the **task being performed**, not the customer type.
2. **Scan the Skill Routing Tables** for trigger keyword matches against the user's request.
3. **Check Cross-Domain Patterns** — if the request spans multiple business functions, identify the matching pattern and adapt it.
4. **Evaluate platform affinities** — for each domain skill in the plan, read its declared `produces` and `benefits_from` properties to determine which platform skills should be added as follow-on steps.
5. **Build a solution plan** using a structured table: one row per skill invocation, with explicit dependencies between steps and governance flags identifying which steps create or expose PHI.
6. **Present the plan to the user** for explicit approval before any execution begins.

The plan table format (from `agents/health-sciences-incubator.md`):

```
| Step | Skill                  | What it produces               | Depends on | Governance  |
|------|------------------------|-------------------------------|------------|-------------|
|  1   | $hcls-provider-cdata-fhir | Relational tables from FHIR bundles | —      | PHI present |
|  2   | $hcls-provider-cdata-omop | OMOP CDM v5.4 tables          | Step 1     | —           |
|  3   | data-governance        | Masking + row-access policies  | Steps 1–2  | HIPAA       |
|  4   | semantic-view          | Semantic views for analytics   | Steps 1–2  | —           |
```

The plan operates at the **skill level**, not the SQL level. One row equals one skill invocation. The SQL-level implementation detail happens inside the skill during Phase 2 and is not exposed in the plan.

The **Plan Gate** is the primary mechanism for human oversight. The orchestrator explicitly states: *"Do NOT proceed to Phase 2 until the user confirms."* For any task that loads, creates, or modifies data, or that involves data acquisition, or that composes multiple skills — the full plan gate is mandatory. There is no bypass.

### Phase 2: Execute (Only After Approval)

Once the plan is approved, the orchestrator invokes skills in the approved order. This is the first point in the conversation where the `skill` tool is called — the orchestrator's design explicitly prohibits calling skills during Phase 1 planning, so that skills do not pull focus away from plan construction. Governance guardrails are applied as a cross-cutting concern on every step. CKE enrichment runs on-demand when evidence grounding is called for. Each major step reports back before the next begins.

---

## 6. Skill Design Principles

### 6.1 Naming Convention as Taxonomy

The README defines the naming convention as encoding the full five-level taxonomy in a flat prefix structure:

```
hcls-{sub-industry}-{function}-{skill}
```

| Component | Values | Description |
|-----------|--------|-------------|
| `hcls` | Fixed prefix | Health Sciences industry identifier |
| sub-industry | `provider`, `pharma`, `payer`, `cross` | Customer segment |
| function | `imaging`, `cdata`, `dsafety`, `genomics`, `lab`, `claims`, `research`, `cke` | Business function |
| skill | `fhir`, `pharmacovigilance`, `nextflow`, etc. | Use case skill name |

*"Cortex Code requires skills at exactly 1 level of nesting from the scan root. The five-level taxonomy is encoded in the name prefix, not directory depth."* — README

### 6.2 Three Skill Types

The README defines three skill types:

| Type | Description |
|------|-------------|
| **Router skill** | Detects user intent and routes to specialized sub-skills. Contains setup, preflight checks, and workflow orchestration. |
| **Sub-skill** | Handles one specific task within a router. Loaded by the router, not directly by the user. |
| **Standalone skill** | Self-contained skill with no router or sub-skills. |

**CKE skills** are a distinct category of standalone skills whose purpose is evidence grounding rather than pipeline construction. They provide on-demand access to external knowledge sources via Cortex Search and are invoked by domain skills when evidence adds value.

### 6.3 The Router Pattern

The README describes the router pattern shared by router skills:

1. **Preflight Check** — probe the CKE search service at skill load (READY / MISSING)
2. **Intent Detection** — classify what the user wants (parse, ingest, search, agent, etc.)
3. **Conditional Step 0** — if CKE is READY, query it for schema/spec context before routing
4. **Sub-skill Loading** — pass grounding context to the sub-skill
5. **Fallback** — if CKE is MISSING, sub-skills use local reference files

The router owns preflight and intent detection. Sub-skills own execution. This separation means adding a new capability within a domain is adding a new sub-skill, not changing the router.

### 6.4 Platform Affinities

The orchestrator agent describes how platform skills are composed into plans: *"Each industry skill declares `produces` — what Snowflake objects it creates — and `benefits_from` — which platform skills enhance it and under what conditions. For each domain skill in the plan, evaluate each `benefits_from` entry against the user's request; if the `when` condition matches, add that platform skill as a follow-on step."*

This means platform routing logic is never hardcoded in the orchestrator — skills carry their own context about what platform capabilities they need, and the orchestrator reads those declarations at plan-build time.

### 6.5 Extensibility Without Modification

From the README: *"Adding a new domain or business function is just adding another skill directory — the orchestrator picks it up automatically."* The contributing workflow is: create the skill directory, write `SKILL.md` with proper frontmatter, register the skill in the metadata registry (`templates/skills_incubator.yaml`), regenerate the orchestrator routing tables, and raise a pull request. No existing skill needs to be modified.

---

## 7. The Knowledge Layer

### 7.1 Cortex Knowledge Extensions (CKEs)

From the README: *"Several skills use CKE — dynamically discoverable metadata served via Cortex Search Services. Instead of hardcoding table schemas or document type definitions, skills query a search service at runtime to get the latest metadata."*

The architecture:

```
Source of Truth (YAML / Excel)
    │
    └──> Backing Table (in DATA_MODEL_KNOWLEDGE schema)
            │
            └──> Cortex Search Service (CKE)
                    │
                    └──> Skills query at runtime via SEARCH_PREVIEW()
```

Consequences the README states explicitly:
- **Schema changes propagate automatically** — edit the source, refresh the table, the search service updates within its `TARGET_LAG`
- **Skills are never stale** — they always query the latest metadata
- **Fallback is built in** — if the search service is down, skills fall back to local files on disk

The orchestrator agent adds the preflight pattern: *"Before invoking any CKE, the skill runs a probe query to verify the Marketplace listing is installed. If MISSING, the skill skips CKE enrichment gracefully and continues with its primary task."*

### 7.2 Data Model Knowledge (DMK)

DMK is the CKE pattern applied to schema metadata. Skills that depend on specific schema structures — table definitions, column types, PHI flags, domain-specific mappings — query a live reference model at runtime via a Cortex Search Service rather than having those definitions hardcoded in skill instructions.

This is how **PHI awareness** operates at the schema level. PHI column status is cataloged in the schema metadata as part of the data model curation process. Skills query this inventory during preflight — before any data operation — to determine whether PHI protection measures must be activated. The README describes this as auto-firing: *"For schema-dependent tasks, Data Model Knowledge auto-fires to ground outputs in live reference models."* This is not a runtime content scanner; it is a pre-cataloged, metadata-driven inventory.

---

## 8. Governance as Infrastructure

The orchestrator agent defines HIPAA compliance as an architectural property applied at multiple layers simultaneously, with a clear principle: governance must be enforced at the layer closest to the data.

### 8.1 At the Skill Level: Mandatory Confirmation Gates

Skills operating near protected health information contain explicit **MANDATORY STOP** instructions — LLM prompt-level directives that require user confirmation before any operation on sensitive data proceeds. The orchestrator agent documents the anti-pattern explicitly: *"Never store or display PHI without masking policies in place."* These gates are surfaced as governance flags in the plan table during Phase 1 — the user sees them before approving the plan.

### 8.2 At the Schema Level: Pre-Cataloged PHI Inventory

PHI columns are flagged in the Data Model Knowledge repository and queried by skills during preflight. The governance layer the README documents for clinical NLP describes *"7-layer governance: tags, masking, row-access, roles, AI guardrails, ML views, audit"* — applied as a distinct plan step, not woven implicitly into the data pipeline.

### 8.3 At the Platform Level: Snowflake-Native Controls

The README describes the Defense-in-Depth architecture for clinical documents — a three-layer system the orchestrator agent documents as the model for production-grade PHI handling:

| Layer | Mechanism | Purpose |
|-------|-----------|---------|
| 1 | `AGENTS.md` (profile-level rules) | Session-wide constraints |
| 2 | Gate micro-skills + Phase skills | Structural decomposition — model cannot skip steps |
| 3 | `hooks.json` | Hard blocks on DDL/DML without user confirmation |

The orchestrator agent specifies: *"Always use IS_ROLE_IN_SESSION() (not CURRENT_ROLE()) in masking/row-access policies"* — a concrete platform-level guardrail applied across all PHI-adjacent workflows, enforced at the Snowflake engine level regardless of what invokes the query.

---

## 9. Composition: How Skills Become Solutions

The README defines composition as the mechanism that transforms individual skills into complete solutions: *"The orchestrator agent detects intent, routes across healthcare business domains, and chains multiple skills together with Snowflake platform skills to deliver complete solutions."*

### 9.1 Cross-Domain Composition Patterns

The README and `templates/skills_incubator.yaml` define a library of named composition patterns — reusable blueprints for workflows that span multiple business functions:

| Pattern | Description |
|---------|-------------|
| Imaging + Clinical Integration | DICOM parse → FHIR ingest → Clinical NLP → Literature enrichment → UI |
| Clinical Data Warehouse | FHIR → OMOP CDM → Governance → Semantic views |
| Drug Safety Signal Detection | FAERS analysis → Literature search → Clinical NLP → Claims correlation |
| Genomics + Clinical Outcomes | nf-core pipeline → Variant annotation → Survival analysis → ML models |
| Real-World Evidence Study | Claims cohort → Trial registry search → OMOP → Survival → Literature validation |
| Clinical Trial Design | Problem validation → Trial search → Literature review → Protocol → Power analysis |
| Lab Data Modernization | Instrument standardization → Incremental pipeline → Analytics dashboard |
| Clinical Document Intelligence | Document extraction → Search → Agent → Governance |

The orchestrator agent describes patterns as *"guides, not rigid scripts"*: steps can be skipped when they don't apply, reordered when the user already has intermediate outputs, combined when a request spans multiple patterns, or extended when additional capabilities are needed.

### 9.2 Skills Do Not Know About Each Other

A direct consequence of the composable architecture: domain skills are mutually independent. Composition is entirely the orchestrator's responsibility. The README states: *"If multiple skills match, the plan should invoke the most specific one first."* Skills never reference each other directly — the orchestrator reads routing tables and pattern definitions, not inter-skill dependencies. This independence is what makes the framework extensible: a new skill cannot break existing skill combinations.

---

## 10. The Skill Lifecycle: From Contribution to Production

The README defines a two-repo model with five phases:

```
Snowflake-Solutions/health-sciences-coco-skills-incubator    ← Incubator (Phase 0–1)
        │
        │  Skills mature here, then graduate ↓
        │
Snowflake-Solutions/cortex-code-skills                       ← SFS production repo (Phase 2+)
```

| Phase | Repo | Who | What |
|-------|------|-----|------|
| **Phase 0: Setup** | Incubator | Tiger Team | Create repo, guidelines, profile, orchestrator |
| **Phase 1: Incubate** | Incubator | Anyone (SEs, SAs, field) | Branch from main, develop skills, raise PR |
| **Phase 2: Harden** | SFS cortex-code-skills | Tiger Team only | Audit, test, promote: draft → review → staging → production |
| **Phase 3: Publish** | Snowflake registry | Tiger Team | `cortex profile publish` production profile for field teams |
| **Phase 4: Consume** | Field environments | Field teams | `cortex profile add health-sciences-solutions -c <connection>` |

**Key design properties from the README:**
- `main` is always the latest curated state — field teams always point to `main`
- No milestone tags or version numbers — PRs gate what gets into `main`
- Contributors never push directly to `main`

All changes follow a **branch-driven workflow**: contributors branch from `main`, develop and test, then raise a pull request. The tiger team reviews and merges. This separates the velocity of contribution (low barrier, branch from anywhere) from the rigor of production deployment (tiger team review required).

---

## 11. Scope and Direction

The `templates/skills_incubator.yaml` profile description defines the framework's intended scope:

> *"Industry Solutions Architect for Health Sciences on Snowflake. Brings together composable industry skills to solve healthcare and life sciences problems end-to-end — from data ingestion and interoperability through analytics, AI, governance, and applications. Covers medical imaging, clinical data management, drug safety, real-world evidence, genomics, and lab operations."*

The README's contributing section states the architectural mechanism for growth: *"New skills snap into the framework as independent building blocks... Adding a new domain or business function is just adding another skill directory — the orchestrator picks it up automatically."* This means the framework's scope expands through contribution, not through redesign.

The incubator is the contribution surface. The production profile is the delivery surface. The orchestrator is the composition engine. These three roles stay constant as the skill library grows.

---

## 12. Frequently Asked Questions

**Q: Does this framework require writing any code?**
A: For most healthcare and life sciences workflows, no custom code is required. The framework composes working pipelines through natural language conversation. Some specialized skills generate code that runs in a local Python environment, but the user does not write that code from scratch.

**Q: Do I need to know which skills exist to use the framework?**
A: No. The orchestrator handles routing automatically from a natural language description of the business problem. The README documents in-session discovery: *"ask the orchestrator to 'add a new skill' and it walks through these steps automatically"* — the same pattern applies to capability discovery.

**Q: What happens if my data contains PHI?**
A: PHI columns are pre-cataloged in the Data Model Knowledge repository. When a plan step involves PHI-flagged tables, the plan automatically includes governance steps and surfaces **MANDATORY STOP** confirmation gates that require explicit user approval before any data operation proceeds. Platform-level masking and row-access policies are enforced at the Snowflake engine level regardless.

**Q: Can our team contribute a new skill?**
A: Yes. The README describes the contribution path: branch from `main`, create a skill directory following the `hcls-{sub}-{func}-{skill}` naming convention, write `SKILL.md`, register in `templates/skills_incubator.yaml`, regenerate the orchestrator, and open a pull request targeting `main`. The framework includes a guided skill development workflow.

**Q: How is this different from a general-purpose AI coding assistant?**
A: The README frames the answer: *"Each skill encodes deep domain expertise (DICOM imaging, FHIR interoperability, FAERS pharmacovigilance, genomics pipelines, claims analytics) as a reusable building block."* A general-purpose assistant has broad capability but none of that encoded domain knowledge — it cannot assemble a compliant imaging pipeline or pharmacovigilance workflow without extensive per-session prompting.

**Q: How does the framework stay current as healthcare standards evolve?**
A: The README states: *"Schema changes propagate automatically — edit the source, refresh the table, the search service updates within its TARGET_LAG. Skills are never stale — they always query the latest metadata."* New standards become new or updated skills in the incubator; they do not require core framework changes.

**Q: When should this document be updated?**
A: Only when the **architecture or design principles** of the framework change — for example, if a new architectural layer is introduced, if the Plan-then-Execute protocol is redesigned, if the governance model changes, or if a new fundamental skill type is introduced. It should **not** be updated when individual skills are added, removed, or updated.

---

*Grounded on: [`README.md`](../README.md) · [`agents/health-sciences-incubator.md`](../agents/health-sciences-incubator.md) · [`templates/skills_incubator.yaml`](../templates/skills_incubator.yaml)*
