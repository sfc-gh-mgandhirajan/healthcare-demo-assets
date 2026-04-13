# HCLS Industry Skill Best Practices

Best practices for building **Health & Life Sciences (HCLS) industry skills** for Cortex Code, combining platform skill patterns (from `SKILL_BEST_PRACTICES.md` in `cortex-code-skills`) with industry-specific patterns learned across the HCLS skills portfolio.

This guide is intended for **all HCLS skill developers** — whether building Provider, Pharma, or Cross-Industry skills.

---

## Table of Contents

1. [How Industry Skills Differ from Platform Skills](#1-how-industry-skills-differ-from-platform-skills)
2. [HCLS Skill Portfolio Overview](#2-hcls-skill-portfolio-overview)
3. [Naming and Organization](#3-naming-and-organization)
4. [Skill Archetypes](#4-skill-archetypes)
5. [Architecture Patterns](#5-architecture-patterns)
6. [Domain Knowledge Management](#6-domain-knowledge-management)
7. [Platform Affinity Design](#7-platform-affinity-design)
8. [Governance Patterns](#8-governance-patterns)
9. [Testing HCLS Skills](#9-testing-hcls-skills)
10. [Portable Seed and Reference Data](#10-portable-seed-and-reference-data)
11. [Cortex Knowledge Extensions (CKEs)](#11-cortex-knowledge-extensions-ckes)
12. [Industry Skill Development Lifecycle](#12-industry-skill-development-lifecycle)
13. [Orchestrator Integration](#13-orchestrator-integration)
14. [Common Pitfalls](#14-common-pitfalls)
15. [Quick Reference Checklist](#15-quick-reference-checklist)

---

## 1. How Industry Skills Differ from Platform Skills

Platform skills teach Cortex Code *how to use a Snowflake feature* (Dynamic Tables, Cortex AI, dbt). Industry skills teach Cortex Code *how to solve a domain problem* using multiple platform features together.

| Dimension | Platform Skill | HCLS Industry Skill |
|-----------|---------------|---------------------|
| Scope | Single Snowflake capability | End-to-end domain workflow |
| Sub-skills | 2-5 (create/debug/optimize) | 1-15+ (one per domain concept + cross-cutting) |
| Naming | Verb-prefix (`create-X`, `manage-X`) | Taxonomic (`hcls-{segment}-{domain}-{capability}`) |
| Domain knowledge | Snowflake docs | Clinical standards, regulatory requirements, domain ontologies |
| Data model | User-defined | Prescriptive or standards-aligned (FHIR, OMOP, DICOM, FAERS) |
| Governance | Optional add-on | Often required (PHI/HIPAA, patient data, adverse events) |
| Platform affinities | Self-contained | Depends on 2-7 platform skills |
| Testing | Unit tests | Domain-specific validation (clinical accuracy, standards compliance, scale) |

**Key insight**: Industry skills are *domain orchestrators* — they coordinate multiple platform skills toward a domain outcome and carry knowledge that Cortex Code cannot infer from general training.

---

## 2. HCLS Skill Portfolio Overview

The portfolio spans five segments with distinct domain patterns. Three segments have skills today; two are planned:

### Provider (Clinical Data & Imaging)

| Skill | Archetype | Domain |
|-------|-----------|--------|
| `hcls-provider-imaging` | Router + 7 sub-skills | DICOM parsing, ingestion, analytics, viewer, governance, ML |
| `hcls-provider-cdata-fhir` | Standalone | FHIR R4 bundle → relational tables |
| `hcls-provider-cdata-clinical-nlp` | Router + 15 sub-skills | Clinical note NLP: extraction, normalization, governance, pipeline |
| `hcls-provider-cdata-clinical-docs` | Standalone | Clinical document intelligence (classification, extraction, search) |
| `hcls-provider-cdata-omop` | Standalone | OMOP CDM v5.4 transformation |
| `hcls-provider-claims-data-analysis` | Standalone | Claims analytics, RWE, utilization metrics |

### Pharma (Drug Safety, Genomics, Lab)

| Skill | Archetype | Domain |
|-------|-----------|--------|
| `hcls-pharma-dsafety-pharmacovigilance` | Standalone | FAERS signal detection, disproportionality metrics |
| `hcls-pharma-dsafety-clinical-trial-protocol` | Standalone | Trial protocol generation |
| `hcls-pharma-genomics-nextflow` | Standalone | nf-core pipeline deployment (rnaseq, sarek, atacseq) |
| `hcls-pharma-genomics-variant-annotation` | Standalone | VCF annotation with ClinVar, gnomAD |
| `hcls-pharma-genomics-single-cell-qc` | Standalone | scverse/scanpy QC with MAD-based filtering |
| `hcls-pharma-genomics-scvi-tools` | Standalone | Deep learning single-cell analysis |
| `hcls-pharma-genomics-survival-analysis` | Standalone | Kaplan-Meier, Cox regression, time-to-event |
| `hcls-pharma-lab-allotrope` | Standalone | Instrument data → Allotrope ASM JSON |

### Payer (Claims, Risk, Quality — Planned)

No dedicated payer skills yet. Use `hcls-provider-claims-data-analysis` for claims analytics and `hcls-pharma-genomics-survival-analysis` for outcomes analysis. Future skills will use the `hcls-payer-*` prefix.

| Potential Skill | Archetype | Domain |
|----------------|-----------|--------|
| `hcls-payer-claims-adjudication` | Standalone | Claims adjudication, denial management, payment integrity |
| `hcls-payer-risk-stratification` | Standalone | Risk adjustment (HCC), member stratification, predictive modeling |
| `hcls-payer-quality-hedis` | Standalone | HEDIS measure calculation, Stars ratings, quality reporting |
| `hcls-payer-utilization-management` | Standalone | Prior authorization, utilization review, care management |

### Med Device (Regulatory, Post-Market, Engineering — Planned)

No dedicated med device skills yet. Use `hcls-pharma-dsafety-clinical-trial-protocol` for device trial protocols and `hcls-provider-imaging` for device imaging data. Future skills will use the `hcls-meddev-*` prefix.

| Potential Skill | Archetype | Domain |
|----------------|-----------|--------|
| `hcls-meddev-regulatory-510k` | Standalone | 510(k) submission preparation, predicate device search, substantial equivalence |
| `hcls-meddev-postmarket-mdr` | Standalone | MDR/MAUDE adverse event analysis, post-market surveillance, UDI tracking |
| `hcls-meddev-engineering-dhf` | Standalone | Design History File management, V&V traceability, design controls |
| `hcls-meddev-regulatory-udi` | Standalone | UDI (Unique Device Identification) registry, GUDID integration |

### Cross-Industry

| Skill | Archetype | Domain |
|-------|-----------|--------|
| `hcls-cross-research-problem-selection` | Standalone | Scientific project ideation and strategy |
| `hcls-cross-skill-development` | Standalone | HCLS skill scaffolding and registration |
| `hcls-cross-cke-pubmed` | CKE | PubMed literature search via Marketplace |
| `hcls-cross-cke-clinical-trials` | CKE | ClinicalTrials.gov search via Marketplace |

---

## 3. Naming and Organization

### Taxonomic Naming Convention

HCLS skills use **taxonomic naming** (not verb-prefix):

```
hcls-{segment}-{domain}-{capability}
```

| Component | Values | Examples |
|-----------|--------|---------|
| `hcls` | Always `hcls` | Fixed prefix |
| segment | `provider`, `pharma`, `payer`, `meddev`, `cross` | Sub-industry |
| domain | `cdata`, `imaging`, `genomics`, `dsafety`, `lab`, `cke`, `claims`, `risk`, `quality`, `regulatory`, `postmarket`, `engineering` | Functional area |
| capability | `clinical-nlp`, `fhir`, `nextflow`, `pubmed` | Specific skill |

**Why taxonomic?** Industry skills are *portfolios*, not single actions. Taxonomy enables:
- Segment-based routing (all `hcls-provider-*` skills share Provider context)
- Portfolio management (which capabilities exist per segment)
- Orchestrator routing (segment → domain → skill)

**Sub-skills** within a router CAN use noun-prefix or function-prefix naming (`extraction-*`, `normalization-*`, `dicom-parser`, `imaging-governance`) since they describe what the sub-skill does within its parent context.

### File Organization

**Standalone skill (most skills)**:
```
skills/{hcls-skill-name}/
├── SKILL.md              # Workflow (setup → steps → output)
├── skill_evidence.yaml   # Promotion lifecycle
├── scripts/              # Python/SQL helper scripts
└── references/           # Domain reference docs
```

**Router skill (imaging, clinical-nlp)**:
```
skills/{hcls-skill-name}/
├── SKILL.md                    # Router (intent detection + routing + preflight)
├── skill_evidence.yaml         # Promotion lifecycle
├── {domain-doc}.pdf            # Domain taxonomy documents
├── data-model-knowledge/       # Schema reference sub-skill
│   ├── SKILL.md
│   ├── references/
│   ├── scripts/
│   └── seed-data/
├── {sub-skill-a}/SKILL.md     # Functional sub-skills
├── {sub-skill-b}/SKILL.md
├── governance/SKILL.md         # Governance sub-skill (if applicable)
└── pipeline-implementation/SKILL.md  # Pipeline sub-skill (if applicable)
```

**CKE skill**:
```
skills/{hcls-cke-name}/
├── SKILL.md              # Preflight + query patterns + fallback
└── skill_evidence.yaml
```

### Flat Discovery Requirement

All skills live at `skills/{hcls-name}/SKILL.md` (one level from scan root). Cortex Code discovers skills by scanning `skills/*/SKILL.md`. Sub-skills are nested inside their parent: `skills/{parent}/{sub-skill}/SKILL.md`.

---

## 4. Skill Archetypes

Different HCLS skills follow different structural patterns. Choose the archetype that fits your domain complexity.

### Archetype A: Standalone Skill

**When**: Single coherent workflow, no need to branch into distinct sub-workflows.

**Examples**: FHIR transformation, pharmacovigilance, variant annotation, survival analysis, Allotrope conversion, OMOP transformation, single-cell QC

**Structure**: One SKILL.md with linear or branching workflow steps. May include scripts and references.

**Characteristics**:
- Under 500 lines (ideally)
- Self-contained workflow
- Platform affinities declared in frontmatter
- May use `scripts/` for Python helpers

### Archetype B: Router + Sub-Skills

**When**: Domain has multiple distinct workflows that share a common data model or knowledge base.

**Examples**: Imaging (7 sub-skills: parse, ingest, analytics, viewer, governance, ML, data-model-knowledge), Clinical NLP (15 sub-skills: 6 extraction, 6 normalization, governance, pipeline, data-model-knowledge)

**Structure**: Router SKILL.md with intent table dispatching to sub-skills. Shared pre-step (data model knowledge query) enriches all sub-skills.

**Characteristics**:
- Router handles intent detection and preflight checks
- Each sub-skill is a standalone workflow within its parent context
- Sub-skills declare `parent_skill` in frontmatter
- Shared cross-cutting concerns (data model, governance) are sub-skills too
- Intent table maps trigger keywords → sub-skill path

### Archetype C: CKE (Cortex Knowledge Extension)

**When**: Skill wraps a Marketplace-shared Cortex Search Service for domain knowledge retrieval.

**Examples**: PubMed CKE, ClinicalTrials.gov CKE

**Structure**: Preflight check → query patterns → fallback behavior.

**Characteristics**:
- Always starts with preflight probe to detect Marketplace listing
- READY/MISSING/ERROR branching
- Never fails parent skills — graceful degradation
- Query patterns with parameterized search
- Other skills invoke CKEs as enrichment (not direct user invocation)

### Archetype D: Compute-Heavy Pipeline

**When**: Skill orchestrates external compute (containers, bioinformatics pipelines, ML training).

**Examples**: Nextflow nf-core pipelines, scvi-tools deep learning, imaging ML

**Structure**: Environment check → data acquisition → pipeline configuration → execution → output verification.

**Characteristics**:
- External dependencies (Docker, Nextflow, GPU compute pools)
- Environment verification as mandatory first step
- Test profile run before production execution
- Samplesheet/config generation as intermediate step
- Output validation against expected artifacts

---

## 5. Architecture Patterns

### Pattern 1: Router-First with Conditional Pre-Step

For router skills (Archetypes B), use intent detection → conditional pre-step → sub-skill dispatch:

```
User Request
    |
Router: Intent Detection (single intent table)
    |
Pre-processing gates (if applicable)
    |
Preflight Check (is domain knowledge service available?)
    +-- READY --> Step 0: Query domain knowledge --> pass context to sub-skill
    +-- MISSING --> Skip Step 0 --> sub-skill uses hardcoded fallback
    |
Load sub-skill
```

### Pattern 2: Preflight-then-Proceed

For standalone skills and CKEs, verify prerequisites before executing:

```
Preflight Check (required data, permissions, Marketplace listings)
    +-- READY --> Execute workflow
    +-- MISSING --> Guide user through setup OR graceful fallback
    +-- ERROR --> Surface error, suggest fix
```

### Pattern 3: Hardcoded Fallback Schema

Every sub-skill that depends on dynamic domain knowledge MUST include a hardcoded fallback schema. This ensures the skill works even when:
- Cortex Search Service hasn't been created yet
- Database/schema permissions are insufficient
- First-time setup before any infrastructure exists

### Pattern 4: Environment-then-Execute

For compute-heavy skills (Archetype D), verify runtime before execution:

```
Step 1: Environment Check (Docker, Nextflow, Python packages, compute pools)
    +-- PASS --> Continue
    +-- FAIL --> Install instructions, stop
Step 2: Test Profile (dry run with minimal data)
    +-- PASS --> Continue to production run
    +-- FAIL --> Debug, stop
Step 3: Production Execution
```

### Pattern 5: Separation of Extraction and Normalization

For skills that extract entities and map to standard codes:

- **Extraction**: LLM/regex reads source data → outputs structured entities with `code = NULL`
- **Normalization**: SQL exact match → deterministic mapping → LLM fuzzy match → UPDATE code fields

**Why separate?** LLMs hallucinate codes. Dedicated normalization with exact-match-first pipelines and candidate-constrained fuzzy matching eliminates code hallucination.

---

## 6. Domain Knowledge Management

### Searchable Model Reference (for Router Skills)

Router skills with complex data models should create a Cortex Search Service over column/table metadata:

1. Build a reference table (e.g., `{DOMAIN}_MODEL_REFERENCE`) with: table_name, column_name, data_type, constraints, description, enum_values, contains_phi
2. Create a Cortex Search Service over this table
3. Router queries the service in Step 0 to ground sub-skills dynamically

**Benefit**: Schema evolution (new columns, changed enums) is automatically picked up by sub-skills.

**Existing examples**:
- Imaging: `DICOM_MODEL_REFERENCE` (222 rows, 18 tables) + `DICOM_MODEL_SEARCH_SVC`
- Clinical NLP: `CLINICAL_NLP_MODEL_REFERENCE` (245 rows, 17 tables) + `CLINICAL_NLP_MODEL_SEARCH_SVC`

### Domain Reference Documents

Skills that encode complex domain knowledge should include reference documents:
- Imaging: `references/dicom-standards.md` (DICOM tag dictionary, SOP classes)
- Clinical NLP: domain taxonomy PDF (entity types, context attributes, note sections)
- Genomics: pipeline-specific references (nf-core configs, genome builds)

### Terminology and Code Systems

Skills that normalize data to standard codes need terminology management:

| Domain | Code Systems | Source |
|--------|-------------|--------|
| Clinical NLP | ICD-10-CM, SNOMED CT, RxNorm, LOINC, MedDRA, ICD-O-3, CPT/ICD-10-PCS | CDC/CMS APIs, UMLS, curated CSVs |
| Pharmacovigilance | MedDRA (PT, SOC) | FAERS data files |
| OMOP | OMOP Vocabulary (concept, concept_relationship) | Athena downloads |
| Genomics | ClinVar, gnomAD, HGNC, Ensembl | Annotation databases |
| Imaging | DICOM UID Registry, SOP Class UIDs | DICOM standard |

**Key principle**: Never hardcode which code system to use. Different customers need different systems (US billing = ICD-10-CM, research = SNOMED CT, pharmacovigilance = MedDRA). Use a preference gate when applicable.

---

## 7. Platform Affinity Design

### Declaring Platform Dependencies

Every HCLS skill MUST declare `platform_affinities` in its SKILL.md frontmatter:

```yaml
platform_affinities:
  produces: [tables, views, dynamic_tables, ...]
  benefits_from:
    - skill: dynamic-tables
      when: "incremental refresh needed for ongoing data feeds"
    - skill: data-governance
      when: "output tables contain PHI or patient data"
    - skill: developing-with-streamlit
      when: "user wants a dashboard or data explorer"
```

### What Goes in `produces`

| Include | Exclude |
|---------|---------|
| Objects the pipeline creates and manages (tables, DTs, views, SPs, tasks, streams) | One-time admin setup (network_rules, external_access_integrations) |
| Governance objects (masking_policies, row_access_policies, tags, database_roles) | Snowflake built-ins (warehouses, resource monitors) |
| Serving objects (cortex_search_service, stages, ml_models) | External dependencies (API keys, UMLS licenses) |

### Common Platform Affinities Across HCLS

| Platform Skill | Used By | When |
|---------------|---------|------|
| `dynamic-tables` | Imaging, Clinical NLP, FHIR | Incremental ingestion/extraction pipelines |
| `data-governance` | Imaging, Clinical NLP, FHIR, Claims | PHI/HIPAA-regulated data |
| `developing-with-streamlit` | Imaging, Pharmacovigilance, Claims, Clinical NLP | Dashboards and review apps |
| `deploy-to-spcs` | Imaging, Nextflow, scvi-tools | Container workloads (viewers, pipelines, GPU training) |
| `machine-learning` | Imaging, Genomics, Clinical NLP | Model training and registry |
| `cortex-ai-functions` | Clinical NLP, Clinical Docs, Pharmacovigilance | LLM-powered extraction and analysis |
| `semantic-view` | Pharmacovigilance, Claims, OMOP | Natural language analytics |
| `search-optimization` | Imaging, Clinical NLP | Full-text search over metadata or notes |

The orchestrator reads these affinities during plan building to sequence platform skills into the solution.

---

## 8. Governance Patterns

### When Governance Is Required

Governance is required (not optional) when the skill handles:
- **PHI** (Protected Health Information) — patient names, dates, MRNs, clinical notes
- **Patient-level data** — adverse event reports, claims, encounters
- **Identifiable genomic data** — variants linked to patient IDs
- **Imaging data** — DICOM headers contain patient demographics

### Governance Approaches by Skill Type

| Skill Type | Governance Pattern |
|-----------|-------------------|
| **Router with sub-skills** (Imaging, Clinical NLP) | Dedicated governance sub-skill with multi-layer framework (tags, masking, RAP, roles, AI guardrails, audit) |
| **Standalone clinical** (FHIR, OMOP, Claims) | Governance section within SKILL.md workflow, platform affinity to `data-governance` |
| **Pharma safety** (Pharmacovigilance) | Patient-level masking on FAERS demographics, role-based access to case reports |
| **Genomics** | De-identification of sample-to-patient mapping, secure compute for variant data |
| **CKEs** | No governance needed (shared read-only services, no patient data) |

### Tag-Based Governance (Recommended)

Use Snowflake object tags for PHI classification rather than manual column tracking:
1. Tag PHI columns with category tags (e.g., `PHI_CATEGORY = 'PATIENT_NAME'`)
2. Attach masking policies to tags (not individual columns)
3. New PHI columns get masked automatically by tagging

This pattern scales across all tables in any HCLS domain.

---

## 9. Testing HCLS Skills

### Testing by Archetype

| Archetype | Test Focus | Approach |
|-----------|-----------|----------|
| **Standalone** | Workflow steps produce correct output | Run each step on sample data, verify output schema and values |
| **Router + Sub-skills** | Intent routing + sub-skill correctness | Test intent detection, then test each sub-skill independently |
| **CKE** | Preflight detection + query results | Test READY/MISSING/ERROR paths, verify search relevance |
| **Compute-heavy** | Environment + pipeline execution | Test profile run, verify output artifacts |

### Domain-Specific Validation

Beyond structural testing, HCLS skills need domain validation:

| Domain | Validation | Success Criteria |
|--------|-----------|-----------------|
| Clinical NLP | Entity extraction accuracy | Clinically correct entities, 100% JSON parse rate, zero code hallucination |
| FHIR | Resource mapping completeness | All resource types parsed, no data loss, correct FK relationships |
| OMOP | CDM conformance | Vocabulary mapping accuracy, measurement/observation routing |
| Imaging | DICOM tag coverage | All standard tags parsed, modality-specific fields present |
| Pharmacovigilance | Signal detection validity | PRR/ROR calculations match published benchmarks |
| Genomics | Variant annotation accuracy | ClinVar pathogenicity matches, allele frequency ranges correct |
| Lab instruments | ASM schema compliance | Valid Allotrope JSON, instrument type detected correctly |

### Scale Testing

For skills that process data at volume, include scale tests:
- **Clinical NLP**: 50+ documents, verify 0 failures
- **FHIR**: 1000+ bundles, verify throughput and memory
- **Imaging**: 100+ DICOM series, verify pipeline latency
- **Claims**: Full quarter of claims data, verify aggregation accuracy

---

## 10. Portable Seed and Reference Data

### Package for Reproducibility

Skills that depend on reference data should package it for portability:

```
seed-data/                          (or references/ for non-loadable data)
├── {reference_table}.csv           # Core reference data
├── {lookup_table}.csv              # Lookup/mapping data
└── setup_{domain}_data.sql         # Complete setup: CREATE + stage + COPY INTO
```

### Setup Script Pattern

```sql
CREATE TABLE IF NOT EXISTS {TABLE} (...);
CREATE STAGE IF NOT EXISTS {domain}_seed_stage;

-- After PUT @{domain}_seed_stage/file.csv
COPY INTO {TABLE} FROM @{domain}_seed_stage/{file}.csv
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');
```

### Idempotent Loaders

Any SP-based loader (e.g., loading from external APIs) MUST be idempotent — check existing row count before loading. This prevents duplicate data on re-runs.

### Examples Across the Portfolio

| Skill | Seed Data | Size |
|-------|----------|------|
| Clinical NLP | CODE_SYSTEM (8 systems), CONCEPT_DIMENSION (154K codes) | CSVs + API SPs |
| Imaging | DICOM_MODEL_REFERENCE (222 rows, 18 tables) | CSV |
| OMOP | OMOP Vocabulary tables (concept, concept_relationship) | Athena download |
| Pharmacovigilance | FAERS quarterly data files (DEMO, DRUG, REAC, OUTC) | FDA download |

---

## 11. Cortex Knowledge Extensions (CKEs)

### What CKEs Are

CKEs are Marketplace-shared Cortex Search Services that provide domain knowledge without copying data into the user's account. They enable RAG-based semantic search across curated corpora.

### Current CKEs

| CKE | Corpus | Consumers |
|-----|--------|-----------|
| PubMed | Biomedical literature | Pharmacovigilance, Clinical NLP, Research |
| ClinicalTrials.gov | Trial registry | Clinical Trial Protocol, Claims, Survival Analysis |

### CKE Integration Pattern

Domain skills that consume CKEs should:
1. Run preflight probe to detect Marketplace listing
2. If READY — execute CKE query and enrich domain result
3. If MISSING — skip enrichment, log a note
4. **Never fail the parent skill** because a CKE is unavailable

### Building New CKEs

New CKEs follow the same pattern:
1. Create skill at `skills/hcls-cross-cke-{name}/SKILL.md`
2. Include: preflight check, query patterns, fallback behavior, Marketplace listing details
3. Register in orchestrator under Cross-Industry > Knowledge Extensions
4. Document which domain skills consume it

---

## 12. Industry Skill Development Lifecycle

### Incubator Phases

HCLS skills follow a phased development approach. The exact phases vary by archetype, but the general pattern is:

#### Phase 1: Domain Knowledge Acquisition

- Identify authoritative standards for your domain
- Catalog all concepts/entities the skill will handle
- Map to standard data models where applicable
- **Stopping point**: Get domain expert review before coding

#### Phase 2: Data Model / Schema Design (if applicable)

- Design target schema aligned to domain standards
- Build searchable model reference (for router skills)
- Define reference/terminology tables
- **Stopping point**: Validate schema against standards

#### Phase 3: Core Workflow Implementation

- Build the primary skill workflow (or sub-skills for router archetypes)
- Follow the appropriate archetype pattern
- Include hardcoded fallbacks for dynamic dependencies
- **Stopping point**: Test on representative sample data

#### Phase 4: Validation and Cross-Checks

- Validate domain-specific correctness
- Cross-check field alignment across sub-skills (if router)
- Verify platform affinities are accurate
- **Stopping point**: All validation checks pass

#### Phase 5: Pipeline / Production Patterns (if applicable)

- Wire workflows into automated pipelines (DTs, tasks, SPs)
- Design for batch execution and warehouse parallelism
- **Stopping point**: Pipeline runs end-to-end on sample data

#### Phase 6: Governance (if handling sensitive data)

- Implement appropriate governance pattern for your data type
- **Stopping point**: Governance tested with role-based access verification

#### Phase 7: E2E Testing

- Run full end-to-end tests at representative scale
- Validate across all workflow paths
- Document test results

#### Phase 8: Cross-Validation Against Best Practices

- Audit skill against platform `SKILL_BEST_PRACTICES.md`
- Check: frontmatter, stopping points, Output sections, file sizes, CYOA validation
- Create `skill_evidence.yaml` for promotion lifecycle
- Create `CROSS_VALIDATION_REPORT.md` documenting audit results

### Promotion Lifecycle

Skills move through promotion stages tracked in `skill_evidence.yaml`:

```
draft --> review --> staging --> production
```

| Stage | Where | Who |
|-------|-------|-----|
| draft | Incubator repo (branch) | Contributor |
| review | Incubator repo (PR to main) | Tiger Team review |
| staging | Incubator repo (main) | Field testing |
| production | cortex-code-skills repo | Tiger Team publishes |

---

## 13. Orchestrator Integration

### Skill Registration

Every skill must be registered in:
1. `templates/skills_incubator.yaml` — incubator registry
2. Orchestrator template (`templates/orchestrator.md.j2`) — routing rules

### Routing Rules

The orchestrator routes by:
1. **Segment** (Provider / Pharma / Cross-Industry) from user context
2. **Domain** within segment (clinical data, imaging, genomics, drug safety)
3. **Trigger keywords** matched against skill descriptions
4. **Task type** for disambiguation when multiple skills overlap

### Cross-Domain Patterns

Some user requests span multiple skills. The orchestrator recognizes cross-domain patterns and builds multi-step plans:

| Pattern | Skills Involved |
|---------|----------------|
| FHIR → OMOP → Analytics | FHIR + OMOP + semantic-view |
| Clinical Notes → Coded Data → Dashboard | Clinical NLP + Streamlit |
| DICOM Ingest → Governance → Viewer | Imaging (3 sub-skills) + data-governance |
| Claims + Literature → RWE | Claims + PubMed CKE |
| Genomics → ML → Deployment | Nextflow/variant-annotation + machine-learning + SPCS |

### Anti-Patterns

The orchestrator enforces these anti-patterns:
- Do not run NLP extraction on raw files (parse/transform first)
- Do not run survival analysis without a cohort definition
- Do not use CKEs for non-evidence tasks
- Do not skip preflight checks
- Do not force-follow a pattern that doesn't match the data
- Do not use DICOM parser standalone when full imaging pipeline is needed

---

## 14. Common Pitfalls

### Universal Pitfalls (All HCLS Skills)

1. **Treating governance as optional for patient data** — Healthcare data almost always has PHI. Build governance early, not as an afterthought.

2. **Missing hardcoded fallback schemas** — Sub-skills that depend on Cortex Search must include fallback schemas for first-time setup or permission issues.

3. **One-time infrastructure in `produces`** — Network rules, external access integrations, and admin setup don't belong in `produces`. Only include objects the pipeline creates and manages.

4. **Monolithic skills for multi-concept domains** — Split by concept category or workflow stage for parallel development and selective execution.

5. **Skipping preflight checks** — Every external dependency (CKEs, Marketplace listings, data model repos) needs a preflight probe with READY/MISSING/ERROR handling.

### NLP/Extraction-Specific Pitfalls

6. **Letting LLMs assign terminology codes during extraction** — Extraction produces display text only. Normalization is a separate step with exact-match-first pipeline.

7. **Hardcoding code system preferences** — Different customers use different code systems. Always ask via a preference gate.

8. **Generic fuzzy match prompts** — Code-system-specific structure guidance (RxNorm TTY hierarchy, LOINC 6-axis, MedDRA PT level) dramatically improves accuracy.

9. **Row-by-row LLM calls** — Use batch SQL UPDATEs with inline Cortex COMPLETE. Warehouses parallelize across rows automatically.

### Genomics/Compute-Specific Pitfalls

10. **Skipping test profile runs** — Always run pipelines on test data before production execution. nf-core `-profile test` catches config issues early.

11. **Assuming compute environment exists** — Verify Docker, Nextflow, Python packages, and compute pools before execution. Environment check must be Step 1.

### CKE-Specific Pitfalls

12. **Failing parent skills when CKE is unavailable** — CKEs are enrichment, not prerequisites. Always degrade gracefully.

13. **Hardcoding CKE database names** — Marketplace listing database names are assigned at install time. Use parameterized references.

---

## 15. Quick Reference Checklist

### Platform Best Practices (from SKILL_BEST_PRACTICES.md)

- [ ] SKILL.md under 500 lines (router can be slightly longer)
- [ ] `name` and `description` in frontmatter with trigger keywords
- [ ] All sub-skills have `parent_skill` declared
- [ ] **⚠️ MANDATORY STOPPING POINT** markers at all approval gates
- [ ] No chaining without user approval
- [ ] `## Output` section in every skill and sub-skill
- [ ] `skill_evidence.yaml` with `promotion_stage`
- [ ] CYOA validation passes (reachability, determinism, termination)
- [ ] No extraneous docs (README.md, CHANGELOG.md)

### HCLS Industry Additions

- [ ] Taxonomic naming: `hcls-{segment}-{domain}-{capability}`
- [ ] `platform_affinities` declared (`produces` + `benefits_from`)
- [ ] Preflight checks for all external dependencies
- [ ] Hardcoded fallback schemas where applicable
- [ ] Governance pattern appropriate for data sensitivity level
- [ ] Domain-specific validation tests (not just structural tests)
- [ ] Portable seed/reference data with setup scripts
- [ ] Registered in `skills_incubator.yaml` and orchestrator template
- [ ] Cross-validated against this guide and platform best practices
- [ ] `CROSS_VALIDATION_REPORT.md` documenting audit results

### Archetype-Specific Checks

**Router + Sub-Skills**:
- [ ] Intent table covers all user-facing workflows
- [ ] Conditional pre-step (data model knowledge) with preflight
- [ ] One sub-skill per concept category (not monolithic)
- [ ] Governance as dedicated sub-skill (if PHI involved)
- [ ] Pipeline implementation sub-skill (if production deployment needed)

**Standalone**:
- [ ] Complete workflow in single SKILL.md
- [ ] Platform affinities guide users to complementary capabilities

**CKE**:
- [ ] Preflight probe with READY/MISSING/ERROR handling
- [ ] Graceful degradation (never fails parent skills)
- [ ] Parameterized database references

**Compute-Heavy**:
- [ ] Environment check as mandatory Step 1
- [ ] Test profile/dry run before production execution
- [ ] Output verification after pipeline completion
