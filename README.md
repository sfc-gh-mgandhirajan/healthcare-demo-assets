# Healthcare & Life Sciences Skills for Cortex Code

A collection of domain-specific skills for [Cortex Code](https://docs.snowflake.com/user-guide/snowflake-cortex/cortex-agents) focused on healthcare and life sciences workflows on Snowflake. These skills turn Cortex Code into an AI-powered assistant that understands clinical documents, medical imaging, genomics, pharmacovigilance, and more — all running natively on Snowflake.

## What This Repository Does

Each **skill** is a set of instructions (Markdown + optional scripts) that teaches Cortex Code how to perform a specific healthcare task. When you register these skills, Cortex Code can:

- **Parse and extract** structured data from clinical documents (discharge summaries, pathology reports, radiology reports) using Cortex AI functions
- **Build DICOM imaging pipelines** with an 18-table data model, metadata search, and ML-ready embeddings
- **Transform healthcare data** between standards (FHIR R4, OMOP CDM v5.4, HL7)
- **Run genomics pipelines** (nf-core, scVI, variant annotation, survival analysis)
- **Detect drug safety signals** from FDA FAERS data
- **Search biomedical literature** via PubMed and ClinicalTrials.gov Cortex Knowledge Extensions

## Prerequisites

- A Snowflake account with [Cortex AI functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions) enabled
- [Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code) (CLI or IDE)
- For clinical document skills: `AI_PARSE_DOCUMENT`, `AI_EXTRACT`, `AI_AGG` access
- For search/agent skills: Cortex Search and Cortex Agent access
- For genomics skills: local Python environment with relevant packages

## Quick Start

### 1. Clone and register

```bash
git clone https://github.com/sfc-gh-jrag/coco-healthcare-skills.git
```

Add to your Cortex Code `skills.json`:

```json
{
  "remote": [
    {
      "url": "https://github.com/sfc-gh-jrag/coco-healthcare-skills",
      "skills_path": "skills"
    }
  ]
}
```

### 2. Or copy individual skills locally

```bash
cp -r coco-healthcare-skills/skills/hcls-provider-cdata-clinical-docs ~/.cortex/skills/
cp -r coco-healthcare-skills/skills/hcls-provider-imaging ~/.cortex/skills/
```

### 3. Start using

Tell Cortex Code what you want to do. The agent profiles route your request to the right skill:

```
"Extract data from my clinical documents on stage"
"Parse DICOM metadata and build a radiology data model"
"Transform my FHIR bundles into relational tables"
"Annotate my VCF file with ClinVar pathogenicity"
```

## Repository Structure

```
coco-healthcare-skills/
├── agents/                    # Agent profiles (orchestrators)
│   ├── health-sciences-incubator.md   # All skills enabled — prototyping and demos
│   └── health-sciences-solutions.md   # Production-grade skills only
├── skills/                    # All skills (18 total)
│   ├── hcls-cross-*           # Cross-industry skills (CKEs, research)
│   ├── hcls-pharma-*          # Pharma & life sciences skills
│   └── hcls-provider-*        # Healthcare provider skills
├── references/                # Shared reference data
├── scripts/                   # Utility scripts (PDF gen, orchestrators)
├── shared/preflight/          # Shared preflight checker module
└── templates/                 # YAML registries & Jinja2 templates
```

## Architecture Concepts

### Skill Types

| Type | Description | Example |
|------|-------------|---------|
| **Router skill** | Detects user intent and routes to specialized sub-skills. Contains setup, preflight checks, and workflow orchestration. | `hcls-provider-imaging`, `hcls-provider-cdata-clinical-docs` |
| **Sub-skill** | Handles one specific task within a router. Loaded by the router, not directly by the user. | `dicom-parser`, `clinical-document-extraction` |
| **Standalone skill** | Self-contained skill with no router or sub-skills. | `hcls-provider-cdata-fhir`, `hcls-pharma-dsafety-pharmacovigilance` |

### CKE (Cortex Knowledge Extensions)

Several skills use **CKE** — dynamically discoverable metadata served via Cortex Search Services. Instead of hardcoding table schemas or document type definitions, skills query a search service at runtime to get the latest metadata.

```
Source of Truth (YAML / Excel)
    │
    └──> Backing Table (in DATA_MODEL_KNOWLEDGE schema)
            │
            └──> Cortex Search Service (CKE)
                    │
                    └──> Skills query at runtime via SEARCH_PREVIEW()
```

This means:
- **Schema changes propagate automatically** — edit the source, refresh the table, the search service updates within its TARGET_LAG
- **Skills are never stale** — they always query the latest metadata
- **Fallback is built in** — if the search service is down, skills fall back to local files on disk

### Router Pattern (DICOM + Clinical Docs)

Both router skills follow the same architecture:

1. **Preflight Check** — probe the CKE search service at skill load (READY / MISSING)
2. **Intent Detection** — classify what the user wants (parse, ingest, search, agent, etc.)
3. **Conditional Step 0** — if CKE is READY, query it for schema/spec context before routing
4. **Sub-skill Loading** — pass grounding context to the sub-skill
5. **Fallback** — if CKE is MISSING, sub-skills use local reference files

### Defense-in-Depth (Clinical Docs)

The clinical documents skill enforces a three-layer guardrail system:

| Layer | Mechanism | Purpose |
|-------|-----------|---------|
| 1 | `AGENTS.md` (profile-level rules) | Session-wide constraints |
| 2 | Gate micro-skills + Phase skills | Structural decomposition — model cannot skip steps |
| 3 | `hooks.json` | Hard blocks on DDL/DML without user confirmation |

Every decision point requires explicit user confirmation via `ask_user_question`. The pipeline is split into **Tier 1 gates** (pre-conditions) and **Tier 2 phases** (execution), with mandatory re-entry between phases.

---

## Featured Solution: Clinical Document Intelligence

**Skill**: [`hcls-provider-cdata-clinical-docs`](skills/hcls-provider-cdata-clinical-docs/)

An end-to-end pipeline for extracting structured intelligence from clinical documents (PDF, DOCX, PNG, JPG, TIFF, TXT) using Snowflake Cortex AI.

### What It Does

```
Clinical PDFs on Stage
    → AI_PARSE_DOCUMENT (OCR / LAYOUT)
    → AI_EXTRACT (classify doc type + extract fields)
    → AI_AGG (handle multi-page split documents)
    → Pivot Views (one per doc type)
    → Cortex Search Service (full-text search)
    → Semantic View + Cortex Agent (natural language queries)
```

### Sub-Skills

| Sub-Skill | Purpose |
|-----------|---------|
| `clinical-document-extraction` | Orchestrator for the full extraction pipeline (gates + phases) |
| `clinical-docs-search` | Create a Cortex Search Service over parsed content |
| `clinical-docs-agent` | Create a Cortex Agent combining Analyst (Semantic View) + Search |
| `clinical-docs-viewer` | Build a Streamlit document viewer |
| `data-model-knowledge` | Query schema and doc type specs via CKE at runtime |

### CKE Architecture (Dual-Layer)

Clinical docs uses two CKE layers for dynamic metadata discovery:

| CKE Layer | Search Service | Answers |
|-----------|---------------|---------|
| **Schema CKE** | `CLINICAL_DOCS_MODEL_SEARCH_SVC` | "What tables exist?" "Which columns contain PHI?" |
| **Spec CKE** | `CLINICAL_DOCS_SPECS_SEARCH_SVC` | "What fields does a discharge summary have?" "What prompt extracts MRN?" |

```
document_type_specs.yaml (authoritative source of truth)
    │
    ├──> CLINICAL_DOCS_SPECS_REFERENCE table → Spec CKE (search service)
    │
    └──> EXTRACTION_CONFIG table (derived)
            └──> GENERATE_DYNAMIC_OBJECTS() stored procedure
                    ├── Pivot views, Semantic View, refresh task
                    ├── Step 7:  CLINICAL_DOCS_MODEL_REFERENCE → Schema CKE
                    └── Step 7b: CLINICAL_DOCS_SPECS_REFERENCE → Spec CKE
```

### Config-Driven Design

The pipeline is fully config-driven. To add a new document type:

1. Add an entry to `references/document_type_specs.yaml`
2. Seed the config table from the spec
3. `CALL GENERATE_DYNAMIC_OBJECTS()` — one call creates everything:
   - Classification prompt update (LISTAGG of all types)
   - New pivot view
   - Refresh task rebuild with new JOIN
   - Semantic View update
   - Schema CKE + Spec CKE refresh

### Key Reference Files

| File | Purpose |
|------|---------|
| `references/document_type_specs.yaml` | Authoritative doc type definitions (fields, prompts, PHI flags) |
| `references/architecture.md` | Pipeline architecture and design decisions |
| `references/metadata_as_cke.md` | CKE pattern documentation and DICOM comparison |
| `clinical-document-extraction/scripts/dynamic_pipeline_setup.sql` | All DDL — tables, procs, CKE services, search services |

---

## Featured Solution: DICOM Medical Imaging

**Skill**: [`hcls-provider-imaging`](skills/hcls-provider-imaging/)

A comprehensive DICOM imaging solution with an 18-table data model, metadata search, and ML-ready embeddings.

### Sub-Skills

| Sub-Skill | Purpose |
|-----------|---------|
| `dicom-parser` | Parse DICOM file metadata with pydicom, generate DDL from 18-table model |
| `dicom-ingestion` | Build ingestion pipelines (COPY INTO, Dynamic Tables, Streams + Tasks) |
| `dicom-analytics` | Imaging metadata analytics, Cortex Search, radiology NLP |
| `imaging-viewer` | Streamlit DICOM viewer |
| `imaging-governance` | HIPAA compliance, PHI masking, de-identification |
| `imaging-ml` | ML model training and deployment for imaging |
| `data-model-knowledge` | Query the DICOM data model via CKE at runtime |

### CKE Architecture

DICOM uses a single Schema CKE layer:

```
dicom_data_model_reference.xlsx → CSV → DICOM_MODEL_REFERENCE table
                                            → DICOM_MODEL_SEARCH_SVC (Cortex Search)
```

Sub-skills query the search service for table definitions, column types, DICOM tag mappings, and PHI indicators. DDL can be generated dynamically using `CORTEX.COMPLETE()` grounded by search results.

---

## All Skills Reference

### Provider — Clinical Data Management

| Skill | Type | Description |
|-------|------|-------------|
| [hcls-provider-cdata-clinical-docs](skills/hcls-provider-cdata-clinical-docs/) | Router (5 sub-skills) | Clinical document intelligence with AI extraction, search, and agent |
| [hcls-provider-cdata-fhir](skills/hcls-provider-cdata-fhir/) | Standalone | Transform FHIR R4 resources into relational Snowflake tables |
| [hcls-provider-cdata-clinical-nlp](skills/hcls-provider-cdata-clinical-nlp/) | Standalone | Extract structured entities from clinical text using NLP |
| [hcls-provider-cdata-omop](skills/hcls-provider-cdata-omop/) | Standalone | Transform EHR/claims data to OMOP CDM v5.4 |

### Provider — Clinical Research

| Skill | Type | Description |
|-------|------|-------------|
| [hcls-provider-imaging](skills/hcls-provider-imaging/) | Router (7 sub-skills) | DICOM medical imaging — parsing, ingestion, analytics, governance, ML |
| [hcls-provider-imaging-dicom-parser](skills/hcls-provider-imaging-dicom-parser/) | Standalone | Standalone DICOM metadata parser (also available as sub-skill) |

### Provider — Revenue Cycle

| Skill | Type | Description |
|-------|------|-------------|
| [hcls-provider-claims-data-analysis](skills/hcls-provider-claims-data-analysis/) | Standalone | Claims RWE — cohort building, utilization, PMPM costs, treatment patterns |

### Pharma — Drug Safety

| Skill | Type | Description |
|-------|------|-------------|
| [hcls-pharma-dsafety-pharmacovigilance](skills/hcls-pharma-dsafety-pharmacovigilance/) | Standalone | FDA FAERS adverse event analysis — PRR/ROR signal detection |
| [hcls-pharma-dsafety-clinical-trial-protocol](skills/hcls-pharma-dsafety-clinical-trial-protocol/) | Standalone | Generate clinical trial protocols for FDA submissions |

### Pharma — Genomics

| Skill | Type | Description |
|-------|------|-------------|
| [hcls-pharma-genomics-nextflow](skills/hcls-pharma-genomics-nextflow/) | Standalone | nf-core bioinformatics pipelines (rnaseq, sarek, atacseq) |
| [hcls-pharma-genomics-scvi-tools](skills/hcls-pharma-genomics-scvi-tools/) | Standalone | Deep learning single-cell analysis (scVI, scANVI, totalVI, PeakVI) |
| [hcls-pharma-genomics-single-cell-qc](skills/hcls-pharma-genomics-single-cell-qc/) | Standalone | Automated scRNA-seq QC with MAD-based filtering |
| [hcls-pharma-genomics-survival-analysis](skills/hcls-pharma-genomics-survival-analysis/) | Standalone | Kaplan-Meier, Cox regression, time-to-event modeling |
| [hcls-pharma-genomics-variant-annotation](skills/hcls-pharma-genomics-variant-annotation/) | Standalone | VCF annotation with ClinVar, gnomAD, ACMG classification |

### Pharma — Lab Operations

| Skill | Type | Description |
|-------|------|-------------|
| [hcls-pharma-lab-allotrope](skills/hcls-pharma-lab-allotrope/) | Standalone | Convert lab instrument files to Allotrope Simple Model JSON |

### Cross-Industry

| Skill | Type | Description |
|-------|------|-------------|
| [hcls-cross-research-problem-selection](skills/hcls-cross-research-problem-selection/) | Standalone | Scientific problem selection framework |
| [hcls-cross-cke-pubmed](skills/hcls-cross-cke-pubmed/) | CKE | RAG search over PubMed biomedical literature via Marketplace |
| [hcls-cross-cke-clinical-trials](skills/hcls-cross-cke-clinical-trials/) | CKE | RAG search over ClinicalTrials.gov via Marketplace |

## Skill Taxonomy

Skills follow a five-level naming convention: `Industry / Sub-Industry / Business Function / Use Case / Sub-Skill`

```
Health Sciences
├── Provider
│   ├── Clinical Research
│   │   ├── hcls-provider-imaging (router + 7 sub-skills)
│   │   └── hcls-provider-imaging-dicom-parser (standalone)
│   ├── Clinical Data Management
│   │   ├── hcls-provider-cdata-fhir
│   │   ├── hcls-provider-cdata-clinical-nlp
│   │   ├── hcls-provider-cdata-omop
│   │   └── hcls-provider-cdata-clinical-docs (router + 5 sub-skills)
│   └── Revenue Cycle
│       └── hcls-provider-claims-data-analysis
│
├── Pharma
│   ├── Drug Safety
│   │   ├── hcls-pharma-dsafety-pharmacovigilance
│   │   └── hcls-pharma-dsafety-clinical-trial-protocol
│   ├── Genomics
│   │   ├── hcls-pharma-genomics-nextflow
│   │   ├── hcls-pharma-genomics-variant-annotation
│   │   ├── hcls-pharma-genomics-single-cell-qc
│   │   ├── hcls-pharma-genomics-scvi-tools
│   │   └── hcls-pharma-genomics-survival-analysis
│   └── Lab Operations
│       └── hcls-pharma-lab-allotrope
│
└── Cross-Industry
    ├── Research Strategy
    │   └── hcls-cross-research-problem-selection
    └── Knowledge Extensions
        ├── hcls-cross-cke-pubmed
        └── hcls-cross-cke-clinical-trials
```

## Agent Profiles

Two orchestrator profiles in `agents/` control which skills are available:

| Profile | File | Purpose |
|---------|------|---------|
| **Incubator** | `health-sciences-incubator.md` | All 18 skills enabled — rapid prototyping, demos, and development |
| **Production** | `health-sciences-solutions.md` | Production-grade skills only — skills graduate here after validation |

Profiles include routing rules (by sub-industry and task type), cross-domain composition patterns, CKE integration guidance, and HIPAA guardrails.

## Cross-Domain Composition Patterns

Skills can be composed for end-to-end solutions:

| Pattern | Skills Involved |
|---------|----------------|
| **Clinical Data Warehouse** | FHIR → OMOP CDM → governance → semantic views |
| **Imaging + Clinical** | dicom-parser → FHIR → clinical-nlp → PubMed CKE → Streamlit |
| **Drug Safety** | pharmacovigilance → PubMed CKE → clinical-nlp → claims analysis |
| **Genomics Pipeline** | nextflow → variant-annotation → survival-analysis → ML |
| **Single-Cell** | single-cell-qc → scvi-tools → ML Registry |
| **Real-World Evidence** | claims → ClinicalTrials CKE → OMOP → survival-analysis → PubMed CKE |
| **Clinical Trial Design** | research-problem-selection → ClinicalTrials CKE → PubMed CKE → protocol generation |
| **Document Intelligence** | clinical-docs extraction → search → agent → governance |

## Contributing a New Skill

Each skill follows this layout:

```
skills/{skill-name}/
├── SKILL.md           # Main instructions (required)
├── scripts/           # Python/SQL helper scripts (optional)
├── references/        # Domain documentation (optional)
└── assets/            # Templates, sample data (optional)
```

To add a new skill:

1. Create the skill folder under `skills/` following the naming convention
2. Write `SKILL.md` with frontmatter (`name`, `description`, optional `parent_skill`, `tools`)
3. Register in `templates/skills_incubator.yaml`
4. Regenerate orchestrators: `python scripts/generate_orchestrators.py --profile incubator`
5. Add to the taxonomy tree and skills reference table in this README

For router skills with sub-skills, see `hcls-provider-cdata-clinical-docs/` or `hcls-provider-imaging/` as templates.

## Acknowledgments

- [Anthropic Life Sciences](https://github.com/anthropics/life-sciences) — original skill foundations
- [scverse](https://scverse.org/) — single-cell analysis ecosystem
- [nf-core](https://nf-co.re/) — bioinformatics pipeline community
- [OHDSI](https://ohdsi.org/) — OMOP Common Data Model
- [HL7 FHIR](https://hl7.org/fhir/) — healthcare interoperability standard
- [FDA FAERS](https://www.fda.gov/drugs/questions-and-answers-fdas-adverse-event-reporting-system-faers/fda-adverse-event-reporting-system-faers-public-dashboard) — pharmacovigilance data

## License

Apache License 2.0. See individual skill directories for specific licenses.

## Disclaimer

These skills are provided for educational and research purposes. They do not constitute medical, legal, or regulatory advice. Professional consultation is required for clinical applications.
