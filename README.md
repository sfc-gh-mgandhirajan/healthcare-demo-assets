# Healthcare & Life Sciences Skills for Cortex Code

A collection of domain-specific skills for [Cortex Code](https://docs.snowflake.com/user-guide/snowflake-cortex/cortex-agents) focused on healthcare and life sciences workflows on Snowflake.

## Repository Structure

```
coco-healthcare-skills/
├── agents/                    # Profile definitions (agent orchestrators)
│   ├── health-sciences-incubator.md   # All skills enabled for prototyping
│   └── health-sciences-solutions.md   # Production-grade skills only
├── skills/                    # All skills
│   ├── hcls-cross-*           # Cross-industry skills
│   ├── hcls-pharma-*          # Pharma & life sciences skills
│   └── hcls-provider-*        # Healthcare provider skills
├── references/                # Shared reference data (DICOM model)
├── scripts/                   # Utility scripts (PDF gen, orchestrators, etc.)
├── shared/preflight/          # Shared preflight checker module
└── templates/                 # YAML registries & Jinja2 templates
```

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

## Skills Reference

### Provider — Clinical Research

| Skill | Description |
|-------|-------------|
| [hcls-provider-imaging](skills/hcls-provider-imaging/) | Router for all DICOM medical imaging tasks — routes to 7 sub-skills: dicom-parser, dicom-ingestion, dicom-analytics, imaging-viewer, imaging-governance, imaging-ml, data-model-knowledge |
| [hcls-provider-imaging-dicom-parser](skills/hcls-provider-imaging-dicom-parser/) | Standalone DICOM metadata parser — extract tags, build radiology metadata tables |

### Provider — Clinical Data Management

| Skill | Description |
|-------|-------------|
| [hcls-provider-cdata-fhir](skills/hcls-provider-cdata-fhir/) | Transform FHIR R4 resources (Patient, Observation, Condition, etc.) into relational Snowflake tables |
| [hcls-provider-cdata-clinical-nlp](skills/hcls-provider-cdata-clinical-nlp/) | Extract structured entities (diagnoses, medications, procedures) from clinical text using NLP |
| [hcls-provider-cdata-omop](skills/hcls-provider-cdata-omop/) | Transform EHR/claims data to OMOP CDM v5.4 for observational research |
| [hcls-provider-cdata-clinical-docs](skills/hcls-provider-cdata-clinical-docs/) | Router: Clinical document intelligence with defense-in-depth guardrails — routes to 5 sub-skills: clinical-document-extraction, clinical-docs-search, clinical-docs-agent, clinical-docs-viewer, data-model-knowledge |

### Provider — Revenue Cycle

| Skill | Description |
|-------|-------------|
| [hcls-provider-claims-data-analysis](skills/hcls-provider-claims-data-analysis/) | Healthcare claims RWE — cohort building, utilization metrics, PMPM costs, treatment patterns |

### Pharma — Drug Safety

| Skill | Description |
|-------|-------------|
| [hcls-pharma-dsafety-pharmacovigilance](skills/hcls-pharma-dsafety-pharmacovigilance/) | FDA FAERS adverse event analysis — PRR/ROR signal detection, drug-event associations |
| [hcls-pharma-dsafety-clinical-trial-protocol](skills/hcls-pharma-dsafety-clinical-trial-protocol/) | Generate clinical trial protocols for FDA submissions (IDE/IND pathways) |

### Pharma — Genomics

| Skill | Description |
|-------|-------------|
| [hcls-pharma-genomics-nextflow](skills/hcls-pharma-genomics-nextflow/) | Run nf-core bioinformatics pipelines (rnaseq, sarek, atacseq) on local or GEO/SRA data |
| [hcls-pharma-genomics-scvi-tools](skills/hcls-pharma-genomics-scvi-tools/) | Deep learning single-cell analysis — scVI, scANVI, totalVI, PeakVI, MultiVI, veloVI |
| [hcls-pharma-genomics-single-cell-qc](skills/hcls-pharma-genomics-single-cell-qc/) | Automated scRNA-seq QC using scverse best practices with MAD-based filtering |
| [hcls-pharma-genomics-survival-analysis](skills/hcls-pharma-genomics-survival-analysis/) | Kaplan-Meier curves, Cox regression, and time-to-event modeling |
| [hcls-pharma-genomics-variant-annotation](skills/hcls-pharma-genomics-variant-annotation/) | Annotate VCF files with ClinVar pathogenicity, gnomAD allele frequencies, ACMG classification |

### Pharma — Lab Operations

| Skill | Description |
|-------|-------------|
| [hcls-pharma-lab-allotrope](skills/hcls-pharma-lab-allotrope/) | Convert lab instrument files (PDF, CSV, Excel, TXT) to Allotrope Simple Model JSON or flattened CSV |

### Cross-Industry

| Skill | Description |
|-------|-------------|
| [hcls-cross-research-problem-selection](skills/hcls-cross-research-problem-selection/) | Scientific problem selection framework (Fischbach & Walsh methodology) |
| [hcls-cross-cke-pubmed](skills/hcls-cross-cke-pubmed/) | Cortex Knowledge Extension — RAG search over PubMed biomedical literature via Marketplace |
| [hcls-cross-cke-clinical-trials](skills/hcls-cross-cke-clinical-trials/) | Cortex Knowledge Extension — RAG search over ClinicalTrials.gov via Marketplace |

## Agent Profiles

Two orchestrator profiles in `agents/` control which skills are available:

| Profile | File | Purpose |
|---------|------|---------|
| **Incubator** | `health-sciences-incubator.md` | All skills enabled — rapid prototyping and demos |
| **Production** | `health-sciences-solutions.md` | Production-grade skills only — skills graduate here from incubator |

Profiles include routing rules (by sub-industry and task type), cross-domain composition patterns, CKE integration guidance, and HIPAA guardrails.

## Skill Structure

Each skill follows this layout:

```
skills/{skill-name}/
├── SKILL.md           # Main instructions (required)
├── scripts/           # Python helper scripts (optional)
├── references/        # Domain documentation (optional)
└── assets/            # Templates, sample data (optional)
```

## Installation

```bash
git clone https://github.com/sfc-gh-jrag/coco-healthcare-skills.git
```

### Register as remote skills in Cortex Code

Add to your `skills.json`:

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

### Or copy individual skills locally

```bash
cp -r coco-healthcare-skills/skills/hcls-provider-cdata-fhir ~/.cortex/skills/
cp -r coco-healthcare-skills/skills/hcls-pharma-dsafety-pharmacovigilance ~/.cortex/skills/
```

## Cross-Domain Composition Patterns

Skills can be composed for end-to-end solutions:

- **Imaging + Clinical**: dicom-parser → FHIR → clinical-nlp → PubMed CKE → Streamlit
- **Clinical Data Warehouse**: FHIR → OMOP CDM → governance → semantic views
- **Drug Safety**: pharmacovigilance → PubMed CKE → clinical-nlp → claims analysis
- **Genomics Pipeline**: nextflow → variant-annotation → survival-analysis → ML
- **Single-Cell**: single-cell-qc → scvi-tools → ML Registry
- **Real-World Evidence**: claims → ClinicalTrials CKE → OMOP → survival-analysis → PubMed CKE
- **Clinical Trial Design**: research-problem-selection → ClinicalTrials CKE → PubMed CKE → protocol generation

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
