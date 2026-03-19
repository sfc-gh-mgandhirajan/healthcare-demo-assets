# Healthcare & Life Sciences Skills for Cortex Code

A collection of domain-specific skills for [Cortex Code](https://docs.snowflake.com/user-guide/snowflake-cortex/cortex-agents) focused on healthcare and life sciences workflows.

These skills are adapted from [Anthropic's life-sciences repository](https://github.com/anthropics/life-sciences), with additional Snowflake-optimized skills for clinical data workflows.

## Available Skills

### From Anthropic Life Sciences

| Skill | Description | Use When |
|-------|-------------|----------|
| [single-cell-rna-qc](skills/hcls-pharma-genomics-single-cell-qc/) | Quality control for single-cell RNA-seq data | QC analysis, filtering low-quality cells, scanpy/scverse workflows |
| [clinical-trial-protocol](skills/hcls-pharma-dsafety-clinical-trial-protocol/) | Generate clinical trial protocols for devices/drugs | Creating FDA submission docs, IDE/IND pathways |
| [scvi-tools](skills/hcls-pharma-genomics-scvi-tools/) | Deep learning for single-cell omics | Batch correction, multi-modal analysis, label transfer |
| [nextflow-development](skills/hcls-pharma-genomics-nextflow/) | Run nf-core bioinformatics pipelines | RNA-seq, variant calling, ATAC-seq analysis |
| [instrument-data-to-allotrope](skills/hcls-pharma-lab-allotrope/) | Convert instrument data to ASM format | Standardizing lab data for LIMS, data lakes |
| [scientific-problem-selection](skills/hcls-cross-research-problem-selection/) | Research problem selection framework | Project ideation, troubleshooting, strategic decisions |

### Snowflake Healthcare Skills

| Skill | Description | Use When |
|-------|-------------|----------|
| [fhir-data-transformation](skills/hcls-provider-cdata-fhir/) | Transform FHIR R4 to Snowflake tables | Parsing FHIR bundles, healthcare interoperability |
| [omop-cdm-modeling](skills/hcls-provider-cdata-omop/) | OMOP Common Data Model ETL | Observational research, OHDSI analytics |
| [variant-annotation](skills/hcls-pharma-genomics-variant-annotation/) | Annotate genomic variants | ClinVar, gnomAD, clinical variant interpretation |
| [survival-analysis](skills/hcls-pharma-genomics-survival-analysis/) | Kaplan-Meier & Cox regression | Clinical outcomes, time-to-event analysis |
| [pharmacovigilance](skills/hcls-pharma-dsafety-pharmacovigilance/) | FDA FAERS adverse event analysis | Drug safety signals, ADR detection |
| [claims-data-analysis](skills/hcls-provider-claims-data-analysis/) | Healthcare claims RWE | Utilization, treatment patterns, PMPM costs |
| [clinical-nlp](skills/hcls-provider-cdata-clinical-nlp/) | Extract entities from clinical text | NER on notes, medication extraction, ICD coding |
| [dicom-parser](skills/hcls-provider-imaging-dicom-parser/) | Parse DICOM medical image metadata | DICOM tags, radiology metadata, imaging analytics |

## Installation

Copy the skill folder to your `.cortex/skills/` directory:

```bash
# Clone the repo
git clone https://github.com/sfc-gh-jrag/coco-healthcare-skills.git

# Copy skills you want
cp -r coco-healthcare-skills/skills/hcls-pharma-genomics-single-cell-qc ~/.cortex/skills/
cp -r coco-healthcare-skills/skills/hcls-provider-cdata-fhir ~/.cortex/skills/
cp -r coco-healthcare-skills/skills/hcls-pharma-dsafety-pharmacovigilance ~/.cortex/skills/
```

Or download individual skill folders directly from GitHub.

## Skill Structure

Each skill follows this structure:

```
skill-name/
├── SKILL.md           # Main instructions (required)
├── scripts/           # Python helper scripts
├── references/        # Domain documentation
└── assets/            # Templates (optional)
```

## Skill Details

### Single-Cell RNA-seq QC

Automated quality control following scverse best practices:
- MAD-based filtering for counts, genes, and mitochondrial content
- Comprehensive visualizations (histograms, violin plots, scatter plots)
- Supports `.h5ad` and 10X `.h5` formats

**Requirements:** `anndata`, `scanpy`, `scipy`, `matplotlib`, `numpy`

### Clinical Trial Protocol

Generate clinical trial protocols for FDA submissions:
- Supports medical devices (IDE pathway) and drugs (IND pathway)
- Research similar trials via ClinicalTrials.gov
- Interactive sample size calculation
- Modular waypoint-based architecture

**Requirements:** `scipy`, `numpy`, ClinicalTrials.gov MCP server

### scvi-tools Deep Learning

Deep learning models for single-cell analysis:
- scVI/scANVI for integration and batch correction
- totalVI for CITE-seq (RNA + protein)
- PeakVI for ATAC-seq
- MultiVI for multiome (RNA + ATAC)
- veloVI for RNA velocity

**Requirements:** `scvi-tools`, `scanpy`, `anndata`, GPU recommended

### Nextflow Development

Run nf-core bioinformatics pipelines:
- RNA-seq (gene expression, differential expression)
- Sarek (germline/somatic variant calling)
- ATAC-seq (chromatin accessibility)
- Supports local FASTQs and GEO/SRA public data

**Requirements:** Docker, Nextflow, Java 11+

### Instrument Data to Allotrope

Convert laboratory instrument files to standardized formats:
- Auto-detect instrument types (PDF, CSV, Excel, TXT)
- Output ASM JSON or flattened CSV
- Generate Python parser code for data engineering handoff

**Requirements:** `allotropy`, `pandas`, `openpyxl`, `pdfplumber`

### Scientific Problem Selection

Conversational framework for research decisions:
- Pitch and refine new project ideas
- Troubleshoot stuck research projects
- Navigate strategic scientific decisions
- Based on Fischbach & Walsh's decision tree framework

**Requirements:** None (conversational skill)

### FHIR Data Transformation

Transform FHIR R4 resources into Snowflake tables:
- Parse FHIR Bundles and NDJSON
- Extract Patient, Observation, Condition, MedicationRequest, etc.
- Flatten nested structures for SQL analytics
- Code system mapping (SNOMED, LOINC, RxNorm)

**Requirements:** `fhir.resources`, `pandas`, `snowflake-connector-python`

### OMOP CDM Modeling

Transform clinical data to OMOP Common Data Model:
- Snowflake DDL for OMOP CDM v5.4
- Vocabulary mapping (ICD-10 → SNOMED, NDC → RxNorm)
- ETL patterns for PERSON, CONDITION_OCCURRENCE, DRUG_EXPOSURE
- Data quality checks

**Requirements:** `pandas`, `snowflake-connector-python`, Athena vocabularies

### Variant Annotation

Annotate genomic variants with clinical databases:
- ClinVar pathogenicity classification
- gnomAD population allele frequencies
- Variant filtering strategies (clinical vs. research)
- ACMG classification support

**Requirements:** `cyvcf2`, `pandas`, `pysam`

### Survival Analysis

Clinical outcomes analysis:
- Kaplan-Meier survival curves
- Log-rank test for group comparisons
- Cox proportional hazards regression
- Publication-ready plots with risk tables

**Requirements:** `lifelines`, `pandas`, `matplotlib`

### Pharmacovigilance

Analyze FDA FAERS data for drug safety:
- Load FAERS quarterly ASCII files to Snowflake
- Calculate PRR, ROR disproportionality metrics
- Signal detection with statistical thresholds
- Drug-event association analysis

**Requirements:** `pandas`, `scipy`, `snowflake-connector-python`

### Claims Data Analysis

Healthcare claims for real-world evidence:
- Medical claims (837P/837I) and pharmacy claims
- Patient cohort building from diagnoses/Rx
- PMPM cost calculations
- Medication adherence (PDC)
- Treatment pattern analysis
- HEDIS-style quality measures

**Requirements:** `pandas`, `numpy`, `snowflake-connector-python`

### Clinical NLP

Extract structured data from clinical text:
- Entity extraction (medications, diagnoses, procedures)
- Section detection (HPI, PMH, Assessment, Plan)
- Negation and context detection
- Vital signs extraction
- Snowflake Cortex LLM integration
- spaCy/scispaCy UDF deployment

**Requirements:** `spacy`, `scispacy`, `medspacy` (optional)

## Contributing

Contributions welcome! To add a new skill:

1. Create a new folder under `skills/`
2. Add `SKILL.md` with proper frontmatter
3. Include `scripts/` and `references/` as needed
4. Submit a pull request

## License

Skills are provided under the Apache License 2.0. See individual skill directories for specific licenses.

## Disclaimer

These skills are provided for educational and research purposes. They do not constitute medical, legal, or regulatory advice. Professional consultation is required for clinical applications.

## Acknowledgments

- Original skills by [Anthropic](https://github.com/anthropics/life-sciences)
- [scverse](https://scverse.org/) ecosystem
- [nf-core](https://nf-co.re/) community
- [OHDSI](https://ohdsi.org/) for OMOP CDM
- [HL7 FHIR](https://hl7.org/fhir/) community
- [FDA FAERS](https://www.fda.gov/drugs/questions-and-answers-fdas-adverse-event-reporting-system-faers/fda-adverse-event-reporting-system-faers-public-dashboard) for pharmacovigilance data
