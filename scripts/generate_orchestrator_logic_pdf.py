#!/usr/bin/env python3
"""Generate a PDF document explaining the orchestrator logic for code owners and reviewers."""

import os
from fpdf import FPDF

OUTPUT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "documentation", "Orchestrator_Logic_Guide.pdf")


class OrchestratorPDF(FPDF):
    def header(self):
        if self.page_no() > 1:
            self.set_font("Helvetica", "I", 8)
            self.set_text_color(100, 100, 100)
            self.cell(0, 10, "Orchestrator Logic Guide -- Health Sciences Industry Solutions Architect", align="C")
            self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    def section_title(self, title):
        self.set_font("Helvetica", "B", 16)
        self.set_text_color(0, 100, 180)
        self.cell(0, 12, title, new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(0, 100, 180)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(4)

    def subsection_title(self, title):
        self.set_font("Helvetica", "B", 12)
        self.set_text_color(50, 50, 50)
        self.cell(0, 10, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def body_text(self, text):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 5.5, text)
        self.ln(2)

    def bold_text(self, text):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 5.5, text)
        self.ln(1)

    def bullet(self, text, indent=10):
        x = self.get_x()
        self.set_x(x + indent)
        self.set_font("Helvetica", "", 10)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 5.5, f"- {text}")
        self.ln(1)

    def sub_bullet(self, text, indent=20):
        x = self.get_x()
        self.set_x(x + indent)
        self.set_font("Helvetica", "", 9)
        self.set_text_color(80, 80, 80)
        self.multi_cell(0, 5, f"- {text}")
        self.ln(0.5)

    def numbered(self, num, text, indent=10):
        x = self.get_x()
        self.set_x(x + indent)
        self.set_font("Helvetica", "", 10)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 5.5, f"{num}. {text}")
        self.ln(1)

    def code_block(self, text):
        self.set_font("Courier", "", 9)
        self.set_fill_color(240, 240, 240)
        self.set_text_color(30, 30, 30)
        for line in text.strip().split("\n"):
            self.cell(0, 5, f"  {line}", new_x="LMARGIN", new_y="NEXT", fill=True)
        self.ln(3)

    def table_header(self, cols, widths):
        self.set_font("Helvetica", "B", 9)
        self.set_fill_color(0, 100, 180)
        self.set_text_color(255, 255, 255)
        for col, w in zip(cols, widths):
            self.cell(w, 7, col, border=1, fill=True, align="C")
        self.ln()
        self.set_text_color(40, 40, 40)

    def table_row(self, cells, widths):
        self.set_font("Helvetica", "", 8)
        self.set_fill_color(250, 250, 250)
        max_h = 0
        cell_texts = []
        for cell, w in zip(cells, widths):
            lines = self.multi_cell(w, 5, cell, split_only=True)
            cell_texts.append(lines)
            h = len(lines) * 5
            if h > max_h:
                max_h = h
        if max_h < 7:
            max_h = 7
        x_start = self.get_x()
        y_start = self.get_y()
        if y_start + max_h > self.h - 20:
            self.add_page()
            y_start = self.get_y()
        for i, (lines, w) in enumerate(zip(cell_texts, widths)):
            self.set_xy(x_start + sum(widths[:i]), y_start)
            self.rect(x_start + sum(widths[:i]), y_start, w, max_h)
            for line in lines:
                self.cell(w, 5, line)
                self.set_xy(x_start + sum(widths[:i]), self.get_y() + 5)
        self.set_xy(x_start, y_start + max_h)


def build_pdf():
    pdf = OrchestratorPDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)

    # -- TITLE PAGE --
    pdf.add_page()
    pdf.ln(40)
    pdf.set_font("Helvetica", "B", 28)
    pdf.set_text_color(0, 100, 180)
    pdf.cell(0, 15, "Orchestrator Logic Guide", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(5)
    pdf.set_font("Helvetica", "", 14)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 10, "Health Sciences Industry Solutions Architect", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(10)
    pdf.set_font("Helvetica", "I", 11)
    pdf.cell(0, 8, "A detailed reference for code owners, reviewers, and contributors.", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(20)
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(120, 120, 120)
    pdf.cell(0, 8, "Repository: Snowflake-Solutions/health-sciences-coco-skills-incubator", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, "Generated from: agents/health-sciences-incubator.md", align="C", new_x="LMARGIN", new_y="NEXT")

    # -- TABLE OF CONTENTS --
    pdf.add_page()
    pdf.section_title("Table of Contents")
    toc = [
        "1. Overview -- What the Orchestrator Does",
        "2. Architecture -- How It's Built",
        "3. Plan-then-Execute Protocol -- The Mandatory Gate",
        "4. Platform Affinities -- Automatic Platform Skill Selection",
        "5. Routing Logic -- How Requests Reach Skills",
        "6. Skill Taxonomy -- The 5-Level Hierarchy",
        "7. Cross-Domain Composition Patterns",
        "8. Cortex Knowledge Extensions (CKEs)",
        "9. Guardrails and Anti-Patterns",
        "10. Generation Pipeline -- Template to Agent",
        "11. QA Validation -- 12-Check Suite",
        "12. File Reference",
    ]
    for item in toc:
        pdf.bullet(item, indent=5)

    # -- 1. OVERVIEW --
    pdf.add_page()
    pdf.section_title("1. Overview -- What the Orchestrator Does")
    pdf.body_text(
        "The orchestrator is a system prompt (Markdown file) that is loaded into Cortex Code "
        "when a user activates the health-sciences-incubator profile. It acts as the \"brain\" "
        "of the Health Sciences Industry Solutions Architect -- receiving natural language requests "
        "from users and composing the right combination of industry skills and Snowflake platform "
        "capabilities into end-to-end solutions."
    )
    pdf.body_text(
        "It does NOT execute code itself. Instead, it:"
    )
    pdf.bullet("Detects the healthcare domain from the user's request (Provider, Pharma, Payer)")
    pdf.bullet("Routes to one or more industry skills using trigger-keyword matching")
    pdf.bullet("Checks platform affinities to add Snowflake platform skills (governance, Streamlit, ML, etc.)")
    pdf.bullet("Builds a numbered solution plan and presents it for user approval")
    pdf.bullet("Executes the approved plan step-by-step, invoking skills via the `skill` tool")
    pdf.bullet("Applies HIPAA governance guardrails as cross-cutting concerns")

    pdf.subsection_title("Key Design Principles")
    pdf.bullet("Composable: Skills are independent building blocks, not monolithic scripts")
    pdf.bullet("Plan-gated: No execution without explicit user approval")
    pdf.bullet("Template-driven: Generated from YAML registry + Jinja2 template (not hand-edited)")
    pdf.bullet("Knowledge-grounded: CKEs provide RAG search over PubMed and ClinicalTrials.gov")
    pdf.bullet("Governance by default: HIPAA guardrails enforced across all workflows")

    # -- 2. ARCHITECTURE --
    pdf.add_page()
    pdf.section_title("2. Architecture -- How It's Built")

    pdf.subsection_title("Three-Layer Architecture")
    pdf.body_text(
        "The system has three layers that work together:"
    )

    pdf.bold_text("Layer 1: YAML Registry (templates/skills_incubator.yaml)")
    pdf.body_text(
        "The single source of truth for all skills. Each skill entry includes: "
        "name, triggers (keywords for routing), description, domain (taxonomy placement), "
        "and optional metadata (sub_skills, cke, used_by, standalone). "
        "The registry also defines cross-domain composition patterns, overlaps between skills, "
        "and profile metadata (name, description, intro text)."
    )

    pdf.bold_text("Layer 2: Jinja2 Template (templates/orchestrator.md.j2)")
    pdf.body_text(
        "A parameterized Markdown template that defines the orchestrator's structure: "
        "Plan-then-Execute protocol, Platform Skill Selection, routing rules, skill taxonomy, "
        "routing tables, cross-domain patterns, guardrails, and anti-patterns. "
        "The template reads from the registry to populate skill-specific sections "
        "(taxonomy tree, routing tables, CKE routing, patterns). "
        "Structural sections (protocol, guardrails, routing rules) are static in the template."
    )

    pdf.bold_text("Layer 3: Generated Agent (agents/health-sciences-incubator.md)")
    pdf.body_text(
        "The output -- a complete Markdown file with YAML frontmatter (name, description, tools) "
        "that Cortex Code loads as a system prompt. This file should NEVER be hand-edited; "
        "all changes go through the registry or template, then regeneration."
    )

    pdf.subsection_title("Generation Flow")
    pdf.code_block(
        "skills_incubator.yaml  --+\n"
        "                         |--> generate_orchestrators.py --> health-sciences-incubator.md\n"
        "orchestrator.md.j2     --+"
    )
    pdf.body_text(
        "Run: python scripts/generate_orchestrators.py --profile incubator"
    )

    pdf.subsection_title("Twin Orchestrator Model")
    pdf.body_text(
        "Two orchestrators are generated from the same template but different registries:"
    )
    w1, w2, w3 = 50, 65, 75
    pdf.table_header(["Property", "Incubator", "Production"], [w1, w2, w3])
    rows = [
        ["Registry", "skills_incubator.yaml", "skills_production.yaml"],
        ["Output", "health-sciences-incubator.md", "health-sciences-solutions.md"],
        ["Skills", "All skills (experimental + mature)", "Only graduated skills"],
        ["Audience", "SEs, SAs, contributors", "Field teams, customers"],
        ["Gate", "Skills land here via PR to main", "Skills graduate via Tiger Team review"],
    ]
    for row in rows:
        pdf.table_row(row, [w1, w2, w3])
    pdf.ln(3)

    # -- 3. PLAN-THEN-EXECUTE --
    pdf.add_page()
    pdf.section_title("3. Plan-then-Execute Protocol")

    pdf.body_text(
        "Every health sciences task follows a mandatory two-phase protocol. "
        "Phase 1 (Plan) MUST complete before Phase 2 (Execute) can begin. This is non-negotiable."
    )

    pdf.subsection_title("Phase 1: Plan (Mandatory Gate)")
    pdf.body_text("The orchestrator performs these steps to build a plan:")
    pdf.numbered(1, "Identify the sub-industry (Provider, Pharma, Payer) from the Routing Rules")
    pdf.numbered(2, "Route by task if sub-industry is ambiguous")
    pdf.numbered(3, "Scan the Skill Routing Tables for trigger keyword matches")
    pdf.numbered(4, "Check Cross-Domain Patterns if the request spans multiple business functions")
    pdf.numbered(5, "Check Platform Affinities for each skill in the plan -- add platform skills where conditions match")
    pdf.numbered(6, "Build a numbered solution plan. Each step specifies: skill name, what it produces, dependencies, and governance applicability")
    pdf.numbered(7, "Present the plan to the user via ask_user_question and wait for explicit approval")

    pdf.subsection_title("Phase 2: Execute (Only After Approval)")
    pdf.numbered(1, "Execute each step in the approved plan order")
    pdf.numbered(2, "Invoke skills using the skill tool -- never bypass skills with raw SQL/Bash")
    pdf.numbered(3, "Run preflight checks (CKEs, Data Model Knowledge auto-detect availability)")
    pdf.numbered(4, "Apply governance guardrails on all patient/clinical data")
    pdf.numbered(5, "Enrich with CKEs when the plan calls for evidence grounding")
    pdf.numbered(6, "Report back after each major step for user course-correction")
    pdf.numbered(7, "Test and validate before declaring success")

    pdf.subsection_title("When the Gate Is Lightweight")
    pdf.body_text("The plan gate can be a single sentence + confirmation for:")
    pdf.bullet("Simple single-skill queries (e.g., 'What adverse events are associated with aspirin?')")
    pdf.bullet("Informational questions (e.g., 'What skills are available for genomics?')")
    pdf.bullet("Follow-up steps within an already-approved plan")
    pdf.body_text(
        "For everything else -- multi-step pipelines, cross-domain composition, "
        "anything touching patient data -- the full plan gate is mandatory."
    )

    pdf.subsection_title("Example: Plan Gate in Action")
    pdf.code_block(
        "User: Design a Phase III trial for a GLP-1 receptor agonist for T2D\n"
        "\n"
        "Orchestrator builds plan:\n"
        "  1. $hcls-cross-cke-clinical-trials -> search for STEP trial references\n"
        "  2. $hcls-cross-cke-pubmed -> review STEP trial publications\n"
        "  3. $hcls-pharma-genomics-survival-analysis -> power analysis for endpoints\n"
        "  4. $hcls-pharma-dsafety-clinical-trial-protocol -> generate protocol\n"
        "  5. cortex-ai-functions -> AI_COMPLETE for narrative generation (affinity)\n"
        "\n"
        "Orchestrator presents plan via ask_user_question -> user approves -> execute"
    )

    # -- 4. PLATFORM AFFINITIES --
    pdf.add_page()
    pdf.section_title("4. Platform Affinities")

    pdf.body_text(
        "Platform affinities are a declarative mechanism for industry skills to declare "
        "which Snowflake platform skills enhance them and under what conditions. "
        "During Phase 1 (Plan), the orchestrator reads each skill's affinities "
        "and evaluates conditions against the user's request to automatically "
        "sequence platform skills into the solution plan."
    )

    pdf.subsection_title("How It Works")
    pdf.body_text("Each SKILL.md declares platform_affinities in its YAML frontmatter:")
    pdf.code_block(
        "---\n"
        "name: hcls-provider-cdata-fhir\n"
        "platform_affinities:\n"
        "  produces: [tables, views, stages]\n"
        "  benefits_from:\n"
        "    - skill: dynamic-tables\n"
        "      when: \"incremental refresh or ongoing FHIR feeds\"\n"
        "    - skill: data-governance\n"
        "      when: \"FHIR tables contain PHI\"\n"
        "    - skill: developing-with-streamlit\n"
        "      when: \"user wants a patient data dashboard\"\n"
        "---"
    )

    pdf.subsection_title("Affinity Evaluation Algorithm")
    pdf.numbered(1, "For each domain skill in the plan, read its platform_affinities from SKILL.md frontmatter")
    pdf.numbered(2, "For each benefits_from entry, evaluate the 'when' condition against the user's request")
    pdf.numbered(3, "If the condition matches, add that platform skill as a follow-on step in the plan")
    pdf.numbered(4, "Deduplicate: if multiple skills trigger the same platform skill, include it once")

    pdf.subsection_title("produces vs benefits_from")
    w1, w2, w3 = 35, 65, 90
    pdf.table_header(["Field", "Purpose", "Example"], [w1, w2, w3])
    pdf.table_row(["produces", "Snowflake objects this skill creates", "tables, views, stages, cortex_search_service"], [w1, w2, w3])
    pdf.table_row(["benefits_from", "Platform skills that enhance this skill + conditions", "data-governance when 'PHI present'"], [w1, w2, w3])
    pdf.ln(3)

    pdf.subsection_title("10 Platform Skills Available")
    w1, w2 = 55, 135
    pdf.table_header(["Platform Skill", "When to Include"], [w1, w2])
    platform_skills = [
        ["dynamic-tables", "Incremental refresh, ongoing data feeds, streaming pipelines"],
        ["data-governance", "PHI/PII present, masking policies, row-access policies, audit"],
        ["data-quality", "Data validation, conformance checks, completeness monitoring"],
        ["semantic-view", "Natural language queries, analytics layer, BI integration"],
        ["developing-with-streamlit", "Dashboards, viewers, interactive UIs"],
        ["deploy-to-spcs", "Container services, GPU compute, custom viewers"],
        ["machine-learning", "Model training, registry, deployment, inference"],
        ["cortex-ai-functions", "AI_PARSE_DOCUMENT, AI_COMPLETE, AI_EXTRACT, text analytics"],
        ["cortex-agent", "Conversational agents over domain data"],
        ["search-optimization", "Full-text or semantic search over extracted content"],
    ]
    for row in platform_skills:
        pdf.table_row(row, [w1, w2])
    pdf.ln(3)

    pdf.subsection_title("Worked Example")
    pdf.body_text('User: "Build a FHIR data pipeline with a patient dashboard and PHI masking"')
    pdf.numbered(1, "$hcls-provider-cdata-fhir selected (triggers: FHIR, HL7, Patient resource)")
    pdf.numbered(2, "Read affinities: produces=[tables, views, stages]")
    pdf.numbered(3, "Evaluate: dynamic-tables when 'incremental refresh' -> YES (pipeline = ongoing feeds)")
    pdf.numbered(4, "Evaluate: data-governance when 'PHI present' -> YES (user said PHI masking)")
    pdf.numbered(5, "Evaluate: developing-with-streamlit when 'dashboard' -> YES")
    pdf.numbered(6, "Final plan: FHIR ingest -> Dynamic Tables -> Data Governance -> Streamlit dashboard")

    # -- 5. ROUTING LOGIC --
    pdf.add_page()
    pdf.section_title("5. Routing Logic")

    pdf.body_text(
        "The orchestrator uses a four-step routing algorithm to determine which skills to invoke. "
        "Routing happens during Phase 1 (Plan) and determines the skill composition."
    )

    pdf.subsection_title("Step 1: Route by Sub-Industry")
    pdf.body_text("Determine the customer/context type:")
    w1, w2, w3 = 60, 45, 85
    pdf.table_header(["Customer Type", "Sub-Industry", "Examples"], [w1, w2, w3])
    pdf.table_row(["Hospital, health system, clinic, IDN", "Provider", "Epic, Cerner, clinical research orgs"], [w1, w2, w3])
    pdf.table_row(["Pharma, biotech, CRO", "Pharma", "Drug development, trials, genomics"], [w1, w2, w3])
    pdf.table_row(["Health plan, TPA, PBM", "Payer", "Claims adjudication, member analytics"], [w1, w2, w3])
    pdf.ln(3)

    pdf.subsection_title("Step 2: Route by Task (Disambiguation)")
    pdf.body_text(
        "When the customer straddles sub-industries (e.g., CRO doing hospital-based trials), "
        "route by the TASK being performed, not the customer type."
    )
    w1, w2, w3 = 55, 70, 65
    pdf.table_header(["Task Type", "Route To", "Regardless Of"], [w1, w2, w3])
    pdf.table_row(["Clinical data / EHR", "Provider > Clinical Data Mgmt", "Customer type"], [w1, w2, w3])
    pdf.table_row(["Drug safety / adverse events", "Pharma > Drug Safety", "Customer type"], [w1, w2, w3])
    pdf.table_row(["Imaging workflows", "Provider > Clinical Research", "Customer type"], [w1, w2, w3])
    pdf.table_row(["Genomic analysis", "Pharma > Genomics", "Customer type"], [w1, w2, w3])
    pdf.table_row(["Claims analysis", "Provider > Revenue Cycle", "Until Payer skills exist"], [w1, w2, w3])
    pdf.ln(3)

    pdf.subsection_title("Step 3: Cross-Industry Skills")
    pdf.body_text(
        "These skills are available to ALL sub-industries. The orchestrator invokes them "
        "whenever they add value, regardless of routing path:"
    )
    pdf.bullet("hcls-cross-research-problem-selection -- scientific problem validation")
    pdf.bullet("hcls-cross-skill-development -- contributor workflow to add new skills")
    pdf.bullet("hcls-cross-cke-pubmed -- PubMed biomedical literature search")
    pdf.bullet("hcls-cross-cke-clinical-trials -- ClinicalTrials.gov registry search")

    pdf.subsection_title("Step 4: Accept Overlaps")
    pdf.body_text(
        "Some skills serve multiple sub-industries. The orchestrator routes to them "
        "regardless of which tree they sit in:"
    )
    pdf.bullet("claims-data-analysis -- Provider (revenue cycle) + Payer (claims processing)")
    pdf.bullet("survival-analysis -- Pharma (clinical outcomes) + Provider (clinical research)")
    pdf.bullet("clinical-nlp -- Provider (EHR extraction) + Pharma (safety narrative mining)")
    pdf.bullet("clinical-docs -- Provider (document intelligence) + Pharma (safety narrative extraction)")

    pdf.subsection_title("Skill-First Rule")
    pdf.body_text(
        "Always check skills before using raw tools. If a matching skill exists, invoke it "
        "as the FIRST action. Skills encode domain expertise, gated workflows, guardrails, "
        "and best practices that raw tool usage (SQL, Bash) does not provide."
    )

    # -- 6. SKILL TAXONOMY --
    pdf.add_page()
    pdf.section_title("6. Skill Taxonomy")

    pdf.body_text("Skills are organized in a five-level hierarchy:")
    pdf.code_block("Industry / Sub-Industry / Business Function / Use Case Skill / Sub-Skill")

    pdf.body_text("The naming convention encodes this hierarchy in a flat directory structure:")
    pdf.code_block("hcls-{sub-industry}-{function}-{skill}")

    pdf.subsection_title("Skill Types")
    w1, w2, w3 = 35, 75, 80
    pdf.table_header(["Type", "Description", "Example"], [w1, w2, w3])
    pdf.table_row(["Router", "Detects intent, routes to sub-skills. Has setup, preflight, workflow.", "hcls-provider-imaging (7 sub-skills)"], [w1, w2, w3])
    pdf.table_row(["Sub-skill", "Handles one task within a router. Loaded by router, not user.", "dicom-parser, clinical-docs-search"], [w1, w2, w3])
    pdf.table_row(["Standalone", "Self-contained, no router or sub-skills.", "hcls-provider-cdata-fhir"], [w1, w2, w3])
    pdf.ln(3)

    pdf.subsection_title("Full Taxonomy Tree")
    pdf.code_block(
        "Health Sciences\n"
        "|-- Provider\n"
        "|   |-- Clinical Research\n"
        "|   |   |-- hcls-provider-imaging (router + 7 sub-skills)\n"
        "|   |   +-- hcls-provider-imaging-dicom-parser (standalone)\n"
        "|   |-- Clinical Data Management\n"
        "|   |   |-- hcls-provider-cdata-fhir\n"
        "|   |   |-- hcls-provider-cdata-clinical-nlp\n"
        "|   |   |-- hcls-provider-cdata-omop\n"
        "|   |   +-- hcls-provider-cdata-clinical-docs (router + 5 sub-skills)\n"
        "|   +-- Revenue Cycle\n"
        "|       +-- hcls-provider-claims-data-analysis\n"
        "|\n"
        "|-- Pharma\n"
        "|   |-- Drug Safety\n"
        "|   |   |-- hcls-pharma-dsafety-pharmacovigilance\n"
        "|   |   +-- hcls-pharma-dsafety-clinical-trial-protocol\n"
        "|   |-- Genomics\n"
        "|   |   |-- hcls-pharma-genomics-nextflow\n"
        "|   |   |-- hcls-pharma-genomics-variant-annotation\n"
        "|   |   |-- hcls-pharma-genomics-single-cell-qc\n"
        "|   |   |-- hcls-pharma-genomics-scvi-tools\n"
        "|   |   +-- hcls-pharma-genomics-survival-analysis\n"
        "|   +-- Lab Operations\n"
        "|       +-- hcls-pharma-lab-allotrope\n"
        "|\n"
        "+-- Cross-Industry\n"
        "    |-- Research Strategy: hcls-cross-research-problem-selection\n"
        "    |-- Skill Development: hcls-cross-skill-development\n"
        "    +-- Knowledge Extensions: cke-pubmed, cke-clinical-trials"
    )

    pdf.subsection_title("Domain Mapping (Registry -> Orchestrator)")
    pdf.body_text(
        "Each skill in skills_incubator.yaml has a 'domain' field that maps to a section "
        "in the generated orchestrator. The DOMAIN_ORDER list in generate_orchestrators.py "
        "controls the section ordering:"
    )
    domains = [
        "Provider > Clinical Research",
        "Provider > Clinical Data Management",
        "Provider > Revenue Cycle",
        "Pharma > Drug Safety",
        "Pharma > Genomics",
        "Pharma > Lab Operations",
        "Cross-Industry > Research Strategy",
        "Cross-Industry > Skill Development",
        "Cross-Industry > Knowledge Extensions",
    ]
    for d in domains:
        pdf.bullet(d)

    # -- 7. CROSS-DOMAIN PATTERNS --
    pdf.add_page()
    pdf.section_title("7. Cross-Domain Composition Patterns")

    pdf.body_text(
        "When a user's request spans multiple business functions, the orchestrator composes "
        "skills using predefined patterns. Patterns are guides, not rigid scripts -- the "
        "orchestrator adapts them to the user's actual request."
    )

    patterns = [
        ("Imaging + Clinical Integration",
         "DICOM parse -> FHIR ingest -> Clinical NLP -> PubMed enrichment -> UI"),
        ("Clinical Data Warehouse (OMOP)",
         "FHIR ingest -> OMOP CDM transform -> HIPAA governance -> Semantic views"),
        ("Drug Safety Signal Detection",
         "FAERS analysis -> PubMed literature -> Clinical NLP -> Claims correlation"),
        ("Genomics + Clinical Outcomes",
         "nf-core pipeline -> Variant annotation -> Survival analysis -> ML models"),
        ("Single-Cell Analysis Pipeline",
         "scRNA-seq QC -> scvi-tools integration -> ML Registry"),
        ("Real-World Evidence Study",
         "Claims cohort -> ClinicalTrials.gov -> OMOP -> Survival -> PubMed validation"),
        ("Clinical Trial Design",
         "Problem validation -> Trial search -> Literature -> Protocol -> Power analysis"),
        ("Lab Data Modernization",
         "Allotrope conversion -> Dynamic Tables -> Analytics dashboard"),
        ("Clinical Data Application (React)",
         "Domain skills -> React/Next.js app -> SPCS deployment -> PHI masking"),
        ("Clinical Document Intelligence",
         "Clinical docs extraction -> NLP enrichment -> PubMed -> FHIR -> Governance -> Semantic views"),
    ]
    w1, w2 = 55, 135
    pdf.table_header(["Pattern", "Skill Chain"], [w1, w2])
    for name, chain in patterns:
        pdf.table_row([name, chain], [w1, w2])
    pdf.ln(3)

    pdf.subsection_title("Adapting Patterns")
    pdf.bullet("Skip steps that don't apply")
    pdf.bullet("Reorder when the user already has intermediate outputs")
    pdf.bullet("Combine patterns when the request spans multiple")
    pdf.bullet("Add steps for capabilities not in the pattern (e.g., governance)")
    pdf.bullet("Always ask if the adaptation is unclear")

    # -- 8. CKEs --
    pdf.add_page()
    pdf.section_title("8. Cortex Knowledge Extensions (CKEs)")

    pdf.body_text(
        "CKEs are standalone composable skills backed by Cortex Search Services from "
        "Snowflake Marketplace. They provide on-demand RAG search over external knowledge "
        "corpora. Domain skills invoke them when evidence grounding adds value."
    )

    pdf.subsection_title("Available CKEs")
    w1, w2, w3 = 45, 50, 95
    pdf.table_header(["CKE Skill", "Data Source", "Used By"], [w1, w2, w3])
    pdf.table_row(
        ["hcls-cross-cke-pubmed", "PubMed biomedical literature",
         "pharmacovigilance, clinical-nlp, research-problem-selection, imaging (analytics), clinical-docs"],
        [w1, w2, w3]
    )
    pdf.table_row(
        ["hcls-cross-cke-clinical-trials", "ClinicalTrials.gov registry",
         "clinical-trial-protocol, claims-data-analysis, survival-analysis"],
        [w1, w2, w3]
    )
    pdf.ln(3)

    pdf.subsection_title("Preflight Pattern")
    pdf.body_text(
        "Before invoking any CKE, the skill runs a probe query to verify the Marketplace "
        "listing is installed. If MISSING, the skill skips CKE enrichment gracefully and "
        "continues with its primary task. This ensures skills work without CKEs but provide "
        "richer results with them."
    )

    pdf.subsection_title("Data Model Knowledge (Internal CKE)")
    pdf.body_text(
        "In addition to external CKEs, two router skills (imaging, clinical-docs) use "
        "internal CKE layers -- Cortex Search Services over their own data models. "
        "These auto-fire as a pre-step (Step 0) to ground DDL generation, extraction config, "
        "and schema queries in live reference models."
    )
    w1, w2, w3 = 55, 55, 80
    pdf.table_header(["Router", "Search Service", "What It Answers"], [w1, w2, w3])
    pdf.table_row(["hcls-provider-imaging", "DICOM_MODEL_SEARCH_SVC", "Table definitions, column types, DICOM tags, PHI indicators"], [w1, w2, w3])
    pdf.table_row(["hcls-provider-cdata-clinical-docs", "CLINICAL_DOCS_MODEL_SEARCH_SVC + CLINICAL_DOCS_SPECS_SEARCH_SVC", "Schema + doc type specs, extraction prompts, field definitions"], [w1, w2, w3])
    pdf.ln(3)

    # -- 9. GUARDRAILS --
    pdf.add_page()
    pdf.section_title("9. Guardrails and Anti-Patterns")

    pdf.subsection_title("HIPAA Guardrails (Always Enforced)")
    pdf.bullet("Always apply HIPAA governance before exposing any patient data")
    pdf.bullet("Never store or display PHI without masking policies in place")
    pdf.bullet("Always use IS_ROLE_IN_SESSION() (not CURRENT_ROLE()) in masking/row-access policies")
    pdf.bullet("Always recommend audit trails via ACCESS_HISTORY for PHI-containing tables")
    pdf.bullet("Prefer de-identified datasets for analytics and ML training")
    pdf.bullet("Always validate FHIR/HL7/OMOP data quality before building downstream tables")
    pdf.bullet("For genomic data: ensure proper consent tracking and data use agreements")
    pdf.bullet("For FAERS/pharmacovigilance: note limitations of spontaneous reporting data")

    pdf.subsection_title("Anti-Patterns (Do NOT)")
    pdf.bullet("Do NOT use clinical-nlp on raw files (PDF, DOCX, images) -- use clinical-docs first")
    pdf.bullet("Do NOT use survival-analysis without a defined cohort -- build the cohort first")
    pdf.bullet("Do NOT invoke CKEs for non-evidence tasks (pipeline construction, SQL generation)")
    pdf.bullet("Do NOT skip preflight checks -- they run automatically")
    pdf.bullet("Do NOT force-follow a pattern when the request only partially matches -- adapt it")
    pdf.bullet("Do NOT use imaging-dicom-parser (standalone) for full imaging workflows -- use the router")
    pdf.bullet("Do NOT bypass the plan gate for multi-step pipelines or patient data workflows")

    # -- 10. GENERATION PIPELINE --
    pdf.add_page()
    pdf.section_title("10. Generation Pipeline")

    pdf.body_text(
        "The orchestrator is never hand-edited. All changes flow through a generation pipeline:"
    )

    pdf.subsection_title("Pipeline Steps")
    pdf.numbered(1, "Edit the YAML registry (templates/skills_incubator.yaml) to add/modify skills, patterns, overlaps, or CKE metadata")
    pdf.numbered(2, "Edit the Jinja2 template (templates/orchestrator.md.j2) to change structural sections (protocol, guardrails, routing rules)")
    pdf.numbered(3, "Run: python scripts/generate_orchestrators.py --profile incubator")
    pdf.numbered(4, "The script loads the YAML, builds SkillObj instances, groups by domain, filters patterns, and renders the template")
    pdf.numbered(5, "Output is written to agents/health-sciences-incubator.md")
    pdf.numbered(6, "If --profile both, a drift check compares the two orchestrators and flags unexpected structural differences")

    pdf.subsection_title("What Goes Where")
    w1, w2 = 95, 95
    pdf.table_header(["Change Type", "Edit In"], [w1, w2])
    pdf.table_row(["Add a new skill", "skills_incubator.yaml (skills section)"], [w1, w2])
    pdf.table_row(["Add triggers or description", "skills_incubator.yaml (skill entry)"], [w1, w2])
    pdf.table_row(["Add a cross-domain pattern", "skills_incubator.yaml (patterns section)"], [w1, w2])
    pdf.table_row(["Add an overlap", "skills_incubator.yaml (overlaps section)"], [w1, w2])
    pdf.table_row(["Change routing rules", "orchestrator.md.j2 (Routing Rules section)"], [w1, w2])
    pdf.table_row(["Change Plan-then-Execute protocol", "orchestrator.md.j2 (protocol section)"], [w1, w2])
    pdf.table_row(["Change guardrails or anti-patterns", "orchestrator.md.j2 (guardrails section)"], [w1, w2])
    pdf.table_row(["Add platform affinities", "SKILL.md frontmatter (in skill directory)"], [w1, w2])
    pdf.table_row(["Change Platform Skill Selection logic", "orchestrator.md.j2 (Platform section)"], [w1, w2])
    pdf.ln(3)

    pdf.subsection_title("Key Code: generate_orchestrators.py")
    pdf.body_text(
        "The generator script is ~190 lines of Python. Key components:"
    )
    pdf.bullet("SkillObj class: wraps each skill's YAML data (name, triggers, description, domain, sub_skills, cke, etc.)")
    pdf.bullet("build_skills(): creates OrderedDict of SkillObj from registry")
    pdf.bullet("build_skills_by_domain(): groups skills by domain in DOMAIN_ORDER sequence")
    pdf.bullet("filter_patterns(): removes patterns with unavailable skills (for production)")
    pdf.bullet("render(): loads Jinja2 template, passes all data, returns rendered Markdown")
    pdf.bullet("Drift check: after generating both profiles, compares line-by-line and flags unexpected structural differences beyond skill refs and profile metadata")

    # -- 11. QA VALIDATION --
    pdf.add_page()
    pdf.section_title("11. QA Validation -- 12-Check Suite")

    pdf.body_text(
        "The QA script (scripts/qa_validate_orchestrator.py) validates the orchestrator "
        "against the filesystem and registry. Run it before every commit:"
    )
    pdf.code_block("python scripts/qa_validate_orchestrator.py")

    w1, w2, w3 = 20, 55, 115
    pdf.table_header(["Check", "Name", "What It Validates"], [w1, w2, w3])
    checks = [
        ["1", "$refs -> SKILL.md name", "Every $ref in orchestrator points to a SKILL.md with matching name"],
        ["2", "SKILL.md -> orchestrator ref", "Every top-level SKILL.md is referenced in the orchestrator"],
        ["3", "Folder name == SKILL.md name", "Directory name matches the name: field in SKILL.md frontmatter"],
        ["4", "Imaging sub-skills", "All imaging sub-skill directories exist and are referenced"],
        ["5", "Taxonomy tree entries", "Skills in the taxonomy tree exist in the filesystem"],
        ["6", "Reference consistency", "Counts $ref occurrences per skill (informational)"],
        ["7", "Standalone skills", "Standalone skills (e.g., dicom-parser) exist and are referenced"],
        ["8", "Twin drift", "Structural differences between incubator and production orchestrators"],
        ["9", "Registry bidirectional", "Every registry entry has a directory and vice versa"],
        ["10", "Platform affinities", "All SKILL.md files have valid platform_affinities frontmatter"],
        ["11", "CKE used_by", "CKE used_by references point to real skills"],
        ["12", "Overlap entries", "Overlap skills exist and are referenced in orchestrator"],
    ]
    for c in checks:
        pdf.table_row(c, [w1, w2, w3])
    pdf.ln(3)

    pdf.body_text(
        "Expected results: All checks pass except CHECK 8 (twin drift) which shows expected "
        "structural differences because the production orchestrator is still a scaffold with "
        "no graduated skills."
    )

    # -- 12. FILE REFERENCE --
    pdf.add_page()
    pdf.section_title("12. File Reference")

    w1, w2 = 85, 105
    pdf.table_header(["File", "Purpose"], [w1, w2])
    files = [
        ["agents/health-sciences-incubator.md", "Generated orchestrator (system prompt) -- NEVER hand-edit"],
        ["agents/health-sciences-solutions.md", "Generated production orchestrator -- NEVER hand-edit"],
        ["templates/orchestrator.md.j2", "Jinja2 template for orchestrator structure"],
        ["templates/skills_incubator.yaml", "YAML registry -- source of truth for all skills"],
        ["templates/skills_production.yaml", "YAML registry -- production skills only"],
        ["scripts/generate_orchestrators.py", "Generation script: YAML + Jinja2 -> .md"],
        ["scripts/qa_validate_orchestrator.py", "12-check QA validation suite"],
        ["skills/hcls-*/SKILL.md", "Industry skill definitions with platform_affinities"],
    ]
    for f in files:
        pdf.table_row(f, [w1, w2])
    pdf.ln(5)

    pdf.bold_text("End of Document")

    pdf.output(OUTPUT_PATH)
    print(f"Generated: {OUTPUT_PATH}")


if __name__ == "__main__":
    build_pdf()
