#!/usr/bin/env python3
from fpdf import FPDF
import os

OUTPUT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "documentation", "HCLS_Skill_Development_Playbook.pdf")

BLUE = (0, 100, 180)
DARK = (40, 40, 40)
GRAY = (100, 100, 100)
LIGHT_BLUE_BG = (240, 245, 250)
TIP_BG = (255, 248, 230)
WARN_BG = (255, 240, 240)
GREEN = (0, 130, 60)


class PlaybookPDF(FPDF):
    def header(self):
        if self.page_no() > 1:
            self.set_font("Helvetica", "I", 8)
            self.set_text_color(*GRAY)
            self.cell(0, 10, "HCLS Skill Development Playbook", align="C")
            self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    def section_title(self, title):
        self.set_font("Helvetica", "B", 16)
        self.set_text_color(*BLUE)
        self.cell(0, 12, title, new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(*BLUE)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(4)

    def subsection_title(self, title):
        self.set_font("Helvetica", "B", 12)
        self.set_text_color(50, 50, 50)
        self.cell(0, 10, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def step_header(self, number, title, time_est=None):
        self.set_font("Helvetica", "B", 13)
        self.set_text_color(*BLUE)
        self.cell(0, 10, f"Step {number}: {title}", new_x="LMARGIN", new_y="NEXT")
        if time_est:
            self.set_font("Helvetica", "I", 9)
            self.set_text_color(*GRAY)
            self.cell(0, 5, f"Estimated time: {time_est}", new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def body_text(self, text):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(*DARK)
        self.multi_cell(0, 5.5, text)
        self.ln(2)

    def bold_text(self, text):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(*DARK)
        self.multi_cell(0, 5.5, text)
        self.ln(1)

    def bullet(self, text, indent=10):
        x = self.get_x()
        self.set_font("Helvetica", "", 10)
        self.set_text_color(*DARK)
        self.set_x(x + indent)
        self.cell(5, 5.5, "-")
        self.multi_cell(0, 5.5, text)
        self.ln(0.5)

    def sub_bullet(self, text, indent=20):
        x = self.get_x()
        self.set_font("Helvetica", "", 9.5)
        self.set_text_color(80, 80, 80)
        self.set_x(x + indent)
        self.cell(5, 5, " ")
        self.multi_cell(0, 5, text)
        self.ln(0.5)

    def numbered_item(self, num, text, indent=10):
        x = self.get_x()
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(*BLUE)
        self.set_x(x + indent)
        self.cell(8, 5.5, f"{num}.")
        self.set_font("Helvetica", "", 10)
        self.set_text_color(*DARK)
        self.multi_cell(0, 5.5, text)
        self.ln(0.5)

    def code_block(self, lines):
        self.set_fill_color(245, 245, 245)
        self.set_draw_color(200, 200, 200)
        x = self.get_x() + 5
        y = self.get_y()
        block_height = len(lines) * 5 + 6
        self.rect(x, y, self.w - self.r_margin - x, block_height, "DF")
        self.set_font("Courier", "", 9)
        self.set_text_color(50, 50, 50)
        self.ln(3)
        for line in lines:
            self.set_x(x + 4)
            self.cell(0, 5, line, new_x="LMARGIN", new_y="NEXT")
        self.ln(4)

    def tip_box(self, text):
        self.set_fill_color(*TIP_BG)
        self.set_draw_color(220, 180, 50)
        x = self.get_x() + 5
        y = self.get_y()
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(150, 120, 0)
        w = self.w - self.r_margin - x
        lines = self.multi_cell(w - 8, 5, f"TIP: {text}", dry_run=True, output="LINES")
        h = len(lines) * 5 + 6
        self.rect(x, y, w, h, "DF")
        self.set_xy(x + 4, y + 3)
        self.multi_cell(w - 8, 5, f"TIP: {text}")
        self.set_xy(self.l_margin, y + h + 3)

    def warning_box(self, text):
        self.set_fill_color(*WARN_BG)
        self.set_draw_color(220, 100, 100)
        x = self.get_x() + 5
        y = self.get_y()
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(180, 50, 50)
        w = self.w - self.r_margin - x
        lines = self.multi_cell(w - 8, 5, f"IMPORTANT: {text}", dry_run=True, output="LINES")
        h = len(lines) * 5 + 6
        self.rect(x, y, w, h, "DF")
        self.set_xy(x + 4, y + 3)
        self.multi_cell(w - 8, 5, f"IMPORTANT: {text}")
        self.set_xy(self.l_margin, y + h + 3)

    def table_header(self, cols, widths):
        self.set_font("Helvetica", "B", 9)
        self.set_fill_color(*BLUE)
        self.set_text_color(255, 255, 255)
        for i, col in enumerate(cols):
            self.cell(widths[i], 7, col, border=1, fill=True, align="C")
        self.ln()

    def table_row(self, cols, widths, fill=False):
        self.set_font("Helvetica", "", 8.5)
        self.set_text_color(*DARK)
        if fill:
            self.set_fill_color(*LIGHT_BLUE_BG)
        else:
            self.set_fill_color(255, 255, 255)
        max_lines = 1
        cell_texts = []
        for i, col in enumerate(cols):
            lines = self.multi_cell(widths[i], 5, col, dry_run=True, output="LINES")
            cell_texts.append(lines)
            max_lines = max(max_lines, len(lines))
        row_height = max_lines * 5
        y_start = self.get_y()
        x_start = self.get_x()
        for i, lines in enumerate(cell_texts):
            self.set_xy(x_start + sum(widths[:i]), y_start)
            self.cell(widths[i], row_height, "", border=1, fill=fill)
            for j, line in enumerate(lines):
                self.set_xy(x_start + sum(widths[:i]) + 1, y_start + j * 5)
                self.cell(widths[i] - 2, 5, line)
        self.set_xy(x_start, y_start + row_height)

    def check_item(self, text):
        x = self.get_x()
        self.set_font("Helvetica", "", 10)
        self.set_text_color(*DARK)
        self.set_x(x + 10)
        self.set_draw_color(150, 150, 150)
        self.rect(self.get_x(), self.get_y() + 1, 3.5, 3.5)
        self.set_x(self.get_x() + 6)
        self.multi_cell(0, 5.5, text)
        self.ln(0.5)


def build_pdf():
    pdf = PlaybookPDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)

    # ==================== TITLE PAGE ====================
    pdf.add_page()
    pdf.ln(40)
    pdf.set_font("Helvetica", "B", 30)
    pdf.set_text_color(*BLUE)
    pdf.cell(0, 15, "HCLS Skill Development", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 15, "Playbook", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(5)
    pdf.set_font("Helvetica", "", 16)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 10, "How to Create & Update Industry Skills", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 10, "in 5 Steps", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(10)
    pdf.set_draw_color(*BLUE)
    pdf.line(60, pdf.get_y(), pdf.w - 60, pdf.get_y())
    pdf.ln(10)
    pdf.set_font("Helvetica", "", 12)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 8, "Health Sciences Industry Solutions", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(15)
    pdf.set_font("Helvetica", "I", 10)
    pdf.cell(0, 8, "March 2026", align="C", new_x="LMARGIN", new_y="NEXT")

    # ==================== PAGE 2: OVERVIEW ====================
    pdf.add_page()
    pdf.section_title("What This Playbook Covers")
    pdf.body_text(
        "This playbook gives you the fastest path to creating or updating an HCLS industry skill "
        "for Cortex Code. It distills the full HCLS_INDUSTRY_SKILL_BEST_PRACTICES.md into "
        "actionable steps using the incubator profile and Cortex Code (CoCo) as your development environment."
    )
    pdf.ln(2)

    pdf.subsection_title("Two Tracks")
    w = [93, 93]
    pdf.table_header(["Track A: Create a New Skill", "Track B: Update an Existing Skill"], w)
    pdf.table_row([
        "5 steps to go from zero to\na working skill in the incubator",
        "5 steps to safely modify,\ntest, validate, and ship an update"
    ], w, fill=True)
    pdf.ln(6)

    pdf.subsection_title("Prerequisites")
    pdf.bullet("Git access to Snowflake-Solutions/health-sciences-coco-skills-incubator")
    pdf.bullet("Cortex Code installed with incubator profile:")
    pdf.sub_bullet("cortex profile add health-sciences-incubator")
    pdf.bullet("Snowflake account with ACCOUNTADMIN or appropriate role")
    pdf.bullet("Familiarity with your domain (the skill teaches Cortex Code what YOU know)")
    pdf.ln(4)

    pdf.subsection_title("Choose Your Archetype")
    pdf.body_text(
        "Before starting, identify which pattern fits your skill. Note: many skills start as Standalone "
        "and evolve into Router + Sub-Skills as the domain is hardened and expanded."
    )
    w_arch = [35, 55, 95]
    pdf.table_header(["Archetype", "When to Use", "Examples"], w_arch)
    arch_rows = [
        ["Standalone", "Initial version of\na new domain skill,\nor truly single-\nworkflow skills", "FHIR (v1), Pharmacovigilance (v1),\nVariant Annotation, OMOP,\nSurvival Analysis"],
        ["Router +\nSub-Skills", "Domain has (or\nwill grow to)\nmultiple distinct\nworkflows", "Imaging (7 sub-skills),\nClinical NLP (15 sub-skills),\nPharmacov. (future: signal + dashboard)"],
        ["CKE", "Wrapping a\nMarketplace shared\nCortex Search", "PubMed CKE,\nClinicalTrials.gov CKE"],
        ["Compute-\nHeavy", "External compute\n(containers, GPU,\npipelines)", "Nextflow nf-core,\nscvi-tools, Imaging ML"],
    ]
    for i, row in enumerate(arch_rows):
        pdf.table_row(row, w_arch, fill=(i % 2 == 0))
    pdf.ln(4)
    pdf.tip_box(
        "Start Standalone, evolve to Router. Clinical NLP started as a single SKILL.md and grew to "
        "15 sub-skills through iterative hardening. Plan for growth from Day 1."
    )

    # ==================== TRACK A: CREATE NEW SKILL ====================
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 20)
    pdf.set_text_color(*BLUE)
    pdf.cell(0, 15, "Track A: Create a New Skill", new_x="LMARGIN", new_y="NEXT")
    pdf.set_draw_color(*BLUE)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
    pdf.ln(6)

    # --- Step 1: Name & Scaffold ---
    pdf.step_header(1, "Name & Scaffold")
    pdf.body_text("Pick your skill name using the HCLS naming convention:")
    pdf.code_block([
        "hcls-{segment}-{domain}-{capability}",
        "",
        "Segments:  provider | pharma | payer | meddev | cross",
        "Domains:   cdata | imaging | genomics | dsafety | lab |",
        "           claims | risk | quality | regulatory |",
        "           postmarket | engineering | cke",
    ])
    pdf.body_text("Create the skill directory and bootstrap your SKILL.md:")
    pdf.code_block([
        "cd health-sciences-coco-skills-incubator",
        "git checkout -b feature/hcls-{your-name}",
        "mkdir -p skills/hcls-{your-name}",
        "touch skills/hcls-{your-name}/SKILL.md",
    ])
    pdf.tip_box(
        "Use the hcls-cross-skill-development skill in CoCo to auto-scaffold: "
        'cortex> "create a new HCLS skill for pharmacovigilance"'
    )

    # --- Step 2: Build Your Skill with CoCo ---
    pdf.add_page()
    pdf.step_header(2, "Build Your Skill with CoCo")
    pdf.body_text(
        "Launch Cortex Code with the incubator profile and use it as your development environment. "
        "CoCo will help you write the SKILL.md, generate SQL/Python scripts, and iterate on domain knowledge."
    )
    pdf.code_block([
        "# Launch CoCo with incubator profile",
        "cortex --profile health-sciences-incubator",
        "",
        '# Ask CoCo to help build your skill',
        'cortex> "help me build the SKILL.md for my new',
        '         pharmacovigilance skill"',
    ])
    pdf.ln(2)

    pdf.bold_text("What CoCo helps you write:")
    pdf.ln(1)

    pdf.bold_text("Part A: Frontmatter (REQUIRED)")
    pdf.code_block([
        "---",
        "name: hcls-pharma-dsafety-pharmacovigilance",
        'description: "Analyze FDA FAERS data for drug safety',
        '  signal detection. Triggers: FAERS, adverse events,',
        '  drug safety, pharmacovigilance, signal detection."',
        "platform_affinities:",
        "  produces: [tables, views]",
        "  benefits_from:",
        "    - skill: data-governance",
        '      when: "FAERS data has patient-level adverse events"',
        "    - skill: developing-with-streamlit",
        '      when: "user wants a signal detection dashboard"',
        "---",
    ])

    pdf.bold_text("Part B: Workflow Steps")
    pdf.body_text(
        "The workflow is the core of your skill -- step-by-step instructions that teach CoCo "
        "how to solve the domain problem. Write it as if you're training a knowledgeable colleague:"
    )
    pdf.bullet("Setup: prerequisites, data sources, Snowflake objects needed")
    pdf.bullet("Step-by-step workflow with SQL/Python code examples")
    pdf.bullet("Decision points: where user input is needed (code systems, data scope, parameters)")
    pdf.bullet("Stopping points: marked with MANDATORY STOPPING POINT before irreversible actions")
    pdf.ln(1)
    pdf.warning_box("Keep SKILL.md under 500 lines. Only include what CoCo doesn't already know -- domain expertise, not Snowflake basics.")

    pdf.add_page()
    pdf.bold_text("Part C: Domain Knowledge (the real value)")
    pdf.body_text(
        "Your skill's unique value is the domain knowledge that CoCo cannot infer from general training. "
        "This is what differentiates an industry skill from a generic Snowflake tutorial."
    )
    pdf.ln(1)

    w_dk = [55, 130]
    pdf.table_header(["Knowledge Type", "How to Add It"], w_dk)
    dk_rows = [
        ["Domain standards\n& schemas", "Encode in SKILL.md workflow (FHIR R4 resources,\nDICOM tags, FAERS file structure, OMOP CDM tables)"],
        ["Reference docs\n(large)", "Add to references/ directory (markdown, CSV)\nLoaded on-demand: 'Load references/faers-structure.md'"],
        ["SQL scripts", "Add to scripts/ directory\nDDL, stored procedures, pipeline setup"],
        ["Python helpers", "Add to scripts/ directory\nData processing, API calls, parsers"],
        ["Seed / reference\ndata", "Add to seed-data/ directory\nCSVs + setup SQL for COPY INTO"],
        ["Domain PDFs", "Add to skill root directory\nTaxonomy docs, concept schemas, standards"],
        ["Data model\nreference", "Create data-model-knowledge/ sub-skill\n(for router skills with 10+ tables)"],
    ]
    for i, row in enumerate(dk_rows):
        pdf.table_row(row, w_dk, fill=(i % 2 == 0))
    pdf.ln(2)

    pdf.bold_text("Example: Clinical NLP Domain Knowledge")
    pdf.body_text(
        "The clinical-nlp skill encodes: 17-table FHIR-aligned data model (245 columns), "
        "terminology seed data (154K codes across 8 code systems), extraction prompts for 6 "
        "concept categories, normalization pipelines with code-system-specific LLM guidance "
        "(RxNorm TTY hierarchy, LOINC 6-axis model, MedDRA PT level), and a 7-layer governance "
        "framework. None of this is in CoCo's general training."
    )

    pdf.bold_text("Part D: Output Section (REQUIRED)")
    pdf.code_block([
        "## Output",
        "",
        "Describe what the skill produces: tables created,",
        "files generated, pipelines deployed, views, etc.",
    ])

    # --- Step 3: Register & Integrate ---
    pdf.add_page()
    pdf.step_header(3, "Register & Integrate into Orchestrator")
    pdf.body_text(
        "Your skill must be registered in the incubator's skill registry so the orchestrator "
        "can route requests to it. This is a two-part process:"
    )
    pdf.ln(1)

    pdf.bold_text("Part A: Add to skills_incubator.yaml")
    pdf.body_text(
        "Edit templates/skills_incubator.yaml and add your skill entry under the appropriate "
        "segment. Follow the existing format:"
    )
    pdf.code_block([
        "# templates/skills_incubator.yaml",
        "skills:",
        "  # ... existing skills ...",
        "  hcls-pharma-dsafety-pharmacovigilance:",
        "    description: 'FAERS signal detection'",
        "    segment: pharma",
        "    domain: drug-safety",
        "    approved: false    # draft stage",
    ])

    pdf.bold_text("Part B: Regenerate the Orchestrator")
    pdf.body_text(
        "The orchestrator is auto-generated from the registry + Jinja2 template. "
        "Never hand-edit agents/health-sciences-incubator.md directly."
    )
    pdf.code_block([
        "# Regenerate orchestrator from registry",
        "python scripts/generate_orchestrators.py --profile incubator",
        "",
        "# Validate orchestrator integrity (all skills referenced,",
        "# all SKILL.md frontmatters valid, routing table complete)",
        "python scripts/qa_validate_orchestrator.py",
    ])
    pdf.ln(1)
    pdf.warning_box(
        "Never hand-edit agents/*.md files. Always edit the registry YAML + template, "
        "then regenerate. This prevents drift between incubator and production orchestrators."
    )

    pdf.ln(2)
    pdf.bold_text("Part C: Test with Cortex Code")
    pdf.code_block([
        "# Test via incubator profile (full orchestrator routing)",
        "cortex --profile health-sciences-incubator",
        '> "analyze FAERS data for aspirin adverse events"',
        "",
        "# Or test skill directly (bypasses orchestrator)",
        "cortex skill add ./skills/hcls-{your-name}",
        '> "your trigger phrase here"',
    ])
    pdf.ln(1)
    pdf.tip_box(
        "Test BOTH paths: (1) direct skill invocation to verify the workflow works, and "
        "(2) orchestrator routing to verify trigger keywords route correctly."
    )

    # --- Step 4: Validate ---
    pdf.add_page()
    pdf.step_header(4, "Validate")
    pdf.body_text(
        "Validation ensures your skill meets the quality bar before it's shared with the team. "
        "Run through these checks systematically."
    )
    pdf.ln(1)

    pdf.bold_text("Structural Validation Checklist")
    pdf.check_item("SKILL.md has name + description in frontmatter with trigger keywords")
    pdf.check_item("platform_affinities declared (produces + benefits_from)")
    pdf.check_item("At least one MANDATORY STOPPING POINT marker")
    pdf.check_item("## Output section present")
    pdf.check_item("Under 500 lines (router can be slightly longer)")
    pdf.check_item("Sub-skills have parent_skill declared (if router)")
    pdf.check_item("Registered in skills_incubator.yaml")
    pdf.check_item("Orchestrator regenerated and QA passes")
    pdf.ln(3)

    pdf.bold_text("Domain Validation")
    pdf.body_text("Test on real representative data for your domain:")
    pdf.ln(1)

    w_dv = [60, 125]
    pdf.table_header(["Domain", "What to Validate"], w_dv)
    dv_rows = [
        ["Clinical NLP", "Entity extraction accuracy, JSON parse rate, zero code\nhallucination, multi-category coverage, 50+ doc scale test"],
        ["FHIR", "All resource types parsed, no data loss, correct FK\nrelationships, NDJSON + Bundle formats"],
        ["Pharmacovigilance", "PRR/ROR calculations match benchmarks, dedup logic,\noutcome stratification correctness"],
        ["Imaging", "DICOM tag coverage, modality-specific fields, pipeline\nlatency at 100+ series"],
        ["Genomics", "ClinVar pathogenicity matches, allele frequency ranges,\npipeline test profile passes"],
        ["OMOP", "Vocabulary mapping accuracy, measurement vs observation\nrouting, CDM conformance"],
    ]
    for i, row in enumerate(dv_rows):
        pdf.table_row(row, w_dv, fill=(i % 2 == 0))

    pdf.ln(3)
    pdf.bold_text("Cross-Validation Against Best Practices")
    pdf.body_text(
        "Audit your skill against HCLS_INDUSTRY_SKILL_BEST_PRACTICES.md (repo root). "
        "Use the severity-based checklist:"
    )
    pdf.ln(1)
    w_cv = [20, 50, 115]
    pdf.table_header(["Sev.", "Check", "What to Verify"], w_cv)
    cv_rows = [
        ["CRIT", "Frontmatter", "name + description present, trigger keywords included"],
        ["CRIT", "Workflow", "At least one stopping point, no chaining without approval"],
        ["CRIT", "Output", "## Output section present in every skill and sub-skill"],
        ["WARN", "File size", "SKILL.md under 500 lines (router can be slightly longer)"],
        ["WARN", "Naming", "Follows hcls-{segment}-{domain}-{capability} convention"],
        ["WARN", "Affinities", "platform_affinities declared with produces + benefits_from"],
        ["WARN", "Sub-skills", "parent_skill declared in all sub-skill frontmatter"],
        ["WARN", "Evidence", "skill_evidence.yaml with promotion_stage"],
        ["PASS", "Preflight", "External dependencies have READY/MISSING/ERROR handling"],
        ["PASS", "Testing", "Domain-specific validation tests documented/passed"],
    ]
    for i, row in enumerate(cv_rows):
        pdf.table_row(row, w_cv, fill=(i % 2 == 0))
    pdf.ln(2)
    pdf.body_text("Document audit results in CROSS_VALIDATION_REPORT.md inside your skill folder.")

    # --- Step 5: Ship ---
    pdf.add_page()
    pdf.step_header(5, "Ship")
    pdf.body_text(
        "Once validation passes, create the promotion evidence and push your skill to the incubator."
    )
    pdf.ln(1)

    pdf.bold_text("Part A: Create skill_evidence.yaml")
    pdf.body_text(
        "This file tracks your skill through the promotion lifecycle (draft -> review -> staging -> production)."
    )
    pdf.code_block([
        'skill_name: "hcls-pharma-dsafety-pharmacovigilance"',
        'skill_type: "customer-facing"',
        'promotion_stage: "draft"',
        'version: "1.0.0"',
        "authors:",
        '  - "sfc-gh-{your-ldap}"',
        "validation:",
        "  testers:",
        "    count: 1",
        "    names:",
        '      - "sfc-gh-{your-ldap}"',
        "customer_impact:",
        "  customers:",
        "    count: 0",
        "    names: []",
        "evidence_links:",
        '  - "Validated on 50 FAERS quarterly files, PRR matches FDA benchmarks"',
        "notes: |",
        "  Initial draft of pharmacovigilance skill.",
        "  Covers FAERS data loading, signal detection, and dashboard.",
    ])

    pdf.bold_text("Part B: Record Customer Evidence")
    pdf.body_text(
        "If you used this skill (or parts of it) in a customer demo or engagement, "
        "record it in skill_evidence.yaml. This is critical for promotion:"
    )
    pdf.code_block([
        "customer_impact:",
        "  customers:",
        "    count: 2",
        "    names:",
        '      - "Acme Health Systems - imaging pipeline demo"',
        '      - "BioPharm Inc - FAERS signal detection POC"',
        "evidence_links:",
        '  - "Demo recording: [link to internal Sharepoint/GDrive]"',
        '  - "Customer feedback: positive, requested follow-up"',
    ])
    pdf.ln(1)
    pdf.tip_box(
        "Customer evidence is what moves a skill from draft to production. "
        "Even a single demo counts. Record it immediately after the engagement."
    )

    pdf.ln(2)
    pdf.bold_text("Part C: Commit and Push")
    pdf.code_block([
        "git add skills/hcls-{your-name}/",
        "git add templates/skills_incubator.yaml",
        "git add agents/health-sciences-incubator.md",
        'git commit -m "Add hcls-{your-name} skill"',
        "git push -u origin feature/hcls-{your-name}",
        "",
        "# Open PR to main",
        "gh pr create --title 'Add hcls-{your-name} skill' \\",
        "  --body 'New skill for {domain}. Validated on {X} samples.'",
    ])

    # ==================== TRACK B: UPDATE EXISTING SKILL ====================
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 20)
    pdf.set_text_color(*BLUE)
    pdf.cell(0, 15, "Track B: Update an Existing Skill", new_x="LMARGIN", new_y="NEXT")
    pdf.set_draw_color(*BLUE)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
    pdf.ln(6)

    # --- Step 1: Branch ---
    pdf.step_header(1, "Branch & Understand")
    pdf.code_block([
        "git checkout -b fix/hcls-{skill-name}-{description}",
        "",
        "# Launch CoCo with incubator profile",
        "cortex --profile health-sciences-incubator",
        "",
        "# Ask CoCo to explain the current skill",
        'cortex> "explain the workflow for hcls-provider-cdata-',
        '         clinical-nlp"',
    ])
    pdf.body_text("Understand the skill structure before making changes:")
    pdf.bullet("For standalone skills: read the workflow steps and Output section")
    pdf.bullet("For router skills: read the intent table, identify which sub-skill to modify")
    pdf.bullet("Check platform_affinities -- your change may need new affinities")

    # --- Step 2: Make Changes ---
    pdf.ln(2)
    pdf.step_header(2, "Make Your Changes with CoCo")
    pdf.body_text("Use CoCo to iterate on changes interactively:")
    pdf.code_block([
        '# Example: adding a new sub-skill to clinical NLP',
        'cortex> "I need to add a new extraction sub-skill for',
        '         oncology entities to clinical-nlp"',
        "",
        '# Example: fixing a normalization bug',
        'cortex> "the LOINC normalization in observations is',
        '         missing the method axis -- help me fix it"',
    ])
    pdf.ln(1)

    pdf.bold_text("Common update patterns:")
    w_up = [55, 130]
    pdf.table_header(["Update Type", "What to Change"], w_up)
    up_rows = [
        ["Add a workflow\nstep", "Add step to SKILL.md workflow. Include SQL/Python\nexamples. Add stopping point if step needs approval."],
        ["Add a sub-skill\n(router only)", "Create new directory + SKILL.md inside parent skill.\nAdd parent_skill to frontmatter. Add intent to router."],
        ["Fix a bug", "Edit the specific step/code. Update any affected\nreferences or cross-checks across sub-skills."],
        ["Add domain\nknowledge", "Add files to references/ or seed-data/.\nUpdate SKILL.md to reference new files."],
        ["Update platform\naffinities", "Edit frontmatter produces/benefits_from.\nRegenerate orchestrator."],
        ["Add governance", "Create governance/ sub-skill or add governance\nsection to SKILL.md. Tag PHI columns."],
        ["Record customer\nevidence", "Update skill_evidence.yaml with demo/engagement\ndetails, increment customer count."],
    ]
    for i, row in enumerate(up_rows):
        pdf.table_row(row, w_up, fill=(i % 2 == 0))

    # --- Step 3: Register changes ---
    pdf.add_page()
    pdf.step_header(3, "Re-Register & Test")
    pdf.body_text(
        "If your change affected the orchestrator (new intent, new sub-skill, changed affinities), "
        "re-register and regenerate:"
    )
    pdf.code_block([
        "# If you changed skills_incubator.yaml or affinities:",
        "python scripts/generate_orchestrators.py --profile incubator",
        "python scripts/qa_validate_orchestrator.py",
        "",
        "# Test your specific change via CoCo",
        "cortex --profile health-sciences-incubator",
        '> "{trigger phrase that exercises your change}"',
    ])
    pdf.ln(1)
    pdf.warning_box("If you modified a router's intent table, test ALL intents (not just the one you changed).")

    # --- Step 4: Validate ---
    pdf.ln(2)
    pdf.step_header(4, "Validate")
    pdf.body_text("Run the same validation checklist from Track A Step 4:")
    pdf.bullet("Structural validation: frontmatter, stopping points, Output, file size")
    pdf.bullet("Domain validation: test on representative data for the modified area")
    pdf.bullet("Cross-validation: audit against best practices if significant changes")
    pdf.bullet("Regression: verify unchanged workflows still work after your edit")
    pdf.ln(1)
    pdf.tip_box(
        "For Clinical NLP, the validation sweep covered 7 code systems x 6 normalization sub-skills = "
        "32 fixes. Any change to one sub-skill should be cross-checked against related sub-skills."
    )

    # --- Step 5: Ship ---
    pdf.ln(2)
    pdf.step_header(5, "Ship")
    pdf.body_text("Update evidence and push:")
    pdf.bullet("Update skill_evidence.yaml: increment version, add testers, add customer evidence")
    pdf.bullet("Update CROSS_VALIDATION_REPORT.md if you did a re-audit")
    pdf.code_block([
        'git add -A',
        'git commit -m "Update hcls-{skill-name}: {description}"',
        "git push -u origin fix/hcls-{skill-name}-{description}",
        "",
        "# Open PR to main",
        "gh pr create --title 'Update hcls-{skill-name}: {desc}'",
    ])

    # ==================== QUICK REFERENCE: ROUTER SKILL ====================
    pdf.add_page()
    pdf.section_title("Quick Reference: Router Skill Anatomy")
    pdf.body_text(
        "Router skills are the most complex archetype. Here's the Clinical NLP skill as a concrete example:"
    )
    pdf.ln(1)
    pdf.code_block([
        "skills/hcls-provider-cdata-clinical-nlp/",
        "  SKILL.md                     # Router: 17 intents, preference gate",
        "  skill_evidence.yaml          # Promotion: draft, v1.0.0",
        "  CROSS_VALIDATION_REPORT.md   # Audit: 0 critical, 9 warnings fixed",
        "  *.pdf                        # Domain taxonomy + concept schema",
        "  data-model-knowledge/        # 245-row model ref + Cortex Search",
        "    SKILL.md / references/ / scripts/ / seed-data/",
        "  extraction-conditions-diagnostics/SKILL.md",
        "  extraction-therapeutics/SKILL.md",
        "  extraction-observations/SKILL.md",
        "  extraction-patient-context/SKILL.md",
        "  extraction-oncology/SKILL.md",
        "  extraction-safety-care-planning/SKILL.md",
        "  normalization-conditions-diagnostics/SKILL.md",
        "  normalization-therapeutics/SKILL.md",
        "  normalization-observations/SKILL.md",
        "  normalization-patient-context/SKILL.md",
        "  normalization-oncology/SKILL.md",
        "  normalization-safety-care-planning/SKILL.md",
        "  governance/SKILL.md          # 7-layer PHI governance",
        "  pipeline-implementation/SKILL.md  # DTs + SP pipeline",
    ])
    pdf.ln(2)

    pdf.subsection_title("Router SKILL.md Template")
    pdf.code_block([
        "---",
        "name: hcls-{name}",
        'description: "... Triggers: ..."',
        "platform_affinities:",
        "  produces: [tables, views, ...]",
        "  benefits_from:",
        "    - skill: data-governance",
        '      when: "output contains PHI"',
        "---",
        "# Title",
        "## Preflight Check (probe data model knowledge svc)",
        "## Intent Detection",
        "| Intent | Triggers | Load |",
        "| EXTRACT_CONDITIONS | 'extract conditions' | extraction-.../SKILL.md |",
        "| NORMALIZE_ALL | 'normalize all' | (iterates all 6) |",
        "| GOVERNANCE | 'governance, PHI' | governance/SKILL.md |",
        "## Step 0: Data Model Knowledge (conditional on preflight)",
        "## Stopping Points",
        "## Output",
    ])
    pdf.ln(2)

    pdf.subsection_title("Sub-Skill SKILL.md Template")
    pdf.code_block([
        "---",
        "name: extraction-conditions-diagnostics",
        'description: "Extract conditions from clinical notes..."',
        "parent_skill: hcls-provider-cdata-clinical-nlp",
        "---",
        "# Conditions & Diagnostics Extraction",
        "## Scope (entity types -> target tables)",
        "## Engine Strategy (Cortex AI + regex supplements)",
        "## Extraction Prompt (structured JSON output schema)",
        "## Post-Processing (LATERAL FLATTEN -> INSERT)",
        "## Stopping Points",
        "## Output",
    ])

    # ==================== GOVERNANCE QUICK REFERENCE ====================
    pdf.add_page()
    pdf.section_title("Quick Reference: Governance Patterns")
    pdf.body_text(
        "Choose the right governance approach based on your data sensitivity:"
    )
    pdf.ln(1)
    pdf.bold_text("Example: Clinical NLP Governance Sub-Skill (7 layers)")
    pdf.bullet("L1: Tag-based PHI classification (16 PHI columns across 12 tables)")
    pdf.bullet("L2: Column masking policies (tag-based, one per data type)")
    pdf.bullet("L3: Row-access policies (department-based + patient-attribution)")
    pdf.bullet("L4: 3-tier database roles (READER < ANALYST < ADMIN)")
    pdf.bullet("L5: Cortex AI guardrails (secure views excluding PHI)")
    pdf.bullet("L6: ML feature views (de-identified, hashed patient IDs)")
    pdf.bullet("L7: Audit queries (ACCESS_HISTORY + QUERY_HISTORY)")
    pdf.ln(3)

    w_gov = [45, 40, 100]
    pdf.table_header(["Data Type", "Governance Level", "What to Implement"], w_gov)
    gov_rows = [
        ["PHI (patient\nnames, MRNs,\ndates, notes)", "High\n(dedicated\nsub-skill)", "Tag-based PHI classification, column masking,\nrow-access policies, 3-tier roles, AI guardrails,\nML de-identification, audit queries"],
        ["Patient-level\n(claims, adverse\nevents, encounters)", "Medium\n(section in\nSKILL.md)", "Column masking on identifiers, role-based\naccess, platform affinity to data-governance"],
        ["Identifiable\ngenomic data\n(sample-to-patient)", "Medium\n(section in\nSKILL.md)", "De-identification of mapping tables, secure\ncompute for variant data, access policies"],
        ["Aggregate /\nde-identified\ndata only", "Low\n(affinity\nonly)", "Declare platform affinity to data-governance.\nNo masking needed for aggregate data."],
        ["Reference data\n(CKEs, ontologies,\npublic datasets)", "None", "No governance needed. Read-only shared\nservices with no patient data."],
    ]
    for i, row in enumerate(gov_rows):
        pdf.table_row(row, w_gov, fill=(i % 2 == 0))

    # ==================== CHEAT SHEET ====================
    pdf.add_page()
    pdf.section_title("Cheat Sheet: Commands & Examples")
    pdf.ln(1)

    pdf.subsection_title("Skill Development with CoCo")
    pdf.code_block([
        "# Scaffold new skill",
        'cortex> "create a new HCLS skill for drug safety"',
        "",
        "# Iterate on an existing skill",
        'cortex> "help me add a FAERS quarterly data loader',
        '         to pharmacovigilance"',
        "",
        "# Test skill via orchestrator routing",
        "cortex --profile health-sciences-incubator",
        '> "extract conditions from these clinical notes"',
        '> "normalize all entities to ICD-10-CM and SNOMED CT"',
        '> "set up PHI governance for clinical NLP tables"',
    ])

    pdf.subsection_title("Orchestrator Management")
    pdf.code_block([
        "# Regenerate after registry changes",
        "python scripts/generate_orchestrators.py --profile both",
        "",
        "# Validate integrity",
        "python scripts/qa_validate_orchestrator.py",
    ])

    pdf.subsection_title("Git Workflow")
    pdf.code_block([
        "# New skill:  feature/hcls-{name}",
        "# Bug fix:    fix/hcls-{name}-{desc}",
        "# Docs:       docs/{desc}",
        "# Refactor:   refactor/{desc}",
        "",
        "git push -u origin {branch}",
        "gh pr create --title '{title}' --body '{description}'",
    ])

    pdf.subsection_title("Validation Queries (Clinical NLP Example)")
    pdf.code_block([
        "# Extraction coverage",
        "SELECT COUNT(*) total, COUNT(DISTINCT provenance_document_id) docs",
        "FROM CONDITION WHERE source = 'GENAI_NLP_NOTE';",
        "",
        "# Normalization rate",
        "SELECT COUNT(*) total, COUNT(code) coded,",
        "  ROUND(COUNT(code)*100.0/NULLIF(COUNT(*),0),1) pct",
        "FROM CONDITION WHERE source = 'GENAI_NLP_NOTE';",
        "",
        "# Zero hallucination check",
        "SELECT c.display, c.code, c.code_system FROM CONDITION c",
        "WHERE c.code IS NOT NULL AND c.source = 'GENAI_NLP_NOTE'",
        "AND NOT EXISTS (SELECT 1 FROM CONCEPT_DIMENSION cd",
        "  WHERE cd.code = c.code);  -- Expected: 0 rows",
    ])

    pdf.subsection_title("Skill Integrity Queries")
    pdf.code_block([
        "# Count all skills",
        "ls -d skills/hcls-*/ | wc -l",
        "",
        "# SKILL.md line counts (should be <500)",
        "wc -l skills/*/SKILL.md | sort -rn",
        "",
        "# Verify all have required sections",
        "grep -l '^name:' skills/*/SKILL.md | wc -l",
        "grep -rl '^## Output' skills/*/SKILL.md | wc -l",
        "",
        "# Check sub-skill parent_skill references",
        "grep -r 'parent_skill:' skills/*/*/SKILL.md",
    ])

    # ==================== WHAT'S NEXT ====================
    pdf.add_page()
    pdf.section_title("What's Next?")
    pdf.body_text(
        "You've created or updated a skill. Here's what happens next in the lifecycle:"
    )
    pdf.ln(2)

    pdf.set_font("Courier", "", 9.5)
    pdf.set_text_color(*DARK)
    flow = [
        "  YOU ARE HERE",
        "       |",
        "       v",
        "  +-----------------------------+",
        "  | Incubator (main branch)     |  Your PR gets merged",
        "  | Test via incubator profile  |  Field teams can try it",
        "  +-----------------------------+",
        "       |  skill matures + customer evidence",
        "       v",
        "  +-----------------------------+",
        "  | Tiger Team Review           |  Audit against best practices",
        "  | (Phase 2: Harden)           |  Cross-validate, test at scale",
        "  +-----------------------------+",
        "       |  approved (3+ testers, 1+ customer)",
        "       v",
        "  +-----------------------------+",
        "  | Production Profile          |  Available to all field teams",
        "  | (cortex-code-skills repo)   |  cortex profile add ...",
        "  +-----------------------------+",
    ]
    for line in flow:
        pdf.cell(0, 5, line, new_x="LMARGIN", new_y="NEXT")
    pdf.ln(6)

    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(*DARK)
    pdf.subsection_title("Key Resources")
    pdf.bullet("HCLS_INDUSTRY_SKILL_BEST_PRACTICES.md -- Full best practices guide (repo root)")
    pdf.bullet("SKILL_BEST_PRACTICES.md -- Platform skill best practices (cortex-code-skills repo)")
    pdf.bullet("documentation/ISF-*.pdf -- Industry Solutions Framework lifecycle")
    pdf.bullet("templates/skills_incubator.yaml -- Skill registry (source of truth)")
    pdf.bullet("agents/health-sciences-incubator.md -- Orchestrator (auto-generated, never hand-edit)")
    pdf.ln(3)

    pdf.subsection_title("Getting Help")
    pdf.bullet("Use hcls-cross-skill-development skill in CoCo for guided scaffolding")
    pdf.bullet("File an issue on the incubator repo for questions or feature requests")
    pdf.bullet("Tag the Tiger Team for promotion reviews")
    pdf.bullet("Ask CoCo directly: 'help me improve my HCLS skill'")

    pdf.output(OUTPUT_PATH)
    print(f"PDF generated: {OUTPUT_PATH}")


if __name__ == "__main__":
    build_pdf()
