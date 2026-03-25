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

    def step_header(self, number, title, time_est):
        self.set_font("Helvetica", "B", 13)
        self.set_text_color(*BLUE)
        self.cell(0, 10, f"Step {number}: {title}", new_x="LMARGIN", new_y="NEXT")
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

    # ===== TITLE PAGE =====
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
    pdf.cell(0, 10, "in 5 Minutes", align="C", new_x="LMARGIN", new_y="NEXT")
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

    # ===== PAGE 2: OVERVIEW =====
    pdf.add_page()
    pdf.section_title("What This Playbook Covers")
    pdf.body_text(
        "This playbook gives you the fastest path to creating or updating an HCLS industry skill "
        "for Cortex Code. It distills the full HCLS_INDUSTRY_SKILL_BEST_PRACTICES.md into "
        "actionable steps you can follow in minutes."
    )
    pdf.ln(2)

    pdf.subsection_title("Two Tracks")
    w = [93, 93]
    pdf.table_header(["Track A: Create a New Skill", "Track B: Update an Existing Skill"], w)
    pdf.table_row([
        "5 steps to go from zero to\na working skill in the incubator",
        "4 steps to safely modify,\ntest, and push an update"
    ], w, fill=True)
    pdf.ln(6)

    pdf.subsection_title("Prerequisites")
    pdf.bullet("Git access to Snowflake-Solutions/health-sciences-coco-skills-incubator")
    pdf.bullet("Cortex Code installed (cortex CLI)")
    pdf.bullet("Snowflake account with ACCOUNTADMIN or appropriate role")
    pdf.bullet("Familiarity with your domain (the skill teaches Cortex Code what YOU know)")
    pdf.ln(4)

    pdf.subsection_title("Choose Your Archetype")
    pdf.body_text(
        "Before starting, identify which pattern fits your skill:"
    )
    w_arch = [35, 55, 95]
    pdf.table_header(["Archetype", "When to Use", "Examples"], w_arch)
    arch_rows = [
        ["Standalone", "Single coherent\nworkflow", "FHIR, Pharmacovigilance, Survival\nAnalysis, Variant Annotation, OMOP"],
        ["Router +\nSub-Skills", "Multiple distinct\nworkflows sharing\na data model", "Imaging (7 sub-skills),\nClinical NLP (15 sub-skills)"],
        ["CKE", "Wrapping a\nMarketplace shared\nCortex Search", "PubMed CKE,\nClinicalTrials.gov CKE"],
        ["Compute-\nHeavy", "External compute\n(containers,\npipelines)", "Nextflow nf-core,\nscvi-tools, Imaging ML"],
    ]
    for i, row in enumerate(arch_rows):
        pdf.table_row(row, w_arch, fill=(i % 2 == 0))
    pdf.ln(4)
    pdf.tip_box("Most new skills are Standalone. Start there unless you have 3+ distinct workflows.")

    # ===== TRACK A: CREATE NEW SKILL =====
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 20)
    pdf.set_text_color(*BLUE)
    pdf.cell(0, 15, "Track A: Create a New Skill", new_x="LMARGIN", new_y="NEXT")
    pdf.set_draw_color(*BLUE)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
    pdf.ln(6)

    # --- Step 1: Name & Scaffold ---
    pdf.step_header(1, "Name & Scaffold", "30 seconds")
    pdf.body_text("Pick your skill name using the HCLS naming convention:")
    pdf.code_block([
        "hcls-{segment}-{domain}-{capability}",
        "",
        "Segments:  provider | pharma | payer | meddev | cross",
        "Domains:   cdata | imaging | genomics | dsafety | lab |",
        "           claims | risk | quality | regulatory |",
        "           postmarket | engineering | cke",
    ])
    pdf.body_text("Create the skill directory:")
    pdf.code_block([
        "cd health-sciences-coco-skills-incubator",
        "mkdir -p skills/hcls-{your-name}",
        "touch skills/hcls-{your-name}/SKILL.md",
    ])
    pdf.tip_box("Use hcls-cross-skill-development skill in Cortex Code to auto-scaffold!")

    # --- Step 2: Write SKILL.md ---
    pdf.ln(2)
    pdf.step_header(2, "Write Your SKILL.md", "2-3 minutes")
    pdf.body_text("Every SKILL.md has three required parts:")
    pdf.ln(2)

    pdf.bold_text("Part A: Frontmatter (REQUIRED)")
    pdf.code_block([
        "---",
        "name: hcls-{segment}-{domain}-{capability}",
        'description: "What this skill does. Triggers: keyword1,',
        '  keyword2, keyword3."',
        "platform_affinities:",
        "  produces: [tables, views]",
        "  benefits_from:",
        "    - skill: data-governance",
        '      when: "output contains PHI"',
        "---",
    ])
    pdf.ln(2)

    pdf.bold_text("Part B: Workflow (the core of your skill)")
    pdf.body_text(
        "Write the step-by-step instructions that teach Cortex Code how to solve "
        "the domain problem. Include:"
    )
    pdf.bullet("Setup / prerequisites")
    pdf.bullet("Step-by-step workflow with code examples (SQL, Python)")
    pdf.bullet("Decision points where user input is needed")
    pdf.bullet("Stopping points marked with: **MANDATORY STOPPING POINT**")
    pdf.ln(2)

    pdf.bold_text("Part C: Output Section (REQUIRED)")
    pdf.code_block([
        "## Output",
        "",
        "Describe what the skill produces: tables created,",
        "files generated, pipelines deployed, etc.",
    ])
    pdf.ln(2)
    pdf.warning_box("Keep SKILL.md under 500 lines. Only include what Cortex Code doesn't already know.")

    # --- Step 3: Add Domain Knowledge ---
    pdf.add_page()
    pdf.step_header(3, "Add Domain Knowledge", "1 minute")
    pdf.body_text(
        "Your skill's value is the domain knowledge Cortex Code can't infer. "
        "Add what's needed for YOUR domain:"
    )
    pdf.ln(2)

    w_dk = [55, 130]
    pdf.table_header(["What You Need", "How to Add It"], w_dk)
    dk_rows = [
        ["Reference docs", "Add to references/ directory (markdown, CSV)\nLoaded on-demand by the skill"],
        ["SQL scripts", "Add to scripts/ directory\nSetup scripts, DDL, stored procedures"],
        ["Python helpers", "Add to scripts/ directory\nData processing, API calls, pipeline tools"],
        ["Seed / reference\ndata", "Add to seed-data/ directory\nCSVs + setup SQL for COPY INTO"],
        ["Domain PDFs", "Add to skill root directory\nTaxonomy docs, schema docs, standards"],
        ["Data model\nreference", "Create data-model-knowledge/ sub-skill\n(for router skills with complex schemas)"],
    ]
    for i, row in enumerate(dk_rows):
        pdf.table_row(row, w_dk, fill=(i % 2 == 0))
    pdf.ln(4)
    pdf.tip_box("Not every skill needs all of these. A simple standalone skill may only need SKILL.md + a SQL script.")

    # --- Step 4: Register & Test ---
    pdf.ln(2)
    pdf.step_header(4, "Register & Test", "1 minute")
    pdf.body_text("Register your skill in the incubator registry:")
    pdf.code_block([
        "# Edit templates/skills_incubator.yaml",
        "# Add your skill entry under the appropriate segment",
        "",
        "# Regenerate the orchestrator",
        "python scripts/generate_orchestrators.py --profile incubator",
    ])
    pdf.ln(2)
    pdf.body_text("Test your skill locally:")
    pdf.code_block([
        "# Option 1: Test via incubator profile",
        "cortex --profile health-sciences-incubator",
        '> "your trigger phrase here"',
        "",
        "# Option 2: Test skill directly",
        "cortex skill add ./skills/hcls-{your-name}",
    ])
    pdf.ln(2)
    pdf.warning_box("Always test your skill on real data before declaring it done. Verify outputs match expectations.")

    # --- Step 5: Validate & Ship ---
    pdf.add_page()
    pdf.step_header(5, "Validate & Ship", "30 seconds")
    pdf.body_text("Run through the validation checklist, then commit:")
    pdf.ln(2)

    pdf.bold_text("Quick Validation Checklist")
    pdf.check_item("SKILL.md has name + description in frontmatter")
    pdf.check_item("platform_affinities declared (produces + benefits_from)")
    pdf.check_item("At least one stopping point with MANDATORY STOPPING POINT marker")
    pdf.check_item("## Output section present")
    pdf.check_item("Under 500 lines")
    pdf.check_item("Tested on representative data")
    pdf.check_item("Registered in skills_incubator.yaml")
    pdf.check_item("Orchestrator regenerated")
    pdf.ln(4)

    pdf.body_text("Create skill_evidence.yaml:")
    pdf.code_block([
        'skill_name: "hcls-{your-name}"',
        'skill_type: "customer-facing"',
        'promotion_stage: "draft"',
        'version: "1.0.0"',
        "authors:",
        '  - "sfc-gh-{your-ldap}"',
    ])
    pdf.ln(2)
    pdf.body_text("Commit and push:")
    pdf.code_block([
        "git checkout -b feature/hcls-{your-name}",
        "git add skills/hcls-{your-name}/",
        "git add templates/skills_incubator.yaml",
        "git add agents/health-sciences-incubator.md",
        'git commit -m "Add hcls-{your-name} skill"',
        "git push -u origin feature/hcls-{your-name}",
        "# Open PR to main",
    ])

    # ===== TRACK B: UPDATE EXISTING SKILL =====
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 20)
    pdf.set_text_color(*BLUE)
    pdf.cell(0, 15, "Track B: Update an Existing Skill", new_x="LMARGIN", new_y="NEXT")
    pdf.set_draw_color(*BLUE)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
    pdf.ln(6)

    # --- Step 1: Branch ---
    pdf.step_header(1, "Branch & Understand", "30 seconds")
    pdf.code_block([
        "git checkout -b fix/hcls-{skill-name}-{description}",
        "",
        "# Read the current skill",
        "cat skills/hcls-{skill-name}/SKILL.md",
        "",
        "# Understand the intent table (for router skills)",
        "grep -A 20 'Intent Detection' skills/hcls-{skill-name}/SKILL.md",
    ])

    # --- Step 2: Make Changes ---
    pdf.ln(2)
    pdf.step_header(2, "Make Your Changes", "2-3 minutes")
    pdf.body_text("Common update patterns:")
    pdf.ln(2)

    w_up = [55, 130]
    pdf.table_header(["Update Type", "What to Change"], w_up)
    up_rows = [
        ["Add a workflow\nstep", "Add step to SKILL.md workflow. Include SQL/Python\nexamples. Add stopping point if step needs approval."],
        ["Add a sub-skill\n(router only)", "Create new directory + SKILL.md inside parent skill.\nAdd parent_skill to frontmatter. Add intent to router table."],
        ["Fix a bug", "Edit the specific step/code. Update any affected\nreferences or cross-checks."],
        ["Add domain\nknowledge", "Add files to references/ or seed-data/.\nUpdate SKILL.md to reference new files."],
        ["Update platform\naffinities", "Edit frontmatter produces/benefits_from.\nRegenerate orchestrator if affinities changed."],
        ["Add governance", "Create governance/ sub-skill or add governance\nsection to SKILL.md. Tag PHI columns."],
    ]
    for i, row in enumerate(up_rows):
        pdf.table_row(row, w_up, fill=(i % 2 == 0))

    # --- Step 3: Test ---
    pdf.ln(4)
    pdf.step_header(3, "Test Your Changes", "1 minute")
    pdf.bullet("For workflow changes: run the modified steps on sample data")
    pdf.bullet("For sub-skill additions: test intent routing + sub-skill execution")
    pdf.bullet("For bug fixes: reproduce the bug first, then verify the fix")
    pdf.bullet("For domain knowledge: verify references load correctly in the workflow")
    pdf.ln(2)
    pdf.warning_box("If you modified a router's intent table, test ALL intents (not just the one you changed).")

    # --- Step 4: Commit ---
    pdf.ln(2)
    pdf.step_header(4, "Validate & Push", "30 seconds")
    pdf.body_text("Run the quick validation checklist from Track A Step 5, then:")
    pdf.code_block([
        'git add -A && git commit -m "Update hcls-{skill-name}: {description}"',
        "git push -u origin fix/hcls-{skill-name}-{description}",
        "# Open PR to main",
    ])

    # ===== ADVANCED: ROUTER SKILL QUICK REFERENCE =====
    pdf.add_page()
    pdf.section_title("Quick Reference: Router Skill Anatomy")
    pdf.body_text(
        "If you're building a router skill (Archetype B), here's the structure at a glance:"
    )
    pdf.ln(2)
    pdf.code_block([
        "skills/hcls-{name}/",
        "  SKILL.md                  # Router (intent table + preflight + Step 0)",
        "  skill_evidence.yaml       # Promotion lifecycle tracking",
        "  data-model-knowledge/     # Schema grounding sub-skill",
        "    SKILL.md",
        "    references/model.csv",
        "    scripts/setup.sql",
        "    seed-data/",
        "  {sub-skill-a}/SKILL.md   # Functional sub-skills",
        "  {sub-skill-b}/SKILL.md",
        "  governance/SKILL.md       # If handling PHI/sensitive data",
        "  pipeline-implementation/  # If production pipeline needed",
        "    SKILL.md",
    ])
    pdf.ln(4)

    pdf.subsection_title("Router SKILL.md Template")
    pdf.code_block([
        "---",
        "name: hcls-{name}",
        'description: "..."',
        "platform_affinities:",
        "  produces: [...]",
        "  benefits_from: [...]",
        "---",
        "",
        "# Title",
        "",
        "## Preflight Check",
        "(probe for data model knowledge service)",
        "",
        "## Intent Detection",
        "| Intent | Triggers | Load |",
        "| INTENT_A | keywords... | sub-skill-a/SKILL.md |",
        "| INTENT_B | keywords... | sub-skill-b/SKILL.md |",
        "",
        "## Step 0: Data Model Knowledge (conditional)",
        "(query Cortex Search, pass context to sub-skill)",
        "",
        "## Stopping Points",
        "- MANDATORY STOPPING POINT: after intent if ambiguous",
        "- MANDATORY STOPPING POINT: before creating DB objects",
        "",
        "## Output",
        "What the skill produces overall.",
    ])
    pdf.ln(4)

    pdf.subsection_title("Sub-Skill SKILL.md Template")
    pdf.code_block([
        "---",
        "name: {sub-skill-name}",
        'description: "..."',
        "parent_skill: hcls-{parent-name}",
        "---",
        "",
        "# Title",
        "",
        "## Workflow",
        "Step 1: ...",
        "Step 2: ...",
        "",
        "## Stopping Points",
        "- MANDATORY STOPPING POINT: ...",
        "",
        "## Output",
        "What this sub-skill produces.",
    ])

    # ===== GOVERNANCE QUICK REFERENCE =====
    pdf.add_page()
    pdf.section_title("Quick Reference: Governance Patterns")
    pdf.body_text(
        "Choose the right governance approach based on your data sensitivity:"
    )
    pdf.ln(2)

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

    # ===== CROSS-VALIDATION QUICK REFERENCE =====
    pdf.ln(8)
    pdf.section_title("Quick Reference: Cross-Validation Audit")
    pdf.body_text(
        "Before promoting a skill, audit it against the best practices. Use this severity guide:"
    )
    pdf.ln(2)

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

    # ===== CHEAT SHEET =====
    pdf.add_page()
    pdf.section_title("Cheat Sheet: Common Commands")
    pdf.ln(2)

    pdf.subsection_title("Skill Development")
    pdf.code_block([
        "# Scaffold a new skill (via Cortex Code)",
        'cortex> "create a new HCLS skill for {domain}"',
        "",
        "# Test skill locally",
        "cortex skill add ./skills/hcls-{name}",
        "",
        "# Test via incubator profile",
        "cortex --profile health-sciences-incubator",
    ])

    pdf.subsection_title("Orchestrator Management")
    pdf.code_block([
        "# Regenerate orchestrators after registry changes",
        "python scripts/generate_orchestrators.py --profile both",
        "",
        "# Validate orchestrator integrity",
        "python scripts/qa_validate_orchestrator.py",
    ])

    pdf.subsection_title("Git Workflow")
    pdf.code_block([
        "# New skill",
        "git checkout -b feature/hcls-{name}",
        "",
        "# Bug fix",
        "git checkout -b fix/hcls-{name}-{desc}",
        "",
        "# Documentation",
        "git checkout -b docs/{desc}",
        "",
        "# Push and PR",
        "git push -u origin {branch}",
        "gh pr create --title '{title}' --body '{description}'",
    ])

    pdf.subsection_title("Useful Queries")
    pdf.code_block([
        "# Count all skills",
        "ls -d skills/hcls-*/ | wc -l",
        "",
        "# Check all SKILL.md line counts",
        "wc -l skills/*/SKILL.md | sort -rn",
        "",
        "# Verify all frontmatter has name field",
        "grep -l '^name:' skills/*/SKILL.md | wc -l",
        "",
        "# Check all have ## Output sections",
        "grep -rl '^## Output' skills/*/SKILL.md | wc -l",
    ])

    # ===== FINAL PAGE: WHAT'S NEXT =====
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
        "       |  skill matures",
        "       v",
        "  +-----------------------------+",
        "  | Tiger Team Review           |  Audit against best practices",
        "  | (Phase 2: Harden)           |  Cross-validate, test at scale",
        "  +-----------------------------+",
        "       |  approved",
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
    pdf.bullet("HCLS_INDUSTRY_SKILL_BEST_PRACTICES.md - Full best practices guide (repo root)")
    pdf.bullet("SKILL_BEST_PRACTICES.md - Platform skill best practices (cortex-code-skills repo)")
    pdf.bullet("documentation/ISF-*.pdf - Industry Solutions Framework lifecycle")
    pdf.bullet("templates/skills_incubator.yaml - Skill registry")
    pdf.bullet("agents/health-sciences-incubator.md - Orchestrator (auto-generated)")
    pdf.ln(4)

    pdf.subsection_title("Getting Help")
    pdf.bullet("Use hcls-cross-skill-development skill in Cortex Code for guided scaffolding")
    pdf.bullet("File an issue on the incubator repo for questions or feature requests")
    pdf.bullet("Tag the Tiger Team for promotion reviews")

    pdf.output(OUTPUT_PATH)
    print(f"PDF generated: {OUTPUT_PATH}")


if __name__ == "__main__":
    build_pdf()
