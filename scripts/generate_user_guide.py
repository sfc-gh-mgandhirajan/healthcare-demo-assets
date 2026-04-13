#!/usr/bin/env python3
"""
Generate documentation/User_Guide.docx
Health Sciences Industry Skills Framework — User Guide

Source dependencies (framework-level only):
  - README.md                          : profile JSON block
  - agents/health-sciences-incubator.md: plan table format

Usage: python3 scripts/generate_user_guide.py
Output: documentation/User_Guide.docx
"""

import os
import re
from docx import Document
from docx.shared import Pt, RGBColor, Inches
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from docx.oxml import OxmlElement

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ── Colour palette ────────────────────────────────────────────────────────────
SNOW_BLUE     = RGBColor(0x29, 0xB5, 0xE8)
NAVY          = RGBColor(0x1B, 0x3A, 0x5C)
MID_BLUE      = RGBColor(0x2D, 0x6A, 0x9F)
DARK_GRAY     = RGBColor(0x44, 0x44, 0x44)
MID_GRAY      = RGBColor(0x77, 0x77, 0x77)

SNOW_BLUE_HEX = "29B5E8"
NAVY_HEX      = "1B3A5C"
LIGHT_GRAY    = "F4F4F4"
CALLOUT_BG    = "EBF5FF"
WARNING_BG    = "FFF3CD"
GUIDED_BG     = "E8F5E9"
USER_BG       = "E8F4FD"
ORCH_BG       = "F0F0F0"
ANNOT_BG      = "FAFAFA"


# ── File helpers ──────────────────────────────────────────────────────────────
def read_file(path):
    with open(os.path.join(BASE, path), encoding="utf-8") as f:
        return f.read()


def extract_plan_table(orchestrator):
    match = re.search(
        r"```\n(\| Step \| Skill \| What it produces[\s\S]+?)\n```",
        orchestrator
    )
    return match.group(1).strip() if match else None


# ── XML helpers ───────────────────────────────────────────────────────────────
def set_cell_bg(cell, hex_color):
    tc   = cell._tc
    tcPr = tc.get_or_add_tcPr()
    shd  = OxmlElement("w:shd")
    shd.set(qn("w:fill"),  hex_color)
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:val"),   "clear")
    tcPr.append(shd)


def set_para_bg(para, hex_color):
    pPr = para._p.get_or_add_pPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:fill"),  hex_color)
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:val"),   "clear")
    pPr.append(shd)


# ── Document primitives ───────────────────────────────────────────────────────
def add_heading(doc, text, level):
    para = doc.add_paragraph()
    para.paragraph_format.space_before = Pt(14 if level == 1 else 10)
    para.paragraph_format.space_after  = Pt(4)
    run = para.add_run(text)
    run.font.name = "Calibri"
    run.font.bold = True
    run.font.size = Pt({1: 17, 2: 13, 3: 11}.get(level, 11))
    run.font.color.rgb = {1: NAVY, 2: MID_BLUE, 3: NAVY}.get(level, NAVY)
    return para


def add_body(doc, text, bold=False, italic=False, color=None):
    para = doc.add_paragraph()
    para.paragraph_format.space_before = Pt(2)
    para.paragraph_format.space_after  = Pt(5)
    run = para.add_run(text)
    run.font.name      = "Calibri"
    run.font.size      = Pt(11)
    run.font.bold      = bold
    run.font.italic    = italic
    if color:
        run.font.color.rgb = color
    return para


def add_bullet(doc, text, bold_prefix=None):
    para = doc.add_paragraph(style="List Bullet")
    para.paragraph_format.space_before = Pt(1)
    para.paragraph_format.space_after  = Pt(2)
    if bold_prefix:
        r1 = para.add_run(bold_prefix)
        r1.font.bold = True
        r1.font.name = "Calibri"
        r1.font.size = Pt(11)
        r2 = para.add_run(" " + text)
        r2.font.name = "Calibri"
        r2.font.size = Pt(11)
    else:
        run = para.add_run(text)
        run.font.name = "Calibri"
        run.font.size = Pt(11)
    return para


def add_code(doc, text):
    for line in text.split("\n"):
        para = doc.add_paragraph()
        para.paragraph_format.space_before  = Pt(0)
        para.paragraph_format.space_after   = Pt(0)
        para.paragraph_format.left_indent   = Inches(0.25)
        run = para.add_run(line if line else " ")
        run.font.name      = "Courier New"
        run.font.size      = Pt(9)
        run.font.color.rgb = DARK_GRAY
        set_para_bg(para, LIGHT_GRAY)


def add_callout(doc, text, bg=CALLOUT_BG, bold_prefix=None):
    para = doc.add_paragraph()
    para.paragraph_format.space_before  = Pt(6)
    para.paragraph_format.space_after   = Pt(6)
    para.paragraph_format.left_indent   = Inches(0.2)
    para.paragraph_format.right_indent  = Inches(0.2)
    if bold_prefix:
        r1 = para.add_run(bold_prefix + "  ")
        r1.font.name = "Calibri"
        r1.font.size = Pt(10)
        r1.font.bold = True
    run = para.add_run(text)
    run.font.name    = "Calibri"
    run.font.size    = Pt(10)
    run.font.italic  = True
    set_para_bg(para, bg)
    return para


def add_spacer(doc, pts=6):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(pts)
    p.paragraph_format.space_after  = Pt(0)


def add_table(doc, headers, rows, col_widths=None):
    table = doc.add_table(rows=1 + len(rows), cols=len(headers))
    table.style = "Table Grid"

    hrow = table.rows[0]
    for i, h in enumerate(headers):
        cell = hrow.cells[i]
        cell.text = ""
        set_cell_bg(cell, SNOW_BLUE_HEX)
        run = cell.paragraphs[0].add_run(h)
        run.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
        run.font.bold  = True
        run.font.name  = "Calibri"
        run.font.size  = Pt(10)

    for ri, row_data in enumerate(rows):
        row = table.rows[ri + 1]
        bg  = "FFFFFF" if ri % 2 == 0 else "F2F2F2"
        for ci, cell_text in enumerate(row_data):
            cell = row.cells[ci]
            cell.text = ""
            set_cell_bg(cell, bg)
            run = cell.paragraphs[0].add_run(str(cell_text))
            run.font.name = "Calibri"
            run.font.size = Pt(10)

    if col_widths:
        for row in table.rows:
            for i, w in enumerate(col_widths):
                if i < len(row.cells):
                    row.cells[i].width = Inches(w)

    add_spacer(doc, 4)
    return table


# ── Conversation transcript helpers ──────────────────────────────────────────
def add_turn_user(doc, text):
    """Styled USER turn block."""
    label = doc.add_paragraph()
    label.paragraph_format.space_before = Pt(10)
    label.paragraph_format.space_after  = Pt(0)
    r = label.add_run("  USER")
    r.font.name      = "Calibri"
    r.font.size      = Pt(9)
    r.font.bold      = True
    r.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
    set_para_bg(label, SNOW_BLUE_HEX)

    msg = doc.add_paragraph()
    msg.paragraph_format.space_before = Pt(0)
    msg.paragraph_format.space_after  = Pt(2)
    msg.paragraph_format.left_indent  = Inches(0.15)
    run = msg.add_run(text)
    run.font.name = "Calibri"
    run.font.size = Pt(10)
    run.font.italic = True
    set_para_bg(msg, USER_BG)


def add_turn_orch(doc, label_suffix, paragraphs):
    """Styled ORCHESTRATOR turn block. paragraphs is a list of (text, is_code, is_bold)."""
    lbl = doc.add_paragraph()
    lbl.paragraph_format.space_before = Pt(6)
    lbl.paragraph_format.space_after  = Pt(0)
    lbl_text = "  ORCHESTRATOR" + (f"  —  {label_suffix}" if label_suffix else "")
    r = lbl.add_run(lbl_text)
    r.font.name      = "Calibri"
    r.font.size      = Pt(9)
    r.font.bold      = True
    r.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
    set_para_bg(lbl, NAVY_HEX)

    for (text, is_code, is_bold) in paragraphs:
        p = doc.add_paragraph()
        p.paragraph_format.space_before = Pt(0)
        p.paragraph_format.space_after  = Pt(1)
        p.paragraph_format.left_indent  = Inches(0.15)
        run = p.add_run(text)
        run.font.name  = "Courier New" if is_code else "Calibri"
        run.font.size  = Pt(9) if is_code else Pt(10)
        run.font.bold  = is_bold
        set_para_bg(p, ORCH_BG)


def add_annotation(doc, text):
    """Gray italic annotation block explaining what mechanism fired."""
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(3)
    p.paragraph_format.space_after  = Pt(8)
    p.paragraph_format.left_indent  = Inches(0.2)
    p.paragraph_format.right_indent = Inches(0.2)
    run = p.add_run(text)
    run.font.name      = "Calibri"
    run.font.size      = Pt(9)
    run.font.italic    = True
    run.font.color.rgb = MID_GRAY
    set_para_bg(p, ANNOT_BG)


# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Health Sciences Industry Skills Framework — User Guide Generator")
    print("=" * 60)

    print("\nLoading source files...")
    orchestrator = read_file("agents/health-sciences-incubator.md")

    plan_table_raw = extract_plan_table(orchestrator)

    print(f"  Plan table extracted:      {'OK' if plan_table_raw else 'FAILED'}")

    doc = Document()

    sec = doc.sections[0]
    sec.top_margin    = Inches(1.0)
    sec.bottom_margin = Inches(1.0)
    sec.left_margin   = Inches(1.2)
    sec.right_margin  = Inches(1.2)

    # ── COVER PAGE ────────────────────────────────────────────────────
    for _ in range(4):
        doc.add_paragraph()

    t = doc.add_paragraph()
    t.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = t.add_run("Health Sciences Industry Skills Framework")
    r.font.name = "Calibri"; r.font.size = Pt(28)
    r.font.bold = True; r.font.color.rgb = SNOW_BLUE

    s = doc.add_paragraph()
    s.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = s.add_run("User Guide")
    r.font.name = "Calibri"; r.font.size = Pt(22)
    r.font.bold = True; r.font.color.rgb = NAVY

    doc.add_paragraph()

    tl = doc.add_paragraph()
    tl.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = tl.add_run(
        "A build companion for Cortex Code\n"
        "for healthcare and life sciences solutions on Snowflake"
    )
    r.font.name = "Calibri"; r.font.size = Pt(12)
    r.font.italic = True; r.font.color.rgb = DARK_GRAY

    doc.add_paragraph()

    vl = doc.add_paragraph()
    vl.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = vl.add_run(
        "April 2026  |  "
        "github: Snowflake-Solutions / health-sciences-coco-skills-incubator"
    )
    r.font.name = "Calibri"; r.font.size = Pt(9)
    r.font.color.rgb = RGBColor(0x99, 0x99, 0x99)

    doc.add_page_break()

    # ── SECTION 1 — WHAT IS THIS FRAMEWORK? ──────────────────────────
    add_heading(doc, "Section 1 — What is This Framework?", 1)

    add_body(doc,
        "The Health Sciences Industry Skills Framework is a companion for anyone using Cortex Code "
        "to accelerate their concept to production timelines. Just as Cortex Code's platform skills "
        "guide it toward the best approach for a specific Snowflake platform capability "
        "(Dynamic Tables, Cortex Search, Streamlit, and so on), industry skills guide Cortex "
        "Code toward the best approach for a healthcare or life sciences industry specific solution.")

    add_body(doc,
        "When you bring a business problem — for example, processing clinical documents, building an imaging "
        "pipeline, running a genomics analysis, or detecting adverse drug events — the framework "
        "with inclusion of appropriate Industry Skills can give Cortex Code the domain expertise "
        "it needs to tackle it correctly: the clinical standards, the data models, the governance "
        "requirements, and the sequencing of Snowflake platform capabilities that the problem calls "
        "for. You get a working solution faster because the deep industry/domain knowledge is already built in.")

    add_body(doc,
        "The interaction is conversational. You describe the business problem; Cortex Code, "
        "guided by the industry skills, builds a plan, gets your approval, and executes. "
        "All workflows involving clinical data or patient information enforce mandatory "
        "confirmation gates before any data is created or modified. Every decision point "
        "is yours to approve.")

    add_spacer(doc)

    doc.add_page_break()

    # ── SECTION 2 — ONE-TIME SETUP ────────────────────────────────────
    add_heading(doc, "Section 2 — One-Time Setup", 1)
    add_body(doc,
        "Complete these steps once to load the Health Sciences Industry Skills Framework "
        "into Cortex Code. After setup, you interact entirely through conversation.")

    add_heading(doc, "2.1  Prerequisites", 2)
    add_bullet(doc, "Cortex Code CLI installed (see Snowflake documentation for installation)")
    add_bullet(doc, "Access to the GitHub repo into which the Industry Skills are packaged")
    add_bullet(doc, "Snowflake account with Cortex AI features enabled")
    add_spacer(doc)

    add_heading(doc, "2.2  Create the Orchestrator Profile", 2)
    add_body(doc,
        "A profile is a one-time setup that tells Cortex Code which orchestrator "
        "agent to use and which skills to load. Once the profile exists at "
        "~/.snowflake/cortex/profiles/health-sciences-incubator.json, every "
        "cortex --profile health-sciences-incubator  session loads the full "
        "Health Sciences framework automatically — no manual skill registration needed.")
    add_spacer(doc, 3)

    add_body(doc, "Step 1 — Check if the profile already exists:", bold=True)
    add_code(doc,
        "cortex profile list\n"
        "# If health-sciences-incubator appears — skip to Section 2.3.")
    add_spacer(doc, 4)

    add_body(doc, "Step 2 — If it does not exist, create it using one of these two options:", bold=True)
    add_spacer(doc, 3)

    add_callout(doc,
        "Open a standard Cortex Code session — no profile needed, just the "
        "base CLI that is already installed. Then ask:\n\n"
        "    \"Set up the health-sciences-incubator profile for me. "
        "The repo is github:Snowflake-Solutions/"
        "health-sciences-coco-skills-incubator\"\n\n"
        "Cortex Code will create the profile JSON file at the correct path. "
        "Verify with:  cortex profile list",
        bg=GUIDED_BG,
        bold_prefix="GUIDED (recommended)")
    add_spacer(doc, 3)

    add_callout(doc,
        "Run this from your terminal:",
        bg=CALLOUT_BG,
        bold_prefix="MANUAL")
    add_code(doc,
        "mkdir -p ~/.snowflake/cortex/profiles\n"
        "\n"
        "cat > ~/.snowflake/cortex/profiles/health-sciences-incubator.json << 'EOF'\n"
        "{\n"
        "  \"name\": \"health-sciences-incubator\",\n"
        "  \"description\": \"Industry Solutions Architect for Health Sciences on Snowflake\",\n"
        "  \"systemPromptRepo\": {\n"
        "    \"source\": \"github:Snowflake-Solutions/health-sciences-coco-skills-incubator/agents/health-sciences-incubator.md\",\n"
        "    \"ref\": \"main\"\n"
        "  },\n"
        "  \"skillRepos\": [\n"
        "    {\n"
        "      \"source\": \"github:Snowflake-Solutions/health-sciences-coco-skills-incubator/skills\",\n"
        "      \"ref\": \"main\"\n"
        "    }\n"
        "  ],\n"
        "  \"mcpServers\": {},\n"
        "  \"commandRepos\": [],\n"
        "  \"hooks\": null,\n"
        "  \"envVars\": {},\n"
        "  \"settingsOverrides\": {}\n"
        "}\n"
        "EOF")
    add_spacer(doc, 4)

    add_body(doc, "What the profile does:", bold=True)
    add_table(doc,
        headers=["Field", "What it does"],
        rows=[
            ["systemPromptRepo",
             "Points to the orchestrator agent markdown on GitHub. Fetched "
             "automatically on launch — no local copy needed. This is the "
             "routing brain: skill taxonomy, HIPAA guardrails, domain routing rules."],
            ["skillRepos",
             "Declares the GitHub skill directory. All hcls-* skills are "
             "cloned into a local cache on first launch automatically. "
             "No manual cortex skill add needed."],
        ],
        col_widths=[1.8, 4.7])
    add_spacer(doc, 3)

    add_body(doc, "Step 3 — Verify the profile is wired correctly:", bold=True)
    add_code(doc,
        "cortex profile show health-sciences-incubator\n"
        "# Should show: Skills (1 repos) with the GitHub source")
    add_spacer(doc, 4)

    add_heading(doc, "2.3  Launch with the Profile", 2)
    add_code(doc, "cortex --profile health-sciences-incubator")
    add_spacer(doc)

    add_heading(doc, "2.4  Validate the Setup", 2)
    add_table(doc,
        headers=["Check", "Command", "Expected result"],
        rows=[
            ["Domain skills loaded",    "/skill",   "Health sciences domain skills listed"],
            ["Orchestrator agent loaded", "/agents", "health-sciences-incubator listed"],
        ],
        col_widths=[1.8, 1.4, 3.3])

    add_heading(doc, "2.5  Keeping Up to Date", 2)
    add_body(doc, "When the repo is updated, sync your local cache with one command:")
    add_code(doc, "cortex profile sync health-sciences-incubator")
    add_spacer(doc)

    add_heading(doc, "2.6  Optional: Activate Knowledge Sources", 2)
    add_body(doc,
        "Two types of knowledge can optionally be activated to make the framework "
        "more accurate and production-aligned. Both are opt-in — every skill "
        "functions without them, falling back to built-in reference definitions.")
    add_spacer(doc, 4)

    add_heading(doc, "2.6.1  Public Domain Knowledge", 3)
    add_body(doc,
        "Some skills can be enriched with shared domain knowledge available as "
        "Cortex Search Services on the Snowflake Marketplace. Installing one adds "
        "evidence grounding to relevant workflows — for example, annotating "
        "pharmacovigilance signals with published biomedical literature, or "
        "benchmarking a trial protocol against similar registered trials.")
    add_table(doc,
        headers=["Knowledge Source", "What it enriches", "Marketplace listing name"],
        rows=[
            ["PubMed Biomedical Research Corpus",
             "Pharmacovigilance, clinical NLP, imaging analytics, research problem selection",
             "PubMed Biomedical Research Corpus"],
            ["Clinical Trials Research Database",
             "Trial protocol design, claims feasibility analysis, survival analysis benchmarking",
             "Clinical Trials Research Database"],
        ],
        col_widths=[1.9, 2.8, 1.9])
    add_body(doc, "To install:", bold=True)
    add_bullet(doc, "Navigate to Data Products > Marketplace in Snowflake")
    add_bullet(doc, "Search for the listing name in the table above and click Get")
    add_bullet(doc,
        "No further configuration is needed. Skills auto-detect the installation "
        "via a preflight probe the next time a relevant workflow runs.")
    add_callout(doc,
        "The examples above are illustrative — additional knowledge sources may be "
        "available for other domains. Ask the orchestrator "
        "\"What knowledge sources can I activate?\" at the start of a session to "
        "discover what is available for your specific domain.",
        bg=CALLOUT_BG)
    add_spacer(doc, 6)

    add_heading(doc, "2.6.2  Enterprise Data Model Knowledge", 3)
    add_body(doc,
        "Skills that build SQL pipelines, DDL, or data extraction workflows can "
        "optionally use your enterprise data model — your actual table names, column "
        "definitions, PHI flags, and schema relationships. Without this, skills "
        "generate solutions using built-in clinical reference schemas. With it, they "
        "generate SQL targeting your exact production tables.")
    add_body(doc,
        "This is set up once per skill domain. You load your schema documentation "
        "into a Snowflake reference table and build a Cortex Search Service on top "
        "of it. The skill queries this service at Step 0 of every session to ground "
        "all SQL and DDL generation in your actual data model.")
    add_body(doc,
        "For example, the following domains currently support enterprise data model knowledge:",
        bold=False)
    add_table(doc,
        headers=["Skill domain", "What the enterprise model covers"],
        rows=[
            ["Clinical NLP",
             "Your NLP output schema: extraction tables, column types, code system mappings, PHI columns"],
            ["Clinical Documents",
             "Your document extraction schema: parsed document tables, classification outputs, field definitions"],
            ["Medical Imaging",
             "Your DICOM metadata schema: tag tables, study/series/instance structure, annotation tables"],
        ],
        col_widths=[2.0, 4.6])
    add_callout(doc,
        "Other domain skills may also support enterprise data model knowledge. "
        "Ask the orchestrator \"Does this skill support enterprise data model knowledge?\" "
        "at the start of a session to check for your specific domain.",
        bg=CALLOUT_BG)
    add_body(doc, "One-time setup for each domain:", bold=True)
    add_bullet(doc,
        "Export your schema to a CSV file with columns: table name, column name, "
        "data type, description, PHI flag, and any relationships")
    add_bullet(doc,
        "Load to Snowflake: COPY INTO a reference table in a DATA_MODEL_KNOWLEDGE schema")
    add_bullet(doc,
        "Build the Cortex Search Service: CREATE CORTEX SEARCH SERVICE over the reference table")
    add_bullet(doc,
        "No profile or skill changes needed — the skill's Step 0 preflight "
        "auto-discovers the service and activates it")
    add_callout(doc,
        "If enterprise data model knowledge is not set up, skills fall back to "
        "their built-in reference schema (FHIR-aligned, clinically standard) and "
        "still produce correct solutions. Enterprise DMK is optional but recommended "
        "when you need solutions aligned to your exact production schema.",
        bg=CALLOUT_BG)

    doc.add_page_break()

    # ── SECTION 3 — HOW TO INTERACT ───────────────────────────────────
    add_heading(doc, "Section 3 — How to Interact with the Framework", 1)
    add_spacer(doc)

    add_heading(doc, "3.1  Describe your business problem in plain language", 2)
    add_body(doc,
        "No special syntax is needed. Describe the business problem: the data you have, "
        "the outcome you need, and any relevant context such as PHI sensitivity, target "
        "schema, or downstream use. Cortex Code, guided by the industry skills, will "
        "identify the right domain pattern and build a plan. The more context you give, "
        "the more accurate the plan.")
    add_spacer(doc, 3)

    add_callout(doc,
        "Not sure where to start? Explore what the framework can do before you "
        "have a specific problem in mind:\n\n"
        "    /skill\n"
        "    Lists all active domain skills loaded in the current session.\n\n"
        "    \"What can you help me with for [segment or domain]?\"\n"
        "    The orchestrator describes available patterns for that area.\n\n"
        "    \"What workflows are available for genomics data?\"\n"
        "    Returns a capability overview for that domain.\n\n"
        "You can also describe a data source or outcome without a specific goal "
        "and the orchestrator will suggest where to start.",
        bg=CALLOUT_BG)
    add_spacer(doc, 3)

    add_body(doc, "Examples of effective prompts:", bold=True)
    add_code(doc,
        '"We have radiology reports in a Snowflake stage. Extract structured\n'
        ' findings and build a semantic search interface for our radiologists."')
    add_spacer(doc, 3)
    add_code(doc,
        '"I need to transform FHIR R4 bundles from our EHR system into\n'
        ' relational tables and map them to a standard clinical data model."')
    add_spacer(doc, 3)
    add_code(doc,
        '"Run a survival analysis on the patient cohort data already loaded\n'
        ' in our Snowflake database and visualise the Kaplan-Meier curve."')
    add_spacer(doc)

    add_heading(doc, "3.2  Phase 1 — The Plan (always comes first)", 2)
    add_body(doc,
        "The orchestrator always responds with a structured plan table before executing "
        "anything. You review and approve the plan — nothing happens until you do.")
    add_spacer(doc, 3)

    add_body(doc, "Plan table format:", bold=True)
    if plan_table_raw:
        add_code(doc, plan_table_raw)
    else:
        add_code(doc,
            "| Step | Skill              | What it produces            | Depends on | Governance |\n"
            "|------|--------------------|-----------------------------|------------|------------|")
    add_spacer(doc, 4)

    add_body(doc, "How to read the plan:", bold=True)
    add_bullet(doc, "One row = one logical step. Internal SQL operations are not listed — they run inside each step.")
    add_bullet(doc, "Governance column flags any step that creates or exposes PHI or PII.")
    add_bullet(doc, "Depends on: a step will not run if a prior required step failed.")
    add_spacer(doc)

    add_heading(doc, "3.3  Approving, modifying, or rejecting the plan", 2)
    add_table(doc,
        headers=["Your response", "What happens next"],
        rows=[
            ["Approve",  "Phase 2 begins — steps execute in the approved order"],
            ["Modify",   "Describe the change; the plan updates and is re-presented for approval"],
            ["Reject",   "Describe what you want instead; a new plan is built from scratch"],
        ],
        col_widths=[1.5, 5.0])

    add_heading(doc, "3.4  When is the full plan gate required?", 2)
    add_table(doc,
        headers=["Situation", "Gate required"],
        rows=[
            ["Loading, creating, or modifying data",                    "Full plan gate"],
            ["Multi-step pipeline or multi-skill composition",           "Full plan gate"],
            ["Data acquisition: downloads, API calls, staging",         "Full plan gate"],
            ["Informational question only (no execution)",              "Lightweight confirmation"],
            ["Follow-up step within an already-approved plan",          "Lightweight confirmation"],
        ],
        col_widths=[4.2, 2.3])

    add_heading(doc, "3.5  Phase 2 — Execute", 2)
    add_body(doc,
        "Steps run in approved order. The orchestrator reports back after each step "
        "so you can review the output and course-correct before the next step begins. "
        "After all approved steps complete, it presents available next steps rather "
        "than proceeding automatically.")

    doc.add_page_break()

    # ── SECTION 4 — FRAMEWORK BEHAVIORS ──────────────────────────────
    add_heading(doc, "Section 4 — Framework Behaviors to Expect", 1)
    add_spacer(doc)

    add_heading(doc, "4.1  Cortex Code handles routing — you don't", 2)
    add_body(doc,
        "Some industry skill areas cover multiple sub-capabilities: they accept a "
        "high-level intent and Cortex Code, guided by the skill, automatically routes "
        "to the right sub-capability — parsing, ingestion, analytics, governance, ML, "
        "search — based on what you asked for. Other areas execute directly without a "
        "routing step. Either way the interaction pattern is identical: describe the "
        "problem, review the plan, approve, execute. You never need to know which "
        "sub-capability was selected or how the routing decision was made.")
    add_spacer(doc)

    add_heading(doc, "4.2  Evidence grounding (optional knowledge sources)", 2)
    add_body(doc,
        "Two types of optional knowledge can enrich domain workflows: public domain "
        "knowledge from the Snowflake Marketplace (biomedical literature, clinical trial "
        "data) and enterprise data model knowledge loaded from your own schema "
        "documentation. For setup instructions for both, see Section 2.6.")
    add_body(doc,
        "Before using any knowledge source, the skill runs a preflight check:")
    add_table(doc,
        headers=["Preflight result", "What happens"],
        rows=[
            ["READY",   "Knowledge source available — outputs enriched with domain evidence"],
            ["MISSING", "Knowledge source not installed — skill runs without enrichment; setup instructions printed inline"],
            ["ERROR",   "Connection issue at runtime — skill continues with graceful fallback"],
        ],
        col_widths=[1.6, 5.0])
    add_callout(doc,
        "No skill fails or stops because of a MISSING knowledge source. "
        "The framework always degrades gracefully and tells you how to enable the "
        "enrichment if you want it.")
    add_spacer(doc)

    add_heading(doc, "4.3  Mandatory confirmation gates (clinical and PHI-bearing workflows)", 2)
    add_body(doc,
        "For workflows involving clinical data or PHI, the framework enforces a "
        "three-layer guardrail system:")
    add_table(doc,
        headers=["Layer", "Mechanism", "What it does"],
        rows=[
            ["1", "Session rules",
             "Profile-level constraints applied to every conversation in the session"],
            ["2", "Confirmation gates",
             "Structured decision points the model cannot skip — each requires your explicit confirmation before the next phase runs"],
            ["3", "Execution hooks",
             "Hard blocks on DDL or DML operations without prior explicit confirmation"],
        ],
        col_widths=[0.6, 2.0, 4.9])
    add_callout(doc,
        "MANDATORY STOP gates are intentional — not a bug. "
        "Each gate requires your explicit confirmation before execution proceeds. "
        "This prevents irreversible actions on clinical or PHI data without your sign-off. "
        "Do not try to pre-answer or bypass them — the framework will not skip a gate.",
        bg=WARNING_BG)

    doc.add_page_break()

    # ── SECTION 5 — WORKED EXAMPLE ────────────────────────────────────
    add_heading(doc, "Section 5 — Worked Example: End-to-End Conversation", 1)
    add_body(doc,
        "The following is a complete annotated conversation transcript showing exactly "
        "what a user types and exactly what the orchestrator responds with at each turn. "
        "Annotations explain which framework mechanism fired and why.")
    add_spacer(doc, 3)

    add_body(doc, "Scenario:", bold=True)
    add_body(doc,
        "A hospital data team has approximately 150 clinical documents — discharge "
        "summaries and pathology reports — stored in a Snowflake internal stage. "
        "They need structured clinical data extracted from those documents and a "
        "semantic search interface built so their analysts can query them.")

    add_spacer(doc, 6)

    # ── Turn 1 ──
    add_turn_user(doc,
        '"Our clinical team has uploaded about 150 discharge summaries and pathology '
        'reports to a Snowflake internal stage. We need to extract structured clinical '
        'data from these documents and build a way for our analysts to search them semantically."')
    add_annotation(doc,
        "The user describes the business problem in plain language — data source, what "
        "they have, what outcome they need. No skill name or technical instruction required.")

    # ── Turn 2 ──
    add_turn_orch(doc, "Phase 1 — Plan", [
        ("I understand the goal. Before running anything, here is my proposed plan:\n", False, False),
        ("Sub-industry: Provider — Clinical Data Management", False, False),
        ("Pattern: Document extraction pipeline + semantic search\n", False, False),
        ("| Step | Skill                          | What it produces                        | Depends on | Governance |", True, False),
        ("|------|--------------------------------|-----------------------------------------|------------|------------|", True, False),
        ("|  1   | Clinical document extraction   | Structured tables from parsed documents | —          | PHI present|", True, False),
        ("|  2   | Semantic search setup          | Cortex Search Service over content      | Step 1     | PHI present|", True, False),
        ("\nBoth steps involve PHI — governance guardrails will be active throughout.", False, False),
        ("Shall I proceed with this plan, or would you like to modify it?", False, False),
    ])
    add_annotation(doc,
        "This is Phase 1 — the Plan gate. The orchestrator has identified the appropriate "
        "domain capabilities, built the plan table, and is waiting for explicit approval. "
        "Nothing has executed. No SQL has run. No data has changed.")

    # ── Turn 3 ──
    add_turn_user(doc, '"Looks good, go ahead."')
    add_annotation(doc,
        "User approves. Phase 2 begins. This is the first point at which the orchestrator "
        "invokes any domain skill.")

    # ── Turn 4 ──
    add_turn_orch(doc, "MANDATORY STOP — Gate 1: Environment", [
        ("Before I run anything, I need to confirm your Snowflake environment.\n", False, False),
        ("Detected:", False, True),
        ("  Account:    MYCOMPANY-PROD", True, False),
        ("  Role:       DATA_ENGINEER", True, False),
        ("  Warehouse:  COMPUTE_WH\n", True, False),
        ("Is this the correct environment? Would you like to use a different warehouse?", False, False),
    ])
    add_annotation(doc,
        "Gate 1 is a mandatory stop — the skill cannot proceed until the environment is "
        "explicitly confirmed. This prevents accidental execution against the wrong account "
        "or role. Even if the user mentioned the environment earlier, the gate still fires.")

    # ── Turn 5 ──
    add_turn_user(doc, '"Yes, that is correct. Use COMPUTE_WH."')

    # ── Turn 6 ──
    add_turn_orch(doc, "MANDATORY STOP — Gate 2: Target Objects", [
        ("Where are the source documents? Please confirm the target database, schema, and stage:\n", False, False),
        ("  Database:  ?", True, False),
        ("  Schema:    ?", True, False),
        ("  Stage:     ?", True, False),
    ])
    add_annotation(doc,
        "Gate 2 asks for the exact Snowflake objects before any DDL or COPY INTO runs. "
        "This ensures the pipeline is built in the right location.")

    # ── Turn 7 ──
    add_turn_user(doc,
        '"Use database HOSPITAL_DB, schema CLINICAL_DATA, stage DISCHARGE_DOCS_STAGE."')

    # ── Turn 8 ──
    add_turn_orch(doc, "MANDATORY STOP — Gate 3: Final Confirmation", [
        ("I will use the following configuration:\n", False, False),
        ("  Database:  HOSPITAL_DB", True, False),
        ("  Schema:    CLINICAL_DATA", True, False),
        ("  Stage:     DISCHARGE_DOCS_STAGE\n", True, False),
        ("I will run the extraction pipeline first (Step 1), then build the search "
         "service (Step 2).", False, False),
        ("Ready to start?", False, False),
    ])
    add_annotation(doc,
        "Gate 3 is the final confirmation before DDL runs. The orchestrator restates the "
        "full configuration so the user can catch any error before irreversible changes begin.")

    # ── Turn 9 ──
    add_turn_user(doc, '"Yes, go ahead."')

    # ── Turn 10 ──
    add_turn_orch(doc, "Step 1 complete", [
        ("Extraction pipeline finished:\n", False, True),
        ("  — 150 documents parsed", True, False),
        ("  — 3 tables created: CLINICAL_DOCS_RAW, CLINICAL_DOCS_CLASSIFIED, CLINICAL_DOCS_EXTRACTED", True, False),
        ("  — Document types: 89 discharge summaries, 61 pathology reports", True, False),
        ("  — 2,847 structured clinical entities extracted\n", True, False),
        ("Starting Step 2: building Cortex Search Service...\n", False, False),
        ("Step 2 complete:", False, True),
        ("  — Cortex Search Service created over CLINICAL_DOCS_EXTRACTED", True, False),
        ("  — Service is ready for semantic queries\n", True, False),
        ("Both steps in the approved plan are complete. What would you like to do next?", False, False),
    ])
    add_annotation(doc,
        "The orchestrator reports after each step so the user can review before proceeding. "
        "After all approved steps complete, it pauses and offers options — consistent with "
        "the Plan-then-Execute model throughout.")

    # ── Turn 11 ──
    add_turn_user(doc, '"What can I do with this now?"')

    # ── Turn 12 ──
    add_turn_orch(doc, "Next steps", [
        ("From here, the natural next steps are:\n", False, False),
        ("1. Conversational agent — build a Cortex Agent so analysts can query "
         "documents in natural language", False, False),
        ("2. Document viewer — deploy a Streamlit interface for browsing, annotating, "
         "and reviewing documents", False, False),
        ("3. Governance layer — apply PHI masking policies, row-access controls, "
         "and audit trails to the extracted tables", False, False),
        ("4. Downstream analytics — run analytics on the extracted entities "
         "(diagnosis trends, medication frequency, length-of-stay patterns)", False, False),
        ("\nWhich would you like to explore?", False, False),
    ])
    add_annotation(doc,
        "The orchestrator does not automatically proceed to new capabilities. "
        "It presents options and waits — a new Plan-then-Execute cycle begins "
        "when the user selects a next step.")

    add_spacer(doc, 8)
    add_callout(doc,
        "This transcript illustrates the consistent interaction pattern regardless of the "
        "healthcare domain. The same flow — describe the business problem, review the plan, "
        "approve, confirm gates, execute, choose next steps — applies whether you are "
        "working with imaging data, genomics pipelines, claims analysis, or drug safety "
        "workflows. The industry skills give Cortex Code the domain knowledge it needs; "
        "you stay in control of every decision point.",
        bg=CALLOUT_BG)

    doc.add_page_break()

    # ── SECTION 6 — TROUBLESHOOTING ───────────────────────────────────
    add_heading(doc, "Section 6 — Troubleshooting", 1)
    add_body(doc, "Common symptoms, causes, and fixes at the framework level.")
    add_spacer(doc)

    add_table(doc,
        headers=["Symptom", "Likely cause", "Fix"],
        rows=[
            ["Skills not found / orchestrator does not recognise the domain",
             "Profile not active or not synced after a repo update",
             "Run: cortex --profile health-sciences-incubator\n"
             "Then: cortex profile sync health-sciences-incubator"],

            ["Knowledge source shows as MISSING",
             "Optional domain knowledge listing not installed from Snowflake Marketplace",
             "The skill continues without it. Install the Marketplace listing "
             "to enable evidence enrichment."],

            ["Confirmation gates keep re-asking for information already provided",
             "Each gate is structurally enforced and cannot read prior conversation context",
             "Confirm each gate individually. This is by design for PHI-bearing workflows "
             "— it is not a bug."],

            ["Orchestrator routed to the wrong domain capability",
             "Request lacked enough domain-specific context",
             "Add domain keywords: the data type, clinical standard, or specific outcome "
             "you need (e.g., 'FHIR', 'DICOM', 'survival analysis', 'adverse event')."],

            ["DDL or data change ran without confirmation",
             "Execution hooks not active in the project",
             "Add hooks.json to the project root. "
             "See the repo for a reference file."],

            ["Plan table did not appear — skill ran immediately",
             "Request was interpreted as informational (no execution needed)",
             "Rephrase to make execution intent clear: "
             "use words like 'build', 'create', 'load', 'extract', 'run'."],

            ["Profile not found on launch",
             "Profile JSON file missing at expected path",
             "Follow Section 2.2 to create:\n"
             "~/.snowflake/cortex/profiles/health-sciences-incubator.json"],
        ],
        col_widths=[2.1, 2.2, 3.2])

    add_spacer(doc)
    add_callout(doc,
        "For issues not listed here, refer to README.md in the GitHub repo "
        "(Snowflake-Solutions/health-sciences-coco-skills-incubator) "
        "for the full troubleshooting section.")

    # ── SAVE ──────────────────────────────────────────────────────────
    out_dir  = os.path.join(BASE, "documentation")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "User_Guide.docx")
    doc.save(out_path)
    print(f"\n{'='*60}")
    print(f"Saved: {out_path}")
    print(f"{'='*60}")
    return out_path


if __name__ == "__main__":
    main()
