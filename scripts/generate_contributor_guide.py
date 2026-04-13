#!/usr/bin/env python3
"""
Generate documentation/Contributor_Guide.docx
Health Sciences Industry Skills Framework — Contributor Guide

Source dependencies (framework-level only):
  - skills/hcls-cross-skill-development/SKILL.md    : workflow steps, taxonomy, stopping points
  - HCLS_INDUSTRY_SKILL_BEST_PRACTICES.md           : archetypes, checklist, pitfalls
  - templates/skills_incubator.yaml                 : live YAML registration examples
  - scripts/generate_orchestrators.py               : DOMAIN_ORDER valid domain values

Usage: python3 scripts/generate_contributor_guide.py
Output: documentation/Contributor_Guide.docx
"""

import os
import re
import yaml
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
SUCCESS_BG    = "EAF7EA"
GUIDED_BG     = "E8F5E9"
STEP_BG       = "F0F4F8"


# ── File helpers ──────────────────────────────────────────────────────────────
def read_file(path):
    with open(os.path.join(BASE, path), encoding="utf-8") as f:
        return f.read()


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
    run.font.name  = "Calibri"
    run.font.bold  = True
    run.font.size  = Pt({1: 17, 2: 13, 3: 11}.get(level, 11))
    run.font.color.rgb = {1: NAVY, 2: MID_BLUE, 3: NAVY}.get(level, NAVY)
    return para


def add_body(doc, text, bold=False, italic=False, color=None):
    para = doc.add_paragraph()
    para.paragraph_format.space_before = Pt(2)
    para.paragraph_format.space_after  = Pt(5)
    run = para.add_run(text)
    run.font.name   = "Calibri"
    run.font.size   = Pt(11)
    run.font.bold   = bold
    run.font.italic = italic
    if color:
        run.font.color.rgb = color
    return para


def add_bullet(doc, text, bold_prefix=None, indent=0):
    style = "List Bullet 2" if indent else "List Bullet"
    para  = doc.add_paragraph(style=style)
    para.paragraph_format.space_before = Pt(1)
    para.paragraph_format.space_after  = Pt(2)
    if bold_prefix:
        r1 = para.add_run(bold_prefix)
        r1.font.bold = True; r1.font.name = "Calibri"; r1.font.size = Pt(11)
        r2 = para.add_run(" " + text)
        r2.font.name = "Calibri"; r2.font.size = Pt(11)
    else:
        run = para.add_run(text)
        run.font.name = "Calibri"; run.font.size = Pt(11)
    return para


def add_numbered(doc, number, text):
    para = doc.add_paragraph(style="List Number")
    para.paragraph_format.space_before = Pt(2)
    para.paragraph_format.space_after  = Pt(3)
    run = para.add_run(text)
    run.font.name = "Calibri"; run.font.size = Pt(11)
    return para


def add_code(doc, text):
    for line in text.split("\n"):
        para = doc.add_paragraph()
        para.paragraph_format.space_before = Pt(0)
        para.paragraph_format.space_after  = Pt(0)
        para.paragraph_format.left_indent  = Inches(0.25)
        run = para.add_run(line if line else " ")
        run.font.name      = "Courier New"
        run.font.size      = Pt(9)
        run.font.color.rgb = DARK_GRAY
        set_para_bg(para, LIGHT_GRAY)


def add_callout(doc, text, bg=CALLOUT_BG, bold_prefix=None):
    para = doc.add_paragraph()
    para.paragraph_format.space_before = Pt(6)
    para.paragraph_format.space_after  = Pt(6)
    para.paragraph_format.left_indent  = Inches(0.2)
    para.paragraph_format.right_indent = Inches(0.2)
    if bold_prefix:
        r1 = para.add_run(bold_prefix + "  ")
        r1.font.name = "Calibri"; r1.font.size = Pt(10); r1.font.bold = True
    run = para.add_run(text)
    run.font.name = "Calibri"; run.font.size = Pt(10); run.font.italic = True
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
        run.font.bold = True; run.font.name = "Calibri"; run.font.size = Pt(10)
    for ri, row_data in enumerate(rows):
        row = table.rows[ri + 1]
        bg  = "FFFFFF" if ri % 2 == 0 else "F2F2F2"
        for ci, cell_text in enumerate(row_data):
            cell = row.cells[ci]
            cell.text = ""
            set_cell_bg(cell, bg)
            run = cell.paragraphs[0].add_run(str(cell_text))
            run.font.name = "Calibri"; run.font.size = Pt(10)
    if col_widths:
        for row in table.rows:
            for i, w in enumerate(col_widths):
                if i < len(row.cells):
                    row.cells[i].width = Inches(w)
    add_spacer(doc, 4)
    return table


def add_step_header(doc, step_num, title):
    """Navy-background step banner: 'Step N — Title'."""
    para = doc.add_paragraph()
    para.paragraph_format.space_before = Pt(12)
    para.paragraph_format.space_after  = Pt(0)
    run = para.add_run(f"  Step {step_num} — {title}")
    run.font.name      = "Calibri"
    run.font.size      = Pt(11)
    run.font.bold      = True
    run.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
    set_para_bg(para, NAVY_HEX)


def add_stop_gate(doc, text):
    """Orange-background STOP gate callout."""
    para = doc.add_paragraph()
    para.paragraph_format.space_before = Pt(4)
    para.paragraph_format.space_after  = Pt(8)
    para.paragraph_format.left_indent  = Inches(0.2)
    para.paragraph_format.right_indent = Inches(0.2)
    run = para.add_run("STOP  " + text)
    run.font.name      = "Calibri"
    run.font.size      = Pt(10)
    run.font.bold      = True
    run.font.color.rgb = RGBColor(0x8B, 0x45, 0x13)
    set_para_bg(para, WARNING_BG)


def add_checklist_item(doc, text):
    para = doc.add_paragraph(style="List Bullet")
    para.paragraph_format.space_before = Pt(1)
    para.paragraph_format.space_after  = Pt(1)
    run = para.add_run(text)
    run.font.name = "Calibri"; run.font.size = Pt(10)
    return para


def add_operation_header(doc, label, subtitle):
    """Section-level operation banner (Add / Update / Remove)."""
    para = doc.add_paragraph()
    para.paragraph_format.space_before = Pt(8)
    para.paragraph_format.space_after  = Pt(2)
    run = para.add_run(f"  {label}")
    run.font.name      = "Calibri"
    run.font.size      = Pt(13)
    run.font.bold      = True
    run.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
    set_para_bg(para, NAVY_HEX)
    if subtitle:
        sub = doc.add_paragraph()
        sub.paragraph_format.space_before = Pt(2)
        sub.paragraph_format.space_after  = Pt(6)
        sub.paragraph_format.left_indent  = Inches(0.1)
        r = sub.add_run(subtitle)
        r.font.name = "Calibri"; r.font.size = Pt(10); r.font.italic = True
        r.font.color.rgb = MID_GRAY


def add_sub_case(doc, number, title):
    """Mid-blue sub-case header inside Update/Remove sections."""
    para = doc.add_paragraph()
    para.paragraph_format.space_before = Pt(10)
    para.paragraph_format.space_after  = Pt(4)
    run = para.add_run(f"  {number}  {title}")
    run.font.name      = "Calibri"
    run.font.size      = Pt(11)
    run.font.bold      = True
    run.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
    set_para_bg(para, "2D6A9F")


# ── Extraction helpers ─────────────────────────────────────────────────────────
def extract_workflow_steps(skill_md):
    steps = []
    pattern = re.compile(
        r"### Step (\d+):\s*(.+?)\n([\s\S]+?)(?=### Step \d+:|## Stopping Points|$)"
    )
    for m in pattern.finditer(skill_md):
        steps.append((int(m.group(1)), m.group(2).strip(), m.group(3).strip()))
    return steps


def extract_taxonomy_table(skill_md):
    rows, in_table = [], False
    for line in skill_md.split("\n"):
        if "Sub-Industry" in line and "Naming Pattern" in line:
            in_table = True; continue
        if in_table:
            if line.startswith("|---"): continue
            if not line.startswith("|"): break
            cells = [c.strip() for c in line.strip("|").split("|")]
            if len(cells) >= 3:
                rows.append(cells[:3])
    return rows


def extract_stopping_points(skill_md):
    match = re.search(r"## Stopping Points\n([\s\S]+?)(?=\n## |\Z)", skill_md)
    if not match:
        return []
    return [l[2:].strip() for l in match.group(1).split("\n")
            if l.strip().startswith("- ")]


def extract_domain_order(gen_script):
    match = re.search(r"DOMAIN_ORDER\s*=\s*\[([\s\S]+?)\]", gen_script)
    if not match:
        return []
    return [m.strip('"').strip("'")
            for m in re.findall(r'["\']([^"\']+)["\']', match.group(1))]


def extract_yaml_example(yaml_text, skill_name):
    lines, result, capture = yaml_text.split("\n"), [], False
    for line in lines:
        if re.match(rf"^\s\s{re.escape(skill_name)}:", line):
            capture = True
        if capture:
            result.append(line)
            if len(result) > 1 and re.match(r"^\s\shcls-", line) and skill_name not in line:
                result.pop(); break
    return "\n".join(result).rstrip()


def extract_checklist(best_practices):
    match = re.search(
        r"## 15\. Quick Reference Checklist\n([\s\S]+?)(?=\n## \d+\.|\Z)",
        best_practices
    )
    if not match:
        return []
    groups, current = [], None
    for line in match.group(1).split("\n"):
        h = re.match(r"^### (.+)$", line.strip())
        if h:
            current = (h.group(1), []); groups.append(current)
        elif current and re.match(r"^- \[[ x]\] ", line.strip()):
            current[1].append(re.sub(r"^- \[[ x]\] ", "", line.strip()))
        elif current and re.match(r"^\*\*(.+)\*\*:", line.strip()):
            sub = re.sub(r"\*\*", "", line.strip()).rstrip(":")
            current[1].append(f"--- {sub} ---")
    return groups


def extract_pitfalls(best_practices):
    match = re.search(
        r"## 14\. Common Pitfalls\n([\s\S]+?)(?=\n## \d+\.|\Z)",
        best_practices
    )
    if not match:
        return []
    groups, current = [], None
    for line in match.group(1).split("\n"):
        h = re.match(r"^### (.+)$", line.strip())
        if h:
            current = (h.group(1), []); groups.append(current)
        elif current:
            m = re.match(r"^\d+\.\s+\*\*(.+?)\*\*\s*[—–-]\s*(.+)$", line.strip())
            if m:
                current[1].append((m.group(1), m.group(2)))
    return groups


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Health Sciences Industry Skills Framework — Contributor Guide Generator")
    print("=" * 60)

    print("\nLoading source files...")
    skill_dev_md   = read_file("skills/hcls-cross-skill-development/SKILL.md")
    best_practices = read_file("HCLS_INDUSTRY_SKILL_BEST_PRACTICES.md")
    skills_yaml    = read_file("templates/skills_incubator.yaml")
    gen_script     = read_file("scripts/generate_orchestrators.py")

    taxonomy_rows    = extract_taxonomy_table(skill_dev_md)
    domain_order     = extract_domain_order(gen_script)
    standalone_yaml  = extract_yaml_example(skills_yaml, "hcls-pharma-dsafety-pharmacovigilance")
    cke_yaml         = extract_yaml_example(skills_yaml, "hcls-cross-cke-pubmed")
    checklist        = extract_checklist(best_practices)
    pitfalls         = extract_pitfalls(best_practices)

    print(f"  Taxonomy rows extracted:    {len(taxonomy_rows)}")
    print(f"  Domain order entries:       {len(domain_order)}")
    print(f"  Checklist groups:           {len(checklist)}")
    print(f"  Pitfall groups:             {len(pitfalls)}")

    doc = Document()
    sec = doc.sections[0]
    sec.top_margin    = Inches(1.0)
    sec.bottom_margin = Inches(1.0)
    sec.left_margin   = Inches(1.2)
    sec.right_margin  = Inches(1.2)

    # ── COVER PAGE ────────────────────────────────────────────────────────────
    for _ in range(4):
        doc.add_paragraph()

    t = doc.add_paragraph()
    t.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = t.add_run("Health Sciences Industry Skills Framework")
    r.font.name = "Calibri"; r.font.size = Pt(28)
    r.font.bold = True; r.font.color.rgb = SNOW_BLUE

    s = doc.add_paragraph()
    s.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = s.add_run("Contributor Guide")
    r.font.name = "Calibri"; r.font.size = Pt(22)
    r.font.bold = True; r.font.color.rgb = NAVY

    doc.add_paragraph()

    tl = doc.add_paragraph()
    tl.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = tl.add_run("How to add, update, and remove industry skills")
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

    # ── SECTION 1 — OVERVIEW ─────────────────────────────────────────────────
    add_heading(doc, "Section 1 — Overview", 1)
    add_body(doc,
        "This guide is for engineers, solution engineers, and field developers "
        "who want to add a new industry skill, update an existing one, or remove "
        "one that is no longer needed. It is organized by task — find the section "
        "that matches what you are trying to do and follow only that section.")
    add_body(doc,
        "The User Guide covers how to interact with the framework to solve "
        "healthcare and life sciences problems. This guide covers how to build "
        "and maintain the skills that power those interactions.")
    add_spacer(doc, 4)

    # ── Section 1.1 — Profile prerequisite ───────────────────────────────────
    add_heading(doc, "1.1  Before you start: set up the incubator profile", 2)
    add_body(doc,
        "Every  cortex --profile health-sciences-incubator  command in this "
        "guide requires a profile to exist on your machine. A profile is a JSON "
        "config file at  ~/.snowflake/cortex/profiles/health-sciences-incubator.json  "
        "that tells Cortex Code which orchestrator agent to use as its system "
        "prompt and which skill repo to load. This is a one-time setup — once "
        "done, every command in this guide works without additional configuration.")
    add_spacer(doc, 3)

    add_body(doc, "Step 1 — Check if the profile already exists:", bold=True)
    add_code(doc,
        "cortex profile list\n"
        "# If health-sciences-incubator appears in the list — you are ready.\n"
        "# Skip the setup below and continue with Section 2.")
    add_spacer(doc, 4)

    add_body(doc, "Step 2 — If it does not exist, set it up using one of these two options:", bold=True)
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

    add_heading(doc, "What a contributor produces", 2)
    add_table(doc,
        headers=["Deliverable", "Description", "Required"],
        rows=[
            ["SKILL.md",
             "Skill definition: YAML frontmatter (name, description, triggers, "
             "platform affinities) + workflow body",
             "Yes — every skill"],
            ["Registry entry",
             "Entry in templates/skills_incubator.yaml with triggers, "
             "description, and domain",
             "Yes — every skill"],
            ["Regenerated orchestrator",
             "agents/health-sciences-incubator.md rebuilt via "
             "python3 scripts/generate_orchestrators.py",
             "Yes — whenever YAML changes"],
            ["skill_evidence.yaml",
             "Promotion lifecycle tracker: draft → review → staging → production",
             "Recommended"],
            ["CROSS_VALIDATION_REPORT.md",
             "Audit results documenting validation against this guide and "
             "HCLS_INDUSTRY_SKILL_BEST_PRACTICES.md",
             "Recommended"],
        ],
        col_widths=[1.8, 3.5, 1.2])
    add_spacer(doc)

    add_heading(doc, "Two ways to contribute: guided or manual", 2)
    add_body(doc,
        "Every contribution operation in this guide can be performed in two ways. "
        "Choose the approach that fits your workflow:")
    add_spacer(doc, 3)
    add_table(doc,
        headers=["Approach", "How", "Best for"],
        rows=[
            ["Guided (recommended)",
             "Start a Cortex Code session with the incubator profile and ask the "
             "orchestrator to walk through the operation. The hcls-cross-skill-"
             "development skill guides you step by step, confirms each decision, "
             "and generates file content interactively.",
             "New contributors, first skill, or any time you want guardrails"],
            ["Manual",
             "Edit files directly, run the orchestrator generator script, and "
             "test via the CLI. Faster for experienced contributors or scripted "
             "workflows.",
             "Experienced contributors, batch changes, CI/CD"],
        ],
        col_widths=[1.5, 3.5, 1.5])
    add_callout(doc,
        "Starting a guided session: run  cortex --profile health-sciences-incubator  "
        "then say \"I want to add a new industry skill for [your domain].\" "
        "The orchestrator routes to hcls-cross-skill-development which walks you "
        "through the full workflow.",
        bg=GUIDED_BG,
        bold_prefix="GUIDED")
    add_spacer(doc)

    add_heading(doc, "Relationship to the platform skill-development skill", 2)
    add_body(doc,
        "The hcls-cross-skill-development skill delegates to the platform "
        "skill-development skill for SKILL.md scaffolding and best-practices "
        "auditing. This guide covers the HCLS-specific steps: taxonomy placement, "
        "registry format, orchestrator regeneration, and CLI testing. Use both "
        "together for best results.")

    doc.add_page_break()

    # ── SECTION 2 — ADD A NEW SKILL ───────────────────────────────────────────
    add_heading(doc, "Section 2 — Add a New Skill", 1)
    add_body(doc,
        "Follow these 8 steps from first commit to merged PR. There are 4 "
        "mandatory STOP gates — confirm at each before proceeding. All reference "
        "material (archetype table, naming conventions, YAML examples, valid "
        "domains) is embedded inline at the step where it is needed.")
    add_spacer(doc, 4)

    # ── Step 1: Choose approach ───────────────────────────────────────────────
    add_step_header(doc, 1, "Choose Your Approach")
    add_spacer(doc, 3)
    add_callout(doc,
        "Option A — Guided: run  cortex --profile health-sciences-incubator  "
        "and say \"I want to add a new skill for [domain].\" "
        "Skip to Step 7 (Branch and PR) — the skill guides you through "
        "Steps 2–6 interactively.",
        bg=GUIDED_BG,
        bold_prefix="GUIDED")
    add_callout(doc,
        "Option B — Manual: continue with Steps 2–7 below.",
        bg=CALLOUT_BG,
        bold_prefix="MANUAL")
    add_spacer(doc, 4)

    # ── Step 2: Choose archetype ──────────────────────────────────────────────
    add_step_header(doc, 2, "Choose Your Archetype")
    add_spacer(doc, 3)
    add_body(doc,
        "The archetype determines how you structure SKILL.md and how the "
        "orchestrator routes to it. Pick the one that fits the complexity "
        "and nature of the domain capability you are building.")
    add_spacer(doc, 3)
    add_table(doc,
        headers=["Archetype", "When to use", "SKILL.md structure", "Examples"],
        rows=[
            ["Standalone",
             "Single cohesive domain workflow, one entry point, one output. "
             "No branching into distinct sub-workflows.",
             "One SKILL.md with full workflow steps. No sub-skills.",
             "hcls-provider-cdata-fhir\nhcls-pharma-dsafety-pharmacovigilance\n"
             "hcls-pharma-genomics-survival-analysis"],
            ["Router + Sub-skills",
             "Domain has 3 or more distinct concept categories or workflow "
             "stages that share a common data model or knowledge base.",
             "Parent SKILL.md routes intent to sub-skills. Each sub-skill has "
             "its own SKILL.md with parent_skill: declared in frontmatter.",
             "hcls-provider-cdata-clinical-nlp (15 sub-skills)\n"
             "hcls-provider-imaging (7 sub-skills)"],
            ["CKE",
             "Skill wraps a Marketplace-shared Cortex Search Service for "
             "shared domain knowledge retrieval. Always invoked by other skills, "
             "never directly by users.",
             "SKILL.md with preflight check, READY/MISSING/ERROR branching, "
             "graceful fallback, and query patterns.",
             "hcls-cross-cke-pubmed\nhcls-cross-cke-clinical-trials"],
            ["Compute-Heavy Pipeline",
             "Skill orchestrates external compute: containers, bioinformatics "
             "pipelines (Nextflow nf-core), or ML training (scvi-tools, PyTorch).",
             "One SKILL.md: environment check → test profile run → pipeline "
             "execution → output verification.",
             "hcls-pharma-genomics-nextflow\nhcls-pharma-genomics-scvi-tools\n"
             "imaging-ml sub-skill"],
        ],
        col_widths=[1.4, 2.0, 2.0, 1.6])
    add_stop_gate(doc,
        "Confirm archetype choice before creating any files. "
        "If unsure between Standalone and Router: use Standalone unless you have "
        "3 or more distinct concept categories with separate output tables.")
    add_spacer(doc, 4)

    # ── Step 3: Determine taxonomy and name ───────────────────────────────────
    add_step_header(doc, 3, "Determine Taxonomy Placement and Skill Name")
    add_spacer(doc, 3)
    add_body(doc,
        "All HCLS skills follow a taxonomic naming pattern. Identify where your "
        "skill fits in the table below, then compose the skill name using the "
        "pattern for that row.")
    add_spacer(doc, 3)
    if taxonomy_rows:
        add_table(doc,
            headers=["Sub-Industry", "Business Function", "Naming Pattern"],
            rows=taxonomy_rows,
            col_widths=[1.8, 2.2, 2.5])
    add_body(doc, "Examples from the current portfolio:", bold=True)
    add_bullet(doc, "hcls-provider-cdata-fhir  (Provider > Clinical Data Management)")
    add_bullet(doc, "hcls-pharma-dsafety-pharmacovigilance  (Pharma > Drug Safety)")
    add_bullet(doc, "hcls-cross-cke-pubmed  (Cross-Industry)")
    add_stop_gate(doc,
        "Confirm the skill name and taxonomy placement before creating the "
        "directory. Names cannot be renamed without updating YAML entries and "
        "all parent_skill: references in sub-skills.")
    add_spacer(doc, 4)

    # ── Step 4: Scaffold SKILL.md ─────────────────────────────────────────────
    add_step_header(doc, 4, "Scaffold SKILL.md")
    add_spacer(doc, 3)
    add_body(doc,
        "Create the skill directory and SKILL.md file. The frontmatter block "
        "at the top of SKILL.md is parsed by the orchestrator — every field "
        "below is required.")
    add_spacer(doc, 3)

    add_heading(doc, "Required frontmatter (all archetypes)", 3)
    add_code(doc,
        "---\n"
        "name: hcls-{segment}-{domain}-{capability}\n"
        "description: \"What the skill does. Triggers: keyword1, keyword2, ...\"\n"
        "platform_affinities:\n"
        "  produces: [tables, views, stages, ...]   # Snowflake objects this skill creates\n"
        "  benefits_from:\n"
        "    - skill: platform-skill-name\n"
        "      when: \"condition under which this platform skill adds value\"\n"
        "---")
    add_spacer(doc, 3)

    add_heading(doc, "Additional frontmatter for Router + Sub-skills", 3)
    add_body(doc,
        "Sub-skills must declare parent_skill in their frontmatter. "
        "The parent router SKILL.md does not need parent_skill.")
    add_code(doc,
        "---\n"
        "name: hcls-provider-cdata-clinical-nlp\n"
        "parent_skill: hcls-provider-cdata-clinical-nlp   # in each sub-skill SKILL.md\n"
        "description: \"Sub-skill description with trigger keywords\"\n"
        "platform_affinities:\n"
        "  produces: [tables, dynamic_tables, views]\n"
        "  benefits_from:\n"
        "    - skill: cortex-ai-functions\n"
        "      when: \"using Cortex COMPLETE for extraction\"\n"
        "---")
    add_spacer(doc, 3)

    add_heading(doc, "Required body sections (all archetypes)", 3)
    add_bullet(doc, "## When to Use This Skill — triggers and use cases")
    add_bullet(doc, "## Workflow — numbered steps; add STOP gates at approval points")
    add_bullet(doc, "## Output — what Snowflake objects the skill creates")
    add_spacer(doc, 3)

    add_heading(doc, "Additional body sections for CKE skills", 3)
    add_bullet(doc, "## Preflight Check — SQL probe with READY/MISSING/ERROR result table")
    add_bullet(doc, "## Fallback — what the parent skill does when CKE is MISSING")
    add_bullet(doc, "## Auto-Detection — how domain skills invoke this CKE on-demand")
    add_bullet(doc, "## Query Patterns — SQL and Cortex Agent API usage")
    add_spacer(doc, 3)

    add_heading(doc, "Additional body sections for Compute-Heavy Pipeline skills", 3)
    add_bullet(doc, "## Environment Check — verify Docker, Python packages, compute pool (mandatory Step 1)")
    add_bullet(doc, "## Test Profile Run — run on test data before production (nf-core: -profile test)")
    add_bullet(doc, "## Output Verification — assert expected artifacts exist after pipeline completes")
    add_spacer(doc, 3)

    add_stop_gate(doc,
        "Review SKILL.md content (frontmatter complete, workflow clear, "
        "output section present) before registering in YAML. Once registered, "
        "the orchestrator will attempt to route to this skill.")
    add_spacer(doc, 4)

    # ── Step 5: Register in YAML ──────────────────────────────────────────────
    add_step_header(doc, 5, "Register in skills_incubator.yaml")
    add_spacer(doc, 3)
    add_body(doc,
        "Open templates/skills_incubator.yaml and add the skill under the "
        "skills: key. This file drives all orchestrator generation — the routing "
        "tables, taxonomy tree, and CKE section in the agent profile are all "
        "generated from it.")
    add_spacer(doc, 3)

    add_heading(doc, "Valid domain values", 3)
    add_body(doc,
        "The domain field must use one of the exact strings in the table below. "
        "Any other value causes the skill to be silently omitted from routing tables.")
    if domain_order:
        rows_d = []
        for d in domain_order:
            parts = d.split(" > ")
            rows_d.append([parts[0] if parts else d,
                           parts[1] if len(parts) > 1 else "",
                           d])
        add_table(doc,
            headers=["Segment", "Function", "Exact string for domain: field"],
            rows=rows_d,
            col_widths=[1.6, 2.0, 2.9])
    add_callout(doc,
        "Before registering a skill with a new domain value, verify that value "
        "exists in DOMAIN_ORDER at the top of scripts/generate_orchestrators.py. "
        "Any domain: value not present in DOMAIN_ORDER causes the skill to be "
        "silently omitted from all generated routing tables — no error is raised. "
        "If you are adding a skill to a domain that does not yet exist in "
        "DOMAIN_ORDER, add it to that list first, then run "
        "python3 scripts/generate_orchestrators.py to verify the new domain "
        "appears in the output.",
        bg=WARNING_BG)
    add_spacer(doc, 4)

    add_heading(doc, "Standalone skill registration — live example", 3)
    add_body(doc,
        "Minimum required fields. The following is extracted from "
        "templates/skills_incubator.yaml (pharmacovigilance):")
    if standalone_yaml:
        add_code(doc, standalone_yaml)
    add_spacer(doc, 4)

    add_heading(doc, "Router skill registration — live example (clinical-docs)", 3)
    add_body(doc,
        "Router skills add a sub_skills list. Each sub-skill entry needs "
        "its own name, triggers, and description:")
    add_code(doc,
        "  hcls-provider-cdata-clinical-docs:\n"
        "    triggers: \"clinical document, document extraction, PDF extraction, ...\"\n"
        "    description: \"Router: clinical document intelligence ...\"\n"
        "    domain: \"Provider > Clinical Data Management\"\n"
        "    sub_skills:\n"
        "      - name: clinical-document-extraction\n"
        "        triggers: \"extract, parse, pipeline, classify documents\"\n"
        "        description: \"Phased extraction: gates -> classify -> extract\"\n"
        "      - name: clinical-docs-search\n"
        "        triggers: \"search documents, find in documents\"\n"
        "        description: \"Cortex Search Service over parsed clinical content\"\n"
        "      - name: clinical-docs-agent\n"
        "        triggers: \"agent, chat, conversational search\"\n"
        "        description: \"Cortex Agent combining Analyst + Search\"")
    add_spacer(doc, 4)

    add_heading(doc, "CKE skill registration — live example (PubMed)", 3)
    add_body(doc,
        "CKE skills add cke: true plus four additional fields. "
        "The following is extracted from templates/skills_incubator.yaml:")
    if cke_yaml:
        add_code(doc, cke_yaml)
    add_spacer(doc, 4)

    # ── Step 6: Regenerate orchestrator ──────────────────────────────────────
    add_step_header(doc, 6, "Regenerate the Orchestrator")
    add_spacer(doc, 3)
    add_body(doc,
        "After registering in YAML, regenerate the orchestrator agent profile "
        "so it picks up the new skill in its routing tables and taxonomy tree.")
    add_code(doc,
        "python3 scripts/generate_orchestrators.py")
    add_spacer(doc, 4)
    add_body(doc, "Verify the output in agents/health-sciences-incubator.md:", bold=True)
    add_bullet(doc, "Skill appears in the Skill Routing Table under the correct domain")
    add_bullet(doc, "Skill appears in the Taxonomy tree under the correct segment")
    add_bullet(doc, "If CKE: skill appears in the CKE section")
    add_bullet(doc, "No warnings about missing DOMAIN_ORDER entries printed during generation")
    add_stop_gate(doc,
        "Verify orchestrator generation output before testing. "
        "If the skill is missing from routing tables, check the domain: field "
        "in YAML matches an entry in DOMAIN_ORDER exactly (see Step 5).")
    add_spacer(doc, 4)

    # ── Step 7: Test locally via the CLI ─────────────────────────────────────
    add_step_header(doc, 7, "Test Locally via the Cortex Code CLI")
    add_spacer(doc, 3)
    add_body(doc,
        "Load your local skill changes into a Cortex Code session and verify "
        "end-to-end routing and execution before opening a PR.")
    add_spacer(doc, 3)

    add_heading(doc, "Load skills from your local clone", 3)
    add_code(doc,
        "# From inside the repo root (most common — after git clone and cd into the repo):\n"
        "cortex skill add ./skills\n"
        "\n"
        "# From the parent directory:\n"
        "cortex skill add ./health-sciences-coco-skills-incubator/skills\n"
        "\n"
        "# Launch with the incubator profile\n"
        "cortex --profile health-sciences-incubator")
    add_spacer(doc, 3)

    add_heading(doc, "Inside the Cortex Code session — verification checklist", 3)
    add_bullet(doc, "Type /skill — confirm your new skill appears in the skill list")
    add_bullet(doc, "Type one of your trigger keywords — confirm the orchestrator routes to your skill")
    add_bullet(doc, "Run the skill end-to-end with test data — confirm expected Snowflake objects are created")
    add_bullet(doc, "For Router skills: test at least 2 distinct sub-skill intents to verify routing table")
    add_bullet(doc, "For CKE skills: test both READY (listing present) and MISSING (listing absent) paths")
    add_bullet(doc, "For Compute-Heavy skills: run the test profile / dry-run mode before production data")
    add_spacer(doc, 3)

    add_heading(doc, "Checking skill discovery", 3)
    add_code(doc,
        "# Verify the skill is globally visible (if registered via cortex skill add)\n"
        "cortex skill list\n"
        "\n"
        "# Check profile configuration\n"
        "cortex profile show health-sciences-incubator")
    add_spacer(doc, 3)

    add_stop_gate(doc,
        "Confirm local testing passes (routing, execution, output) before "
        "opening a PR. The Tiger Team review assumes basic validation is done.")
    add_spacer(doc, 4)

    # ── Step 8: Branch and PR ─────────────────────────────────────────────────
    add_step_header(doc, 8, "Branch and PR")
    add_spacer(doc, 3)
    add_body(doc,
        "Commit all three changed artifacts — SKILL.md, YAML entry, and "
        "regenerated orchestrator — together. The Tiger Team reviews and merges.")
    add_code(doc,
        "# Branch naming convention\n"
        "git checkout -b feature/{skill-name}\n"
        "\n"
        "# Stage all three artifacts\n"
        "git add skills/{skill-name}/\n"
        "git add templates/skills_incubator.yaml\n"
        "git add agents/health-sciences-incubator.md\n"
        "\n"
        "git commit -m \"Add {skill-name}: {one-line description}\"\n"
        "git push -u origin feature/{skill-name}\n"
        "# Open PR targeting main")
    add_spacer(doc, 3)
    add_body(doc, "PR review checklist (Tiger Team confirms):", bold=True)
    add_bullet(doc, "Taxonomic naming correct and consistent with portfolio")
    add_bullet(doc, "Frontmatter complete: name, description, produces, benefits_from")
    add_bullet(doc, "YAML entry present with correct domain value")
    add_bullet(doc, "Orchestrator regenerated and routing table updated")
    add_bullet(doc, "Quality checklist from Section 5 reviewed")

    doc.add_page_break()

    # ── SECTION 3 — UPDATE AN EXISTING SKILL ─────────────────────────────────
    add_heading(doc, "Section 3 — Update an Existing Skill", 1)
    add_body(doc,
        "The update path depends on what changed. Use the table below to "
        "identify your update type, then follow only the steps for that type.")
    add_spacer(doc, 4)

    add_table(doc,
        headers=["What changed?", "Update type", "Orchestrator regen needed?"],
        rows=[
            ["Workflow text, examples, references, body content",
             "3.1  Content-only",
             "No"],
            ["triggers:, description:, domain:, sub_skills: list in YAML",
             "3.2  Trigger or metadata",
             "Yes"],
            ["Adding sub-skills to an existing skill, renaming, changing archetype",
             "3.3  Structural",
             "Yes"],
        ],
        col_widths=[2.5, 1.8, 1.7])
    add_spacer(doc, 4)

    # ── 3.1 Content-only ─────────────────────────────────────────────────────
    add_sub_case(doc, "3.1", "Content-Only Change")
    add_spacer(doc, 3)
    add_body(doc,
        "Use when you are editing workflow steps, examples, reference links, "
        "or body text inside SKILL.md but not changing any of the frontmatter "
        "fields (name, description, triggers, platform_affinities) or the "
        "YAML registry entry.")
    add_spacer(doc, 3)
    add_numbered(doc, 1, "Edit skills/{skill-name}/SKILL.md — update the relevant body sections")
    add_numbered(doc, 2, "No orchestrator regen needed — routing tables are unaffected")
    add_numbered(doc, 3,
        "Pull latest to verify your change is visible in session:")
    add_code(doc,
        "cortex profile sync health-sciences-incubator\n"
        "cortex --profile health-sciences-incubator")
    add_spacer(doc, 3)
    add_numbered(doc, 4,
        "Open PR with the SKILL.md change only. No need to include YAML or "
        "regenerated orchestrator unless those were also modified.")
    add_spacer(doc, 4)

    # ── 3.2 Trigger or metadata ───────────────────────────────────────────────
    add_sub_case(doc, "3.2", "Trigger or Metadata Change")
    add_spacer(doc, 3)
    add_body(doc,
        "Use when you are changing the triggers, description, domain, or "
        "sub_skills list in skills_incubator.yaml — or updating the matching "
        "frontmatter fields in SKILL.md. The orchestrator must be regenerated "
        "so routing tables pick up the new trigger keywords.")
    add_spacer(doc, 3)
    add_numbered(doc, 1,
        "Edit skills/{skill-name}/SKILL.md frontmatter (name, description, "
        "platform_affinities) if needed")
    add_numbered(doc, 2,
        "Edit the matching entry in templates/skills_incubator.yaml "
        "(triggers, description, domain, or sub_skills list)")
    add_numbered(doc, 3, "Regenerate the orchestrator:")
    add_code(doc,
        "python3 scripts/generate_orchestrators.py")
    add_spacer(doc, 3)
    add_numbered(doc, 4, "Test that the updated trigger routes correctly:")
    add_code(doc,
        "cortex skill add ./health-sciences-coco-skills-incubator/skills\n"
        "cortex --profile health-sciences-incubator\n"
        "# Type the new trigger keyword — confirm routing")
    add_spacer(doc, 3)
    add_numbered(doc, 5,
        "Open PR with all three: SKILL.md, skills_incubator.yaml, "
        "agents/health-sciences-incubator.md")
    add_spacer(doc, 4)

    # ── 3.3 Structural ────────────────────────────────────────────────────────
    add_sub_case(doc, "3.3", "Structural Change")
    add_spacer(doc, 3)
    add_body(doc,
        "Use for larger changes: adding sub-skills to a standalone skill, "
        "renaming a skill, or changing archetype. These changes affect "
        "directory structure and cross-references.")
    add_spacer(doc, 4)

    add_heading(doc, "Adding sub-skills to an existing standalone skill", 3)
    add_numbered(doc, 1,
        "Create the sub-skill directory: "
        "skills/{parent-skill}/{sub-skill-name}/SKILL.md")
    add_numbered(doc, 2,
        "Add parent_skill: {parent-skill-name} to the sub-skill SKILL.md "
        "frontmatter")
    add_numbered(doc, 3,
        "Add the sub-skill to the sub_skills: list in "
        "templates/skills_incubator.yaml under the parent skill entry")
    add_numbered(doc, 4,
        "Regenerate: python3 scripts/generate_orchestrators.py")
    add_numbered(doc, 5,
        "Test: verify both the parent router and the new sub-skill route correctly")
    add_spacer(doc, 4)

    add_heading(doc, "Renaming a skill", 3)
    add_numbered(doc, 1,
        "Rename the directory: skills/{old-name}/ → skills/{new-name}/")
    add_numbered(doc, 2,
        "Update name: in SKILL.md frontmatter to the new name")
    add_numbered(doc, 3,
        "Remove the old entry from skills_incubator.yaml and add a new "
        "entry with the new name")
    add_numbered(doc, 4,
        "Update all parent_skill: references in sub-skills if renaming a router")
    add_numbered(doc, 5,
        "Regenerate: python3 scripts/generate_orchestrators.py")
    add_numbered(doc, 6,
        "Verify old name is gone from routing tables; new name is present")
    add_spacer(doc, 4)

    add_heading(doc, "Changing archetype (e.g. Standalone → Router)", 3)
    add_body(doc,
        "Treat this as a partial removal + add. Keep the existing SKILL.md "
        "as the new router entry, then follow Section 2 to add sub-skill "
        "directories and update the YAML sub_skills list. "
        "Re-run the full Section 2 workflow for all new files.")

    doc.add_page_break()

    # ── SECTION 4 — REMOVE A SKILL ────────────────────────────────────────────
    add_heading(doc, "Section 4 — Remove a Skill", 1)
    add_body(doc,
        "Removing a skill requires four steps. Always regenerate the "
        "orchestrator after removal so the routing tables no longer "
        "reference the deleted skill.")
    add_spacer(doc, 4)

    add_step_header(doc, 1, "Delete the skill directory")
    add_spacer(doc, 3)
    add_body(doc, "To remove an entire skill (router + all sub-skills):")
    add_code(doc,
        "rm -rf skills/{skill-name}/")
    add_body(doc, "To remove a single sub-skill only (leave the router):")
    add_code(doc,
        "rm -rf skills/{parent-skill}/{sub-skill-name}/")
    add_spacer(doc, 4)

    add_step_header(doc, 2, "Remove from skills_incubator.yaml")
    add_spacer(doc, 3)
    add_body(doc,
        "Open templates/skills_incubator.yaml and delete the complete entry "
        "for the removed skill under the skills: key. "
        "If removing a sub-skill only, delete just that sub-skill from the "
        "parent's sub_skills: list — leave the parent entry intact.")
    add_spacer(doc, 4)

    add_step_header(doc, 3, "Regenerate the orchestrator")
    add_spacer(doc, 3)
    add_code(doc,
        "python3 scripts/generate_orchestrators.py")
    add_spacer(doc, 3)
    add_body(doc, "Verify in agents/health-sciences-incubator.md:", bold=True)
    add_bullet(doc, "Removed skill no longer appears in the Skill Routing Table")
    add_bullet(doc, "Removed skill no longer appears in the Taxonomy tree")
    add_bullet(doc, "If it was a CKE: no longer appears in the CKE section")
    add_spacer(doc, 4)

    add_step_header(doc, 4, "Clean up cross-references (CKE only)")
    add_spacer(doc, 3)
    add_body(doc,
        "If the removed skill was a CKE, find all skills that invoke it "
        "and remove or update those references:")
    add_bullet(doc,
        "Search for the skill name in benefits_from: sections of other skills — "
        "remove entries that reference the deleted CKE")
    add_bullet(doc,
        "Search for invoke_when: or used_by: references to the deleted CKE "
        "in skills_incubator.yaml and remove them")
    add_code(doc,
        "grep -r \"{skill-name}\" skills/ --include=\"*.md\"\n"
        "grep -r \"{skill-name}\" templates/skills_incubator.yaml")
    add_spacer(doc, 4)

    add_heading(doc, "Opening the PR", 2)
    add_body(doc,
        "Include all changed artifacts in the PR: deleted directory, "
        "updated skills_incubator.yaml, and regenerated orchestrator. "
        "Add a brief note in the PR description explaining why the skill "
        "was removed (deprecated, replaced by another skill, or retired).")
    add_code(doc,
        "git checkout -b remove/{skill-name}\n"
        "git add -A\n"
        "git commit -m \"Remove {skill-name}: {reason}\"\n"
        "git push -u origin remove/{skill-name}")

    doc.add_page_break()

    # ── SECTION 5 — QUICK REFERENCE ──────────────────────────────────────────
    add_heading(doc, "Section 5 — Quick Reference", 1)
    add_body(doc,
        "Reference tables for contributors who already know the workflow. "
        "Use these to look up commands, valid domain strings, the full quality "
        "checklist, and common pitfalls.")
    add_spacer(doc, 4)

    # 5.1 CLI cheat sheet
    add_heading(doc, "5.1  Cortex Code CLI command reference", 2)
    add_table(doc,
        headers=["Command", "When to use"],
        rows=[
            ["cortex --profile health-sciences-incubator",
             "Start a guided session or test the framework locally with the "
             "incubator orchestrator active"],
            ["cortex skill add ./health-sciences-coco-skills-incubator/skills",
             "Load skills from your local clone into the current session "
             "(use when testing changes before pushing)"],
            ["cortex skill list",
             "Verify a skill is discovered in the current session"],
            ["cortex profile show health-sciences-incubator",
             "Inspect the profile config: skill repos, system prompt source, "
             "active settings"],
            ["cortex profile list",
             "List all registered profiles on this machine"],
            ["cortex profile sync health-sciences-incubator",
             "Pull the latest orchestrator and skills from GitHub into your "
             "local cache — use after a PR is merged to main"],
            ["cortex skill remove \"github:Snowflake-Solutions/"
             "health-sciences-coco-skills-incubator#main\"",
             "Remove globally-registered skills (only needed if skills were "
             "added via cortex skill add instead of profile skillRepos)"],
            ["python3 scripts/generate_orchestrators.py",
             "Regenerate agents/health-sciences-incubator.md from "
             "templates/skills_incubator.yaml after any YAML changes"],
        ],
        col_widths=[3.2, 3.3])
    add_spacer(doc)

    # 5.2 Valid domain values
    add_heading(doc, "5.2  Valid domain values", 2)
    add_body(doc,
        "Use one of these exact strings in the domain: field in "
        "skills_incubator.yaml. Any other value causes the skill to be "
        "silently omitted from routing tables.")
    if domain_order:
        rows_d = []
        for d in domain_order:
            parts = d.split(" > ")
            rows_d.append([parts[0] if parts else d,
                           parts[1] if len(parts) > 1 else "",
                           d])
        add_table(doc,
            headers=["Segment", "Function", "Exact string for domain: field"],
            rows=rows_d,
            col_widths=[1.6, 2.0, 2.9])
    add_callout(doc,
        "Any domain: value not present in DOMAIN_ORDER causes the skill to be "
        "silently omitted from routing tables — no error is raised. "
        "If your domain value is not listed above, add it to DOMAIN_ORDER in "
        "scripts/generate_orchestrators.py before registering your skill.",
        bg=WARNING_BG)
    add_spacer(doc)

    # 5.3 Quality checklist
    add_heading(doc, "5.3  Quality checklist", 2)
    add_body(doc,
        "Review all items before opening a PR. The first two groups apply "
        "to every skill. The archetype-specific group applies based on the "
        "archetype you chose in Step 2.")
    add_spacer(doc, 3)
    if checklist:
        for (group_name, items) in checklist:
            group_name_clean = re.sub(r"\(.+?\)", "", group_name).strip()
            add_heading(doc, group_name_clean, 3)
            for item in items:
                if item.startswith("---") and item.endswith("---"):
                    label = item.strip("-").strip()
                    add_body(doc, label, bold=True)
                else:
                    clean = re.sub(r"\*\*⚠️\s*", "", item)
                    clean = re.sub(r"\*\*", "", clean)
                    add_checklist_item(doc, "☐  " + clean)
            add_spacer(doc, 3)
    else:
        add_callout(doc,
            "Checklist could not be extracted from "
            "HCLS_INDUSTRY_SKILL_BEST_PRACTICES.md. "
            "See Section 15 of that file directly.",
            bg=WARNING_BG)
    add_spacer(doc)

    # 5.4 Common pitfalls
    add_heading(doc, "5.4  Common pitfalls", 2)
    add_body(doc,
        "The most frequent issues found during review. Read this before "
        "writing your first skill — most are easier to avoid than to fix.")
    add_spacer(doc, 3)
    if pitfalls:
        for (group_name, items) in pitfalls:
            group_name_clean = re.sub(r"\(.+?\)", "", group_name).strip()
            add_heading(doc, group_name_clean, 3)
            if items:
                add_table(doc,
                    headers=["Pitfall", "What to do instead"],
                    rows=[(name, fix) for (name, fix) in items],
                    col_widths=[2.2, 4.3])
            add_spacer(doc, 3)
    else:
        add_callout(doc,
            "Pitfalls could not be extracted from "
            "HCLS_INDUSTRY_SKILL_BEST_PRACTICES.md. "
            "See Section 14 of that file directly.",
            bg=WARNING_BG)

    # ── SAVE ──────────────────────────────────────────────────────────────────
    out_dir  = os.path.join(BASE, "documentation")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "Contributor_Guide.docx")
    doc.save(out_path)
    print(f"\n{'=' * 60}")
    print(f"Saved: {out_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
