#!/usr/bin/env python3
import os
import re
import sys

BASE = "/Users/mgandhirajan/Documents/CoCo/HCLS/coco-healthcare-skills/skills/health-sciences"
ORCH = "/Users/mgandhirajan/Documents/CoCo/HCLS/coco-healthcare-skills/agents/health-sciences-solutions.md"
ORCH_INC = "/Users/mgandhirajan/Documents/CoCo/HCLS/coco-healthcare-skills/agents/health-sciences-incubator.md"

with open(ORCH) as f:
    orch_content = f.read()

# Collect all SKILL.md name: values and their paths
skill_names = {}
for root, dirs, files in os.walk(BASE):
    if "SKILL.md" in files:
        path = os.path.join(root, "SKILL.md")
        with open(path) as f:
            for line in f:
                if line.startswith("name:"):
                    name = line.strip().replace("name: ", "")
                    relpath = os.path.relpath(root, BASE)
                    folder = os.path.basename(root)
                    skill_names[name] = {"path": relpath, "folder": folder, "fullpath": path}
                    break

# Collect all $skill-name references from orchestrator (excluding $skill-name literal)
orch_refs = set(re.findall(r'\$hcls-[a-z0-9-]+', orch_content))

# Imaging sub-skills (internal to router, short names)
imaging_sub_skills = {"dicom-parser", "dicom-ingestion", "dicom-analytics",
                      "imaging-viewer", "imaging-governance", "imaging-ml", "data-model-knowledge"}

# Top-level skills (exclude imaging sub-skills)
top_level_skills = {n: v for n, v in skill_names.items()
                    if v["folder"] not in imaging_sub_skills}

print("=" * 60)
print("QA VALIDATION REPORT")
print("=" * 60)
fails = 0

# CHECK 1: Every $ref in orchestrator has a matching SKILL.md
print("\n--- CHECK 1: Orchestrator $refs -> SKILL.md name match ---")
for ref in sorted(orch_refs):
    name = ref[1:]  # strip $
    if name in skill_names:
        print(f"  PASS: {ref} -> {skill_names[name]['path']}")
    else:
        print(f"  FAIL: {ref} -- no SKILL.md with name: {name}")
        fails += 1

# CHECK 2: Every top-level SKILL.md referenced in orchestrator
print("\n--- CHECK 2: Top-level SKILL.md -> orchestrator reference ---")
for name, info in sorted(top_level_skills.items()):
    if f"${name}" in orch_content:
        print(f"  PASS: {name} -- referenced in orchestrator")
    else:
        print(f"  MISS: {name} ({info['path']}) -- NOT in orchestrator")
        fails += 1

# CHECK 3: Folder name == SKILL.md name (top-level only)
print("\n--- CHECK 3: Folder name == SKILL.md name ---")
for name, info in sorted(top_level_skills.items()):
    if name == info["folder"]:
        print(f"  PASS: {name}")
    else:
        print(f"  FAIL: folder={info['folder']} != name={name} ({info['path']})")
        fails += 1

# CHECK 4: Imaging sub-skills exist in filesystem
print("\n--- CHECK 4: Imaging sub-skills in filesystem ---")
imaging_dir = os.path.join(BASE, "provider/clinical-research/hcls-provider-imaging")
for sub in sorted(imaging_sub_skills):
    subdir = os.path.join(imaging_dir, sub)
    if os.path.isdir(subdir):
        # check if referenced in routing table
        if sub in orch_content:
            print(f"  PASS: {sub} -- exists & referenced")
        else:
            print(f"  WARN: {sub} -- exists but NOT referenced in orchestrator")
    else:
        print(f"  FAIL: {sub} -- directory NOT found")
        fails += 1

# CHECK 5: Taxonomy tree skill names in orchestrator match filesystem dirs
print("\n--- CHECK 5: Taxonomy tree entries -> filesystem ---")
in_tree = False
tree_skills = []
for line in orch_content.split("\n"):
    if "```" in line and in_tree:
        break
    if in_tree:
        match = re.search(r'(hcls-[a-z0-9-]+)', line)
        if match:
            tree_skills.append(match.group(1))
    if line.strip() == "```" or "Health Sciences" in line:
        in_tree = True

for ts in tree_skills:
    found = False
    for root, dirs, files in os.walk(BASE):
        if os.path.basename(root) == ts:
            found = True
            break
    if found:
        print(f"  PASS: {ts} in tree -> exists in filesystem")
    else:
        print(f"  FAIL: {ts} in tree -> NOT in filesystem")
        fails += 1

# CHECK 6: Consistency - every $ref used consistently (same name everywhere)
print("\n--- CHECK 6: Reference consistency (count per skill) ---")
for ref in sorted(orch_refs):
    count = orch_content.count(ref)
    name = ref[1:]
    sections = []
    for i, line in enumerate(orch_content.split("\n"), 1):
        if ref in line:
            sections.append(i)
    print(f"  {ref}: {count} occurrences (lines: {sections})")

# CHECK 7: Standalone hcls-provider-imaging-dicom-parser
print("\n--- CHECK 7: Standalone skills ---")
standalone = os.path.join(BASE, "provider/clinical-research/hcls-provider-imaging-dicom-parser")
if os.path.isdir(standalone):
    if "hcls-provider-imaging-dicom-parser" in skill_names:
        if "$hcls-provider-imaging-dicom-parser" in orch_content:
            print(f"  NOTE: hcls-provider-imaging-dicom-parser exists & referenced")
        else:
            print(f"  WARN: hcls-provider-imaging-dicom-parser exists in filesystem with SKILL.md but NOT referenced in orchestrator")
    else:
        print(f"  FAIL: dir exists but no SKILL.md name match")
        fails += 1

# CHECK 8: Twin orchestrator drift detection
print("\n--- CHECK 8: Twin orchestrator drift (incubator vs production) ---")
if os.path.exists(ORCH_INC):
    with open(ORCH_INC) as f:
        inc_content = f.read()

    allowed_diff_markers = {
        "name:", "description:", "# Health Sciences",
        "incubator", "Incubator", "production", "Production",
        "approved", "experimental", "All skills referenced here",
    }

    prod_lines = orch_content.splitlines()
    inc_lines = inc_content.splitlines()

    structural_sections = [
        "## Routing Rules",
        "## Skill Routing Tables",
        "## Guardrails",
        "## Getting Started",
        "## Cortex Knowledge Extensions",
    ]

    drift_count = 0
    for section in structural_sections:
        def extract_section(lines, header):
            capturing = False
            result = []
            for line in lines:
                if line.strip() == header:
                    capturing = True
                    continue
                if capturing and line.startswith("## ") and line.strip() != header:
                    break
                if capturing:
                    result.append(line)
            return result

        prod_section = extract_section(prod_lines, section)
        inc_section = extract_section(inc_lines, section)

        if prod_section == inc_section:
            print(f"  PASS: {section} -- identical")
        else:
            prod_filtered = [l for l in prod_section if l.strip()]
            inc_filtered = [l for l in inc_section if l.strip()]
            if prod_filtered == inc_filtered:
                print(f"  PASS: {section} -- identical (whitespace only)")
            else:
                print(f"  FAIL: {section} -- STRUCTURAL DRIFT detected ({len(prod_filtered)} vs {len(inc_filtered)} lines)")
                drift_count += 1
                fails += 1

    if drift_count == 0:
        print(f"  RESULT: No structural drift between orchestrators")
    else:
        print(f"  RESULT: {drift_count} sections have drift -- regenerate from template!")
else:
    print(f"  SKIP: {ORCH_INC} not found (incubator orchestrator not generated)")

# SUMMARY
print(f"\n{'=' * 60}")
print(f"TOTAL FAILURES: {fails}")
print(f"{'=' * 60}")
sys.exit(fails)
