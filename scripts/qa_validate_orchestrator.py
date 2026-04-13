#!/usr/bin/env python3
import os
import re
import sys
import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE = os.path.join(REPO, "skills")
ORCH_INC = os.path.join(REPO, "agents/health-sciences-incubator.md")
ORCH_PROD = os.path.join(REPO, "agents/health-sciences-solutions.md")
REGISTRY = os.path.join(REPO, "templates/skills_incubator.yaml")

ORCH = ORCH_INC

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

# Top-level skills (exclude imaging sub-skills AND clinical-docs sub-skills)
clinical_docs_sub_skills = {"clinical-docs-agent", "clinical-docs-search", "clinical-docs-viewer",
                            "clinical-document-extraction", "confirm-doc-types", "confirm-environment",
                            "confirm-pipeline-config", "phase-classify", "phase-extract",
                            "phase-parse-and-refresh", "data-model-knowledge"}
top_level_skills = {n: v for n, v in skill_names.items()
                    if v["folder"] not in imaging_sub_skills
                    and v["folder"] not in clinical_docs_sub_skills
                    and n not in clinical_docs_sub_skills}

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
imaging_dir = os.path.join(BASE, "hcls-provider-imaging")
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

# CHECK 7: Twin orchestrator drift detection
print("\n--- CHECK 7: Twin orchestrator drift (incubator vs production) ---")
if os.path.exists(ORCH_PROD):
    with open(ORCH_PROD) as f:
        prod_content = f.read()

    prod_has_skills = "$hcls-" in prod_content

    if not prod_has_skills:
        print("  SKIP: Production has no skills yet (empty scaffold) -- drift check deferred")
    else:
        prod_lines = prod_content.splitlines()
        inc_lines = orch_content.splitlines()

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
    print(f"  SKIP: {ORCH_PROD} not found (production orchestrator not generated)")

# CHECK 9: Registry <-> skill directories bidirectional
print("\n--- CHECK 9: Registry vs directories (bidirectional) ---")
with open(REGISTRY) as f:
    reg = yaml.safe_load(f)
reg_skills = set(reg.get('skills', {}).keys())
skill_dir_names = {d for d in os.listdir(BASE) if d.startswith("hcls-") and os.path.isdir(os.path.join(BASE, d))}

reg_not_dir = reg_skills - skill_dir_names
dir_not_reg = skill_dir_names - reg_skills
for r in sorted(reg_not_dir):
    print(f"  FAIL: Registry has '{r}' but no directory exists")
    fails += 1
for d in sorted(dir_not_reg):
    print(f"  FAIL: Directory '{d}/' exists but not in registry")
    fails += 1
if not reg_not_dir and not dir_not_reg:
    print(f"  PASS: All {len(reg_skills)} registry entries match directories")

# CHECK 10: Platform affinities validation
print("\n--- CHECK 10: Platform affinities frontmatter ---")
pre_c10_fails = fails
VALID_PLATFORM_SKILLS = {
    "dynamic-tables", "data-governance", "data-quality", "semantic-view",
    "developing-with-streamlit", "deploy-to-spcs", "machine-learning",
    "cortex-ai-functions", "cortex-agent", "search-optimization",
    "skill-development",
}
for d in sorted(skill_dir_names):
    skill_md = os.path.join(BASE, d, "SKILL.md")
    if not os.path.exists(skill_md):
        continue
    with open(skill_md) as f:
        content = f.read()
    fm_match = re.match(r'^---\n(.*?)\n---', content, re.DOTALL)
    if not fm_match:
        continue
    try:
        fm = yaml.safe_load(fm_match.group(1))
    except Exception:
        print(f"  FAIL: {d}/SKILL.md frontmatter YAML parse error")
        fails += 1
        continue
    pa = fm.get('platform_affinities')
    if pa is None:
        print(f"  FAIL: {d}/SKILL.md missing platform_affinities")
        fails += 1
        continue
    if 'produces' not in pa:
        print(f"  FAIL: {d}/SKILL.md platform_affinities missing 'produces'")
        fails += 1
    if 'benefits_from' not in pa:
        print(f"  FAIL: {d}/SKILL.md platform_affinities missing 'benefits_from'")
        fails += 1
    for bf in pa.get('benefits_from', []):
        skill_ref = bf.get('skill', '')
        if skill_ref and skill_ref not in VALID_PLATFORM_SKILLS:
            print(f"  FAIL: {d} references unknown platform skill '{skill_ref}'")
            fails += 1
        if not bf.get('when'):
            print(f"  FAIL: {d} affinity for '{skill_ref}' missing 'when' condition")
            fails += 1
if fails == pre_c10_fails:
    print(f"  PASS: All {len(skill_dir_names)} SKILL.md files have valid platform_affinities")

# CHECK 11: CKE used_by references
print("\n--- CHECK 11: CKE used_by references ---")
cke_skills = {k: v for k, v in reg.get('skills', {}).items() if v.get('cke')}
for cke_name, cke_data in cke_skills.items():
    for used_by_ref in cke_data.get('used_by', []):
        base_ref = used_by_ref.split(' (')[0].strip()
        if base_ref not in skill_dir_names and base_ref not in reg_skills:
            print(f"  FAIL: CKE '{cke_name}' used_by references unknown skill '{used_by_ref}'")
            fails += 1
        else:
            print(f"  PASS: CKE '{cke_name}' used_by '{used_by_ref}'")

# CHECK 12: Overlap entries
print("\n--- CHECK 12: Overlap references ---")
overlaps = reg.get('overlaps', [])
for o in overlaps:
    skill_ref = o.get('skill', '')
    if skill_ref not in skill_dir_names:
        print(f"  FAIL: Overlap references unknown skill '{skill_ref}'")
        fails += 1
    elif not o.get('serves'):
        print(f"  FAIL: Overlap for '{skill_ref}' missing 'serves' field")
        fails += 1
    else:
        print(f"  PASS: Overlap '{skill_ref}' serves '{o['serves']}'")

# SUMMARY
print(f"\n{'=' * 60}")
print(f"TOTAL FAILURES: {fails}")
print(f"{'=' * 60}")
sys.exit(fails)
