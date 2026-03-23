---
name: hcls-cross-skill-development
description: "Add a new industry skill to the Health Sciences Solutions Architect. Use when a contributor wants to create a new skill, register it with the orchestrator, and regenerate routing. Triggers: add skill, new skill, create skill, register skill, add industry skill, scaffold skill, contribute skill."
platform_affinities:
  produces: []
  benefits_from:
    - skill: skill-development
      when: "always — delegates to the platform skill-development skill for scaffolding and best practices"
---

# Health Sciences Skill Development

Guided workflow to add a new industry skill to the Health Sciences Solutions Architect, register it in the orchestrator, and regenerate routing.

## When to Use

Invoke this skill when:
- Creating a new health sciences skill from scratch
- Registering an existing skill in the orchestrator
- Regenerating orchestrator routing after skill changes

## Prerequisites

- Platform `skill-development` skill available (provides scaffolding, best practices, audit)
- Write access to the `health-sciences-coco-skills-incubator` repo

## Workflow

### Step 1: Determine Skill Placement

Identify where the skill fits in the taxonomy:

| Sub-Industry | Business Function | Naming Pattern |
|--------------|-------------------|----------------|
| Provider | Clinical Research | `hcls-provider-{function}-{skill}` |
| Provider | Clinical Data Management | `hcls-provider-cdata-{skill}` |
| Provider | Revenue Cycle | `hcls-provider-{function}-{skill}` |
| Pharma | Drug Safety | `hcls-pharma-dsafety-{skill}` |
| Pharma | Genomics | `hcls-pharma-genomics-{skill}` |
| Pharma | Lab Operations | `hcls-pharma-lab-{skill}` |
| Payer | Claims Processing | `hcls-payer-{function}-{skill}` |
| Cross-Industry | (any) | `hcls-cross-{skill}` |

**STOP**: Confirm the skill name and placement with the user before proceeding.

### Step 2: Scaffold the Skill

1. Create the skill directory: `skills/{skill-name}/`
2. Invoke the platform `skill-development` skill (`create-from-scratch`) to generate the SKILL.md scaffold
3. Add HCLS-specific frontmatter fields:
   ```yaml
   ---
   name: hcls-{sub}-{func}-{skill}
   description: "What the skill does. Triggers: keyword1, keyword2, ..."
   platform_affinities:
     produces: [tables, views, stages, ...]
     benefits_from:
       - skill: platform-skill-name
         when: "condition under which this platform skill adds value"
   ---
   ```
4. Write the skill body following the standard structure:
   - `# Title`
   - `## When to Use` — triggers, use cases
   - `## Workflow` — numbered steps with stopping points
   - `## Output` — what the skill produces

### Step 3: Register in the Orchestrator

1. Open `templates/skills_incubator.yaml`
2. Add the skill under the `skills:` key:
   ```yaml
   {skill-name}:
     triggers: "keyword1, keyword2, keyword3"
     description: "One-line description"
     domain: "{Sub-Industry} > {Business Function}"
   ```
3. If the skill is a CKE, add: `cke: true`, `data_source:`, `used_by:`, `invoke_when:`
4. If the skill serves multiple sub-industries, add an entry under `overlaps:`

### Step 4: Regenerate Orchestrator

```bash
python3 scripts/generate_orchestrators.py
```

Verify:
- The skill appears in the correct Skill Routing Table
- The skill appears in the Taxonomy tree
- If CKE, it appears in the CKE section
- No warnings about missing DOMAIN_ORDER entries

### Step 5: Test Locally

```bash
cortex skill add ./health-sciences-coco-skills-incubator/skills
cortex --profile health-sciences-incubator
```

Test that:
- The skill is discovered (`cortex skill list`)
- The orchestrator routes to it when trigger keywords are used
- The skill executes correctly

### Step 6: Branch and PR

1. Branch: `git checkout -b feature/{skill-name}`
2. Commit all files: SKILL.md, registry entry, regenerated orchestrator
3. Push and open a PR targeting `main`
4. Tiger Team reviews and merges

## Stopping Points

- After Step 1: Confirm skill name and taxonomy placement
- After Step 2: Review SKILL.md before registering
- After Step 4: Verify orchestrator generation output
- After Step 5: Confirm local testing passes

## Output

A fully registered industry skill with:
- `skills/{skill-name}/SKILL.md` with frontmatter, workflow, platform affinities
- Registry entry in `templates/skills_incubator.yaml`
- Regenerated orchestrator with routing table entry
- Branch ready for PR
