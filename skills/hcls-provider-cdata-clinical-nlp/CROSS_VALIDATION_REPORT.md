# Cross-Validation Report: Clinical NLP Skill vs Platform Best Practices

**Date**: 2026-03-25
**Source**: `Snowflake-Solutions/cortex-code-skills/src/skill-development/SKILL_BEST_PRACTICES.md`
**Target**: `hcls-provider-cdata-clinical-nlp` (router + 15 sub-skills)

---

## Audit Summary

| Category | 🔴 Critical | 🟡 Warning | 🟢 Pass |
|----------|------------|-----------|---------|
| Frontmatter | 0 | 2 | 3 |
| Structure | 0 | 2 | 4 |
| Workflow | 0 | 1 | 4 |
| Sub-Skills / CYOA | 0 | 0 | 5 |
| Tooling & Scripts | 0 | 2 | 1 |
| Lifecycle & Promotion | 0 | 2 | 0 |
| **Total** | **0** | **9** | **17** |

**Verdict: No critical blockers. 9 warnings to address.**

---

## Detailed Findings

### Frontmatter

| Check | Status | Notes |
|-------|--------|-------|
| `name` present, kebab-case | 🟢 | All 16 SKILL.md files have `name` in kebab-case |
| `description` with triggers | 🟢 | Router has comprehensive trigger phrase list (40+ keywords) |
| `name` follows verb-prefix pattern | 🟡 | Router: `hcls-provider-cdata-clinical-nlp` uses **domain-prefix** not **verb-prefix**. Sub-skills: `extraction-*`, `normalization-*` use **noun-prefix**. Best practice says `create-X`, `manage-X`, `audit-X`. **However**: Industry skills use taxonomic naming (`hcls-provider-cdata-*`) for organizational hierarchy — this is intentional and should be documented as an acceptable industry pattern. |
| `parent_skill` on sub-skills | 🟢 | All 15 sub-skills declare `parent_skill: hcls-provider-cdata-clinical-nlp` |
| Old name preserved if renamed | 🟡 | N/A — skill was never renamed, but no legacy trigger phrases exist for potential future renames |

### Structure

| Check | Status | Notes |
|-------|--------|-------|
| Router SKILL.md < 500 lines | 🟢 | 308 lines |
| All sub-skills < 500 lines | 🟡 | **2 sub-skills exceed 500 lines**: `pipeline-implementation` (715 lines), `governance` (638 lines). Best practice recommends < 500 per file. These are complex reference-heavy sub-skills — content could be moved to `references/` files. |
| Workflow section present | 🟢 | Router has full ASCII workflow diagram. All sub-skills have step-based workflows. |
| Stopping points section | 🟢 | Router has "Stopping Points" section. Governance has 5 explicit stopping points per layer. Pipeline has implicit stopping via platform skill invocations. |
| Output section | 🟡 | **Missing explicit "Output" section** in most sub-skills. Extraction sub-skills describe post-processing INSERT but don't have a formal "## Output" section. Best practice requires this. |
| No extraneous docs (README, CHANGELOG) | 🟢 | None present. Clean. |

### Workflow

| Check | Status | Notes |
|-------|--------|-------|
| Numbered steps | 🟢 | All sub-skills use numbered steps (Step 0, Step 1, Step 2, etc.) |
| ⚠️ checkpoints marked | 🟡 | Router has stopping points but **does not use ⚠️ emoji markers** per best practice convention. Governance uses plain text "Stopping Points" list. Best practice requires `**⚠️ MANDATORY STOPPING POINT**` format. |
| No chaining without approval | 🟢 | Router stops for: ambiguous intent, before DB object creation, before large dataset processing. Normalization has Terminology Preference Gate (explicit user approval). |
| Clear actions (Ask/Load/Execute) | 🟢 | Sub-skills use clear verb patterns: "Extract ALL conditions", "INSERT INTO CONDITION", "UPDATE CONDITION" |
| Decision points with routing | 🟢 | Router has single intent table → clear routing. Terminology Preference Gate has clear options → `$NORM_CODE_SYSTEMS` variable. |

### Sub-Skills / CYOA Analysis

| Property | Status | Notes |
|----------|--------|-------|
| **Reachability** | 🟢 | All 15 sub-skills are reachable from the router intent table. No orphaned files. |
| **Determinism** | 🟢 | Each intent maps to exactly one sub-skill (or "all 6" for EXTRACT_ALL/NORMALIZE_ALL). No ambiguous routing. |
| **Termination** | 🟢 | All extraction sub-skills terminate after INSERT. All normalization sub-skills terminate after UPDATE with results dict. Governance terminates after each layer with stopping point. |
| **Transition clarity** | 🟢 | Router uses directive language: `Load extraction-conditions-diagnostics/SKILL.md`. No passive "see also" patterns. |
| **Loop bounds** | 🟢 | No retry loops in any sub-skill. Fuzzy match has confidence threshold as natural bound. |

### Tooling & Scripts

| Check | Status | Notes |
|-------|--------|-------|
| Scripts documented with usage | 🟡 | `data-model-knowledge/scripts/setup_clinical_nlp_model_knowledge_repo.sql` exists but is SQL (not Python). No `pyproject.toml` needed. `seed-data/setup_seed_data.sql` has clear step-by-step instructions. However, script documentation in the sub-skill follows a narrative format, not the best practice "### Tool: X" format with Parameters/Usage/Example blocks. |
| `pyproject.toml` for Python scripts | 🟡 | No `pyproject.toml` exists. The pipeline-implementation SP is inline SQL/Python (not a standalone script), so uv isn't applicable. However, if any future scripts are added, this would be needed. **Not blocking** — no Python scripts currently exist outside of embedded SP code. |
| Absolute paths in examples | 🟢 | SQL examples use fully qualified Snowflake object names. No relative path issues. |

### Lifecycle & Promotion

| Check | Status | Notes |
|-------|--------|-------|
| `skill_evidence.yaml` present | 🟡 | **Missing**. Required for the cortex-code-skills promotion lifecycle (draft → review → staging → production). Should be created with `promotion_stage: "draft"`, author, and version. |
| No overlap with existing skills | 🟡 | No registry overlap check has been run. The skill is industry-specific (clinical NLP) and unlikely to overlap with platform skills, but the check should be documented. |

---

## Gaps Requiring Action

### Gap 1: Sub-skills over 500 lines (🟡 Warning)
**Files**: `pipeline-implementation/SKILL.md` (715), `governance/SKILL.md` (638)
**Recommendation**: Move reference SQL blocks to `references/` files. Pipeline could split monitoring queries and warehouse sizing into `references/monitoring.md`. Governance could split L6 (ML Feature Views) and L7 (Audit) into `references/`.
**Decision**: Defer — these files are loaded only when needed (sub-skills, not router). The content is reference-heavy SQL that CoCo needs for implementation. Splitting would add load steps without reducing effective context usage since the full sub-skill is loaded on demand.

### Gap 2: Missing ⚠️ mandatory stopping point markers (🟡 Warning)
**Recommendation**: Add `**⚠️ MANDATORY STOPPING POINT**` markers to:
- Router: Before creating database objects, before large dataset processing
- Governance: Each layer transition (L1→L2, L2→L3, etc.)
- Pipeline: Before creating DTs, before creating MERGE tasks, before creating SP
**Action**: Should fix — low effort, high compliance value.

### Gap 3: Missing "## Output" sections in sub-skills (🟡 Warning)
**Recommendation**: Add brief "## Output" section to each extraction and normalization sub-skill stating what they produce (e.g., "Rows inserted into CONDITION table with promoted NLP fields").
**Action**: Should fix — low effort, improves discoverability.

### Gap 4: Missing `skill_evidence.yaml` (🟡 Warning)
**Recommendation**: Create `skill_evidence.yaml` in skill root with:
```yaml
skill_name: "hcls-provider-cdata-clinical-nlp"
skill_type: "customer-facing"
promotion_stage: "draft"
version: "1.0.0"
authors:
  - "sfc-gh-mgandhirajan"
validation:
  testers:
    count: 1
    names: ["sfc-gh-mgandhirajan"]
customer_impact:
  customers:
    count: 0
    names: []
notes: |
  Industry-specific clinical NLP skill for healthcare.
  E2E tested: 5 phases passed (dedup, extraction, normalization, multi-category, scale).
```
**Action**: Should create — required for promotion lifecycle.

### Gap 5: Verb-prefix naming convention (🟡 Warning — Intentional Deviation)
**Observation**: Platform skills use verb-prefix (`create-skill`, `audit-skill`). Industry skills use domain-prefix (`hcls-provider-cdata-clinical-nlp`).
**Recommendation**: Document this as an **accepted industry pattern** in the industry best practices guide. The taxonomic naming (`hcls-{segment}-{domain}-{capability}`) serves organizational hierarchy needs that verb-prefix doesn't address (guild routing, persona targeting, portfolio management).
**Action**: No change to naming — document the pattern.

---

## Compliance Summary

| Best Practice | Clinical NLP Compliance |
|--------------|----------------------|
| Conciseness (router < 500 lines) | ✅ 308 lines |
| Degrees of freedom matched to fragility | ✅ Low freedom for SQL patterns (exact scripts), Medium for LLM prompts (parameterized), High for architecture choices |
| Sub-skills only for distinct branches | ✅ 6 extraction × 6 normalization × governance × pipeline × data-model = distinct workflows |
| Single intent table | ✅ One table in router with 17 intents |
| CYOA validation passes | ✅ Reachability, determinism, termination, transition clarity, loop bounds |
| No chaining without approval | ✅ Terminology Preference Gate, stopping points |
| References for detailed content | ✅ `data-model-knowledge/references/`, `seed-data/` |
| No extraneous docs | ✅ No README.md, CHANGELOG.md |
| Platform affinities declared | ✅ 7 platform skill affinities + produces list |
| Error handling guidance | ✅ Hardcoded fallback schema when search service unavailable |
