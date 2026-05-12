---
name: hcls-pharma-lab-ml-optimization
description: Apply machine learning to optimize laboratory workflows. Use this skill when users want to reduce turnaround time (TAT), increase throughput, lower cost-per-sample, or eliminate bottlenecks in clinical, research, QC, or manufacturing labs. The skill walks the user through a structured 7-phase methodology - define the workflow, define optimization metrics, identify the most time-consuming steps, inventory source systems and entities at every step, place ML models in front of bottlenecks, run inference to predict the target variable, and explain the predictions to surface key drivers. Outputs are a workflow map, a metric tree, a bottleneck-ranked process diagram, a feature inventory keyed to source systems, ML model placement recommendations, prediction outputs, and SHAP/feature-importance-based explanations. Common triggers include lab optimization, lab workflow, turnaround time, TAT, throughput, bottleneck, predictive maintenance, sample backlog, instrument utilization, lab efficiency, lab ML, optimize lab.
platform_affinities:
  produces: [tables, models]
  benefits_from:
    - skill: machine-learning
      when: "training, registering, and serving the predictive models that sit in front of bottleneck steps"
    - skill: cortex-ai-functions
      when: "generating natural-language root-cause narratives from prediction explanations using AI_COMPLETE"
    - skill: dynamic-tables
      when: "building incremental feature pipelines from LIMS, instruments, and scheduling source systems"
    - skill: data-quality
      when: "validating that source-system inputs are complete and within range before scoring"
    - skill: developing-with-streamlit
      when: "building a lab operations dashboard that surfaces predictions, drivers, and recommended actions"
---

# Lab Workflow Optimization with Machine Learning

A structured methodology for applying ML to **lab workflow optimization** — clinical, research, QC, or manufacturing labs. Instead of jumping straight to model training, the skill forces a disciplined walk through the lab process, the metrics that matter, and the points where prediction creates leverage.

> **Research / operational use only.** This skill is not a medical device. Predictions surfaced here are decision support for lab operations (scheduling, capacity, maintenance), not for clinical diagnosis or patient care.

## When to Use

Invoke this skill when the user asks to:

- Reduce **turnaround time (TAT)** for lab results
- Increase **throughput** or **instrument utilization**
- Identify **bottlenecks** in a lab process
- Predict **sample backlogs**, **rework**, **QC failures**, or **instrument downtime** before they happen
- Build a **predictive maintenance** program for lab instruments
- Lower **cost-per-sample** or improve **on-time delivery**
- Decide *where* in a lab workflow ML actually adds value (and where it doesn't)

If the request is *only* about ingesting/standardizing lab instrument files, route to `hcls-pharma-lab-allotrope` instead. If the request is *only* about training a generic model with no lab-process framing, route to the platform `machine-learning` skill.

## Methodology Overview

The skill executes 8 phases in order (a deliverable-scoping Phase 0 followed by 7 methodology phases). Do not skip phases — each phase produces an artifact the next phase consumes.

| Phase | Goal | Output |
|-------|------|--------|
| 0 | Define the deliverable | Selected output mode (methodology / Streamlit app / pipeline / other) + synthetic-data decision if app |
| 1 | Define the workflow | Ordered list of steps, owners, handoffs, decision points |
| 2 | Define the optimization metrics | Metric tree (primary KPI + supporting metrics + constraints) |
| 3 | Identify the most time-consuming steps | Ranked bottlenecks with cycle-time evidence |
| 4 | Inventory source systems, entities, metrics per step | Step × Source-System × Entity × Metric matrix |
| 5 | Place ML models in front of bottlenecks | Model placement diagram with target variables |
| 6 | Run ML models to predict the target variable | Predictions in Snowflake (table or UDF), with monitoring |
| 7 | Explain predictions and identify key drivers | SHAP values + global/cohort feature importance + accuracy / F1 / AUC / MAE |

Each phase has a corresponding reference file in `references/` with prompts, checklists, and examples.

## How to Run This Skill

When this skill is invoked:

1. **Do NOT** start with a generic plan, template, or canonical lab workflow. Every lab is different — assumptions about workflow steps are wrong by default.
2. **Start at Phase 0 immediately.** Before any methodology work, ask the user what the final deliverable should be (methodology doc, Streamlit app, production pipeline, or something else).
3. **Then move to Phase 1.** Ask the user for their lab's specific workflow steps (written list OR diagram upload OR explicit request to co-construct from a template).
4. **Phases 0 and 1 are hard gates.** Do not produce downstream artifacts until both are answered and confirmed.
5. **Each phase gates the next.** End each phase with a numbered confirmation of the artifact and an explicit ask: "Does this look right? Ready to move to Phase N+1?" Wait for confirmation before proceeding.
6. **If the user uploads a process diagram** (PNG / JPG / PDF / Visio export / whiteboard screenshot), read it visually, extract the step list, echo it back as a numbered list, and ask for corrections before proceeding.

## Operating Rules (apply across all phases)

These rules are non-negotiable and apply to every phase, every prompt, and every artifact this skill produces.

### Rule 1 — No-assumption policy on abbreviations and jargon

When the user uses **any** abbreviation, acronym, code, or domain shorthand that has not already been defined in this conversation, the skill **must stop and ask** for the expansion before using it in any artifact.

- Examples that always require clarification: `tnp`, `TAT`, `MRN`, `QNS`, `OOS`, `H&P`, `CBC`, `SMI`, `NSCLC`, `MTM`, `LDT`, `TNP test`, instrument codenames, internal LIMS test codes.
- Even if the abbreviation is *commonly* used in healthcare (e.g., TAT = turnaround time), **still confirm** the user means the common meaning before using it. Different labs use the same letters for different things.
- Phrasing template: *"You mentioned `<abbreviation>` — can you spell that out for me? I want to make sure I'm modeling the right thing."*
- Persist confirmed expansions in a "Glossary" block that gets carried into every later phase artifact (workflow, metric tree, feature inventory, model contract, dashboard labels).
- **Never** infer the meaning of an abbreviation from context, lab type, or the most likely industry usage. If in doubt, ask.

### Rule 2 — Phases gate each other; never silently choose

The skill must never make a substantive choice on the user's behalf for: deliverable mode, lab type / use case, workflow steps, **primary KPI**, bottleneck step, **target variable**, **model placement (which step the model runs before / after)**, prediction horizon, decision threshold. Each must be **elicited, echoed, and confirmed** by the user.

**Specific failure modes that are forbidden** (these have shown up in testing — do NOT repeat them):

- **Defaulting to TAT** as the primary KPI without asking. TAT is *one* option among many. Different lab types optimize different things. Always ask Phase 2 Step 2.1 explicitly.
- **Skipping the target-variable question** in Phase 5. Even if the bottleneck name strongly suggests a target ("rerun queue" → rerun prediction), the user MUST be asked Step 5.A explicitly.
- **Skipping the model-placement question** in Phase 5 Step 5.C. Even if the placement seems obvious ("right before the bottleneck"), the user MUST be asked which step the model runs AFTER and which step it runs BEFORE, with the numbered Phase 1 workflow shown back to them.
- **"I'll assume X — is that OK?"** counts as silently choosing. The correct phrasing is "What X do you want?" (open) or a structured `ask_user_question` with explicit options. Never preselect.
- **Burying the question inside a status update.** A line like "I'll proceed with TAT as the primary KPI" — even if followed by "let me know if you want something different" — is a violation. Ask first, proceed after the answer.
- **Reading defaults from a canonical list and silently picking the first one.** Canonical lists in `references/` are conversation aids, never defaults.

If you find yourself about to write a sentence that picks a value the user has not explicitly given you, stop and convert it into a question.

### Rule 2a — `ask_user_question` auto-appends "Something else"

Whenever this skill uses the `ask_user_question` tool, **never add a "Something else", "Other", "Specify manually", "I'll type it", or "I need clarification" option yourself.** The tool appends a "Something else" free-text choice automatically. Adding your own duplicates it (the user sees two "Something else" rows in the picker).

When you actually want a free-text branch, rely on the auto-appended choice and document the follow-up behavior in your option-handling table (as Phase 0 and Phase 1 do).

### Rule 2b — `ask_user_question` argument shape (avoid `H.map is not a function`)

The `ask_user_question` tool errors out with `Tool execution failed: H.map is not a function` when the arguments are not the exact shape below. Always pass:

```json
{
  "questions": [
    {
      "question": "<full question text ending with '?'>",
      "header":   "<short chip label, max ~12 chars>",
      "multiSelect": false,
      "options": [
        { "label": "<option label>", "description": "<what this option means>" },
        { "label": "<option label>", "description": "<what this option means>" }
      ]
    }
  ]
}
```

Hard rules:

- `questions` MUST be a JSON array (a list), even when you're asking only one question. Never pass a single object.
- Never pass `questions` as a JSON-encoded string — pass it as a real list/array of dicts.
- `options` MUST be an array of `{ label, description }` objects. Never pass an array of bare strings (`["a","b"]`).
- `options` MUST have at least 2 entries and at most 6 (the auto-appended "Something else" does not count).
- Do NOT include "Something else" / "Other" / "Custom" / "Specify manually" entries — see Rule 2a.
- `multiSelect` MUST be a boolean (`false` for the standard pick-one questions in this skill).
- Every option MUST have both `label` AND `description` (non-empty strings). Missing `description` will trip the `m.options.map(...)` step internally.

If `ask_user_question` fails with the `H.map` error, treat it as a malformed-args bug in your call — re-emit the call with the exact JSON shape above. Do not silently fall back to a free-text prompt without telling the user.

### Rule 3 — Build, test, and run any code that is produced

When the deliverable is **B (Streamlit demo app)**, **C (production pipeline scaffold)**, or any other mode that involves writing code:

1. **Build it locally.** Write code to disk, install required dependencies into the active environment (`pip install ...` or equivalent), and produce a runnable artifact.
2. **Test it before handing off.** Run the test battery below. Do not declare the deliverable done if any test fails.
3. **Run it locally without asking the user to run it.** Do not say "you can run this with `streamlit run app.py`" and stop. Actually start the app (or pipeline / notebook) in the background, capture the URL or process ID, verify it serves traffic / completes without error, then report the URL or output to the user.
4. **Report what you ran.** Include: command executed, exit code or PID, dependencies installed, test outcomes, smoke-test screenshots/logs, and the access URL (for Streamlit) or output table location (for pipelines).

#### Required test battery for Streamlit apps (Mode B)

Run all of these before declaring the deliverable done:

- **Syntax check** — `python -m py_compile app.py` and any imported modules; zero errors.
- **Import check** — `python -c "import app"` (or equivalent) succeeds with all dependencies installed.
- **Synthetic-data generation** — generation script runs end-to-end, produces the expected number of rows per entity, and persists to disk or to Snowflake. Validate row counts, schema, and that key correlations from the Phase 0 spec are present (e.g., older reagent lots → higher rerun probability).
- **Model training** — training runs end-to-end, model registers / serializes successfully, training metrics are within the plausible ranges declared in Phase 0.
- **Holdout evaluation** — accuracy / F1 / AUC / MAE compute without NaN / Inf and are reported in the app.
- **SHAP computation** — SHAP values compute for at least the first 100 predictions; `LAB_PREDICTION_EXPLANATIONS` (or in-memory equivalent) is populated.
- **App boot** — `streamlit run app.py --server.headless true` starts; HTTP 200 on the root URL within 30 seconds; no exception in stderr during boot.
- **Page render smoke test** — at least one filter / dropdown / page transition in the app fires without exception (use `requests` or Playwright via the `cortex browser` tool).
- **Reproducibility** — re-running the generation script with the same seed yields byte-identical (or row-identical) outputs.

If any test fails, fix the code, re-run the full battery, and only then hand off.

#### Required test battery for production pipeline scaffolds (Mode C)

- DDL compiles (`sql_execute` with `only_compile=true` for every CREATE statement).
- Dynamic Tables refresh end-to-end on a small sample.
- Snowpark ML training notebook executes top-to-bottom without error against the sample.
- Model Registry registration succeeds and the model is callable from SQL.
- Inference Dynamic Table populates `LAB_PREDICTIONS` with at least one row per active prediction unit.
- Monitoring task executes its first run successfully.

### Rule 4 — Always cite synthetic data as synthetic

Any UI surface (Streamlit page, notebook, exported PDF) that is built on synthetic data must show a persistent banner or disclaimer that the data is synthetic and is for illustration only.

---

## Phase 0: Define the Deliverable

**Goal:** Decide *what the user wants out of this engagement* before doing any methodology work. The deliverable shapes what Phases 1-7 produce.

> **HARD GATE — DO NOT PROCEED PAST THIS PHASE WITHOUT THE USER PICKING A DELIVERABLE.**

### Required: ask the user which final deliverable they want

Present the three options below and ask the user to pick one. Use `ask_user_question` if the calling agent supports it; otherwise list the options inline and wait for an answer. **Do NOT add a "Something else" / "Other" option yourself — `ask_user_question` automatically appends a "Something else" choice for free-text input. Adding your own will cause it to appear twice.**

| # | Deliverable | What you produce |
|---|-------------|------------------|
| A | **Methodology only** | A written 7-phase methodology document tailored to the user's workflow + metric tree + bottleneck ranking + model placement plan + explainability spec. No code, no data, no models. |
| B | **Streamlit demo app** (synthetic data) | A runnable Streamlit dashboard that demonstrates the end-to-end flow on synthetic-but-realistic data: feature table, trained model(s), live predictions, SHAP per-prediction explanations, global feature importance, cohort heatmap, accuracy / F1 / AUC / MAE, calibration plots. |
| C | **Production pipeline scaffold** | Snowflake DDL + Dynamic Tables for features + Snowpark ML training notebook + Model Registry registration + inference Dynamic Table + monitoring task. Wired against the user's real source tables. |

(The tool will render a fourth choice — "Something else" — automatically. If the user picks it, ask them to describe the deliverable in their own words and confirm scope before proceeding.)

### If the user picks B (Streamlit demo app), ask the synthetic-data question

After the user picks B, ask explicitly:

> "To make the Streamlit app runnable end-to-end, I'd like to generate a **synthetic-but-realistic dataset** modeled on your workflow (using realistic distributions for sample types, instrument states, queue depths, reagent lots, QC outcomes, etc.), train ML models on it, and surface real prediction outputs — SHAP values per prediction, global feature importance, accuracy, F1, AUC, MAE, calibration. **Is it OK to generate the synthetic dataset?**"
>
> **Options:**
> 1. **Yes, generate synthetic data** — proceed with full simulation.
> 2. **No, I will provide real data** — pause and wait for the user to load real tables; later phases use those.
> 3. **Hybrid** — generate synthetic for some entities, use real for others. Ask which entities are real vs. synthetic.

If the user picks B + "Yes, generate synthetic data" or B + Hybrid, the deliverable is committed to producing actual model outputs (not placeholders) at Phase 6/7:

- Trained model(s) registered in Snowflake Model Registry
- Predictions table populated (real numeric values, not stubs)
- SHAP values per prediction persisted
- Held-out evaluation: **accuracy**, **F1** (per class + macro), **precision**, **recall**, **AUC** for classifiers; **MAE**, **RMSE**, **R²** for regressors
- Calibration / reliability plot for classifiers
- Global feature importance + cohort heatmap

### Synthetic data requirements (when B + synthetic)

The synthetic dataset must be:

- **Realistic ranges** — values within plausible bounds for clinical / lab / instrument data (e.g., reagent lot age in days not years, QC drift in expected SD ranges, queue depth bounded by physical capacity).
- **Plausible correlations** — features are not independent. e.g., older reagent lots correlate with higher rerun rates; instrument calibration age correlates with QC drift; shift role count inversely correlates with review time.
- **Base rates aligned to clinical reality** — rerun rates ~3-15%, autoverification rates ~60-90%, QC failure rates < 5%, instrument MTBF in 100s-1000s of hours.
- **Documented generation logic** — every feature has a documented distribution and noise model in the Streamlit app's "About this data" panel so the user can see this is synthetic and how it was generated.
- **Reproducible** — fixed random seed, generation script committed alongside the app.

The synthetic data is ALWAYS marked as synthetic in the UI (banner at top of the app: "Demo data — synthetic, generated for illustration only").

### Capture template

```
Deliverable mode:    A / B / C / D
  (if D)             <user-specified description>

If B:
  Synthetic data OK? Yes / No / Hybrid
  If Hybrid:         Real entities = <list>;  Synthetic entities = <list>
  Target metrics to surface:
    Classifier:      accuracy, F1, precision, recall, AUC, calibration
    Regressor:       MAE, RMSE, R²
    Explainability:  SHAP per-prediction, global importance, cohort heatmap
```

**Exit criterion:** Deliverable mode picked AND (if B) synthetic-data decision made AND captured. Echo the captured selection back to the user and confirm before moving to Phase 1.

---



## Phase 1: Define the Workflow

**Goal:** Get a shared, written model of the lab process before any modeling work.

> **HARD GATE — DO NOT PROCEED PAST THIS PHASE WITHOUT USER-SUPPLIED WORKFLOW STEPS.**
>
> Every lab is different. Generic / canonical workflows in `references/01-workflow-definition.md` are starting points only — they are NOT a substitute for the user's actual process. The model MUST elicit the user's specific workflow steps before any later phase runs.

### Required: elicit lab type, then workflow

Phase 1 is two ordered sub-steps. **Do not ask for workflow steps before the lab type is captured.**

### Step 1.1 — Ask the lab type / use case FIRST

Before the workflow-elicitation question, the skill **must** ask what kind of lab use case this is. This anchors everything that follows: vocabulary, default workflow templates, canonical metrics, and source-system inventories. Without it, option 3 ("Start from a default for my lab type") has no basis to draw from.

Use a structured `ask_user_question` (one question, multiple options). The tool will auto-append a "Something else" choice — do **not** add one yourself.

| Option | Label |
|--------|-------|
| 1 | **Clinical chemistry / immunoassay** |
| 2 | **Molecular / NGS / genomics** |
| 3 | **Anatomic pathology / histology** |
| 4 | **Mass spec (LC-MS/MS, GC-MS)** |
| 5 | **Manufacturing QC / release testing** |
| 6 | **R&D / discovery / contract testing** |

(The tool auto-renders a "Something else" choice for free-text — handle it by capturing the user's description and matching to the closest template if any.)

**Question text to use:**

> "Before anything else — what kind of lab use case are we working on? This anchors the workflow templates, metrics, and source-system assumptions for the rest of the session.
>
> 1. Clinical chemistry / immunoassay
> 2. Molecular / NGS / genomics
> 3. Anatomic pathology / histology
> 4. Mass spec (LC-MS/MS, GC-MS)
> 5. Manufacturing QC / release testing
> 6. R&D / discovery / contract testing"

After the user answers, **echo the choice back** ("Got it — we're working on a *<lab type>* lab.") and then proceed to Step 1.2.

### Step 1.2 — Ask how the user wants to provide the workflow steps

Now that the lab type is known, ask the workflow-elicitation question. Use a single 3-option `ask_user_question`. The `ask_user_question` tool **automatically appends a "Something else" choice** — do NOT include "Something else" as one of your options or it will appear twice.

| Option | Label | Description |
|--------|-------|-------------|
| 1 | **Type or paste a step list** | User will type / paste a numbered or bulleted workflow. |
| 2 | **Upload a process diagram** | User will attach a PNG / JPG / PDF / Visio / BPMN / whiteboard photo of the workflow. |
| 3 | **Start from a default for my lab type** | Skill picks the canonical workflow for the lab type from Step 1.1 out of `references/01-workflow-definition.md` and walks the user through it step-by-step for confirmation. |

(The tool will render a fourth choice — "Something else" — automatically.)

**Question text to use:**

> "Now I need your lab's specific workflow steps. How would you like to provide them?
>
> 1. Type or paste a step list
> 2. Upload a process diagram
> 3. Start from a default for my lab type"

### Immediately follow up based on the user's choice

The skill **must** issue the matching follow-up prompt in the **very next turn** — do not wait for the user to volunteer the input.

| If user picks | Follow-up prompt the skill issues immediately |
|---------------|-----------------------------------------------|
| 1 — Type or paste a step list | "Great — please paste your workflow now. List every step from sample arrival to final result, in order. Number them or use bullets. Include even trivial steps and any rerun / reflex / escalation branches." |
| 2 — Upload a process diagram | "Please upload the diagram now (PNG / JPG / PDF / Visio export / BPMN / whiteboard photo are all fine). Once it's uploaded I'll read the steps off it and echo them back to you." |
| 3 — Start from a default for my lab type | The lab type is already known from Step 1.1. Load the matching canonical workflow from `references/01-workflow-definition.md`, present it as a numbered list, and ask the user to confirm / edit / add / remove each step before proceeding. |
| Something else (auto-added by the tool) | "Tell me how you'd like to share it and we'll work with that." (e.g., voice transcript, link to a Confluence / Notion page, BPM tool export, dictated step-by-step.) |

After the user supplies the workflow (in whichever form), proceed to capture the missing context:

1. What is the unit of work? (sample, batch, plate, slide, run…)
2. For each step, who owns it? (accessioning tech, analyst, instrument operator, reviewer, LIMS auto-rule…)
3. Where are the handoffs? Where does the sample wait between steps?
4. What are the decision points / branches? (rerun, reflex test, escalate, reject, send-out)
5. Are there workflow variants per sample type or test? Which ones?

### Echo-and-confirm protocol

After capturing the steps, **always** echo the numbered list back to the user with the format below and ask "Is this correct? Any steps to add, remove, or reorder?" Do not move to Phase 2 until the user confirms.

```
Lab type:        <type>
Unit of work:    <unit>
Workflow variants:
  - <variant 1>
  - <variant 2>

Steps:
  1. <step>     owner=<role>   typical_duration=<X>   handoff_to=<step or queue>
  2. <step>     owner=<role>   ...
  N. <step>     owner=<role>

Decision points:
  - After step <K>: <branch logic>
```

See `references/01-workflow-definition.md` for elicitation prompts, capture template, and canonical lab workflows used as conversation starters.

**Exit criterion:** A numbered, owner-tagged list of steps with handoffs and decision points called out, **explicitly confirmed by the user**. Do not proceed to Phase 2 without this confirmation.

---

## Phase 2: Define the Optimization Metrics

**Goal:** Decide *what* "optimized" means before deciding *how* to optimize it. Optimizing the wrong metric is the most common ML-for-ops failure mode.

> **HARD GATE — DO NOT PROCEED PAST THIS PHASE WITHOUT A USER-CONFIRMED PRIMARY KPI WITH A NUMERIC TARGET.**
>
> Do **not** assume TAT (or any other metric) is the primary KPI. Different labs care about different things — a manufacturing-QC lab is usually optimizing first-pass yield or batch release time, an R&D lab is usually optimizing throughput, a clinical lab may be optimizing TAT *or* autoverification rate *or* cost-per-sample. **Ask the user. Do not pick a default.**

### Step 2.1 — Ask for the primary KPI explicitly

Use a structured `ask_user_question` to elicit the primary KPI. Options come from the canonical lab-KPI catalog in `references/02-optimization-metrics.md`. The tool will auto-append a "Something else" choice — do NOT add one yourself.

| Option | Label |
|--------|-------|
| 1 | **Turnaround Time (TAT)** — order-to-result elapsed time |
| 2 | **Throughput** — samples processed per unit time |
| 3 | **On-Time Delivery** — % results released within SLA |
| 4 | **First-Pass Yield** — % samples passing without rerun |
| 5 | **Cost per Sample** |
| 6 | **Instrument Utilization / Uptime** |

(The tool auto-renders a "Something else" choice for free-text — handle it by capturing the user's KPI in their own words and matching to the metric tree pattern in `references/02-optimization-metrics.md`.)

**Question text to use:**

> "What is the **primary KPI** you want this work to move? Pick the one number leadership cares about most. (Other metrics will become supporting metrics or guardrails — we'll capture those next.)
>
> 1. Turnaround Time (TAT)
> 2. Throughput
> 3. On-Time Delivery
> 4. First-Pass Yield
> 5. Cost per Sample
> 6. Instrument Utilization / Uptime"

After the user answers, **echo the choice back** — for example: "Got it — primary KPI is *<choice>*." Do not proceed to Step 2.2 without this echo.

### Step 2.2 — Capture the numeric target

Free-text follow-up:

> "What is the current value of *<chosen KPI>* and what is the target value you want to reach? Include units (minutes, hours, %, USD, etc.)."

Capture: `current = <value> <unit>`, `target = <value> <unit>`, `minimum acceptable = <value> <unit>`.

**Refuse to proceed without numeric values.** "Faster" or "better" is not a target.

### Step 2.3 — Capture supporting metrics

Free-text:

> "What other metrics must move with the primary KPI, or at least not regress? (e.g., for TAT: rerun rate, autoverification rate, QC pass rate.)"

### Step 2.4 — Capture hard constraints / guardrails

Free-text:

> "What hard constraints must we never violate? (e.g., regulatory TAT, accreditation requirements like CAP / CLIA / ISO 15189, max instrument hours, budget, headcount.)"

### Step 2.5 — Echo the metric tree back and confirm

Echo the captured tree using the format below and ask "Is this correct?" — do not move to Phase 3 until the user confirms.

```
PRIMARY KPI:    <name>   current=<X>   target=<Y>   min-acceptable=<Z>
SUPPORTING:
  - <metric>    current=<X>   must-move-to=<Y>
  - <metric>    must-not-regress-past=<Y>
GUARDRAILS:
  - <constraint>     threshold=<value>     source=<authority>
```

See `references/02-optimization-metrics.md` for canonical lab metrics, anti-patterns (e.g., reducing TAT by deferring QC is *not* a win), and the metric-tree pattern.

**Exit criterion:** A user-confirmed primary KPI with a numeric current value and target value, a supporting-metrics list, and explicit guardrail constraints. **No assumed KPI. No "faster / better" without numbers.**

---

## Phase 3: Identify the Most Time-Consuming Steps

**Goal:** Rank steps by cycle time so we know where the leverage is. ML applied anywhere else is wasted effort.

For each step from Phase 1:

1. What is the **median cycle time** for this step?
2. What is the **P95 cycle time**? (the long tail is usually where bottlenecks hide)
3. What is the **wait time** before this step starts? (often bigger than the step itself)
4. How **variable** is this step? (low variance → less ML opportunity; high variance → ML sweet spot)
5. Is this step **gated** by an external factor? (instrument schedule, reagent lot, reviewer availability)

Rank steps by `wait_time + P95_cycle_time`. The top 1-3 are your bottlenecks. See `references/03-bottleneck-identification.md` for queueing-theory-aware diagnostics and what each pattern usually means.

**Exit criterion:** A ranked list of bottleneck steps with quantitative evidence (median, P95, wait time, variance).

---

## Phase 4: Inventory Source Systems, Entities, Metrics per Step

**Goal:** Map *what data is available where* before designing models. This is also a feasibility gate — if the bottleneck step has no upstream signal, ML can't help.

For each step, fill in a row of the **Step × Source-System × Entity × Metric** matrix:

| Step | Source System | Entity | Available Metrics / Features | Latency | Owner |
|------|---------------|--------|------------------------------|---------|-------|
| Accessioning | LIMS | Sample, Patient, Order | sample_type, priority, ordering_provider, draw_time, volume | seconds | Accessioning tech |
| Centrifugation | LIMS + centrifuge logs | Sample, Run | spin_program, rotor_id, spin_start_ts, temp | minutes | Tech |
| Analyzer run | Instrument middleware | Sample, Test, Run, QC | reagent_lot, calibration_age, prior_QC_drift, queue_depth | seconds | Instrument |
| Tech review | LIMS | Result, Reviewer | flag_count, delta-check_hits, reviewer_id, shift | minutes | Reviewer |
| Final release | LIMS, EHR (HL7) | Result | autoverify_eligible, communication_channel | seconds | LIMS rules |

Source systems typically include: **LIMS** (Beaker, Sunquest, Cerner, NovoPath, custom), **Instrument middleware** (Roche cobas, Abbott AlinIQ, Beckman REMISOL, Sysmex WAM), **EHR / HL7 interface**, **Scheduling / staffing systems**, **Inventory / reagent tracking**, **Maintenance logs**, **Building / environmental sensors**, **ERP / cost data**.

See `references/04-source-systems-inventory.md` for canonical entity/metric inventories per lab type.

**Exit criterion:** A populated matrix covering every step from Phase 1, with explicit gaps marked `MISSING`.

---

## Phase 5: Place ML Models in Front of Bottlenecks

**Goal:** Position predictive models *before* each bottleneck so the prediction can change the routing or scheduling decision. A prediction made *after* the bottleneck has no leverage.

> **HARD GATE — DO NOT PROCEED PAST THIS PHASE WITHOUT AN EXPLICIT, USER-CONFIRMED TARGET VARIABLE FOR EACH MODEL.**
>
> The single most important question of this phase is: **"What exactly are we predicting?"** Do not infer the target variable from context. Do not pick one from the canonical list silently. Ask the user directly, in plain language, and have them confirm.

### Required: ask the target-variable question explicitly (per bottleneck)

For each bottleneck identified in Phase 3, run this two-step elicitation. **Do not skip Step 5.A. Do not propose a target and ask "OK?" — that counts as silently choosing.**

#### Step 5.A — Free-text target-variable question (REQUIRED, ONE PER BOTTLENECK)

Issue this question verbatim using a free-text prompt (or `ask_user_question` with a single open-ended question and no options if the agent supports that):

> **"For the bottleneck `[bottleneck step name]` — what is the target variable for the model we'll place in front of it? In other words, what is the single thing you want to know *before* this step happens that would let you change the decision (route, prioritize, pre-empt, schedule)? Describe it in plain language; I will not pick one for you."**

If — and only if — the user responds with "I don't know" or "you suggest one", drop into Step 5.A.alt. Otherwise capture their plain-language target and proceed to Step 5.B.

#### Step 5.A.alt — Suggestion mode (only if the user explicitly asked you to suggest)

Use a structured `ask_user_question` with these canonical targets. The tool will auto-append a "Something else" choice — do NOT add one yourself.

| Option | Label |
|--------|-------|
| 1 | **Will this sample need a rerun?** (binary, before analyzer run) |
| 2 | **Predicted cycle time of this batch / sample** (regression, before queue assignment) |
| 3 | **Will this instrument fail QC in the next 24 h?** (binary, before next maintenance window) |
| 4 | **Predicted tech-review time** (regression, before sample is queued for review) |
| 5 | **Predicted time-to-failure of this instrument** (time-to-event, before failure) |
| 6 | **Will this result auto-verify?** (binary, at result generation) |

After the user picks, **echo the choice back** and then move to Step 5.B.

#### Step 5.B — Capture full model contract and confirm

Capture using this template and echo it back for confirmation:

```
Bottleneck:           <step name>
Target variable:      <plain-language name>            e.g., "will this sample need a rerun"
Variable type:        binary / multiclass / regression / time-to-event
Positive class:       <only if binary/multiclass>     e.g., "rerun = TRUE"
Unit of prediction:   sample / batch / instrument-hour / shift / ...
Prediction horizon:   <how far in advance>             e.g., "5 minutes before analyzer queue"
Decision driven:      <route / prioritize / schedule / pre-empt>
Decision threshold:   <how the prediction is consumed> e.g., "P(rerun) > 0.3 -> route to backup analyzer"
Success metric:       <how we measure that placement worked>
```

Ask the user to confirm the captured block before proceeding to model placement.

#### Step 5.C — Locate the model in the workflow (REQUIRED, ONE PER MODEL)

Do **not** skip this step. The model has no operational leverage if its placement in the workflow is implicit.

Pull the numbered workflow from Phase 1 back into the conversation, then ask the user (verbatim):

> **"Looking at your workflow:**
>
> ```
> 1. <step 1>
> 2. <step 2>
> ...
> N. <step N>
> ```
>
> **Between which two steps should this model run? The prediction must land BEFORE the bottleneck step `[bottleneck]` so it can change the decision (route / prioritize / pre-empt / schedule). Specify:**
>
> - **Run AFTER:** `<step number / name>` — the latest step whose data is available as input
> - **Run BEFORE:** `<step number / name>` — the next step the prediction will influence**"**

Capture using this template and echo it back for confirmation:

```
Model placement
  Run AFTER step:    <step name>     (data available as input up to this step)
  Run BEFORE step:   <step name>     (the bottleneck whose decision the prediction changes)
  Insertion latency: <max acceptable latency for the prediction to be useful>
  Trigger:           <event / schedule that fires the prediction>
```

If the user proposes a placement where the prediction lands *at* or *after* the bottleneck, push back: "That placement runs after the bottleneck — the prediction won't be actionable. We need to place it before step `<bottleneck>`. Which earlier step has the input data we need?" Iterate until placement is BEFORE the bottleneck.

Confirm the placement block with the user before moving to Step 5.D.

#### Step 5.D — Complete the model contract

After placement is confirmed, complete the rest of the model contract:

1. Re-confirm the **decision** the prediction will drive (route, prioritize, pre-empt, schedule).
2. Define the **feature scope** — which Phase 4 features are available *at prediction time* (no leakage). Use the "Run AFTER" step from 5.C as the cutoff.
3. Define the **model contract** — input schema, output schema, refresh cadence, owner.

Draw the workflow with **model insertion points** marked. See `references/05-model-placement.md` for canonical placements (rerun prediction, TAT prediction, QC failure prediction, instrument predictive maintenance, autoverification confidence).

**Exit criterion:** For each bottleneck:
- User-confirmed target variable AND
- User-confirmed placement (run-after step + run-before step) AND
- Documented model contract: target, variable type, decision, prediction horizon, feature scope, input/output schema.

**No assumed targets. No assumed placements. No "I picked TAT-prediction for you, OK?" shortcuts.**

---

## Phase 6: Run ML Models to Predict the Target Variable

**Goal:** Train, register, and serve the models defined in Phase 5. Delegate the platform mechanics to the `machine-learning` skill — this skill owns the lab-specific framing.

Workflow:

1. **Build features** — typically a Snowflake **Dynamic Table** that joins the Phase 4 source systems and produces one row per prediction unit (sample, batch, instrument-hour). Delegate to `dynamic-tables`. *Mode B with synthetic data:* generate a synthetic feature table per the Phase 0 spec (realistic distributions, plausible correlations, base rates aligned to clinical reality, fixed seed).
2. **Train** — Snowpark ML or external framework, register in Snowflake **Model Registry**. Delegate to `machine-learning`. Hold out a time-aware test split.
3. **Evaluate on the holdout** and persist metrics to `LAB_MODEL_METRICS`:
   - Classifiers: **accuracy**, **F1** (per class + macro), **precision**, **recall**, **AUC**, **calibration** (Brier score, reliability plot bins).
   - Regressors: **MAE**, **RMSE**, **R²**, residual distribution.
4. **Score** — call the registered model from a Dynamic Table, Task, or Streamlit app. Persist predictions to a `LAB_PREDICTIONS` table keyed by the prediction unit.
5. **Monitor** — track prediction quality (calibration, drift) and operational impact (did TAT actually drop?).
6. **Validate input quality** — gate scoring on `data-quality` checks; predictions on stale or out-of-range features should be flagged, not silently consumed.

Output schema (recommended):

```
LAB_PREDICTIONS (
  prediction_id            STRING,
  prediction_unit_type     STRING,   -- 'SAMPLE', 'BATCH', 'INSTRUMENT_HOUR', ...
  prediction_unit_id       STRING,
  model_name               STRING,
  model_version            STRING,
  target_variable          STRING,
  predicted_value          VARIANT,  -- numeric or class probability
  prediction_ts            TIMESTAMP_NTZ,
  decision_window_end_ts   TIMESTAMP_NTZ,
  feature_snapshot_id      STRING,
  data_quality_status      STRING    -- 'OK', 'STALE', 'OUT_OF_RANGE', ...
);
```

See `references/06-model-execution.md` for the Snowflake delivery patterns (Dynamic Table feature pipeline + Model Registry inference + Task-based monitoring).

**Exit criterion:** Predictions are landing in `LAB_PREDICTIONS` for each Phase 5 model, with monitoring in place.

---

## Phase 7: Explain Predictions and Identify Key Drivers

**Goal:** A prediction without an explanation does not change behavior. Lab leads need to know *why* a sample is flagged for rerun, *why* an instrument is predicted to fail QC, *why* this batch will blow TAT.

For each model:

1. **Per-prediction explanation** — SHAP values (preferred), or feature contributions for tree models, or attention weights / integrated gradients for deep models. Persist in a `LAB_PREDICTION_EXPLANATIONS` table.
2. **Global feature importance** — surface the top drivers across the population. Refresh on a cadence aligned with model retraining.
3. **Cohort analysis** — compare drivers across slices that matter to the lab (instrument, shift, reagent lot, sample type, ordering location).
4. **Narrative explanation** — use `cortex-ai-functions` (`AI_COMPLETE`) to convert the top SHAP features into a one-sentence explanation a tech can read on the bench.
5. **Action loop** — every explanation should map to a concrete operational action (reroute, recalibrate, reschedule, restock, escalate).

Output schema (recommended):

```
LAB_PREDICTION_EXPLANATIONS (
  prediction_id              STRING,
  feature_name               STRING,
  feature_value              VARIANT,
  contribution               FLOAT,    -- SHAP value or equivalent
  rank                       INTEGER,
  explanation_method         STRING,   -- 'SHAP', 'FEATURE_IMPORTANCE', 'AI_NARRATIVE'
  generated_ts               TIMESTAMP_NTZ
);
```

See `references/07-explainability.md` for SHAP implementation patterns on Snowpark ML and AI_COMPLETE narrative templates.

**Exit criterion:** Every prediction in `LAB_PREDICTIONS` has corresponding rows in `LAB_PREDICTION_EXPLANATIONS`, and a global feature-importance report is published.

---

## Cross-Domain Composition

Common pairings:

- **Lab Allotrope + Lab ML Optimization** — `hcls-pharma-lab-allotrope` standardizes instrument outputs into ASM; this skill consumes the standardized data as Phase 4 source-system input.
- **Lab ML Optimization + machine-learning** — Phase 6 delegates training/registry to the platform `machine-learning` skill.
- **Lab ML Optimization + dynamic-tables** — Phase 4 / Phase 6 feature pipeline.
- **Lab ML Optimization + cortex-ai-functions** — Phase 7 narrative explanations via `AI_COMPLETE`.
- **Lab ML Optimization + developing-with-streamlit** — operations dashboard surfacing predictions, drivers, and recommended actions.
- **Lab ML Optimization + data-quality** — gating scoring on input completeness and range.

## Anti-Patterns

- **Modeling before metrics.** Don't train anything until Phase 2 has a numeric target.
- **Modeling non-bottlenecks.** A great model on a 5-second step is a waste of effort.
- **Predicting after the bottleneck.** Predictions must land *before* the decision they're meant to influence.
- **No explanation.** A binary flag with no driver is operationally useless.
- **No data-quality gate.** Silent scoring on stale features causes silent harm.
- **Optimizing TAT by skipping QC / review.** Always check the metric tree's guardrails before celebrating.
