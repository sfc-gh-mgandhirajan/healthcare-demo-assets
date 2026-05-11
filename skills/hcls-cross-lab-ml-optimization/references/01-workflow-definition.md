# Phase 1 Reference — Workflow Definition

The goal of Phase 1 is a shared, written model of the lab process. Without it, every subsequent phase has nothing to anchor against.

## Elicitation Prompts

Ask the user (in this order):

1. "What kind of lab is this? (clinical chemistry, molecular, pathology, R&D, QC, contract testing, manufacturing release...)"
2. "What is the unit of work? (sample, batch, plate, slide, run...)"
3. "Walk me through the workflow from arrival to final result. List every step, even the ones that feel trivial."
4. "Who owns each step?"
5. "Where are the handoffs (step → step), and where do units wait between steps?"
6. "What are the decision points / branches? (rerun, reflex test, escalate, reject, send out)"
7. "Are there variants of the workflow per sample type or test? Which ones?"

## Capture Template

```
Lab type:
Unit of work:
Workflow variants:
  - <variant 1>
  - <variant 2>

Steps:
  1. <step name>           owner=<role>   typical_duration=<X min>   handoff_to=<step or queue>
  2. ...
  N. <step name>           owner=<role>

Decision points:
  - After step <K>: <branch logic>
```

## Canonical Lab Workflows (starting points)

### Clinical Chemistry / Immunoassay

1. Order receipt (LIMS / EHR HL7 ORM)
2. Phlebotomy / sample draw
3. Specimen transport
4. Accessioning (LIMS)
5. Centrifugation / pre-analytical processing
6. Aliquoting / sample sorting
7. Analyzer queue
8. Analyzer run (on-instrument)
9. Auto-verification rules (LIMS)
10. Tech review (manual)
11. Result release (LIMS → EHR HL7 ORU)

### Molecular / NGS

1. Sample accessioning
2. Nucleic acid extraction
3. QC of extract (qubit, tapestation)
4. Library prep
5. Library QC
6. Pooling / normalization
7. Sequencer queue
8. Sequencing run
9. Demultiplexing
10. Bioinformatics pipeline
11. Variant interpretation
12. Sign-out

### Mass Spec (LC-MS/MS)

1. Accessioning
2. Sample prep (extraction, derivatization)
3. Plate loading
4. LC-MS/MS run
5. Data analysis / peak integration
6. QC review
7. Result release

### Manufacturing QC Release

1. Sample receipt from production
2. Test plan assignment
3. Method execution (per analyte)
4. QC pass/fail evaluation
5. Investigation (if OOS)
6. Batch release decision

## Common Pitfalls

- **Combining sub-steps.** "Sample prep" hides 4-7 sub-steps that often contain the bottleneck.
- **Ignoring waits.** Wait time between steps is usually larger than step time itself — capture it as its own row.
- **Single happy path.** Real labs have rerun loops, reflex tests, escalations. Capture them.
- **Skipping LIMS auto-rules.** Auto-verification, delta checks, and reflex rules are *steps* with their own latency and failure modes.
