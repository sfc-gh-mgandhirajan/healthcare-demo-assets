# Phase 4 Reference — Source Systems, Entities, Metrics

Map *what data is available where* before designing models. This is the feasibility gate — if a bottleneck has no upstream signal, ML cannot help.

## Inventory Matrix

For every step from Phase 1, fill one row:

| Step | Source System | Entity | Available Metrics / Features | Latency | Owner |
|------|---------------|--------|------------------------------|---------|-------|

Mark explicit gaps as `MISSING` so they become a data-acquisition work item rather than a silent assumption.

## Canonical Source Systems

| Source | Examples | Typical Entities | Typical Metrics |
|--------|----------|------------------|------------------|
| LIMS | Beaker, Sunquest, Cerner, NovoPath, custom | Sample, Order, Patient, Test, Result, Reviewer | timestamps, priority, sample type, flags, delta-check hits |
| Instrument middleware | Roche cobas, Abbott AlinIQ, Beckman REMISOL, Sysmex WAM | Run, Reagent Lot, Calibrator, QC | reagent age, calibration age, QC drift, run start/stop |
| EHR / HL7 | Epic, Cerner | Encounter, Order, Patient | order priority, ordering provider, location, patient demographics |
| Sequencer LIMS | Illumina BSSH, Clarity LIMS | Library, Pool, Run | library QC, % passing filter, cluster density |
| Scheduling / staffing | UKG, custom | Shift, Tech | tech-on-shift, shift role, planned-vs-actual |
| Inventory / reagents | LIMS module, Smart Inventory | Reagent Lot, Calibrator | lot expiry, on-hand, lot QC history |
| Maintenance logs | CMMS, instrument logs | Instrument, MaintenanceEvent | last PM, error codes, downtime |
| Building / environmental | BMS, BACnet | Room, Sensor | temperature, humidity, power events |
| ERP / cost | SAP, Oracle | CostCenter, Activity | reagent cost, labor cost, allocation |

## Per-Lab-Type Starting Inventories

### Clinical Chemistry

| Step | Source | Entity | Metrics |
|------|--------|--------|---------|
| Accessioning | LIMS | Sample, Order | priority, sample_type, ordering_location, draw_time |
| Centrifugation | LIMS + centrifuge | Sample, Spin | spin_program, rotor, temp |
| Analyzer queue | LIMS | Sample, Queue | queue_depth, planned_run_id |
| Analyzer run | Middleware | Run, Reagent | reagent_lot, calibrator_age, prior_QC_drift |
| Auto-verification | LIMS rules | Result | flag_count, delta-check hits, prior_value |
| Tech review | LIMS | Result, Reviewer | reviewer_id, shift, queue_depth |
| Release | LIMS / HL7 | Result | autoverify_eligible, channel |

### NGS / Molecular

| Step | Source | Entity | Metrics |
|------|--------|--------|---------|
| Extraction | LIMS | Sample | extraction_kit_lot, operator |
| Extraction QC | Tapestation/Qubit | QC | conc_ng_ul, A260/280, RIN/DIN |
| Library prep | LIMS | Library | prep_kit, index_set, operator |
| Library QC | Tapestation | LibraryQC | size_distribution, conc |
| Pooling | LIMS | Pool | n_libraries, normalization_method |
| Sequencer queue | Sequencer LIMS | Pool | queue_depth, planned_run |
| Sequencing | Sequencer | Run | cluster_density, %passing_filter, error_rate |
| Demux | Pipeline | Sample | barcode_balance |
| Bioinformatics | Pipeline | Sample | runtime, coverage, %on_target |
| Sign-out | LIMS | Result | reviewer, reflex_logic |

### Manufacturing QC

| Step | Source | Entity | Metrics |
|------|--------|--------|---------|
| Receipt | LIMS / ERP | Batch | batch_id, product, line, planned_release |
| Test plan | LIMS | Plan | n_methods, gating_methods |
| Method execution | Instrument | Run | method_id, instrument_id, operator |
| QC | Instrument | QC | spec_limits, observed_value |
| Investigation | LIMS | Investigation | OOS_root_cause, n_retests |
| Release | LIMS / ERP | Batch | release_decision, lead_time |

## Latency Categories

Tag each row's latency so Phase 5 knows whether the feature is usable in the model's prediction window:

- `seconds` — available immediately (LIMS, EHR HL7)
- `minutes` — short delay (instrument middleware push)
- `hours` — daily refresh (inventory, ERP)
- `event-driven` — only on event (maintenance, OOS)

## Anti-Patterns

- **Listing systems without mapping to steps.** A source system without a step linkage is unusable.
- **Ignoring feature availability at prediction time.** A feature that arrives after the bottleneck cannot be used to predict it.
- **Assuming joins work.** Sample IDs across LIMS / instrument / ERP are usually inconsistent. Plan for ID reconciliation.
