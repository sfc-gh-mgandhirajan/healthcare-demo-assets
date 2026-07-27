# Patient Experience Command Center — App Data Contract

The **stable interface** between the deployed Snowflake backend (this plugin) and the React app. The app
reads **only** the objects listed here (the `VW_*` views + the agent) — never the underlying
`MDL_*` / `DENORM_*` / `DT_*` objects. Every mockup panel below is mapped to exactly one serving object.

- **Database / schema:** `CUSTOMER_DERIVED_DB.PATIENT_EXPERIENCE`
- **Query warehouse:** `COMPUTE_WH` · **Agent:** `AGT_PATIENT_EXPERIENCE`
- Reference: mockup `patient-experience-command-center-mockup.html` (Patient Journey Board, 5 stages + agent rail).
- Access: app role needs `SELECT` on the `VW_*` views + run access on the agent (`USAGE`/`REFERENCES`+`SELECT` on its semantic view). No base-table access required.

---

## Coverage matrix — every mockup element → backend object

| Mockup element (chart id) | Serving object | Status |
|---|---|---|
| Header: HCAHPS star, composite, VBP $ at risk, awaiting recovery, formal grievances, composite YoY | `VW_HOUSE_KPIS` | ✅ built |
| Journey ribbon — 5 stage scores + trend | `VW_JOURNEY_RIBBON` | ✅ built |
| s1 · Wait vs target by area (`cWait`) | `VW_WAIT_BY_AREA` | ✅ built |
| s1 · Access momentum: on-time + LWBS, 5 quarters (`cNoShow`) | `VW_ACCESS_MOMENTUM` | ✅ built |
| s2 · Communication top-box vs target & benchmark (`cComm`) | `VW_MEASURE_BENCHMARK` | ✅ built |
| s2 · Communication subdomains 5-quarter momentum (`cCommTrend`) | `VW_MEASURE_TREND` | ✅ built |
| s2 · Equity lens by race (`cEquity`) | `VW_EQUITY_GAP` | ✅ built |
| s3 · Responsiveness drivers (`cDriver`) | `VW_DRIVER_IMPORTANCE` | ✅ built (model SHAP/gain) |
| s3 · Composite by unit (`cUnit`) | `VW_UNIT_SCORECARD` | ✅ built |
| s4 · Composite YoY waterfall (`cPointsLost`) | `VW_POINTS_LOST` | ✅ built |
| s4 · Discharge readiness / close-the-loop funnel (`cDischarge`) | `VW_DISCHARGE_READINESS` | ✅ built |
| s5 · Grievance aging vs regulatory window (`cRecovery`) | `VW_GRIEVANCE_AGING` | ✅ built |
| s5 · Service-recovery SLA adherence (`cResolution`) | `VW_RECOVERY_SLA` | ✅ built |
| All stages · Voice-of-Patient sentiment table | `VW_VOICE_THEMES` | ✅ built (see voice notes) |
| All stages · AI insight card | `VW_TAB_INSIGHTS` | ✅ built |
| Agent rail · scoped Q&A + cited verbatims | `AGT_PATIENT_EXPERIENCE` (+ `COMMENT_SEARCH`) | ✅ built |
| Agent rail · governed action queue + lifecycle | `VW_ACTION_QUEUE` + `ACTION_EVENT_LOG` | ✅ built |

---

## Object schemas

### Header — `VW_HOUSE_KPIS` (1 row)
`COMPOSITE_TOPBOX, STAR_LEVEL, PTS_TO_NEXT_STAR, RECOMMEND_TOPBOX_PCT, HIGH_RISK_ENCOUNTERS,
AVG_DETRACTOR_RISK, NET_INPATIENT_REVENUE, VBP_AT_RISK_USD, COMPOSITE_YOY_DELTA_PTS,
PATIENTS_AWAITING_RECOVERY, FORMAL_GRIEVANCES_OPEN`
- Maps to the 4 header KPI tiles: star (`STAR_LEVEL`/`PTS_TO_NEXT_STAR`), composite (`COMPOSITE_TOPBOX` +
  `COMPOSITE_YOY_DELTA_PTS` for the ▼ delta), VBP (`VBP_AT_RISK_USD`), awaiting recovery
  (`PATIENTS_AWAITING_RECOVERY` + `FORMAL_GRIEVANCES_OPEN`).

### Journey ribbon — `VW_JOURNEY_RIBBON` (5 rows)
`STAGE_ORDER, STAGE_KEY (s1..s5), STAGE_NAME, SCORE_PCT, PRIOR_PCT, QOQ_DELTA, TREND`

### s1 Arrival & Access
- **`VW_WAIT_BY_AREA`** — `AREA, AVG_WAIT_MIN, TARGET_MIN` (bullet bar; bar=`AVG_WAIT_MIN`, tick=`TARGET_MIN`).
- **`VW_ACCESS_MOMENTUM`** — `QUARTER, ON_TIME_PCT, LWBS_PCT, N_VISITS` (5-quarter sparklines).

### s2 Admission & Communication
- **`VW_MEASURE_BENCHMARK`** — `MEASURE_CODE, MEASURE_NAME, TOPBOX_PCT, TARGET_TOPBOX, NATIONAL_AVG_TOPBOX`
  (bullet bar: bar=`TOPBOX_PCT`, solid tick=`TARGET_TOPBOX`, dashed tick=`NATIONAL_AVG_TOPBOX`). Filter to
  the communication measures (`H_COMP_1/2/3`, plus info-consistency proxy) for this card; reusable for any measure.
- **`VW_MEASURE_TREND`** — `QUARTER, MEASURE_CODE, TOPBOX_PCT` (5-quarter momentum per measure).
- **`VW_EQUITY_GAP`** — `RACE, NURSE_TOPBOX, HOUSE_AVG, GAP_PTS, N_RESPONSES` (diverging bar on `GAP_PTS`).

### s3 Care & Responsiveness
- **`VW_DRIVER_IMPORTANCE`** — `RANK, DRIVER, IMPORTANCE` (model-derived; map `F_*` driver codes to labels).
- **`VW_UNIT_SCORECARD`** — `UNIT_ID, UNIT_NAME, SERVICE_LINE, COMPOSITE_TOPBOX, STAR_LEVEL,
  PTS_TO_NEXT_STAR, RECOMMEND_TOPBOX_PCT, N_RESPONSES, AVG_DETRACTOR_RISK, HIGH_RISK_ENCOUNTERS`
  (bar vs house avg from `VW_HOUSE_KPIS.COMPOSITE_TOPBOX`).

### s4 Discharge & Transition
- **`VW_POINTS_LOST`** — `MEASURE_CODE, DOMAIN_NAME, CUR_TOPBOX, PRIOR_TOPBOX, DELTA_PTS,
  WEIGHTED_CONTRIB_PTS` (waterfall on `WEIGHTED_CONTRIB_PTS`).
- **`VW_DISCHARGE_READINESS`** — `STEP_ORDER, STEP, RATE_PCT, TARGET_PCT` (funnel: 3 steps).

### s5 Follow-up & Loyalty
- **`VW_GRIEVANCE_AGING`** — `AGING_BUCKET, BUCKET_ORDER, N_GRIEVANCES` (horizontal count bars).
- **`VW_RECOVERY_SLA`** — `SLA_ORDER, SLA_METRIC, ADHERENCE_PCT, TARGET_PCT` (bullet bar).

### Voice-of-Patient (all stages) — `VW_VOICE_THEMES`
`THEME, JOURNEY_STAGE, MENTIONS, AVG_SENTIMENT, PCT_POSITIVE, PCT_NEUTRAL, PCT_NEGATIVE, NET_SENTIMENT`
- Filter by `JOURNEY_STAGE` to the active stage. `PCT_*` drive the pos/neu/neg bar; `NET_SENTIMENT` the net
  chip; `MENTIONS` the volume.
- **Voice notes (see caveats):** the mockup's per-row **QoQ trend arrow** and **"correlated operational
  signal"** text are NOT columns here. Options: (a) surface them via the **agent** (it can compute QoQ from
  comment dates and cite the correlated metric), or (b) add a `VW_VOICE_THEMES_TREND` enrichment later. The
  static HCAHPS-domain subtitle (`vmap`) is presentation text in the app.

### AI insights (all stages) — `VW_TAB_INSIGHTS`
`STAGE_ORDER, STAGE_NAME, SCORE_PCT, QOQ_DELTA, TREND, INSIGHT` — one AI recommendation per stage for the
"Experience Agent" insight card.

### Agent rail — `AGT_PATIENT_EXPERIENCE`
- Conversational Q&A scoped per stage; call via Cortex Agents REST API (`.../agents/AGT_PATIENT_EXPERIENCE:run`).
  Tools: Analyst (`SV_PATIENT_EXPERIENCE_INTELLIGENCE`), Search (`COMMENT_SEARCH`, cited verbatims),
  `data_to_chart`. Onboarding questions embedded in the agent spec.

### Governed action queue — `VW_ACTION_QUEUE` (+ `ACTION_EVENT_LOG`)
`ENCOUNTER_ID, PATIENT_ID, UNIT_ID, RISK_SCORE, RISK_TIER, PRIMARY_REASON, ALL_REASONS,
RECOMMENDED_ACTION, OWNING_TEAM, RECOMMENDED_CHANNEL`
- HIGH-risk, contactable (consent-filtered), un-actioned; sort by `RISK_SCORE` desc. The mockup's
  "Act via agent" writes an `ACTION_EVENT_LOG` row (STATUS OPEN→IN_PROGRESS→DONE); Tier-1 in-app,
  Tier-2 notification, Tier-3 MCP/reverse-ETL are optional delivery adapters keyed off that record.

---

## Sample queries
```sql
SELECT * FROM CUSTOMER_DERIVED_DB.PATIENT_EXPERIENCE.VW_HOUSE_KPIS;
SELECT * FROM CUSTOMER_DERIVED_DB.PATIENT_EXPERIENCE.VW_JOURNEY_RIBBON;
SELECT * FROM CUSTOMER_DERIVED_DB.PATIENT_EXPERIENCE.VW_WAIT_BY_AREA;
SELECT * FROM CUSTOMER_DERIVED_DB.PATIENT_EXPERIENCE.VW_VOICE_THEMES WHERE JOURNEY_STAGE = 'Care & Responsiveness';
SELECT * FROM CUSTOMER_DERIVED_DB.PATIENT_EXPERIENCE.VW_ACTION_QUEUE ORDER BY RISK_SCORE DESC LIMIT 50;
```

## Units, freshness, and caveats
- **Units:** all `*_TOPBOX*` / `*_PCT` fields are 0–100; `*_RISK_SCORE`/`AVG_DETRACTOR_RISK` are 0–1;
  `AVG_WAIT_MIN`/`TARGET_MIN` are minutes; `VBP_AT_RISK_USD`/`NET_INPATIENT_REVENUE` are dollars.
- **Freshness & refresh-mode integrity** (all views carry a 1-hour target lag; underlying refresh modes audited):
  - **INCREMENTAL** (process only new/changed rows — cost-safe AI): `CALL_TRANSCRIPT` (AI_TRANSCRIBE),
    `COMMENT_ENRICHED` (sentiment + classify), `DT_ACTION_WORKQUEUE`. So `VW_VOICE_THEMES`,
    `VW_ACTION_QUEUE`, and the agent's voice search stay fresh without re-paying AI on every refresh.
  - **Task-incremental:** `VW_ACTION_QUEUE` risk fields come from `MDL_DISSAT_RISK`, scored by the
    change-detecting `MDL_DISSAT_RISK_SCORE_TASK` (delta only; full re-score only on model-version bump).
  - **FULL refresh (by design — no per-refresh AI cost):** the large descriptive spines
    (`DENORM_*`, `FS_PX_RISK_FEATURES`) and the small aggregate models (`MDL_STAR_RATING`,
    `MDL_POINTS_LOST`, `MDL_EQUITY_GAP`, `MDL_JOURNEY_STAGE_SCORE`, `MDL_VOICE_SENTIMENT`) — Snowflake
    auto-selects FULL for these complex/aggregate queries; they recompute on their lag, no AI involved.
  - **Known exception:** `MDL_TAB_INSIGHTS` (feeds `VW_TAB_INSIGHTS`) is a FULL DT that calls `AI_COMPLETE`
    over 5 rows, so it re-runs 5 AI calls when it refreshes (only when journey scores change). Accepted as
    trivial; can be given a longer `TARGET_LAG` if desired.
  - `VW_*` over CORE reference/event tables (`VW_WAIT_BY_AREA`, `VW_ACCESS_MOMENTUM`, `VW_MEASURE_*`,
    `VW_DISCHARGE_READINESS`, `VW_GRIEVANCE_AGING`, `VW_RECOVERY_SLA`) read live source tables.
- **PII:** `PATIENT_ID` / `ENCOUNTER_ID` are sensitive internal keys — do not surface externally. The
  work-queue is already consent-filtered (`Do Not Contact` excluded).
- **Demo-data-vintage caveats (logic correct, synthetic data quirks):**
  1. `VW_GRIEVANCE_AGING` — synthetic `REGULATORY_DUE_DATE`s predate today, so all open grievances bucket as
     "Overdue". A demo-data refresh that re-bases grievance dates to "now" restores the spread.
  2. `VW_RECOVERY_SLA` — synthetic resolution timing is slow, so adherence reads well below target
     (reinforces the "recovery loop closing too slowly" narrative, but is more extreme than the mockup).
  3. `VW_DISCHARGE_READINESS` — "7-day follow-up scheduled" reads low because few synthetic appointments are
     `Follow-up` within a 7-day lead; call-reached / issue-resolved match the mockup (~59% / ~41%).
  These are addressable with a small demo-data-prep step (like `refine_experience_signal.sql`), separate
  from the plugin's customer path; the view logic is production-correct.
