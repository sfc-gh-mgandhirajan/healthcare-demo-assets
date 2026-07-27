# Pharmacy Cost Command Center — App Data Contract

Hand-off for building the **React app (Snowflake App on SPCS)** in a separate session.
Everything the app needs is here; you should NOT need the plugin-build context.

## Where the data lives
- **Sandbox (built + validated now):** `AA_HC_PAYER_DERIVED_DATA_PRODUCTS.PHARMACY_COST_INTELLIGENCE_SANDBOX`
- **Prod target (customer deploy):** `AA_HC_PAYER_DERIVED_DATA_PRODUCTS.PHARMACY_COST_INTELLIGENCE` (same object names, no `_SANDBOX`). Parameterize DB/schema in the app config; object names + columns are identical.
- **Connection:** use your Snowflake CLI connection (`snow ... -c <conn>`). The DB/schema above are the reference build's names; a customer deploy points at their own `target_database.target_schema` (object names + columns are identical).

## The app reads ONLY these objects (stable contract)
The 12 `VW_*` views (fixed columns) + 1 agent. All are accelerator-owned, fixed-schema — customer variability is already absorbed upstream. Numbers are the **current data's honest values** (not the mockup's $88 PMPM / $312M / $28.6M — see scale note below).

### KPI tiles → `VW_KPI_SUMMARY` (1 row)
`NET_PMPM_CURRENT, NET_PMPM_PRIOR, PMPM_YOY_PCT, TARGET_PMPM, PMPM_VS_PLAN, PROJECTED_NEXT12_SPEND, ADDRESSABLE_TOTAL, NET_SPEND_CURRENT_12, ADDRESSABLE_PCT_OF_SPEND`
Current values: PMPM $120.23 (prior $90.18, +33.3%), plan $93.30, +$26.93 vs plan, proj-12 ~$130M, addressable $20.12M (16.2%).

### Panel → view → columns (maps to mockup `nav3-leftpane.html`)
| Mockup panel (id) | View | Columns |
|---|---|---|
| Trend & Forecast (cTrend) | `VW_PMPM_TREND` | MONTH, IS_FORECAST, PMPM_ACTUAL, PMPM_FORECAST_BASE, PMPM_FORECAST_HIGH, PMPM_PLAN, NET_SPEND, TREND_ANNUAL_PCT |
| Why it's moving — P/V/M (cMix) | `VW_TREND_PVM` | COMPONENT, AMOUNT, PCT_OF_TREND, TOTAL_CHANGE |
| Specialty vs traditional (cSpecialty) | `VW_SPEND_BY_BENEFIT` | BENEFIT_TYPE, NET_SPEND, GROSS_SPEND, CLAIM_LINES, PCT_OF_NET_SPEND |
| High-cost & emerging (cHiCost) | `VW_HIGH_COST_CLASSES` | THERAPEUTIC_CLASS, NET_SPEND_CURRENT, NET_SPEND_PRIOR, YOY_GROWTH_PCT |
| Top contributors to trend (cContrib) | `VW_TREND_CONTRIBUTORS` | THERAPEUTIC_CLASS, DOLLARS_ADDED_YOY, YOY_GROWTH_PCT, VOLUME_EFFECT, PRICE_EFFECT, MIX_EFFECT, DOMINANT_DRIVER, DRIVER_RANK |
| Addressable prize by lever (cBridge) | `VW_ADDRESSABLE_BRIDGE` | LEVER_ORDER, LEVER_KEY, LEVER_NAME, STATUS, GROSS_POOL, CAPTURE_RATE, ADDRESSABLE_AMOUNT, PCT_OF_TOTAL, RATIONALE |
| Waste concentration (cConc) | `VW_WASTE_CONCENTRATION` | CLAIMANT_BAND, MEMBERS, NET_SPEND, PCT_OF_SPEND |
| Rebate yield by class (cRebate) | `VW_REBATE_YIELD` | THERAPEUTIC_CLASS, REBATE_AMOUNT, GROSS_SPEND, REBATE_YIELD_PCT |
| Formulary leakage (cOffForm) | `VW_FORMULARY_LEAKAGE` | DRUG_NAME, THERAPEUTIC_CLASS, STEERABLE_NET_SPEND, CLAIM_LINES |
| UM — prior-auth outcomes (cPA) | `VW_PA_OUTCOMES` | DECISION, PA_COUNT, PCT_OF_TOTAL |
| Adherence — PDC by class (cPdcClass) | `VW_ADHERENCE_BY_CLASS` | THERAPEUTIC_CLASS, AVG_PDC, ADHERENT_PCT, MEASURED_MEMBERS, PDC_THRESHOLD |

Notes: dollar columns are raw dollars (format client-side, e.g. `$M`); rate/pct columns are already percentages EXCEPT `AVG_PDC`, `PDC_THRESHOLD`, `CAPTURE_RATE` which are 0–1 fractions.

### Conversational panel → Cortex Agent
- **Agent FQN:** `AA_HC_PAYER_DERIVED_DATA_PRODUCTS.PHARMACY_COST_INTELLIGENCE_SANDBOX.AGT_PHARMACY_COST_COMMAND_CENTER`
- Tools: `query_pharmacy_cost` (Analyst over `SV_PHARMACY_COST_INTELLIGENCE`), `search_coverage_policies` (Cortex Search, returns policy citations), `create_chart`.
- Call via the **Cortex Agent Run REST API** (`POST /api/v2/databases/.../schemas/.../agents/<name>:run`, streaming SSE) with the SPCS service's OAuth token. Verified working (PMPM + GLP-1 policy questions).

### "AI Powered Insights" block (each tab) → `MDL_TAB_INSIGHTS` (4 rows, pre-baked)
The mockup's per-tab **AI Powered Insights** card reads a pre-generated row — do NOT call the agent live for these. One row per band, refreshed on cadence (suspended daily task; runs offline).
`TAB_ORDER (1–4), TAB_KEY (bandA–bandD), TAB_TITLE, AGENT_PERSONA, INSIGHT_MD, TAGS (ARRAY of SQL|POLICY), RUN_ID, GENERATED_AT`
- Map to tabs by `TAB_ORDER` (1=Spend Performance & Outlook, 2=Cost Drivers & Composition, 3=Addressable Savings Opportunity, 4=Cost Management Levers).
- `INSIGHT_MD` is ready-to-render GitHub-flavored **markdown bullets** (bold numbers) — render with any MD component. Content is deeper/cross-cutting analysis (not chart restatements).
- `AGENT_PERSONA` → the "Cost Intelligence Agent · <band>" byline. `TAGS` → source chips (`SQL` = quantitative, `POLICY` = cited a coverage policy).
- How it's built (context only, app just SELECTs): 1 Cortex Agent run over all 4 bands → 1 `AI_COMPLETE` (Claude Opus 4.8, schema-enforced) → 4 rows.

## Scale note (do not "fix" in the app)
Current data is smaller/differently-proportioned than the mockup by design decision: ~$124M net current (vs mockup $312M), +21–33% trend (vs +11%), $20.1M addressable (vs $28.6M), specialty ~61% (vs 62%). App shows live values; mockup numbers are illustrative only.

## Packaging (DONE — app is bundled + deploy is validated)
The app is now bundled at `assets/app/` and the Phase-4 `app-deploy` skill deploys it via the native `snow app deploy` path. Validated end-to-end in sandbox: all `/api/*` return 200; KPI ($120.23, +33.3%), the 4-row insights block, and the live agent all render.
- The plugin is the **deployer**: `app-deploy` renders `snowflake.yml` (where the app object lands) + **bakes the data-target into `lib/constants.ts`** (which schema the app queries) from `config.json`, runs `snow app deploy`, then runs `GRANT_APP_ACCESS`.
- **App Runtime env caveat (learned the hard way):** `app.yml` has NO `env:` support, and the auto-injected `SNOWFLAKE_DATABASE`/`SNOWFLAKE_SCHEMA` are the app's own service context (`SNOWFLAKE_SCHEMA = SERVICE_CONTEXT_NESTED_SCHEMA`, `SNOWFLAKE_DATABASE` empty) — NOT the data location. The data product FQN MUST be baked into source at deploy time. Do not rely on `SNOWFLAKE_DATABASE/SCHEMA`.
- `next build` is auto-detected server-side from `package.json`; no build step in `app.yml`.
- Mockup reference: `wiki/solution-accelerators/pharmacy-cost-command-center-accelerator/pharmacy-cost-command-center-nav3-leftpane.html`.
- Native deploy mechanics: the `snowflake-apps` skill / `snow app deploy`.
