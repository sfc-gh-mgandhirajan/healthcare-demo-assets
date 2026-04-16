# RWD Cohort Explorer — Demo Walkthrough Prompts
## For Real World Data Lab Webinar

---

## Demo Flow (5–7 minutes)

### Step 1: Introduction (30 sec)
Open the app and point out:
- Sidebar: 3-step progress indicator (Describe → Review → Explore)
- Sidebar: Snowflake features powering the app
- Main area: Quick-start example queries ready to click
- "This is a guided workflow — no SQL knowledge needed"

---

### Step 2: Build a Cohort — Type 2 Diabetes + CKD (2–3 min)

**Click the first suggestion button:**
> Find adults with type 2 diabetes and CKD stage 3 or higher, with HbA1c above 8%, who failed metformin.

**While AI responds (step 1 — Describe):**
- "Cortex AI interprets the clinical intent and asks smart clarifying questions"
- "Notice the step indicator in the sidebar — we're in step 1"

**When AI asks clarifying questions, type:**
> Adults 18+. Most recent HbA1c. Metformin failure means they have a prescription that ended. Go ahead.

**AI generates the cohort spec — app moves to step 2 (Review):**
- Point out the readable inclusion/exclusion criteria
- "You can expand the SQL to see exactly what will run — full transparency"
- "Two clear choices: Confirm & Run, or Refine if something needs changing"

**Click "Confirm & run cohort":**
- App executes the query and moves to step 3 (Explore)
- Point out the green success banner with cohort size

**Walk through the tabs:**
- **Demographics**: Age, sex, region, payer mix charts
- **Clinical metrics**: HbA1c and eGFR distributions with clinical thresholds
- **HCP prioritization**: Top providers treating this cohort, filterable by specialty/geography

---

### Step 3: Quick Second Cohort (1 min)

**Click "Start over" in the sidebar, then click the second suggestion:**
> Show me patients with both heart failure and type 2 diabetes who are over 60 years old.

**When AI asks questions, type:**
> Include all heart failure types. No exclusions. Generate the spec.

**Click "Confirm & run cohort" on the review screen.**

**Talking points:**
- "Notice how fast the full cycle is — describe, review, explore"
- "Different cohort, different demographic profile"

---

### Step 4: RWE Insights — The Evidence Story (1–2 min)

**This is the key differentiator. Switch to the "RWE insights" tab.**

**Walk through the three sections:**

1. **Treatment landscape**
   - "Here we see the therapy utilization across the cohort — broken down by line of therapy"
   - "Notice metformin as first-line, then the mix of GLP-1, SGLT2, DPP-4 as second-line"
   - "The discontinuation rate tells us about real-world adherence"

2. **Unmet need signals**
   - "This is where RWD becomes RWE — we're quantifying unmet need"
   - Point to poor glycemic control rate: "X% of patients still have HbA1c above 8"
   - Point to therapy gaps: "Y% are on first-line only with no intensification"
   - "These are the signals that drive medical strategy"

3. **AI evidence brief**
   - Click "Generate RWE evidence brief"
   - "Cortex AI synthesizes all the data into a structured evidence brief"
   - "This is publication-ready — cohort overview, key findings, implications"
   - "Took seconds, not weeks"

**Key narrative arc:**
- "We started with a natural language question → built a governed cohort → now we have actionable real world evidence"
- "This is the RWD-to-RWE pipeline — all in Snowflake, all governed"

---

### Step 5: AI Summary & Export (1 min)

**In the Save & export tab:**
1. Click "Generate summary"
   - "Cortex AI creates a clinical narrative suitable for a medical affairs report"
2. Show the Save to Snowflake option
   - "Cohort persists as a governed Snowflake table"
3. Show CSV download
   - "Patient IDs exported for downstream analysis"

---

### Step 6: Advanced Query (Optional, 1 min)

**Click "Start over", then type:**
> Find patients with CKD stage 3 or higher who have low eGFR and are on first-line diabetes therapy.

**When AI asks questions:**
> Low eGFR means below 60. First-line therapy means metformin. No age restriction.

**Click "Confirm & run cohort".**

**Talking point:**
- "Combining labs, medications, and conditions in one natural language query — no SQL required"

---

## Key Messages to Weave In

| When | Message |
|------|---------|
| AI asks clarifying questions | "The AI clarifies like a human analyst — no ambiguity in the final cohort" |
| Review screen | "Full transparency — you see exactly what criteria and SQL will run before executing" |
| Confirm button | "One click to execute on governed data — no ETL, no data copies" |
| Demographics tab | "Real-time analytics on live Snowflake tables" |
| **RWE insights tab** | **"This is where RWD becomes RWE — quantified unmet need, treatment patterns, evidence brief"** |
| **Evidence brief** | **"Cortex generates a structured evidence brief in seconds, not weeks"** |
| HCP tab | "Field teams can identify high-value providers instantly" |
| AI summary | "Cortex generates publication-ready narratives" |
| Save/export | "Cohorts persist as Snowflake tables with full governance" |

---

## Backup Prompts (if something goes wrong)

If the AI doesn't generate a cohort spec:
> Please generate the cohort specification now based on what we discussed.

If the cohort query returns no results:
> (Click "Start over" and try a simpler query)
> Find all patients with type 2 diabetes.

If a button doesn't respond:
> Refresh the page and start over — state resets cleanly.

---

## Data Summary (for Q&A)
- **5,000 synthetic patients** across 4 US regions
- **11 clinical tables** + 2 derived views
- Conditions: T2D, CKD (stages 3-5), Heart Failure, ESRD, etc.
- Labs: HbA1c, eGFR, Creatinine, Fasting Glucose
- Medications: Metformin, GLP-1 agonists, SGLT2 inhibitors, Insulin, etc.
- 200 providers across 5 specialties
- All data is synthetic — no PHI/PII
