# Clinical Document Intelligence for Edwards Lifesciences
## Executive Demo Talk Track

---

## ACT 1: BACKGROUND & PERSONA (2-3 minutes)
*Goal: Establish empathy. Make them feel seen before you show them anything.*

---

**[Open with the persona — not the technology]**

> "Let me paint a picture you'll recognize.
>
> It's a Tuesday morning. Your VP of Regulatory just got word that the FDA has scheduled a pre-approval inspection for the ALLIANCE trial — four weeks out.
>
> She turns to the TMF Manager and asks a simple question: **'Are we audit-ready?'**
>
> What happens next is... not simple.
>
> The TMF Manager opens Veeva Vault. She starts pulling document inventories — trial by trial, zone by zone. She cross-references protocol versions against what's filed with the FDA. She checks whether every site is using the correct version of the Informed Consent Form — because the last inspection finding at a competitor was exactly that: a site in Germany was consenting patients on an outdated ICF.
>
> This takes **days**. Sometimes **weeks**. And even then, she's not confident she hasn't missed something.
>
> Meanwhile, down the hall, the Head of Clinical Strategy is preparing for a board meeting. She needs to answer: **'What's our competitive position in transcatheter tricuspid? Who else is recruiting? How do our trial designs compare?'**
>
> She has a team of analysts manually searching ClinicalTrials.gov, copying results into PowerPoint slides. By the time the deck is done, the data is already stale.
>
> These aren't edge cases. This is every week at a company running five pivotal trials across three regulatory jurisdictions."

**[Pause. Let it land.]**

> "Now — everyone in this room knows Snowflake as your enterprise data warehouse. Your analytics teams run dashboards on it. Your data engineers build pipelines in it.
>
> But what if I told you that the platform you already own could solve both of those problems — the TMF readiness gap and the competitive intelligence gap — in a single application?"

---

## ACT 2: THE "WHAT IF?" PIVOT (1-2 minutes)
*Goal: Shift from pain to possibility. Create tension between the old way and what's now possible.*

---

> "What if your TMF Manager could ask a question in plain English — 'What's the completeness percentage for TRISCEND II?' — and get an answer in seconds, not days?
>
> What if your Regulatory VP could see a real-time governance dashboard showing every trial, every TMF zone, every document — color-coded by risk — without waiting for someone to build a report?
>
> What if your Clinical Strategy team could ask, 'What competing tricuspid valve trials are currently recruiting against us?' and get back a structured competitive analysis — with NCT numbers, sponsors, endpoints — pulled from 5.7 million trial records on ClinicalTrials.gov... in the same application where your internal documents live?
>
> What if a single AI agent could search your internal eTMF, query your governance metrics, generate an audit report, AND search the global trial registry — all orchestrated automatically based on the question asked?
>
> That's not a roadmap. That's what I'm about to show you."

---

## ACT 3: LIVE DEMO FLOW (8-10 minutes)
*Goal: Walk through 4 screens. Each screen maps to a persona and a pain point. Keep it conversational.*

---

### Screen 1: Trial Portfolio Overview
*Persona: CMO / VP Clinical Operations*

> "This is your portfolio at a glance. Five active trials. For each one — document count, completeness percentage, average document age.
>
> Notice TRISCEND II is at 75% completeness. That's a flag. If an inspector walks in tomorrow, we have gaps.
>
> ALT-FLOW II only has two documents — it's an early-stage program. That's expected. But ALLIANCE at 83% with an FDA inspection coming? That needs attention.
>
> This is the view your CMO wants on Monday morning. No spreadsheets. No email chains. Just: where are we, and where are the risks?"

**[Click into a trial]**

### Screen 2: Trial Drilldown — Document Inventory
*Persona: TMF Manager / Document Controller*

> "Now I'm the TMF Manager. I click into ALLIANCE and I see every document — protocol versions, informed consent forms by site, regulatory submissions.
>
> I can see that Site-001 has ICF v3.1 — current. Site-002 also has v3.1 — current. Good.
>
> I can see we have three protocol versions — v3.0, v4.0 with Amendment 1, v5.0 with Amendment 2. The amendment history is tracked.
>
> If an inspector asks, 'Show me the consent form for Site-001 and confirm it matches the latest approved protocol version' — my TMF Manager can answer that in seconds.
>
> Today, that's a multi-hour scavenger hunt."

### Screen 3: Governance Dashboard
*Persona: VP Regulatory Affairs / Quality*

> "This is the governance view. TMF zones — these are the ICH-GCP mandated categories that every TMF must be organized by.
>
> Zone 1, Trial Management — 100% complete. Zone 5, Regulatory — we have gaps. Zone 8, Third Parties — needs attention.
>
> This is what a regulatory VP needs before an inspection: a single view that says 'here's where we're strong, here's where we're exposed.' It's the difference between walking into an FDA inspection confident versus walking in hoping they don't ask about Zone 5."

### Screen 4: AI Agent — The Showstopper
*Persona: All of the above — this is the unifier*

> "Now here's where it gets interesting. This is a Cortex Agent — Snowflake's AI orchestration layer. It has four tools at its disposal:"

**[Point to the suggested questions — they're now organized by persona]**

> "Notice the questions are organized by role. Each persona sees questions relevant to their world. Let me walk you through one from each."

**Query 1 — CMO lens (tmf_analytics):**
> "Let's start as the CMO: **'Which trials have the lowest document completeness and what's the risk exposure?'**"

*[Agent routes to tmf_analytics — Cortex Analyst generates SQL against the semantic view]*

> "It automatically knew that was a metrics question, routed it to Cortex Analyst, generated SQL against our governance data, and gave me a risk-ranked answer. No one told it which tool to use — it figured it out."

**Query 2 — TMF Manager lens (etmf_search):**
> "Now I'm the TMF Manager: **'Show me all protocol amendments across trials — are any sites using outdated versions?'**"

*[Agent routes to etmf_search — Cortex Search scans across parsed PDF content]*

> "Different question, different tool. This time it searched across the actual PDF content of our 21 parsed clinical documents and found the amendment history. This is the query that keeps TMF Managers up at night before an inspection."

**Query 3 — VP Regulatory lens (audit_report_gen):**
> "Now the VP of Regulatory needs to prepare for the FDA: **'Generate an audit readiness report for the ALLIANCE trial.'**"

*[Agent routes to audit_report_gen — calls the stored procedure]*

> "This generated a comprehensive audit report — executive summary, completeness score with RAG status, document inventory, zone-by-zone analysis, and specific action items. This is the report your TMF Manager would spend three days building manually. The agent produced it on demand."

**Query 4 — Clinical Strategy lens (clinical_trials_registry) — THE SHOWSTOPPER:**
> "And now — the competitive intelligence play. This is the one that changes the conversation: **'What are Medtronic, Abbott, and Boston Scientific doing in our therapeutic space? How do their trial designs compare to ours?'**"

*[Agent routes to clinical_trials_registry — searches 5.7M ClinicalTrials.gov records]*

> "This is pulling from ClinicalTrials.gov — 5.7 million trial records. It mapped out what your three biggest competitors are doing across TAVR, mitral, and tricuspid. Trial names, NCT numbers, endpoints, enrollment targets, recruiting status.
>
> Your competitive intelligence team would take a week to compile this. The agent did it in 30 seconds. And it's always current — because it's pulling from a live Marketplace dataset.
>
> Now imagine your Head of Clinical Strategy asking this every Monday morning before the leadership meeting. That's the kind of advantage that changes how a company operates."

**[Optional bonus — show the second Clinical Strategy question if time allows]:**
> "And if we have time — watch this: **'Compare enrollment criteria between our ENCIRCLE trial and similar mitral valve replacement trials on ClinicalTrials.gov.'** This cross-references our internal protocol documents with the global trial registry. Internal + external intelligence in a single query."

---

## ACT 4: CLOSING NARRATIVE (2 minutes)
*Goal: Zoom out. Connect back to the business. Land the "not just an EDW" message.*

---

> "Let me step back and tell you what just happened.
>
> We took 21 clinical trial documents — protocols, informed consent forms, regulatory submissions — and turned them into an intelligent, queryable, governed system.
>
> We used Cortex AI to parse the PDFs. Cortex Search to make them semantically searchable. A Semantic View to make governance metrics queryable in plain English. A Cortex Agent to orchestrate across all four capabilities. And a ClinicalTrials.gov knowledge extension — from the Snowflake Marketplace — to add competitive intelligence without building anything.
>
> All of it runs on Snowflake. The app you just saw is deployed on Snowpark Container Services — inside your Snowflake account. The data never leaves your security perimeter.
>
> **For your TMF Manager** — this is the difference between a three-day audit prep scramble and a 30-second query.
>
> **For your VP of Regulatory** — this is real-time governance visibility across every trial, every zone, every jurisdiction.
>
> **For your Clinical Strategy team** — this is competitive intelligence at the speed of a question.
>
> **For your CMO** — this is a Monday morning portfolio health check that actually tells you where the risks are.
>
> And for your CIO and data team — this is proof that the platform you already invested in isn't just a warehouse. It's a clinical intelligence platform.
>
> The question isn't whether you need this. The question is: how many inspection cycles, how many analyst-weeks, how many competitive blind spots happen between now and when you decide to build it?"

**[Pause.]**

> "We can help you get there. Let's talk about what a pilot looks like for your team."

---

## APPENDIX: OBJECTION HANDLING

| Objection | Response |
|---|---|
| "We already have Veeva Vault for eTMF" | "Veeva is your system of record — it stays. This sits on top, giving you AI-powered search, cross-trial analytics, and competitive intel that Veeva doesn't offer. Think of it as the intelligence layer over your existing eTMF." |
| "Is the AI accurate enough for regulated use?" | "The AI is used for search, summarization, and analytics — not for regulatory decision-making. Every answer is grounded in your actual documents and data. And because it runs on Snowflake, you have full audit trails, role-based access, and data governance built in." |
| "Our documents are in the hundreds of thousands" | "That's exactly why you need this. Manual TMF management doesn't scale. Cortex Search and Cortex Analyst are built for enterprise scale. The ClinicalTrials.gov index alone has 5.7 million records." |
| "What about PHI/PII in clinical documents?" | "Everything runs inside your Snowflake account. The data never leaves your security perimeter. Snowflake's governance framework — dynamic masking, RBAC, audit logging — applies to every document and every query." |
| "How long would this take to build for real?" | "What you just saw was built in days using synthetic data. A production pilot with your real eTMF documents would take 4-6 weeks, depending on integration with your existing document management system." |

---

## DEMO TIMING GUIDE

| Section | Duration | Key Moment |
|---|---|---|
| Act 1: Background & Persona | 2-3 min | "This is every week at a company running five pivotal trials" |
| Act 2: What If? | 1-2 min | "That's not a roadmap. That's what I'm about to show you." |
| Act 3: Live Demo | 8-10 min | Competitive intel query is the jaw-drop moment |
| Act 4: Closing | 2 min | "It's a clinical intelligence platform" |
| **Total** | **~15 min** | Leave 10-15 min for Q&A |
