# PhysicianAssist — Modular Agentic AI for Physician Workflows

A **modular, EHR-integrable** clinical decision support solution built on Snowflake. A single **Cortex Agent** — exposed as a standard REST API — orchestrates both structured clinical data queries (via Cortex Analyst) and industry-tuned medical imaging AI (MedGemma 4B on SPCS). Any EHR system, clinical workflow, or patient portal can integrate this agent with a single API call — no Snowflake-specific SDK required.

## Why This Matters

- **Modular by design** — The Cortex Agent is a standard REST API (SSE streaming). Epic, Cerner, athenahealth, or any application with HTTP capability can embed it into existing physician workflows.
- **Industry-tuned AI/ML** — MedGemma 4B (Google's medical vision-language model) runs on Snowflake's GPU infrastructure (SPCS), purpose-built for ECG, X-ray, and echo interpretation — not a general-purpose chatbot.
- **Agentic orchestration** — The agent intelligently routes physician questions to the right tool: structured patient data queries go to Cortex Analyst, medical image interpretation goes to MedGemma. Physicians ask natural language questions; the agent figures out the rest.
- **Zero data movement** — Patient records, model inference, and agent orchestration all stay within Snowflake's governed perimeter. No PHI leaves the platform.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Any EHR / Clinical App / Patient Portal            │
│  (Epic, Cerner, custom app, this React demo, etc.)  │
└──────────────────────┬──────────────────────────────┘
                       │ Standard REST API (SSE Streaming)
                       ▼
        ┌──────────────────────────────┐
        │  Cortex Agent (claude-4-sonnet)  │
        │  POST /api/v2/databases/.../agents/:run  │
        └──────┬───────────────┬───────┘
               │               │
     ┌─────────▼──────┐  ┌─────▼────────────────────┐
     │ PATIENT_ANALYST │  │ MEDGEMMA_INTERPRETER     │
     │ Cortex Analyst  │  │ Stored Proc → SPCS GPU   │
     │ + Semantic View │  │ (MedGemma 4B)            │
     └────────┬───────┘  └─────┬────────────────────┘
              │                │
     6 Interactive Tables    Image Stage (ECG, X-ray, Echo)
```

> **The included React app is a reference frontend** simulating a patient panel in an EHR. The core value is the Cortex Agent REST API — swap the frontend for any clinical application.

## Project Structure

```
├── configure.py                  # One-command setup: rewrites all files for your DB/schema/account
├── himss-physician-app/          # React + TypeScript + Tailwind (Vite)
│   ├── src/
│   │   ├── components/           # PatientSidebar, PatientDetailPanel, ChatPanel, ChatBubble
│   │   ├── hooks/                # useAgentChat (SSE streaming), usePatientData
│   │   └── types/                # TypeScript interfaces
│   └── public/images/dummy/      # Sample medical images (ECG, X-ray, Echo, etc.)
├── sql/
│   ├── deploy_medgemma.sql       # Step 2: MedGemma deployment prerequisites (compute pool, EAI, secrets)
│   ├── setup_data.sql            # Step 3: DDL for tables, data, MedGemma stored procedure
│   └── himss_patient_semantic_model.yaml  # Semantic View definition (6 tables, 17 VQRs)
└── README.md
```

## Snowflake Objects

> Default database/schema: `DEMO_DB.HIMSS_DEMO` — configurable via `configure.py`

| Object | Type | Purpose |
|--------|------|---------|
| `<DB>.<SCHEMA>.PATIENTS_IT` | Interactive Table | Patient demographics |
| `<DB>.<SCHEMA>.CONDITIONS_IT` | Interactive Table | Diagnoses & conditions |
| `<DB>.<SCHEMA>.MEDICATIONS_IT` | Interactive Table | Prescriptions |
| `<DB>.<SCHEMA>.VITALS_IT` | Interactive Table | Vital signs |
| `<DB>.<SCHEMA>.ENCOUNTERS_IT` | Interactive Table | Visits & encounters |
| `<DB>.<SCHEMA>.MEDICAL_IMAGES_IT` | Interactive Table | Image metadata |
| `<DB>.<SCHEMA>.HIMSS_PATIENT_SEMANTIC_VIEW` | Semantic View | Text-to-SQL (17 verified queries) |
| `<DB>.<SCHEMA>.MEDGEMMA_MEDICAL_INTERPRETER` | Stored Procedure | MedGemma 4B inference proxy |
| `SNOWFLAKE_INTELLIGENCE.AGENTS.HIMSS_PHYSICIAN_AGENT` | Cortex Agent | Orchestrator (claude-4-sonnet) |

## Prerequisites

- Snowflake account (commercial AWS/Azure/GCP) with Cortex Agent, Cortex Analyst, and SPCS enabled
- Python 3.8+ (for the configure script)
- Node.js 18+
- A [Hugging Face account](https://huggingface.co/join) with access to [MedGemma 4B](https://huggingface.co/google/medgemma-4b-it) (accept the license)
- A Hugging Face access token ([generate here](https://huggingface.co/settings/tokens))
- A Snowflake PAT (Programmatic Access Token) for API authentication

---

## Setup

### Step 1: Configure for your environment

Run the configure script to update all files (SQL, YAML, frontend) with your database, schema, and account:

```bash
python3 configure.py --db MY_DB --schema MY_SCHEMA --account myorg-myaccount --warehouse MY_WH \
    --interactive-wh MY_INTERACTIVE_WH --image-stage MY_DB.MY_SCHEMA.ECG_STAGE \
    --agent-db SNOWFLAKE_INTELLIGENCE --agent-schema AGENTS --agent-name MY_AGENT
```

Or run interactively:

```bash
python3 configure.py
```

This updates:
- `sql/deploy_medgemma.sql` — SET variables for DB/schema
- `sql/setup_data.sql` — SET variables for DB/schema/warehouse/image stage
- `sql/himss_patient_semantic_model.yaml` — all `database:` / `schema:` fields and VQR SQL
- `himss-physician-app/.env.example` — account, database, schema, warehouse, agent config

> **Defaults**: `DEMO_DB.HIMSS_DEMO` on `myorg-myaccount` with `DEMO_BUILD_WH`. If these work for you, skip this step.

### Step 2: Deploy MedGemma to SPCS

#### 2a. Run the prerequisite SQL

Open `sql/deploy_medgemma.sql` in a Snowflake worksheet and execute it. Before running, update the secrets:

| Placeholder | What to put |
|-------------|-------------|
| `hf_YOUR_TOKEN_HERE` | Your Hugging Face access token |
| `your_snowflake_pat_here` | Your Snowflake PAT |

This creates:
- `MEDGEMMA_GPU_POOL` — GPU compute pool (GPU_NV_S, 1 node)
- `HF_TOKEN_SECRET` — Hugging Face token for gated model access
- `MEDGEMMA_PAT_SECRET` — PAT for stored procedure authentication
- `MEDGEMMA_SPCS_EAI` — External access integration
- `MEDGEMMA_DEMO.PUBLIC.ECG_STAGE` — Stage for medical images

#### 2b. Import MedGemma via Snowsight UI

1. In Snowsight, navigate to **AI & ML → Models → Import model**
2. **Model handle**: `google/medgemma-4b-it`
3. **Task**: `text-generation`
4. Check **"Trust remote code"**
5. **HF token secret**: `<YOUR_DB>.<YOUR_SCHEMA>.HF_TOKEN_SECRET`
6. **Model name**: `MEDGEMMA_4B`
7. **Version**: `v1`
8. **Database/Schema**: `<YOUR_DB>.<YOUR_SCHEMA>`
9. Click **Continue to deployment**
10. **Service name**: `MEDGEMMA_SERVICE`
11. Check **"Create REST API endpoint"**
12. **Compute pool**: `MEDGEMMA_GPU_POOL`
13. **GPU**: `1`
14. Click **Deploy**

Deployment takes ~10-15 minutes. Monitor at **Monitoring → Services & jobs → Jobs tab**.

#### 2c. Retrieve your SPCS endpoint URL

After deployment completes, run:

```sql
SHOW ENDPOINTS IN SERVICE <YOUR_DB>.<YOUR_SCHEMA>.MEDGEMMA_SERVICE;
```

Copy the `ingress_url` value — it looks like:
```
https://<unique-id>-<org>-<account>.snowflakecomputing.app
```

You'll need this URL (with `/__call__` appended) in the next step.

#### 2d. Upload medical images to stage

```
PUT file:///path/to/himss-physician-app/public/images/dummy/*.png
    @<YOUR_IMAGE_STAGE>/dummy/;
```

### Step 3: Create Tables, Data & Stored Procedure

Open `sql/setup_data.sql` in a Snowflake worksheet. Update the SPCS endpoint at the top:

```sql
SET MEDGEMMA_ENDPOINT = 'https://<your-endpoint>.snowflakecomputing.app/__call__';
```

The DB, schema, and warehouse variables were already set by `configure.py` in Step 1. Execute the entire script.

### Step 4: Deploy the Semantic View

```sql
SELECT SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
  '<YOUR_DB>.<YOUR_SCHEMA>',
  $$<paste contents of sql/himss_patient_semantic_model.yaml>$$
);
```

### Step 5: Create the Cortex Agent

Create the agent via Snowsight UI or DDL:
- **Agent name**: `HIMSS_PHYSICIAN_AGENT` in `SNOWFLAKE_INTELLIGENCE.AGENTS`
- **Model**: `claude-4-sonnet`
- **Tools**: `PATIENT_ANALYST` (semantic view), `MEDGEMMA_MEDICAL_INTERPRETER` (stored procedure)

### Step 6: Frontend App

```bash
cd himss-physician-app
cp .env.example .env.local
```

Edit `.env.local` with your values:

```
VITE_SNOWFLAKE_PAT=<your Snowflake PAT>
VITE_SNOWFLAKE_ACCOUNT=<your account identifier>
VITE_SNOWFLAKE_DATABASE=<your database>
VITE_SNOWFLAKE_SCHEMA=<your schema>
VITE_SNOWFLAKE_WAREHOUSE=<your interactive warehouse>
VITE_AGENT_DATABASE=<agent database>          # default: SNOWFLAKE_INTELLIGENCE
VITE_AGENT_SCHEMA=<agent schema>              # default: AGENTS
VITE_AGENT_NAME=<agent name>                  # default: HIMSS_PHYSICIAN_AGENT
VITE_AGENT_MODEL=<agent model>                # default: claude-4-sonnet
```

Then:

```bash
npm install
npm run dev
```

The app runs at `http://localhost:5173`. The Vite dev server proxies `/api` requests to your Snowflake account.

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `VITE_SNOWFLAKE_PAT` | Snowflake PAT for API authentication | (required) |
| `VITE_SNOWFLAKE_ACCOUNT` | Snowflake account identifier | `myorg-myaccount` |
| `VITE_SNOWFLAKE_DATABASE` | Database name | `DEMO_DB` |
| `VITE_SNOWFLAKE_SCHEMA` | Schema name | `HIMSS_DEMO` |
| `VITE_SNOWFLAKE_WAREHOUSE` | Warehouse for interactive queries | `HIMSS_INTERACTIVE_WH` |
| `VITE_AGENT_DATABASE` | Cortex Agent database | `SNOWFLAKE_INTELLIGENCE` |
| `VITE_AGENT_SCHEMA` | Cortex Agent schema | `AGENTS` |
| `VITE_AGENT_NAME` | Cortex Agent name | `HIMSS_PHYSICIAN_AGENT` |
| `VITE_AGENT_MODEL` | Cortex Agent model | `claude-4-sonnet` |

### SQL Session Variables (setup_data.sql)

| Variable | Description | Default |
|----------|-------------|---------|
| `MY_DB` | Database name | `DEMO_DB` |
| `MY_SCHEMA` | Schema name | `HIMSS_DEMO` |
| `MY_WAREHOUSE` | Build warehouse | `DEMO_BUILD_WH` |
| `MEDGEMMA_ENDPOINT` | SPCS endpoint URL | (required) |
| `IMAGE_STAGE` | Fully-qualified image stage | `MEDGEMMA_DEMO.PUBLIC.ECG_STAGE` |

---

## Verification

After completing all steps, verify the deployment (replace `<DB>.<SCHEMA>` with your values):

```sql
SELECT SYSTEM$GET_SERVICE_STATUS('<DB>.<SCHEMA>.MEDGEMMA_SERVICE');

CALL <DB>.<SCHEMA>.MEDGEMMA_MEDICAL_INTERPRETER(
    'text', NULL,
    'Patient: 67F, HTN, T2DM. Meds: Metoprolol 50mg, Lisinopril 20mg, Metformin 1000mg.',
    'Are there any drug interactions?'
);

CALL <DB>.<SCHEMA>.MEDGEMMA_MEDICAL_INTERPRETER(
    'image', 'IMG-7001', NULL, 'Interpret this ECG'
);
```

## EHR Integration Guide

The Cortex Agent is a standard REST endpoint. To integrate into any EHR or clinical workflow:

### 1. API Call (any language/platform)

```bash
curl -X POST "https://<account>.snowflakecomputing.com/api/v2/databases/<AGENT_DB>/schemas/<AGENT_SCHEMA>/agents/<AGENT_NAME>:run" \
  -H "Authorization: Bearer <PAT>" \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -d '{
    "model": "claude-4-sonnet",
    "messages": [{
      "role": "user",
      "content": [{"type": "text", "text": "What medications is patient P-1001 on and are there any interactions?"}]
    }]
  }'
```

### 2. Integration Patterns

| Pattern | How |
|---------|-----|
| **EHR Sidebar Widget** | Embed an iframe or micro-frontend that calls the Agent REST API. Physician clicks a patient → widget sends context + question. |
| **SMART on FHIR App** | Launch as a SMART app from Epic/Cerner. Map FHIR patient context to Agent queries. |
| **Backend Service** | Call the Agent API from your middleware (Node.js, Python, Java, .NET). Parse SSE events and render in your existing UI. |
| **Clinical Decision Alert** | Trigger Agent queries from HL7/FHIR events (e.g., new lab result → "any drug interactions with current meds?"). |
| **Batch / Async** | Call the stored procedure (`MEDGEMMA_MEDICAL_INTERPRETER`) directly via SQL for batch image interpretation workflows. |

### 3. What You Get

| Component | Integration Surface | What It Does |
|-----------|---------------------|--------------|
| **Cortex Agent** | REST API (SSE) | Agentic orchestrator — routes questions to the right tool |
| **Cortex Analyst** | Via Agent (or direct REST) | Natural language → SQL over patient data |
| **MedGemma (SPCS)** | Via Agent or `CALL` stored proc | Industry-tuned medical image interpretation |
| **Semantic View** | Cortex Analyst tool | Governed, validated text-to-SQL with 17 verified queries |

> The React app in this repo demonstrates all of these patterns. Use `useAgentChat.ts` as a reference implementation for SSE streaming integration.

## Key Capabilities

1. **Modular REST API** — Standard HTTP endpoint; integrates with any EHR, portal, or clinical workflow
2. **Industry-Tuned AI/ML** — MedGemma 4B on SPCS, purpose-built for medical imaging (not a general chatbot)
3. **Agentic Orchestration** — Cortex Agent routes physician queries to structured data or imaging AI automatically
4. **Cortex Analyst** — Natural language to SQL over 6 clinical tables via semantic view
5. **Zero Data Movement** — Patient records and model inference stay within Snowflake's governed perimeter
6. **Fully Configurable** — All endpoints, databases, models, and stages are parameterized via env vars and `configure.py`

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Model import fails | Check HF token is valid and MedGemma license is accepted |
| Service won't start | Verify compute pool has available GPU nodes: `SHOW COMPUTE POOLS` |
| Stored procedure timeout | MedGemma cold start can take 1-2 min. Retry after service warms up |
| "MEDGEMMA_REST_URL not configured" | Run `SET MEDGEMMA_ENDPOINT = '...'` before calling setup_data.sql |
| Frontend 401 errors | Verify PAT in `.env.local` is valid and not expired |
| Proxy errors in dev | Check `VITE_SNOWFLAKE_ACCOUNT` in `.env.local` matches your account |
| Wrong database/schema | Re-run `python3 configure.py` with correct values |

## License

Internal demo — not for distribution.
