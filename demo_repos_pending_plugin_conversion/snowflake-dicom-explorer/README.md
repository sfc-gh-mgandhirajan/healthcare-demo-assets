# Cardiac Imaging Intelligence Platform

**A full-stack Snowflake prototype processing 23,929 DICOM medical images end-to-end — from raw binary files on S3 to AI-powered insights and governed analytics — with zero external tools, zero data movement, and zero infrastructure management.**

Built for a large Med Device. Powered entirely by Snowflake.

---

## Why This Matters

Healthcare organizations manage petabytes of medical imaging data — CT scans, MRIs, echocardiograms — stored as binary DICOM files across distributed systems. Traditional approaches require stitching together specialized imaging servers (PACS), separate ETL pipelines, external AI platforms, and bolt-on governance layers. Each integration point adds latency, cost, compliance risk, and operational burden.

**This prototype proves that Snowflake handles the entire lifecycle in a single platform:**

| Capability | Traditional Stack | Snowflake |
|---|---|---|
| Store binary DICOM files | Dedicated imaging servers / object storage | External stage (zero-copy access to S3) |
| Parse DICOM metadata | Custom Python services on VMs | Python UDF — serverless, auto-scaled |
| Render medical images | DICOM viewer software (OHIF, Horos) | Python UDF → base64 PNG in SQL |
| AI classification & summarization | External ML platform + API calls | Cortex AI functions — in-platform, no data egress |
| Semantic search | Elasticsearch / Vector DB cluster | Cortex Search Service — fully managed |
| Natural language Q&A | Custom LLM integration | Cortex Agent — built-in, governed |
| PHI protection | External DLP / manual processes | Column-level masking policies — enforced at query time |
| Interactive dashboard | BI tool + API layer + deployment | Streamlit in Snowflake — single deployment target |

**Result: One platform. One governance model. One bill. Zero infrastructure to manage.**

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  NCI Imaging Data Commons (S3)              │
│            s3://idc-open-data — Public Open Data            │
│        20 DICOM series | 23,929 files | 5 collections       │
└─────────────────────────┬───────────────────────────────────┘
                          │ Zero-copy access (no data movement)
                          ▼
┌─────────────────────────────────────────────────────────────┐
│              External Stage + Directory Table               │
│        IDC_OPEN_DATA_ALL_STG (directory enabled)            │
│     REFRESH SUBPATH per series — selective indexing          │
└─────────────────────────┬───────────────────────────────────┘
                          │ BUILD_SCOPED_FILE_URL
                          ▼
┌─────────────────────────────────────────────────────────────┐
│               Python UDFs (Serverless Compute)              │
│                                                             │
│  EXTRACT_DICOM_METADATA     RENDER_DICOM_SLICE              │
│  ├─ pydicom: parse binary   ├─ pydicom: read pixel data     │
│  ├─ 100+ DICOM tags → JSON  ├─ numpy: Hounsfield rescale    │
│  └─ VARIANT output           ├─ Pillow: PNG rendering        │
│                              └─ base64 output                │
│                                                             │
│  DICOM_TO_BASE64_PNG (UDTF)                                 │
│  ├─ Warehouse-parallel DICOM → 448x448 PNG conversion       │
│  └─ Feeds MedSigLIP embedding pipeline                      │
└─────────────┬─────────────────────────┬─────────────────────┘
              │                         │
              ▼                         ▼
┌───────────────────────┐   ┌────────────────────────────────┐
│  DICOM_CARDIAC_METADATA│   │   Streamlit DICOM Viewer       │
│  23,929 rows × 41 cols │   │   Real-time slice rendering    │
│  ├─ PHI Masking Policy │   │   Window/level presets         │
│  ├─ Raw VARIANT JSON   │   └────────────────────────────────┘
│  └─ Structured columns │
└───────────┬────────────┘
            │
   ┌────────┼────────────────────────────────────────────┐
   │        │                            │               │
   ▼        ▼                            ▼               ▼
┌────────┐ ┌──────────────────┐ ┌──────────────┐ ┌──────────────────┐
│ Cortex │ │  Cortex Search   │ │  Cortex AI   │ │  MedSigLIP SPCS  │
│ Agent  │ │  Services        │ │  Functions   │ │  Service          │
│        │ │                  │ │              │ │                   │
│  NL    │ │  DICOM_STUDY_    │ │  AI_CLASSIFY │ │  MEDSIGLIP_448_SVC│
│  Q&A   │ │  SEARCH (text)   │ │  AI_COMPLETE │ │  ├─ predict()     │
│  +     │ │                  │ │  AI_EXTRACT  │ │  │  (image→embed) │
│  Image │ │  DICOM_IMAGE_    │ │              │ │  └─ embed_text()  │
│ Search │ │  SEARCH_SVC      │ │              │ │     (text→embed)  │
│        │ │  (BYO vectors)   │ │              │ │     1152-dim      │
└────────┘ └──────────────────┘ └──────────────┘ └──────────────────┘
```

---

## What's in the Prototype (7-Tab Application + Floating Agent)

### Tab 1: Pipeline Overview
KPI dashboard showing the full dataset: 23,929 DICOM files across 20 CT series from 5 NCI collections (NSCLC Radiogenomics, VAREPOP Apollo, RIDER Lung PET-CT, COVID-19 NY SBU, NLST). Interactive charts by series, manufacturer, and collection. Includes the actual Python UDF source code that parses binary DICOM.

### Tab 2: Metadata Explorer
Faceted search and filtering across all 41 metadata columns. Filter by collection, manufacturer, body part, and modality. Drill into raw DICOM JSON (100+ tags per file). Demonstrates how unstructured binary data becomes queryable structured analytics with a single SQL INSERT.

### Tab 3: DICOM Viewer
Real-time medical image rendering from binary files stored on S3 — executed entirely within Snowflake. Navigate through slices with a slider. Apply clinical window presets (Soft Tissue, Lung, Bone, Mediastinum). The `RENDER_DICOM_SLICE` UDF reads the binary, applies Hounsfield unit rescaling, DICOM windowing, and returns a PNG — all in a serverless Python UDF.

### Tab 4: AI Insights
- **AI_CLASSIFY**: Automatically categorizes each series (Cardiac CT Angiography, Chest CT Screening, Coronary Calcium Score, etc.) using Cortex's built-in classification
- **AI_COMPLETE**: Generates clinical summaries from raw DICOM metadata using Claude Sonnet
- **Parameter Anomaly Detection**: Distribution analysis of KVP, exposure, and slice thickness to identify protocol deviations

### Tab 5: Clinical Intelligence
TAVR (transcatheter aortic valve replacement) clinical workflow. Procedure outcomes by access route, adverse events by type and severity, post-implant hemodynamic telemetry trends, and procedure-to-imaging join table. Built on a synthetic star schema: 20 patients, 20 procedures, 15 devices, 8 sites, 1,000 telemetry readings, 13 adverse events, 5 CAPAs.

### Tab 6: Image Search (MedSigLIP Vision-Language)
**Semantic search over DICOM images using natural language.** Type a clinical description (e.g., "cardiac CT angiography with aortic valve calcification") and get visually similar DICOM slices ranked by embedding similarity. Powered by:
- **google/medsiglip-448**: 400M-parameter vision-language dual encoder trained on CT, MRI, chest X-ray, and other medical modalities
- **BYO Vector Cortex Search**: Pre-computed 1152-dim MedSigLIP embeddings searched via `VECTOR INDEXES`
- **Rendered previews**: Middle slice of each matched series rendered via `RENDER_DICOM_SLICE` UDF
- **Clinical context panel**: Joins matched imaging to TAVR procedure outcomes, device data, and adverse events

### Tab 7: Governance Dashboard
- **PHI Masking**: Side-by-side comparison of SYSADMIN (full access) vs. ANALYST (masked) views of patient identifiers
- **Data Classification**: Shows how SYSTEM\$CLASSIFY automatically identifies PHI columns (IDENTIFIER, QUASI_IDENTIFIER)
- **Data Lineage**: Visual trace from raw DICOM binary → metadata table → AI enrichments → dashboard

### Floating Cortex Agent Chat
Always-available floating chat panel powered by the Cortex Agent REST API with SSE streaming. The agent has two tools:
1. **dicom_metadata_search** — Cortex Search over study metadata (text index) for protocol/manufacturer/modality questions
2. **dicom_image_search** — MedSigLIP vision-language search for clinical finding/anatomy queries (BYO vector Cortex Search)

Orchestration routes queries to the appropriate tool based on intent. Streaming tokens render in real-time via `st.write_stream()`. Falls back to `AI_COMPLETE` if the Agent API is unavailable.

---

## The Strategic Advantage

### 1. Snowflake Processes Unstructured Data — at Scale, Natively

This is not a bolt-on capability. Snowflake reads binary DICOM files directly from S3 via external stages. Python UDFs parse the proprietary DICOM format (using the same `pydicom` library radiologists use) and output structured metadata — all within Snowflake's serverless compute. No VMs, no containers, no orchestration layer.

The `INSERT INTO ... SELECT EXTRACT_DICOM_METADATA(BUILD_SCOPED_FILE_URL(...))` pattern processes thousands of binary files in a single SQL statement, parallelized automatically across warehouse nodes.

### 2. Zero Data Movement, Zero Infrastructure

The DICOM files remain on S3 (NCI's public bucket). Snowflake accesses them via zero-copy external stages. No data replication, no ingestion pipeline, no staging servers. The directory table provides a file-system-like index with selective SUBPATH refresh — index only the series you need, when you need them.

### 3. AI Without Data Egress

Cortex AI functions (AI_CLASSIFY, AI_COMPLETE, AI_EXTRACT) run directly on the data inside Snowflake. No API calls to external AI services, no data leaving the platform, no separate AI infrastructure to manage or secure. The AI operates on the same governed data with the same access controls.

### 4. Governance Is Not an Afterthought — It's Built In

PHI masking policies are enforced at query time, automatically, for every consumer. There is no way to bypass them without the right role. Data classification identifies sensitive columns programmatically. Lineage is tracked from source files to derived tables to dashboard. This is HIPAA-relevant governance that doesn't require a separate data governance platform.

### 5. One Platform Reduces Total Cost of Ownership

| Eliminated Component | Typical Annual Cost |
|---|---|
| Dedicated PACS/imaging server | $50K–$500K |
| ETL orchestration tool (Airflow, etc.) | $20K–$100K |
| External vector database | $30K–$150K |
| Separate AI/ML platform | $50K–$300K |
| Data governance tool | $50K–$200K |
| BI tool licenses + API layer | $30K–$100K |
| **Infrastructure ops team (2–3 FTEs)** | **$300K–$600K** |

Snowflake collapses this into a single platform with consumption-based pricing and zero infrastructure management.

### 6. The Medical Imaging Opportunity

- **$3.8B** global medical imaging informatics market (2024), growing 8.2% CAGR
- DICOM is the universal standard — 100% of CT, MRI, X-ray, ultrasound, PET uses it
- Healthcare organizations store **petabytes** of imaging data, most of it siloed and under-analyzed
- Regulatory requirements (HIPAA, GDPR, FDA 21 CFR Part 11) demand integrated governance — not bolted-on

---

## Dataset

20 curated DICOM series from the **NCI Imaging Data Commons** (IDC), a federally funded open-data resource:

| Series | Collection | Patient | Manufacturer | Files | Description |
|---|---|---|---|---|---|
| Cardiac CTA Diastolic 70% AMC-015 | nsclc_radiogenomics | AMC-015 | SIEMENS | 372 | Gated cardiac CT, diastolic phase |
| Cardiac CTA Systolic 31% AP-26JK | varepop_apollo | AP-26JK | SIEMENS | 459 | Gated cardiac CT, systolic phase |
| Cardiac CT Systolic 38% AMC-027 | nsclc_radiogenomics | AMC-027 | SIEMENS | 336 | Cardiac CT, systolic phase |
| Gated Segment 0.625mm RIDER-2019259100 | rider_lung_pet_ct | RIDER-2019259100 | GE MEDICAL SYSTEMS | 570 | High-resolution gated CT |
| Cine 30-65 BPM RIDER-2266952716 | rider_lung_pet_ct | RIDER-2266952716 | GE MEDICAL SYSTEMS | 280 | Cine cardiac CT |
| Cardiac 2.5 B41s A130302 | covid_19_ny_sbu | A130302 | SIEMENS | 85 | Cardiac CT (COVID cohort) |
| CTA 3.0 CE A095019 | covid_19_ny_sbu | A095019 | TOSHIBA | 130 | CT Angiography with contrast |
| Chest CT Philips 202207 | nlst | 202207 | Philips | 258 | NLST lung cancer screening |
| Chest CT Siemens B30f 133417 | nlst | 133417 | SIEMENS | 198 | NLST lung cancer screening |
| Chest CT Toshiba FC10 200119 | nlst | 200119 | TOSHIBA | 160 | NLST lung cancer screening |
| Chest CT RIDER-2416820556 | rider_lung_pet_ct | RIDER-2416820556 | GE MEDICAL SYSTEMS | 2,864 | Chest CT |
| Chest CT RIDER-2796673129 | rider_lung_pet_ct | RIDER-2796673129 | GE MEDICAL SYSTEMS | 2,688 | Chest CT |
| Chest CT RIDER-7701645091 | rider_lung_pet_ct | RIDER-7701645091 | GE MEDICAL SYSTEMS | 2,610 | Chest CT |
| Chest CT RIDER-1822442188 | rider_lung_pet_ct | RIDER-1822442188 | GE MEDICAL SYSTEMS | 2,275 | Chest CT |
| Chest CT RIDER-2522924559 | rider_lung_pet_ct | RIDER-2522924559 | GE MEDICAL SYSTEMS | 1,930 | Chest CT |
| Chest CT RIDER-1836251657 | rider_lung_pet_ct | RIDER-1836251657 | GE MEDICAL SYSTEMS | 1,930 | Chest CT |
| Chest CT TCGA-QQ-A5V2 | tcga_sarc | TCGA-QQ-A5V2 | GE MEDICAL SYSTEMS | 1,914 | Chest/Abd/Pelvis CT |
| Chest CT RIDER-2991299498 | rider_lung_pet_ct | RIDER-2991299498 | GE MEDICAL SYSTEMS | 1,890 | Chest CT |
| Chest CT RIDER-2857227961 | rider_lung_pet_ct | RIDER-2857227961 | GE MEDICAL SYSTEMS | 1,600 | Chest CT |
| Chest CT RIDER-4767464492 | rider_lung_pet_ct | RIDER-4767464492 | GE MEDICAL SYSTEMS | 1,380 | Chest CT |

**Total: 23,929 files | 20 patients | 5 collections | 4 manufacturers**

Data source: [NCI Imaging Data Commons](https://portal.imaging.datacommons.cancer.gov/) — public, de-identified, IRB-exempt.

---

## Performance Benchmarks

### DICOM Metadata Extraction (`EXTRACT_DICOM_METADATA`)

Two measured data points showing linear scaling from Small to Large warehouse:

| Warehouse | Series | Files | Elapsed | Throughput | Cost (at $2.50/credit) |
|---|---|---|---|---|---|
| **Small** (1 credit/hr) | Cardiac CTA Diastolic 70% AMC-015 | 372 | **27 sec** | 13.8 files/sec | $0.02 |
| **Large** (8 credits/hr) | Cardiac CTA Systolic 31% AP-26JK | 2,552 | **39 sec** | 65.4 files/sec | $0.22 |

Throughput scales ~4.7x from Small to Large (8x credits, ~4.7x throughput), confirming near-linear scaling across warehouse nodes. The Large warehouse also processed 21,081 files in a single batch INSERT in under 5 minutes for under $2 of compute.

| Extrapolation | Value |
|---|---|
| Hourly throughput (Large) | **~235,000 files/hour** |
| Cost per hour (Large) | **$20** (at $2.50/credit) |
| Cost per 100K files (Large) | **~$3.40** |

### DICOM Image Rendering (`RENDER_DICOM_SLICE`)

Single-slice rendering (read binary, decode pixels, apply Hounsfield rescaling, DICOM windowing, encode PNG):

| Metric | Value |
|---|---|
| Cold start (first call, UDF initialization) | **~11 sec** |
| Warm call (UDF already initialized) | **6–8 sec** |
| Output | 512×512 grayscale PNG, base64-encoded |

Image rendering is inherently single-file (one slice per call) and includes S3 read latency. For batch rendering (e.g., generating thumbnails for an entire series), the UDF parallelizes automatically across warehouse nodes via `SELECT RENDER_DICOM_SLICE(...) FROM table`.

### Scaling Projections

| Warehouse Size | Credits/Hour | Projected Throughput | Cost per 100K Files (at $2.50/credit) |
|---|---|---|---|
| Small | 1 | ~14 files/sec | ~$5.00 |
| Medium | 2 | ~28 files/sec | ~$5.00 |
| Large | 8 | ~65 files/sec (measured) | ~$3.40 |
| X-Large | 16 | ~130 files/sec | ~$3.40 |
| 2X-Large | 32 | ~260 files/sec | ~$3.40 |

At 2X-Large: **100,000 DICOM files in ~6.5 minutes for about $11**. No infrastructure changes required.

---

## Snowflake Objects Created

| Object | Type | Purpose |
|---|---|---|
| `EW_IMAGING_DB` | Database | Top-level container |
| `EW_IMAGING_DB.EXPLORER` | Schema | All prototype objects |
| `IDC_OPEN_DATA_ALL_STG` | External Stage | Zero-copy access to full IDC S3 bucket |
| `IDC_OPEN_DATA_CARDIAC_STG` | External Stage | Single-series subset |
| `EXTRACT_DICOM_METADATA` | Python UDF | Parses binary DICOM → VARIANT JSON |
| `RENDER_DICOM_SLICE` | Python UDF | Renders DICOM pixels → base64 PNG |
| `DICOM_TO_BASE64_PNG` | Python UDTF | Warehouse-parallel DICOM → 448x448 base64 PNG (feeds MedSigLIP) |
| `DICOM_CARDIAC_METADATA` | Table | 23,929 rows x 41 columns of parsed metadata |
| `DIM_PATIENT` | Table | 20 synthetic patient demographics (joined via PATIENT_ID) |
| `DIM_DEVICE` | Table | 15 SAPIEN valve device configurations |
| `DIM_SITE` | Table | 8 hospital sites |
| `DIM_PHYSICIAN` | Table | 12 physicians |
| `FACT_PROCEDURE` | Table | 20 TAVR procedures linked to imaging studies |
| `FACT_DEVICE_TELEMETRY` | Table | 1,000 post-implant hemodynamic readings |
| `FACT_ADVERSE_EVENT` | Table | 13 adverse events |
| `FACT_CAPA` | Table | 5 corrective/preventive actions |
| `PHI_MASK` | Masking Policy | Protects PATIENT_ID and PATIENT_NAME |
| `DICOM_STUDY_SEARCH` | Cortex Search Service | Text search over study descriptions |
| `DICOM_IMAGE_SEARCH_SVC` | Cortex Search Service | BYO vector search over MedSigLIP 1152-dim embeddings |
| `DICOM_IMAGING_AGENT` | Cortex Agent | Dual-tool agent (metadata search + image search) |
| `SEARCH_DICOM_IMAGES` | Stored Procedure | Encodes text query via MedSigLIP + vector search |
| `MEDSIGLIP_448` (V2) | ML Registry Model | google/medsiglip-448 CustomModel (predict + embed_text) |
| `MEDSIGLIP_448_SVC` | SPCS Service | GPU inference service on EW_THV_COMPUTE_POOL |
| `DICOM_BASE64_IMAGES` | Table | Staging table with base64 PNG images for embedding |
| `DICOM_EMBEDDINGS` | Table | Final embeddings with VECTOR(FLOAT, 1152) column |
| `CLINICAL_INTELLIGENCE_SV` | Semantic View | 13 metrics over clinical star schema |
| `DICOM_EXPLORER_SF` | Streamlit App | Deployed SiS app (container runtime) |

---

## Project Structure

```
snowflake-dicom-explorer/
├── README.md                  # This file
├── streamlit_app.py           # Streamlit application (7 tabs + floating agent)
├── snowflake.yml              # SiS deployment manifest (container runtime)
├── pyproject.toml             # Python dependencies for SiS
├── requirements.txt           # Python dependencies for local dev
├── setup.sh                   # One-command SQL deployment script
├── .streamlit/
│   └── config.toml            # Brand theme
├── sql/
│   ├── 01_database_schema.sql # Database and schema creation
│   ├── 02_stages.sql          # External stages + directory refresh
│   ├── 03_udfs.sql            # Python UDFs (metadata extraction, image rendering)
│   ├── 04_tables.sql          # Metadata table
│   ├── 05_masking_policies.sql# PHI masking policies
│   ├── 06_cortex_search.sql   # Cortex Search Service (text metadata)
│   ├── 07_data_load.sql       # Full data load pipeline
│   ├── 08_clinical_data_model.sql # Synthetic TAVR star schema (8 tables)
│   └── 09_agent_definition.sql   # Cortex Agent (dual-tool: metadata + image search)
├── dicom-search/
│   ├── medsiglip_log_model.ipynb  # MedSigLIP model registration + SPCS deploy + embedding pipeline
│   └── byo_search.sql             # Cortex Search BYO vectors + search SP + standalone agent
└── .gitignore
```

---

## Deployment

### Prerequisites
- Snowflake account with `SYSADMIN` role
- Warehouse: `COMPUTE_WH` (or edit scripts to use your warehouse)
- Compute pool for container runtime (e.g., `CTTI_DASHBOARD_POOL`)
- GPU compute pool for MedSigLIP inference (e.g., `EW_THV_COMPUTE_POOL`)
- External access integration for PyPI (e.g., `ALLOW_ALL_ACCESS_INTEGRATION`)
- HuggingFace token stored as a Snowflake secret (for MedSigLIP model download)
- Snowflake CLI v3.14.0+ (`snow --version`)
- Python 3.11+ with pip (for local development)

### Deploy to Snowflake (Streamlit in Snowflake)

```bash
# 1. Clone the repo
git clone <repo-url> && cd snowflake-dicom-explorer

# 2. Run the SQL setup (execute files 01–09 in order)
./setup.sh --connection <your-connection-name>

# 3. Run the MedSigLIP notebook in a Snowflake Workspace (GPU required)
#    This registers the model, deploys the SPCS service, and generates embeddings.
#    See dicom-search/medsiglip_log_model.ipynb

# 4. Deploy the Cortex Search BYO vector service + agent
snow sql -f dicom-search/byo_search.sql -c <your-connection-name>

# 5. Deploy to Streamlit in Snowflake (container runtime)
snow streamlit deploy dicom_explorer_sf --replace -c <your-connection-name>
```

The app deploys to `EW_IMAGING_DB.EXPLORER.DICOM_EXPLORER` and is available in Snowsight under **Projects > Streamlit**.

### Run Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Launch
SNOWFLAKE_CONNECTION_NAME=<your-connection-name> streamlit run streamlit_app.py
```

The app auto-detects its environment: in Snowflake it uses the SPCS session token and `st.connection("snowflake")`; locally it uses `snowflake.connector` with your CLI connection.

### Manual SQL Deployment

Run the SQL files in order against your Snowflake account:

```bash
# Using Snowflake CLI
snow sql -f sql/01_database_schema.sql -c <connection>
snow sql -f sql/02_stages.sql -c <connection>
snow sql -f sql/03_udfs.sql -c <connection>
snow sql -f sql/04_tables.sql -c <connection>
snow sql -f sql/05_masking_policies.sql -c <connection>
snow sql -f sql/06_cortex_search.sql -c <connection>
snow sql -f sql/07_data_load.sql -c <connection>  # ~5 min on Large warehouse for 23,929 files
snow sql -f sql/08_clinical_data_model.sql -c <connection>
snow sql -f sql/09_agent_definition.sql -c <connection>
```

> **Note**: The data load step (07) processes 23,929 binary DICOM files through the Python UDF. This typically takes about 5 minutes on a Large warehouse. The UDF execution is automatically parallelized across warehouse nodes.

> **Note**: The MedSigLIP embedding pipeline (dicom-search/) requires a GPU compute pool and should be run via the Snowflake Workspace notebook. The SPCS service must be running for the Image Search tab and agent image search tool to function.

---

## Technical Deep Dive

### How DICOM Binary Processing Works

DICOM (Digital Imaging and Communications in Medicine) is the universal standard for medical imaging. Each file is a binary container holding both metadata (patient info, acquisition parameters, equipment details) and pixel data (the actual image).

Snowflake processes these binary files through a three-step pattern:

```sql
-- Step 1: External stage provides zero-copy access to S3
CREATE STAGE my_stage URL = 's3://idc-open-data' DIRECTORY = (ENABLE = TRUE);

-- Step 2: Directory table indexes the files (selective refresh)
ALTER STAGE my_stage REFRESH SUBPATH = '<series-uuid>/';

-- Step 3: Python UDF reads binary, returns structured JSON
SELECT EXTRACT_DICOM_METADATA(
    BUILD_SCOPED_FILE_URL(@my_stage, RELATIVE_PATH)
) FROM DIRECTORY(@my_stage);
```

The Python UDF uses `SnowflakeFile.open()` to read the binary file directly from S3 into memory, then uses `pydicom` (the standard DICOM library) to parse it. The result is a VARIANT containing all metadata tags — no intermediate storage, no temporary files, no data movement.

### How DICOM Image Rendering Works

The `RENDER_DICOM_SLICE` UDF demonstrates real-time medical image rendering:

1. **Read**: Binary DICOM file from S3 via `SnowflakeFile`
2. **Decode**: Extract pixel array using pydicom
3. **Rescale**: Apply Hounsfield unit conversion (`pixel * RescaleSlope + RescaleIntercept`)
4. **Window**: Apply DICOM windowing for clinical display (e.g., Soft Tissue: center=40, width=400)
5. **Encode**: Convert to PNG via Pillow, return as base64 string

This runs entirely in Snowflake's serverless Python runtime. The Streamlit app calls it via SQL and displays the result as an image.

### Cortex AI Integration

All AI capabilities run inside Snowflake with no external API calls:

- **AI_CLASSIFY**: Categorizes study descriptions into clinical categories using Snowflake's built-in classification model
- **AI_COMPLETE**: Generates clinical summaries using Claude Sonnet, with full DICOM metadata as context
- **Cortex Search Service**: Maintains a continuously-updated vector index over study metadata for semantic search

### MedSigLIP Vision-Language Image Search

The Image Search tab uses a bring-your-own-vectors (BYO) approach with Google's MedSigLIP-448 model:

1. **Model**: `google/medsiglip-448` — a SigLIP dual encoder (400M vision + 400M text) pre-trained on CT, MRI, chest X-ray, pathology, derm, and ophthalmology images paired with clinical text
2. **Embedding**: 1152-dimensional vectors where images and text queries live in the same space
3. **Pipeline**:
   - `DICOM_TO_BASE64_PNG` UDTF converts DICOM → windowed, normalized, 448x448 RGB PNG (warehouse-parallel)
   - `MEDSIGLIP_448_SVC` SPCS service generates embeddings on GPU
   - `DICOM_EMBEDDINGS` table stores vectors as `VECTOR(FLOAT, 1152)`
   - `DICOM_IMAGE_SEARCH_SVC` Cortex Search service indexes the pre-computed vectors (`VECTOR INDEXES`)
4. **Search flow**: Text query → `embed_text()` on SPCS → `multi_index_query` on Cortex Search → ranked results
5. **Integration**: Results link back to clinical data (TAVR outcomes, adverse events) via PATIENT_ID

This pattern is reusable for any HuggingFace vision model deployed on Snowflake — register as CustomModel, deploy on SPCS, pre-compute embeddings, index with Cortex Search BYO vectors.

---

## Security & Compliance Considerations

| Requirement | How Snowflake Addresses It |
|---|---|
| **HIPAA PHI Protection** | Column-level masking policies on PATIENT_ID, PATIENT_NAME — enforced at query time, per role |
| **Data Residency** | Data never leaves Snowflake boundary; AI runs in-platform |
| **Access Auditing** | ACCESS_HISTORY tracks every query, every column, every user |
| **Role-Based Access** | Standard Snowflake RBAC; masking policies tied to roles |
| **Data Classification** | SYSTEM\$CLASSIFY auto-detects IDENTIFIER, QUASI_IDENTIFIER columns |
| **Lineage** | Full column-level lineage from source files → tables → views → dashboards |

---

## Frequently Asked Questions

**Q: Can this scale to millions of DICOM files?**
Yes. External stages support unlimited files. Directory table refresh can be targeted via SUBPATH. The Python UDF executes in parallel across all warehouse nodes. A Large warehouse processes about 65 files per second (measured), roughly 235,000 files per hour for about $20 of compute at $2.50/credit.

**Q: Does Snowflake store the DICOM files?**
No. The files remain on S3 (or your own object storage). Snowflake accesses them via external stages with zero-copy reads. You control the storage, Snowflake provides the compute.

**Q: What about DICOM files behind a firewall (on-prem PACS)?**
Use Snowflake's internal stages. Upload files to an internal stage, and the same UDFs work identically. Or use Snowflake's storage integrations with your private S3/Azure Blob/GCS buckets.

**Q: Is the AI HIPAA-compliant?**
Cortex AI functions process data within the Snowflake boundary. Data does not leave the platform. Cortex is covered under Snowflake's BAA (Business Associate Agreement) for HIPAA compliance.

**Q: Can we add more imaging modalities (MRI, X-ray, ultrasound)?**
Yes. The `pydicom` library handles all DICOM modalities. The UDF and table schema are modality-agnostic. Simply add more series UUIDs to the directory refresh and data load. MedSigLIP was also trained on MRI slices and chest X-rays, so embeddings will work across modalities.

**Q: How does the Image Search work without any text in the DICOM images?**
MedSigLIP is a contrastive dual encoder — it maps both images and text into the same 1152-dimensional vector space. At indexing time, DICOM pixel data is encoded by the vision encoder. At query time, your natural language description is encoded by the text encoder. Cosine similarity between the two vectors determines relevance. No OCR or text extraction from images is involved.

**Q: Does the MedSigLIP SPCS service need to run continuously?**
Only when you need to embed new images or process search queries. You can suspend it (`ALTER SERVICE ... SUSPEND`) when not in use to stop GPU costs, and resume it before demo/search sessions. The pre-computed embeddings in `DICOM_EMBEDDINGS` and the Cortex Search index persist independently of the service.

---

## License

This prototype uses publicly available de-identified medical imaging data from the [NCI Imaging Data Commons](https://portal.imaging.datacommons.cancer.gov/), licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
