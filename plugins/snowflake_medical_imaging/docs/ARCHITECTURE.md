# Architecture

End-to-end architecture of the Snowflake for Medical Imaging platform, as deployed and
verified in the development account on 2026-08-10. Every object named here was
read back out of `INFORMATION_SCHEMA` and `SHOW`, not transcribed from the
templates.

Data model is in [DATA_MODEL.md](DATA_MODEL.md).

Deployed surface, counted from `INFORMATION_SCHEMA`: **26 tables** (22 by design
in `IMAGING`, 1 in `BENCH`, plus 3 scratch tables that are residue -- see
DATA_MODEL section 6), **28 views** (20 + 8), **15 procedures** (13 + 2),
**5 functions**, 5 stages, 3 tasks, 1 SPCS service with 3 containers, 2 compute
pools, 10 policy attachments.

---

## 1. System context

What the thing is, and where the boundary sits. Nothing crosses the account
boundary except the browser session.

```mermaid
flowchart LR
    OS["Object storage<br/>S3 / Azure / GCS<br/>DICOM files"]
    BR["Radiologist browser<br/>OHIF SPA"]
    HF["Model weights<br/>build time only"]

    subgraph SF["Snowflake account boundary - nothing egresses at runtime"]
        direction TB
        SPCS["SERVING PLANE<br/>VOXEL_DICOMWEB_SVC<br/>router + gateway + prefetch<br/>no warehouse in the frame path"]
        WH["CATALOG PLANE<br/>VOXEL_WH MEDIUM Gen2<br/>header-only extract UDTF"]
        TBL[("VOXEL_DB.IMAGING<br/>catalog, manifest, frame plane<br/>29,837 instances")]
        V3D["INFERENCE PLANE<br/>Vista3D on GPU pool<br/>suspended when idle"]
        POL["GOVERNANCE PLANE<br/>masking x3, projection x1<br/>row access x1"]
    end

    OS -->|"headers only<br/>0.716% of bytes"| WH
    OS -->|"COPY FILES<br/>pixels"| SPCS
    BR <-->|"HTTPS<br/>auth at ingress"| SPCS
    HF -.->|"no runtime egress"| V3D

    WH --> TBL
    SPCS -->|"QIDO + metadata<br/>caller's rights"| TBL
    TBL -->|"segmentable volumes"| V3D
    V3D -->|"organ volumes<br/>+ DICOM-SEG"| TBL
    POL -.->|"enforced by engine"| TBL

    classDef out fill:#f5f5f5,stroke:#999,color:#333
    classDef sf fill:#e8f4fd,stroke:#1a73e8,color:#0b3d6b
    classDef db fill:#fff,stroke:#1a73e8,color:#0b3d6b
    class OS,BR,HF out
    class WH,SPCS,V3D,POL sf
    class TBL db
```

**The load-bearing property:** the serving plane never touches a warehouse to
return pixels. Frames are a positioned read against a mounted stage volume. The
warehouse is only in the path for catalog queries.

---

## 2. Ingest: metadata path

One pass per file, header only. This is where the 0.716% comes from.

```mermaid
flowchart TB
    A["External stage<br/>IDC_OPEN_DATA_STG"] --> B

    subgraph SCAN["TASK_VOXEL_SCAN - every 5 min, XSMALL"]
        B["SP_SCAN_ALL_SOURCES<br/>Python: LIST then RESULT_SCAN"]
        B --> C[("DICOM_SCAN_SCRATCH<br/>transient")]
        C --> D{"compare to manifest<br/>by name + md5"}
        D -->|new| E1["STATUS = NEW"]
        D -->|"md5 changed"| E2["STATUS = NEW<br/>supersedes prior"]
        D -->|"absent from stage"| E3["STATUS = DELETED"]
    end

    E1 --> M[("DICOM_FILE_MANIFEST<br/>29,837 rows")]
    E2 --> M
    E3 --> M

    subgraph EXTRACT["TASK_VOXEL_EXTRACT - MEDIUM"]
        M --> F{"FILE_SIZE > max_object_bytes"}
        F -->|yes| Q["quarantine unopened<br/>CAPABILITY-0002"]
        F -->|no| G["bucket into partitions<br/>ceil batch / 8"]
        G --> H["EXTRACT_DICOM_HEADER_AND_FRAMES<br/>vectorized UDTF, end_partition<br/>8 IO threads per partition"]
        H --> I[("DICOM_EXTRACT_BATCH<br/>temp, ROW_KIND = HEADER or FRAME")]
    end

    I --> J[("DICOM_INSTANCE<br/>29,837")]
    I --> K[("DICOM_FRAME_OFFSET<br/>43 - encapsulated only")]
    I --> L[("DICOM_LOAD_ERRORS<br/>typed, countable")]
    Q --> L

    J --> N["dedupe by SOPInstanceUID<br/>losers marked SUPERSEDED"]

    subgraph RECON["TASK_VOXEL_RECONCILE - XSMALL"]
        O["SP_RECONCILE_DELETES<br/>blast-radius guard 5%<br/>returns REFUSED, never raises"]
    end
    M --> O
    O --> J

    classDef stage fill:#fff4e5,stroke:#e8a33d,color:#663c00
    classDef tbl fill:#e8f4fd,stroke:#1a73e8,color:#0b3d6b
    classDef bad fill:#fdecea,stroke:#d93025,color:#7f1d17
    class A stage
    class C,M,I,J,K,L tbl
    class Q,L bad
```

**Why the UDTF is partitioned:** a vectorized `process` is 1-to-1 so it cannot
emit N frame rows per file. `end_partition` can, and it requires `PARTITION BY`
on a real column, which is why the bucket is materialized rather than computed
inline. Bucket count derives from the actual batch size, not the configured
limit.

**Failures are rows, not poisoned cells.** `DICOM_LOAD_ERRORS` joins to
`DICOM_ERROR_CATALOG` so every failure carries a class, a severity, a retryable
flag and a runbook step. `COUNT(*) GROUP BY ERR_CODE` is an answerable question,
which it is not when failures are written into a metadata column.

---

## 3. Ingest: pixel path

**Metadata and pixels come from different stages.** This is the single most
counter-intuitive thing in the build, and getting it wrong produces a catalog
that is 100% healthy and serves zero pixels.

```mermaid
flowchart LR
    A["External stage<br/>IDC_OPEN_DATA_STG<br/>s3://idc-open-data"]
    B["Internal stage<br/>VOXEL_PREAD_STG<br/>SNOWFLAKE_SSE"]
    C["Stage volume mount<br/>/mnt/dicom"]

    A -->|"read by UDTF for METADATA"| M[("DICOM_INSTANCE")]
    A -->|"SP_SYNC_PREAD_STAGE<br/>COPY FILES, path preserved"| B
    B -->|"ALTER STAGE REFRESH<br/>then restart service"| C
    C -->|"os.pread by offset"| G["gateway container"]

    M -.->|"V_PREAD_COVERAGE<br/>catalogued vs present"| CHK{"PREAD_STATE"}
    B -.-> CHK
    CHK -->|COMPLETE| OK["361 of 361 series"]
    CHK -->|NO_BYTES| ERR["viewer 404s every frame<br/>ingest notices nothing"]

    classDef stage fill:#fff4e5,stroke:#e8a33d,color:#663c00
    classDef bad fill:#fdecea,stroke:#d93025,color:#7f1d17
    class A,B,C stage
    class ERR bad
```

SPCS stage volumes mount **internal stages only**, so the external stage the
catalog is built from cannot be mounted. Both must be populated. `DIRECTORY()`
is stale after an out-of-band upload, hence the mandatory `REFRESH`.

---

## 4. Read path: how a frame is served

```mermaid
sequenceDiagram
    autonumber
    participant B as Browser / OHIF
    participant I as SPCS ingress
    participant R as router - nginx :8000
    participant G as gateway - FastAPI :8080
    participant S as Snowflake SQL
    participant V as stage volume /mnt/dicom
    participant H as hotring /dev/shm/voxel

    B->>I: GET /dicom-web/studies
    I->>I: authenticate, inject<br/>Sf-Context-Current-User-Token
    I->>R: forward with identity headers
    R->>G: proxy_pass, headers forwarded verbatim
    G->>S: QIDO query as THE CALLER
    Note over S: row access + masking policies<br/>evaluated for that human
    S-->>G: DICOM JSON, numeric VRs as numbers
    G-->>B: application/dicom+json

    B->>G: GET .../frames/1,2,3
    G->>S: series_frame_plan - caller's rights
    S-->>G: RESOLUTION per instance
    alt ARITHMETIC - native, 29,830 instances
        G->>G: offset = PIXEL_DATA_START + n * frame_bytes<br/>computed in BITS, refuses unaligned
    else STORED - encapsulated, 7 instances
        G->>S: read DICOM_FRAME_OFFSET
    end
    G->>H: try warm read
    alt hotring miss
        G->>V: os.pread, frame list coalesced to one span
    end
    G-->>B: multipart/related, X-Voxel-Served-From
```

**Offsets are never the authorization token.** They are only ever produced as a
side effect of a policy-enforced read, so a caller who cannot see a study cannot
obtain its byte layout. There is no presigned-URL path handing coordinates to the
client.

**A 404 is deliberately ambiguous** between "does not exist", "you cannot see it"
and "excluded as unservable".

---

## 5. Deployment topology

```mermaid
flowchart TB
    subgraph ACC["Account level - created by ACCOUNTADMIN, then ownership transferred"]
        direction LR
        WH["VOXEL_WH<br/>MEDIUM, AUTO_SUSPEND 60<br/>INITIALLY_SUSPENDED"]
        P1["VOXEL_WEB_POOL<br/>CPU_X64_M, 1-2 nodes<br/>INITIALLY_SUSPENDED"]
        P2["VOXEL_GPU_POOL<br/>GPU_NV_S, 1-2 nodes<br/>INITIALLY_SUSPENDED"]
        MISC["VOXEL_RADIOLOGIST role<br/>VOXEL_BUILD_EGRESS_RULE 10 hosts :443<br/>VOXEL_BUILD_EAI build time only"]
    end

    subgraph DB["VOXEL_DB - owned by SYSADMIN"]
        direction LR
        IMG["schema IMAGING<br/>22 tables, 20 views<br/>13 procedures, 5 functions"]
        BN["schema BENCH<br/>BENCH_RUN + 8 cost views<br/>2 procedures, survives teardown"]
        STG["stages<br/>IDC_OPEN_DATA_STG external<br/>VOXEL_PREAD_STG internal, mounted<br/>VOXEL_SEG_OUT, VOXEL_MODELS<br/>VOXEL_IMAGES image repo"]
    end

    subgraph SRV["VOXEL_DICOMWEB_SVC - one service, one origin"]
        direction LR
        C1["router<br/>nginx + OHIF dist<br/>:8000 PUBLIC"]
        C2["gateway<br/>FastAPI + pydicom<br/>:8080 internal"]
        C3["prefetch<br/>same image, sidecar"]
    end

    JOB["Vista3D batch job<br/>ephemeral, suspends the pool on exit<br/>including the failure path"]

    P1 --> SRV
    P2 --> JOB
    STG -->|"mount: internal only"| SRV
    STG --> JOB
    C1 -->|proxy_pass| C2
    C3 -.->|"warms hotring tmpfs"| C2
    IMG -->|"caller's rights queries"| C2
    MISC -.->|"GRANT USAGE ON SERVICE ROLE<br/>voxel_viewer"| SRV

    classDef susp fill:#eef7ee,stroke:#34a853,color:#1e4620
    classDef pub fill:#fdecea,stroke:#d93025,color:#7f1d17
    class WH,P1,P2,JOB susp
    class C1 pub
```

Cost-relevant facts baked into the topology:

- Both pools are created `INITIALLY_SUSPENDED`, which is **not** the default. A
  pool created the default way provisions `MIN_NODES` immediately and idle-bills.
  `00_ACCOUNT_SETUP` asserts `BILLING_CHECK = OK`.
- The EAI exists for image builds and is **not attached to the running service**,
  so `external_access_integrations = null` is a provable claim from
  `DESCRIBE SERVICE`, not an assertion.
- Exactly one endpoint is public. A second public endpoint would put the SPA and
  its data source on different origins, and the ingress baseline CSP
  `connect-src 'self'` hard-blocks that. CORS cannot loosen it.

---

## 6. Deploy order

```mermaid
flowchart LR
    A["account<br/>ACCOUNTADMIN"] --> B["core"]
    B --> C["pipeline"]
    C --> D["bench"]
    D --> E["validate<br/>CHECKS"]
    E --> F["dicomweb"]
    E --> G["seg"]
    E --> H["deid"]
    E --> I["conformance"]

    A -.- A1["db, schema, warehouse,<br/>2 pools, image repo,<br/>network rule, EAI, role"]
    B -.- B1["01 schema, 02 stages,<br/>03 catalog, 04 reference,<br/>05 frame plane, 06 UDFs,<br/>07 masking + DEID_SALT"]
    C -.- C1["10 config, 11 procedures,<br/>12 tasks suspended,<br/>13 backfill"]
    D -.- D1["20 cost ledger,<br/>21 bench profiles"]
    F -.- F1["build + push 2 images,<br/>create service,<br/>5 verifications, grants"]
    G -.- G1["30 seg queue + results,<br/>register model, infer, land"]
    H -.- H1["40 tag plane,<br/>41 pixel plane"]
    I -.- I1["50 conformance,<br/>51 migration compat"]

    classDef ph fill:#e8f4fd,stroke:#1a73e8,color:#0b3d6b
    class A,B,C,D,E,F,G,H,I ph
```

Phases after `validate` are independent of each other. `deid` carries an abort
gate: if any DICOM-SEG instance is catalogued while the four SEG-defining
sequence tags are unclassified, it **refuses to attach** the VARIANT masking
policy rather than blind the viewer.

Rendering is a pure text transform. `assets/renderer/render.py` reads
`build_manifest.yaml` plus `config.json` and writes one SQL file per object, with
`StrictUndefined` so a missing config key fails at render time instead of
emitting `None` into a `CREATE` statement.

---

## 7. Governance enforcement

```mermaid
flowchart TB
    Q["SELECT from DICOM_INSTANCE"] --> RA{"STUDY_SCOPE_POLICY<br/>row access on SOURCE_NAME"}
    RA -->|"no matching scope"| Z["zero rows"]
    RA -->|entitled| MM

    subgraph MM["Column policies"]
        direction LR
        M1["PHI_TEXT_MASK<br/>name, physician, institution"] --> R1["***MASKED***"]
        M2["PHI_ID_MASK<br/>patient id, accession, device sn"] --> R2["salted SHA2-256<br/>via PSEUDO_ID"]
        M3["PHI_DATE_MASK<br/>birth date"] --> R3["year only"]
        M4["PROTECT_FILE_PATH<br/>projection, ENFORCEMENT FAIL"] --> R4["query FAILS if projected"]
    end

    R2 --> SALT[("DEID_SALT<br/>granted to NOBODY")]

    US[("USER_STUDY_SCOPE")] --> RA
    SM[("SOURCE_SCOPE_MAP")] --> RA

    classDef sec fill:#eef7ee,stroke:#34a853,color:#1e4620
    classDef bad fill:#fdecea,stroke:#d93025,color:#7f1d17
    class SALT,R1,R2,R3 sec
    class Z,R4 bad
```

**The salt is not in the policy body and not in the repository.** It is generated
at deploy time from `UUID_STRING()` into `DEID_SALT`, which is granted to no
role, and read through `PSEUDO_ID`. Verified: a role with `SELECT` on the table
but no grant on `DEID_SALT` and no `USAGE` on `PSEUDO_ID` still reads the masked
column correctly, because **masking policy bodies resolve their references with
the policy owner's rights**. The same role cannot read the salt and cannot call
the function as a brute-force oracle.

Test negative cases with `USE SECONDARY ROLES NONE`. An interactive session has
secondary roles active and will authorize through an inherited role, handing you
a false negative.

Known limits, stated rather than implied: `PROTECT_FILE_PATH` protects against
analysts and BI building a bulk export path, **not** against the viewer role,
which must be allowlisted because the gateway resolves frames by selecting
`FILE_NAME` under caller's rights. `MASK_RAW_METADATA` is currently detached, so
`PHI_RAW_METADATA_UNMASKED` reports WARN.

---

## 8. Segmentation loop

```mermaid
flowchart TB
    A[("DICOM_INSTANCE")] --> B["V_SERIES_GEOMETRY<br/>compare N_INSTANCES to N_POSITIONS"]
    B --> C{"IS_SEGMENTABLE_VOLUME"}
    C -->|"MULTIPHASE_4D - 80 series"| D["SKIPPED with phase count<br/>34 refused at enqueue"]
    C -->|"mixed geometry"| E["SKIPPED - 4 refused"]
    C -->|VOLUME_3D| F[("SEG_QUEUE<br/>PENDING")]

    F --> G["SP_SEG_CLAIM<br/>atomic, ordered by priority"]
    G --> H["GPU job on VOXEL_GPU_POOL<br/>assemble volume, run Vista3D"]
    H --> I["VOXEL_SEG_OUT stage<br/>mask.nii.gz + segmentation.dcm + parquet"]

    I --> J["SP_LAND_SEG_RESULTS"]
    J --> K[("SEG_RESULT<br/>organ volumes")]
    J --> L[("SEG_RUN<br/>timings")]

    I --> M["COPY FILES to pread stage<br/>then rescan"]
    M --> A

    K --> N{"V_SEG_OVERLAY_COVERAGE"}
    A --> N
    N -->|RENDERABLE| O["numbers AND overlay"]
    N -->|RESULTS_ONLY| P["correct volumes,<br/>NO overlay - silent"]

    H -->|finally| S["suspend GPU pool<br/>fires on the failure path too"]

    classDef bad fill:#fdecea,stroke:#d93025,color:#7f1d17
    classDef ok fill:#eef7ee,stroke:#34a853,color:#1e4620
    class D,E,P bad
    class O,S ok
```

**Two independent write paths.** `SEG_RESULT` is written from the batch job's
parquet; the viewer renders from `DICOM_INSTANCE`. A series can report correct
organ volumes through every SQL surface and show no overlay at all.
`V_SEG_OVERLAY_COVERAGE` is the join that detects it.

`SEG_RESULT` idempotency is keyed on `(series, MODEL_VERSION)`, so re-running
under a new version leaves both generations. Any per-series aggregate must use
`COUNT(DISTINCT STRUCTURE_ID)` or it reports twice the organ count, which reads
as an anatomical claim.

---

## 9. Orchestration and observability

```mermaid
flowchart LR
    subgraph DAG["Serverless task DAG - created SUSPENDED"]
        T1["TASK_VOXEL_SCAN<br/>5 MINUTE, XSMALL<br/>SUSPEND_AFTER_NUM_FAILURES 3"]
        T2["TASK_VOXEL_EXTRACT<br/>MEDIUM"]
        T3["TASK_VOXEL_RECONCILE<br/>XSMALL"]
        T1 --> T2 --> T3
    end

    subgraph SLO["SLO-shaped views - NO alerting wired"]
        S1["V_PIPELINE_STATUS"]
        S2["V_TASK_HEALTH"]
        S3["V_ERROR_SUMMARY"]
        S4["V_PREAD_COVERAGE"]
        S5["V_SEG_QUEUE_HEALTH"]
        S6["V_COST_FRESHNESS"]
    end

    subgraph INV["CHECKS - 17 invariants, read-only"]
        I1["stale offsets - wrong pixels at HTTP 200"]
        I2["limbo files - neither done nor failed"]
        I3["frame arithmetic past EOF"]
        I4["UID uniqueness"]
        I5["PHI columns masked"]
        I6["manifest reconciles to catalog"]
    end

    DAG --> SLO
    DAG --> INV

    classDef gap fill:#fff4e5,stroke:#e8a33d,color:#663c00
    class SLO gap
```

`SUSPEND_TASK_AFTER_NUM_FAILURES` is root-only. `SP_RECONCILE_DELETES` returns a
`REFUSED:` string rather than raising, specifically so a refusal cannot count
toward that failure budget.

Six views are shaped like SLOs and **nothing alerts on them.** That is a real
gap, not a design choice.

---

## 10. What this deliberately does not do

| Not done | Why |
|---|---|
| STOW-RS writes | Returns 501. A write endpoint that accepts and discards is worse than one that says no |
| Server-side transcode | Safari lacks `credentialless` COEP so it gets single-threaded decode. A transcode path means decoding PHI in the serving container |
| Presigned browser-direct frame fetch | Hands the client coordinates to bypass authorization and holes the ingress access history |
| Hybrid Tables for the offset hot path | ~16k ops/sec per database shared with everything else, no incremental primitive maintains it |
| Iframe embed in Epic or Cerner | The proxy force-sets `X-Frame-Options: DENY`. Use SMART-on-FHIR launch-out |
| Runtime egress | No EAI attached to the serving service, so it is provable rather than promised |
| Alerting | Six SLO views exist, nothing pages |
| Enforcement of `V_PIXEL_SERVABILITY` | Advisory only. An advisory view mistaken for a control is its own risk |
