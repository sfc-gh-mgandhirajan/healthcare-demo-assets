# Data model

The 26 tables in the deployed schema -- 22 by design in `IMAGING`, 1 in `BENCH`,
and 3 scratch tables that are residue (section 6) -- read back out of
`VOXEL_DB.INFORMATION_SCHEMA` rather than transcribed from the templates.
Row counts are live as of 2026-08-10 Pacific.

Architecture is in [ARCHITECTURE.md](ARCHITECTURE.md).

**One thing to know before reading the diagrams.** Only **one** foreign key is
actually declared in the schema: `DICOM_SOURCE_PREFIX.SOURCE_NAME` references
`DICOM_SOURCE_CONFIG`. Every other relationship shown is a **logical join
enforced by procedures and asserted by CHECKS**, not by a constraint. Snowflake
does not enforce foreign keys, so declaring more would create the appearance of
integrity that nothing upholds. Relationships below are marked accordingly.

---

## 1. Ingest core: manifest, catalog, frame plane

The spine. A file becomes a manifest row, then exactly one catalog row, then zero
or more frame-offset rows.

```mermaid
erDiagram
    DICOM_SOURCE_CONFIG ||--o{ DICOM_SOURCE_PREFIX : "declares FK"
    DICOM_SOURCE_CONFIG ||--o{ DICOM_FILE_MANIFEST : "owns"
    DICOM_FILE_MANIFEST ||--o| DICOM_INSTANCE : "parses to exactly one"
    DICOM_FILE_MANIFEST ||--o{ DICOM_LOAD_ERRORS : "fails as"
    DICOM_INSTANCE ||--o{ DICOM_FRAME_OFFSET : "encapsulated only"
    DICOM_CAPABILITY ||--o{ DICOM_INSTANCE : "classifies syntax"
    DICOM_ERROR_CATALOG ||--o{ DICOM_LOAD_ERRORS : "types"
    DICOM_SCAN_SCRATCH }o--|| DICOM_SOURCE_CONFIG : "transient scan"

    DICOM_SOURCE_CONFIG {
        VARCHAR SOURCE_NAME PK
        VARCHAR STAGE_FQN "no @, fully qualified"
        VARCHAR FILE_PATTERN "LIKE pattern, not regex"
        NUMBER WINDOW_CENTER
        NUMBER WINDOW_WIDTH
        BOOLEAN ENABLED
        TIMESTAMP_LTZ LAST_SCAN_AT
        NUMBER LAST_SCAN_FILES
    }

    DICOM_SOURCE_PREFIX {
        VARCHAR SOURCE_NAME PK
        VARCHAR PREFIX PK
        BOOLEAN ENABLED
    }

    DICOM_FILE_MANIFEST {
        VARCHAR SOURCE_NAME PK
        VARCHAR FILE_NAME PK
        NUMBER FILE_SIZE
        VARCHAR FILE_MD5 "change detection"
        VARCHAR STATUS "NEW LOADED DELETED SUPERSEDED QUARANTINED"
        TIMESTAMP_LTZ FIRST_SEEN_AT
        TIMESTAMP_LTZ LAST_SEEN_AT
        TIMESTAMP_LTZ LOADED_AT
    }

    DICOM_INSTANCE {
        VARCHAR SOP_INSTANCE_UID "unique, asserted"
        VARCHAR SERIES_INSTANCE_UID
        VARCHAR STUDY_INSTANCE_UID
        VARCHAR SOP_CLASS_UID
        VARCHAR SOURCE_NAME "row access policy key"
        VARCHAR FILE_NAME "projection policy"
        NUMBER FILE_SIZE
        VARCHAR FILE_MD5 "staleness detection"
        VARCHAR TRANSFER_SYNTAX_UID
        NUMBER PIXEL_DATA_START "arithmetic offset base"
        NUMBER IMAGE_ROWS
        NUMBER IMAGE_COLUMNS
        NUMBER BITS_ALLOCATED
        NUMBER SAMPLES_PER_PIXEL
        NUMBER NUMBER_OF_FRAMES
        VARCHAR MODALITY
        VARCHAR PATIENT_ID "PHI_ID_MASK"
        VARCHAR PATIENT_NAME "PHI_TEXT_MASK"
        VARCHAR PATIENT_BIRTH_DATE "PHI_DATE_MASK"
        VARCHAR ACCESSION_NUMBER "PHI_ID_MASK"
        VARCHAR INSTITUTION_NAME "PHI_TEXT_MASK"
        VARCHAR REFERRING_PHYSICIAN_NAME "PHI_TEXT_MASK"
        VARCHAR DEVICE_SERIAL_NUMBER "PHI_ID_MASK"
        VARCHAR BURNED_IN_ANNOTATION "catalogued, never read"
        VARCHAR IMAGE_POSITION_PATIENT "geometry gate input"
        VARIANT RAW_METADATA "full DICOM JSON, UNMASKED today"
        TIMESTAMP_LTZ LOADED_AT
    }

    DICOM_FRAME_OFFSET {
        VARCHAR SOP_INSTANCE_UID PK
        NUMBER FRAME_NUMBER PK "1-based"
        NUMBER FRAME_OFFSET "payload, not item tag"
        NUMBER FRAME_LENGTH
        VARCHAR OFFSET_SOURCE "BOT EOT ITEM_SCAN"
        VARCHAR FILE_MD5 "must match instance or STALE"
        TIMESTAMP_LTZ COMPUTED_AT
    }

    DICOM_CAPABILITY {
        VARCHAR TRANSFER_SYNTAX_UID PK
        VARCHAR SYNTAX_NAME
        BOOLEAN IS_ENCAPSULATED "false means arithmetic offsets"
        BOOLEAN PARSE_SUPPORTED
        BOOLEAN FRAMES_SERVABLE
        BOOLEAN TRANSCODE_SUPPORTED
        NUMBER MAX_OBJECT_BYTES
    }

    DICOM_ERROR_CATALOG {
        VARCHAR ERR_CODE PK
        VARCHAR ERR_CLASS "DATA CAPACITY CONFIG CAPABILITY INVARIANT"
        VARCHAR SEVERITY
        BOOLEAN RETRYABLE
        BOOLEAN QUARANTINE
        VARCHAR RUNBOOK_STEP "travels with the failure"
    }

    DICOM_LOAD_ERRORS {
        VARCHAR SOURCE_NAME
        VARCHAR FILE_NAME
        VARCHAR PIPELINE_STEP
        VARCHAR ERR_CODE
        VARCHAR ERR_DETAIL
        TIMESTAMP_LTZ OCCURRED_AT
        BOOLEAN RESOLVED
    }

    DICOM_SCAN_SCRATCH {
        VARCHAR SOURCE_NAME
        VARCHAR FILE_NAME
        NUMBER FILE_SIZE
        VARCHAR FILE_MD5
    }
```

Live counts: manifest 29,837 / instances 29,837 / frame offsets **43** / errors 0.

`FILE_MD5` appearing in three tables is deliberate. It is how
`INVARIANT_0001_NO_STALE_OFFSETS` detects offsets computed against a file version
that no longer exists, which is the case that serves **the wrong pixels at HTTP
200**. Only 7 instances carry stored offsets; the other 29,830 resolve
arithmetically and store nothing.

---

## 2. Segmentation

```mermaid
erDiagram
    DICOM_INSTANCE ||--o| SEG_QUEUE : "series enqueued, logical"
    SEG_QUEUE ||--o{ SEG_RUN : "attempts, logical"
    SEG_QUEUE ||--o{ SEG_RESULT : "produces, logical"
    SEG_LABEL_MAP ||--o{ SEG_RESULT : "names structures"
    SEG_RESULT }o--o| DICOM_INSTANCE : "DICOM-SEG re-ingested"

    SEG_QUEUE {
        VARCHAR SERIES_INSTANCE_UID PK
        VARCHAR STUDY_INSTANCE_UID
        VARCHAR MODALITY
        NUMBER N_INSTANCES
        VARCHAR STATE "PENDING CLAIMED DONE FAILED SKIPPED"
        NUMBER ATTEMPTS
        NUMBER PRIORITY
        VARCHAR CLAIMED_BY
        TIMESTAMP_LTZ CLAIMED_AT
        VARCHAR ERR_CODE
        VARCHAR ERR_DETAIL "names the refusal reason"
    }

    SEG_RESULT {
        VARCHAR SERIES_INSTANCE_UID PK
        NUMBER STRUCTURE_ID PK
        VARCHAR STRUCTURE_NAME
        NUMBER VOXEL_COUNT
        FLOAT VOLUME_MM3 "divide by 1000 for mL"
        VARIANT BBOX_MIN
        VARIANT BBOX_MAX
        VARIANT CENTROID
        FLOAT MEAN_HU
        VARCHAR MASK_STAGE_PATH
        VARCHAR MODEL_NAME
        VARCHAR MODEL_VERSION "part of idempotency key"
        NUMBER INFERENCE_MS
    }

    SEG_RUN {
        VARCHAR RUN_ID PK
        VARCHAR SERIES_INSTANCE_UID
        NUMBER N_SLICES
        NUMBER N_STRUCTURES
        NUMBER GPU_SECONDS
        NUMBER LOAD_MS
        NUMBER INFERENCE_MS
        NUMBER POSTPROCESS_MS
        VARCHAR MODEL_NAME
        VARCHAR MODEL_VERSION
        VARCHAR STATE
    }

    SEG_LABEL_MAP {
        NUMBER LABEL_ID PK
        VARCHAR STRUCTURE_NAME
        VARCHAR SNOMED_CODE
    }
```

Live: queue 192 rows (2 DONE, 151 PENDING, 39 SKIPPED), results 21, runs 2,
label map 8.

Two traps encoded here:

- **`SEG_RESULT` is keyed on `(series, STRUCTURE_ID)` but idempotency is keyed on
  `(series, MODEL_VERSION)`**, so a re-run under a new model version leaves both
  generations in the table. Per-series aggregates must use
  `COUNT(DISTINCT STRUCTURE_ID)`.
- **Joining `SEG_RESULT` to `DICOM_INSTANCE` on `SERIES_INSTANCE_UID` fans out**
  (structures x slices). Aggregate `SEG_RESULT` separately before joining.

Only 8 of Vista3D's label ids are mapped; anything else lands as `label_<id>`
rather than being dropped.

---

## 3. Governance and de-identification

```mermaid
erDiagram
    USER_STUDY_SCOPE }o--o{ SOURCE_SCOPE_MAP : "matched by row access policy"
    SOURCE_SCOPE_MAP ||--o| DICOM_SOURCE_CONFIG : "scopes source"
    DICOM_TAG_POLICY ||--o{ DICOM_INSTANCE : "governs RAW_METADATA keys"
    DEID_SALT ||--o{ DICOM_INSTANCE : "salts pseudonyms via UDF"
    MODALITY_PIXEL_RISK ||--o{ PIXEL_VERDICT : "default risk by modality"
    PIXEL_VERDICT }o--|| DICOM_INSTANCE : "burned-in annotation verdict"

    USER_STUDY_SCOPE {
        VARCHAR USER_NAME PK
        VARCHAR SITE PK
        VARCHAR DEPARTMENT PK
        VARCHAR GRANTED_BY
        TIMESTAMP_LTZ GRANTED_AT
        TIMESTAMP_LTZ EXPIRES_AT
    }

    SOURCE_SCOPE_MAP {
        VARCHAR SOURCE_NAME PK
        VARCHAR SITE
        VARCHAR DEPARTMENT
    }

    DICOM_TAG_POLICY {
        VARCHAR TAG_HEX PK
        VARCHAR TAG_KEYWORD
        VARCHAR VR
        VARCHAR TAG_CLASS "SAFE INDIRECT DIRECT UID PIXEL"
        VARCHAR PS315_ACTION
        VARCHAR RATIONALE
    }

    DEID_SALT {
        VARCHAR SALT_NAME PK "phi_id, uid"
        VARCHAR SALT_VALUE "deploy-generated, granted to NOBODY"
        TIMESTAMP_LTZ CREATED_AT
    }

    PIXEL_VERDICT {
        VARCHAR SOP_INSTANCE_UID PK
        VARCHAR VERDICT
        VARCHAR METHOD
        FLOAT CONFIDENCE
        VARIANT DETECTED_REGIONS
        VARCHAR REVIEWED_BY
        VARCHAR MODEL_VERSION
    }

    MODALITY_PIXEL_RISK {
        VARCHAR MODALITY PK
        VARCHAR RISK
        VARCHAR RATIONALE
    }
```

Live: tag policy 60 rows, salts 2, scope 1 user / 3 sources, modality risk 15,
`PIXEL_VERDICT` **0 rows** so `V_PIXEL_SERVABILITY` reports `PRESUMED_CLEAN` by
default-deny logic.

`DEID_SALT` is the security-critical table. It carries no grants at all, and the
masking policy reaches it through `PSEUDO_ID` under the policy owner's rights.
Rotating a salt invalidates every previously issued pseudonym, by design.

`DICOM_TAG_POLICY` drives a generated `OBJECT_PICK` allowlist. **`OBJECT_PICK`
does not recurse** — it filters top-level keys only, so allowlisting a sequence
passes its entire nested payload including UIDs. That makes allowlisting a
sequence a much larger decision than allowlisting a scalar, and it is why
`MASK_RAW_METADATA` is currently detached.

---

## 4. Conformance and cost ledger

```mermaid
erDiagram
    CONFORMANCE_EXPECTATION ||--o{ CONFORMANCE_RESULT : "declared vs measured"
    CONFORMANCE_RESULT ||--o{ FRAME_LATENCY_SAMPLE : "same RUN_ID"
    BENCH_RUN

    CONFORMANCE_EXPECTATION {
        VARCHAR SUITE PK "QIDO WADO STOW TRANSPORT"
        VARCHAR TEST_NAME PK
        VARCHAR DICOM_REFERENCE "PS3.18 section"
        VARCHAR EXPECTED "PASS or NOT_IMPLEMENTED"
        VARCHAR RATIONALE
    }

    CONFORMANCE_RESULT {
        VARCHAR RUN_ID PK
        VARCHAR SUITE PK
        VARCHAR TEST_NAME PK
        VARCHAR OUTCOME "PASS FAIL NOT_IMPLEMENTED SKIPPED ERROR"
        NUMBER HTTP_STATUS
        NUMBER LATENCY_MS
        VARCHAR CLIENT "provenance of the assertion"
        VARCHAR CLIENT_VERSION
        VARCHAR DETAIL
    }

    FRAME_LATENCY_SAMPLE {
        VARCHAR RUN_ID
        VARCHAR SERIES_INSTANCE_UID
        VARCHAR SOP_INSTANCE_UID
        NUMBER FRAME_NUMBER
        NUMBER N_FRAMES "1 means single, more means batched"
        NUMBER BYTES_RETURNED
        NUMBER LATENCY_MS
        VARCHAR SERVED_FROM "warm or stage"
        VARCHAR TRANSFER_SYNTAX_UID
    }

    BENCH_RUN {
        VARCHAR RUN_ID PK
        VARCHAR PROFILE "INGEST or ZERO_USER"
        VARCHAR VARIANT_LABEL
        VARCHAR DEPLOYMENT_TAG
        TIMESTAMP_LTZ STARTED_AT
        TIMESTAMP_LTZ ENDED_AT
        VARCHAR WAREHOUSE_NAME
        VARCHAR WAREHOUSE_SIZE "NULL on pre-migration rows"
        NUMBER BATCH_LIMIT
        NUMBER PARTITIONS
        NUMBER IO_THREADS
        NUMBER FILES
        NUMBER INSTANCES
        NUMBER SERIES
        NUMBER STUDIES
        NUMBER FRAMES
        NUMBER BYTES_READ "header bytes, NOT file bytes"
        NUMBER ERRORS
    }
```

Live: expectations 40, results 356, latency samples 99, bench runs 3.

`CLIENT` on `CONFORMANCE_RESULT` records **provenance per assertion** —
`voxel-unit-tests` and `voxel-proxy-harness` are not the same evidence as a
third-party client over the wire. Conflating them is how a conformance statement
becomes worthless. Current posture: 32 MATCH / 8 UNTESTED / 0 MISMATCH.

`BYTES_READ` stores `SUM(PIXEL_DATA_START)`, i.e. header bytes only. Charging the
run the full file size would overstate I/O by roughly 140x on this corpus and
understate the exact advantage being measured.

`BENCH_RUN` lives in schema `BENCH`, which teardown clones to
`VOXEL_DB_BENCH_KEEP` before dropping the database.

---

## 5. View layer

Views by purpose. All 28 read from the tables above; none materialize.
20 live in `IMAGING`, 8 in `BENCH`.

```mermaid
flowchart LR
    subgraph SRC["Tables"]
        T1[("DICOM_INSTANCE")]
        T2[("DICOM_FILE_MANIFEST")]
        T3[("DICOM_FRAME_OFFSET")]
        T4[("DICOM_LOAD_ERRORS")]
        T5[("SEG_*")]
        T6[("CONFORMANCE_*")]
        T7[("ACCOUNT_USAGE")]
    end

    subgraph CORR["Correctness"]
        C1["V_FRAME_COVERAGE<br/>ARITHMETIC STORED STALE GAPS"]
        C2["V_SERIES_GEOMETRY<br/>VOLUME_3D vs MULTIPHASE_4D"]
        C3["V_PREAD_COVERAGE<br/>catalogued vs bytes present"]
        C4["V_SEG_OVERLAY_COVERAGE<br/>numbers vs overlay"]
        C5["V_CAPABILITY_GAPS"]
    end

    subgraph OPS["Operations"]
        O1["V_PIPELINE_STATUS"]
        O2["V_TASK_HEALTH"]
        O3["V_ERROR_SUMMARY"]
        O4["V_LOAD_ERRORS_RECENT"]
        O5["V_SEG_QUEUE_HEALTH"]
        O6["V_DICOM_SERIES"]
    end

    subgraph GOVV["Governance"]
        G1["V_PHI_LINEAGE_SWEEP"]
        G2["V_TAG_POLICY_DRIFT<br/>via GET_DDL, not ACCOUNT_USAGE"]
        G3["V_UNCLASSIFIED_TAGS<br/>work queue, NOT a gate"]
        G4["V_PIXEL_SERVABILITY<br/>advisory only"]
    end

    subgraph ECON["Economics + conformance"]
        E1["V_BENCH_UNIT_ECONOMICS"]
        E2["V_BENCH_FRAME_PLANE_SAVINGS"]
        E3["V_COST_HOURLY"]
        E4["V_COST_FRESHNESS<br/>gates every cost claim"]
        E5["V_COST_BILLED_VS_CONSUMED"]
        E6["V_CONFORMANCE_SCORE"]
        E7["V_FRAME_LATENCY"]
        E8["V_SEG_UNIT_ECONOMICS"]
    end

    subgraph MIG["Migration compatibility"]
        M1["V_OBJECT_CATALOG"]
        M2["V_INSTANCE_PATHS"]
    end

    T1 --> C1
    T3 --> C1
    T1 --> C2
    T1 --> C3
    T2 --> C3
    T1 --> C4
    T5 --> C4
    T1 --> C5
    T2 --> O1
    T4 --> O3
    T4 --> O4
    T5 --> O5
    T1 --> O6
    T1 --> G1
    T1 --> G3
    T1 --> G4
    T6 --> E6
    T6 --> E7
    T7 --> E3
    T7 --> E4
    T7 --> E5
    T1 --> M1
    T1 --> M2

    classDef adv fill:#fff4e5,stroke:#e8a33d,color:#663c00
    class G3,G4 adv
```

`V_OBJECT_CATALOG` and `V_INSTANCE_PATHS` reproduce the incumbent's column
names and its `meta VARIANT` model so a migration is mostly a view swap. Stated
precisely: the incumbent writes `meta:['00100010']` and Snowflake needs
`meta['00100010']`, so it is a mechanical one-character edit per query, **not**
"queries run unchanged".

---

## 6. Schema residue worth cleaning

Three tables in `IMAGING` are **not part of the design** and appear in no
template. They are leftovers from expanding the corpus by hand:

| Table | Rows |
|---|---|
| `SCRATCH_IDC_LISTING` | 170,029 |
| `SCRATCH_IDC_SERIES` | 4,139 |
| `SCRATCH_IDC_PICK` | 348 |

They cost storage, they will confuse anyone reading the schema, and nothing
reconciles them. `DROP TABLE` is safe — no view, procedure or task references
them. They are excluded from the diagrams above deliberately.
