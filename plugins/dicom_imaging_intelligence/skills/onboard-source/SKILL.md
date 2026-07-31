---
name: onboard-source
description: "Point the DICOM pipeline at a customer's own DICOM stage or object storage instead of the bundled demo data. Creates the storage integration and stage, registers the source and its scan prefixes, backfills, and hands over to the schedule. Triggers: onboard dicom source, use my own dicom, my own stage, customer dicom stage, add dicom source, point at my bucket, register imaging source, swap dicom stage, my S3 dicom."
---

# Onboard your own DICOM source

Swapping the source is **rows in two tables**, not a code change. Nothing in this
skill edits a `.sql` or `.py` file.

## Preconditions

- The `deploy` skill has already run (`DICOM_SOURCE_CONFIG`,
  `DICOM_SOURCE_PREFIX`, and the procedures must exist).
- Cloud-side access to the bucket/container holding the DICOM files, and someone
  who can edit its IAM trust policy.

## Paths & connection (set once)

```bash
CONN="<snowflake connection name>"
DB="<target_database from config.json>"
SCHEMA="<target_schema from config.json>"
```

## Step 1: Storage access

Ask which cloud. For S3 (Azure and GCS are analogous — the shape is identical,
only the provider block differs):

```sql
CREATE STORAGE INTEGRATION my_dicom_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<acct>:role/<role>'
    STORAGE_ALLOWED_LOCATIONS = ('s3://my-bucket/dicom/');

DESC INTEGRATION my_dicom_int;
```

MANDATORY STOPPING POINT: `DESC INTEGRATION` returns
`STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`. Those two values must
be added to the bucket role's trust policy **before** the stage will work. Give
them to the user and wait — do not proceed to Step 2 and report a confusing
permission error.

An internal stage works too, and needs none of this. The scan handles internal
and external stages identically.

## Step 2: Create and prove the stage

```sql
CREATE STAGE MY_DICOM_STG
    URL = 's3://my-bucket/dicom/'
    STORAGE_INTEGRATION = my_dicom_int;

LIST @MY_DICOM_STG LIMIT 10;
```

Confirm `LIST` returns files before going further. A directory table is **not**
needed and not used.

> If `LIST` at the stage root is slow or errors on size, the bucket is too large
> to enumerate in one call. That is not a failure — it is why prefixes exist. Use
> several narrow prefixes in Step 4 instead of one empty one.

## Step 3: Choose the window fallback

`WINDOW_CENTER` / `WINDOW_WIDTH` are the fallback used when a file carries no
window tags of its own. This matters more than it looks:

| Study type | Center / Width |
|---|---|
| Cardiac, soft tissue, mediastinal | `40` / `400` |
| Chest, lung parenchyma | `-600` / `1500` |
| Bone | `300` / `1500` |
| Brain | `40` / `80` |

Windowing a lung study at 40/400 clips nearly all parenchyma to black. Nothing
errors — the images render, the embeddings compute, and retrieval quality is
quietly poor. Ask what the study mix is rather than defaulting.

## Step 4: Register the source

```sql
INSERT INTO IDENTIFIER(:db_schema || '.DICOM_SOURCE_CONFIG')
    (SOURCE_NAME, STAGE_FQN, FILE_PATTERN, WINDOW_CENTER, WINDOW_WIDTH, ENABLED, DESCRIPTION)
VALUES ('SITE_A', '<DB>.<SCHEMA>.MY_DICOM_STG', '%.dcm', 40, 400, TRUE, 'Site A cardiac CT');

-- One empty prefix = scan the whole stage. New series under it are then
-- discovered automatically with no further config.
INSERT INTO IDENTIFIER(:db_schema || '.DICOM_SOURCE_PREFIX') (SOURCE_NAME, PREFIX, ENABLED)
VALUES ('SITE_A', '', TRUE);
```

Two rules worth stating to the user before they choose prefixes:

- **Paths must be unique across sources.** Metadata, base64 images, and
  embeddings are all keyed on `FILE_NAME` alone, so two sources exposing the same
  relative path would cross-contaminate. `SP_SCAN_ALL_SOURCES` detects this and
  refuses to modify the manifest rather than corrupting data silently.
- **Disabling a prefix marks its files `DELETED`.** That is intended — they are
  out of scope — but it is surprising the first time. Disabling the whole
  *source* does not, because delete-detection only considers sources present in a
  given scan.

## Step 5: Backfill

```sql
CALL SP_SCAN_ALL_SOURCES();          -- discover; reports new / updated / deleted
CALL SP_LOAD_METADATA(100000);       -- parse; failures quarantine, never abort
CALL SP_REFRESH_SERIES_REGISTRY();   -- derive series from the parsed tags
CALL SP_CONVERT_IMAGES(100000);      -- render base64 for embedding
CALL SP_EMBED_PENDING(100000);       -- needs the GPU service; SKIPs cleanly if down
```

Then look at what happened:

```sql
SELECT * FROM V_DICOM_PIPELINE_STATUS;
SELECT * FROM V_DICOM_LOAD_ERRORS_RECENT;
```

On a large source, run the scan and then let the scheduled tasks drain it in
bounded batches rather than forcing a single huge backfill.

## Step 6: Confirm series appeared, then schedule

```sql
SELECT SERIES_INSTANCE_UID, SERIES_LABEL, MODALITY, FILE_COUNT
FROM DICOM_SERIES_REGISTRY ORDER BY FILE_COUNT DESC LIMIT 20;
```

Series appear automatically, keyed on the real `SERIES_INSTANCE_UID` tag — the
customer never enumerates their series anywhere. Labels default to
`SERIES_DESCRIPTION` from the file; `DICOM_SERIES_LABEL_SEED` exists only to
override those with curated names and is empty for customer sources.

Hand over to the schedule with `/dicom-imaging-intelligence:pipeline-ops`.

## Optional: retire the demo source

```sql
UPDATE DICOM_SOURCE_CONFIG SET ENABLED = FALSE WHERE SOURCE_NAME = 'IDC_OPEN_DATA';
```

Stops future scans. Already-loaded rows are left alone, and the source is not
mass-marked `DELETED`.

## Hand-off

- `/dicom-imaging-intelligence:pipeline-ops` — resume the schedule and monitor
- `/dicom-imaging-intelligence:image-search-deploy` — if you want image search over the new source
