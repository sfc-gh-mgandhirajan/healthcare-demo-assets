---
name: bench
description: Measure and defend Voxel cost and performance claims - run ingest benchmarks, compute unit economics, compare billed vs consumed credits, quantify frame plane savings vs a row-per-frame cache. Use when preparing a cost comparison, validating a performance claim, or asked how much this costs. Triggers - benchmark, unit economics, cost per study, cheaper than alternatives, TCO, credits, cost comparison, prove the savings.
---

# Benchmark and defend the numbers

The point of this skill is to make cost claims **falsifiable**. Any number quoted
without a query behind it is a liability in a compete conversation.

## Rule zero: check freshness before quoting anything

```sql
SELECT * FROM VOXEL_DB.BENCH.V_COST_FRESHNESS;
```

Three lags that will make you say something wrong:

- `ACCOUNT_USAGE` warehouse metering: up to **45 minutes**
- SPCS / compute pool metering: up to **180 minutes**
- Metering views bucket by **hour**, so a run inside the current hour is
  under-reported

`V_COST_FRESHNESS` uses `MAX(START_TIME)` and clamps with `GREATEST(0, ...)`
because using `END_TIME` produces *negative* lag — the current hour's bucket ends
in the future. A negative freshness number is the tell that someone read the wrong
column.

## Billed vs consumed — these are different numbers

```sql
SELECT * FROM VOXEL_DB.BENCH.V_COST_BILLED_VS_CONSUMED;
```

`METERING_DAILY_HISTORY` reports **billed** credits; `WAREHOUSE_METERING_HISTORY`
reports **consumed**. They differ, and the gap is not an error:

- Cloud services are only billed above a **10% of daily compute** threshold, so
  small cloud-services usage is consumed but free.
- Quoting consumed when a customer's invoice shows billed is how a cost model
  loses credibility on the first slide.

Use **billed** for anything a customer will compare against an invoice.

## Currency is often NULL, and that is honest

`V_COST_RATE` reads `ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY`. If the role
cannot read `ORGANIZATION_USAGE`, `COST_TOTAL` comes back **NULL** rather than
being computed from an assumed rate.

That is deliberate. A fabricated $/credit rate produces a number that looks
authoritative and is wrong. Report credits, and let the customer apply their own
contracted rate.

## Run the benchmarks

```sql
-- End-to-end ingest: files/sec, credits, credits per 1000 files
CALL VOXEL_DB.BENCH.SP_BENCH_INGEST('label', 2000);

-- Idle cost with nobody using the system. Often the most persuasive number.
CALL VOXEL_DB.BENCH.SP_BENCH_ZERO_USER();

SELECT * FROM VOXEL_DB.BENCH.BENCH_RUN ORDER BY STARTED_AT DESC;
```

`SP_BENCH_ZERO_USER` matters because a row-per-frame cache in a managed Postgres
bills for storage and instance uptime whether or not anyone is looking at an image.
Voxel's steady state with zero users is a suspended warehouse and a suspended
compute pool.

## Unit economics

```sql
SELECT * FROM VOXEL_DB.BENCH.V_BENCH_UNIT_ECONOMICS;
```

Normalize to something a customer recognizes — credits per 1000 instances, per
study, per GB — not raw credits, which are unanchored.

## The frame plane saving

```sql
SELECT * FROM VOXEL_DB.BENCH.V_BENCH_FRAME_PLANE_SAVINGS;
```

This quantifies the structural difference rather than a tuning difference:

- the incumbent stores **one row per frame** in a managed Postgres instance and
  documents ~1.7 KB per cache entry, giving ~3 GB for 100k CT entries. It needs a
  Spark job to repopulate after ingest and a priority scorer to decide what to
  preload.
- Voxel stores **zero rows** for any natively-encoded instance, because
  `offset(n) = pixel_data_start + n * rows * cols * (bits/8) * samples` and every
  term is already in the metadata the viewer holds.

Across the 2,848 **native** instances that is **100% arithmetic, 0 rows stored**.
State the scope out loud, because the deployed corpus is no longer purely native and
the obvious query will not return zero:

```sql
SELECT RESOLUTION, COUNT(*) FROM VOXEL_DB.IMAGING.V_FRAME_COVERAGE
GROUP BY 1;
-- expect ~2,848 ARITHMETIC + 7 STORED

SELECT COUNT(*) AS STORED_OFFSET_ROWS FROM VOXEL_DB.IMAGING.DICOM_FRAME_OFFSET;
-- expect 43, NOT 0 — the 7 encapsulated instances need them
```

**Say "zero rows for native instances", never "the table is empty."** The 7
compressed instances were added deliberately to exercise the stored-offset path, and
they hold 43 rows. If you claim an empty table in a room where someone runs the
query, you lose the entire argument over a number that was never load-bearing.

The saving is not just storage. Offsets are **discovered once at ingest** rather than
maintained by a separate job — no Spark repopulation step, no staleness question, no
priority scorer deciding what to preload.

Be careful with the stronger version of this claim. Voxel *does* have an eviction
policy (`VOXEL_HOTRING_BUDGET_BYTES`) and *does* have a cache-priming job (the
prefetch sidecar) for the RAM frame ring — a different tier from the offset plane. An
earlier draft of this skill claimed Voxel had deleted those operational surfaces
outright. It has not, and one of them failed in exactly the way the claim said was
impossible: the sidecar starved for its whole life because the gateway never wrote
the hint file, and 100% of reads silently went cold.

So the defensible claim is narrow and still strong: **the offset plane has no cache
to prime, evict, or invalidate.** Argue that, not a system-wide absence of caching.

Be honest about the boundary: a corpus that is largely JPEG2000-compressed will be
`STORED`, not `ARITHMETIC`. **Be precise about what narrows, because it is easy to
overstate this and an earlier draft of this skill did.**

`DICOM_FRAME_OFFSET` is **one row per frame** — `SOP_INSTANCE_UID`, `FRAME_NUMBER`,
`FRAME_OFFSET`, `FRAME_LENGTH`, `OFFSET_SOURCE`, `FILE_MD5`, `COMPUTED_AT`. For an
encapsulated corpus that is structurally the *same shape* as what the incumbent stores. Do
not claim a storage-layout win here; there isn't one:

```sql
SELECT COUNT(*) AS ROWS_TOTAL, COUNT(DISTINCT SOP_INSTANCE_UID) AS INSTANCES
FROM VOXEL_DB.IMAGING.DICOM_FRAME_OFFSET;   -- 43 rows across 7 instances
```

The **12 bytes per frame** figure is real but it describes the *gateway's in-memory
manifest*, not the table: `struct.pack_into("<QI", packed, 12 * (frame_no - 1), ...)`
in `frames.py` — an 8-byte offset plus a 4-byte length in a packed array, rather than
a Python object per frame. That is a serving-path memory property, not a database
storage property. Attribute it to the right layer or someone who reads the schema
will catch it.

What genuinely survives on a fully-compressed corpus is the operational argument:
offsets are discovered **once at ingest** rather than maintained by a separate job,
and there is no cache to prime, evict, or invalidate on the offset plane. That is
narrower than "we store nothing," and it is defensible.

## Frame serving latency — including a result that argues against us

This is the number a radiologist actually feels, so it belongs in any performance
conversation. Measured over the real ingress, single-frame reads, cold-start outliers
excluded:

| Tier | n | min | p50 | max | avg payload |
|---|---|---|---|---|---|
| `stage` (SPCS stage volume) | 22 | 72 ms | **81 ms** | 159 ms | 343 KiB |
| `warm` (RAM hot ring) | 6 | 76 ms | **79 ms** | 241 ms | 340 KiB |

Also measured: cold first touch of an instance **630 ms** p50 (manifest fetch + file
open), and an 8-frame batched request **1366 ms** p50 for 2.4 MiB — one round trip
instead of eight.

```sql
SELECT SERVED_FROM, COUNT(*) AS N, ROUND(MEDIAN(LATENCY_MS)) AS P50_MS,
       ROUND(AVG(BYTES_RETURNED)/1024) AS AVG_KB
FROM VOXEL_DB.IMAGING.FRAME_LATENCY_SAMPLE
WHERE N_FRAMES = 1 AND LATENCY_MS < 500
GROUP BY SERVED_FROM;
```

**Do not claim the RAM tier as a win.** 79 ms against 81 ms on equivalent payloads is
noise. Latency at this corpus size is dominated by the ingress round-trip, not the
storage read, and the stage volume is already in the node's page cache — the hot ring
is competing against RAM that was effectively free.

Volunteer this. A negative result you surfaced yourself is worth more than a
favourable number, because it tells the room that the favourable numbers were measured
the same way. The honest framing is: "the architecture supports a RAM tier, we built
it, and at this scale it buys nothing — here is the query."

Two caveats that cut both ways and should be stated: n is small (28 samples), and the
corpus is 2,855 files across 10 patients. This is directional, not a benchmark. Do not
let it be quoted as either a proof or a disproof of scaling behaviour.

## Conformance posture — the strongest credibility artifact available

Rule 4 below says to volunteer what is not implemented. The conformance score is the
mechanized version of that, and it is the most useful thing to put in front of a
skeptical audience:

```sql
SELECT ASSESSMENT, COUNT(*) FROM VOXEL_DB.IMAGING.V_CONFORMANCE_SCORE
GROUP BY ASSESSMENT;
```

**32 MATCH / 8 UNTESTED / 0 MISMATCH / 0 UNDECLARED** after an external client drove
the live ingress (23 assertions, 0 failing). All 8 UNTESTED are declared
`NOT_IMPLEMENTED`, not unknowns.

Two properties worth pointing out explicitly, because they are what make the number
mean anything:

The view uses a `FULL OUTER JOIN`, so it reports **UNDECLARED** — tested but absent
from the declared surface — as well as UNTESTED. Drift in that direction is the kind
nobody instruments, and it caught a real omission on the first run.

Before the harness ran, the same view read 22 MATCH / 17 UNTESTED. It did not default
to pass. If a scoring view cannot report its own ignorance, its passes are worthless.

## Attribution — do not repeat this mistake

An early version of this project attributed ingest throughput to threading inside
the UDTF. Measurement showed the dominant factor was **reading only the header**
rather than the whole object. Threading helped materially less.

Both make things faster, so a benchmark that only reports the total looks
consistent with either story. Attributing a win to the wrong mechanism means the
next optimization is aimed at the wrong thing.

Check the real distribution before attributing anything:

```sql
SELECT ROUND(AVG(FILE_SIZE)/1024.0, 1) AS AVG_KIB,
       ROUND(AVG(PIXEL_DATA_START)/1024.0, 1) AS AVG_HEADER_KIB,
       ROUND(AVG(PIXEL_DATA_START) / NULLIF(AVG(FILE_SIZE), 0) * 100, 2) AS PCT_READ
FROM VOXEL_DB.IMAGING.DICOM_INSTANCE;
```

`PCT_READ` is the honest version of the claim: that fraction of each object is what
ingestion actually reads.

## Comparing against a competitor

Rules that keep a comparison defensible:

1. **Same corpus.** Different data is not a benchmark.
2. **Include idle.** A cost model that only counts active query time flatters
   anything with an always-on component.
3. **Quote billed, not consumed.**
4. **State what is not implemented.** `CONFORMANCE_EXPECTATION` lists 8 declared
   `NOT_IMPLEMENTED` items. Volunteering them is what makes the rest credible.
5. **Show the query.** Every number above has one.
