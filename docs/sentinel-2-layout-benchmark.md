# Sentinel-2 layout benchmark plan

Status: the one-month smoke test and the eight-layout, two-year local benchmark
are complete. Smoke measurements are in `benchmark-results/sentinel-2-smoke/`;
the 96 full layout/query/engine rows are in
`benchmark-results/sentinel-2-layout/results.csv`, with five individual runs
and write-side measurements in each engine's `*-run.json` file.

## Source and scope

Use the Planetary Computer `sentinel-2-l2a` `geoparquet-items` snapshot for
2023-01-01 through 2024-12-31 (UTC). Keep a read-only, unchanged local copy of
the 24 source Parquet files. Inventory their names, sizes, row counts, and
checksums before generating layouts. The current remote listing totals
15,214,864,748 bytes and 11,318,221 items. This snapshot
currently lists one source file per month, despite the collection metadata's
weekly partition-frequency label. Record the retrieval date and asset URL.

The counts below were read from the Parquet footers of the 24 files listed on
2026-11-17 and verified against the downloaded source. The source file sizes
and SHA-256 digests are in
`benchmark-results/sentinel-2-layout/source-inventory.json`.
They describe the snapshot at that time, not an API search count. The original
100,000–150,000 items/month estimate was too low: the observed range is
284,256–747,929 items/month.

| Month     |          Items |       Source bytes |
| --------- | -------------: | -----------------: |
| 2023-01   |        464,849 |        655,119,771 |
| 2023-02   |        478,693 |        701,388,389 |
| 2023-03   |        725,448 |        973,313,012 |
| 2023-04   |        688,161 |        953,230,039 |
| 2023-05   |        559,208 |        774,358,174 |
| 2023-06   |        683,000 |        913,690,505 |
| 2023-07   |        686,130 |        924,426,677 |
| 2023-08   |        747,929 |        995,858,546 |
| 2023-09   |        665,388 |        906,211,205 |
| 2023-10   |        548,866 |        767,524,651 |
| 2023-11   |        482,242 |        664,056,226 |
| 2023-12   |        352,465 |        495,533,344 |
| 2024-01   |        297,467 |        411,179,549 |
| 2024-02   |        339,996 |        469,760,690 |
| 2024-03   |        379,543 |        528,494,255 |
| 2024-04   |        378,030 |        522,319,540 |
| 2024-05   |        390,208 |        540,926,502 |
| 2024-06   |        392,845 |        538,432,417 |
| 2024-07   |        394,316 |        516,954,444 |
| 2024-08   |        386,222 |        453,082,566 |
| 2024-09   |        284,256 |        337,608,430 |
| 2024-10   |        358,191 |        424,256,656 |
| 2024-11   |        305,161 |        360,193,028 |
| 2024-12   |        329,607 |        386,946,132 |
| **Total** | **11,318,221** | **15,214,864,748** |

The earlier benchmark harness and notebook are in the sibling
`stac-geoparquet-hashing` checkout (`src/cosgp/cli/benchmark/` and
`notebooks/duckdb-geoparquet-benchmarks.ipynb`). Reuse its fixed STAC search
predicates and `run.json` result convention. Its runner resolves parameters
per dataset, uses one DuckDB process, and records only timing and row count;
this experiment must pin parameters across layouts and record cold-read and
write-side measurements separately.

## First-pass layouts

The reference is A. Change one variable from A in each other layout.

| ID  | File granularity | Rows per row group | Sort within file           |
| --- | ---------------- | -----------------: | -------------------------- |
| A   | month            |            125,000 | MGRS tile, datetime, id    |
| B   | quarter          |            125,000 | MGRS tile, datetime, id    |
| C   | year             |            125,000 | MGRS tile, datetime, id    |
| D   | month            |             10,000 | MGRS tile, datetime, id    |
| E   | month            |            125,000 | bbox Hilbert, datetime, id |
| F   | unpartitioned    |            125,000 | MGRS tile, datetime, id    |
| G   | month            |            500,000 | MGRS tile, datetime, id    |
| H   | month            |            125,000 | datetime, id               |

Keep schema, compression, writer version, dictionary/statistics settings, and
query parameters identical. Every file needs a GeoParquet 1.1 `bbox` covering
column. Generate one layout from the immutable source, measure it, save the
results and file metadata, then remove the generated layout before starting
the next. Never remove the source. Check free space before each build.

## Run order

1. Count rows by month from source Parquet footers and record the source
   inventory. Check actual field names and GeoParquet metadata.
2. Implement a layout generator, a query runner, and result aggregation with
   layout and query definitions separated. Include a reproducible config.
3. Smoke-test three layouts (A, D, H) against three queries: small bbox/week,
   tile/full time series, and global/day. Verify identical counts and inspect
   row groups, covering metadata, and catalog links.
4. Run all eight layouts and six fixed queries with DuckDB and PyArrow. The
   six queries are bbox/week, bbox/year, tile/all years, bbox/month/cloud < 20,
   global/day, and full-collection count. Use the same bbox, tile, and UTC
   dates throughout. Record each repeat, median, p95, result count, files
   opened, peak process memory, engine and writer versions. Attempt fresh
   processes between repeats. On local disk, OS cache may persist: label the
   runs local and never describe them as cold remote reads. Bytes read and
   range requests are remote-only metrics; record them as unavailable locally.
5. For each layout, simulate adding 2024-12 to a collection containing the
   preceding 23 months. Record bytes written, number and bytes of files
   rewritten, final file count, `collection.json` size, and
   `_manifest.parquet` size. Every Parquet file must have a corresponding
   `geoparquet-items-...` asset in the collection; readers must not need a
   bucket listing.
6. Aggregate one row per layout, query, and engine in the existing `run.json`
   style and a flat results table. Compare read time, write amplification,
   file count, and catalog growth. Recommend a default only from measured
   results. State missing metrics and assumptions explicitly.

The eight-layout pass characterizes the main effects. Add 50,000-row groups
or combined settings only if measured results suggest they could change the
recommendation. Run remote finalists before treating any local winner as the
production winner.

The 2024-06 smoke test used 392,845 items. All 18 layout/query/engine results
agreed on counts: 152 for bbox/week, 12 for the chosen tile, and 12,826 for
global/day. This validates the harness on one source month; its timings do not
rank the layouts for the two-year collection.

## Full local results

All eight layouts returned the same counts in both engines: 152 bbox/week,
7,317 bbox/year, 394 tile time-series, 366 bbox/month/cloud, 12,826
global/day, and 11,318,221 full-collection count. Each median below is from
five fresh reader processes. Times include engine initialization but exclude
process startup and manifest selection. They are local timings with an uncleared
OS file cache, so millisecond differences should not be treated as stable
production wins.

| Query                   | Fastest DuckDB median | Fastest PyArrow median | Interpretation                                                        |
| ----------------------- | --------------------: | ---------------------: | --------------------------------------------------------------------- |
| Small bbox, week        |            A: 42.9 ms |              A: 6.3 ms | Monthly files are best here.                                          |
| Small bbox, year        |            A: 49.3 ms |             C: 18.2 ms | PyArrow benefits from fewer files; DuckDB does not in this local run. |
| Tile, all years         |            A: 49.2 ms |             C: 23.8 ms | PyArrow favors year or quarter files.                                 |
| Bbox, month, cloud < 20 |            A: 43.6 ms |              A: 6.7 ms | Monthly A leads both engines.                                         |
| Global, day             |            A: 40.9 ms |              H: 3.5 ms | H and G are within 0.3 ms of A in PyArrow.                            |
| Full count              |            G: 46.5 ms |             C: 21.3 ms | Metadata-heavy count favors different layouts by engine.              |

The 10,000-row-group layout D was slower than A for all six queries in both
engines. Hilbert layout E did not show a material local advantage over A.
500,000-row-group G improved DuckDB's full count by 1.5 ms but lost on the
selective queries. Datetime-sorted H had a 0.3 ms PyArrow global/day lead over
A, with slower tile and bbox/year reads. These differences should be checked
remotely before using them to tune production layout.

Appending 2024-12 means starting with the preceding 23 months, publishing the
new month, and rewriting `collection.json` and `_manifest.parquet`. Parquet
bytes below are measured sizes of newly published output, not source bytes or
temporary sorting I/O. The catalog and manifest sizes are after the append.

| Layout                 | New Parquet bytes | Prior Parquet files replaced | Final Parquet files | `collection.json` bytes | `_manifest.parquet` bytes |
| ---------------------- | ----------------: | ---------------------------: | ------------------: | ----------------------: | ------------------------: |
| A, month/125k/tile     |       139,856,464 |                            0 |                  24 |                   5,203 |                     5,588 |
| B, quarter/125k/tile   |       410,120,334 |                            1 |                   8 |                   2,083 |                     4,210 |
| C, year/125k/tile      |     2,219,833,538 |                            1 |                   2 |                     861 |                     3,559 |
| D, month/10k/tile      |       153,262,134 |                            0 |                  24 |                   5,203 |                     5,582 |
| E, month/125k/Hilbert  |       145,489,662 |                            0 |                  24 |                   5,203 |                     5,587 |
| F, one file/125k/tile  |     6,262,069,128 |                            1 |                   1 |                     668 |                     3,368 |
| G, month/500k/tile     |       136,794,412 |                            0 |                  24 |                   5,203 |                     5,585 |
| H, month/125k/datetime |       143,288,872 |                            0 |                  24 |                   5,203 |                     5,583 |

The two metadata files are also rewritten on every append. For C and F, the
append additionally wrote a 139,820,257-byte sorted staging chunk, recorded
separately in `run.json`. The published Parquet bytes for B, C, and F replace
existing files of 275,710,159, 2,074,333,960, and 6,099,249,257 bytes,
respectively. Temporary DuckDB sort spill was not measured.

**Provisional default: A, monthly files sorted by MGRS tile then datetime,
with 125,000 rows per row group.** It leads the short-window queries in both
engines, avoids Parquet rewrites when a month is appended, and needs only 24
catalog assets across two years. Quarter B is a reasonable finalist if remote
PyArrow tile and year-spanning reads dominate: it uses eight files and improves
those local queries, while appending December writes about 2.9 times as much
Parquet data as A. Year C and the single-file F impose much larger append
rewrites for their PyArrow gains.

## What remains unmeasured

- These results are local and are **not remote cold-read benchmarks**. Each
  repeat used a new process; the OS file cache was not cleared. Remote bytes
  read, range requests, and actual files opened are null in the result files.
  `candidate_files` records the manifest-selected file count instead.
- The reader timer starts after manifest selection and stops after the count.
  It includes the engine import and query setup but excludes process startup,
  catalog parsing, and manifest read. The full-collection baseline is a count,
  which engines may answer from Parquet metadata rather than scanning rows.
- The write-side results measure bytes and file replacement, not elapsed
  generation time or object-store PUT operations. Published output and staging
  bytes are separate; DuckDB temporary sort spill was not captured.
- One bbox, tile, and set of dates were held fixed; these are representative
  probes, not a geographic or seasonal sample. A remote A-versus-B comparison
  should precede a production decision.

## Resource and interpretation limits

The source listing is 15.2 GB decimal. The workspace initially had about
59 GiB free, so temporary sort space must be checked during the smoke test.
Ask before downloading more than approximately 50 GB. Local timings are useful
for narrowing candidates, but they do not measure object-store latency, range
requests, or remote bytes. Fresh reader processes do not clear the OS file
cache. Do not reuse timing results from the older 2025 benchmark as results
for this 2023–2024 experiment.
