# Sentinel-2 remote follow-up

Status (2026-11-23): all five selected layouts (A, B, E, G, H) are published
and fully measured with DuckDB and PyArrow. The 60 layout/query/engine rows are
in `benchmark-results/sentinel-2-remote/results.csv`; ten `*-run.json` files
hold the five individual reads for each query. Counted remote reads totaled
4,301,068,990 bytes, below the 50 GB download cap. The local eight-layout
benchmark remains in `benchmark-results/sentinel-2-layout/`.

## Scope

Run A, B, E, G, and H from the local benchmark. This tests monthly versus
quarterly files, tile versus Hilbert versus datetime sorting, and 125,000
versus 500,000 rows per group. Yearly C is a possible follow-up for a workload
dominated by long-range reads; it was outside this five-layout remote pass.
The original source files remain local and unchanged.

Use a new object-store prefix for these benchmark outputs. Each layout keeps
its own `collection.json`, `_manifest.parquet`, and every Parquet file named in
the collection assets. The uploader builds one layout from the 24-month source,
uploads it, then removes only the generated layout directory. It does not
repeat the append simulation; the local `run.json` files already record that
write cost. Before uploading, it checks that regenerated Parquet hashes match
the files measured locally.

```shell
source-coop login
uv run --extra benchmark python -m stac_geoparquet_archives.benchmark.remote_publish \
  --prefix stac-geoparquet/sentinel-2-layout-benchmark-2023-2024-dd27913 \
  --layout A --layout B --layout E --layout G --layout H
uv run --extra benchmark python -m stac_geoparquet_archives.benchmark.remote run \
  --prefix developmentseed/stac-geoparquet/sentinel-2-layout-benchmark-2023-2024-dd27913 \
  --layout A --layout B --layout E --layout G --layout H --repeats 5
```

The continuation script runs E, G, then H sequentially, with up to eight outer
retries per publish or read command. It resumes completed objects and query
rows after a failure. Source Cooperative closed several multipart upload
connections; increasing part concurrency from four to 12 made the errors more
frequent, so the publisher uses four 16 MiB parts concurrently. Credentials
expired during H's upload and were refreshed before resuming. The uploader also
retries transient HTTP 520 and 524 responses from upload and HEAD requests.

The remote runner reads the collection and manifest through a local counting
HTTP proxy, then uses DuckDB `httpfs` or PyArrow's native S3 reader through the
same proxy. Every repeat is a new process. The timer covers catalog and
manifest reads, engine initialization, and the query. The proxy records GET and
HEAD requests, Range requests, response bytes, and distinct data files touched.
The result schema matches the local `*-run.json` and `results.csv`, with
per-repeat I/O details in `individual_reads`. Results go to
`benchmark-results/sentinel-2-remote/`.
Completed queries are saved immediately, and `--resume` continues after an
interruption. The runner stops before 50 GB of counted downloads and limits
any one repeat to 2 GB; ask before raising either limit.

These are remote object-store reads, but fresh processes do not prove that a
CDN or object-store cache is cold. Record the observed cache headers and do not
call the measurements cold if an upstream cache cannot be controlled. A local
loopback proxy adds a small, shared overhead to all layouts. The upload is a
separate write cost; the local append simulation remains the source of
write-amplification figures.

## Measured comparison

The table gives median wall seconds from five fresh processes for each query
and engine. Timings include the catalog and manifest fetch, engine setup, and
query. All row counts matched the local results. Detailed p95, bytes read,
Range requests, files opened, and peak memory are in the CSV; individual
repeats are in the `*-run.json` files. PyArrow used a 120-second request timeout
and 30-second connection timeout across all layouts. DuckDB's external file
cache was disabled.

| Engine | Query | A month/tile | B quarter/tile | E month/Hilbert | G month/tile/500k | H month/datetime |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| DuckDB | Small bbox, week | 3.53 | 5.27 | **3.09** | 5.20 | 3.16 |
| DuckDB | Small bbox, year | 33.62 | **11.76** | 13.76 | 37.40 | 51.39 |
| DuckDB | Tile, all years | 7.88 | 6.54 | 7.98 | **5.95** | 20.91 |
| DuckDB | Bbox, month, cloud < 20 | 4.41 | 5.71 | **2.77** | 6.01 | 8.67 |
| DuckDB | Global, day | 3.63 | 2.78 | **2.44** | 2.81 | 2.59 |
| DuckDB | Full collection count | 6.47 | 5.92 | 5.01 | **4.61** | 5.75 |
| PyArrow | Small bbox, week | **5.27** | 6.08 | **5.27** | 6.14 | 6.35 |
| PyArrow | Small bbox, year | 30.30 | **16.14** | 29.16 | 40.99 | 54.73 |
| PyArrow | Tile, all years | 50.33 | **14.40** | 44.76 | 45.29 | 56.13 |
| PyArrow | Bbox, month, cloud < 20 | 5.94 | 6.33 | **5.33** | 7.96 | 8.14 |
| PyArrow | Global, day | 4.99 | 4.68 | 4.87 | 4.95 | **3.00** |
| PyArrow | Full collection count | 56.43 | **13.81** | 40.41 | 37.81 | 28.75 |

E's Hilbert sort improved short bbox reads over A: DuckDB's one-week query
fetched 2.29 MB in four Range requests versus A's 2.75 MB in seven. E also
reduced DuckDB's one-year bbox median from 33.62 to 13.76 seconds. B's eight
quarterly files led the year-long bbox in both engines and the PyArrow tile
series and full count. G's 500k row groups helped DuckDB's tile series and
full count but slowed selective bbox queries. H's datetime sort led only the
PyArrow global-day query; its bbox and tile queries were slower than E or B.

The append simulation below published December 2024 after the prior 23 months.
These are measured Parquet output bytes; `collection.json` and the manifest are
also rewritten. All files remain linked from each layout's `collection.json`.

| Layout | Final files | New Parquet bytes | Prior Parquet files replaced | `collection.json` bytes | `_manifest.parquet` bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| A | 24 | 139,856,464 | 0 | 5,203 | 5,588 |
| B | 8 | 410,120,334 | 1 | 2,083 | 4,210 |
| E | 24 | 145,489,662 | 0 | 5,203 | 5,587 |
| G | 24 | 136,794,412 | 0 | 5,203 | 5,585 |
| H | 24 | 143,288,872 | 0 | 5,203 | 5,583 |

**Recommended default: E, monthly files sorted by bbox Hilbert position then
datetime, with 125,000 rows per row group.** It serves the short spatial/time
queries well in both engines and appends a month without rewriting Parquet
files. Choose B when year-spanning bbox and full tile-series reads dominate and
rewriting one quarterly file on append is acceptable. H's single-day PyArrow
lead does not offset its slower spatial and tile searches for this workload.

The public catalog returned `cf-cache-status: DYNAMIC` on an independent HEAD
check. Per-range upstream cache status was not captured, so these results
cannot be certified as cold object-store reads. The local loopback proxy adds
shared overhead to all layouts. The full-count query may be answered from
Parquet metadata and does not measure row-scan throughput. One bbox, tile, and
set of dates were held fixed; the rankings should be checked against a broader
geographic sample before production rollout.

## Not measured remotely

Yearly C, 10k-row-group D, and unpartitioned F were measured locally but not
uploaded for this remote pass. The 50k-row-group setting was not generated in
the local or remote first pass. B's long-range gain makes C worth testing if
long-range reads outweigh the local append cost: C rewrote a yearly file and
published 2,219,833,538 Parquet bytes for the December append. Write-side
elapsed time and temporary sort spill were not measured. Remote upstream cache
state could not be controlled, and range-level cache headers were not recorded.
