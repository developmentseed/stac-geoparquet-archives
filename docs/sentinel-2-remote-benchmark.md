# Sentinel-2 STAC GeoParquet layout benchmark

## What we tested

We compared five ways to store Sentinel-2 L2A STAC items as GeoParquet. A
_layout_ sets the time span of each file, the number of items in each Parquet
row group, and the order of items within a file. Time partitions determine how
many files a search opens and how much data an update rewrites. Row groups and
sort order determine how much data a reader can skip inside those files.

All layouts came from the same unchanged Planetary Computer snapshot: 24 source
files covering 2023–2024, with 11,318,221 items (15.2 GB of source Parquet).
They used the same schema, ZSTD compression, Parquet writer settings, and
GeoParquet 1.1 `bbox` covering column. Each collection catalog links every
Parquet file, so readers do not need a bucket listing.

We ran six fixed searches against each layout on Source Cooperative using both
DuckDB and PyArrow. Each query was repeated five times in fresh processes. The
timer includes catalog and manifest retrieval, reader setup, and the query.
All layouts returned the same item counts. The tables below show median wall
time; lower is better. The [results table](../benchmark-results/sentinel-2-remote/results.csv)
also records p95, bytes read, Range requests, files opened, and peak memory.
The [individual run files](../benchmark-results/sentinel-2-remote/) contain
each repeat and the writer settings.

## Layouts

The file count covers the full two-year collection. “Append write” is the
measured Parquet output when December 2024 was added after the prior 23 months;
it excludes temporary sorting I/O and the small catalog and manifest rewrite.
MGRS is Sentinel-2's tile grid. Hilbert order clusters nearby item bbox centers.
Every sort uses item `id` to break ties.

| Layout | Time per file | Rows per group | Order within each file           | Files | December append write           |
| ------ | ------------- | -------------: | -------------------------------- | ----: | ------------------------------- |
| A      | Month         |        125,000 | MGRS tile, then time             |    24 | 140 MB; no prior file rewritten |
| B      | Quarter       |        125,000 | MGRS tile, then time             |     8 | 410 MB; quarter file rewritten  |
| E      | Month         |        125,000 | Hilbert bbox position, then time |    24 | 145 MB; no prior file rewritten |
| G      | Month         |        500,000 | MGRS tile, then time             |    24 | 137 MB; no prior file rewritten |
| H      | Month         |        125,000 | Time                             |    24 | 143 MB; no prior file rewritten |

## Results

Median wall time in seconds, over five runs per query. “Tile series” searches
one MGRS tile across both years. “Cloudy month” searches a bbox for one month
with `eo:cloud_cover < 20`. The full count can be answered from Parquet metadata;
it is not a full row scan.

### DuckDB

| Query                            |     A |     B |     E |     G |     H |
| -------------------------------- | ----: | ----: | ----: | ----: | ----: |
| Small bbox, one week             |  3.53 |  5.27 |  3.09 |  5.20 |  3.16 |
| Small bbox, one year             | 33.62 | 11.76 | 13.76 | 37.40 | 51.39 |
| Tile series, both years          |  7.88 |  6.54 |  7.98 |  5.95 | 20.91 |
| Bbox and cloud filter, one month |  4.41 |  5.71 |  2.77 |  6.01 |  8.67 |
| Global, one day                  |  3.63 |  2.78 |  2.44 |  2.81 |  2.59 |
| Full collection count            |  6.47 |  5.92 |  5.01 |  4.61 |  5.75 |

### PyArrow

| Query                            |     A |     B |     E |     G |     H |
| -------------------------------- | ----: | ----: | ----: | ----: | ----: |
| Small bbox, one week             |  5.27 |  6.08 |  5.27 |  6.14 |  6.35 |
| Small bbox, one year             | 30.30 | 16.14 | 29.16 | 40.99 | 54.73 |
| Tile series, both years          | 50.33 | 14.40 | 44.76 | 45.29 | 56.13 |
| Bbox and cloud filter, one month |  5.94 |  6.33 |  5.33 |  7.96 |  8.14 |
| Global, one day                  |  4.99 |  4.68 |  4.87 |  4.95 |  3.00 |
| Full collection count            | 56.43 | 13.81 | 40.41 | 37.81 | 28.75 |

## Takeaways

- **E is the recommended default for the tested workload.** Its monthly files
  support cheap appends, and Hilbert ordering gave the best or nearly best
  short bbox reads in both engines. For DuckDB's week-long bbox search, E read
  2.29 MB in four Range requests, versus A's 2.75 MB in seven.
- **B is better for long-range reads.** Its eight quarterly files led the
  one-year bbox search in both engines and PyArrow's tile-series query. The
  trade-off is a 410 MB December append that rewrites the quarter file, roughly
  three times A's monthly append output.
- **G and H each have narrower wins.** G helped DuckDB's tile series and full
  count but slowed selective bbox searches. H made PyArrow's global one-day
  query fastest, while its bbox and tile searches were much slower.

These are remote reads, but fresh processes did not control Source
Cooperative's upstream cache, so they are **not verified cold-cache timings**.
Only one bbox, tile, and set of dates were tested. The snapshot is from
Planetary Computer; the planned production source may differ. Test additional
regions and production-source items before treating the recommendation as
final. Yearly, 10,000-row-group, and unpartitioned layouts were measured
locally but not in this remote pass; 50,000-row groups were not tested.
