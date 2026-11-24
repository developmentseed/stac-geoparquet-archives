# **Plan: Sentinel-2 & Landsat stac-geoparquet on Source Cooperative**

Nov 10, 2026 · @Zac

Draft for Pete. Goal: publish well-partitioned, validated STAC-GeoParquet archives of Sentinel-2 L2A and Landsat Collection 2 on Source Cooperative, with a repeatable pipeline to keep them fresh.

## **Recommendation**

Build one pipeline for two upstream sources, with a local CLI and AWS queue/Lambda execution for bulk runs. Start with Element 84's Earth Search STAC for Sentinel-2 and test whether ESA's Copernicus Data Space (CDSE) can serve as the primary crawl; use CDSE for completeness checks and ESA asset hrefs in either case. Pull Landsat from USGS (daily bulk Parquet plus the landsatlook STAC API). Generating our own Sentinel-2 items is out of scope, and Planetary Computer is only a count check. Partition both by month and sort rows within each file by the mission's native grid, then datetime: a plain sort, since stac-hash is not proving faster in testing. Keep one Hive-style layout so DuckDB and rustac can prune by path. Validate every item on the way in (valid STAC, string asset hrefs that resolve) and every file on the way out (gpio best-practice checks). Run the spike on a laptop, then dispatch bulk and scheduled updates from GitHub Actions to AWS queue/Lambda workers. Publish under a Source Cooperative-owned account with Development Seed as the listed maintainer.

Why not just re-host Planetary Computer's exports: PC's geoparquet snapshots have gone stale or missing more than once (issues \#400, \#437, \#446), PC asset hrefs need SAS signing so they are not usable as plain strings, and Chris Holmes already did the PC copy as a WIP product in April 2025 and never updated it. Starting from the original providers is more work up front but gives us a product that is independent of PC's export cadence and whose asset links work for anyone.

The published archive should remain usable across cloud providers. Include the ESA or USGS href and verified `alternate` mirrors wherever they exist. AWS queues and Lambda are the chosen execution environment for bulk work, while Source Cooperative hosts the output.

## **What already exists**

Nobody is maintaining a public, best-practice stac-geoparquet archive of either mission today, which is the gap this fills.

| Dataset                                                                                         | Who                       | Source             | Layout                                                                                | Status (Nov 2026\)                                                                                                                                                                                                                                                                           |
| ----------------------------------------------------------------------------------------------- | ------------------------- | ------------------ | ------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [PC geoparquet-items](https://planetarycomputer.microsoft.com/docs/quickstarts/stac-geoparquet) | Microsoft                 | pgstac export      | sentinel-2-l2a partitioned weekly (W-MON); landsat by time                            | Hrefs are abfs://, need SAS signing; repeated reports of stale or missing files ([\#400](https://github.com/microsoft/PlanetaryComputer/issues/400), [\#437](https://github.com/microsoft/PlanetaryComputer/issues/437), [\#446](https://github.com/microsoft/PlanetaryComputer/issues/446)) |
| [cholmes/stac-geoparquet-public](https://source.coop/cholmes/stac-geoparquet-public)            | Chris Holmes, Source Coop | Bulk copy of PC    | Unlisted, WIP                                                                         | Created Apr 2025, last updated Aug 2025, no updates since; author notes files are large and "not yet perfect"                                                                                                                                                                                |
| [HLS STAC GeoParquet Archive](https://maap-project.github.io/hls-stac-geoparquet-archive/)      | NASA MAAP (DevSeed built) | HLS pipeline items | Hive: `{collection}/year=/month=/{collection}-{year}-{month}.parquet` on S3 us-west-2 | Live; the closest precedent for our layout and tooling (rustac DuckdbClient)                                                                                                                                                                                                                 |
| [USGS Bulk Metadata Service](https://www.usgs.gov/landsat-missions/bulk-metadata-service)       | USGS EROS                 | Archive metadata   | One `.parquet.gz` per sensor/level (OT, ETM, TM, MSS × L1/L2, plus ARD)               | Updated daily; Parquet added Mar 2026; not STAC, not spatially sorted                                                                                                                                                                                                                        |
| [Earth Search](https://github.com/Element84/earth-search)                                       | Element 84                | Cirrus pipeline    | STAC API only; SNS topic publishes every new item; S3 inventory of the data bucket    | Live; sentinel-2-c1-l2a \~15M items, landsat-c2-l2 \~8.5M                                                                                                                                                                                                                                    |
| [terrabyte (DLR)](https://docs.terrabyte.lrz.de/services/data-access/stac-geoparquet)           | DLR                       | pgstac export      | One file glob per collection                                                          | Monthly export, on-prem only, not public                                                                                                                                                                                                                                                     |

Tooling we can lean on: [rustac](https://github.com/stac-utils/rustac) (read/write stac-geoparquet, DuckDB search), [stac-geoparquet](https://github.com/stac-utils/stac-geoparquet) 0.7 (Python, spec lives here), [stac-hash](https://github.com/gadomski/stac-hash) 0.0.2 (space-time Morton sort key, Aug 2026), [geoparquet-io](https://geoparquet.org/geoparquet-io) (`gpio check all`, Hilbert sort, partitioning, STAC generation, upload to S3), and [pgstac-geoparquet-exporter](https://github.com/developmentseed/pgstac-geoparquet-exporter) (complete and incremental exports from pgstac).

## **Sentinel-2: where to pull from**

Earth Search (`sentinel-2-c1-l2a`) is the initial Sentinel-2 crawl candidate because it is public, needs no token, and serves clean STAC items. The revised document calls for pushing on the Copernicus Data Space Ecosystem (CDSE) as well. Test CDSE authentication, quotas, pagination, and coverage before choosing the primary crawl. Use CDSE for completeness checks and ESA hrefs regardless; Planetary Computer remains a count cross-check. Generating Sentinel-2 items from product metadata is out of scope.

|                 | Planetary Computer                                                                                                                                                                     | Earth Search (Element 84\)                                                                                                                       | CDSE (ESA origin)                                              |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------- |
| Collection      | `sentinel-2-l2a` (\~ESA baseline, Esri-processed)                                                                                                                                      | `sentinel-2-c1-l2a` (ESA Collection 1, COGs on AWS us-west-2)                                                                                    | `sentinel-2-l2a` (SAFE on CloudFerro S3)                       |
| Bulk path       | Existing weekly geoparquet on Azure, free to read                                                                                                                                      | None official; STAC API paging \+ SNS topic for new items \+ S3 inventory of the COG bucket                                                      | STAC API only (OData being decommissioned); quotas per account |
| Asset hrefs     | `abfs://` or blob URLs that need SAS signing: not usable as plain strings                                                                                                              | Public `s3://` and `https://` to `e84-earth-search-sentinel-data`, anonymous reads                                                               | `s3://` on CloudFerro needing CDSE credentials                 |
| Freshness       | Export has lagged by months and dropped files ([\#400](https://github.com/microsoft/PlanetaryComputer/issues/400), [\#446](https://github.com/microsoft/PlanetaryComputer/issues/446)) | Near-real-time; E84 publishes each ingested item to SNS                                                                                          | Hours after acquisition when the indexer is healthy            |
| Backfill effort | Lowest: copy \~1 TB of parquet, re-sort, rewrite hrefs                                                                                                                                 | Medium: \~15M items through a STAC API; weeks of paging if naive, days if sharded by MGRS tile and month and run in parallel                     | Highest: auth, quotas, and a schema still settling             |
| Known gaps      | Missing scenes acknowledged by MSFT; Collection 1 reprocessing not reflected                                                                                                           | Pre-Jul-2023 L1C href bug (L1C only); AWS archive missing hundreds of thousands of C1 products per Sinergise/Planet forum (reprocessing ongoing) | Authoritative, but STAC catalogue has had indexing outages     |
| Licensing       | Open, but hosted under MSFT terms                                                                                                                                                      | Open (AWS Open Data)                                                                                                                             | Open                                                           |

What "pulling from an API" actually costs: Earth Search has no official bulk dump, so a full backfill is on the order of 15M items at up to \~1,000 per page, roughly 15,000+ requests plus retries. That is tractable (days with a handful of parallel workers sharded by MGRS tile and month) but it must be resumable and idempotent, which is the main engineering cost of this path over copying PC. Two things make it cheaper than it sounds:

- Element 84 publishes an S3 inventory of its Sentinel-2 bucket, and Sinergise publishes one for `sentinel-s2-l2a`. Listing them gives the complete key space on both the COG side and the ESA side, so gaps are detectable without scanning the API.
- After the backfill, each run only needs items whose `updated` is past the last cursor: a cheap polled query that works on any STAC API, so no vendor notification service is needed.

### **Source choice and independence for Sentinel-2**

The source catalog and published asset hrefs are separate choices. The revised document calls for pushing on CDSE, so test its token, quota, coverage, and pagination before fixing the primary crawl. Keep provider and verified mirror hrefs in the output.

| Option                     | Role                                         | Independence                                                                        | Cost                                                                 |
| -------------------------- | -------------------------------------------- | ----------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| Earth Search (Element 84\) | Initial crawl candidate                                | Medium: independent company, open, no auth, but describes its COG derivative on AWS | Lowest; maintained by the team that maintains the sentinel-2 tooling |
| CDSE STAC API (ESA)        | Completeness referee and source of ESA hrefs | Highest among ready-made STAC                                                       | Token and quotas; occasional indexing outages                        |
| Generate items ourselves   | Out of scope                                 | Full                                                                                | Highest: \~15M products to parse and every edge case becomes ours    |
| Planetary Computer         | Count check only                             | Lowest                                                                              | Easy but stale                                                       |

**Mirrors in the output.** Each item carries the ESA product href as the primary asset and `alternate` entries (alternate-assets extension) for the Earth Search COGs and the AWS and GCP copies where we can verify the key exists. That is the concrete form of not picking a winner: a user on any cloud gets a working string, even though we crawled one catalog. One known wrinkle: Earth Search ids (`S2A_T13TDE_20260904T175741_L2A`) differ from ESA product ids (`S2A_MSIL2A_...`), so the mapping has to be done on tile plus sensing time. We need to decide which catalog supplies the canonical item, and add `derived_from` links to both upstream STAC items.

## **Landsat: where to pull from**

Start from USGS, using the daily bulk Parquet as the manifest and the landsatlook STAC API as the source of the actual STAC items. This is the cleaner path because USGS already hands us a complete, daily-refreshed table of every scene, which solves the hardest part of the API path (knowing what exists and what changed).

|              | Planetary Computer (`landsat-c2-l2`)                | USGS landsatlook STAC (`landsat-c2l2-sr`, `landsat-c2l2-st`, `landsat-c2l1`, ARD)                                       | USGS Bulk Metadata Parquet                                                                                                                                                                           |
| ------------ | --------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| What it is   | pgstac export of PC's own STAC, Azure-hosted assets | USGS's STAC items for every Collection 2 product on AWS (`usgs-landsat` bucket, requester-pays)                         | Flat table of all C2 L1/L2/ARD products, `.parquet.gz` per sensor and level, refreshed daily since Mar 2026                                                                                          |
| STAC-ness    | Yes, PC flavour                                     | Yes, authoritative, includes landsat extension fields and corrected geometry                                            | No: USGS metadata columns, no geometry column, no assets                                                                                                                                             |
| Asset hrefs  | Need SAS signing                                    | `s3://usgs-landsat/...` requester-pays and `https://landsatlook.usgs.gov/data/...` which need a USGS login for full-res | Only product IDs                                                                                                                                                                                     |
| Bulk path    | Geoparquet export on Azure                          | API paging (\~8.5M items across L2 alone)                                                                               | One download per sensor/level; USGS publishes a [guide to diffing versions](https://www.usgs.gov/software/using-landsat-bulk-metadata-parquet-files) to detect added, updated and reprocessed scenes |
| Reprocessing | Not reflected reliably                              | Reflected as items are regenerated                                                                                      | Reflected daily; this is the only source that tells us _what_ was reprocessed                                                                                                                        |

**Path A, USGS-native (recommended).** Download the bulk Parquet for OT/ETM/TM L2 (and L1 later), diff against yesterday's to get the add/update/reprocess set, fetch those items from landsatlook STAC by ID (`/collections/{c}/items/{id}`, no search paging), and append. Backfill is the same loop run over the full table: \~8.5M item GETs, parallelised, resumable. Output carries USGS hrefs unchanged plus an `alternate` asset block pointing at the AWS requester-pays key so users without a USGS login still have a working string.

One caution on the USGS path: the `https://landsatlook.usgs.gov/data/` hrefs in USGS items return full-res only with an EROS login, while the `s3://usgs-landsat` keys are requester-pays. Neither is anonymous. That is a property of Landsat on AWS, not of our product, but the README should say so plainly.

## **Partitioning strategy**

Partition by time at the file level, sort by space-time within each file. Time is the one dimension every STAC query filters on and the one that makes incremental updates cheap (only the current month's file is rewritten), while sorting handles spatial pruning through row-group statistics without multiplying file counts.

**Layout, same across the collections (Hive-style, matches the HLS archive):**

```
{collection}/
  collection.json                      # STAC Collection with one `geoparquet-items-{YYYY-MM}` asset per file
  year=2024/month=07/
    {collection}-2024-07.parquet
  year=2024/month=08/
    {collection}-2024-08.parquet
  _manifest.parquet                    # one row per file: path, item count, bbox, min/max datetime, sha256
```

For example, the collection's `assets` include `geoparquet-items-2024-07` with href `./year=2024/month=07/{collection}-2024-07.parquet`, media type `application/vnd.apache.parquet`, and role `data`.

| Choice          | Sentinel-2 L2A                                                                                             | Landsat C2 L2                                          | Why                                                                                                                                                                                                                                                                                                                                                              |
| --------------- | ---------------------------------------------------------------------------------------------------------- | ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Partition key   | `year=/month=`                                                                                             | `year=/month=`                                         | PC chose weekly for S2; monthly gives \~150k rows per file for S2 and \~40k for Landsat, both well under the point where a file needs splitting. Predictable paths let DuckDB and rustac prune by glob.                                                                                                                                                          |
| Secondary split | None in v1; revisit if a month exceeds \~1.5 GB                 | None; Landsat months are small                         | Keep one file per month until size forces otherwise.                                                                                                                                                                                                                                                                                                             |
| Row sort        | `grid:code` (MGRS tile), then `datetime`, then `id`                                                        | `landsat:wrs_path`, `landsat:wrs_row`, then `datetime` | A plain sort on the mission's native grid clusters rows spatially (tiles in one UTM zone and band sit together) and orders time inside each tile. stac-hash was the first candidate and is not proving faster than this, so no extra column. Hilbert via `gpio sort hilbert` is the fallback if the smoke suite shows bbox queries touching too many row groups. |
| Row group size  | \~50k rows (\~20 to 40 MB)                                                                                 | \~50k rows                                             | Small enough that a bbox filter skips most groups, large enough to keep metadata overhead down. rustac's default is 150k; we'd go smaller because items are wide.                                                                                                                                                                                                |
| bbox covering   | Yes: `bbox` struct column with GeoParquet 1.1 covering metadata                                            | Yes                                                    | Required for engines to prune on bbox stats.                                                                                                                                                                                                                                                                                                                     |
| Geometry        | WKB, GeoParquet 1.1 (Parquet native GEOMETRY as a v2 output later)                                         | Same                                                   | Widest reader support today.                                                                                                                                                                                                                                                                                                                                     |
| Compression     | ZSTD                                                                                                       | ZSTD                                                   | gpio default and the CNG best-practice recommendation.                                                                                                                                                                                                                                                                                                           |
| Assets          | Struct column of `{href, type, roles, title, ...}` per asset key, hrefs as strings; mirrors as `alternate` | Same                                                   | See validation. One level of `alternate` nesting, no deeper.                                                                                                                                                                                                                                                                                                     |
| Antimeridian    | Split geometries per STAC best practice before computing bbox                                              | Same                                                   | Avoids whole-world bboxes that defeat pruning.                                                                                                                                                                                                                                                                                                                   |

**Why time-first rather than space-first.** Spatial partitioning (H3, quadkey, admin) is right for static vector data like buildings, where queries are almost always by area and the data never changes. Imagery metadata is the opposite: new items arrive every day, nearly all queries carry a date range, and a spatial partition would force us to touch every file on each daily update. The HLS archive, PC, terrabyte and the geoparquet-io docs all land on the same conclusion.

**Sort key detail.** stac-hash (a Morton code over datetime and centroid) was the first candidate, but in testing it is not proving faster than a plain sort, so v1 sorts by native grid then datetime and records that order in the file metadata. The plain sort also costs nothing to explain: a user who filters on `grid:code` or a WRS path/row gets row-group pruning for free, and the bbox covering handles everything else. We keep the smoke-query suite (under Validation) so the choice can be revisited with numbers; re-sorting a month file takes seconds, and the stac-spec hash extension discussion ([\#1378](https://github.com/radiantearth/stac-spec/discussions/1378)) can continue independently of this product.

## **Validation**

Validate three times: each item before it is written, each file after it is written, and the whole product before it is promoted. A file that fails any gate never reaches the public prefix.

**Per item (in the ingest worker)**

- STAC 1.0/1.1 schema validation plus the extensions the item declares (eo, proj, view, sat, sentinel-2 / landsat), via `rustac validate` or pystac. Items that fail go to a `_rejects/` prefix with the error, not to the output.
- Every asset `href` is a non-empty string, absolute, with a scheme in `{https, s3}`. No relative hrefs, no `abfs://`, no signed query strings. This is the "assets be strings" requirement: the stac-geoparquet spec stores assets as a struct, and a null or non-string href silently breaks downstream readers.
- At least one asset with a data role (`data` or a band) exists; `geometry` is valid and matches `bbox`; `datetime` or `start_datetime`/`end_datetime` is set; `id` is unique within the collection.
- Sampled href resolution: for 1 in N items (N \= 500 for backfill, 20 for daily), issue a `HEAD` against the first data asset and record status. Fails the batch if the 4xx/5xx rate exceeds 1%. This catches provider-side URL changes before users do.

**Per file (after write)**

- `gpio check all`: GeoParquet metadata version, bbox column and covering, spatial ordering, ZSTD, row-group sizing.
- Round-trip: read the file with rustac and with the stac-geoparquet Python library and confirm both return the same item count and the same first and last ids. The two libraries do not yet share an implementation, and the HLS archive work surfaced differences between them.
- Partition invariants: every row's `datetime` falls inside the file's `year=/month=` path; row count and sha256 are written to `_manifest.parquet`.

**Per product (before promote)**

- Completeness against the provider: item count per month compared with the CDSE catalogue and the Sinergise S3 inventory (S2) and the USGS bulk Parquet (Landsat). Drift above 0.1% blocks the promote and opens an issue with the missing ids.
- Cross-check against the PC export counts per month, reported but not blocking.
- A smoke query suite run with DuckDB over `https://data.source.coop/...` (bbox \+ date, by-id lookup, cloud-cover histogram) with timing, so regressions in sort or row-group choices show up as slow queries.

## **Running it**

The CLI should run on a laptop for development and a one-month spike. GitHub Actions triggers bulk and scheduled runs; AWS queues and Lambda workers handle the parallel backfill and updates. The manifest and source cursors make each worker resumable and idempotent. This replaces the earlier single-VM assumption.

| Stage | Execution | Gate |
| --- | --- | --- |
| Spike (one month per collection) | Local CLI | Settle schema, sort, row groups, and round-trip validation. |
| Backfill | GitHub Actions dispatches work to an AWS queue; Lambda workers crawl by collection and month | Validate a three-month sample, then backfill the full archive. |
| Daily update | GitHub Actions scheduled trigger with queue/Lambda workers | Poll source updates, diff the USGS bulk Parquet, and rewrite affected month files. |
| Promote | Write to `_staging/`, validate, then publish | A failed validation leaves the public files untouched. |

**Infrastructure.** Keep the same CLI behavior locally and in Lambda. Define the AWS queue, Lambda workers, and the permissions needed for source reads and Source Cooperative uploads. The queue provides retries and distributes the bulk workload. Use the source catalogs and manifests for completeness rather than a vendor notification topic.

**Idempotency rules.** Key work by (collection, year, month). Record the source cursor (or USGS bulk file date) in the manifest so a retried worker resumes safely. Reprocessed items (same id, newer `updated`) replace the old row, and the affected monthly file is rewritten. Publication happens only after validation succeeds.

**Cost and timing.** Re-estimate compute cost and backfill duration for the queue/Lambda design after the one-month spike; the earlier single-VM estimate does not apply. Engineering time remains concentrated in the resumable crawlers and validation.

## **Where to publish: whose account**

Publish under a Source Cooperative-owned account (for example `source/sentinel-2-c1-l2a-stac` and `source/landsat-c2-l2-stac`), with Development Seed named as maintainer in the README and the pipeline repo. This is the case I'd make to Pete and to Radiant Earth.

|                                | Source-owned account                                                                                                                                                                                                | Development Seed account                                                                                                   |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| Signal to users                | A platform-curated reference archive, like `source/source-stats`; neutral, likely to be trusted and cited                                                                                                           | A vendor's product; fine, but reads as one firm's side project                                                             |
| Longevity                      | Survives DevSeed losing interest or funding; Source can hand maintenance to another org                                                                                                                             | Tied to our account and our attention                                                                                      |
| Fit with Source's stated model | Source's FOSS4G NA 2026 talk describes a platform that is open to consume and curated to publish, with whitelisted publishers; a flagship STAC archive is exactly the kind of thing the platform itself should host | Also fine; we would be one of the whitelisted publishers                                                                   |
| Credit                         | README, repo, and a "maintained by Development Seed" line; Groundwork post and talks                                                                                                                                | Our name on the URL                                                                                                        |
| Friction                       | Needs Radiant Earth to agree and to grant write credentials to our CI                                                                                                                                               | None beyond getting a publisher account                                                                                    |
| Precedent                      | Chris Holmes (Source Coop staff) already started this under his personal account, so Source clearly wants it to exist                                                                                               | HLS archive lives under NASA MAAP's bucket, not DevSeed's: same pattern of the data owner hosting, the builder maintaining |

The compromise if Radiant Earth is slow to decide: publish under `developmentseed/` now, with the README stating the intent to move it to a Source-owned account, and keep the layout identical so the move is a copy plus a redirect note. Since Chris's WIP product exists, the first step is a conversation with him rather than a cold proposal: we can offer to replace his copy with a maintained one.

Either way, the product pages should carry the STAC Collection JSON (with one `geoparquet-items-{YYYY-MM}` asset per monthly file), a DuckDB and a rustac example, the update cadence, and the completeness numbers from the last run.

## **Where the code lives**

The pipeline is a product, not a library, so it should get its own repo under `developmentseed/`, and anything reusable it produces should be pushed down into the existing library repos rather than accumulated in the pipeline.

The current spread, and what each piece is for:

| Repo                                                                                        | Org             | Role                                                              | Keep doing                                                                                                                               |
| ------------------------------------------------------------------------------------------- | --------------- | ----------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| [stac-geoparquet](https://github.com/stac-utils/stac-geoparquet)                            | stac-utils      | The spec and the Python reference library (0.7)                   | Spec changes (hash column, partitioning guidance) go here                                                                                |
| [rustac](https://github.com/stac-utils/rustac) / rustac-py                                  | stac-utils      | Rust read/write, DuckDB search, CLI; the engine we'd use to write | Writer features (sort, row-group size, bbox covering) go here                                                                            |
| [stac-hash](https://github.com/gadomski/stac-hash)                                          | gadomski        | Space-time sort key, 0.0.2, alpha                                 | Move to stac-utils once the extension draft settles; the pipeline depends on it, so it should not live in a personal namespace long term |
| [pgstac-geoparquet-exporter](https://github.com/developmentseed/pgstac-geoparquet-exporter) | developmentseed | Export from pgstac, full and incremental                          | Not needed here (no pgstac in the loop); the incremental-export patterns are worth borrowing                                             |
| [stac-fastapi-geoparquet](https://github.com/stac-utils/stac-fastapi-geoparquet)            | stac-utils      | Serve a STAC API over geoparquet                                  | Update it to discover GeoParquet files from STAC Collection links on server startup; a separate refresh job should not be needed                                                                     |
| [stac-map](https://github.com/developmentseed/stac-map)                                     | developmentseed | Browser viewer with stac-geoparquet support                       | Link from the product README as the "open this in a map" path                                                                            |

**Current repo: `developmentseed/stac-geoparquet-archives`.** One repo, one pipeline, a `collections/` directory with a YAML per collection (source, partition key, validation thresholds), the AWS queue/Lambda infrastructure, the Actions workflows, and the validation suite. It is deliberately not a library: the CLI is `uv run archive backfill|update|validate|promote`. Adding a third collection (NAIP, Sentinel-1 GRD) should be a YAML file and a source adapter, not a new repo.

**Why developmentseed rather than stac-utils.** stac-utils is for community-maintained libraries and specs; an opinionated pipeline with our cloud credentials and Source Coop upload keys in it is operational code we own. If other orgs want to run it for their own archives, that is the moment to consider moving it.

**Writer direction.** Push rustac forward as the pipeline writer, with the Python stac-geoparquet library as a read-side compatibility check. Writer features for sort, row-group size, and bbox covering belong in rustac.

## **Phases**

| Phase | Work | Gate |
| --- | --- | --- |
| 0. Decide | Confirm sort, Sentinel-2 scope and source, and Source Cooperative account ownership. | Scope and source choice agreed with Pete. |
| 1. Spike | Run a one-month sample per collection with the local CLI. | Schema fixed; `gpio check all` passes; rustac and the Python library read the same items. |
| 2. Pipeline | Build resumable source crawlers, AWS queue/Lambda workers, validation, and GitHub Actions triggers. | Three-month sample passes; staging-to-promotion dry run succeeds. |
| 3. Backfill | Run the full archive and compare completeness with CDSE, inventories, and USGS bulk Parquet. | Per-month item counts meet the provider threshold. |
| 4. Publish and run | Promote to Source Cooperative and run scheduled updates. | Public query examples and update reporting work. |

The durations in the original sketch were estimates for one engineer. Re-estimate them after the spike and the queue/Lambda design.

## **Decisions and open questions**

The updated Google Doc records these directions:

- **Writer:** Push rustac forward; use the Python stac-geoparquet library to check read compatibility.
- **Sentinel-2 source:** Push on CDSE. Test token access, quotas, pagination, and coverage before deciding whether it can be the primary crawl or remains the completeness referee.
- **Mirrors:** Include as many verified provider and public mirror hrefs as practical, using `alternate` assets.
- **Landsat scope:** Start with L2 surface reflectance (SR) and surface temperature (ST). Leave L1 and ARD for later.
- **Execution:** Keep a CLI that works on a laptop. Run bulk work through AWS queues and Lambda workers, triggered by GitHub Actions.
- **Demo API:** Update `stac-fastapi-geoparquet` to discover files from Collection links when a server starts. A separate archive refresh for the demo API should not be required.

Still open:

- Sort by native grid then datetime, or datetime first? Confirm with the spike benchmarks.
- Include only `sentinel-2-c1-l2a` in v1, or also the pre-Collection-1 archive and L1C?
- Who will talk to Chris Holmes and Radiant Earth about a Source-owned publishing account, and when?
- Publish a separate slim (no-assets) variant, or defer it? The revised document removed the proposed slim layout, so it is not part of the current implementation plan.

## **Sources**

- [stac-utils/stac-geoparquet](https://github.com/stac-utils/stac-geoparquet) and the [STAC GeoParquet spec](https://stac-utils.github.io/stac-geoparquet/latest/spec/stac-geoparquet-spec)
- [stac-utils/rustac](https://github.com/stac-utils/rustac) and [rustac on PyPI](https://pypi.org/project/rustac)
- [stac-hash on PyPI](https://pypi.org/project/stac-hash/) (0.0.2, Aug 2026\) and [stac-spec discussion \#1378](https://github.com/radiantearth/stac-spec/discussions/1378) on a space-time hash extension
- [cholmes/stac-geoparquet-public on Source Cooperative](https://source.coop/cholmes/stac-geoparquet-public)
- [Planetary Computer: Bulk STAC item queries with GeoParquet](https://planetarycomputer.microsoft.com/docs/quickstarts/stac-geoparquet); PC issues [\#400](https://github.com/microsoft/PlanetaryComputer/issues/400), [\#437](https://github.com/microsoft/PlanetaryComputer/issues/437), [\#446](https://github.com/microsoft/PlanetaryComputer/issues/446)
- [HLS STAC GeoParquet Archive](https://maap-project.github.io/hls-stac-geoparquet-archive/) (NASA MAAP)
- [USGS Landsat Bulk Metadata Service](https://www.usgs.gov/landsat-missions/bulk-metadata-service), [Landsat Bulk Metadata Now Accessible in New Format](https://www.usgs.gov/landsat-missions/news/landsat-bulk-metadata-now-accessible-new-format), [Using the Landsat Bulk Metadata Parquet Files](https://www.usgs.gov/software/using-landsat-bulk-metadata-parquet-files), [USGS Landsat STAC](https://usgs.gov/landsat-missions/spatiotemporal-asset-catalog-stac)
- [Element84/earth-search README](https://github.com/Element84/earth-search) (collections, SNS topic, S3 inventory); [Sentinel-2 COGs on AWS Open Data](https://registry.opendata.aws/sentinel-2-l2a-cogs/)
- [CDSE STAC API docs](https://documentation.dataspace.copernicus.eu/APIs/STAC.html) and [CDSE forum: STAC missing S2 L2A since 2026-06-11](https://forum.dataspace.copernicus.eu/t/stac-catalogue-missing-sentinel-2-l2a-data-since-2026-06-11/5216)
- [geoparquet-io docs: spatial performance](https://geoparquet.org/geoparquet-io/concepts/spatial-indices), [best practices](https://geoparquet.org/geoparquet-io/concepts/best-practices), [Introducing geoparquet-io](https://cloudnativegeo.org/blog/2026/03/introducing-geoparquet-io/)
- [Source Cooperative docs](https://docs.source.coop/), [FOSS4G NA 2026 talk: Source Cooperative](https://talks.osgeo.org/foss4g-na-2026/talk/ESP7VZ/), [Source Cooperative Update: 1PB and Growing](https://radiant.earth/blog/2025/06/source-cooperative-update-1pb-and-growing/)
- [developmentseed/pgstac-geoparquet-exporter](https://github.com/developmentseed/pgstac-geoparquet-exporter), [DevSeed blog: stac-geoparquet](https://developmentseed.org/blog/2025-05-07-stac-geoparquet), [stac-map](https://developmentseed.org/blog/2025-09-02-stacmap/)
