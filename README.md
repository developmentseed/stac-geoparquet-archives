# stac-geoparquet-archives

[![CI](https://img.shields.io/github/actions/workflow/status/developmentseed/stac-geoparquet-archives/ci.yml?branch=main&style=for-the-badge&label=CI)](https://github.com/developmentseed/stac-geoparquet-archives/actions/workflows/ci.yml)

stac-geoparquet catalogs of public geospatial datasets, hosted on source.coop.

The STAC catalog and collections live in [catalog/](./catalog/catalog.json).

## Sentinel-2 layout benchmark

The [benchmark plan](docs/sentinel-2-layout-benchmark.md) records the eight
first-pass layouts, the source inventory, fixed queries, and measurement limits.
The [remote follow-up](docs/sentinel-2-remote-benchmark.md) reports results for
five layouts, the upload command, and remote I/O instrumentation.
The generator uses one layout directory at a time and deletes that generated
directory after both engines finish. It never deletes the source snapshot.

```shell
uv sync --extra benchmark
uv run --extra benchmark python -m stac_geoparquet_archives.benchmark.source \
  --month 2024-06
uv run --extra benchmark python -m stac_geoparquet_archives.benchmark.cli run \
  --source data/benchmark-source --smoke

# After the smoke run, retrieve all 24 source files and run eight layouts:
uv run --extra benchmark python -m stac_geoparquet_archives.benchmark.source
uv run --extra benchmark python -m stac_geoparquet_archives.benchmark.cli run \
  --source data/benchmark-source
```

The Hilbert layout also needs DuckDB's spatial extension: run
`uv run --extra benchmark python -c 'import duckdb; duckdb.connect().execute("INSTALL spatial")'`
once before the full run. Results are written to
`benchmark-results/sentinel-2-layout/` as `run.json`-style files per layout and
engine plus `results.csv`. The local runner uses fresh processes between
repeats, but does not clear the OS file cache. Remote bytes and request counts
are therefore left empty rather than inferred from local reads.
The smoke run writes to `benchmark-results/sentinel-2-smoke/`; the two-year run
writes to `benchmark-results/sentinel-2-layout/`.

## Contributing

We use [uv](https://docs.astral.sh/uv/) to manage the project.
To get started:

```shell
git clone https://github.com/developmentseed/stac-geoparquet-archives
cd stac-geoparquet-archives
uv sync
```

Before opening a pull request, make sure the same checks that run in CI pass locally:

```shell
scripts/lint
uv run pytest
```

Use `scripts/format` to fix any formatting issues.
Linting and formatting JSON and Markdown uses [Prettier](https://prettier.io/) via `npx`, so you'll need [Node.js](https://nodejs.org/) (the version is in `.nvmrc`).

## License

[MIT](./LICENSE)
