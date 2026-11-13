# stac-geoparquet-archives

[![CI](https://img.shields.io/github/actions/workflow/status/developmentseed/stac-geoparquet-archives/ci.yml?branch=main&style=for-the-badge&label=CI)](https://github.com/developmentseed/stac-geoparquet-archives/actions/workflows/ci.yml)

stac-geoparquet catalogs of public geospatial datasets, hosted on source.coop.

The STAC catalog and collections live in [catalog/](./catalog/catalog.json).

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
