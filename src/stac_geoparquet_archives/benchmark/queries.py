"""Fixed STAC search predicates, adapted from cosgp's benchmark queries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Query:
    name: str
    start: str | None = None
    end: str | None = None
    bbox: bool = False
    tile: bool = False
    cloud: bool = False


QUERIES = (
    Query("bbox_week", "week_start", "week_end", bbox=True),
    Query("bbox_year", "year_start", "year_end", bbox=True),
    Query("tile_timeseries", tile=True),
    Query("bbox_month_cloud", "month_start", "month_end", bbox=True, cloud=True),
    Query("global_day", "day_start", "day_end"),
    Query("full_collection_count"),
)


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value)


def selected_files(manifest: list[dict], query: Query, params: dict) -> list[str]:
    """Use the manifest's date bounds; no bucket LIST is needed."""
    if not query.start:
        return [entry["path"] for entry in manifest]
    start = parse_time(params[query.start])
    end = parse_time(params[query.end])
    return [
        entry["path"]
        for entry in manifest
        if parse_time(entry["max_datetime"]) >= start
        and parse_time(entry["min_datetime"]) < end
    ]


def duckdb_where(query: Query, params: dict) -> tuple[str, list]:
    clauses = []
    values = []
    if query.start:
        clauses.extend(("datetime >= ?", "datetime < ?"))
        values.extend((parse_time(params[query.start]), parse_time(params[query.end])))
    if query.bbox:
        xmin, ymin, xmax, ymax = params["bbox"]
        clauses.extend(
            ("bbox.xmax >= ?", "bbox.xmin <= ?", "bbox.ymax >= ?", "bbox.ymin <= ?")
        )
        values.extend((xmin, xmax, ymin, ymax))
    if query.tile:
        clauses.append('"s2:mgrs_tile" = ?')
        values.append(params["tile"])
    if query.cloud:
        clauses.append('"eo:cloud_cover" < ?')
        values.append(params["max_cloud_cover"])
    return (" WHERE " + " AND ".join(clauses) if clauses else ""), values


def arrow_filter(query: Query, params: dict):
    import pyarrow.dataset as ds

    predicate = None

    def add(term):
        nonlocal predicate
        predicate = term if predicate is None else predicate & term

    if query.start:
        add(ds.field("datetime") >= parse_time(params[query.start]))
        add(ds.field("datetime") < parse_time(params[query.end]))
    if query.bbox:
        xmin, ymin, xmax, ymax = params["bbox"]
        add(ds.field("bbox", "xmax") >= xmin)
        add(ds.field("bbox", "xmin") <= xmax)
        add(ds.field("bbox", "ymax") >= ymin)
        add(ds.field("bbox", "ymin") <= ymax)
    if query.tile:
        add(ds.field("s2:mgrs_tile") == params["tile"])
    if query.cloud:
        add(ds.field("eo:cloud_cover") < params["max_cloud_cover"])
    return predicate
