"""Build one benchmark layout at a time from immutable monthly source files."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

from .config import Layout

MONTH_RE = re.compile(r"_(20\d\d)-(\d\d)-")
COLLECTION = "sentinel-2-l2a"


def source_months(source_dir: Path) -> dict[str, Path]:
    files = {}
    for path in sorted(source_dir.glob("*.parquet")):
        match = MONTH_RE.search(path.name)
        if match:
            key = f"{match[1]}-{match[2]}"
            if key in files:
                raise ValueError(f"duplicate source month: {key}")
            files[key] = path
    if not files:
        raise ValueError(f"no monthly Parquet files in {source_dir}")
    return files


def period(month: str, granularity: str) -> str:
    year, number = month.split("-")
    if granularity == "month":
        return month
    if granularity == "quarter":
        return f"{year}-Q{(int(number) - 1) // 3 + 1}"
    if granularity == "year":
        return year
    return "all"


def relative_path(key: str, granularity: str) -> Path:
    if granularity == "month":
        year, month = key.split("-")
        return Path(f"year={year}/month={month}/{COLLECTION}-{key}.parquet")
    if granularity == "quarter":
        year, quarter = key.split("-")
        return Path(f"year={year}/quarter={quarter}/{COLLECTION}-{key}.parquet")
    if granularity == "year":
        return Path(f"year={key}/{COLLECTION}-{key}.parquet")
    return Path(f"{COLLECTION}-all.parquet")


def groups(months: dict[str, Path], granularity: str) -> dict[str, list[Path]]:
    result = defaultdict(list)
    for month, path in sorted(months.items()):
        result[period(month, granularity)].append(path)
    return dict(result)


def sort_sql(sort: str) -> str:
    if sort == "tile":
        return '"s2:mgrs_tile" NULLS LAST, datetime, id'
    if sort == "datetime":
        return "datetime, id"
    return (
        "ST_Hilbert(ST_Point((bbox.xmin + bbox.xmax) / 2, "
        "(bbox.ymin + bbox.ymax) / 2), "
        "ST_Extent(ST_MakeEnvelope(-180, -90, 180, 90))), datetime, id"
    )


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_file(
    paths: list[Path], output: Path, layout: Layout, settings: dict, schema
) -> dict:
    """Stream a DuckDB external sort into exact-sized PyArrow row groups."""
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq

    output.parent.mkdir(parents=True, exist_ok=True)
    geo = json.loads(schema.metadata[b"geo"])
    if (
        geo.get("version") != "1.1.0"
        or "bbox" not in geo["columns"]["geometry"]["covering"]
    ):
        raise ValueError("source lacks GeoParquet 1.1 bbox covering")
    con = duckdb.connect()
    large_period = layout.granularity in {"year", "unpartitioned"}
    sort_memory_limit = "16GB" if large_period else "8GB"
    sort_threads = 1 if large_period else 2
    con.execute(f"SET memory_limit = '{sort_memory_limit}'")
    con.execute("SET max_temp_directory_size = '30GB'")
    con.execute(f"SET threads = {sort_threads}")
    con.execute("SET preserve_insertion_order = false")
    con.execute(
        f"SET temp_directory = '{str(output.parent / 'duckdb-tmp').replace(chr(39), chr(39) * 2)}'"
    )
    if layout.sort == "hilbert":
        con.execute("LOAD spatial")
    query = (
        "SELECT * FROM read_parquet(?, union_by_name = true) "
        f"ORDER BY {sort_sql(layout.sort)}"
    )
    reader = con.sql(query, params=[[str(p) for p in paths]]).to_arrow_reader(
        batch_size=65536
    )
    temporary = output.with_suffix(".building")
    writer = None
    pending = []
    pending_rows = 0
    row_group = layout.row_group_size
    try:
        for batch in reader:
            table = pa.Table.from_batches([batch])
            for field in schema:
                if field.name not in table.schema.names:
                    table = table.append_column(
                        field.name, pa.nulls(table.num_rows, type=field.type)
                    )
            aligned = table.select(schema.names).cast(schema)
            batch = aligned.to_batches()[0]
            if writer is None:
                writer = pq.ParquetWriter(
                    temporary,
                    schema,
                    compression=settings["compression"],
                    compression_level=settings["compression_level"],
                    version=settings["parquet_version"],
                    data_page_version=settings["data_page_version"],
                    write_page_index=settings["write_page_index"],
                    write_statistics=settings["write_statistics"],
                    use_dictionary=settings["use_dictionary"],
                )
            offset = 0
            while offset < batch.num_rows:
                take = min(row_group - pending_rows, batch.num_rows - offset)
                pending.append(batch.slice(offset, take))
                pending_rows += take
                offset += take
                if pending_rows == row_group:
                    table = pa.Table.from_batches(pending, schema=writer.schema)
                    writer.write_table(table, row_group_size=row_group)
                    pending.clear()
                    pending_rows = 0
        if writer is None:
            raise ValueError("empty period")
        if pending:
            writer.write_table(
                pa.Table.from_batches(pending, schema=writer.schema),
                row_group_size=row_group,
            )
        writer.close()
        writer = None
        os.replace(temporary, output)
    finally:
        if writer is not None:
            writer.close()
        temporary.unlink(missing_ok=True)
        con.close()

    return inspect_file(output)


def inspect_file(output: Path) -> dict:
    """Recover metadata from a completed file when resuming a layout."""
    import duckdb
    import pyarrow.parquet as pq

    metadata = pq.read_metadata(output)
    scan = duckdb.connect()
    bounds = scan.execute(
        "SELECT min(datetime), max(datetime), min(bbox.xmin), min(bbox.ymin), "
        "max(bbox.xmax), max(bbox.ymax) FROM read_parquet(?)",
        [str(output)],
    ).fetchone()
    scan.close()
    assert bounds is not None
    return {
        "path": str(output),
        "item_count": metadata.num_rows,
        "row_groups": metadata.num_row_groups,
        "size_bytes": output.stat().st_size,
        "min_datetime": bounds[0].astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "max_datetime": bounds[1].astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "bbox": list(bounds[2:]),
        "sha256": sha256(output),
    }


def write_indexes(root: Path, entries: dict[str, dict], layout: Layout) -> dict:
    import pyarrow as pa
    import pyarrow.parquet as pq

    ordered = [entries[key] for key in sorted(entries)]
    assets = {}
    for key in sorted(entries):
        item = entries[key]
        assets[f"geoparquet-items-{key}"] = {
            "href": str(Path(item["path"]).relative_to(root)),
            "type": "application/vnd.apache.parquet",
            "roles": ["data"],
        }
    collection = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": COLLECTION,
        "description": "Sentinel-2 L2A layout benchmark output",
        "license": "proprietary",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {
                "interval": [
                    [
                        min(e["min_datetime"] for e in ordered),
                        max(e["max_datetime"] for e in ordered),
                    ]
                ]
            },
        },
        "links": [],
        "assets": assets,
    }
    (root / "collection.json").write_text(json.dumps(collection, indent=2) + "\n")
    records = [
        {
            "path": str(Path(e["path"]).relative_to(root)),
            "item_count": e["item_count"],
            "xmin": e["bbox"][0],
            "ymin": e["bbox"][1],
            "xmax": e["bbox"][2],
            "ymax": e["bbox"][3],
            "min_datetime": e["min_datetime"],
            "max_datetime": e["max_datetime"],
            "sha256": e["sha256"],
        }
        for e in ordered
    ]
    pq.write_table(
        pa.Table.from_pylist(records), root / "_manifest.parquet", compression="zstd"
    )
    return {
        "file_count": len(ordered),
        "collection_json_bytes": (root / "collection.json").stat().st_size,
        "manifest_bytes": (root / "_manifest.parquet").stat().st_size,
    }


def build_layout(
    source_dir: Path,
    root: Path,
    layout: Layout,
    settings: dict,
    *,
    append_month: str | None = None,
    resume: bool = False,
) -> dict:
    import pyarrow as pa
    import pyarrow.parquet as pq

    months = source_months(source_dir)
    schema = pa.unify_schemas(
        [pq.read_schema(path) for path in months.values()],
        promote_options="permissive",
    )
    if root.exists() and any(root.iterdir()) and not resume:
        raise FileExistsError(f"layout output is not empty: {root}")
    if resume and (root / "layout.json").exists():
        raise FileExistsError(f"layout already completed: {root}")
    root.mkdir(parents=True, exist_ok=True)
    scratch = root / "_chunks"

    def build_group(paths: list[Path], target: Path) -> dict:
        if layout.granularity not in {"year", "unpartitioned"} or len(paths) == 1:
            return write_file(paths, target, layout, settings, schema)
        from .merge import merge_sorted_files

        sorted_inputs = []
        for source in paths:
            chunk = scratch / source.name
            if not chunk.exists():
                write_file([source], chunk, layout, settings, schema)
            sorted_inputs.append(chunk)
        merge_sorted_files(
            sorted_inputs, target, schema, settings, layout.row_group_size
        )
        return inspect_file(target)

    initial = {month: path for month, path in months.items() if month != append_month}
    entries = {}
    for key, paths in groups(initial, layout.granularity).items():
        target = root / relative_path(key, layout.granularity)
        target.parent.mkdir(parents=True, exist_ok=True)
        if resume and target.exists():
            expected = sum(pq.read_metadata(path).num_rows for path in paths)
            if pq.read_metadata(target).num_rows != expected:
                raise ValueError(f"unsafe resume: row count differs for {target}")
            entries[key] = inspect_file(target)
        else:
            entries[key] = build_group(paths, target)
    before = write_indexes(root, entries, layout)
    append = None
    if append_month:
        if append_month not in months:
            raise ValueError(f"missing append month {append_month}")
        key = period(append_month, layout.granularity)
        old_size = entries[key]["size_bytes"] if key in entries else 0
        paths = groups(months, layout.granularity)[key]
        target = root / relative_path(key, layout.granularity)
        staging_before = sum(path.stat().st_size for path in scratch.glob("*.parquet"))
        entries[key] = build_group(paths, target)
        staging_after = sum(path.stat().st_size for path in scratch.glob("*.parquet"))
        after = write_indexes(root, entries, layout)
        append = {
            "month": append_month,
            "parquet_bytes_written": entries[key]["size_bytes"],
            "parquet_files_rewritten": int(old_size > 0),
            "previous_file_bytes_rewritten": old_size,
            "staging_chunk_bytes_written": staging_after - staging_before,
            "metadata_files_rewritten": 2,
            "metadata_bytes_written": after["collection_json_bytes"]
            + after["manifest_bytes"],
            "total_files_rewritten": int(old_size > 0) + 2,
            "total_bytes_written": entries[key]["size_bytes"]
            + after["collection_json_bytes"]
            + after["manifest_bytes"],
            "before": before,
            "after": after,
        }
    expected_rows = sum(pq.read_metadata(path).num_rows for path in months.values())
    actual_rows = sum(entry["item_count"] for entry in entries.values())
    if actual_rows != expected_rows:
        raise ValueError(
            f"layout row count {actual_rows} differs from source {expected_rows}"
        )
    for entry in entries.values():
        metadata = pq.read_metadata(entry["path"])
        geo = json.loads(metadata.metadata[b"geo"])
        if (
            geo.get("version") != "1.1.0"
            or "bbox" not in geo["columns"]["geometry"]["covering"]
        ):
            raise ValueError(f"GeoParquet bbox covering missing: {entry['path']}")
        if any(
            metadata.row_group(i).num_rows > layout.row_group_size
            for i in range(metadata.num_row_groups)
        ):
            raise ValueError(f"row group exceeds requested size: {entry['path']}")
    payload = {
        "layout": vars(layout),
        "writer": {
            **{
                k: settings[k]
                for k in (
                    "compression",
                    "compression_level",
                    "parquet_version",
                    "data_page_version",
                    "write_page_index",
                    "write_statistics",
                    "use_dictionary",
                )
            },
            "pyarrow_version": __import__("pyarrow").__version__,
            "duckdb_version": __import__("duckdb").__version__,
            "sort_memory_limit": (
                "16GB" if layout.granularity in {"year", "unpartitioned"} else "8GB"
            ),
            "sort_temp_limit": "30GB",
            "sort_threads": 1 if layout.granularity in {"year", "unpartitioned"} else 2,
        },
        "generated_at": datetime.now(UTC).isoformat(),
        "source_files": [str(p) for p in months.values()],
        "files": list(entries.values()),
        "append": append,
    }
    if scratch.exists():
        shutil.rmtree(scratch)
    (root / "layout.json").write_text(json.dumps(payload, indent=2) + "\n")
    return payload
