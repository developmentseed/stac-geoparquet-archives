"""Small end-to-end checks of the sequential layout experiment."""

from __future__ import annotations

import hashlib
import json
import struct
from datetime import UTC, datetime
from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
pytest.importorskip("duckdb")

from stac_geoparquet_archives.benchmark.config import Layout
from stac_geoparquet_archives.benchmark.layout import build_layout
from stac_geoparquet_archives.benchmark.runner import measure_once


def source_file(path: Path, month: int) -> None:
    bbox_type = pa.struct(
        [(name, pa.float64()) for name in ("xmin", "ymin", "xmax", "ymax")]
    )
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": [],
                "covering": {
                    "bbox": {
                        name: ["bbox", name]
                        for name in ("xmin", "ymin", "xmax", "ymax")
                    }
                },
            }
        },
    }
    fields = [
        ("id", pa.string()),
        ("geometry", pa.binary()),
        ("bbox", bbox_type),
        ("datetime", pa.timestamp("us", tz="UTC")),
        ("s2:mgrs_tile", pa.string()),
        ("collection", pa.string()),
    ]
    if month == 11:
        fields.append(("eo:cloud_cover", pa.float64()))
    schema = pa.schema(fields, metadata={b"geo": json.dumps(geo).encode()})
    rows = [
        {
            "id": f"{month}-{i}",
            "geometry": struct.pack("<BIdd", 1, 1, -105.5, 39.5),
            "bbox": {"xmin": -106.0, "ymin": 39.0, "xmax": -105.0, "ymax": 40.0},
            "datetime": datetime(2024, month, min(i + 1, 28), tzinfo=UTC),
            "s2:mgrs_tile": "13TCF" if i % 2 == 0 else "13TCE",
            "eo:cloud_cover": float(i * 10),
            "collection": "sentinel-2-l2a",
        }
        for i in range(7)
    ]
    rows.reverse()
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


@pytest.mark.parametrize(
    "granularity,rewritten",
    [("month", 0), ("quarter", 1), ("year", 1), ("unpartitioned", 1)],
)
def test_build_append_and_query(
    tmp_path: Path, granularity: str, rewritten: int
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    for month in (11, 12):
        source_file(source / f"part-0000_2024-{month:02d}-01.parquet", month)
    before = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in source.glob("*.parquet")
    }
    output = tmp_path / "layout"
    layout = Layout("test", granularity, 3, "tile")
    settings = {
        "compression": "zstd",
        "compression_level": 3,
        "parquet_version": "2.6",
        "data_page_version": "1.0",
        "write_page_index": False,
        "write_statistics": True,
        "use_dictionary": True,
    }
    result = build_layout(source, output, layout, settings, append_month="2024-12")
    assert result["append"]["parquet_files_rewritten"] == rewritten
    assert result["append"]["parquet_bytes_written"] > 0
    assert sum(entry["item_count"] for entry in result["files"]) == 14
    assert len({str(pq.read_schema(entry["path"])) for entry in result["files"]}) == 1
    assert len(json.loads((output / "collection.json").read_text())["assets"]) == len(
        result["files"]
    )
    for entry in result["files"]:
        metadata = pq.read_metadata(entry["path"])
        assert json.loads(metadata.metadata[b"geo"])["version"] == "1.1.0"
        assert all(
            metadata.row_group(i).num_rows <= 3 for i in range(metadata.num_row_groups)
        )
        bbox_stats = [
            metadata.row_group(0).column(i).statistics
            for i in range(metadata.row_group(0).num_columns)
            if metadata.row_group(0).column(i).path_in_schema.startswith("bbox.")
        ]
        assert len(bbox_stats) == 4 and all(stat is not None for stat in bbox_stats)
        sorted_rows = pq.read_table(
            entry["path"], columns=["s2:mgrs_tile", "datetime", "id"]
        ).to_pylist()
        assert sorted_rows == sorted(
            sorted_rows,
            key=lambda row: (row["s2:mgrs_tile"], row["datetime"], row["id"]),
        )
    params = {"tile": "13TCF"}
    for engine in ("duckdb", "pyarrow"):
        assert measure_once(output, engine, "tile_timeseries", params)["rows"] == 8
    after = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in source.glob("*.parquet")
    }
    assert before == after
