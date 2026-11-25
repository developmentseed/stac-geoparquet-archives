"""Local, fresh-process benchmark runner for DuckDB and PyArrow."""

from __future__ import annotations

import json
import resource
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from .queries import QUERIES, arrow_filter, duckdb_where, selected_files


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = fraction * (len(ordered) - 1)
    lo = int(position)
    return ordered[lo] + (ordered[min(lo + 1, len(ordered) - 1)] - ordered[lo]) * (
        position - lo
    )


def measure_once(layout_dir: Path, engine: str, query_name: str, params: dict) -> dict:
    import pyarrow.parquet as pq

    query = next(q for q in QUERIES if q.name == query_name)
    index = pq.read_table(layout_dir / "_manifest.parquet").to_pylist()
    selected = selected_files(index, query, params)
    paths = [str(layout_dir / path) for path in selected]
    started = time.perf_counter()
    if not paths:
        count = 0
    elif engine == "duckdb":
        import duckdb

        con = duckdb.connect()
        con.execute("SET enable_external_file_cache = false")
        where, values = duckdb_where(query, params)
        row = con.execute(
            "SELECT count(*) FROM read_parquet(?)" + where, [paths, *values]
        ).fetchone()
        assert row is not None
        count = row[0]
        con.close()
    elif engine == "pyarrow":
        import pyarrow.dataset as ds

        dataset = ds.dataset(paths, format="parquet")
        count = dataset.count_rows(filter=arrow_filter(query, params))
    else:
        raise ValueError(engine)
    elapsed = time.perf_counter() - started
    # On macOS ru_maxrss is bytes; on Linux it is KiB.
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform != "darwin":
        peak *= 1024
    return {
        "query": query_name,
        "rows": count,
        "seconds": elapsed,
        "peak_memory_bytes": peak,
        "candidate_files": len(paths),
        "files_opened": None,
        "bytes_read": None,
        "range_requests": None,
    }


def run_engine(
    layout_dir: Path,
    engine: str,
    params: dict,
    output_dir: Path,
    repeats: int = 5,
    query_names: list[str] | None = None,
    run_kind: str = "full",
) -> Path:
    layout = json.loads((layout_dir / "layout.json").read_text())
    names = query_names or [q.name for q in QUERIES]
    results = []
    for name in names:
        runs = []
        for _ in range(repeats):
            command = [
                sys.executable,
                "-m",
                "stac_geoparquet_archives.benchmark.cli",
                "once",
                str(layout_dir),
                engine,
                name,
                json.dumps(params),
            ]
            completed = subprocess.run(
                command, check=True, capture_output=True, text=True
            )
            runs.append(json.loads(completed.stdout))
        counts = {run["rows"] for run in runs}
        if len(counts) != 1:
            raise ValueError(f"inconsistent {name} counts: {counts}")
        timings = [run["seconds"] for run in runs]
        results.append(
            {
                "query": name,
                "rows": runs[0]["rows"],
                "best_seconds": min(timings),
                "median_seconds": statistics.median(timings),
                "p95_seconds": percentile(timings, 0.95),
                "runs": timings,
                "peak_memory_bytes": max(run["peak_memory_bytes"] for run in runs),
                "candidate_files": runs[0]["candidate_files"],
                "files_opened": None,
                "bytes_read": None,
                "range_requests": None,
            }
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    run_file = output_dir / f"{layout['layout']['id']}-{engine}-run.json"
    payload = {
        "dataset_name": layout["layout"]["id"],
        "dataset_path": str(layout_dir),
        "timestamp": datetime.now(UTC).isoformat(),
        "engine": engine,
        "engine_version": __import__(engine).__version__,
        "repeats": repeats,
        "suite": "sentinel2_layout",
        "run_kind": run_kind,
        "params": params,
        "layout": layout["layout"],
        "source_files": layout["source_files"],
        "source_item_count": sum(item["item_count"] for item in layout["files"]),
        "writer": layout["writer"],
        "files": layout["files"],
        "storage": "local",
        "cache_method": "fresh process per repeat; OS file cache not cleared",
        "results": results,
        "append": layout["append"],
    }
    run_file.write_text(json.dumps(payload, indent=2) + "\n")
    return run_file
