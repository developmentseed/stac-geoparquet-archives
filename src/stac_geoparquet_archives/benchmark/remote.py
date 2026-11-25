"""Remote read benchmark against explicitly named object-store files."""

from __future__ import annotations

import argparse
import json
import resource
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import urlopen

from .cli import aggregate
from .config import load_config
from .queries import QUERIES, arrow_filter, duckdb_where, selected_files
from .remote_proxy import counting_proxy
from .runner import percentile


def measure_remote_once(
    upstream_origin: str,
    prefix: str,
    layout: str,
    engine: str,
    query_name: str,
    params: dict,
    max_bytes_read: int = 2_000_000_000,
) -> dict:
    import pyarrow as pa
    import pyarrow.parquet as pq

    query = next(item for item in QUERIES if item.name == query_name)
    root = f"{prefix.strip('/')}/{layout}"
    with counting_proxy(upstream_origin, root, max_bytes_read) as (proxy, stats):
        started = time.perf_counter()
        with urlopen(f"{proxy}/{root}/collection.json", timeout=120) as response:
            collection = json.load(response)
        with urlopen(f"{proxy}/{root}/_manifest.parquet", timeout=120) as response:
            manifest = pq.read_table(pa.BufferReader(response.read())).to_pylist()
        assets = {item["href"] for item in collection["assets"].values()}
        chosen = selected_files(manifest, query, params)
        if not set(chosen) <= assets:
            raise ValueError(
                "manifest contains a Parquet file absent from collection.json"
            )
        paths = [f"{root}/{path}" for path in chosen]
        catalog_seconds = time.perf_counter() - started
        if not paths:
            count = 0
        elif engine == "duckdb":
            import duckdb

            connection = duckdb.connect()
            connection.execute("LOAD httpfs")
            connection.execute("SET enable_external_file_cache = false")
            where, values = duckdb_where(query, params)
            urls = [f"{proxy}/{path}" for path in paths]
            row = connection.execute(
                "SELECT count(*) FROM read_parquet(?)" + where, [urls, *values]
            ).fetchone()
            assert row is not None
            count = row[0]
            connection.close()
        elif engine == "pyarrow":
            import pyarrow.dataset as ds
            from pyarrow import fs

            filesystem = fs.S3FileSystem(
                anonymous=True,
                scheme="http",
                endpoint_override=urlsplit(proxy).netloc,
                region="us-east-1",
                request_timeout=120,
                connect_timeout=30,
            )
            dataset = ds.dataset(paths, filesystem=filesystem, format="parquet")
            count = dataset.count_rows(filter=arrow_filter(query, params))
        else:
            raise ValueError(engine)
        seconds = time.perf_counter() - started
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform != "darwin":
            peak *= 1024
        return {
            "query": query_name,
            "rows": count,
            "seconds": seconds,
            "catalog_seconds": catalog_seconds,
            "query_seconds": seconds - catalog_seconds,
            "peak_memory_bytes": peak,
            "candidate_files": len(paths),
            **stats.snapshot(),
        }


def run_remote_engine(
    upstream_origin: str,
    prefix: str,
    layout: str,
    engine: str,
    params: dict,
    local_results: Path,
    output: Path,
    repeats: int,
    download_cap_bytes: int,
    max_bytes_per_run: int,
    resume: bool,
) -> Path:
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"{layout}-{engine}-run.json"
    if path.exists() and not resume:
        raise FileExistsError(f"remote results already exist: {path}; use --resume")
    reference = json.loads((local_results / f"{layout}-{engine}-run.json").read_text())
    expected_rows = {item["query"]: item["rows"] for item in reference["results"]}
    remote_path = f"{upstream_origin}/{prefix.strip('/')}/{layout}"
    if path.exists():
        previous = json.loads(path.read_text())
        if previous["dataset_path"] != remote_path or previous["repeats"] != repeats:
            raise ValueError(f"cannot resume with changed remote parameters: {path}")
        results = previous["results"]
    else:
        results = []
    downloaded = sum(
        read["bytes_read"]
        for run_path in output.glob("*-run.json")
        for result in json.loads(run_path.read_text())["results"]
        for read in result.get("individual_reads", [])
    )
    reference.update(
        {
            "dataset_path": remote_path,
            "timestamp": datetime.now(UTC).isoformat(),
            "repeats": repeats,
            "run_kind": "remote",
            "storage": "remote_http",
            "cache_method": "fresh process per repeat; local HTTP proxy; upstream CDN and server caches unknown",
            "results": results,
        }
    )
    for query in QUERIES:
        if any(item["query"] == query.name for item in results):
            continue
        runs = []
        for _ in range(repeats):
            if downloaded >= download_cap_bytes - max_bytes_per_run:
                raise RuntimeError(
                    f"remote reads reached {downloaded:,} bytes; ask before exceeding the download cap"
                )
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "stac_geoparquet_archives.benchmark.remote",
                    "once",
                    upstream_origin,
                    prefix,
                    layout,
                    engine,
                    query.name,
                    json.dumps(params),
                    "--max-bytes-read",
                    str(max_bytes_per_run),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            if completed.returncode:
                raise RuntimeError(
                    f"remote {layout}/{engine}/{query.name} failed:\n{completed.stderr}"
                )
            run = json.loads(completed.stdout)
            runs.append(run)
            downloaded += run["bytes_read"]
        counts = {run["rows"] for run in runs}
        expected = expected_rows[query.name]
        if counts != {expected}:
            raise ValueError(
                f"remote {layout}/{engine}/{query.name}: {counts} != {expected}"
            )
        times = [run["seconds"] for run in runs]
        results.append(
            {
                "query": query.name,
                "rows": expected,
                "best_seconds": min(times),
                "median_seconds": statistics.median(times),
                "p95_seconds": percentile(times, 0.95),
                "runs": times,
                "median_catalog_seconds": statistics.median(
                    run["catalog_seconds"] for run in runs
                ),
                "median_query_seconds": statistics.median(
                    run["query_seconds"] for run in runs
                ),
                "peak_memory_bytes": max(run["peak_memory_bytes"] for run in runs),
                "candidate_files": runs[0]["candidate_files"],
                "files_opened": max(run["files_opened"] for run in runs),
                "bytes_read": statistics.median(run["bytes_read"] for run in runs),
                "range_requests": statistics.median(
                    run["range_requests"] for run in runs
                ),
                "http_requests": statistics.median(
                    run["http_requests"] for run in runs
                ),
                "individual_reads": runs,
            }
        )
        reference["results"] = results
        path.write_text(json.dumps(reference, indent=2) + "\n")
        aggregate(output)
        print(f"{layout} {engine} {query.name} complete", file=sys.stderr, flush=True)
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    once = sub.add_parser("once")
    for name in ("upstream_origin", "prefix", "layout", "engine", "query", "params"):
        once.add_argument(name)
    once.add_argument("--max-bytes-read", type=int, default=2_000_000_000)
    bench = sub.add_parser("run")
    bench.add_argument("--upstream-origin", default="https://data.source.coop")
    bench.add_argument("--prefix", required=True)
    bench.add_argument("--layout", action="append", required=True)
    bench.add_argument("--engine", action="append", choices=("duckdb", "pyarrow"))
    bench.add_argument("--repeats", type=int, default=5)
    bench.add_argument("--download-cap-bytes", type=int, default=50_000_000_000)
    bench.add_argument("--max-bytes-per-run", type=int, default=2_000_000_000)
    bench.add_argument("--resume", action="store_true")
    bench.add_argument("--config", type=Path, default=Path("benchmark/sentinel2.json"))
    bench.add_argument(
        "--local-results",
        type=Path,
        default=Path("benchmark-results/sentinel-2-layout"),
    )
    bench.add_argument(
        "--results", type=Path, default=Path("benchmark-results/sentinel-2-remote")
    )
    args = parser.parse_args()
    if args.command == "once":
        print(
            json.dumps(
                measure_remote_once(
                    args.upstream_origin,
                    args.prefix,
                    args.layout,
                    args.engine,
                    args.query,
                    json.loads(args.params),
                    args.max_bytes_read,
                )
            )
        )
        return
    if args.download_cap_bytes <= args.max_bytes_per_run:
        raise ValueError("download cap must exceed the per-run byte cap")
    params = load_config(args.config)["query"]
    for layout in args.layout:
        for engine in args.engine or ("duckdb", "pyarrow"):
            path = run_remote_engine(
                args.upstream_origin,
                args.prefix,
                layout,
                engine,
                params,
                args.local_results,
                args.results,
                args.repeats,
                args.download_cap_bytes,
                args.max_bytes_per_run,
                args.resume,
            )
            print(f"wrote {path}", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
