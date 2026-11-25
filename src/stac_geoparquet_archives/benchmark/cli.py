"""Command-line entry points for the sequential layout experiment."""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import sys
from pathlib import Path

from .config import load_config
from .layout import build_layout, source_months
from .runner import measure_once, run_engine


def aggregate(results_dir: Path) -> Path:
    rows = []
    counts: dict[str, set[int]] = {}
    for path in sorted(results_dir.glob("*-run.json")):
        run = json.loads(path.read_text())
        for result in run["results"]:
            counts.setdefault(result["query"], set()).add(result["rows"])
            rows.append(
                {
                    "layout": run["layout"]["id"],
                    "granularity": run["layout"]["granularity"],
                    "row_group_size": run["layout"]["row_group_size"],
                    "sort": run["layout"]["sort"],
                    "query": result["query"],
                    "engine": run["engine"],
                    "rows": result["rows"],
                    "best_seconds": result["best_seconds"],
                    "median_seconds": result["median_seconds"],
                    "p95_seconds": result["p95_seconds"],
                    "peak_memory_bytes": result["peak_memory_bytes"],
                    "candidate_files": result["candidate_files"],
                    "files_opened": result["files_opened"],
                    "bytes_read": result["bytes_read"],
                    "range_requests": result["range_requests"],
                    "storage": run["storage"],
                }
            )
    mismatched = {name: values for name, values in counts.items() if len(values) != 1}
    if mismatched:
        raise ValueError(f"query result counts differ between layouts: {mismatched}")
    output = results_dir / "results.csv"
    if rows:
        with output.open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    one = sub.add_parser("once")
    one.add_argument("layout_dir", type=Path)
    one.add_argument("engine", choices=["duckdb", "pyarrow"])
    one.add_argument("query")
    one.add_argument("params")
    bench = sub.add_parser("run")
    bench.add_argument("--config", type=Path, default=Path("benchmark/sentinel2.json"))
    bench.add_argument("--source", type=Path, required=True)
    bench.add_argument("--work", type=Path, default=Path("data/benchmark-work"))
    bench.add_argument(
        "--results", type=Path, default=Path("benchmark-results/sentinel-2-layout")
    )
    bench.add_argument("--layout", action="append")
    bench.add_argument("--repeats", type=int, default=5)
    bench.add_argument("--smoke", action="store_true")
    bench.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    if args.command == "once":
        print(
            json.dumps(
                measure_once(
                    args.layout_dir, args.engine, args.query, json.loads(args.params)
                )
            )
        )
        return
    config = load_config(args.config)
    available_months = set(source_months(args.source))
    expected_months = {
        f"{year}-{month:02d}" for year in config["years"] for month in range(1, 13)
    }
    if args.smoke:
        if available_months != {"2024-06"}:
            raise ValueError("smoke source must contain only 2024-06")
    elif available_months != expected_months:
        raise ValueError(
            f"full run requires exactly {len(expected_months)} source months; "
            f"found {len(available_months)}"
        )
    if args.smoke and args.results == Path("benchmark-results/sentinel-2-layout"):
        args.results = Path("benchmark-results/sentinel-2-smoke")
    selected = args.layout or (
        ["A", "D", "H"] if args.smoke else [layout.id for layout in config["layouts"]]
    )
    queries = ["bbox_week", "tile_timeseries", "global_day"] if args.smoke else None
    for layout in config["layouts"]:
        if layout.id not in selected:
            continue
        target = args.work / layout.id
        if target.exists() and any(target.iterdir()) and not args.resume:
            raise FileExistsError(f"refusing to replace existing output: {target}")
        free = shutil.disk_usage(
            args.work.parent if args.work.parent.exists() else Path(".")
        ).free
        source_bytes = sum(p.stat().st_size for p in args.source.glob("*.parquet"))
        # The source itself is kept separately. A layout build needs room for
        # sorted chunks plus the pre-append and replacement output files.
        if free < source_bytes * 1.25:
            raise RuntimeError(
                f"insufficient free space for layout {layout.id}: {free} bytes"
            )
        print(f"building {layout.id}", file=sys.stderr, flush=True)
        build_layout(
            args.source,
            target,
            layout,
            config,
            append_month=None if args.smoke else "2024-12",
            resume=args.resume,
        )
        for engine in ("duckdb", "pyarrow"):
            path = run_engine(
                target,
                engine,
                config["query"],
                args.results,
                repeats=args.repeats,
                query_names=queries,
                run_kind="smoke" if args.smoke else "full",
            )
            print(f"wrote {path}", file=sys.stderr, flush=True)
        # Deletion is limited to this generated layout directory; the source
        # path is never passed to rmtree.
        shutil.rmtree(target)
        aggregate(args.results)


if __name__ == "__main__":
    main()
