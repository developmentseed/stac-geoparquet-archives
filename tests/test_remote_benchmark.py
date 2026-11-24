"""Exercise the remote reader through a range-capable object-store stand-in."""

from __future__ import annotations

import json
import subprocess
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from stac_geoparquet_archives.benchmark import remote, remote_publish
from stac_geoparquet_archives.benchmark.queries import Query
from stac_geoparquet_archives.benchmark.remote import measure_remote_once


@pytest.mark.parametrize("engine", ["duckdb", "pyarrow"])
def test_remote_reader_counts_actual_range_io(tmp_path: Path, engine: str) -> None:
    if engine == "duckdb":
        duckdb = pytest.importorskip("duckdb")
        try:
            duckdb.connect().execute("LOAD httpfs")
        except duckdb.Error:
            pytest.skip("DuckDB httpfs extension is not installed")
    root = tmp_path / "bucket" / "benchmark" / "A"
    data = root / "year=2024" / "month=06" / "sentinel-2-l2a-2024-06.parquet"
    data.parent.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "id": ["one", "two"],
                "s2:mgrs_tile": ["13TCF", "13TCE"],
                "datetime": [datetime(2024, 6, 1, tzinfo=UTC)] * 2,
            }
        ),
        data,
    )
    relative = str(data.relative_to(root))
    (root / "collection.json").write_text(
        json.dumps({"assets": {"geoparquet-items-2024-06": {"href": relative}}})
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "path": relative,
                    "min_datetime": "2024-06-01T00:00:00Z",
                    "max_datetime": "2024-06-01T00:00:00Z",
                }
            ]
        ),
        root / "_manifest.parquet",
    )

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            pass

        def do_HEAD(self) -> None:
            self.serve_file()

        def do_GET(self) -> None:
            self.serve_file()

        def serve_file(self) -> None:
            path = tmp_path / unquote(urlsplit(self.path).path).lstrip("/")
            if not path.is_file():
                self.send_error(404)
                return
            size = path.stat().st_size
            start, end = 0, size - 1
            requested = self.headers.get("Range")
            if requested:
                first, last = requested.removeprefix("bytes=").split("-", 1)
                start = int(first)
                end = int(last) if last else end
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            else:
                self.send_response(200)
            self.send_header("Content-Length", str(end - start + 1))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            if self.command == "GET":
                with path.open("rb") as stream:
                    stream.seek(start)
                    self.wfile.write(stream.read(end - start + 1))

    try:
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    except PermissionError:
        pytest.skip("loopback sockets are disabled in this sandbox")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        result = measure_remote_once(
            f"http://127.0.0.1:{server.server_port}",
            "bucket/benchmark",
            "A",
            engine,
            "tile_timeseries",
            {"tile": "13TCF"},
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    assert result["rows"] == 1
    assert result["candidate_files"] == result["files_opened"] == 1
    assert result["range_requests"] > 0
    assert result["bytes_read"] > 0


def test_remote_runner_preserves_local_row_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    local = tmp_path / "local"
    local.mkdir()
    (local / "A-duckdb-run.json").write_text(
        json.dumps(
            {"results": [{"query": "first", "rows": 1}, {"query": "second", "rows": 2}]}
        )
    )
    monkeypatch.setattr(remote, "QUERIES", (Query("first"), Query("second")))
    monkeypatch.setattr(remote, "aggregate", lambda output: None)

    def fake_run(arguments: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        query = arguments[8]
        result = {
            "query": query,
            "rows": 1 if query == "first" else 2,
            "seconds": 1.0,
            "catalog_seconds": 0.2,
            "query_seconds": 0.8,
            "peak_memory_bytes": 100,
            "candidate_files": 1,
            "files_opened": 1,
            "bytes_read": 20,
            "range_requests": 1,
            "http_requests": 2,
        }
        return subprocess.CompletedProcess(arguments, 0, json.dumps(result), "")

    monkeypatch.setattr(remote.subprocess, "run", fake_run)
    output = tmp_path / "remote"
    path = remote.run_remote_engine(
        "https://example.com",
        "bucket/prefix",
        "A",
        "duckdb",
        {},
        local,
        output,
        1,
        1000,
        100,
        False,
    )
    saved = json.loads(path.read_text())
    assert [item["rows"] for item in saved["results"]] == [1, 2]


def test_remote_publish_retries_transient_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    botocore = pytest.importorskip("botocore.exceptions")
    monkeypatch.setattr(remote_publish.time, "sleep", lambda seconds: None)

    class Client:
        calls = 0

        def head_object(self, **_: str) -> dict[str, int]:
            self.calls += 1
            if self.calls == 1:
                raise botocore.ClientError(
                    {"Error": {"Code": "520", "Message": "transient"}},
                    "HeadObject",
                )
            return {"ContentLength": 42}

    client = Client()
    result = remote_publish.head_object_with_retry(client, "bucket", "key")
    assert result == {"ContentLength": 42}
    assert client.calls == 2
