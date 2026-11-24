"""Count remote object-store reads through a local, transparent HTTP proxy."""

from __future__ import annotations

import threading
from collections import Counter
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


class RequestStats:
    def __init__(self, max_bytes_read: int | None = None) -> None:
        self.lock = threading.Lock()
        self.max_bytes_read = max_bytes_read
        self.requests = 0
        self.range_requests = 0
        self.bytes_read = 0
        self.files: set[str] = set()
        self.statuses: Counter[int] = Counter()

    def record_request(self, path: str, has_range: bool) -> None:
        with self.lock:
            self.requests += 1
            self.range_requests += int(has_range)
            if (
                path.endswith(".parquet")
                and path.rsplit("/", 1)[-1] != "_manifest.parquet"
            ):
                self.files.add(path)

    def record_response(self, status: int) -> None:
        with self.lock:
            self.statuses[status] += 1

    def record_bytes(self, size: int) -> bool:
        with self.lock:
            if (
                self.max_bytes_read is not None
                and self.bytes_read + size > self.max_bytes_read
            ):
                return False
            self.bytes_read += size
            return True

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "http_requests": self.requests,
                "range_requests": self.range_requests,
                "bytes_read": self.bytes_read,
                "files_opened": len(self.files),
                "http_statuses": dict(self.statuses),
            }


@contextmanager
def counting_proxy(
    upstream_base: str, allowed_prefix: str, max_bytes_read: int | None = None
):
    """Forward only paths under the configured benchmark prefix."""
    parsed = urlsplit(upstream_base)
    local_test_origin = parsed.scheme == "http" and parsed.hostname in {
        "127.0.0.1",
        "localhost",
    }
    if (
        not (parsed.scheme == "https" or local_test_origin)
        or not parsed.netloc
        or parsed.path not in ("", "/")
    ):
        raise ValueError("upstream_base must be an HTTPS origin or local test server")
    prefix = "/" + allowed_prefix.strip("/") + "/"
    stats = RequestStats(max_bytes_read)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format: str, *args: object) -> None:
            pass

        def do_HEAD(self) -> None:
            self._forward()

        def do_GET(self) -> None:
            self._forward()

        def _forward(self) -> None:
            path = urlsplit(self.path).path
            if not path.startswith(prefix) or ".." in path.split("/"):
                self.send_error(403)
                return
            range_header = self.headers.get("Range")
            stats.record_request(path, range_header is not None)
            headers = {"Accept-Encoding": "identity"}
            if range_header:
                headers["Range"] = range_header
            request = Request(
                upstream_base.rstrip("/") + self.path,
                headers=headers,
                method=self.command,
            )
            try:
                response = urlopen(request, timeout=120)
            except HTTPError as error:
                response = error
            with response:
                status = response.status
                assert status is not None
                if range_header and status == 200:
                    stats.record_response(status)
                    self.send_error(502, "upstream ignored Range")
                    return
                self.send_response(status)
                for header in (
                    "Content-Length",
                    "Content-Range",
                    "Content-Type",
                    "Accept-Ranges",
                    "ETag",
                    "Last-Modified",
                ):
                    value = response.headers.get(header)
                    if value is not None:
                        self.send_header(header, value)
                self.send_header("Connection", "close")
                self.end_headers()
                stats.record_response(status)
                if self.command == "GET":
                    while chunk := response.read(1024 * 1024):
                        if not stats.record_bytes(len(chunk)):
                            self.close_connection = True
                            return
                        self.wfile.write(chunk)
                self.close_connection = True

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", stats
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
