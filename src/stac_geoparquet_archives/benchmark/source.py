"""Fetch and inventory the exact Planetary Computer snapshot files."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import threading
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from pathlib import Path

TOKEN_URL = "https://planetarycomputer.microsoft.com/api/sas/v1/token/pcstacitems/items"
BASE_URL = "https://pcstacitems.blob.core.windows.net/items"
MONTH_PATTERN = re.compile(r"_(20\d\d-\d\d)-")


class TokenProvider:
    def __init__(self, payload: dict) -> None:
        self.token = payload["token"]
        self.expiry = datetime.fromisoformat(payload["msft:expiry"])
        self.lock = threading.Lock()

    def get(self) -> str:
        with self.lock:
            if self.expiry <= datetime.now(UTC) + timedelta(minutes=5):
                payload = json.load(urllib.request.urlopen(TOKEN_URL, timeout=30))
                self.token = payload["token"]
                self.expiry = datetime.fromisoformat(payload["msft:expiry"])
            return self.token

    def invalidate(self) -> None:
        with self.lock:
            self.expiry = datetime.min.replace(tzinfo=UTC)


def snapshot_listing(years: set[int]) -> tuple[list[dict], TokenProvider]:
    provider = TokenProvider(json.load(urllib.request.urlopen(TOKEN_URL, timeout=30)))
    token = provider.get()
    url = (
        BASE_URL + "?restype=container&comp=list"
        "&prefix=sentinel-2-l2a.parquet%2F&" + token
    )
    root = ET.fromstring(urllib.request.urlopen(url, timeout=60).read())
    files = []
    for blob in root.findall(".//Blob"):
        name = blob.findtext("Name", "")
        match = MONTH_PATTERN.search(name)
        if match and int(match[1][:4]) in years:
            files.append(
                {
                    "month": match[1],
                    "name": name,
                    "size_bytes": int(blob.findtext("Properties/Content-Length", "0")),
                    "etag": blob.findtext("Properties/Etag"),
                }
            )
    return sorted(files, key=lambda f: f["month"]), provider


def fetch_file(
    entry: dict, provider: TokenProvider, output: Path, segments: int = 1
) -> dict:
    token = provider.get()
    url = (
        BASE_URL + "/" + urllib.parse.quote(entry["name"], safe="/+-:._") + "?" + token
    )
    target = output / Path(entry["name"]).name
    if target.exists():
        if target.stat().st_size != entry["size_bytes"]:
            raise ValueError(f"source size changed: {target}")
    else:
        free = shutil.disk_usage(output).free
        if free < entry["size_bytes"] * 3:
            raise RuntimeError(f"insufficient free space to retrieve {target}")
        partial = target.with_suffix(".partial")
        offset = partial.stat().st_size if partial.exists() else 0
        etag = f'"{entry["etag"].strip(chr(34))}"'
        if segments > 1:
            remaining = entry["size_bytes"] - offset
            count = min(segments, remaining)
            step = (remaining + count - 1) // count

            def part(index: int) -> Path:
                start = offset + index * step
                end = min(entry["size_bytes"] - 1, start + step - 1)
                destination = partial.with_name(partial.name + f".{index:02d}")
                request = urllib.request.Request(
                    url, headers={"If-Match": etag, "Range": f"bytes={start}-{end}"}
                )
                with (
                    urllib.request.urlopen(request, timeout=120) as source,
                    destination.open("wb") as dest,
                ):
                    if source.status != 206:
                        raise ValueError(
                            f"range request was not honored: {entry['name']}"
                        )
                    shutil.copyfileobj(source, dest, length=8 * 1024 * 1024)
                if destination.stat().st_size != end - start + 1:
                    raise ValueError(f"truncated range: {entry['name']} {index}")
                return destination

            with ThreadPoolExecutor(max_workers=count) as pool:
                chunks = list(pool.map(part, range(count)))
            with partial.open("ab") as dest:
                for chunk in chunks:
                    with chunk.open("rb") as source:
                        shutil.copyfileobj(source, dest, length=8 * 1024 * 1024)
                    chunk.unlink()
        else:
            headers = {"If-Match": etag}
            if offset:
                headers["Range"] = f"bytes={offset}-"
            request = urllib.request.Request(url, headers=headers)
            with (
                urllib.request.urlopen(request, timeout=120) as source,
                partial.open("ab" if offset else "wb") as dest,
            ):
                if source.headers.get("ETag", "").strip('"') != entry["etag"].strip(
                    '"'
                ):
                    raise ValueError(f"remote ETag changed: {entry['name']}")
                if offset and source.status != 206:
                    raise ValueError(f"range resume was not honored: {entry['name']}")
                shutil.copyfileobj(source, dest, length=8 * 1024 * 1024)
        if partial.stat().st_size != entry["size_bytes"]:
            raise ConnectionError(f"truncated download: {partial}")
        os.replace(partial, target)
    digest = hashlib.sha256()
    with target.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    target.chmod(0o444)
    import pyarrow.parquet as pq

    metadata = pq.read_metadata(target)
    return {
        **entry,
        "path": str(target),
        "sha256": digest.hexdigest(),
        "item_count": metadata.num_rows,
    }


def fetch_with_retries(
    entry: dict, provider: TokenProvider, output: Path, segments: int
) -> dict:
    for attempt in range(8):
        try:
            return fetch_file(entry, provider, output, segments)
        except (ConnectionError, TimeoutError, OSError) as error:
            if getattr(error, "code", None) == 403:
                provider.invalidate()
            if attempt == 7:
                raise
            print(
                f"retrying {entry['month']} after {type(error).__name__}: {error}",
                flush=True,
            )
            time.sleep(min(2**attempt, 30))
    raise AssertionError("unreachable")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", type=int, nargs="+", default=[2023, 2024])
    parser.add_argument("--output", type=Path, default=Path("data/benchmark-source"))
    parser.add_argument(
        "--month", action="append", help="Retrieve selected YYYY-MM only"
    )
    parser.add_argument("--exclude-month", action="append")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--segments", type=int, default=1)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    files, provider = snapshot_listing(set(args.years))
    if args.month:
        files = [f for f in files if f["month"] in args.month]
    if args.exclude_month:
        files = [f for f in files if f["month"] not in args.exclude_month]
    print(
        f"{len(files)} source files, {sum(f['size_bytes'] for f in files)} bytes",
        flush=True,
    )
    inventory_path = args.output / "inventory.json"
    old = (
        json.loads(inventory_path.read_text())["files"]
        if inventory_path.exists()
        else []
    )
    inventory = {entry["month"]: entry for entry in old}
    for entry in files:
        previous = inventory.get(entry["month"])
        if previous and previous["etag"] != entry["etag"]:
            raise ValueError(f"remote snapshot changed for {entry['month']}")
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        jobs = {
            pool.submit(
                fetch_with_retries, entry, provider, args.output, args.segments
            ): entry
            for entry in files
        }
        for future in as_completed(jobs):
            entry = future.result()
            inventory[entry["month"]] = entry
            inventory_path.write_text(
                json.dumps(
                    {
                        "asset": "abfs://items/sentinel-2-l2a.parquet",
                        "retrieved_at": datetime.now(UTC).isoformat(),
                        "files": [inventory[key] for key in sorted(inventory)],
                    },
                    indent=2,
                )
                + "\n"
            )
            print(f"verified {entry['month']}: {entry['item_count']} items", flush=True)
    print(f"wrote {args.output / 'inventory.json'}")


if __name__ == "__main__":
    main()
