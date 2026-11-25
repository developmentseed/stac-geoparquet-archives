"""Build and upload selected layouts one at a time for remote reads."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from pathlib import Path

from .config import load_config
from .layout import build_layout, source_months


def head_object_with_retry(client, bucket: str, key: str):
    from botocore.exceptions import (
        ClientError,
        ConnectionClosedError,
        EndpointConnectionError,
        ReadTimeoutError,
    )

    for attempt in range(8):
        try:
            return client.head_object(Bucket=bucket, Key=key)
        except ClientError as error:
            code = error.response["Error"]["Code"]
            if code in {"404", "NoSuchKey"}:
                return None
            if code not in {"500", "502", "503", "504", "520", "524"}:
                raise
        except (ConnectionClosedError, EndpointConnectionError, ReadTimeoutError):
            pass
        if attempt == 7:
            raise RuntimeError(f"HEAD failed after eight attempts: {key}") from None
        print(
            f"retrying HEAD {key} after transient error ({attempt + 1}/8)",
            file=sys.stderr,
            flush=True,
        )
        time.sleep(min(2 ** (attempt + 1), 30))
    raise AssertionError("unreachable")


def upload_file(client, source: Path, bucket: str, key: str) -> None:
    from boto3.exceptions import S3UploadFailedError
    from boto3.s3.transfer import TransferConfig
    from botocore.exceptions import (
        ClientError,
        ConnectionClosedError,
        EndpointConnectionError,
        ReadTimeoutError,
    )

    size = source.stat().st_size
    existing = head_object_with_retry(client, bucket, key)
    if existing is not None and existing["ContentLength"] == size:
        print(f"already uploaded {key}", file=sys.stderr, flush=True)
        return
    transfer = TransferConfig(
        multipart_threshold=32 * 1024 * 1024,
        multipart_chunksize=16 * 1024 * 1024,
        max_concurrency=4,
    )
    for attempt in range(8):
        try:
            client.upload_file(str(source), bucket, key, Config=transfer)
            break
        except (
            S3UploadFailedError,
            ClientError,
            ConnectionClosedError,
            EndpointConnectionError,
            ReadTimeoutError,
        ) as error:
            retryable = isinstance(
                error,
                (ConnectionClosedError, EndpointConnectionError, ReadTimeoutError),
            ) or any(
                marker in str(error)
                for marker in (
                    "(520)",
                    "(524)",
                    "(503)",
                    "Connection was closed",
                )
            )
            if not retryable or attempt == 7:
                raise
            print(
                f"retrying {key} after transient upload error ({attempt + 1}/8)",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(min(2 ** (attempt + 1), 30))
    uploaded = head_object_with_retry(client, bucket, key)
    if uploaded is None or uploaded["ContentLength"] != size:
        raise ValueError(f"uploaded size mismatch for {key}")
    print(f"uploaded {key}: {size:,} bytes", file=sys.stderr, flush=True)


def main() -> None:
    import boto3
    from botocore.config import Config

    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=Path("data/benchmark-source"))
    parser.add_argument("--work", type=Path, default=Path("data/benchmark-work"))
    parser.add_argument("--config", type=Path, default=Path("benchmark/sentinel2.json"))
    parser.add_argument(
        "--local-results",
        type=Path,
        default=Path("benchmark-results/sentinel-2-layout"),
    )
    parser.add_argument("--layout", action="append", required=True)
    parser.add_argument("--bucket", default="developmentseed")
    parser.add_argument(
        "--prefix", required=True, help="Object key prefix within bucket"
    )
    parser.add_argument("--profile", default="source-coop")
    parser.add_argument("--endpoint", default="https://data.source.coop")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    config = load_config(args.config)
    layouts = {item.id: item for item in config["layouts"]}
    if set(args.layout) - layouts.keys():
        raise ValueError(f"unknown layouts: {set(args.layout) - layouts.keys()}")
    if len(source_months(args.source)) != 24:
        raise ValueError("full source requires 24 months")
    prefix = args.prefix.strip("/")
    client = boto3.Session(profile_name=args.profile).client(
        "s3",
        endpoint_url=args.endpoint,
        config=Config(
            region_name="us-west-2",
            signature_version="s3v4",
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            connect_timeout=30,
            read_timeout=300,
            retries={"max_attempts": 10, "mode": "standard"},
            s3={"addressing_style": "path"},
        ),
    )
    for name in args.layout:
        key_prefix = f"{prefix}/{name}/"
        destination = f"s3://{args.bucket}/{key_prefix}"
        listing = client.list_objects_v2(
            Bucket=args.bucket, Prefix=key_prefix, MaxKeys=1
        )
        if listing.get("KeyCount", 0) and not args.resume:
            raise FileExistsError(f"remote destination is not empty: {destination}")
        target = args.work / f"remote-{name}"
        if target.exists() and not args.resume:
            raise FileExistsError(f"local staging directory exists: {target}")
        if target.joinpath("layout.json").exists() and args.resume:
            built = json.loads(target.joinpath("layout.json").read_text())
        else:
            print(f"building {name}", file=sys.stderr, flush=True)
            built = build_layout(
                args.source, target, layouts[name], config, resume=args.resume
            )
        reference = json.loads(
            (args.local_results / f"{name}-duckdb-run.json").read_text()
        )
        if (
            sum(item["item_count"] for item in built["files"])
            != reference["source_item_count"]
        ):
            raise ValueError(f"{name} row count differs from local benchmark")
        generated_hashes = {
            Path(item["path"]).name: item["sha256"] for item in built["files"]
        }
        reference_hashes = {
            Path(item["path"]).name: item["sha256"] for item in reference["files"]
        }
        if generated_hashes != reference_hashes:
            raise ValueError(f"{name} file hashes differ from local benchmark")
        for item in built["files"]:
            source = Path(item["path"])
            relative = source.relative_to(target)
            upload_file(client, source, args.bucket, key_prefix + str(relative))
        for metadata_name in ("_manifest.parquet", "collection.json"):
            source = target / metadata_name
            upload_file(client, source, args.bucket, key_prefix + metadata_name)
        print(f"uploaded {name} to {destination}", file=sys.stderr, flush=True)
        # Only the generated layout is removed after a successful upload.
        shutil.rmtree(target)


if __name__ == "__main__":
    main()
