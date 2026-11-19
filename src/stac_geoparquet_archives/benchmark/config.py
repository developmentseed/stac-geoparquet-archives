"""Shared layout and query configuration."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Layout:
    id: str
    granularity: str
    row_group_size: int
    sort: str

    @classmethod
    def from_dict(cls, value: dict) -> Layout:
        result = cls(**value)
        if result.granularity not in {"month", "quarter", "year", "unpartitioned"}:
            raise ValueError(result.granularity)
        if result.sort not in {"tile", "hilbert", "datetime"}:
            raise ValueError(result.sort)
        if result.row_group_size < 1:
            raise ValueError(result.row_group_size)
        return result


def load_config(path: Path) -> dict:
    config = json.loads(path.read_text())
    config["layouts"] = [Layout.from_dict(v) for v in config["layouts"]]
    return config
