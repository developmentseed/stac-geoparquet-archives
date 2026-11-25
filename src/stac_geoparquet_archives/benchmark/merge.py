"""Bounded-memory merge of monthly Parquet files sorted by tile and datetime."""

from __future__ import annotations

import os
from pathlib import Path


def _key(table, index: int) -> tuple:
    tile = table["s2:mgrs_tile"][index].as_py()
    return (
        tile is None,
        tile or "",
        table["datetime"][index].as_py(),
        table["id"][index].as_py(),
    )


def merge_sorted_files(
    inputs: list[Path], output: Path, schema, settings: dict, row_group_size: int
) -> None:
    """Merge sorted batches while keeping only one small batch per input in RAM."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    output.parent.mkdir(parents=True, exist_ok=True)
    readers = [pq.ParquetFile(path).iter_batches(batch_size=4096) for path in inputs]
    current = []
    for reader in readers:
        batch = next(reader, None)
        current.append(pa.Table.from_batches([batch]) if batch is not None else None)
    temporary = output.with_suffix(".building")
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
    pending = []
    pending_rows = 0
    try:
        while any(table is not None for table in current):
            # Each input is already sorted. Emit the prefix from the input
            # with the lowest first key, stopping at the next input's key.
            # This avoids repeatedly sorting all buffered rows together.
            heads = sorted(
                (_key(table, 0), index)
                for index, table in enumerate(current)
                if table is not None
            )
            index = heads[0][1]
            table = current[index]
            assert table is not None
            if len(heads) == 1:
                count = table.num_rows
            else:
                limit = heads[1][0]
                low, high = 0, table.num_rows
                while low < high:
                    middle = (low + high) // 2
                    if _key(table, middle) <= limit:
                        low = middle + 1
                    else:
                        high = middle
                count = low
            if count == 0:
                raise RuntimeError("merge made no progress")
            output_part = table.slice(0, count)
            remaining = table.slice(count)
            if remaining.num_rows:
                current[index] = remaining
            else:
                batch = next(readers[index], None)
                current[index] = (
                    pa.Table.from_batches([batch]) if batch is not None else None
                )
            while output_part.num_rows:
                take = min(row_group_size - pending_rows, output_part.num_rows)
                pending.append(output_part.slice(0, take))
                pending_rows += take
                output_part = output_part.slice(take)
                if pending_rows == row_group_size:
                    writer.write_table(
                        pa.concat_tables(pending), row_group_size=row_group_size
                    )
                    pending.clear()
                    pending_rows = 0
        if pending:
            writer.write_table(pa.concat_tables(pending), row_group_size=row_group_size)
        writer.close()
        writer = None
        os.replace(temporary, output)
    finally:
        if writer is not None:
            writer.close()
        temporary.unlink(missing_ok=True)
