from __future__ import annotations

from pathlib import Path
from typing import Any


def write_parquet(data: dict[str, list[dict[str, Any]]], out_dir: str, compression: str = "snappy") -> list[str]:
    """Write one parquet file per table.

    Requires optional dependency: polars.
    """
    try:
        import polars as pl
    except Exception as e:  # pragma: no cover - optional dependency
        raise RuntimeError("--out parquet requires optional dependency 'polars' (pip install polars)") from e

    # Map "none" to polars-compatible value
    polars_compression = "uncompressed" if compression == "none" else compression

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths: list[str] = []

    for table, rows in data.items():
        p = out / f"{table}.parquet"
        if rows:
            pl.DataFrame(rows).write_parquet(str(p), compression=polars_compression)
        else:
            pl.DataFrame([]).write_parquet(str(p), compression=polars_compression)
        paths.append(str(p))

    return paths
