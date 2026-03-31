from __future__ import annotations

from pathlib import Path
from typing import Any


def write_parquet(data: dict[str, list[dict[str, Any]]], out_dir: str) -> list[str]:
    """Write one parquet file per table.

    Requires optional dependency: polars.
    """
    try:
        import polars as pl
    except Exception as e:  # pragma: no cover - optional dependency
        raise RuntimeError("--out parquet requires optional dependency 'polars' (pip install polars)") from e

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths: list[str] = []

    for table, rows in data.items():
        p = out / f"{table}.parquet"
        if rows:
            pl.DataFrame(rows).write_parquet(str(p))
        else:
            pl.DataFrame([]).write_parquet(str(p))
        paths.append(str(p))

    return paths
