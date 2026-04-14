from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


def write_csv(data: dict[str, list[dict[str, Any]]], out_dir: str, engine: str = "python") -> list[str]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths: list[str] = []

    use_polars = engine == "polars"
    pl = None
    if use_polars:
        try:
            import polars as pl_mod

            pl = pl_mod
        except Exception as e:  # pragma: no cover - optional dependency
            raise RuntimeError("engine=polars requires optional dependency 'polars' (pip install polars)") from e

    for table, rows in data.items():
        p = out / f"{table}.csv"
        if not rows:
            p.write_text("", encoding="utf-8")
            paths.append(str(p))
            continue

        if use_polars and pl is not None:
            pl.DataFrame(rows).write_csv(str(p))
        else:
            cols = list(rows[0].keys())
            with p.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=cols)
                w.writeheader()
                w.writerows(rows)
        paths.append(str(p))
    return paths
