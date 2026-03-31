from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


def write_csv(data: dict[str, list[dict[str, Any]]], out_dir: str) -> list[str]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths: list[str] = []
    for table, rows in data.items():
        p = out / f"{table}.csv"
        if not rows:
            p.write_text("", encoding="utf-8")
            paths.append(str(p))
            continue
        cols = list(rows[0].keys())
        with p.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows)
        paths.append(str(p))
    return paths
