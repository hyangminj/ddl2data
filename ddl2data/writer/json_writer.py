from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def write_json(
    data: dict[str, list[dict[str, Any]]], out_path: str | None = None, engine: str = "python"
) -> str:
    if engine == "polars":
        try:
            import polars as pl
        except Exception as e:  # pragma: no cover - optional dependency
            raise RuntimeError("engine=polars requires optional dependency 'polars' (pip install polars)") from e
        payload_obj: dict[str, list[dict[str, Any]]] = {}
        for table, rows in data.items():
            payload_obj[table] = pl.DataFrame(rows).to_dicts() if rows else []
        payload = json.dumps(payload_obj, indent=2, default=str)
    else:
        payload = json.dumps(data, indent=2, default=str)

    if out_path:
        Path(out_path).write_text(payload, encoding="utf-8")
    return payload
