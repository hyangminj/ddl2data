from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def write_json(data: dict[str, list[dict[str, Any]]], out_path: str | None = None) -> str:
    payload = json.dumps(data, indent=2, default=str)
    if out_path:
        Path(out_path).write_text(payload, encoding="utf-8")
    return payload
