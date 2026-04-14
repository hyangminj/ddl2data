from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

from ddl2data.config import ColumnMeta, TableMeta


def _infer_attr_type(col: ColumnMeta | None, value: Any) -> str:
    if col is not None and col.extra.get("dynamodb_attr_type"):
        return str(col.extra["dynamodb_attr_type"])
    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return "N"
    if isinstance(value, (bytes, bytearray)):
        return "B"
    return "S"


def _serialize_binary(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        payload = bytes(value)
    else:
        payload = str(value).encode("utf-8")
    return base64.b64encode(payload).decode("ascii")


def _attribute_value(col: ColumnMeta | None, value: Any) -> dict[str, Any]:
    if value is None:
        return {"NULL": True}

    attr_type = _infer_attr_type(col, value)
    if attr_type == "S":
        return {"S": str(value)}
    if attr_type == "N":
        return {"N": str(value)}
    if attr_type == "BOOL":
        return {"BOOL": bool(value)}
    if attr_type == "B":
        return {"B": _serialize_binary(value)}
    raise ValueError(f"Unsupported DynamoDB attribute type '{attr_type}'")


def _render_table_lines(table: TableMeta, rows: list[dict[str, Any]]) -> str:
    columns = {col.name: col for col in table.columns}
    payload_lines = []
    for row in rows:
        item = {name: _attribute_value(columns.get(name), value) for name, value in row.items()}
        payload_lines.append(json.dumps({"Item": item}, separators=(",", ":"), default=str))
    return "\n".join(payload_lines)


def write_dynamodb_json(
    tables: list[TableMeta],
    data: dict[str, list[dict[str, Any]]],
    out_path: str | None = None,
) -> str | list[str]:
    by_name = {table.name: table for table in tables}
    if len(data) > 1 and not out_path:
        raise SystemExit("--out dynamodb-json with multiple tables requires --output-path directory")

    if len(data) == 1:
        table_name, rows = next(iter(data.items()))
        payload = _render_table_lines(by_name[table_name], rows)
        if out_path:
            Path(out_path).write_text(payload, encoding="utf-8")
        return payload

    output_dir = Path(out_path or "./output_dynamodb_json")
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[str] = []
    for table_name, rows in data.items():
        payload = _render_table_lines(by_name[table_name], rows)
        path = output_dir / f"{table_name}.ddb.json"
        path.write_text(payload, encoding="utf-8")
        written.append(str(path))
    return written
