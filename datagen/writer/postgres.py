from __future__ import annotations

from typing import Any


def _sql_literal(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v).replace("'", "''")
    return f"'{s}'"


def render_insert_sql(table: str, rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    cols = list(rows[0].keys())
    col_sql = ", ".join(f'"{c}"' for c in cols)
    lines: list[str] = []
    for row in rows:
        values = ", ".join(_sql_literal(row.get(c)) for c in cols)
        lines.append(f'INSERT INTO "{table}" ({col_sql}) VALUES ({values});')
    return "\n".join(lines)
