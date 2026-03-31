from __future__ import annotations

from collections import Counter
from typing import Any


def build_report(data: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    tables: dict[str, Any] = {}
    for table, rows in data.items():
        row_count = len(rows)
        columns = list(rows[0].keys()) if rows else []
        col_stats: dict[str, Any] = {}

        for c in columns:
            vals = [r.get(c) for r in rows]
            null_count = sum(1 for v in vals if v is None)
            non_null = [v for v in vals if v is not None]
            unique_count = len(set(map(str, non_null)))

            top_values = []
            if non_null:
                cnt = Counter(map(str, non_null))
                top_values = [{"value": k, "count": v} for k, v in cnt.most_common(3)]

            col_stats[c] = {
                "null_count": null_count,
                "null_ratio": (null_count / row_count) if row_count else 0.0,
                "unique_non_null": unique_count,
                "top_values": top_values,
            }

        tables[table] = {
            "rows": row_count,
            "columns": len(columns),
            "column_stats": col_stats,
        }

    return {
        "tables": tables,
        "total_tables": len(data),
        "total_rows": sum(len(v) for v in data.values()),
    }
