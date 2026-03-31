from __future__ import annotations

from typing import Any

from datagen.config import TableMeta


def validate_generated_data(
    tables: list[TableMeta], data: dict[str, list[dict[str, Any]]], sample_limit: int = 20
) -> dict[str, Any]:
    by_name = {t.name: t for t in tables}
    issues: list[dict[str, Any]] = []
    counts = {"fk_violations": 0, "non_null_violations": 0, "unique_collisions": 0}

    for table_name, rows in data.items():
        meta = by_name.get(table_name)
        if not meta:
            continue

        for idx, row in enumerate(rows):
            for col in meta.columns:
                if not col.nullable and row.get(col.name) is None:
                    counts["non_null_violations"] += 1
                    if len(issues) < sample_limit:
                        issues.append(
                            {
                                "type": "non_null",
                                "table": table_name,
                                "row_index": idx,
                                "column": col.name,
                            }
                        )

        for fk in meta.foreign_keys:
            parent_rows = data.get(fk.ref_table, [])
            parent_vals = {r.get(fk.ref_column) for r in parent_rows}
            for idx, row in enumerate(rows):
                v = row.get(fk.column)
                if v is None:
                    continue
                if v not in parent_vals:
                    counts["fk_violations"] += 1
                    if len(issues) < sample_limit:
                        issues.append(
                            {
                                "type": "foreign_key",
                                "table": table_name,
                                "row_index": idx,
                                "column": fk.column,
                                "value": v,
                                "ref": f"{fk.ref_table}.{fk.ref_column}",
                            }
                        )

        unique_sets: list[list[str]] = []
        unique_sets.extend([[c.name] for c in meta.columns if c.unique or c.primary_key])
        unique_sets.extend([u.columns for u in meta.unique_constraints if u.columns])

        seen: dict[tuple[str, ...], set[tuple[Any, ...]]] = {}
        for cols in unique_sets:
            key = tuple(cols)
            if key not in seen:
                seen[key] = set()
            for idx, row in enumerate(rows):
                tup = tuple(row.get(c) for c in cols)
                if any(v is None for v in tup):
                    continue
                if tup in seen[key]:
                    counts["unique_collisions"] += 1
                    if len(issues) < sample_limit:
                        issues.append(
                            {
                                "type": "unique",
                                "table": table_name,
                                "row_index": idx,
                                "columns": cols,
                                "value": list(tup),
                            }
                        )
                else:
                    seen[key].add(tup)

    total_failures = sum(counts.values())
    return {
        "pass": total_failures == 0,
        "counts": {
            "fk_violations": counts["fk_violations"],
            "non_null_violations": counts["non_null_violations"],
            "unique_collisions": counts["unique_collisions"],
            "total_failures": total_failures,
            "sample_issue_count": len(issues),
        },
        "sample_issues": issues,
    }
