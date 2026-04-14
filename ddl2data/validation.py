from __future__ import annotations

import re
from typing import Any

from ddl2data.config import CheckConstraintMeta, ColumnMeta, TableMeta
from ddl2data.generator.base import _extract_in_values, _normalize_check_expression, _split_compound_check


def _evaluate_numeric_op(value: float, op: str, limit: float) -> bool:
    if op == ">=":
        return value >= limit
    if op == ">":
        return value > limit
    if op == "<=":
        return value <= limit
    if op == "<":
        return value < limit
    if op in {"=", "=="}:
        return value == limit
    if op in {"!=", "<>"}:
        return value != limit
    return True


def _evaluate_check(row: dict[str, Any], col_by_name: dict[str, ColumnMeta], check: CheckConstraintMeta) -> bool:
    mode, parts = _split_compound_check(check.expression)
    if mode == "and":
        return all(_evaluate_check(row, col_by_name, CheckConstraintMeta(expression=part)) for part in parts)
    if mode == "or":
        return any(_evaluate_check(row, col_by_name, CheckConstraintMeta(expression=part)) for part in parts)
    expr = parts[0]

    m_between = re.search(r"(\w+)\s+BETWEEN\s+(-?\d+(?:\.\d+)?)\s+AND\s+(-?\d+(?:\.\d+)?)", expr, flags=re.IGNORECASE)
    if m_between:
        col_name, a_raw, b_raw = m_between.groups()
        if col_name not in col_by_name:
            return True
        value = row.get(col_name)
        if value is None:
            return True
        try:
            num = float(value)
        except (TypeError, ValueError):
            return False
        lo, hi = sorted((float(a_raw), float(b_raw)))
        return lo <= num <= hi

    m_in = re.search(r"(\w+)\s+IN\s*\(([^\)]+)\)", expr, flags=re.IGNORECASE)
    if m_in:
        col_name, raw = m_in.groups()
        if col_name not in col_by_name:
            return True
        value = row.get(col_name)
        if value is None:
            return True
        values = _extract_in_values(raw)
        if not values:
            return True
        try:
            return float(value) in {float(v) for v in values}
        except (TypeError, ValueError):
            return str(value) in values

    m_regex = re.search(r"(\w+)\s*~\s*'([^']+)'", expr, flags=re.IGNORECASE)
    if not m_regex:
        m_regex = re.search(r"REGEXP_LIKE\s*\(\s*(\w+)\s*,\s*'([^']+)'\s*\)", expr, flags=re.IGNORECASE)
    if m_regex:
        col_name, pattern = m_regex.groups()
        if col_name not in col_by_name:
            return True
        value = row.get(col_name)
        if value is None:
            return True
        try:
            return re.search(pattern, str(value)) is not None
        except re.error:
            return True

    m = re.search(r"(\w+)\s*(>=|<=|<>|!=|>|<|=|==)\s*(-?\d+(?:\.\d+)?)", expr)
    if m:
        col_name, op, raw = m.groups()
        if col_name not in col_by_name:
            return True
        value = row.get(col_name)
        if value is None:
            return True
        try:
            num = float(value)
        except (TypeError, ValueError):
            return False
        return _evaluate_numeric_op(num, op, float(raw))

    return True


def _check_column_values(row: dict[str, Any], check: CheckConstraintMeta) -> dict[str, Any]:
    mode, parts = _split_compound_check(check.expression)
    if mode in {"and", "or"}:
        values: dict[str, Any] = {}
        for part in parts:
            values.update(_check_column_values(row, CheckConstraintMeta(expression=part)))
        return values
    expr = parts[0]

    for pattern in [
        r"(\w+)\s+BETWEEN\s+(-?\d+(?:\.\d+)?)\s+AND\s+(-?\d+(?:\.\d+)?)",
        r"(\w+)\s+IN\s*\(([^\)]+)\)",
        r"(\w+)\s*~\s*'([^']+)'",
        r"REGEXP_LIKE\s*\(\s*(\w+)\s*,\s*'([^']+)'\s*\)",
        r"(\w+)\s*(>=|<=|<>|!=|>|<|=|==)\s*(-?\d+(?:\.\d+)?)",
    ]:
        match = re.search(pattern, expr, flags=re.IGNORECASE)
        if match:
            col_name = match.group(1)
            return {col_name: row.get(col_name)}
    return {}


def validate_check_constraints(
    tables: list[TableMeta], data: dict[str, list[dict[str, Any]]], sample_limit: int = 20
) -> dict[str, Any]:
    by_name = {t.name: t for t in tables}
    details: list[dict[str, Any]] = []
    violations = 0

    for table_name, rows in data.items():
        meta = by_name.get(table_name)
        if not meta or not meta.check_constraints:
            continue
        col_by_name = {c.name: c for c in meta.columns}
        checks: list[CheckConstraintMeta] = []
        seen_checks: set[str] = set()
        for check in meta.check_constraints:
            normalized = _normalize_check_expression(check.expression)
            if normalized in seen_checks:
                continue
            checks.append(CheckConstraintMeta(expression=normalized, name=check.name))
            seen_checks.add(normalized)
        for idx, row in enumerate(rows):
            for check in checks:
                if _evaluate_check(row, col_by_name, check):
                    continue
                violations += 1
                if len(details) < sample_limit:
                    details.append(
                        {
                            "type": "check",
                            "table": table_name,
                            "row_index": idx,
                            "check": _normalize_check_expression(check.expression),
                            "column_values": _check_column_values(row, check),
                        }
                    )

    return {"check_violations": violations, "check_details": details}


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
        seen_unique_sets: set[tuple[str, ...]] = set()
        for cols in [[c.name] for c in meta.columns if c.unique or c.primary_key]:
            key = tuple(cols)
            if key not in seen_unique_sets:
                unique_sets.append(cols)
                seen_unique_sets.add(key)
        for cols in [u.columns for u in meta.unique_constraints if u.columns]:
            key = tuple(cols)
            if key not in seen_unique_sets:
                unique_sets.append(cols)
                seen_unique_sets.add(key)

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
