from __future__ import annotations

from pathlib import Path

import re

import sqlglot
from sqlglot import exp

from datagen.config import (
    CheckConstraintMeta,
    ColumnMeta,
    ForeignKey,
    TableMeta,
    UniqueConstraintMeta,
)


def _extract_type_and_len(kind: exp.DataType | None) -> tuple[str, int | None]:
    if kind is None:
        return "text", None
    type_name = kind.this.value.lower() if hasattr(kind.this, "value") else str(kind.this).lower()
    max_length = None
    if kind.expressions:
        first = kind.expressions[0]
        if isinstance(first, exp.DataTypeParam):
            v = first.this
            try:
                max_length = int(str(v))
            except ValueError:
                max_length = None
    return type_name, max_length


def _reference_table_and_columns(ref: exp.Reference) -> tuple[str, list[str]]:
    target = ref.this
    if isinstance(target, exp.Schema):
        table_expr = target.this
        tbl = table_expr.name if isinstance(table_expr, exp.Table) else str(table_expr)
        cols = [c.name for c in target.expressions] or ["id"]
        return tbl, cols
    if isinstance(target, exp.Table):
        return target.name, [c.name for c in ref.expressions] or ["id"]
    return str(target), ["id"]


def _class_name(node: object) -> str:
    return node.__class__.__name__.lower()


def _extract_unique_columns(node: object) -> list[str]:
    cols: list[str] = []
    exprs = getattr(node, "expressions", None) or []
    for e in exprs:
        name = getattr(e, "name", None)
        if isinstance(name, str):
            cols.append(name)
    return cols


def _fallback_unique_constraints(schema_sql: str) -> list[list[str]]:
    matches = re.findall(r"UNIQUE\s*\(([^\)]+)\)", schema_sql, flags=re.IGNORECASE)
    out: list[list[str]] = []
    for m in matches:
        cols = [c.strip().strip('"`') for c in m.split(",") if c.strip()]
        if cols:
            out.append(cols)
    return out


def _fallback_checks(schema_sql: str) -> list[str]:
    return re.findall(r"CHECK\s*\(([^\)]+)\)", schema_sql, flags=re.IGNORECASE)


def parse_ddl_text(ddl_text: str) -> list[TableMeta]:
    tables: list[TableMeta] = []
    statements = sqlglot.parse(ddl_text)

    for stmt in statements:
        if not isinstance(stmt, exp.Create):
            continue
        schema = stmt.this
        if not isinstance(schema, exp.Schema):
            continue
        table_expr = schema.this
        if not isinstance(table_expr, exp.Table):
            continue

        table_name = table_expr.name
        columns: list[ColumnMeta] = []
        foreign_keys: list[ForeignKey] = []
        unique_constraints: list[UniqueConstraintMeta] = []
        check_constraints: list[CheckConstraintMeta] = []

        for item in schema.expressions:
            if isinstance(item, exp.ColumnDef):
                name = item.this.name
                t_name, max_len = _extract_type_and_len(item.args.get("kind"))
                nullable = True
                primary_key = False
                unique = False
                for c in item.constraints:
                    c_kind = c.kind
                    if isinstance(c_kind, exp.NotNullColumnConstraint):
                        nullable = False
                    elif isinstance(c_kind, exp.PrimaryKeyColumnConstraint):
                        primary_key = True
                        nullable = False
                    elif isinstance(c_kind, exp.UniqueColumnConstraint):
                        unique = True
                        unique_constraints.append(UniqueConstraintMeta(columns=[name]))
                    elif isinstance(c_kind, exp.Reference):
                        ref_tbl, ref_cols = _reference_table_and_columns(c_kind)
                        ref_col = ref_cols[0] if ref_cols else "id"
                        foreign_keys.append(ForeignKey(column=name, ref_table=ref_tbl, ref_column=ref_col))
                    elif "check" in _class_name(c_kind):
                        check_constraints.append(CheckConstraintMeta(expression=c_kind.sql()))

                columns.append(
                    ColumnMeta(
                        name=name,
                        type_name=t_name,
                        nullable=nullable,
                        primary_key=primary_key,
                        unique=unique,
                        max_length=max_len,
                    )
                )
            elif isinstance(item, exp.Constraint):
                c_name = item.this.name if getattr(item, "this", None) is not None else None
                for e in item.expressions:
                    if isinstance(e, exp.ForeignKey):
                        local_cols = [c.name for c in e.expressions]
                        ref = e.args.get("reference")
                        if isinstance(ref, exp.Reference):
                            ref_tbl, ref_cols = _reference_table_and_columns(ref)
                            for i, local_col in enumerate(local_cols):
                                ref_col = ref_cols[min(i, len(ref_cols) - 1)]
                                foreign_keys.append(
                                    ForeignKey(column=local_col, ref_table=ref_tbl, ref_column=ref_col)
                                )
                    elif "unique" in _class_name(e):
                        cols = _extract_unique_columns(e)
                        if cols:
                            unique_constraints.append(UniqueConstraintMeta(columns=cols, name=c_name))
                            if len(cols) == 1:
                                for col in columns:
                                    if col.name == cols[0]:
                                        col.unique = True
                    elif "check" in _class_name(e):
                        check_constraints.append(CheckConstraintMeta(expression=e.sql(), name=c_name))
            elif isinstance(item, exp.ForeignKey):
                local_cols = [c.name for c in item.expressions]
                ref = item.args.get("reference")
                if isinstance(ref, exp.Reference):
                    ref_tbl, ref_cols = _reference_table_and_columns(ref)
                    for i, local_col in enumerate(local_cols):
                        ref_col = ref_cols[min(i, len(ref_cols) - 1)]
                        foreign_keys.append(
                            ForeignKey(column=local_col, ref_table=ref_tbl, ref_column=ref_col)
                        )
            elif "unique" in _class_name(item):
                cols = _extract_unique_columns(item)
                if cols:
                    unique_constraints.append(UniqueConstraintMeta(columns=cols))
            elif "check" in _class_name(item):
                check_constraints.append(CheckConstraintMeta(expression=item.sql()))

        # Fallback extraction from rendered SQL for dialect/AST variants
        schema_sql = schema.sql()
        seen_unique = {tuple(u.columns) for u in unique_constraints}
        for cols in _fallback_unique_constraints(schema_sql):
            key = tuple(cols)
            if key not in seen_unique:
                unique_constraints.append(UniqueConstraintMeta(columns=cols))
                seen_unique.add(key)
            if len(cols) == 1:
                for col in columns:
                    if col.name == cols[0]:
                        col.unique = True

        seen_checks = {c.expression for c in check_constraints}
        for expr in _fallback_checks(schema_sql):
            expr_clean = expr.strip()
            if expr_clean and expr_clean not in seen_checks:
                check_constraints.append(CheckConstraintMeta(expression=expr_clean))
                seen_checks.add(expr_clean)

        tables.append(
            TableMeta(
                name=table_name,
                columns=columns,
                foreign_keys=foreign_keys,
                unique_constraints=unique_constraints,
                check_constraints=check_constraints,
            )
        )

    return tables


def parse_ddl_file(path: str | Path) -> list[TableMeta]:
    ddl_text = Path(path).read_text(encoding="utf-8")
    return parse_ddl_text(ddl_text)
