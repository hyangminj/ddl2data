from __future__ import annotations

from pathlib import Path

import sqlglot
from sqlglot import exp

from datagen.config import ColumnMeta, ForeignKey, TableMeta


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

        for item in schema.expressions:
            if isinstance(item, exp.ColumnDef):
                name = item.this.name
                t_name, max_len = _extract_type_and_len(item.args.get("kind"))
                nullable = True
                primary_key = False
                unique = False
                for c in item.constraints:
                    if isinstance(c.kind, exp.NotNullColumnConstraint):
                        nullable = False
                    elif isinstance(c.kind, exp.PrimaryKeyColumnConstraint):
                        primary_key = True
                        nullable = False
                    elif isinstance(c.kind, exp.UniqueColumnConstraint):
                        unique = True
                    elif isinstance(c.kind, exp.Reference):
                        ref_tbl, ref_cols = _reference_table_and_columns(c.kind)
                        ref_col = ref_cols[0] if ref_cols else "id"
                        foreign_keys.append(ForeignKey(column=name, ref_table=ref_tbl, ref_column=ref_col))
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

        tables.append(TableMeta(name=table_name, columns=columns, foreign_keys=foreign_keys))

    return tables


def parse_ddl_file(path: str | Path) -> list[TableMeta]:
    ddl_text = Path(path).read_text(encoding="utf-8")
    return parse_ddl_text(ddl_text)
