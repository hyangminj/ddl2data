from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from datagen.config import ColumnMeta, ForeignKey, TableMeta


def _safe_len(col_type: object) -> int | None:
    length = getattr(col_type, "length", None)
    return int(length) if isinstance(length, int) else None


def load_schema_from_db(engine: Engine, tables: Iterable[str] | None = None) -> list[TableMeta]:
    """Load table metadata from a live DB using SQLAlchemy inspector.

    Returns TableMeta compatible with the DDL parser path.
    """
    inspector = inspect(engine)
    table_names = list(tables) if tables else inspector.get_table_names()

    metas: list[TableMeta] = []
    for table_name in table_names:
        pk_info = inspector.get_pk_constraint(table_name) or {}
        pk_cols = set(pk_info.get("constrained_columns") or [])

        unique_cols: set[str] = set()
        for u in inspector.get_unique_constraints(table_name) or []:
            for c in (u.get("column_names") or []):
                unique_cols.add(c)

        columns: list[ColumnMeta] = []
        for c in inspector.get_columns(table_name):
            c_name = c["name"]
            c_type = c.get("type")
            c_type_name = str(c_type).lower() if c_type is not None else "text"
            columns.append(
                ColumnMeta(
                    name=c_name,
                    type_name=c_type_name,
                    nullable=bool(c.get("nullable", True)),
                    primary_key=c_name in pk_cols,
                    unique=c_name in unique_cols,
                    max_length=_safe_len(c_type),
                )
            )

        fks: list[ForeignKey] = []
        for fk in inspector.get_foreign_keys(table_name) or []:
            ref_table = fk.get("referred_table")
            constrained = fk.get("constrained_columns") or []
            referred = fk.get("referred_columns") or []
            if not ref_table:
                continue
            for i, local_col in enumerate(constrained):
                ref_col = referred[min(i, len(referred) - 1)] if referred else "id"
                fks.append(ForeignKey(column=local_col, ref_table=ref_table, ref_column=ref_col))

        metas.append(TableMeta(name=table_name, columns=columns, foreign_keys=fks))

    return metas
