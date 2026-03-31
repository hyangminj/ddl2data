from __future__ import annotations

from typing import Any


def _sql_literal(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v).replace("\\", "\\\\").replace("'", "''")
    return f"'{s}'"


def _quote_ident(name: str, dialect: str) -> str:
    if dialect == "bigquery":
        # Support dataset.table and project.dataset.table paths.
        clean = name.strip("`")
        return f"`{clean}`"
    if dialect == "mysql":
        return f"`{name}`"
    return f'"{name}"'  # postgres/sqlite style


def _render_insert_sql_python(
    table: str,
    rows: list[dict[str, Any]],
    dialect: str = "postgres",
    bq_insert_all: bool = False,
) -> str:
    cols = list(rows[0].keys())
    col_sql = ", ".join(_quote_ident(c, dialect) for c in cols)
    table_sql = _quote_ident(table, dialect)

    if dialect == "bigquery" and bq_insert_all:
        lines = [f"INSERT ALL"]
        for row in rows:
            values = ", ".join(_sql_literal(row.get(c)) for c in cols)
            lines.append(f"INTO {table_sql} ({col_sql}) VALUES ({values})")
        lines.append("SELECT 1;")
        return "\n".join(lines)

    lines: list[str] = []
    for row in rows:
        values = ", ".join(_sql_literal(row.get(c)) for c in cols)
        lines.append(f"INSERT INTO {table_sql} ({col_sql}) VALUES ({values});")
    return "\n".join(lines)


def _render_insert_sql_polars(
    table: str,
    rows: list[dict[str, Any]],
    dialect: str = "postgres",
    bq_insert_all: bool = False,
) -> str:
    try:
        import polars as pl
    except Exception as e:  # pragma: no cover - depends on optional dep
        raise RuntimeError("engine=polars requires optional dependency 'polars' (pip install polars)") from e

    cols = list(rows[0].keys())
    col_sql = ", ".join(_quote_ident(c, dialect) for c in cols)
    table_sql = _quote_ident(table, dialect)
    df = pl.DataFrame(rows)

    if dialect == "bigquery" and bq_insert_all:
        lines = ["INSERT ALL"]
        for tup in df.iter_rows(named=True):
            values = ", ".join(_sql_literal(tup.get(c)) for c in cols)
            lines.append(f"INTO {table_sql} ({col_sql}) VALUES ({values})")
        lines.append("SELECT 1;")
        return "\n".join(lines)

    lines: list[str] = []
    for tup in df.iter_rows(named=True):
        values = ", ".join(_sql_literal(tup.get(c)) for c in cols)
        lines.append(f"INSERT INTO {table_sql} ({col_sql}) VALUES ({values});")
    return "\n".join(lines)


def render_insert_sql(
    table: str,
    rows: list[dict[str, Any]],
    dialect: str = "postgres",
    engine: str = "python",
    bq_insert_all: bool = False,
) -> str:
    if not rows:
        return ""
    if engine == "polars":
        return _render_insert_sql_polars(table, rows, dialect=dialect, bq_insert_all=bq_insert_all)
    return _render_insert_sql_python(table, rows, dialect=dialect, bq_insert_all=bq_insert_all)
