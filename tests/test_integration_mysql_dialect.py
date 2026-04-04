from __future__ import annotations

from datagen.writer.postgres import render_insert_sql


def test_mysql_render_insert_sql_uses_backticks() -> None:
    sql = render_insert_sql("users", [{"id": 1, "name": "alice"}], dialect="mysql")

    assert "INSERT INTO `users`" in sql
    assert "(`id`, `name`)" in sql


def test_mysql_render_insert_sql_handles_special_values() -> None:
    sql = render_insert_sql(
        "flags",
        [{"id": 1, "label": "O'Reilly", "enabled": True, "note": None}],
        dialect="mysql",
    )

    assert "O''Reilly" in sql
    assert "TRUE" in sql
    assert "NULL" in sql
