from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from datagen.generator.base import generate_all
from datagen.parser.ddl import parse_ddl_text
from datagen.parser.graph import generation_order
from datagen.parser.introspect import load_schema_from_db
from datagen.validation import validate_generated_data
from datagen.writer.csv_writer import write_csv
from datagen.writer.json_writer import write_json
from datagen.writer.postgres import render_insert_sql


def test_parse_ddl_captures_unique_and_check_constraints():
    ddl = """
    CREATE TABLE users (
      id INT PRIMARY KEY,
      email VARCHAR(120) UNIQUE,
      age INT,
      CONSTRAINT uq_users_email_age UNIQUE (email, age),
      CONSTRAINT ck_users_age CHECK (age >= 18)
    );
    """
    tables = parse_ddl_text(ddl)
    users = tables[0]

    assert any(c.name == "email" and c.unique for c in users.columns)
    assert any(len(u.columns) == 2 and u.columns == ["email", "age"] for u in users.unique_constraints)
    assert any("age" in ck.expression.lower() for ck in users.check_constraints)


def test_introspect_captures_composite_unique_and_checks_sqlite():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    email TEXT NOT NULL,
                    age INTEGER,
                    CONSTRAINT uq_users_email_age UNIQUE (email, age),
                    CONSTRAINT ck_users_age CHECK (age >= 18)
                );
                """
            )
        )

    tables = load_schema_from_db(engine)
    users = {t.name: t for t in tables}["users"]

    assert any(u.columns == ["email", "age"] for u in users.unique_constraints)
    assert any("age" in ck.expression.lower() for ck in users.check_constraints)


def test_unique_aware_generation_avoids_obvious_duplicates():
    ddl = """
    CREATE TABLE users (
      id INT PRIMARY KEY,
      email VARCHAR(32) UNIQUE,
      age INT
    );
    """
    tables = parse_ddl_text(ddl)
    order = generation_order(tables)
    data = generate_all(tables, order, rows=50, dist_overrides={})
    emails = [r["email"] for r in data["users"] if r.get("email") is not None]

    assert len(set(emails)) == len(emails)


def test_validation_detects_non_null_fk_and_unique_collisions():
    ddl = """
    CREATE TABLE users (
      id INT PRIMARY KEY,
      email VARCHAR(30) UNIQUE NOT NULL
    );
    CREATE TABLE orders (
      id INT PRIMARY KEY,
      user_id INT NOT NULL,
      FOREIGN KEY (user_id) REFERENCES users(id)
    );
    """
    tables = parse_ddl_text(ddl)
    data = {
        "users": [
            {"id": 1, "email": "a@example.com"},
            {"id": 2, "email": "a@example.com"},
        ],
        "orders": [
            {"id": 10, "user_id": 999},
            {"id": 11, "user_id": None},
        ],
    }

    summary = validate_generated_data(tables, data)
    assert summary["pass"] is False
    assert summary["counts"]["fk_violations"] == 1
    assert summary["counts"]["non_null_violations"] == 1
    assert summary["counts"]["unique_collisions"] >= 1


def test_bigquery_writer_uses_backticks():
    sql = render_insert_sql("users", [{"id": 1, "name": "alice"}], dialect="bigquery")
    assert "INSERT INTO `users`" in sql
    assert "(`id`, `name`)" in sql


def test_polars_engine_clear_error_without_dependency(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "polars":
            raise ModuleNotFoundError("No module named polars")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(RuntimeError, match="engine=polars"):
        write_json({"users": [{"id": 1}]}, engine="polars")

    with pytest.raises(RuntimeError, match="engine=polars"):
        write_csv({"users": [{"id": 1}]}, str(tmp_path), engine="polars")

    with pytest.raises(RuntimeError, match="engine=polars"):
        render_insert_sql("users", [{"id": 1}], engine="polars")
