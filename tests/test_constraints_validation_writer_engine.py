import ipaddress
from pathlib import Path
import re

import pytest
from sqlalchemy import create_engine, text

import datagen.generator.base as generator_base
from datagen.generator.base import generate_all
from datagen.parser.ddl import parse_ddl_text
from datagen.parser.graph import generation_order
from datagen.parser.introspect import load_schema_from_db
from datagen.validation import validate_generated_data
from datagen.writer.csv_writer import write_csv
from datagen.writer.json_writer import write_json
from datagen.writer.postgres import render_insert_sql
from datagen.writer.parquet_writer import write_parquet


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
    assert all(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email) for email in emails)


def test_email_generation_respects_format_and_length():
    ddl = """
    CREATE TABLE users (
      id INT PRIMARY KEY,
      contact_email VARCHAR(16) NOT NULL
    );
    """
    tables = parse_ddl_text(ddl)
    order = generation_order(tables)
    data = generate_all(tables, order, rows=25, dist_overrides={})
    emails = [r["contact_email"] for r in data["users"]]

    assert all(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email) for email in emails)
    assert all(len(email) <= 16 for email in emails)


def test_named_string_columns_use_field_aware_generation(monkeypatch: pytest.MonkeyPatch):
    ddl = """
    CREATE TABLE contacts (
      id INT PRIMARY KEY,
      username VARCHAR(24) NOT NULL,
      ip_address VARCHAR(45) NOT NULL,
      street_address VARCHAR(32) NOT NULL,
      home_address VARCHAR(48) NOT NULL,
      city VARCHAR(24) NOT NULL,
      state VARCHAR(24) NOT NULL,
      postal_code VARCHAR(12) NOT NULL,
      country VARCHAR(24) NOT NULL,
      phone_number VARCHAR(24) NOT NULL
    );
    """

    monkeypatch.setattr(generator_base.fake, "user_name", lambda: "demo.user")
    monkeypatch.setattr(generator_base.fake, "ipv4", lambda: "203.0.113.10")
    monkeypatch.setattr(generator_base.fake, "street_address", lambda: "123 Test St")
    monkeypatch.setattr(generator_base.fake, "address", lambda: "500 Test Ave\nSuite 9")
    monkeypatch.setattr(generator_base.fake, "city", lambda: "Seoul")
    monkeypatch.setattr(generator_base.fake, "state", lambda: "Seoul Special City")
    monkeypatch.setattr(generator_base.fake, "postcode", lambda: "04524")
    monkeypatch.setattr(generator_base.fake, "country", lambda: "Korea")
    monkeypatch.setattr(generator_base.fake, "phone_number", lambda: "+82-2-555-0100")

    tables = parse_ddl_text(ddl)
    order = generation_order(tables)
    data = generate_all(tables, order, rows=2, dist_overrides={})

    for row in data["contacts"]:
        assert row["username"] == "demo.user"
        assert ipaddress.ip_address(row["ip_address"])
        assert row["street_address"] == "123 Test St"
        assert row["home_address"] == "500 Test Ave, Suite 9"
        assert row["city"] == "Seoul"
        assert row["state"] == "Seoul Special City"
        assert row["postal_code"] == "04524"
        assert row["country"] == "Korea"
        assert row["phone_number"] == "+82-2-555-0100"


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


def test_parquet_writer_clear_error_without_dependency(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "polars":
            raise ModuleNotFoundError("No module named polars")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(RuntimeError, match="out parquet"):
        write_parquet({"users": [{"id": 1}]}, str(tmp_path))
