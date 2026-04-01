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
from datagen.config import ColumnMeta


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


def test_integer_primary_keys_do_not_use_zero_edge_values():
    ddl = """
    CREATE TABLE users (
      id INT PRIMARY KEY,
      age INT NOT NULL
    );
    """
    tables = parse_ddl_text(ddl)
    order = generation_order(tables)
    data = generate_all(tables, order, rows=25, dist_overrides={})
    ids = [r["id"] for r in data["users"]]

    assert ids[0] == 1
    assert ids[20] == 21
    assert all(i > 0 for i in ids)
    assert len(set(ids)) == len(ids)


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
        assert row["username"].startswith("demo.user") or row["username"].startswith("user.edge")
        assert ipaddress.ip_address(row["ip_address"])
        assert row["street_address"].startswith("123 Test St") or row["street_address"].startswith("9999 Edge")
        assert row["home_address"].startswith("500 Test Ave, Suite 9") or row["home_address"].startswith("9999 Edge")
        assert row["city"].startswith("Seoul") or row["city"].startswith("EdgeCity")
        assert row["state"].startswith("Seoul Special City") or row["state"].startswith("EdgeState")
        assert row["postal_code"].startswith("04524") or row["postal_code"].startswith("99999")
        assert row["country"].startswith("Korea") or row["country"].startswith("EdgeCountry")
        assert row["phone_number"].startswith("+82-2-555-0100") or row["phone_number"].startswith("555-0100")


def test_unique_named_string_columns_use_unique_tokens(monkeypatch: pytest.MonkeyPatch):
    ddl = """
    CREATE TABLE contacts (
      id INT PRIMARY KEY,
      ip_address VARCHAR(15) UNIQUE NOT NULL,
      country VARCHAR(8) UNIQUE NOT NULL
    );
    """

    monkeypatch.setattr(generator_base.fake, "ipv4", lambda: "203.0.113.10")
    monkeypatch.setattr(generator_base.fake, "country", lambda: "Korea")

    tables = parse_ddl_text(ddl)
    order = generation_order(tables)
    data = generate_all(tables, order, rows=300, dist_overrides={})

    ips = [row["ip_address"] for row in data["contacts"]]
    countries = [row["country"] for row in data["contacts"]]

    assert len(set(ips)) == len(ips)
    assert len(set(countries)) == len(countries)
    assert all(len(value) <= 15 for value in ips)
    assert all(len(value) <= 8 for value in countries)


def test_edge_structured_values_stretch_to_column_length(monkeypatch: pytest.MonkeyPatch):
    ddl = """
    CREATE TABLE contacts (
      id INT PRIMARY KEY,
      username VARCHAR(12) NOT NULL,
      city VARCHAR(10) NOT NULL,
      contact_email VARCHAR(16) NOT NULL
    );
    """

    monkeypatch.setattr(generator_base.fake, "user_name", lambda: "demo")
    monkeypatch.setattr(generator_base.fake, "city", lambda: "Seoul")

    tables = parse_ddl_text(ddl)
    order = generation_order(tables)
    row = generate_all(tables, order, rows=1, dist_overrides={})["contacts"][0]

    assert len(row["username"]) == 12
    assert len(row["city"]) == 10
    assert len(row["contact_email"]) == 16
    assert re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", row["contact_email"])


def test_fit_helpers_preserve_unique_suffixes_and_zero_length_behavior():
    short_col = ColumnMeta(name="country", type_name="VARCHAR", max_length=2)
    zero_col = ColumnMeta(name="country", type_name="VARCHAR", max_length=0)

    assert generator_base._fit_prefix(short_col, "100", "Korea") == "00"
    assert generator_base._fit_prefix(short_col, "101", "Korea") == "01"
    assert generator_base._fit_suffix(zero_col, "", "abcdef", sep="") == ""
    assert generator_base._truncate_string(zero_col, "abcdef") == ""


def test_unique_ip_variants_keep_distinct_suffixes_when_truncated():
    ipv4_col = ColumnMeta(name="ipv4_address", type_name="VARCHAR", max_length=7)
    ip_col = ColumnMeta(name="ip_address", type_name="VARCHAR", max_length=7)
    ipv6_col = ColumnMeta(name="ipv6_address", type_name="VARCHAR", max_length=8)

    ipv4_a = generator_base._structured_string_value(ipv4_col, unique_token=1)
    ipv4_b = generator_base._structured_string_value(ipv4_col, unique_token=257)
    ip_a = generator_base._structured_string_value(ip_col, unique_token=1)
    ip_b = generator_base._structured_string_value(ip_col, unique_token=257)
    ipv6_a = generator_base._structured_string_value(ipv6_col, unique_token=16)
    ipv6_b = generator_base._structured_string_value(ipv6_col, unique_token=17)

    assert ipv4_a != ipv4_b
    assert ip_a != ip_b
    assert ipv6_a != ipv6_b
    assert len(ipv4_a or "") <= 7
    assert len(ipv4_b or "") <= 7
    assert len(ip_a or "") <= 7
    assert len(ip_b or "") <= 7
    assert len(ipv6_a or "") <= 8
    assert len(ipv6_b or "") <= 8


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
