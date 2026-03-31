import json
from pathlib import Path

import pytest

from datagen.generator.base import generate_all
from datagen.parser.ddl import parse_ddl_text
from datagen.parser.graph import generation_order
from datagen.report import build_report
from datagen.validation import validate_check_constraints, validate_generated_data
from datagen.writer.csv_writer import write_csv
from datagen.writer.json_writer import write_json
from datagen.writer.postgres import render_insert_sql


MIXED_DDL = """
CREATE TABLE users (
  id INT PRIMARY KEY,
  email VARCHAR(100) UNIQUE NOT NULL,
  age INT,
  status VARCHAR(1),
  CONSTRAINT ck_age CHECK (age >= 18 AND age <= 120),
  CONSTRAINT ck_status CHECK (status IN ('A', 'B', 'C'))
);

CREATE TABLE orders (
  id INT PRIMARY KEY,
  user_id INT NOT NULL,
  amount NUMERIC,
  qty INT,
  CONSTRAINT ck_amount CHECK (amount BETWEEN 1 AND 10000),
  CONSTRAINT ck_qty CHECK (qty > 0),
  FOREIGN KEY (user_id) REFERENCES users(id)
);
"""


def test_e2e_mixed_constraints_with_per_table_rows():
    """Full pipeline: parse -> generate with per-table rows -> validate -> report."""
    tables = parse_ddl_text(MIXED_DDL)
    order = generation_order(tables)
    data = generate_all(
        tables,
        order,
        rows=10,
        dist_overrides={},
        table_rows={"users": 20, "orders": 50},
    )

    assert len(data["users"]) == 20
    assert len(data["orders"]) == 50

    for row in data["users"]:
        if row["age"] is not None:
            assert 18 <= row["age"] <= 120
        assert row["status"] in {"A", "B", "C"}

    for row in data["orders"]:
        if row["amount"] is not None:
            assert 1 <= float(row["amount"]) <= 10000
        if row["qty"] is not None:
            assert float(row["qty"]) > 0

    user_ids = {row["id"] for row in data["users"]}
    assert all(row["user_id"] in user_ids for row in data["orders"])

    validation = validate_generated_data(tables, data)
    assert validation["pass"] is True
    assert validation["counts"]["fk_violations"] == 0

    report = build_report(data, validation=validation)
    assert report["total_tables"] == 2
    assert report["tables"]["users"]["rows"] == 20
    assert report["validation"]["pass"] is True


def test_e2e_compound_and_or_checks():
    """AND/OR compound CHECK constraints generate valid data."""
    ddl = """
    CREATE TABLE items (
      id INT PRIMARY KEY,
      score INT,
      status VARCHAR(1),
      CONSTRAINT ck_score CHECK (score >= 0 AND score <= 100),
      CONSTRAINT ck_status CHECK (status IN ('A', 'B') OR status IN ('C', 'D'))
    );
    """
    tables = parse_ddl_text(ddl)
    order = generation_order(tables)
    data = generate_all(tables, order, rows=100, dist_overrides={})

    for row in data["items"]:
        if row["score"] is not None:
            assert 0 <= row["score"] <= 100
        assert row["status"] in {"A", "B", "C", "D"}


def test_e2e_bigquery_typed_literals():
    """BigQuery output uses DATE/TIMESTAMP typed literals."""
    ddl = """
    CREATE TABLE events (
      id INT PRIMARY KEY,
      event_date DATE,
      created_at TIMESTAMP
    );
    """
    tables = parse_ddl_text(ddl)
    order = generation_order(tables)
    data = generate_all(tables, order, rows=5, dist_overrides={})

    sql = render_insert_sql("events", data["events"], dialect="bigquery")
    assert "DATE '" in sql
    assert "TIMESTAMP '" in sql


def test_e2e_parquet_compression(tmp_path: Path):
    """Parquet output with compression creates files."""
    pytest.importorskip("polars")
    from datagen.writer.parquet_writer import write_parquet

    data = {"users": [{"id": i, "name": f"user_{i}"} for i in range(10)]}

    for codec in ["snappy", "zstd", "gzip", "none"]:
        out_dir = tmp_path / codec
        files = write_parquet(data, str(out_dir), compression=codec)
        assert len(files) == 1
        assert Path(files[0]).exists()


def test_e2e_strict_check_violations():
    """Strict CHECK mode detects violations in pre-built bad data."""
    ddl = "CREATE TABLE t (id INT PRIMARY KEY, age INT, CONSTRAINT ck CHECK (age >= 18));"
    tables = parse_ddl_text(ddl)

    bad_data = {"t": [{"id": 1, "age": 10}, {"id": 2, "age": 25}]}
    result = validate_check_constraints(tables, bad_data)

    assert result["check_violations"] >= 1
    assert result["check_details"][0]["type"] == "check"


def test_e2e_polars_fk_only_table():
    """Polars engine handles FK-only tables without falling back."""
    pytest.importorskip("polars")

    ddl = """
    CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
    CREATE TABLE logs (
      ts TIMESTAMP,
      user_id INT,
      msg TEXT,
      FOREIGN KEY (user_id) REFERENCES users(id)
    );
    """
    tables = parse_ddl_text(ddl)
    order = generation_order(tables)
    data = generate_all(tables, order, rows=50, dist_overrides={}, engine="polars")

    assert len(data["logs"]) == 50
    user_ids = {row["id"] for row in data["users"]}
    assert all(row["user_id"] in user_ids for row in data["logs"])


def test_e2e_all_output_formats(tmp_path: Path):
    """Generate data and render in all SQL dialects + JSON + CSV."""
    ddl = "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20));"
    tables = parse_ddl_text(ddl)
    order = generation_order(tables)
    data = generate_all(tables, order, rows=5, dist_overrides={})

    for dialect in ["postgres", "mysql", "sqlite", "bigquery"]:
        sql = render_insert_sql("t", data["t"], dialect=dialect)
        assert "INSERT" in sql

    json_out = write_json(data)
    assert "t" in json.loads(json_out)

    csv_files = write_csv(data, str(tmp_path / "csv"))
    assert len(csv_files) == 1
