from ddl2data.generator.base import generate_all
from ddl2data.parser.ddl import parse_ddl_text
from ddl2data.parser.graph import generation_order
from ddl2data.writer.postgres import render_insert_sql


def test_check_constraint_generation_between_in_and_positive():
    ddl = """
    CREATE TABLE products (
      id INT PRIMARY KEY,
      status VARCHAR(10) NOT NULL,
      qty INT NOT NULL,
      price NUMERIC NOT NULL,
      CONSTRAINT ck_status CHECK (status IN ('A', 'B', 'C')),
      CONSTRAINT ck_qty CHECK (qty > 0),
      CONSTRAINT ck_price CHECK (price BETWEEN 10 AND 20)
    );
    """
    tables = parse_ddl_text(ddl)
    order = generation_order(tables)
    data = generate_all(tables, order, rows=40, dist_overrides={})
    rows = data["products"]

    assert all(r["status"] in {"A", "B", "C"} for r in rows)
    assert all(float(r["qty"]) > 0 for r in rows)
    assert all(10 <= float(r["price"]) <= 20 for r in rows)


def test_check_constraint_generation_regex_regexp_like():
    ddl = """
    CREATE TABLE regions (
      id INT PRIMARY KEY,
      code VARCHAR(2),
      CONSTRAINT ck_code CHECK (REGEXP_LIKE(code, '^[A-Z]{2}$'))
    );
    """
    tables = parse_ddl_text(ddl)
    order = generation_order(tables)
    data = generate_all(tables, order, rows=10, dist_overrides={})

    assert all(r["code"] == "KR" for r in data["regions"])


def test_per_table_row_counts_override_global_rows():
    ddl = """
    CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
    CREATE TABLE events (id INT PRIMARY KEY, user_id INT, FOREIGN KEY (user_id) REFERENCES users(id));
    """
    tables = parse_ddl_text(ddl)
    order = generation_order(tables)

    data = generate_all(
        tables,
        order,
        rows=3,
        dist_overrides={},
        table_rows={"users": 2, "events": 7},
    )

    assert len(data["users"]) == 2
    assert len(data["events"]) == 7


def test_bigquery_writer_dataset_table_and_insert_all():
    sql = render_insert_sql(
        "my_dataset.users",
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
        dialect="bigquery",
        bq_insert_all=True,
    )
    assert sql.startswith("INSERT ALL")
    assert "INTO `my_dataset.users`" in sql
    assert sql.strip().endswith("SELECT 1;")


def test_polars_engine_fallback_to_python_for_fk_and_checks():
    ddl = """
    CREATE TABLE users (id INT PRIMARY KEY, age INT, CONSTRAINT ck_age CHECK (age >= 18));
    CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, FOREIGN KEY (user_id) REFERENCES users(id));
    """
    tables = parse_ddl_text(ddl)
    order = generation_order(tables)

    data = generate_all(tables, order, rows=20, dist_overrides={}, engine="polars")

    assert len(data["users"]) == 20
    assert len(data["orders"]) == 20
    assert all(float(r["age"]) >= 18 for r in data["users"])
    user_ids = {r["id"] for r in data["users"]}
    assert all(r["user_id"] in user_ids for r in data["orders"])
