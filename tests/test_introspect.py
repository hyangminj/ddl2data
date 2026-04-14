from sqlalchemy import create_engine, text

from ddl2data.parser.graph import generation_order
from ddl2data.parser.introspect import load_schema_from_db


def test_load_schema_from_db_sqlite_fk_order():
    engine = create_engine("sqlite+pysqlite:///:memory:")

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE users (
                  id INTEGER PRIMARY KEY,
                  email TEXT NOT NULL,
                  age INTEGER
                );
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE orders (
                  id INTEGER PRIMARY KEY,
                  user_id INTEGER NOT NULL,
                  amount REAL,
                  FOREIGN KEY(user_id) REFERENCES users(id)
                );
                """
            )
        )

    tables = load_schema_from_db(engine)
    by_name = {t.name: t for t in tables}

    assert "users" in by_name
    assert "orders" in by_name

    users = by_name["users"]
    orders = by_name["orders"]

    assert any(c.name == "id" and c.primary_key for c in users.columns)
    assert any(c.name == "email" and c.nullable is False for c in users.columns)

    assert len(orders.foreign_keys) == 1
    assert orders.foreign_keys[0].column == "user_id"
    assert orders.foreign_keys[0].ref_table == "users"
    assert orders.foreign_keys[0].ref_column == "id"

    order = generation_order(tables)
    assert order.index("users") < order.index("orders")


def test_load_schema_from_db_with_table_filter():
    engine = create_engine("sqlite+pysqlite:///:memory:")

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);"))
        conn.execute(text("CREATE TABLE events (id INTEGER PRIMARY KEY, user_id INTEGER);"))

    tables = load_schema_from_db(engine, tables=["users"])

    assert len(tables) == 1
    assert tables[0].name == "users"
