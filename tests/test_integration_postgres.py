from __future__ import annotations

from collections.abc import Callable

import pytest
from sqlalchemy.engine import Engine
from sqlalchemy import text

from ddl2data.cli import _insert_via_sqlalchemy
from ddl2data.parser.graph import generation_order
from ddl2data.parser.introspect import load_schema_from_db


pytestmark = [pytest.mark.integration, pytest.mark.postgres]


def test_postgres_introspect_basic_schema(
    postgres_engine: Engine, postgres_table_factory: Callable[[str], str]
) -> None:
    table_name = postgres_table_factory("users")

    with postgres_engine.begin() as conn:
        _ = conn.execute(
            text(
                f'''
                CREATE TABLE "{table_name}" (
                    id INTEGER PRIMARY KEY,
                    email VARCHAR(120) UNIQUE NOT NULL,
                    age INTEGER
                )
                '''
            )
        )

    tables = load_schema_from_db(postgres_engine, tables=[table_name])
    users = tables[0]
    by_name = {column.name: column for column in users.columns}

    assert users.name == table_name
    assert by_name["id"].primary_key is True
    assert by_name["email"].nullable is False
    assert by_name["email"].unique is True


def test_postgres_introspect_fk_and_generation_order(
    postgres_engine: Engine, postgres_table_factory: Callable[[str], str]
) -> None:
    users_table = postgres_table_factory("users")
    orders_table = postgres_table_factory("orders")

    with postgres_engine.begin() as conn:
        _ = conn.execute(
            text(
                f'''
                CREATE TABLE "{users_table}" (
                    id INTEGER PRIMARY KEY,
                    email TEXT NOT NULL
                )
                '''
            )
        )
        _ = conn.execute(
            text(
                f'''
                CREATE TABLE "{orders_table}" (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES "{users_table}"(id),
                    amount NUMERIC
                )
                '''
            )
        )

    tables = load_schema_from_db(postgres_engine, tables=[users_table, orders_table])
    order = generation_order(tables)
    orders = {table.name: table for table in tables}[orders_table]

    assert len(orders.foreign_keys) == 1
    assert orders.foreign_keys[0].column == "user_id"
    assert orders.foreign_keys[0].ref_table == users_table
    assert orders.foreign_keys[0].ref_column == "id"
    assert order.index(users_table) < order.index(orders_table)


def test_postgres_introspect_check_and_unique_constraints(
    postgres_engine: Engine, postgres_table_factory: Callable[[str], str]
) -> None:
    table_name = postgres_table_factory("accounts")

    with postgres_engine.begin() as conn:
        _ = conn.execute(
            text(
                f'''
                CREATE TABLE "{table_name}" (
                    id INTEGER PRIMARY KEY,
                    email TEXT NOT NULL,
                    age INTEGER,
                    CONSTRAINT uq_{table_name}_email UNIQUE (email),
                    CONSTRAINT ck_{table_name}_age CHECK (age >= 18)
                )
                '''
            )
        )

    tables = load_schema_from_db(postgres_engine, tables=[table_name])
    accounts = tables[0]

    assert any(unique.columns == ["email"] for unique in accounts.unique_constraints)
    assert any(check.name == f"ck_{table_name}_age" for check in accounts.check_constraints)
    assert any("age >= 18" in check.expression.lower() for check in accounts.check_constraints)


def test_postgres_insert_via_sqlalchemy_inserts_rows(
    postgres_engine: Engine, postgres_table_factory: Callable[[str], str], postgres_url: str
) -> None:
    table_name = postgres_table_factory("seed")

    with postgres_engine.begin() as conn:
        _ = conn.execute(
            text(
                f'''
                CREATE TABLE "{table_name}" (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                )
                '''
            )
        )

    _insert_via_sqlalchemy(postgres_url, {table_name: [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]})

    with postgres_engine.connect() as conn:
        count = int(conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar_one())

    assert count == 2
