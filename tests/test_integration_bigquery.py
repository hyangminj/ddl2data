from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import pytest

from ddl2data.writer.postgres import render_insert_sql

if TYPE_CHECKING:
    from google.cloud import bigquery


pytestmark = [pytest.mark.integration, pytest.mark.bigquery]


def _make_table_id(project: str, dataset: str, prefix: str = "ddl2data_it") -> str:
    return f"{project}.{dataset}.{prefix}_{uuid.uuid4().hex[:8]}"


def test_bigquery_render_insert_sql_literals_and_backticks() -> None:
    sql = render_insert_sql(
        "demo_dataset.events",
        [{"id": 1, "event_date": "2024-01-01", "created_at": "2024-01-01T12:30:00", "active": True}],
        dialect="bigquery",
    )

    assert "INSERT INTO `demo_dataset.events`" in sql
    assert "(`id`, `event_date`, `created_at`, `active`)" in sql
    assert "DATE '2024-01-01'" in sql
    assert "TIMESTAMP '2024-01-01T12:30:00'" in sql
    assert "TRUE" in sql


def test_bigquery_insert_all_executes(bq_client: bigquery.Client, bq_dataset: str) -> None:
    from google.cloud import bigquery

    table_id = _make_table_id(bq_client.project, bq_dataset, prefix="insert_all")
    table = bigquery.Table(
        table_id,
        schema=[
            bigquery.SchemaField("id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING"),
        ],
    )
    _ = bq_client.create_table(table)

    try:
        sql = render_insert_sql(
            table_id,
            [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
            dialect="bigquery",
            bq_insert_all=True,
        )
        _ = bq_client.query(sql).result()

        rows = list(bq_client.query(f"SELECT COUNT(*) AS count FROM `{table_id}`").result())
        assert rows[0]["count"] == 2
    finally:
        bq_client.delete_table(table_id, not_found_ok=True)


def test_bigquery_standard_insert_executes(bq_client: bigquery.Client, bq_dataset: str) -> None:
    from google.cloud import bigquery

    table_id = _make_table_id(bq_client.project, bq_dataset, prefix="standard_insert")
    table = bigquery.Table(
        table_id,
        schema=[
            bigquery.SchemaField("id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("event_date", "DATE"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ],
    )
    _ = bq_client.create_table(table)

    try:
        sql = render_insert_sql(
            table_id,
            [{"id": 1, "event_date": "2024-01-01", "created_at": "2024-01-01T09:00:00"}],
            dialect="bigquery",
        )
        _ = bq_client.query(sql).result()

        rows = list(bq_client.query(f"SELECT id FROM `{table_id}`").result())
        assert rows[0]["id"] == 1
    finally:
        bq_client.delete_table(table_id, not_found_ok=True)
