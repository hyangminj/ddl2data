from __future__ import annotations

from collections.abc import Callable
import json

import pytest

from datagen.generator.base import generate_all
from datagen.parser.dynamodb import load_schema_from_dynamodb, parse_dynamodb_extra_attrs
from datagen.writer.dynamodb_json_writer import write_dynamodb_json
from tests.conftest import DynamoDBTestClient, wait_for_dynamodb_table


pytestmark = [pytest.mark.integration, pytest.mark.dynamodb]


def test_dynamodb_loads_hash_and_range_keys(
    dynamodb_client: DynamoDBTestClient, dynamodb_table_factory: Callable[[str], str]
) -> None:
    table_name = dynamodb_table_factory("users")
    _ = dynamodb_client.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "N"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_dynamodb_table(dynamodb_client, table_name)

    tables = load_schema_from_dynamodb(table_name, client=dynamodb_client)
    users = tables[0]
    by_name = {column.name: column for column in users.columns}

    assert users.name == table_name
    assert len(users.columns) == 2
    assert by_name["pk"].primary_key is True
    assert by_name["pk"].nullable is False
    assert by_name["sk"].primary_key is True
    assert by_name["sk"].nullable is False


def test_dynamodb_loads_gsi_and_lsi_keys(
    dynamodb_client: DynamoDBTestClient, dynamodb_table_factory: Callable[[str], str]
) -> None:
    table_name = dynamodb_table_factory("events")
    _ = dynamodb_client.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsi1pk", "AttributeType": "S"},
            {"AttributeName": "lsi1sk", "AttributeType": "N"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        LocalSecondaryIndexes=[
            {
                "IndexName": "lsi1",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "lsi1sk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi1",
                "KeySchema": [{"AttributeName": "gsi1pk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
                "BillingMode": "PAY_PER_REQUEST",
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_dynamodb_table(dynamodb_client, table_name)

    tables = load_schema_from_dynamodb(table_name, client=dynamodb_client)
    by_name = {column.name: column for column in tables[0].columns}

    assert "gsi1pk" in by_name
    assert "lsi1sk" in by_name
    assert by_name["gsi1pk"].primary_key is False
    assert by_name["gsi1pk"].nullable is True
    assert by_name["lsi1sk"].primary_key is False
    assert by_name["lsi1sk"].nullable is True


def test_dynamodb_merges_extra_attributes(
    dynamodb_client: DynamoDBTestClient, dynamodb_table_factory: Callable[[str], str]
) -> None:
    table_name = dynamodb_table_factory("profiles")
    _ = dynamodb_client.create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_dynamodb_table(dynamodb_client, table_name)

    extras = parse_dynamodb_extra_attrs(["email:string", "score:int"])
    tables = load_schema_from_dynamodb(table_name, client=dynamodb_client, extra_attrs=extras)
    by_name = {column.name: column for column in tables[0].columns}

    assert len(tables[0].columns) == 3
    assert by_name["email"].extra["dynamodb_attr_type"] == "S"
    assert by_name["score"].extra["dynamodb_attr_type"] == "N"
    assert by_name["score"].type_name == "int"


def test_dynamodb_json_writer_renders_typed_payloads(
    dynamodb_client: DynamoDBTestClient, dynamodb_table_factory: Callable[[str], str]
) -> None:
    table_name = dynamodb_table_factory("users")
    _ = dynamodb_client.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "N"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_dynamodb_table(dynamodb_client, table_name)

    tables = load_schema_from_dynamodb(
        table_name,
        client=dynamodb_client,
        extra_attrs=parse_dynamodb_extra_attrs(["email:string", "score:int"]),
    )
    order = [table_name]
    data = generate_all(tables, order, rows=2, dist_overrides={})
    payload = write_dynamodb_json(tables, data)
    assert isinstance(payload, str)
    lines = [json.loads(line) for line in payload.splitlines()]

    assert len(lines) == 2
    assert set(lines[0]["Item"]).issuperset({"pk", "sk", "email", "score"})
    assert set(lines[0]["Item"]["pk"]) == {"S"}
    assert set(lines[0]["Item"]["sk"]) == {"N"}
    assert set(lines[0]["Item"]["email"]) == {"S"}
    assert set(lines[0]["Item"]["score"]) == {"N"}
