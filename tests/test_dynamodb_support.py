import json

import pytest

from datagen.cli import main
from datagen.parser.dynamodb import load_schema_from_dynamodb, parse_dynamodb_extra_attrs
from datagen.writer.dynamodb_json_writer import write_dynamodb_json


class FakeDynamoDBClient:
    def describe_table(self, TableName: str) -> dict[str, object]:
        assert TableName == "users"
        return {
            "Table": {
                "TableName": "users",
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "N"},
                    {"AttributeName": "gsi1pk", "AttributeType": "S"},
                ],
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ],
                "GlobalSecondaryIndexes": [
                    {
                        "IndexName": "gsi1",
                        "KeySchema": [{"AttributeName": "gsi1pk", "KeyType": "HASH"}],
                    }
                ],
            }
        }


def test_load_schema_from_dynamodb_merges_keys_and_extra_attrs():
    extras = parse_dynamodb_extra_attrs(["email:string", "active:boolean"])
    tables = load_schema_from_dynamodb("users", client=FakeDynamoDBClient(), extra_attrs=extras)
    users = tables[0]
    by_name = {col.name: col for col in users.columns}

    assert users.name == "users"
    assert by_name["pk"].primary_key is True
    assert by_name["pk"].nullable is False
    assert by_name["pk"].extra["dynamodb_attr_type"] == "S"
    assert by_name["sk"].primary_key is True
    assert by_name["gsi1pk"].primary_key is False
    assert by_name["gsi1pk"].nullable is True
    assert by_name["email"].extra["dynamodb_attr_type"] == "S"
    assert by_name["active"].extra["dynamodb_attr_type"] == "BOOL"


def test_write_dynamodb_json_renders_typed_json_lines():
    tables = load_schema_from_dynamodb(
        "users",
        client=FakeDynamoDBClient(),
        extra_attrs=parse_dynamodb_extra_attrs(["email:string", "active:boolean"]),
    )
    payload = write_dynamodb_json(
        tables,
        {
            "users": [
                {"pk": "user#1", "sk": 1, "email": "a@example.com", "active": True},
                {"pk": "user#2", "sk": 2, "email": None, "active": False},
            ]
        },
    )
    assert isinstance(payload, str)
    lines = [json.loads(line) for line in payload.splitlines()]

    assert lines[0]["Item"]["pk"] == {"S": "user#1"}
    assert lines[0]["Item"]["sk"] == {"N": "1"}
    assert lines[0]["Item"]["email"] == {"S": "a@example.com"}
    assert lines[0]["Item"]["active"] == {"BOOL": True}
    assert lines[1]["Item"]["email"] == {"NULL": True}


def test_main_supports_dynamodb_schema_and_json_output(tmp_path, monkeypatch: pytest.MonkeyPatch):
    def fake_load_schema_from_dynamodb(table_name: str, *, region_name=None, extra_attrs=None):
        assert table_name == "users"
        assert region_name == "ap-northeast-2"
        assert extra_attrs is not None and len(extra_attrs) == 2
        return load_schema_from_dynamodb("users", client=FakeDynamoDBClient(), extra_attrs=extra_attrs)

    monkeypatch.setattr("datagen.cli.load_schema_from_dynamodb", fake_load_schema_from_dynamodb)
    output_path = tmp_path / "users.ddb.json"
    monkeypatch.setattr(
        "sys.argv",
        [
            "datagen",
            "--schema-from-dynamodb",
            "--dynamodb-table",
            "users",
            "--dynamodb-region",
            "ap-northeast-2",
            "--dynamodb-extra-attr",
            "email:string",
            "--dynamodb-extra-attr",
            "active:boolean",
            "--rows",
            "2",
            "--out",
            "dynamodb-json",
            "--output-path",
            str(output_path),
            "--seed",
            "7",
        ],
    )

    main()
    lines = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()]

    assert len(lines) == 2
    assert "pk" in lines[0]["Item"]
    assert "sk" in lines[0]["Item"]
    assert "email" in lines[0]["Item"]
    assert "active" in lines[0]["Item"]


def test_parse_dynamodb_extra_attrs_rejects_invalid_tokens():
    with pytest.raises(ValueError, match="Invalid --dynamodb-extra-attr token 'broken'"):
        parse_dynamodb_extra_attrs(["broken"])
