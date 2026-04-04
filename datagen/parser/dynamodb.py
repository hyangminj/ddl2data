from __future__ import annotations

from collections.abc import Iterable
import importlib
import os
from typing import Any, Protocol

from datagen.config import ColumnMeta, TableMeta

DYNAMODB_ATTR_TYPE_ALIASES = {
    "s": ("S", "text"),
    "str": ("S", "text"),
    "string": ("S", "text"),
    "text": ("S", "text"),
    "uuid": ("S", "uuid"),
    "date": ("S", "date"),
    "datetime": ("S", "datetime"),
    "n": ("N", "numeric"),
    "number": ("N", "numeric"),
    "numeric": ("N", "numeric"),
    "decimal": ("N", "numeric"),
    "float": ("N", "float"),
    "double": ("N", "float"),
    "int": ("N", "int"),
    "integer": ("N", "int"),
    "bigint": ("N", "int"),
    "bool": ("BOOL", "boolean"),
    "boolean": ("BOOL", "boolean"),
    "b": ("B", "text"),
    "binary": ("B", "text"),
    "bytes": ("B", "text"),
}


class DynamoDBClientProtocol(Protocol):
    def describe_table(self, *, TableName: str) -> dict[str, Any]: ...


def _get_dynamodb_client(region_name: str | None = None):
    try:
        boto3 = importlib.import_module("boto3")
    except ModuleNotFoundError as e:
        raise RuntimeError(
            "DynamoDB schema loading requires boto3. Install with: pip install boto3"
        ) from e
    endpoint_url = os.environ.get("DYNAMODB_ENDPOINT_URL")
    return boto3.client("dynamodb", region_name=region_name, endpoint_url=endpoint_url)


def _ddb_column(name: str, attr_type: str, *, generator_type: str, nullable: bool, primary_key: bool, source: str) -> ColumnMeta:
    return ColumnMeta(
        name=name,
        type_name=generator_type,
        nullable=nullable,
        primary_key=primary_key,
        unique=primary_key,
        extra={"dynamodb_attr_type": attr_type, "dynamodb_source": source},
    )


def _canonical_dynamodb_attr_spec(raw_type: str) -> tuple[str, str]:
    canonical = DYNAMODB_ATTR_TYPE_ALIASES.get(raw_type.strip().lower())
    if canonical is None:
        supported = ", ".join(sorted(DYNAMODB_ATTR_TYPE_ALIASES))
        raise ValueError(f"Unsupported DynamoDB attribute type '{raw_type}'. Use one of: {supported}")
    return canonical


def parse_dynamodb_extra_attrs(raw_entries: Iterable[str] | None) -> list[ColumnMeta]:
    columns: list[ColumnMeta] = []
    seen: set[str] = set()

    for entry in raw_entries or []:
        for token in [t.strip() for t in str(entry).split(",") if t.strip()]:
            if ":" not in token:
                raise ValueError(f"Invalid --dynamodb-extra-attr token '{token}'. Use name:type")
            name, raw_type = token.split(":", 1)
            attr_name = name.strip()
            if not attr_name:
                raise ValueError(f"Invalid --dynamodb-extra-attr token '{token}'. Attribute name cannot be empty")
            attr_type, generator_type = _canonical_dynamodb_attr_spec(raw_type)
            if attr_name in seen:
                raise ValueError(f"Duplicate DynamoDB extra attribute '{attr_name}'")
            seen.add(attr_name)
            columns.append(
                _ddb_column(
                    attr_name,
                    attr_type,
                    generator_type=generator_type,
                    nullable=True,
                    primary_key=False,
                    source="extra",
                )
            )

    return columns


def _generator_type_from_dynamodb_attr_type(attr_type: str) -> str:
    if attr_type == "S":
        return "text"
    if attr_type == "N":
        return "numeric"
    if attr_type == "BOOL":
        return "boolean"
    if attr_type == "B":
        return "text"
    raise ValueError(f"Unsupported DynamoDB attribute type '{attr_type}'")


def _merge_columns(base: list[ColumnMeta], extra: list[ColumnMeta]) -> list[ColumnMeta]:
    merged: dict[str, ColumnMeta] = {col.name: col for col in base}
    for col in extra:
        existing = merged.get(col.name)
        if existing is None:
            merged[col.name] = col
            continue
        if existing.extra.get("dynamodb_attr_type") != col.extra.get("dynamodb_attr_type"):
            raise ValueError(f"DynamoDB extra attribute '{col.name}' conflicts with table schema type")
    return list(merged.values())


def load_schema_from_dynamodb(
    table_name: str,
    *,
    region_name: str | None = None,
    extra_attrs: list[ColumnMeta] | None = None,
    client: DynamoDBClientProtocol | None = None,
) -> list[TableMeta]:
    dynamodb = client if client is not None else _get_dynamodb_client(region_name)
    response = dynamodb.describe_table(TableName=table_name)
    description = response.get("Table", {})

    attr_defs = {
        item["AttributeName"]: item["AttributeType"]
        for item in description.get("AttributeDefinitions", [])
        if item.get("AttributeName") and item.get("AttributeType")
    }

    base_key_names = {item.get("AttributeName") for item in description.get("KeySchema", []) if item.get("AttributeName")}
    key_names: list[str] = []
    for section in [description.get("KeySchema", [])]:
        for item in section:
            attr_name = item.get("AttributeName")
            if attr_name and attr_name not in key_names:
                key_names.append(attr_name)

    for index_group in (description.get("LocalSecondaryIndexes", []), description.get("GlobalSecondaryIndexes", [])):
        for index in index_group:
            for item in index.get("KeySchema", []):
                attr_name = item.get("AttributeName")
                if attr_name and attr_name not in key_names:
                    key_names.append(attr_name)

    columns: list[ColumnMeta] = []
    for attr_name in key_names:
        attr_type = attr_defs.get(attr_name)
        if attr_type is None:
            raise RuntimeError(f"DynamoDB attribute definition missing for key '{attr_name}' on table '{table_name}'")
        columns.append(
            _ddb_column(
                attr_name,
                attr_type,
                generator_type=_generator_type_from_dynamodb_attr_type(attr_type),
                nullable=attr_name not in base_key_names,
                primary_key=attr_name in base_key_names,
                source="key",
            )
        )

    merged_columns = _merge_columns(columns, extra_attrs or [])
    return [TableMeta(name=description.get("TableName", table_name), columns=merged_columns)]
