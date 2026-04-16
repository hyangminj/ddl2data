from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from ddl2data.cli import _merge_config, _parse_dist_map, _resolve_tables_from_args, build_parser
from ddl2data.config import CheckConstraintMeta, ColumnMeta, TableMeta
from ddl2data.config_loader import load_config
from ddl2data.validation import _check_column_values, _evaluate_check, validate_check_constraints
from ddl2data.writer.csv_writer import write_csv
from ddl2data.writer.dynamodb_json_writer import _attribute_value, write_dynamodb_json
from ddl2data.writer.json_writer import write_json


def _make_args(**overrides: object) -> argparse.Namespace:
    base = {
        "schema_from_db": False,
        "schema_from_dynamodb": False,
        "dynamodb_table": None,
        "dynamodb_extra_attr": None,
        "dynamodb_region": None,
        "db_url": None,
        "tables": None,
        "ddl": None,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_merge_config_applies_defaults_and_config_maps(monkeypatch: pytest.MonkeyPatch):
    args = build_parser().parse_args(["--config", "cfg.toml"])

    monkeypatch.setattr(
        "ddl2data.cli.load_config",
        lambda _path: {
            "ddl": "schema.sql",
            "rows": 5,
            "dist": ["users.age:normal,mean=30,std=3"],
            "table_rows": {"users": 2, "orders": 7},
            "dynamodb_extra_attr": {"email": "string", "active": "boolean"},
        },
    )

    merged = _merge_config(args)

    assert merged.ddl == "schema.sql"
    assert merged.rows == 5
    assert merged.out == "postgres"
    assert merged.engine == "python"
    assert merged.dist == ["users.age:normal,mean=30,std=3"]
    assert set(merged.table_rows) == {"users=2", "orders=7"}
    assert set(merged.dynamodb_extra_attr) == {"email:string", "active:boolean"}
    assert merged.strict_checks is False
    assert merged.bq_insert_all is False


def test_merge_config_prefers_cli_values(monkeypatch: pytest.MonkeyPatch):
    args = build_parser().parse_args(
        [
            "--config",
            "cfg.toml",
            "--rows",
            "3",
            "--out",
            "json",
            "--dist",
            "orders.amount:uniform,min=1,max=9",
            "--table-rows",
            "users=9",
            "--dynamodb-extra-attr",
            "email:string",
        ]
    )

    monkeypatch.setattr(
        "ddl2data.cli.load_config",
        lambda _path: {
            "ddl": "schema.sql",
            "rows": 10,
            "out": "postgres",
            "dist": ["users.age:normal,mean=30,std=3"],
            "table_rows": {"users": 2},
            "dynamodb_extra_attr": {"active": "boolean"},
        },
    )

    merged = _merge_config(args)

    assert merged.rows == 3
    assert merged.out == "json"
    assert merged.dist == ["orders.amount:uniform,min=1,max=9"]
    assert merged.table_rows == ["users=9"]
    assert merged.dynamodb_extra_attr == ["email:string"]


def test_merge_config_uses_table_rows_when_rows_missing(monkeypatch: pytest.MonkeyPatch):
    args = build_parser().parse_args(["--config", "cfg.toml"])
    monkeypatch.setattr("ddl2data.cli.load_config", lambda _path: {"ddl": "schema.sql", "table_rows": ["users=4"]})

    merged = _merge_config(args)

    assert merged.rows == 0
    assert merged.table_rows == ["users=4"]


def test_parse_dist_map_rejects_invalid_spec():
    with pytest.raises(SystemExit, match="Invalid --dist token 'broken'"):
        _parse_dist_map(["broken"])


def test_resolve_tables_from_args_validates_schema_sources():
    with pytest.raises(SystemExit, match="Choose only one schema source"):
        _resolve_tables_from_args(_make_args(schema_from_db=True, schema_from_dynamodb=True))

    with pytest.raises(SystemExit, match="--schema-from-dynamodb requires --dynamodb-table"):
        _resolve_tables_from_args(_make_args(schema_from_dynamodb=True))

    with pytest.raises(SystemExit, match="--schema-from-db requires --db-url"):
        _resolve_tables_from_args(_make_args(schema_from_db=True))

    with pytest.raises(SystemExit, match="Provide either --ddl <schema.sql>"):
        _resolve_tables_from_args(_make_args())


def test_resolve_tables_from_args_for_database_source(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, object] = {}

    def fake_create_engine(url: str) -> str:
        captured["url"] = url
        return "engine"

    def fake_load_schema_from_db(engine: object, table_names: list[str] | None):
        captured["engine"] = engine
        captured["tables"] = table_names
        return ["users", "orders"]

    monkeypatch.setattr("ddl2data.cli.create_engine", fake_create_engine)
    monkeypatch.setattr("ddl2data.cli.load_schema_from_db", fake_load_schema_from_db)

    result = _resolve_tables_from_args(
        _make_args(
            schema_from_db=True,
            db_url="sqlite:///tmp.db",
            tables="users, orders",
        )
    )

    assert result == ["users", "orders"]
    assert captured == {"url": "sqlite:///tmp.db", "engine": "engine", "tables": ["users", "orders"]}


def test_load_config_covers_supported_and_invalid_inputs(tmp_path: Path):
    yaml_cfg = tmp_path / "ddl2data.yaml"
    yaml_cfg.write_text("rows: 8\nout: json\n", encoding="utf-8")
    assert load_config(None) == {}
    assert load_config(str(yaml_cfg)) == {"rows": 8, "out": "json"}

    missing = tmp_path / "missing.toml"
    with pytest.raises(SystemExit, match="Config file not found"):
        load_config(str(missing))

    unsupported = tmp_path / "ddl2data.txt"
    unsupported.write_text("rows=1", encoding="utf-8")
    with pytest.raises(SystemExit, match="Unsupported config extension"):
        load_config(str(unsupported))

    invalid_root = tmp_path / "ddl2data.json"
    invalid_root.write_text(json.dumps([1, 2, 3]), encoding="utf-8")
    with pytest.raises(SystemExit, match="Config file root must be an object/map"):
        load_config(str(invalid_root))


def test_write_json_and_csv_persist_outputs(tmp_path: Path):
    data = {
        "users": [{"id": 1, "name": "alice"}],
        "empty": [],
    }

    json_path = tmp_path / "data.json"
    payload = write_json(data, str(json_path))
    assert json.loads(payload)["users"][0]["name"] == "alice"
    assert json.loads(json_path.read_text(encoding="utf-8"))["users"][0]["id"] == 1

    csv_paths = write_csv(data, str(tmp_path / "csv"))
    assert {Path(path).name for path in csv_paths} == {"users.csv", "empty.csv"}
    assert (tmp_path / "csv" / "empty.csv").read_text(encoding="utf-8") == ""
    assert "id,name" in (tmp_path / "csv" / "users.csv").read_text(encoding="utf-8")


def test_dynamodb_writer_handles_multiple_tables_and_binary_values(tmp_path: Path):
    tables = [
        TableMeta(
            name="users",
            columns=[
                ColumnMeta("pk", "string", nullable=False, primary_key=True, extra={"dynamodb_attr_type": "S"}),
                ColumnMeta("blob", "bytes", extra={"dynamodb_attr_type": "B"}),
                ColumnMeta("active", "boolean", extra={"dynamodb_attr_type": "BOOL"}),
            ],
        ),
        TableMeta(
            name="events",
            columns=[ColumnMeta("pk", "string", nullable=False, primary_key=True, extra={"dynamodb_attr_type": "S"})],
        ),
    ]

    assert _attribute_value(None, None) == {"NULL": True}
    assert _attribute_value(None, 1.5) == {"N": "1.5"}
    assert _attribute_value(None, b"\x01") == {"B": "AQ=="}
    with pytest.raises(ValueError, match="Unsupported DynamoDB attribute type 'M'"):
        _attribute_value(ColumnMeta("meta", "map", extra={"dynamodb_attr_type": "M"}), "x")

    with pytest.raises(SystemExit, match="requires --output-path directory"):
        write_dynamodb_json(tables, {"users": [{"pk": "user#1"}], "events": [{"pk": "event#1"}]})

    single_path = tmp_path / "users.ddb.json"
    payload = write_dynamodb_json(
        [tables[0]],
        {"users": [{"pk": "user#1", "blob": b"\x01", "active": True}]},
        str(single_path),
    )
    assert isinstance(payload, str)
    assert json.loads(single_path.read_text(encoding="utf-8"))["Item"]["blob"] == {"B": "AQ=="}

    written = write_dynamodb_json(
        tables,
        {"users": [{"pk": "user#1"}], "events": [{"pk": "event#1"}]},
        str(tmp_path / "ddb"),
    )
    assert {Path(path).name for path in written} == {"users.ddb.json", "events.ddb.json"}


def test_check_validation_handles_compound_predicates_and_sample_limits():
    table = TableMeta(
        name="users",
        columns=[
            ColumnMeta("age", "int"),
            ColumnMeta("status", "string"),
            ColumnMeta("code", "string"),
            ColumnMeta("score", "int"),
        ],
        check_constraints=[
            CheckConstraintMeta("age >= 18 AND age <= 30"),
            CheckConstraintMeta("status IN ('A', 'B')"),
            CheckConstraintMeta("REGEXP_LIKE(code, '^[A-Z]{2}$')"),
            CheckConstraintMeta("score BETWEEN 10 AND 20"),
            CheckConstraintMeta("score <> 15"),
        ],
    )
    data = {
        "users": [
            {"age": 31, "status": "C", "code": "kr", "score": 25},
            {"age": 17, "status": "A", "code": "US", "score": 15},
        ]
    }

    result = validate_check_constraints([table], data, sample_limit=3)

    assert result["check_violations"] == 6
    assert len(result["check_details"]) == 3
    assert result["check_details"][0]["type"] == "check"


def test_check_helpers_cover_or_predicates_and_column_value_extraction():
    columns = {
        "age": ColumnMeta("age", "int"),
        "code": ColumnMeta("code", "string"),
    }
    check = CheckConstraintMeta("age = 10 OR REGEXP_LIKE(code, '^[A-Z]{2}$')")

    assert _evaluate_check({"age": 9, "code": "KR"}, columns, check) is True
    assert _evaluate_check({"age": 9, "code": "k"}, columns, check) is False
    assert _check_column_values({"age": 9, "code": "k"}, check) == {"age": 9, "code": "k"}
