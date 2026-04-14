import json
from pathlib import Path

import pytest

from ddl2data.cli import _merge_config, _parse_table_rows_map, build_parser, main
from ddl2data.config_loader import load_config
from ddl2data.report import build_report


def test_load_toml_config(tmp_path: Path):
    cfg = tmp_path / "ddl2data.toml"
    cfg.write_text(
        '\n'.join([
            'ddl = "schema.sql"',
            'rows = 100',
            'out = "postgres"',
            'seed = 42',
        ]),
        encoding="utf-8",
    )
    data = load_config(str(cfg))
    assert data["ddl"] == "schema.sql"
    assert data["rows"] == 100
    assert data["out"] == "postgres"


def test_build_report_shape():
    generated = {
        "users": [
            {"id": 1, "name": "A", "age": 20},
            {"id": 2, "name": "B", "age": None},
            {"id": 3, "name": "A", "age": 25},
        ]
    }
    rep = build_report(generated)
    assert rep["total_tables"] == 1
    assert rep["total_rows"] == 3
    assert rep["tables"]["users"]["rows"] == 3
    assert rep["tables"]["users"]["column_stats"]["age"]["null_count"] == 1

    top = rep["tables"]["users"]["column_stats"]["name"]["top_values"]
    assert top[0]["value"] == "A"
    assert top[0]["count"] == 2


def test_load_json_config(tmp_path: Path):
    cfg = tmp_path / "ddl2data.json"
    cfg.write_text(json.dumps({"rows": 10, "out": "json"}), encoding="utf-8")
    data = load_config(str(cfg))
    assert data["rows"] == 10
    assert data["out"] == "json"


def test_parse_table_rows_map():
    parsed = _parse_table_rows_map(["users=10,orders=20", "events=5"])
    assert parsed == {"users": 10, "orders": 20, "events": 5}


def test_parse_table_rows_map_rejects_empty_table_name():
    with pytest.raises(SystemExit, match="Table name cannot be empty"):
        _parse_table_rows_map(["=10"])


def test_merge_config_rejects_negative_rows():
    args = build_parser().parse_args(["--ddl", "schema.sql", "--rows", "-1", "--out", "json"])
    with pytest.raises(SystemExit, match="--rows must be >= 0"):
        _merge_config(args)


def test_main_rejects_invalid_dist_token(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        "sys.argv",
        ["ddl2data", "--ddl", "schema.sql", "--rows", "1", "--dist", "broken", "--out", "json"],
    )
    with pytest.raises(SystemExit, match="Invalid --dist token 'broken'"):
        main()
