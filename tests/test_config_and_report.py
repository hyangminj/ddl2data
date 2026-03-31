import json
from pathlib import Path

from datagen.config_loader import load_config
from datagen.report import build_report


def test_load_toml_config(tmp_path: Path):
    cfg = tmp_path / "datagen.toml"
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
    cfg = tmp_path / "datagen.json"
    cfg.write_text(json.dumps({"rows": 10, "out": "json"}), encoding="utf-8")
    data = load_config(str(cfg))
    assert data["rows"] == 10
    assert data["out"] == "json"
