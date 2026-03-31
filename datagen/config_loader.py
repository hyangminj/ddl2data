from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _load_toml(path: Path) -> dict[str, Any]:
    try:
        import tomllib  # py3.11+

        return tomllib.loads(path.read_text(encoding="utf-8"))
    except ModuleNotFoundError:
        try:
            import tomli
        except Exception as e:  # pragma: no cover
            raise SystemExit("TOML config requires tomli on Python 3.10. Install with: pip install tomli") from e
        return tomli.loads(path.read_text(encoding="utf-8"))


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        import yaml  # type: ignore
    except Exception as e:  # pragma: no cover
        raise SystemExit("YAML config requires PyYAML. Install with: pip install pyyaml") from e

    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SystemExit("Config file root must be an object/map")
    return data


def load_config(path_str: str | None) -> dict[str, Any]:
    if not path_str:
        return {}

    path = Path(path_str)
    if not path.exists():
        raise SystemExit(f"Config file not found: {path}")

    ext = path.suffix.lower()
    if ext in {".json"}:
        data = json.loads(path.read_text(encoding="utf-8"))
    elif ext in {".toml"}:
        data = _load_toml(path)
    elif ext in {".yaml", ".yml"}:
        data = _load_yaml(path)
    else:
        raise SystemExit("Unsupported config extension. Use .json/.toml/.yaml/.yml")

    if not isinstance(data, dict):
        raise SystemExit("Config file root must be an object/map")
    return data
