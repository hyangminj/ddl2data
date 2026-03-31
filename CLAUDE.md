# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Setup

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .                    # core install
pip install -e ".[polars]"          # include polars engine support
```

Entry points: `datagen` CLI or `python -m datagen.cli`.

## Running Tests

```bash
pytest                              # all tests
pytest tests/test_parser_graph.py   # single file
pytest -k "test_fk"                 # by name pattern
```

## Architecture

The pipeline has four distinct layers:

**1. Parse** (`datagen/parser/`)
- `ddl.py` — parses SQL DDL via `sqlglot` into `TableMeta` dataclasses; includes fallback regex extraction for `UNIQUE`/`CHECK` clauses that sqlglot variants may miss
- `graph.py` — topological sort of tables by FK dependencies (determines generation order)
- `introspect.py` — SQLAlchemy inspector path for `--schema-from-db`

**2. Data model** (`datagen/config.py`)
- Core dataclasses: `TableMeta`, `ColumnMeta`, `ForeignKey`, `UniqueConstraintMeta`, `CheckConstraintMeta`, `DistSpec`
- These flow unchanged through parse → generate → validate → write

**3. Generate** (`datagen/generator/`)
- `base.py` — row-wise Python generator (`_generate_table_rows_python`); handles FK resolution, distribution overrides, edge-case injection (every 20th row), unique deduplication, and CHECK enforcement via regex heuristics
- `base.py` — `_generate_table_rows_polars`: vectorized path for simple tables; auto-falls back to Python for any table with FK/CHECK/unique constraints
- `dist.py` — distribution sampling (normal, poisson, weighted, exponential, pareto, zipf, peak)

**4. Write** (`datagen/writer/`)
- `postgres.py` — SQL rendering for all SQL dialects (postgres/mysql/sqlite/bigquery); BigQuery supports `INSERT ALL` mode and backtick-quoted table paths
- `json_writer.py`, `csv_writer.py`, `parquet_writer.py` — respective format writers; `parquet` requires polars

**Orchestration** (`datagen/cli.py`)
- `build_parser()` → `_merge_config()` (CLI args + config file) → parse → `generate_all()` → `validate_generated_data()` → write
- `config_loader.py` handles JSON/TOML/YAML config files; CLI args always win over config

## Key Behaviors

- **FK ordering**: parent tables are always generated before child tables via topological sort
- **Distribution priority**: `table.column` spec beats bare `column` spec
- **Edge cases**: 1-in-20 rows gets special edge values (special chars, zero, near-max-length strings)
- **Polars fallback**: `--engine polars` is silently downgraded to Python for constraint-heavy tables — no error, no warning
- **CHECK enforcement** is heuristic-only: regex patterns handle `BETWEEN`, `IN`, `~`/`REGEXP_LIKE`, and binary numeric comparisons; complex nested expressions are not enforced
- **Unique collision handling**: up to 10 retry attempts per unique column before accepting a duplicate

## Adding a New Output Format

1. Add a writer module in `datagen/writer/`
2. Add the format name to `--out` choices in `cli.py:build_parser()`
3. Handle the new format in `cli.py:main()` dispatch block
