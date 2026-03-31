# AGENTS.md
Guidance for coding agents working in this repository.

## Scope
- Applies to `/Users/hyangmin/github/datagen`.
- No prior repo-local `AGENTS.md` was present.
- No Cursor rules were found: no `.cursor/rules/`, no `.cursorrules`.
- No Copilot rules were found: no `.github/copilot-instructions.md`.
- If any of those files are added later, treat them as additional constraints.

## Project Summary
- `datagen` is a Python CLI that generates synthetic data from SQL DDL or live DB schemas.
- Entrypoints:
  - `datagen`
  - `python -m datagen.cli`
- Tests live in `tests/`.
- `pyproject.toml` sets `testpaths = ["tests"]`.

## Setup
Use a virtual environment. System Python may block installs.

```bash
python3 -m venv .venv
. .venv/bin/activate
.venv/bin/python -m pip install -e .
```

Optional Polars support:

```bash
.venv/bin/python -m pip install -e ".[polars]"
```

If pytest is missing:

```bash
.venv/bin/python -m pip install pytest
```

## Build / Lint / Test
There is no dedicated build step beyond package installation.

Build/package check:
```bash
.venv/bin/python -m pip install -e .
```

Run all tests:
```bash
.venv/bin/python -m pytest
```

Run one file:
```bash
.venv/bin/python -m pytest tests/test_parser_graph.py
```

Run one test:
```bash
.venv/bin/python -m pytest tests/test_roadmap_items.py::test_bigquery_writer_dataset_table_and_insert_all
```

Run by name pattern:
```bash
.venv/bin/python -m pytest -k "test_fk"
```

Manual CLI smoke checks:
```bash
.venv/bin/python -m datagen.cli --ddl schema.sql --rows 5 --out json --output-path /tmp/datagen.json
.venv/bin/python -m datagen.cli --ddl schema.sql --rows 5 --out postgres
```

Lint/type-check reality:
- No Ruff, Black, isort, Flake8, mypy, or similar config was found in repo root.
- Use language-server diagnostics as the primary static check.
- The repo is not fully type-clean today; there are pre-existing diagnostics, especially around loose typing and optional `polars` imports.

## Repo Map
- `datagen/cli.py` — argument parsing, config merge, orchestration, output dispatch.
- `datagen/config.py` — shared dataclasses (`TableMeta`, `ColumnMeta`, etc.).
- `datagen/config_loader.py` — JSON/TOML/YAML config loading.
- `datagen/parser/ddl.py` — SQL parsing via `sqlglot` plus regex fallback extraction.
- `datagen/parser/graph.py` — FK dependency ordering.
- `datagen/parser/introspect.py` — SQLAlchemy inspector path.
- `datagen/generator/base.py` — Python generator plus Polars fallback logic.
- `datagen/generator/dist.py` — distribution parsing/sampling.
- `datagen/writer/` — SQL, JSON, CSV, Parquet writers.
- `datagen/validation.py` — FK/non-null/unique validation.
- `datagen/report.py` — report generation.

## Architecture Rules
- Preserve the parse → generate → validate → write flow.
- Keep the dataclasses in `datagen/config.py` as the shared contract across layers.
- Parent tables must still generate before child tables.
- Keep table-qualified distribution overrides higher priority than bare-column overrides.
- Polars support is best-effort and must still fall back to Python for constraint-heavy tables.

## Style: General
- Follow existing Python style; do not introduce a new formatter convention.
- Use `from __future__ import annotations` in module files.
- Order imports as: stdlib, blank line, third-party, blank line, local imports.
- Use snake_case for functions, variables, and tests.
- Use PascalCase for dataclasses and types.
- Keep module constants uppercase.
- Prefer small helper functions over large mixed-responsibility blocks.

## Style: Types and Data
- Prefer modern built-in generics like `dict[str, int]` and `list[str]`.
- Prefer `str | None` over `Optional[str]`.
- Reuse existing dataclasses instead of inventing parallel metadata structures.
- Do not add `Any` casually when a narrower type is clear.
- Match the existing style of passing plain Python dict/list structures between layers.

## Style: Error Handling
- For CLI/config validation failures, raise `SystemExit` with a direct message.
- For optional dependency failures, raise a clear `RuntimeError` or `SystemExit` with install guidance.
- Preserve chained exceptions with `from e` when re-raising.
- Do not swallow errors unless the file already implements an intentional best-effort fallback path.
- Keep user-facing error messages explicit about the flag, file, or dependency that failed.

## Style: Dependencies and Imports
- Prefer existing dependencies already declared in `pyproject.toml`:
  - `sqlglot`
  - `Faker`
  - `SQLAlchemy`
  - `PyYAML`
  - optional `polars`
- Avoid new dependencies unless clearly justified.
- Keep optional imports inside the branch that needs them and fail with a clear message if unavailable.

## Style: CLI Behavior
- New CLI flags belong in `build_parser()`.
- Preserve the current config merge rule: CLI args override config file values.
- Preserve stdout/file behavior:
  - print output when no path is provided
  - write to a file or directory when requested
- Keep help text and errors concise and user-facing.

## Tests
- Tests are plain pytest functions, not test classes.
- Test names describe behavior in full snake_case.
- Prefer focused behavior/regression tests over vague broad assertions.
- Match existing test patterns:
  - inline DDL fixtures in the test body
  - direct assertions on returned dict/list values
  - exact message checks for error cases when relevant
- Put regression tests near the most relevant existing test file.

## Change Strategy
- Make surgical changes.
- Do not refactor unrelated code while fixing a bug.
- Preserve architecture unless the task explicitly asks for broader change.
- If changing generator behavior, verify both values and constraints.
- If changing CLI/config behavior, test both direct CLI input and config-driven behavior.
- If changing Polars behavior, preserve Python fallback semantics.

## Verification Before Finishing
- Logic change in parser/generator/writer: run the most relevant single test or test file.
- Broader behavior change: run `.venv/bin/python -m pytest`.
- CLI change: run at least one real CLI command manually.
- If Polars behavior is involved, test with and without the optional dependency when practical.

## Source of Truth
- Trust `README.md`, `pyproject.toml`, and the existing tests.
- When docs and tests disagree, inspect the implementation and update carefully rather than guessing.
