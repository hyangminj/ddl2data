# Datagen Roadmap Implementation Tasks

This document contains self-contained implementation tasks for the `datagen` project. Each task includes the exact files to modify, what to change, and expected behavior. Task 4 (parquet compression) is already complete.

---

## Project Overview

`datagen` is a CLI tool that generates synthetic test data from SQL DDL. Key modules:
- `datagen/generator/base.py` — row generation with CHECK/FK/unique enforcement
- `datagen/writer/postgres.py` — SQL rendering for postgres/mysql/sqlite/bigquery dialects
- `datagen/validation.py` — post-generation validation (FK, non-null, unique)
- `datagen/cli.py` — CLI argument parsing and orchestration
- `datagen/config.py` — dataclasses: `TableMeta`, `ColumnMeta`, `ForeignKey`, `CheckConstraintMeta`, `DistSpec`

Python 3.10+, dependencies: sqlglot, Faker, SQLAlchemy, PyYAML. Optional: polars.

---

## Task 1: Expand CHECK parser for nested AND/OR

**File:** `datagen/generator/base.py`

### Current behavior
`_enforce_simple_check()` (line ~159) handles single-clause CHECKs only: `BETWEEN`, `IN(...)`, regex, binary comparison. Compound expressions like `CHECK(age >= 18 AND age <= 65)` are NOT handled — the function tries to match the entire string as one pattern and fails silently.

### Required changes

1. Add a helper function `_split_compound_check(expr: str) -> tuple[str, list[str]]`:
   - Input: a CHECK expression string
   - Strip outer parentheses if the entire expression is wrapped: `(age >= 18 AND age <= 65)` → `age >= 18 AND age <= 65`
   - Detect top-level `AND` or `OR` keywords (case-insensitive), respecting parenthesis depth
   - **Critical edge case**: `BETWEEN X AND Y` contains AND but is NOT a compound expression. Before splitting on AND, use regex to temporarily replace `BETWEEN <number> AND <number>` with a placeholder (e.g., `BETWEEN <number> __AND_PLACEHOLDER__ <number>`), split on remaining ` AND `, then restore placeholders.
   - Return `("and", [sub1, sub2, ...])` or `("or", [sub1, sub2, ...])` or `("single", [expr])`

2. Modify `_enforce_simple_check()`:
   - At the very beginning (before any pattern matching), call `_split_compound_check(expr)`
   - If result is `"and"`: iterate sub-expressions, create a temporary `CheckConstraintMeta` for each, recursively call `_enforce_simple_check(row, col_by_name, sub_check)`
   - If result is `"or"`: pick one sub-expression randomly, create temp `CheckConstraintMeta`, recursively call
   - If result is `"single"`: proceed with existing logic (no change)

### Test cases that must pass
```python
# AND compound
ddl = "CREATE TABLE t (id INT PRIMARY KEY, age INT, CHECK(age >= 18 AND age <= 65));"
# All generated rows should have 18 <= age <= 65

# OR compound
ddl = "CREATE TABLE t (id INT PRIMARY KEY, status VARCHAR(1), CHECK(status IN ('A','B') OR status IN ('C','D')));"
# All generated rows should have status in {A, B, C, D}

# BETWEEN with AND (should NOT split)
ddl = "CREATE TABLE t (id INT PRIMARY KEY, price NUMERIC, CHECK(price BETWEEN 10 AND 50));"
# Should still work as before: 10 <= price <= 50
```

---

## Task 2: Add strict CHECK enforcement with violation counters

**Files:** `datagen/validation.py`, `datagen/cli.py`

### Required changes

#### `datagen/validation.py`
Add function `validate_check_constraints(tables, data, sample_limit=20) -> dict`:
- For each table with `check_constraints`, evaluate each row against each CHECK expression
- Reuse the same pattern matching logic from `_enforce_simple_check` in `generator/base.py`, but in **read-only evaluation mode** — return True/False instead of mutating the row
- Create a helper `_evaluate_check(row, col_by_name, check) -> bool`:
  - `BETWEEN a AND b`: check if value is in range
  - `IN (...)`: check if value is in the list
  - `col >= N` / `col > N` / etc.: check the comparison
  - Compound AND/OR: evaluate recursively (reuse splitting from Task 1)
  - Return True if check passes, False otherwise. If pattern not recognized, return True (optimistic).
- Output format:
```python
{
    "check_violations": <int>,
    "check_details": [
        {"table": "t", "row_index": 0, "check": "age >= 18", "column_values": {"age": 15}},
        ...
    ]  # up to sample_limit
}
```

#### `datagen/cli.py`
- In `build_parser()`: add `--strict-checks` argument (action="store_true", default=None)
- In `_merge_config()` defaults dict: add `"strict_checks": False`
- In `_merge_config()` config key list: add `"strict_checks"`
- In `main()`: after existing `validate_generated_data()` call, if `args.strict_checks`:
  ```python
  check_validation = validate_check_constraints(tables, data)
  validation["counts"]["check_violations"] = check_validation["check_violations"]
  validation["sample_issues"].extend(check_validation["check_details"])
  if check_validation["check_violations"] > 0:
      validation["pass"] = False
      validation["counts"]["total_failures"] += check_validation["check_violations"]
  ```

---

## Task 3: BigQuery typed literals

**File:** `datagen/writer/postgres.py`

### Current behavior
`_sql_literal()` renders all string values as `'value'` regardless of dialect. For BigQuery, dates and timestamps should use typed literal syntax like `DATE '2024-01-15'` and `TIMESTAMP '2024-01-15T10:30:00'`.

### Required changes

1. Add `import re` at the top of the file (after `from __future__ import annotations`)

2. Add new function after `_sql_literal`:
```python
def _bq_sql_literal(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v)
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return f"DATE '{s}'"
    if re.match(r"^\d{4}-\d{2}-\d{2}T", s):
        return f"TIMESTAMP '{s}'"
    s = s.replace("\\", "\\\\").replace("'", "''").replace("\n", "\\n")
    return f"'{s}'"
```

3. In `_render_insert_sql_python` (two places where `_sql_literal` is called, lines ~40 and ~47):
   - Change `_sql_literal(row.get(c))` to `(_bq_sql_literal if dialect == "bigquery" else _sql_literal)(row.get(c))`
   - Or define `literal_fn = _bq_sql_literal if dialect == "bigquery" else _sql_literal` at the top of the function and use `literal_fn(...)` in both places

4. Same change in `_render_insert_sql_polars` (two places, lines ~71 and ~78)

### Expected output example
```sql
-- Before (current)
INSERT INTO `users` (`id`, `created_at`) VALUES (1, '2024-01-15');
INSERT INTO `users` (`id`, `created_at`) VALUES (2, '2024-01-15T10:30:00');

-- After
INSERT INTO `users` (`id`, `created_at`) VALUES (1, DATE '2024-01-15');
INSERT INTO `users` (`id`, `created_at`) VALUES (2, TIMESTAMP '2024-01-15T10:30:00');
```

---

## Task 4: Parquet compression CLI ✅ DONE

Already implemented. `--parquet-compression` flag added with snappy/zstd/lz4/gzip/none options.

---

## Task 5: Polars FK-only generation without full Python fallback

**File:** `datagen/generator/base.py`

### Current behavior
`_generate_table_rows_polars()` (line ~299) falls back to Python if the table has ANY of: `foreign_keys`, `check_constraints`, or `unique/primary_key` columns. This means FK-only tables (like a simple join table) never use the polars path.

### Required changes

In `_generate_table_rows_polars`, replace the fallback condition (line ~309):

**Before:**
```python
has_row_level_constraints = bool(table.foreign_keys or table.check_constraints or any(c.unique or c.primary_key for c in table.columns))
if has_row_level_constraints:
    return _generate_table_rows_python(table, rows, dist_overrides, generated)
```

**After:**
```python
has_strict_constraints = bool(table.check_constraints or any(c.unique or c.primary_key for c in table.columns))
if has_strict_constraints:
    return _generate_table_rows_python(table, rows, dist_overrides, generated)
```

Then, in the column generation loop that follows, handle FK columns:

```python
data_cols: dict[str, list[Any]] = {}
for col in table.columns:
    # Handle FK columns by sampling from parent table
    fk = next((x for x in table.foreign_keys if x.column == col.name), None)
    if fk and generated.get(fk.ref_table):
        parent_values = [r.get(fk.ref_column) for r in generated[fk.ref_table]]
        if parent_values:
            data_cols[col.name] = random.choices(parent_values, k=rows)
            continue

    dist = dist_overrides.get(f"{table.name}.{col.name}") or dist_overrides.get(col.name)
    if dist is not None:
        data_cols[col.name] = [sample_with_dist(dist) for _ in range(rows)]
        continue

    if col.nullable:
        vals = [(_default_value(col) if random.random() >= 0.05 else None) for _ in range(rows)]
    else:
        vals = [_default_value(col) for _ in range(rows)]
    data_cols[col.name] = vals
```

### Key behavior
- Tables with ONLY foreign keys (no check, no unique/pk) → use polars path with FK sampling
- Tables with check_constraints OR unique/pk → still fallback to Python
- Tables with no constraints at all → use polars path (no change from before)

---

## Task 6: End-to-end test fixtures

**File:** `tests/test_e2e_mixed.py` (NEW FILE)

Create comprehensive integration tests:

```python
import json
from pathlib import Path

from datagen.generator.base import generate_all
from datagen.parser.ddl import parse_ddl_text
from datagen.parser.graph import generation_order
from datagen.validation import validate_generated_data, validate_check_constraints
from datagen.report import build_report
from datagen.writer.json_writer import write_json
from datagen.writer.csv_writer import write_csv
from datagen.writer.postgres import render_insert_sql


MIXED_DDL = """
CREATE TABLE users (
  id INT PRIMARY KEY,
  email VARCHAR(100) UNIQUE NOT NULL,
  age INT,
  status VARCHAR(1),
  CONSTRAINT ck_age CHECK (age >= 18 AND age <= 120),
  CONSTRAINT ck_status CHECK (status IN ('A', 'B', 'C'))
);

CREATE TABLE orders (
  id INT PRIMARY KEY,
  user_id INT NOT NULL,
  amount NUMERIC,
  qty INT,
  CONSTRAINT ck_amount CHECK (amount BETWEEN 1 AND 10000),
  CONSTRAINT ck_qty CHECK (qty > 0),
  FOREIGN KEY (user_id) REFERENCES users(id)
);
"""


def test_e2e_mixed_constraints_with_per_table_rows():
    """Full pipeline: parse → generate with per-table rows → validate → report."""
    tables = parse_ddl_text(MIXED_DDL)
    order = generation_order(tables)
    data = generate_all(
        tables, order, rows=10, dist_overrides={},
        table_rows={"users": 20, "orders": 50},
    )

    assert len(data["users"]) == 20
    assert len(data["orders"]) == 50

    # CHECK constraints satisfied
    for r in data["users"]:
        if r["age"] is not None:
            assert 18 <= r["age"] <= 120
        assert r["status"] in {"A", "B", "C"}

    for r in data["orders"]:
        if r["amount"] is not None:
            assert 1 <= float(r["amount"]) <= 10000
        if r["qty"] is not None:
            assert float(r["qty"]) > 0

    # FK integrity
    user_ids = {r["id"] for r in data["users"]}
    assert all(r["user_id"] in user_ids for r in data["orders"])

    # Validation passes
    validation = validate_generated_data(tables, data)
    assert validation["counts"]["fk_violations"] == 0


def test_e2e_compound_and_or_checks():
    """AND/OR compound CHECK constraints generate valid data."""
    ddl = """
    CREATE TABLE items (
      id INT PRIMARY KEY,
      score INT,
      CONSTRAINT ck_score CHECK (score >= 0 AND score <= 100)
    );
    """
    tables = parse_ddl_text(ddl)
    order = generation_order(tables)
    data = generate_all(tables, order, rows=100, dist_overrides={})

    for r in data["items"]:
        if r["score"] is not None:
            assert 0 <= r["score"] <= 100


def test_e2e_bigquery_typed_literals():
    """BigQuery output uses DATE/TIMESTAMP typed literals."""
    ddl = """
    CREATE TABLE events (
      id INT PRIMARY KEY,
      event_date DATE,
      created_at TIMESTAMP
    );
    """
    tables = parse_ddl_text(ddl)
    order = generation_order(tables)
    data = generate_all(tables, order, rows=5, dist_overrides={})

    sql = render_insert_sql("events", data["events"], dialect="bigquery")
    assert "DATE '" in sql
    assert "TIMESTAMP '" in sql


def test_e2e_parquet_compression(tmp_path: Path):
    """Parquet output with compression creates files."""
    from datagen.writer.parquet_writer import write_parquet

    data = {"users": [{"id": i, "name": f"user_{i}"} for i in range(10)]}

    for codec in ["snappy", "zstd", "gzip", "none"]:
        out_dir = tmp_path / codec
        files = write_parquet(data, str(out_dir), compression=codec)
        assert len(files) == 1
        assert Path(files[0]).exists()


def test_e2e_strict_check_violations():
    """Strict CHECK mode detects violations in pre-built bad data."""
    ddl = "CREATE TABLE t (id INT PRIMARY KEY, age INT, CONSTRAINT ck CHECK (age >= 18));"
    tables = parse_ddl_text(ddl)

    # Manually craft data with a violation
    bad_data = {"t": [{"id": 1, "age": 10}, {"id": 2, "age": 25}]}
    result = validate_check_constraints(tables, bad_data)
    assert result["check_violations"] >= 1


def test_e2e_polars_fk_only_table():
    """Polars engine handles FK-only tables without falling back."""
    ddl = """
    CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
    CREATE TABLE logs (
      ts TIMESTAMP,
      user_id INT,
      msg TEXT,
      FOREIGN KEY (user_id) REFERENCES users(id)
    );
    """
    tables = parse_ddl_text(ddl)
    order = generation_order(tables)
    data = generate_all(tables, order, rows=50, dist_overrides={}, engine="polars")

    assert len(data["logs"]) == 50
    user_ids = {r["id"] for r in data["users"]}
    assert all(r["user_id"] in user_ids for r in data["logs"])


def test_e2e_all_output_formats(tmp_path: Path):
    """Generate data and render in all SQL dialects + JSON + CSV."""
    ddl = "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20));"
    tables = parse_ddl_text(ddl)
    order = generation_order(tables)
    data = generate_all(tables, order, rows=5, dist_overrides={})

    for dialect in ["postgres", "mysql", "sqlite", "bigquery"]:
        sql = render_insert_sql("t", data["t"], dialect=dialect)
        assert "INSERT" in sql

    json_out = write_json(data)
    assert "t" in json.loads(json_out)

    csv_files = write_csv(data, str(tmp_path / "csv"))
    assert len(csv_files) == 1
```

---

## Task 7: Update README roadmap

**File:** `README.md`

### Move to ✅ Implemented section (add these bullets):
- Nested AND/OR compound CHECK constraint support
- `--strict-checks` mode with CHECK violation counters in report
- BigQuery typed literals (DATE, TIMESTAMP) in SQL output
- `--parquet-compression` option (snappy/zstd/lz4/gzip/none)
- Polars engine FK-only table optimization (reduced python fallback surface)

### Update ⏳ Remaining section to:
- Even richer CHECK parser coverage for function-heavy constraints (e.g., COALESCE, CASE WHEN)
- Dialect-specific SQL polishing for edge cases (escaping/typing nuances by engine)
- Optional per-table advanced generation profiles (beyond row count overrides)

### Update Hand-off TODO to:
1. Add support for function-based CHECK constraints (COALESCE, LENGTH, UPPER, etc.)
2. Add multi-column composite CHECK evaluation (e.g., `CHECK(start_date < end_date)`)
3. Add STRUCT/ARRAY column type support for BigQuery
4. Add changelog/release note for `v0.3.0` snapshot

---

## Execution Order

Tasks 1, 3, 5 are independent — can be done in parallel.
Task 2 depends on Task 1 (reuses compound splitting logic).
Task 6 depends on Tasks 1-5 being complete.
Task 7 is last.

```
[Task 1] ──┐
[Task 3] ──┼──→ [Task 2] ──→ [Task 6] ──→ [Task 7]
[Task 5] ──┘
```

## Verification

After all tasks, run:
```bash
pytest -v
datagen --ddl schema.sql --rows 50 --out json --seed 42
datagen --ddl schema.sql --rows 10 --out bigquery
datagen --ddl schema.sql --rows 100 --out parquet --parquet-compression zstd --output-path /tmp/pq_test
datagen --ddl schema.sql --rows 50 --strict-checks --report-path /tmp/report.json --out json
```
