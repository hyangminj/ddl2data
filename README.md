# datagen

`datagen` is a CLI tool that generates realistic synthetic test data from SQL DDL.

It parses table definitions and foreign-key relationships, generates rows with sensible defaults + statistical distributions, and can either:

- print/save output (`postgres` SQL, `json`, `csv`), or
- insert directly into a database via SQLAlchemy.

---

## What it does (MVP)

- Parse DDL with `sqlglot` (table/column/basic constraints/FK)
- Build FK dependency graph and generate parent tables first
- Generate fake values by column type (Faker + type mapping)
- Support distribution overrides:
  - `normal`
  - `poisson`
  - `weighted`
- Add occasional edge cases:
  - nullable columns can become `NULL`
  - special-character strings
  - near max-length strings
- Output formats:
  - PostgreSQL `INSERT` SQL
  - JSON
  - CSV (per-table files)
- Optional direct insert: `--db-url`

---

## Install

```bash
cd datagen
python -m venv .venv
. .venv/bin/activate
pip install -e .
```

After install, both are available:

- `python -m datagen.cli ...`
- `datagen ...`

---

## Quick start

### 1) Generate PostgreSQL INSERT SQL to stdout

```bash
datagen --ddl schema.sql --rows 100 --out postgres
```

### 2) Save JSON output

```bash
datagen --ddl schema.sql --rows 100 --out json --output-path data.json
```

### 3) Save CSV output (one file per table)

```bash
datagen --ddl schema.sql --rows 100 --out csv --output-path ./csv_out
```

### 4) Insert directly into PostgreSQL

```bash
datagen \
  --ddl schema.sql \
  --rows 1000 \
  --db-url postgresql+psycopg://user:pass@localhost:5432/mydb
```

---

## Distribution overrides (`--dist`)

Syntax:

```text
--dist <column_or_table.column>:<kind>,k=v,k=v
```

Examples:

```bash
# global column rule
--dist age:normal,mean=35,std=7

# table-qualified (higher priority than global)
--dist users.age:normal,mean=30,std=6

# poisson counts
--dist daily_orders:poisson,lambda=3

# weighted categorical values
--dist tier:weighted,A=60%,B=30%,C=10%
```

Priority when both exist:

1. `table.column`
2. `column`

---

## Reproducible generation

Use `--seed`:

```bash
datagen --ddl schema.sql --rows 100 --seed 42 --out json
```

This seeds Python random + Faker for stable reruns.

---

## CLI reference

```bash
datagen --ddl schema.sql --rows 100 [--out postgres|json|csv] [--output-path PATH]
        [--db-url URL]
        [--dist ...] [--dist ...]
        [--seed INT]
```

- `--ddl` (required): input DDL file
- `--rows` (required): rows generated **per table**
- `--out`: output format (default: `postgres`)
- `--output-path`: output file path (`json`/`postgres`) or directory (`csv`)
- `--db-url`: direct database insert target
- `--dist`: distribution override(s)
- `--seed`: deterministic run seed

---

## Example DDL

```sql
CREATE TABLE users (
  id INT PRIMARY KEY,
  age INT,
  tier VARCHAR(10)
);

CREATE TABLE orders (
  id INT PRIMARY KEY,
  user_id INT,
  amount NUMERIC,
  FOREIGN KEY (user_id) REFERENCES users(id)
);
```

With this schema, `users` rows are generated first, then `orders.user_id` references generated `users.id` values.

---

## Tests

```bash
pytest -q
```

Current coverage includes parser/graph and generator/writer smoke tests.

---

## Current MVP limitations

- `--rows` is per table (not per-table custom counts yet)
- Distribution set is currently limited to `normal`, `poisson`, `weighted`
- Direct DB insert path is validated for PostgreSQL-focused workflow
- DDL support is strong for common patterns, but not every advanced SQL dialect feature

---

## Roadmap (proposed)

- More distributions: zipf / exponential / pareto / peak-hour
- Better constraint handling (composite unique, richer checks)
- Multi-dialect writers (MySQL/SQLite/BigQuery)
- Config file support (YAML/TOML)
- Stronger validation/reporting of generated datasets
- Performance engine option (`--engine python|polars`) for vectorized generation and faster file output on large datasets
