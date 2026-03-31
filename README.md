# datagen

`datagen` is a CLI tool that generates realistic synthetic test data from SQL DDL **or from a live DB schema**.

It parses table definitions and foreign-key relationships, generates rows with sensible defaults + statistical distributions, and can either:

- print/save output (`postgres`/`mysql`/`sqlite`/`bigquery` SQL, `json`, `csv`, `parquet`), or
- insert directly into a database via SQLAlchemy.

---

## What it does

- Parse DDL with `sqlglot` (table/column/basic constraints/FK)
- Capture unique/check constraint metadata (including composite unique where available)
- Or introspect schema directly from DB via SQLAlchemy inspector (`--schema-from-db`)
- Build FK dependency graph and generate parent tables first
- Generate fake values by column type (Faker + type mapping)
- Support distribution overrides:
  - `normal`
  - `poisson`
  - `weighted`
  - `exponential`
  - `pareto`
  - `zipf`
  - `peak` (time-of-day concentration)
- Add occasional edge cases:
  - nullable columns can become `NULL`
  - special-character strings
  - near max-length strings
- Output formats:
  - PostgreSQL `INSERT` SQL
  - MySQL `INSERT` SQL
  - SQLite `INSERT` SQL
  - JSON
  - CSV (per-table files)
  - Parquet (per-table files)
- Optional direct insert: `--insert --db-url ...`
- Config-file driven runs (`--config`) with JSON/TOML/YAML
- Optional generation report output (`--report-path`)

---

## Install

```bash
cd datagen
python3 -m venv .venv
. .venv/bin/activate
pip install -e .
```

Optional Polars support for `--out parquet` or `--engine polars`:

```bash
pip install -e ".[polars]"
```

After install, both are available:

- `python -m datagen.cli ...`
- `datagen ...`

---

## Quick start

### 1) Generate SQL from DDL (postgres/mysql/sqlite)

```bash
datagen --ddl schema.sql --rows 100 --out postgres
datagen --ddl schema.sql --rows 100 --out mysql
datagen --ddl schema.sql --rows 100 --out sqlite
datagen --ddl schema.sql --rows 100 --out bigquery
```

### 2) Generate JSON from DDL

```bash
datagen --ddl schema.sql --rows 100 --out json --output-path data.json
```

### 3) Generate CSV from DDL (one file per table)

```bash
datagen --ddl schema.sql --rows 100 --out csv --output-path ./csv_out
```

### 3-1) Generate Parquet from DDL (one file per table)

```bash
datagen --ddl schema.sql --rows 100 --out parquet --parquet-compression zstd --output-path ./parquet_out
```

### 4) Read schema directly from DB and print SQL

```bash
datagen --schema-from-db --db-url postgresql+psycopg://user:pass@localhost:5432/mydb --rows 100 --out postgres
```

### 5) Read schema from DB (selected tables only)

```bash
datagen \
  --schema-from-db \
  --db-url postgresql+psycopg://user:pass@localhost:5432/mydb \
  --tables users,orders,events \
  --rows 100 \
  --out json
```

### 6) Insert generated rows directly into DB

```bash
datagen \
  --ddl schema.sql \
  --rows 1000 \
  --insert \
  --db-url postgresql+psycopg://user:pass@localhost:5432/mydb
```

### 7) Run from config file (JSON/TOML/YAML)

```bash
datagen --config datagen.toml
```

Example `datagen.toml`:

```toml
ddl = "schema.sql"
rows = 500
out = "postgres"
seed = 42
table_rows = { users = 100, orders = 500, events = 2000 }
dist = [
  "users.age:normal,mean=33,std=7",
  "orders.amount:pareto,alpha=1.7,xm=1",
]
```

### 8) Write generation report (with validation summary)

```bash
datagen --ddl schema.sql --rows 500 --strict-checks --report-path report.json --out json --output-path data.json
```

Report JSON now includes validation counts and sample issues for:
- FK integrity violations
- non-null violations
- unique collisions
- supported CHECK violations when `--strict-checks` is enabled

### 9) Use Polars generation+output engine (optional)

```bash
datagen --ddl schema.sql --rows 100000 --out csv --engine polars --output-path ./csv_out
```

`--engine polars` now applies to generation + render/write stages where practical.
Fallback behavior is explicit:
- tables with CHECK constraints fall back to the Python row-wise generator
- tables with unique or primary-key constraints fall back to the Python row-wise generator
- FK-only tables can stay on the Polars generation path

If `polars` is not installed and `--engine polars` is selected in writers, a clear dependency error is raised.
`--out parquet` also requires the optional `polars` dependency regardless of `--engine`.

### 10) Per-table row counts (with global fallback)

```bash
datagen --ddl schema.sql --rows 100 --table-rows users=20,orders=500 --table-rows events=2000 --out json
```

- `--rows` remains the global default.
- `--table-rows table=count` overrides per table.
- Supports config files via `table_rows` map.

### 11) BigQuery dataset table + INSERT ALL mode

```bash
datagen --ddl schema.sql --rows 100 --out bigquery --table-rows users=10 --output-path out.sql
datagen --ddl schema.sql --rows 100 --out bigquery --bq-insert-all --output-path out_insert_all.sql
```

BigQuery writer supports table paths like `dataset.table` (quoted as `` `dataset.table` ``) and optional `INSERT ALL` rendering (`--bq-insert-all`).

### 12) Richer CHECK-constraint aware generation (heuristics)

Common CHECK forms now influence generated values:
- comparison: `age >= 18`, `qty > 0`, `score <= 100`, `x != 0`
- range: `price BETWEEN 10 AND 20`
- enum list: `status IN ('A','B','C')`
- regex-like: `code ~ '^[A-Z]{2}$'`, `REGEXP_LIKE(code, '^[0-9]+$')` (practical subset)

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

# heavy-tail behaviors
--dist amount:pareto,alpha=1.5,xm=1
--dist category_rank:zipf,skew=1.8,n=200

# peak-hour timestamps
--dist created_at:peak,hours=9-11,18-20
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
datagen [--ddl schema.sql | --schema-from-db --db-url URL [--tables t1,t2]]
        [--config config.toml]
        [--rows 100]
        [--table-rows users=20,orders=500] [--table-rows events=2000]
        [--out postgres|mysql|sqlite|bigquery|json|csv|parquet]
        [--engine python|polars]
        [--bq-insert-all]
        [--output-path PATH]
        [--insert --db-url URL]
        [--dist ...] [--dist ...]
        [--seed INT]
        [--report-path report.json]
        [--strict-checks]
        [--parquet-compression snappy|zstd|lz4|gzip|none]
```

- `--config`: load defaults from JSON/TOML/YAML config file
- `--ddl`: input DDL file
- `--schema-from-db`: introspect table schema from DB
- `--tables`: optional comma-separated table filter for DB introspection mode
- `--rows`: global default rows per table (required unless `--table-rows`/config overrides all needed tables)
- `--table-rows`: per-table row overrides (`table=count`), repeatable and comma-friendly
- `--out`: output format (default: `postgres`), includes `bigquery` and `parquet` (`parquet` requires `polars`)
- `--engine`: `python` (default) or `polars` for generation + render/write where practical (auto-fallback to python for constraint-heavy tables)
- `--bq-insert-all`: for `--out bigquery`, render `INSERT ALL ... SELECT 1;` blocks
- `--output-path`: output file path (`json`/`sql`) or directory (`csv`/`parquet`)
- `--db-url`: DB connection URL
- `--insert`: insert generated rows into `--db-url`
- `--dist`: distribution override(s)
- `--seed`: deterministic run seed
- `--report-path`: write a JSON profile report (rows/null-ratio/top-values) plus validation summary
- `--strict-checks`: validate generated rows against supported CHECK constraints and add violation counts to the report
- `--parquet-compression`: parquet codec for `--out parquet` (`snappy`, `zstd`, `lz4`, `gzip`, `none`)

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

## Current limitations

- DDL parsing and DB introspection cover common schemas well, but advanced dialect-specific features are still partial.
- CHECK-aware generation and `--strict-checks` cover a practical subset: numeric comparisons, `BETWEEN`, `IN (...)`, regex-like forms, and nested `AND`/`OR`.
- Function-heavy or cross-column CHECK expressions are handled best-effort rather than fully modeled.
- `--engine polars` still falls back to Python for tables with CHECK, unique, or primary-key constraints.
