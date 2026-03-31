# loadforge

Synthetic relational data generation for pipeline, staging, and service testing.

- PyPI package: `loadforge`
- CLI command: `datagen`
- Python module: `datagen`

`loadforge` turns SQL DDL or a live database schema into fake but structured data that is useful for load tests, end-to-end pipeline checks, local development, and staging environment seeding.

It parses tables and foreign keys, generates rows in parent-before-child order, applies type-aware defaults plus optional distributions, validates the generated data, and writes it out as SQL, JSON, CSV, or Parquet. It can also insert directly into a database through SQLAlchemy.

---

## Why use it

Use `loadforge` when you need to:

- stress a pipeline or service with large synthetic datasets
- generate relational fixtures from existing DDL
- seed staging or local environments while preserving FK order
- inspect a live schema and generate data without hand-writing metadata
- export the same dataset shape in SQL, JSON, CSV, or Parquet
- produce validation reports before loading data elsewhere

---

## Core capabilities

- Input sources:
  - SQL DDL via `--ddl`
  - live schema introspection via `--schema-from-db --db-url ...`
- Relationship-aware generation:
  - foreign-key dependency graph
  - parent tables generated before child tables
  - FK-only tables can use the Polars generation path
- Output formats:
  - PostgreSQL `INSERT`
  - MySQL `INSERT`
  - SQLite `INSERT`
  - BigQuery `INSERT` and `INSERT ALL`
  - JSON
  - CSV
  - Parquet
- Validation and reporting:
  - FK integrity checks
  - non-null checks
  - unique collision checks
  - optional CHECK validation with `--strict-checks`
  - JSON report output via `--report-path`
- Generation controls:
  - per-table row overrides with `--table-rows`
  - reproducible runs with `--seed`
  - config-file driven runs with JSON, TOML, or YAML
  - per-column and per-table distribution overrides with `--dist`

---

## Install

Recommended for CLI use:

```bash
pipx install loadforge
```

Or install with `pip`:

```bash
pip install loadforge
```

Optional Polars support:

```bash
pip install "loadforge[polars]"
```

From source:

```bash
git clone https://github.com/hyangminj/datagen.git
cd datagen
python3 -m venv .venv
. .venv/bin/activate
pip install -e .
```

Optional Polars support from source:

```bash
pip install -e ".[polars]"
```

Sanity check:

```bash
datagen --help
```

GitHub Releases also include wheel (`.whl`) and source (`.tar.gz`) artifacts.

---

## Quick start

Minimal schema:

```sql
CREATE TABLE users (
  id INT PRIMARY KEY,
  email VARCHAR(100) UNIQUE NOT NULL,
  age INT,
  tier VARCHAR(10)
);

CREATE TABLE orders (
  id INT PRIMARY KEY,
  user_id INT NOT NULL,
  amount NUMERIC,
  FOREIGN KEY (user_id) REFERENCES users(id)
);
```

Generate JSON:

```bash
datagen --ddl schema.sql --rows 100 --out json --output-path data.json
```

Generate PostgreSQL inserts:

```bash
datagen --ddl schema.sql --rows 100 --out postgres --output-path seed.sql
```

Generate CSV files, one per table:

```bash
datagen --ddl schema.sql --rows 100 --out csv --output-path ./csv_out
```

With this schema, `users` is generated first and `orders.user_id` is filled from generated `users.id` values.

---

## Common workflows

### Generate SQL for different targets

```bash
datagen --ddl schema.sql --rows 100 --out postgres
datagen --ddl schema.sql --rows 100 --out mysql
datagen --ddl schema.sql --rows 100 --out sqlite
datagen --ddl schema.sql --rows 100 --out bigquery
```

### Read schema directly from a database

```bash
datagen \
  --schema-from-db \
  --db-url postgresql+psycopg://user:pass@localhost:5432/mydb \
  --rows 100 \
  --out postgres
```

Limit DB introspection to selected tables:

```bash
datagen \
  --schema-from-db \
  --db-url postgresql+psycopg://user:pass@localhost:5432/mydb \
  --tables users,orders,events \
  --rows 100 \
  --out json
```

### Insert generated rows directly into a database

```bash
datagen \
  --ddl schema.sql \
  --rows 1000 \
  --insert \
  --db-url postgresql+psycopg://user:pass@localhost:5432/mydb
```

### Control row counts per table

```bash
datagen \
  --ddl schema.sql \
  --rows 100 \
  --table-rows users=20,orders=500 \
  --table-rows events=2000 \
  --out json
```

- `--rows` stays the global default
- `--table-rows table=count` overrides individual tables
- config files can use a `table_rows` map

### Generate a validation report

```bash
datagen \
  --ddl schema.sql \
  --rows 500 \
  --strict-checks \
  --report-path report.json \
  --out json \
  --output-path data.json
```

The report includes counts and sample issues for:

- FK violations
- non-null violations
- unique collisions
- supported CHECK violations when `--strict-checks` is enabled

### Use Parquet or the Polars engine

Parquet output:

```bash
datagen \
  --ddl schema.sql \
  --rows 100 \
  --out parquet \
  --parquet-compression zstd \
  --output-path ./parquet_out
```

Polars generation and write path:

```bash
datagen \
  --ddl schema.sql \
  --rows 100000 \
  --out csv \
  --engine polars \
  --output-path ./csv_out
```

Engine behavior:

- tables with CHECK constraints fall back to the Python row-wise generator
- tables with unique or primary-key constraints fall back to the Python row-wise generator
- FK-only tables can remain on the Polars path
- `--out parquet` requires the optional `polars` dependency regardless of `--engine`

### BigQuery-specific output

```bash
datagen --ddl schema.sql --rows 100 --out bigquery --output-path out.sql
datagen --ddl schema.sql --rows 100 --out bigquery --bq-insert-all --output-path out_insert_all.sql
```

BigQuery output supports:

- dataset-qualified table names like `dataset.table`
- `INSERT ALL` rendering via `--bq-insert-all`
- typed `DATE` and `TIMESTAMP` literals

---

## Config file example

```bash
datagen --config datagen.toml
```

Example `datagen.toml`:

```toml
ddl = "schema.sql"
rows = 500
out = "postgres"
seed = 42
strict_checks = true
table_rows = { users = 100, orders = 500, events = 2000 }
dist = [
  "users.age:normal,mean=33,std=7",
  "orders.amount:pareto,alpha=1.7,xm=1",
]
```

CLI arguments override config file values.

---

## Distribution overrides

Syntax:

```text
--dist <column_or_table.column>:<kind>,k=v,k=v
```

Supported distribution kinds:

- `normal`
- `poisson`
- `weighted`
- `exponential`
- `pareto`
- `zipf`
- `peak`

Examples:

```bash
# global column rule
--dist age:normal,mean=35,std=7

# table-qualified rule (higher priority than global)
--dist users.age:normal,mean=30,std=6

# poisson counts
--dist daily_orders:poisson,lambda=3

# weighted categorical values
--dist tier:weighted,A=60%,B=30%,C=10%

# heavy-tail behavior
--dist amount:pareto,alpha=1.5,xm=1
--dist category_rank:zipf,skew=1.8,n=200

# peak-hour timestamps
--dist created_at:peak,hours=9-11,18-20
```

Priority when both exist:

1. `table.column`
2. `column`

---

## CHECK-aware generation

`loadforge` uses practical heuristics for common CHECK constraints during generation and can optionally validate them again with `--strict-checks`.

Supported forms include:

- numeric comparison: `age >= 18`, `qty > 0`, `score <= 100`, `x != 0`
- ranges: `price BETWEEN 10 AND 20`
- enum lists: `status IN ('A', 'B', 'C')`
- regex-like checks: `code ~ '^[A-Z]{2}$'`, `REGEXP_LIKE(code, '^[0-9]+$')`
- nested compound forms built from supported expressions with `AND` and `OR`

This is intentionally best-effort, not a full SQL expression engine.

---

## Reproducible runs

Use `--seed` to stabilize Python random and Faker output:

```bash
datagen --ddl schema.sql --rows 100 --seed 42 --out json
```

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

Flag summary:

- `--config`: load defaults from JSON, TOML, YAML, or YML
- `--ddl`: input DDL file
- `--schema-from-db`: inspect table metadata from a live database
- `--tables`: optional comma-separated table filter for introspection mode
- `--rows`: global default rows per table
- `--table-rows`: per-table row overrides
- `--out`: output format, default `postgres`
- `--engine`: `python` or `polars`
- `--bq-insert-all`: BigQuery `INSERT ALL ... SELECT 1;` mode
- `--output-path`: output file path or output directory depending on format
- `--db-url`: SQLAlchemy database URL for introspection or direct insert
- `--insert`: insert generated rows directly into the target database
- `--dist`: distribution overrides
- `--seed`: deterministic generation seed
- `--report-path`: write a JSON report with validation summary
- `--strict-checks`: validate supported CHECK constraints after generation
- `--parquet-compression`: Parquet compression codec

---

## Limitations

- DDL parsing and DB introspection cover common schemas well, but advanced dialect-specific features are still partial.
- CHECK-aware generation and `--strict-checks` cover a practical subset, not arbitrary SQL expressions.
- Function-heavy or cross-column CHECK expressions are best-effort rather than fully modeled.
- `--engine polars` still falls back to Python for tables with CHECK, unique, or primary-key constraints.

---

## License

Apache License 2.0. See [LICENSE](LICENSE).
