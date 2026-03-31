# datagen

`datagen` is a CLI tool that generates realistic synthetic test data from SQL DDL **or from a live DB schema**.

It parses table definitions and foreign-key relationships, generates rows with sensible defaults + statistical distributions, and can either:

- print/save output (`postgres` SQL, `json`, `csv`), or
- insert directly into a database via SQLAlchemy.

---

## What it does

- Parse DDL with `sqlglot` (table/column/basic constraints/FK)
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
- Optional direct insert: `--insert --db-url ...`
- Config-file driven runs (`--config`) with JSON/TOML/YAML
- Optional generation report output (`--report-path`)

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

### 1) Generate SQL from DDL (postgres/mysql/sqlite)

```bash
datagen --ddl schema.sql --rows 100 --out postgres
datagen --ddl schema.sql --rows 100 --out mysql
datagen --ddl schema.sql --rows 100 --out sqlite
```

### 2) Generate JSON from DDL

```bash
datagen --ddl schema.sql --rows 100 --out json --output-path data.json
```

### 3) Generate CSV from DDL (one file per table)

```bash
datagen --ddl schema.sql --rows 100 --out csv --output-path ./csv_out
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
dist = [
  "users.age:normal,mean=33,std=7",
  "orders.amount:pareto,alpha=1.7,xm=1",
]
```

### 8) Write generation report

```bash
datagen --ddl schema.sql --rows 500 --report-path report.json --out json --output-path data.json
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
        --rows 100
        [--out postgres|mysql|sqlite|json|csv]
        [--output-path PATH]
        [--insert --db-url URL]
        [--dist ...] [--dist ...]
        [--seed INT]
        [--report-path report.json]
```

- `--config`: load defaults from JSON/TOML/YAML config file
- `--ddl`: input DDL file
- `--schema-from-db`: introspect table schema from DB
- `--tables`: optional comma-separated table filter for DB introspection mode
- `--rows` (required): rows generated **per table**
- `--out`: output format (default: `postgres`)
- `--output-path`: output file path (`json`/`sql`) or directory (`csv`)
- `--db-url`: DB connection URL
- `--insert`: insert generated rows into `--db-url`
- `--dist`: distribution override(s)
- `--seed`: deterministic run seed
- `--report-path`: write a JSON profile report (rows/null-ratio/top-values)

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

- `--rows` is per table (not per-table custom counts yet)
- DDL support is strong for common patterns, but not every advanced SQL dialect feature
- Constraint-aware generation is basic (complex uniqueness/check constraints not fully modeled)

---

## Roadmap (proposed)

- Better constraint handling (composite unique, richer checks)
- Multi-dialect writers (MySQL/SQLite/BigQuery)
- Config file support (YAML/TOML)
- Stronger validation/reporting of generated datasets
- Performance engine option (`--engine python|polars`) for vectorized generation and faster file output on large datasets
