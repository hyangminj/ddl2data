# ddl2data

Turn any SQL schema into realistic test data — instantly.

- PyPI package: `ddl2data`
- CLI command: `ddl2data`
- Python module: `datagen`

`ddl2data` turns SQL DDL, a live relational schema, or a live DynamoDB table schema into fake but structured data that is useful for load tests, end-to-end pipeline checks, local development, and staging environment seeding.

For relational schemas it parses tables and foreign keys, generates rows in parent-before-child order, applies type-aware defaults plus optional distributions, validates the generated data, and writes it out as SQL, JSON, CSV, or Parquet. It can also insert generated rows directly into a relational database through SQLAlchemy. For DynamoDB it can load key and index metadata from a real table and emit typed DynamoDB JSON payloads.

---

## Why use it

Use `ddl2data` when you need to:

- stress a pipeline or service with large synthetic datasets
- generate relational fixtures from existing DDL
- seed staging or local environments while preserving foreign-key order
- inspect a live schema and generate data without hand-writing metadata
- generate DynamoDB-shaped typed JSON from a real table definition
- export the same dataset shape in SQL, JSON, CSV, Parquet, or DynamoDB JSON
- produce validation reports before loading data elsewhere

---

## How it works

```text
Schema input
  -> parse DDL, inspect a live relational DB, or inspect a DynamoDB table
  -> build metadata and dependency order
  -> generate rows with Faker and optional distributions
  -> validate FK, null, unique, and optional CHECK constraints
  -> write SQL/JSON/CSV/Parquet/DynamoDB JSON or insert into a DB
```

Core modules:

- `datagen/parser/ddl.py`: SQL DDL parsing via `sqlglot`
- `datagen/parser/introspect.py`: relational schema introspection via SQLAlchemy
- `datagen/parser/dynamodb.py`: DynamoDB schema loading via `boto3`
- `datagen/generator/base.py`: main row generation engine
- `datagen/generator/dist.py`: distribution parsing and sampling
- `datagen/writer/`: SQL, JSON, CSV, Parquet, and DynamoDB JSON writers
- `datagen/validation.py` and `datagen/report.py`: validation and reporting

---

## Core capabilities

- Input sources:
  - SQL DDL via `--ddl`
  - live relational schema introspection via `--schema-from-db --db-url ...`
  - live DynamoDB table schema via `--schema-from-dynamodb --dynamodb-table ...`
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
  - DynamoDB typed JSON
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
  - optional `python` or `polars` engine selection

---

## Install

Recommended for CLI use:

```bash
pipx install ddl2data
```

Or install with `pip`:

```bash
pip install ddl2data
```

Optional Polars support:

```bash
pip install "ddl2data[polars]"
```

DynamoDB schema loading requires `boto3`:

```bash
pip install boto3
```

From source:

```bash
git clone https://github.com/hyangminj/ddl2data.git
cd ddl2data
python3 -m venv .venv
. .venv/bin/activate
.venv/bin/python -m pip install -e .
```

Contributor setup with test and Polars extras:

```bash
.venv/bin/python -m pip install -e ".[test,polars]"
```

Sanity check:

```bash
ddl2data --help
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
ddl2data --ddl schema.sql --rows 100 --out json --output-path data.json
```

Generate PostgreSQL inserts:

```bash
ddl2data --ddl schema.sql --rows 100 --out postgres --output-path seed.sql
```

Generate CSV files, one per table:

```bash
ddl2data --ddl schema.sql --rows 100 --out csv --output-path ./csv_out
```

With this schema, `users` is generated first and `orders.user_id` is filled from generated `users.id` values.

---

## Common workflows

### Generate SQL for different targets

```bash
ddl2data --ddl schema.sql --rows 100 --out postgres
ddl2data --ddl schema.sql --rows 100 --out mysql
ddl2data --ddl schema.sql --rows 100 --out sqlite
ddl2data --ddl schema.sql --rows 100 --out bigquery
```

### Read schema directly from a relational database

```bash
ddl2data \
  --schema-from-db \
  --db-url postgresql+psycopg://user:pass@localhost:5432/mydb \
  --rows 100 \
  --out postgres
```

Limit introspection to selected tables:

```bash
ddl2data \
  --schema-from-db \
  --db-url postgresql+psycopg://user:pass@localhost:5432/mydb \
  --tables users,orders,events \
  --rows 100 \
  --out json
```

### Insert generated rows directly into a relational database

```bash
ddl2data \
  --ddl schema.sql \
  --rows 1000 \
  --insert \
  --db-url postgresql+psycopg://user:pass@localhost:5432/mydb
```

### Generate DynamoDB typed JSON from a live table schema

```bash
ddl2data \
  --schema-from-dynamodb \
  --dynamodb-table users \
  --dynamodb-region us-east-1 \
  --dynamodb-extra-attr email:string \
  --dynamodb-extra-attr score:int \
  --rows 100 \
  --out dynamodb-json \
  --output-path users.jsonl
```

Notes:

- key attributes and GSI or LSI key attributes are inferred from the live table
- use `--dynamodb-extra-attr name:type` to add non-key fields to generated output
- supported extra attribute aliases include `string`, `uuid`, `date`, `datetime`, `numeric`, `float`, `int`, and `boolean`

### Control row counts per table

```bash
ddl2data \
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
ddl2data \
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
ddl2data \
  --ddl schema.sql \
  --rows 100 \
  --out parquet \
  --parquet-compression zstd \
  --output-path ./parquet_out
```

Polars generation and write path:

```bash
ddl2data \
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
ddl2data --ddl schema.sql --rows 100 --out bigquery --output-path out.sql
ddl2data --ddl schema.sql --rows 100 --out bigquery --bq-insert-all --output-path out_insert_all.sql
```

BigQuery output supports:

- dataset-qualified table names like `dataset.table`
- `INSERT ALL` rendering via `--bq-insert-all`
- typed `DATE` and `TIMESTAMP` literals

---

## Config file example

```bash
ddl2data --config datagen.toml
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

`ddl2data` uses practical heuristics for common CHECK constraints during generation and can optionally validate them again with `--strict-checks`.

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
ddl2data --ddl schema.sql --rows 100 --seed 42 --out json
```

---

## Development and testing

### Local setup

```bash
python3 -m venv .venv
. .venv/bin/activate
.venv/bin/python -m pip install -e ".[test,polars]"
```

If you only need the core package:

```bash
.venv/bin/python -m pip install -e .
```

If you need DynamoDB schema loading outside the test extra:

```bash
.venv/bin/python -m pip install boto3
```

### Useful test commands

Run the full suite:

```bash
.venv/bin/python -m pytest
```

Run only non-integration tests:

```bash
.venv/bin/python -m pytest -m "not integration"
```

Run one file:

```bash
.venv/bin/python -m pytest tests/test_parser_graph.py
```

Run one integration target:

```bash
.venv/bin/python -m pytest tests/test_integration_postgres.py
.venv/bin/python -m pytest tests/test_integration_dynamodb.py
.venv/bin/python -m pytest tests/test_integration_bigquery.py
```

Available markers:

- `integration`
- `postgres`
- `dynamodb`
- `bigquery`

### Local integration services

The repo includes `docker-compose.yml` for PostgreSQL and LocalStack DynamoDB:

```bash
docker compose up -d postgres localstack
docker compose ps
```

Service summary:

- PostgreSQL: `localhost:5432`
- LocalStack DynamoDB: `http://localhost:4566`

Suggested `.env.test`:

```dotenv
TEST_POSTGRES_URL=postgresql+psycopg2://testuser:testpass@localhost:5432/testdb
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=us-east-1
DYNAMODB_ENDPOINT_URL=http://localhost:4566
TEST_BQ_PROJECT=your-gcp-project-id
TEST_BQ_DATASET=datagen_integration_test
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

`.env.test` is already ignored and is the right place for local test-only credentials.

### BigQuery authentication

Two common options work for the integration tests:

Application Default Credentials:

```bash
gcloud auth application-default login
```

Service-account key file:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

If `TEST_BQ_PROJECT` is missing or credentials are unavailable, the BigQuery integration tests skip automatically.

---

## CLI reference

```bash
ddl2data [--ddl schema.sql
        | --schema-from-db --db-url URL [--tables t1,t2]
        | --schema-from-dynamodb --dynamodb-table NAME]
        [--dynamodb-region REGION]
        [--dynamodb-extra-attr name:type]
        [--config config.toml]
        [--rows 100]
        [--table-rows users=20,orders=500] [--table-rows events=2000]
        [--out postgres|mysql|sqlite|bigquery|json|csv|parquet|dynamodb-json]
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
- `--schema-from-db`: inspect relational table metadata from a live database
- `--tables`: optional comma-separated table filter for relational introspection mode
- `--schema-from-dynamodb`: inspect a live DynamoDB table definition
- `--dynamodb-table`: DynamoDB table name for `--schema-from-dynamodb`
- `--dynamodb-region`: AWS region for DynamoDB schema loading
- `--dynamodb-extra-attr`: add synthetic non-key DynamoDB attributes, repeatable
- `--rows`: global default rows per table
- `--table-rows`: per-table row overrides
- `--out`: output format, default `postgres`
- `--engine`: `python` or `polars`
- `--bq-insert-all`: BigQuery `INSERT ALL ... SELECT 1;` mode
- `--output-path`: output file path or output directory depending on format
- `--db-url`: SQLAlchemy database URL for introspection or direct insert
- `--insert`: insert generated rows directly into the target relational database
- `--dist`: distribution overrides
- `--seed`: deterministic generation seed
- `--report-path`: write a JSON report with validation summary
- `--strict-checks`: validate supported CHECK constraints after generation
- `--parquet-compression`: Parquet compression codec

---

## Limitations

- DDL parsing and relational DB introspection cover common schemas well, but advanced dialect-specific features are still partial.
- DynamoDB schema loading models key and index attributes plus explicitly declared extra attributes; it does not infer full document structure from item samples.
- CHECK-aware generation and `--strict-checks` cover a practical subset, not arbitrary SQL expressions.
- Function-heavy or cross-column CHECK expressions are best-effort rather than fully modeled.
- `--engine polars` still falls back to Python for tables with CHECK, unique, or primary-key constraints.

---

## Future work

- More distribution types for realistic synthetic workloads
- Additional pipeline edge-case generation such as skew, timestamp boundaries, and duplicate-heavy batches
- Broader dialect coverage and richer schema inference for advanced database features

---

## License

Apache License 2.0. See [LICENSE](LICENSE).
