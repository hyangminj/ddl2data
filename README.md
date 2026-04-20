# ddl2data

> Turn any SQL schema into realistic test data — instantly.

```bash
pipx install ddl2data
ddl2data --ddl schema.sql --rows 100000 --out postgres
```

---

## The problem

You're about to run a migration on a 10-billion-row production table.
Your staging environment has 500 rows.

You need realistic data — with the right foreign key relationships, the right
skew in the `amount` column, and enough volume to actually stress the pipeline.
But you can't copy production. So you write another hand-crafted seed script.

**ddl2data exists so you never write that script again.**

Point it at your DDL (or a live database), tell it how many rows you want, and
get back SQL inserts, JSON, CSV, Parquet, or DynamoDB JSON — with FK ordering,
type-aware values, and optional statistical distributions baked in.

---

## Who it's for

**Data engineers** running pipeline migrations, load tests, or query benchmarks
who need large, realistic relational fixtures without touching production data.

**Backend developers** who want a seeded local or staging database that reflects
real usage patterns — not just `user_1`, `user_2`, `user_3`.

---

## Quick look

```sql
-- schema.sql
CREATE TABLE users (
  id    INT PRIMARY KEY,
  email VARCHAR(100) UNIQUE NOT NULL,
  age   INT,
  tier  VARCHAR(10)
);

CREATE TABLE orders (
  id      INT PRIMARY KEY,
  user_id INT NOT NULL REFERENCES users(id),
  amount  NUMERIC
);
```

```bash
# 100k orders, FK order preserved, amount follows a power-law distribution
ddl2data \
  --ddl schema.sql \
  --rows 100000 \
  --table-rows users=5000 \
  --dist amount:pareto,alpha=1.5,xm=1 \
  --out postgres \
  --output-path seed.sql
```

`users` is generated first. Every `orders.user_id` is a valid `users.id`.
No FK violations. No hand-wiring.

---

## Why not just use Faker directly?

You could. But you'd spend an afternoon on:

- topological sort so parent rows exist before child rows
- unique constraint retries
- type mapping for every column dialect
- wiring distributions per column
- writing a CSV/Parquet/SQL serializer

ddl2data does all of that. Your schema is already the spec.

---

## Install

```bash
pipx install ddl2data        # recommended for CLI use
pip install ddl2data         # or plain pip
pip install "ddl2data[polars]"  # + Polars engine for large volumes
pip install boto3            # + DynamoDB schema loading
```

---

## Input sources

### SQL DDL file

```bash
ddl2data --ddl schema.sql --rows 1000 --out json
```

### Live relational database (introspect schema directly)

```bash
ddl2data \
  --schema-from-db \
  --db-url postgresql+psycopg://user:pass@localhost:5432/mydb \
  --tables users,orders,events \
  --rows 1000 \
  --out postgres
```

### Live DynamoDB table

```bash
ddl2data \
  --schema-from-dynamodb \
  --dynamodb-table users \
  --dynamodb-region us-east-1 \
  --dynamodb-extra-attr email:string \
  --dynamodb-extra-attr score:int \
  --rows 500 \
  --out dynamodb-json \
  --output-path users.jsonl
```

---

## Output formats

| Flag | Output |
|---|---|
| `--out postgres` | PostgreSQL `INSERT` statements |
| `--out mysql` | MySQL `INSERT` statements |
| `--out sqlite` | SQLite `INSERT` statements |
| `--out bigquery` | BigQuery `INSERT` (or `INSERT ALL` with `--bq-insert-all`) |
| `--out json` | JSON array |
| `--out csv` | CSV files, one per table |
| `--out parquet` | Parquet files, one per table |
| `--out dynamodb-json` | DynamoDB typed JSON (`.jsonl`) |

Or insert directly into a running database:

```bash
ddl2data \
  --ddl schema.sql \
  --rows 10000 \
  --insert \
  --db-url postgresql+psycopg://user:pass@localhost:5432/mydb
```

---

## Realistic distributions

Real production data is never uniform. ddl2data lets you model that.

```bash
# Age clusters around 33
--dist users.age:normal,mean=33,std=7

# Order amounts follow a power law (a few large, many small)
--dist orders.amount:pareto,alpha=1.5,xm=1

# Tier is heavily skewed toward free users
--dist users.tier:weighted,free=70%,pro=25%,enterprise=5%

# Timestamps spike at peak hours
--dist events.created_at:peak,hours=9-11,18-20

# Category ranks follow Zipf (rank 1 dominates)
--dist products.category_rank:zipf,skew=1.8,n=200

# Event counts follow Poisson
--dist daily_logins:poisson,lambda=3
```

Distribution priority: `table.column` overrides a bare `column` rule.

---

## Per-table row counts

```bash
ddl2data \
  --ddl schema.sql \
  --rows 100 \
  --table-rows users=5000 \
  --table-rows orders=200000 \
  --table-rows events=1000000 \
  --out parquet \
  --output-path ./out
```

---

## Validation and reporting

```bash
ddl2data \
  --ddl schema.sql \
  --rows 50000 \
  --strict-checks \
  --report-path report.json \
  --out json \
  --output-path data.json
```

The JSON report includes counts and sample issues for:

- FK violations
- non-null violations
- unique collisions
- CHECK constraint violations (when `--strict-checks` is set)

---

## Reproducible runs

```bash
ddl2data --ddl schema.sql --rows 1000 --seed 42 --out json
```

Same seed → same output. Useful for deterministic CI fixtures.

---

## Config file

For complex setups, drive everything from a config file:

```toml
# ddl2data.toml
ddl            = "schema.sql"
rows           = 500
out            = "postgres"
seed           = 42
strict_checks  = true

[table_rows]
users  = 100
orders = 500
events = 2000

dist = [
  "users.age:normal,mean=33,std=7",
  "orders.amount:pareto,alpha=1.7,xm=1",
]
```

```bash
ddl2data --config ddl2data.toml
```

CLI arguments override config values.

---

## Large-volume generation

For tables without CHECK, UNIQUE, or PRIMARY KEY constraints, the Polars engine
skips row-by-row Python and generates data in bulk:

```bash
ddl2data \
  --ddl schema.sql \
  --rows 5000000 \
  --out parquet \
  --engine polars \
  --parquet-compression zstd \
  --output-path ./out
```

Tables with constraints fall back to the Python engine automatically.

---

## How it works

```
Schema input
  → parse DDL / introspect live DB / load DynamoDB metadata
  → build table dependency graph (topological sort)
  → generate rows with Faker + optional distributions
  → validate FK, null, unique, CHECK constraints
  → write SQL / JSON / CSV / Parquet / DynamoDB JSON
    or INSERT directly into a running database
```

Core modules:

| Module | Role |
|---|---|
| `ddl2data/parser/ddl.py` | SQL DDL parsing via `sqlglot` |
| `ddl2data/parser/introspect.py` | Live schema introspection via SQLAlchemy |
| `ddl2data/parser/dynamodb.py` | DynamoDB schema loading via `boto3` |
| `ddl2data/generator/base.py` | Row generation engine |
| `ddl2data/generator/dist.py` | Distribution parsing and sampling |
| `ddl2data/writer/` | SQL, JSON, CSV, Parquet, DynamoDB JSON writers |
| `ddl2data/validation.py` | FK, null, unique, CHECK validation |
| `ddl2data/report.py` | JSON validation report |

---

## CLI reference

```
ddl2data [--ddl FILE | --schema-from-db | --schema-from-dynamodb --dynamodb-table NAME]
         [--db-url URL] [--tables t1,t2]
         [--dynamodb-region REGION] [--dynamodb-extra-attr name:type]
         [--config FILE]
         [--rows N] [--table-rows table=N] ...
         [--out FORMAT] [--engine python|polars]
         [--bq-insert-all]
         [--output-path PATH]
         [--insert]
         [--dist SPEC] ...
         [--seed INT]
         [--report-path FILE]
         [--strict-checks]
         [--parquet-compression snappy|zstd|lz4|gzip|none]
```

---

## Development

```bash
git clone https://github.com/hyangminj/ddl2data.git
cd ddl2data
python3 -m venv .venv && . .venv/bin/activate
pip install -e ".[test,polars]"
```

Run tests:

```bash
pytest                                          # full suite
pytest -m "not integration"                     # unit tests only
pytest tests/test_integration_postgres.py       # PostgreSQL integration
pytest tests/test_integration_dynamodb.py       # DynamoDB integration
pytest tests/test_integration_bigquery.py       # BigQuery integration
```

Local services (PostgreSQL + LocalStack DynamoDB):

```bash
docker compose up -d postgres localstack
```

Copy `.env.test.example` to `.env.test` and fill in your credentials.

---

## Limitations

- DDL parsing covers common SQL well; advanced dialect-specific features are partial.
- DynamoDB schema loading models key and index attributes plus explicitly declared extra attributes; it does not infer document structure from item samples.
- CHECK-aware generation covers a practical subset — not arbitrary SQL expressions.
- `--engine polars` falls back to Python for tables with CHECK, UNIQUE, or PRIMARY KEY constraints.

---

## License

[Apache 2.0](LICENSE)
