# datagen

MVP synthetic data generator from SQL DDL.

## Features

- Parse SQL DDL with `sqlglot` to extract tables, columns, basic constraints, and FKs
- Build FK dependency graph and table generation order
- Generate fake rows with Faker defaults by type
- Optional per-column distributions:
  - `--dist age:normal,mean=35,std=7`
  - `--dist users.age:normal,mean=35,std=7` (table-qualified)
  - `--dist events:poisson,lambda=2`
  - `--dist tier:weighted,A=60%,B=40%`
- Occasional edge-case rows (NULL where allowed, special chars, near max length strings)
- Output formats:
  - PostgreSQL INSERT SQL
  - JSON
  - CSV (one file per table)
- Optional direct DB insert via SQLAlchemy (`--db-url`)

## Install

```bash
cd datagen
pip install -e .
```

## Usage

```bash
python -m datagen.cli --ddl schema.sql --rows 100 --out postgres
python -m datagen.cli --ddl schema.sql --rows 100 --out json --output-path data.json
python -m datagen.cli --ddl schema.sql --rows 100 --out csv --output-path ./csv_out
python -m datagen.cli --ddl schema.sql --rows 100 --db-url postgresql+psycopg://user:pass@localhost/db
python -m datagen.cli --ddl schema.sql --rows 100 --dist age:normal,mean=30,std=6 --dist tier:weighted,A=70%,B=30%
python -m datagen.cli --ddl schema.sql --rows 100 --dist users.age:normal,mean=30,std=6 --seed 42
```

Console script (after install):

```bash
datagen --ddl schema.sql --rows 50 --out postgres
```

## Notes

- `--rows` is per table in this MVP.
- FK values are selected from already generated parent rows according to dependency order.
