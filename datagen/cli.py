from __future__ import annotations

import argparse
import random
from pathlib import Path

from faker import Faker
from sqlalchemy import MetaData, Table, create_engine

from datagen.generator.base import generate_all
from datagen.generator.dist import parse_dist_arg
from datagen.parser.ddl import parse_ddl_file
from datagen.parser.graph import generation_order
from datagen.parser.introspect import load_schema_from_db
from datagen.writer.csv_writer import write_csv
from datagen.writer.json_writer import write_json
from datagen.writer.postgres import render_insert_sql


def _insert_via_sqlalchemy(db_url: str, data: dict[str, list[dict]]) -> None:
    engine = create_engine(db_url)
    meta = MetaData()
    with engine.begin() as conn:
        for table_name, rows in data.items():
            if not rows:
                continue
            table = Table(table_name, meta, autoload_with=conn)
            conn.execute(table.insert(), rows)


def _resolve_tables_from_args(args: argparse.Namespace):
    if args.schema_from_db:
        if not args.db_url:
            raise SystemExit("--schema-from-db requires --db-url")
        engine = create_engine(args.db_url)
        table_names = [x.strip() for x in args.tables.split(",")] if args.tables else None
        return load_schema_from_db(engine, table_names)

    if not args.ddl:
        raise SystemExit("Provide either --ddl <schema.sql> or --schema-from-db")

    return parse_ddl_file(args.ddl)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Generate synthetic data from DDL or live DB schema")

    # Input source
    p.add_argument("--ddl", help="Path to schema.sql")
    p.add_argument("--schema-from-db", action="store_true", help="Introspect tables from --db-url instead of using --ddl")
    p.add_argument("--tables", help="Comma-separated table names to include when using --schema-from-db")

    # Generation/output
    p.add_argument("--rows", type=int, required=True, help="Rows per table")
    p.add_argument("--out", choices=["postgres", "json", "csv"], default="postgres")
    p.add_argument("--db-url", help="DB URL for schema introspection and/or direct insert")
    p.add_argument("--insert", action="store_true", help="Insert generated rows into --db-url")
    p.add_argument(
        "--dist",
        action="append",
        default=[],
        help="Distribution override, e.g. age:normal,mean=35,std=7 or users.age:normal,mean=35,std=7",
    )
    p.add_argument("--seed", type=int, default=None, help="Random seed for reproducible generation")
    p.add_argument("--output-path", default=None, help="Optional output file/path (json/sql file or csv dir)")
    return p


def main() -> None:
    args = build_parser().parse_args()

    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)

    tables = _resolve_tables_from_args(args)
    order = generation_order(tables)
    dist_map = dict(parse_dist_arg(d) for d in args.dist)
    data = generate_all(tables, order, args.rows, dist_map)

    if args.insert:
        if not args.db_url:
            raise SystemExit("--insert requires --db-url")
        _insert_via_sqlalchemy(args.db_url, data)

    if args.out == "json":
        payload = write_json(data, args.output_path)
        if not args.output_path:
            print(payload)
        return

    if args.out == "csv":
        out_dir = args.output_path or "./output_csv"
        files = write_csv(data, out_dir)
        print("\n".join(files))
        return

    chunks = []
    for table_name in order:
        sql = render_insert_sql(table_name, data.get(table_name, []))
        if sql:
            chunks.append(sql)
    output = "\n".join(chunks)
    if args.output_path:
        Path(args.output_path).write_text(output, encoding="utf-8")
    else:
        print(output)


if __name__ == "__main__":
    main()
