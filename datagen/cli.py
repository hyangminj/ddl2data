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


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Generate synthetic data from DDL")
    p.add_argument("--ddl", required=True, help="Path to schema.sql")
    p.add_argument("--rows", type=int, required=True, help="Rows per table")
    p.add_argument("--out", choices=["postgres", "json", "csv"], default="postgres")
    p.add_argument("--db-url", help="Insert directly into DB (PostgreSQL for MVP)")
    p.add_argument("--dist", action="append", default=[], help="Distribution override, e.g. age:normal,mean=35,std=7 or users.age:normal,mean=35,std=7")
    p.add_argument("--seed", type=int, default=None, help="Random seed for reproducible generation")
    p.add_argument("--output-path", default=None, help="Optional output file/path (json/sql file or csv dir)")
    return p


def main() -> None:
    args = build_parser().parse_args()

    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)

    tables = parse_ddl_file(args.ddl)
    order = generation_order(tables)
    dist_map = dict(parse_dist_arg(d) for d in args.dist)
    data = generate_all(tables, order, args.rows, dist_map)

    if args.db_url:
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
