from __future__ import annotations

import argparse
import random
from pathlib import Path
from typing import Any

from faker import Faker
from sqlalchemy import MetaData, Table, create_engine

from datagen.config_loader import load_config
from datagen.generator.base import generate_all
from datagen.generator.dist import parse_dist_arg
from datagen.parser.ddl import parse_ddl_file
from datagen.parser.graph import generation_order
from datagen.parser.introspect import load_schema_from_db
from datagen.report import build_report
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

    p.add_argument("--config", help="Path to config file (.json/.toml/.yaml/.yml)")

    # Input source
    p.add_argument("--ddl", help="Path to schema.sql")
    p.add_argument("--schema-from-db", action="store_true", default=None, help="Introspect tables from --db-url instead of using --ddl")
    p.add_argument("--tables", help="Comma-separated table names to include when using --schema-from-db")

    # Generation/output
    p.add_argument("--rows", type=int, help="Rows per table")
    p.add_argument("--out", choices=["postgres", "mysql", "sqlite", "json", "csv"], default=None)
    p.add_argument("--db-url", help="DB URL for schema introspection and/or direct insert")
    p.add_argument("--insert", action="store_true", default=None, help="Insert generated rows into --db-url")
    p.add_argument(
        "--dist",
        action="append",
        default=None,
        help="Distribution override, e.g. age:normal,mean=35,std=7 or users.age:normal,mean=35,std=7",
    )
    p.add_argument("--seed", type=int, default=None, help="Random seed for reproducible generation")
    p.add_argument("--output-path", default=None, help="Optional output file/path (json/sql file or csv dir)")
    p.add_argument("--report-path", default=None, help="Optional JSON report path for generated data profile")
    return p


def _merge_config(args: argparse.Namespace) -> argparse.Namespace:
    cfg = load_config(args.config)

    defaults: dict[str, Any] = {
        "out": "postgres",
        "dist": [],
        "schema_from_db": False,
        "insert": False,
    }

    for k, v in defaults.items():
        if getattr(args, k) is None:
            setattr(args, k, v)

    # config as base
    for key in [
        "ddl",
        "schema_from_db",
        "tables",
        "rows",
        "out",
        "db_url",
        "insert",
        "seed",
        "output_path",
        "report_path",
    ]:
        if getattr(args, key) is None:
            if key in cfg:
                setattr(args, key, cfg[key])

    # dist handling (list merge, CLI wins by append)
    cfg_dist = cfg.get("dist", []) if isinstance(cfg, dict) else []
    if not isinstance(cfg_dist, list):
        cfg_dist = []
    cli_dist = args.dist or []
    if not cli_dist and cfg_dist:
        args.dist = [str(x) for x in cfg_dist]
    elif cli_dist:
        args.dist = cli_dist
    else:
        args.dist = []

    if args.rows is None:
        raise SystemExit("--rows is required (or set rows in --config)")

    if args.out is None:
        args.out = "postgres"

    return args


def main() -> None:
    args = build_parser().parse_args()
    args = _merge_config(args)

    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)

    tables = _resolve_tables_from_args(args)
    order = generation_order(tables)
    dist_map = dict(parse_dist_arg(d) for d in (args.dist or []))
    data = generate_all(tables, order, int(args.rows), dist_map)

    if args.insert:
        if not args.db_url:
            raise SystemExit("--insert requires --db-url")
        _insert_via_sqlalchemy(args.db_url, data)

    if args.report_path:
        report = build_report(data)
        write_json(report, args.report_path)

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

    dialect = args.out if args.out in {"postgres", "mysql", "sqlite"} else "postgres"
    chunks = []
    for table_name in order:
        sql = render_insert_sql(table_name, data.get(table_name, []), dialect=dialect)
        if sql:
            chunks.append(sql)
    output = "\n".join(chunks)
    if args.output_path:
        Path(args.output_path).write_text(output, encoding="utf-8")
    else:
        print(output)


if __name__ == "__main__":
    main()
