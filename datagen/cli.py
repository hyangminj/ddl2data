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
from datagen.validation import validate_check_constraints, validate_generated_data
from datagen.writer.csv_writer import write_csv
from datagen.writer.json_writer import write_json
from datagen.writer.postgres import render_insert_sql
from datagen.writer.parquet_writer import write_parquet


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


def _parse_table_rows_map(raw_entries: list[str] | None) -> dict[str, int]:
    out: dict[str, int] = {}
    for entry in raw_entries or []:
        for token in [t.strip() for t in str(entry).split(",") if t.strip()]:
            if "=" not in token:
                raise SystemExit(f"Invalid --table-rows token '{token}'. Use table=count")
            table, count_raw = token.split("=", 1)
            table = table.strip()
            try:
                count = int(count_raw.strip())
            except ValueError as e:
                raise SystemExit(f"Invalid row count in --table-rows token '{token}'") from e
            if count < 0:
                raise SystemExit(f"Row count must be >=0 in --table-rows token '{token}'")
            out[table] = count
    return out


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Generate synthetic data from DDL or live DB schema")

    p.add_argument("--config", help="Path to config file (.json/.toml/.yaml/.yml)")

    # Input source
    p.add_argument("--ddl", help="Path to schema.sql")
    p.add_argument("--schema-from-db", action="store_true", default=None, help="Introspect tables from --db-url instead of using --ddl")
    p.add_argument("--tables", help="Comma-separated table names to include when using --schema-from-db")

    # Generation/output
    p.add_argument("--rows", type=int, help="Default rows per table")
    p.add_argument(
        "--table-rows",
        action="append",
        default=None,
        help="Per-table row count override, e.g. users=100,orders=200 (repeatable)",
    )
    p.add_argument("--out", choices=["postgres", "mysql", "sqlite", "bigquery", "json", "csv", "parquet"], default=None)
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
    p.add_argument("--strict-checks", action="store_true", default=None, help="Validate generated rows against supported CHECK constraints")
    p.add_argument("--engine", choices=["python", "polars"], default=None, help="Generation/render engine")
    p.add_argument("--bq-insert-all", action="store_true", default=None, help="When --out bigquery, render INSERT ALL syntax")
    p.add_argument("--parquet-compression", choices=["snappy", "zstd", "lz4", "gzip", "none"], default=None, help="Parquet compression codec (default: snappy)")
    return p


def _merge_config(args: argparse.Namespace) -> argparse.Namespace:
    cfg = load_config(args.config)

    defaults: dict[str, Any] = {
        "out": "postgres",
        "dist": [],
        "schema_from_db": False,
        "insert": False,
        "engine": "python",
        "table_rows": [],
        "bq_insert_all": False,
        "parquet_compression": "snappy",
        "strict_checks": False,
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
        "strict_checks",
        "engine",
        "bq_insert_all",
        "parquet_compression",
    ]:
        if getattr(args, key) is None:
            if key in cfg:
                setattr(args, key, cfg[key])

    # dist handling (CLI wins)
    cfg_dist = cfg.get("dist", []) if isinstance(cfg, dict) else []
    if not isinstance(cfg_dist, list):
        cfg_dist = []
    cli_dist = args.dist or []
    if cli_dist:
        args.dist = cli_dist
    else:
        args.dist = [str(x) for x in cfg_dist]

    # table-rows: CLI value or config map/list
    if args.table_rows:
        table_rows_raw = args.table_rows
    else:
        cfg_tr = cfg.get("table_rows", {}) if isinstance(cfg, dict) else {}
        if isinstance(cfg_tr, dict):
            table_rows_raw = [f"{k}={v}" for k, v in cfg_tr.items()]
        elif isinstance(cfg_tr, list):
            table_rows_raw = [str(x) for x in cfg_tr]
        else:
            table_rows_raw = []
    args.table_rows = table_rows_raw

    if args.rows is None and not args.table_rows:
        raise SystemExit("--rows is required unless --table-rows is set (or configured)")

    if args.rows is None:
        args.rows = 0

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
    table_rows_map = _parse_table_rows_map(args.table_rows)
    data = generate_all(
        tables,
        order,
        int(args.rows),
        dist_map,
        table_rows=table_rows_map,
        engine=args.engine,
    )

    if args.insert:
        if not args.db_url:
            raise SystemExit("--insert requires --db-url")
        _insert_via_sqlalchemy(args.db_url, data)

    validation = validate_generated_data(tables, data)
    if args.strict_checks:
        check_validation = validate_check_constraints(tables, data)
        validation["counts"]["check_violations"] = check_validation["check_violations"]
        validation["sample_issues"].extend(check_validation["check_details"])
        validation["counts"]["sample_issue_count"] = len(validation["sample_issues"])
        if check_validation["check_violations"] > 0:
            validation["pass"] = False
            validation["counts"]["total_failures"] += check_validation["check_violations"]

    if args.report_path:
        report = build_report(data, validation=validation)
        write_json(report, args.report_path, engine=args.engine)

    if args.out == "json":
        payload = write_json(data, args.output_path, engine=args.engine)
        if not args.output_path:
            print(payload)
        return

    if args.out == "csv":
        out_dir = args.output_path or "./output_csv"
        files = write_csv(data, out_dir, engine=args.engine)
        print("\n".join(files))
        return

    if args.out == "parquet":
        out_dir = args.output_path or "./output_parquet"
        files = write_parquet(data, out_dir, compression=args.parquet_compression)
        print("\n".join(files))
        return

    dialect = args.out if args.out in {"postgres", "mysql", "sqlite", "bigquery"} else "postgres"
    chunks = []
    for table_name in order:
        sql = render_insert_sql(
            table_name,
            data.get(table_name, []),
            dialect=dialect,
            engine=args.engine,
            bq_insert_all=bool(args.bq_insert_all),
        )
        if sql:
            chunks.append(sql)
    output = "\n".join(chunks)
    if args.output_path:
        Path(args.output_path).write_text(output, encoding="utf-8")
    else:
        print(output)


if __name__ == "__main__":
    main()
