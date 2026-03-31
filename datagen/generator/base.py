from __future__ import annotations

import random
from typing import Any

from faker import Faker

from datagen.config import ColumnMeta, DistSpec, TableMeta
from datagen.generator.dist import sample_with_dist

fake = Faker()


TYPE_MAP = {
    "int": "int",
    "integer": "int",
    "bigint": "int",
    "smallint": "int",
    "serial": "int",
    "bigserial": "int",
    "float": "float",
    "double": "float",
    "real": "float",
    "numeric": "float",
    "decimal": "float",
    "bool": "bool",
    "boolean": "bool",
    "date": "date",
    "timestamp": "datetime",
    "timestamptz": "datetime",
    "datetime": "datetime",
    "uuid": "uuid",
    "varchar": "str",
    "char": "str",
    "text": "str",
}


def _kind(type_name: str) -> str:
    t = type_name.lower()
    for k, v in TYPE_MAP.items():
        if k in t:
            return v
    return "str"


def _edge_value(col: ColumnMeta) -> Any:
    k = _kind(col.type_name)
    if k == "str":
        base = "edge_!@#$%^&*()_+|"
        if col.max_length:
            return (base * ((col.max_length // len(base)) + 1))[: col.max_length]
        return base
    if k == "int":
        return 0
    if k == "float":
        return 0.0
    if k == "bool":
        return random.choice([True, False])
    if k == "date":
        return fake.date()
    if k == "datetime":
        return fake.iso8601()
    return ""


def _default_value(col: ColumnMeta) -> Any:
    k = _kind(col.type_name)
    if k == "int":
        return random.randint(1, 100000)
    if k == "float":
        return round(random.uniform(1.0, 10000.0), 2)
    if k == "bool":
        return fake.pybool()
    if k == "date":
        return fake.date()
    if k == "datetime":
        return fake.iso8601()
    if k == "uuid":
        return str(fake.uuid4())
    if k == "str":
        val = fake.text(max_nb_chars=min(col.max_length or 40, 120)).replace("\n", " ")
        if col.max_length:
            return val[: col.max_length]
        return val
    return fake.word()


def generate_table_rows(
    table: TableMeta,
    rows: int,
    dist_overrides: dict[str, DistSpec],
    generated: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    for i in range(rows):
        row: dict[str, Any] = {}
        make_edge = (i % 20 == 0)

        for col in table.columns:
            # FK assignment first
            fk = next((x for x in table.foreign_keys if x.column == col.name), None)
            if fk and generated.get(fk.ref_table):
                parent = random.choice(generated[fk.ref_table])
                row[col.name] = parent.get(fk.ref_column)
                continue

            dist = dist_overrides.get(f"{table.name}.{col.name}") or dist_overrides.get(col.name)
            if dist is not None:
                sampled = sample_with_dist(dist)
                if sampled is not None:
                    row[col.name] = sampled
                    continue

            if col.nullable and random.random() < 0.05:
                row[col.name] = None
            elif make_edge and not col.nullable:
                row[col.name] = _edge_value(col)
            else:
                row[col.name] = _default_value(col)

        # keep simple deterministic-ish PK when integer and missing/nullable
        for col in table.columns:
            if col.primary_key and row.get(col.name) is None and _kind(col.type_name) == "int":
                row[col.name] = i + 1

        out.append(row)

    return out


def generate_all(
    tables: list[TableMeta],
    ordered_names: list[str],
    rows: int,
    dist_overrides: dict[str, DistSpec],
) -> dict[str, list[dict[str, Any]]]:
    by_name = {t.name: t for t in tables}
    generated: dict[str, list[dict[str, Any]]] = {}
    for name in ordered_names:
        generated[name] = generate_table_rows(by_name[name], rows, dist_overrides, generated)
    return generated
