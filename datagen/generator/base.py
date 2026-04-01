from __future__ import annotations

import random
import re
from typing import Any

from faker import Faker

from datagen.config import CheckConstraintMeta, ColumnMeta, DistSpec, TableMeta
from datagen.generator.dist import sample_with_dist

fake = Faker()

EMAIL_DOMAIN_CANDIDATES = ("example.test", "example.com", "x.io", "b.co")


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


def _string_field_key(col: ColumnMeta) -> str | None:
    name = col.name.lower()
    if "email" in name or name == "mail":
        return "email"
    if "ipv4" in name:
        return "ipv4"
    if "ipv6" in name:
        return "ipv6"
    if name == "ip" or name.startswith("ip_") or name.endswith("_ip") or "ip_address" in name:
        return "ip"
    if "username" in name or "user_name" in name:
        return "username"
    if "phone" in name or "mobile" in name or "msisdn" in name:
        return "phone"
    if "street" in name:
        return "street_address"
    if "address" in name:
        return "address"
    if "postal" in name or name == "zip" or "zip_code" in name or "zipcode" in name:
        return "postal_code"
    if "city" in name:
        return "city"
    if "state" in name or "province" in name or "region" in name:
        return "state"
    if "country" in name:
        return "country"
    return None


def _truncate_string(col: ColumnMeta, value: str) -> str:
    return value[: col.max_length] if col.max_length else value


def _stretch_string_to_limit(col: ColumnMeta, value: str, fill: str) -> str:
    if col.max_length is None or len(value) >= col.max_length:
        return _truncate_string(col, value)
    return f"{value}{fill * (col.max_length - len(value))}"


def _fit_suffix(col: ColumnMeta, value: str, suffix: str, sep: str = " ") -> str:
    full_suffix = f"{sep}{suffix}" if suffix else ""
    if col.max_length is None:
        return f"{value}{full_suffix}"
    if len(full_suffix) >= col.max_length:
        return full_suffix[-col.max_length :]
    return f"{value[: col.max_length - len(full_suffix)]}{full_suffix}"


def _fit_prefix(col: ColumnMeta, prefix: str, value: str, sep: str = " ") -> str:
    full_prefix = f"{prefix}{sep}" if prefix else ""
    if col.max_length is None:
        return f"{full_prefix}{value}"
    if len(full_prefix) >= col.max_length:
        return full_prefix[: col.max_length]
    return f"{full_prefix}{value[: col.max_length - len(full_prefix)]}"


def _ipv4_unique_value(first: int, second: int, unique_token: int) -> str:
    token = max(unique_token, 0)
    third = (token >> 8) & 255
    fourth = token & 255
    return f"{first}.{second}.{third}.{fourth}"


def _email_domain(max_length: int | None) -> str:
    for domain in EMAIL_DOMAIN_CANDIDATES:
        if max_length is None or max_length >= len(domain) + 2:
            return domain
    return EMAIL_DOMAIN_CANDIDATES[-1]


def _email_value(col: ColumnMeta, local_part: str) -> str:
    domain = _email_domain(col.max_length)
    local = re.sub(r"[^a-z0-9._+-]+", "", local_part.lower()) or "user"
    if col.max_length is None:
        return f"{local}@{domain}"

    max_local_length = col.max_length - len(domain) - 1
    if max_local_length < 1:
        return f"u@{domain}"[: col.max_length]

    trimmed_local = local[:max_local_length].rstrip("._+-") or "u"
    return f"{trimmed_local}@{domain}"


def _structured_string_value(col: ColumnMeta, unique_token: int | None = None, edge: bool = False) -> str | None:
    field_key = _string_field_key(col)
    if field_key is None:
        return None

    if field_key == "email":
        if edge:
            return _email_value(col, f"{col.name}.edge.boundary.value.xxxxxxxxxxxx")
        local_part = (
            f"{fake.user_name()}.{unique_token}"
            if unique_token is not None
            else f"{fake.user_name()}.{random.randint(1, 9999)}"
        )
        return _email_value(col, local_part)

    if field_key == "username":
        raw = "user.edge.boundary" if edge else fake.user_name()
        if unique_token is not None:
            return _fit_suffix(col, raw, str(unique_token), sep=".")
        if edge:
            return _stretch_string_to_limit(col, raw, "x")
        return _truncate_string(col, raw)

    if field_key == "ipv4":
        if unique_token is not None:
            return _truncate_string(col, _ipv4_unique_value(198, 18, unique_token))
        if edge:
            return _truncate_string(col, "255.255.255.255")
        return _truncate_string(col, fake.ipv4())

    if field_key == "ipv6":
        if unique_token is not None:
            return _truncate_string(col, f"2001:db8::{unique_token:x}")
        if edge:
            return _truncate_string(col, "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff")
        return _truncate_string(col, fake.ipv6())

    if field_key == "ip":
        if unique_token is not None:
            return _truncate_string(col, _ipv4_unique_value(198, 19, unique_token))
        if edge:
            return _truncate_string(col, "255.255.255.255")
        return _truncate_string(col, fake.ipv4())

    if field_key == "phone":
        raw = "555-0100 ext 999999" if edge else fake.phone_number().replace("\n", " ")
        if unique_token is not None:
            return _fit_suffix(col, raw, str(unique_token), sep=" x")
        if edge:
            return _stretch_string_to_limit(col, raw, "9")
        return _truncate_string(col, raw)

    if field_key == "street_address":
        raw = "9999 Edge Boundary Street" if edge else fake.street_address().replace("\n", ", ")
        if unique_token is not None:
            return _fit_prefix(col, str(unique_token), raw)
        if edge:
            return _stretch_string_to_limit(col, raw, "x")
        return _truncate_string(col, raw)

    if field_key == "address":
        raw = "9999 Edge Boundary Ave, Suite 999" if edge else fake.address().replace("\n", ", ")
        if unique_token is not None:
            return _fit_prefix(col, str(unique_token), raw)
        if edge:
            return _stretch_string_to_limit(col, raw, "x")
        return _truncate_string(col, raw)

    if field_key == "city":
        raw = "EdgeCityBoundary" if edge else fake.city()
        if unique_token is not None:
            return _fit_suffix(col, raw, str(unique_token))
        if edge:
            return _stretch_string_to_limit(col, raw, "x")
        return _truncate_string(col, raw)

    if field_key == "state":
        raw = "EdgeStateBoundary" if edge else fake.state()
        if unique_token is not None:
            return _fit_suffix(col, raw, str(unique_token))
        if edge:
            return _stretch_string_to_limit(col, raw, "x")
        return _truncate_string(col, raw)

    if field_key == "postal_code":
        raw = "99999-9999" if edge else fake.postcode()
        if unique_token is not None:
            return _fit_suffix(col, raw, str(unique_token), sep="-")
        if edge:
            return _stretch_string_to_limit(col, raw, "0")
        return _truncate_string(col, raw)

    if field_key == "country":
        raw = "EdgeCountryBoundary" if edge else fake.country()
        if unique_token is not None:
            return _fit_suffix(col, raw, str(unique_token))
        if edge:
            return _stretch_string_to_limit(col, raw, "x")
        return _truncate_string(col, raw)

    return None


def _edge_value(col: ColumnMeta) -> Any:
    k = _kind(col.type_name)
    if k == "str":
        structured = _structured_string_value(col, edge=True)
        if structured is not None:
            return structured
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
        structured = _structured_string_value(col)
        if structured is not None:
            return structured
        max_chars = min(col.max_length or 40, 120)
        if max_chars < 5:
            val = fake.lexify(text="?" * max_chars) if max_chars > 0 else ""
        else:
            val = fake.text(max_nb_chars=max_chars).replace("\n", " ")
        if col.max_length:
            return val[: col.max_length]
        return val
    return fake.word()


def _typed_number(limit: float, col_kind: str, epsilon: float = 0.01) -> int | float:
    if col_kind == "int":
        return int(round(limit))
    return float(limit + epsilon)


def _coerce_to_col_type(value: Any, col: ColumnMeta) -> Any:
    if value is None:
        return None
    k = _kind(col.type_name)
    try:
        if k == "int":
            return int(float(value))
        if k == "float":
            return float(value)
        if k == "str":
            s = str(value)
            return s[: col.max_length] if col.max_length else s
    except Exception:
        return value
    return value


def _apply_numeric_op(current: Any, op: str, limit: float, col: ColumnMeta) -> Any:
    k = _kind(col.type_name)
    try:
        num = float(current)
    except (TypeError, ValueError):
        num = None

    if op == ">=":
        target = limit if num is None or num < limit else num
    elif op == ">":
        target = limit + (1 if k == "int" else 0.01) if num is None or num <= limit else num
    elif op == "<=":
        target = limit if num is None or num > limit else num
    elif op == "<":
        target = limit - (1 if k == "int" else 0.01) if num is None or num >= limit else num
    elif op in {"=", "=="}:
        target = limit
    elif op in {"!=", "<>"}:
        target = limit + (1 if k == "int" else 0.01) if num is None or num == limit else num
    else:
        return current
    return _typed_number(float(target), k, epsilon=0.0 if op in {"=", "==", ">=", "<="} else 0.01)


def _extract_in_values(raw: str) -> list[str]:
    return [x.strip().strip("'\"") for x in raw.split(",") if x.strip()]


def _normalize_check_expression(expr: str) -> str:
    expr = expr.strip()
    match = re.fullmatch(r"CHECK\s*\((.*)\)", expr, flags=re.IGNORECASE | re.DOTALL)
    if match:
        expr = match.group(1).strip()
    return expr


def _is_wrapped_expr(expr: str) -> bool:
    if not (expr.startswith("(") and expr.endswith(")")):
        return False
    depth = 0
    for i, ch in enumerate(expr):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0 and i != len(expr) - 1:
                return False
    return depth == 0


def _split_compound_check(expr: str) -> tuple[str, list[str]]:
    expr = _normalize_check_expression(expr)
    while _is_wrapped_expr(expr):
        expr = expr[1:-1].strip()

    placeholder = "__DATAGEN_BETWEEN_AND__"
    masked = re.sub(
        r"\bBETWEEN\s+(-?\d+(?:\.\d+)?)\s+AND\s+(-?\d+(?:\.\d+)?)",
        lambda m: f"BETWEEN {m.group(1)} {placeholder} {m.group(2)}",
        expr,
        flags=re.IGNORECASE,
    )

    def _split_top_level(keyword: str) -> list[str]:
        parts: list[str] = []
        depth = 0
        start = 0
        i = 0
        in_single = False
        in_double = False
        while i < len(masked):
            ch = masked[i]
            if ch == "'" and not in_double:
                if in_single and i + 1 < len(masked) and masked[i + 1] == "'":
                    i += 2
                    continue
                in_single = not in_single
                i += 1
                continue
            if ch == '"' and not in_single:
                if in_double and i + 1 < len(masked) and masked[i + 1] == '"':
                    i += 2
                    continue
                in_double = not in_double
                i += 1
                continue
            if in_single or in_double:
                i += 1
                continue
            if ch == "(":
                depth += 1
                i += 1
                continue
            if ch == ")":
                depth = max(depth - 1, 0)
                i += 1
                continue
            if depth == 0 and masked[i : i + len(keyword)].upper() == keyword:
                before = masked[i - 1] if i > 0 else " "
                after_idx = i + len(keyword)
                after = masked[after_idx] if after_idx < len(masked) else " "
                if not (before.isalnum() or before == "_") and not (after.isalnum() or after == "_"):
                    parts.append(masked[start:i].strip().replace(placeholder, "AND"))
                    start = after_idx
                    i = after_idx
                    continue
            i += 1
        if not parts:
            return []
        parts.append(masked[start:].strip().replace(placeholder, "AND"))
        return [part for part in parts if part]

    or_parts = _split_top_level("OR")
    if len(or_parts) > 1:
        cleaned = []
        for part in or_parts:
            while _is_wrapped_expr(part):
                part = part[1:-1].strip()
            cleaned.append(part)
        return ("or", cleaned)

    and_parts = _split_top_level("AND")
    if len(and_parts) > 1:
        cleaned = []
        for part in and_parts:
            while _is_wrapped_expr(part):
                part = part[1:-1].strip()
            cleaned.append(part)
        return ("and", cleaned)

    return ("single", [expr.replace(placeholder, "AND")])


def _regex_to_sample(pattern: str) -> str | None:
    # Practical limited support for common forms used in checks.
    p = pattern.strip().strip("'\"")
    if p in {"^[A-Za-z]+$", "^[a-zA-Z]+$"}:
        return "SampleText"
    if p in {"^[0-9]+$", "^\\d+$"}:
        return str(random.randint(1000, 9999))
    if p in {"^[A-Z]{2}$"}:
        return "KR"
    if p == "^[A-Z]{3}$":
        return "SEO"
    return None


def _enforce_simple_check(row: dict[str, Any], col_by_name: dict[str, ColumnMeta], check: CheckConstraintMeta) -> None:
    mode, parts = _split_compound_check(check.expression)
    if mode == "and":
        for part in parts:
            _enforce_simple_check(row, col_by_name, CheckConstraintMeta(expression=part))
        return
    if mode == "or":
        _enforce_simple_check(row, col_by_name, CheckConstraintMeta(expression=random.choice(parts)))
        return
    expr = parts[0]

    # col BETWEEN a AND b
    m_between = re.search(r"(\w+)\s+BETWEEN\s+(-?\d+(?:\.\d+)?)\s+AND\s+(-?\d+(?:\.\d+)?)", expr, flags=re.IGNORECASE)
    if m_between:
        col_name, a_raw, b_raw = m_between.groups()
        col = col_by_name.get(col_name)
        if not col:
            return
        lo, hi = sorted((float(a_raw), float(b_raw)))
        current = row.get(col_name)
        if current is None:
            row[col_name] = _typed_number(lo, _kind(col.type_name), epsilon=0.0)
            return
        else:
            try:
                val = float(current)
            except (TypeError, ValueError):
                val = lo
        if val < lo:
            row[col_name] = _typed_number(lo, _kind(col.type_name), epsilon=0.0)
        elif val > hi:
            row[col_name] = _typed_number(hi, _kind(col.type_name), epsilon=0.0)
        return

    # col IN (...)
    m_in = re.search(r"(\w+)\s+IN\s*\(([^\)]+)\)", expr, flags=re.IGNORECASE)
    if m_in:
        col_name, raw = m_in.groups()
        col = col_by_name.get(col_name)
        if not col:
            return
        values = _extract_in_values(raw)
        if values:
            row[col_name] = _coerce_to_col_type(random.choice(values), col)
        return

    # regex-like checks: col ~ 'pattern' OR REGEXP_LIKE(col, 'pattern')
    m_regex = re.search(r"(\w+)\s*~\s*'([^']+)'", expr, flags=re.IGNORECASE)
    if not m_regex:
        m_regex = re.search(r"REGEXP_LIKE\s*\(\s*(\w+)\s*,\s*'([^']+)'\s*\)", expr, flags=re.IGNORECASE)
    if m_regex:
        col_name, pattern = m_regex.groups()
        col = col_by_name.get(col_name)
        if not col:
            return
        sample = _regex_to_sample(pattern)
        if sample is not None:
            row[col_name] = _coerce_to_col_type(sample, col)
        return

    # Generic binary comparisons against numeric literal
    m = re.search(r"(\w+)\s*(>=|<=|<>|!=|>|<|=|==)\s*(-?\d+(?:\.\d+)?)", expr)
    if m:
        col_name, op, raw = m.groups()
        col = col_by_name.get(col_name)
        if not col:
            return
        limit = float(raw)
        row[col_name] = _apply_numeric_op(row.get(col_name), op, limit, col)


def _make_unique_candidate(col: ColumnMeta, i: int, attempt: int) -> Any:
    k = _kind(col.type_name)
    base = i + 1 + attempt
    if k == "int":
        return base
    if k == "float":
        return float(base)
    if k == "uuid":
        return str(fake.uuid4())
    if k == "str":
        structured = _structured_string_value(col, unique_token=base)
        if structured is not None:
            return structured
        raw = f"{col.name}_{base}_{random.randint(1, 9999)}"
        return raw[: col.max_length] if col.max_length else raw
    return _default_value(col)


def _generate_table_rows_python(
    table: TableMeta,
    rows: int,
    dist_overrides: dict[str, DistSpec],
    generated: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    col_by_name = {c.name: c for c in table.columns}
    unique_columns = {c.name for c in table.columns if c.unique or c.primary_key}
    seen_unique: dict[str, set[Any]] = {c: set() for c in unique_columns}

    for i in range(rows):
        row: dict[str, Any] = {}
        make_edge = i % 20 == 0

        for col in table.columns:
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

        for col in table.columns:
            if not col.primary_key or _kind(col.type_name) != "int":
                continue

            value = row.get(col.name)
            if value is None or (make_edge and value == 0):
                row[col.name] = i + 1

        for ck in table.check_constraints:
            _enforce_simple_check(row, col_by_name, ck)

        for col in table.columns:
            if col.name not in unique_columns:
                continue
            value = row.get(col.name)
            if value is None:
                continue
            attempts = 0
            while value in seen_unique[col.name] and attempts < 10:
                value = _make_unique_candidate(col, i, attempts + 1)
                attempts += 1
            row[col.name] = value
            seen_unique[col.name].add(value)

        out.append(row)

    return out


def _generate_table_rows_polars(
    table: TableMeta,
    rows: int,
    dist_overrides: dict[str, DistSpec],
    generated: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Best-effort vectorized generation path for simple tables.

    Fallback policy: if table has check/unique constraints, use python engine.
    """
    has_strict_constraints = bool(table.check_constraints or any(c.unique or c.primary_key for c in table.columns))
    if has_strict_constraints:
        return _generate_table_rows_python(table, rows, dist_overrides, generated)

    try:
        import polars as pl
    except Exception:
        return _generate_table_rows_python(table, rows, dist_overrides, generated)

    data_cols: dict[str, list[Any]] = {}
    for col in table.columns:
        fk = next((x for x in table.foreign_keys if x.column == col.name), None)
        if fk and generated.get(fk.ref_table):
            parent_values = [r.get(fk.ref_column) for r in generated[fk.ref_table]]
            if parent_values:
                data_cols[col.name] = random.choices(parent_values, k=rows)
                continue

        dist = dist_overrides.get(f"{table.name}.{col.name}") or dist_overrides.get(col.name)
        if dist is not None:
            data_cols[col.name] = [sample_with_dist(dist) for _ in range(rows)]
            continue

        if col.nullable:
            vals = [(_default_value(col) if random.random() >= 0.05 else None) for _ in range(rows)]
        else:
            vals = [_default_value(col) for _ in range(rows)]
        data_cols[col.name] = vals

    df = pl.DataFrame(data_cols)
    return df.to_dicts()


def generate_all(
    tables: list[TableMeta],
    ordered_names: list[str],
    rows: int,
    dist_overrides: dict[str, DistSpec],
    table_rows: dict[str, int] | None = None,
    engine: str = "python",
) -> dict[str, list[dict[str, Any]]]:
    by_name = {t.name: t for t in tables}
    generated: dict[str, list[dict[str, Any]]] = {}
    table_rows = table_rows or {}
    for name in ordered_names:
        row_count = int(table_rows.get(name, rows))
        if engine == "polars":
            generated[name] = _generate_table_rows_polars(by_name[name], row_count, dist_overrides, generated)
        else:
            generated[name] = _generate_table_rows_python(by_name[name], row_count, dist_overrides, generated)
    return generated
