from __future__ import annotations

import math
import random
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from datagen.config import DistSpec

SUPPORTED_DISTRIBUTIONS = {
    "normal",
    "poisson",
    "weighted",
    "exponential",
    "pareto",
    "zipf",
    "peak",
}


def parse_dist_arg(arg: str) -> tuple[str, DistSpec]:
    token = arg.strip()
    if ":" not in token:
        raise ValueError(f"Invalid --dist token '{arg}'. Use column:kind,param=value")

    # col:normal,mean=10,std=2
    col, spec = token.split(":", 1)
    col = col.strip()
    spec = spec.strip()
    if not col or not spec:
        raise ValueError(f"Invalid --dist token '{arg}'. Use column:kind,param=value")

    parts = [p.strip() for p in spec.split(",") if p.strip()]
    if not parts:
        raise ValueError(f"Invalid --dist token '{arg}'. Use column:kind,param=value")

    kind = parts[0].lower()
    if kind not in SUPPORTED_DISTRIBUTIONS:
        supported = ", ".join(sorted(SUPPORTED_DISTRIBUTIONS))
        raise ValueError(f"Unsupported distribution kind '{kind}' in --dist token '{arg}'. Use one of: {supported}")

    params = _parse_params(kind, parts[1:], arg)

    return col, DistSpec(kind=kind, params=params)


def _parse_params(kind: str, param_parts: list[str], raw_arg: str) -> dict[str, Any]:
    if kind == "weighted":
        params = _parse_weighted(",".join(param_parts))
        if not params:
            raise ValueError(
                f"Weighted distribution in --dist token '{raw_arg}' requires at least one value=weight pair"
            )
        return params

    params: dict[str, Any] = {}
    last_key: str | None = None
    for part in param_parts:
        if "=" in part:
            k, v = part.split("=", 1)
            key = k.strip().lower()
            value = v.strip()
            if not key or not value:
                raise ValueError(f"Invalid parameter '{part}' in --dist token '{raw_arg}'. Use key=value")
            params[key] = _parse_value(value)
            last_key = key
            continue
        if kind == "peak" and last_key == "hours":
            params["hours"] = f"{params['hours']},{part}"
            continue
        raise ValueError(f"Invalid parameter '{part}' in --dist token '{raw_arg}'. Use key=value")
    return params


def _parse_value(v: str) -> Any:
    if v.endswith("%"):
        return float(v[:-1]) / 100.0
    for cast in (int, float):
        try:
            return cast(v)
        except ValueError:
            pass
    return v


def _parse_weighted(spec: str) -> dict[str, float]:
    # A=60%,B=40%
    out: dict[str, float] = {}
    for token in [x.strip() for x in spec.split(",") if x.strip()]:
        m = re.match(r"([^=]+)=(.+)", token)
        if not m:
            continue
        key = m.group(1).strip()
        raw = m.group(2).strip()
        val = _parse_value(raw)
        out[key] = float(val)
    total = sum(out.values()) or 1.0
    return {k: v / total for k, v in out.items()}


def _sample_poisson(lam: float) -> int:
    # Knuth algorithm
    l = math.exp(-lam)
    k = 0
    prob = 1.0
    while prob > l:
        k += 1
        prob *= random.random()
    return k - 1


def _sample_zipf(skew: float, n: int = 100) -> int:
    ranks = list(range(1, n + 1))
    weights = [1.0 / (r**skew) for r in ranks]
    return random.choices(ranks, weights=weights, k=1)[0]


def _sample_peak_time(hours_spec: str, date_from: datetime | None = None) -> str:
    # hours=9-11,18-20
    ranges: list[tuple[int, int]] = []
    for token in [x.strip() for x in hours_spec.split(",") if x.strip()]:
        if "-" not in token:
            continue
        a, b = token.split("-", 1)
        try:
            start = max(0, min(23, int(a)))
            end = max(0, min(23, int(b)))
            if start <= end:
                ranges.append((start, end))
        except ValueError:
            continue

    if not ranges:
        hour = random.randint(0, 23)
    else:
        span_weights = [(end - start + 1) for start, end in ranges]
        start, end = random.choices(ranges, weights=span_weights, k=1)[0]
        hour = random.randint(start, end)

    base = date_from or datetime.now(timezone.utc).replace(tzinfo=None)
    dt = base.replace(hour=hour, minute=random.randint(0, 59), second=random.randint(0, 59), microsecond=0)
    # small random jitter day-wise for variety
    dt += timedelta(days=random.randint(-7, 7))
    return dt.isoformat()


def sample_with_dist(spec: DistSpec) -> Any:
    kind = spec.kind
    p = spec.params

    if kind == "normal":
        return random.gauss(float(p.get("mean", 0.0)), float(p.get("std", 1.0)))

    if kind == "poisson":
        lam = float(p.get("lambda", 1.0))
        return _sample_poisson(lam)

    if kind == "weighted":
        keys = list(p.keys())
        weights = list(p.values())
        return random.choices(keys, weights=weights, k=1)[0]

    if kind == "exponential":
        rate = float(p.get("rate", p.get("lambda", 1.0)))
        if rate <= 0:
            rate = 1.0
        return random.expovariate(rate)

    if kind == "pareto":
        alpha = float(p.get("alpha", 1.5))
        if alpha <= 0:
            alpha = 1.5
        xm = float(p.get("xm", 1.0))
        return xm * (1 + random.paretovariate(alpha))

    if kind == "zipf":
        skew = float(p.get("skew", 1.5))
        if skew <= 0:
            skew = 1.5
        max_rank = int(p.get("n", 100))
        if max_rank < 2:
            max_rank = 100
        return _sample_zipf(skew=skew, n=max_rank)

    if kind == "peak":
        return _sample_peak_time(str(p.get("hours", "9-11,18-20")))

    return None
