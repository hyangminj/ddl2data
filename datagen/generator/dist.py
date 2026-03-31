from __future__ import annotations

import random
import re
from typing import Any

from datagen.config import DistSpec


def parse_dist_arg(arg: str) -> tuple[str, DistSpec]:
    # col:normal,mean=10,std=2
    col, spec = arg.split(":", 1)
    parts = [p.strip() for p in spec.split(",") if p.strip()]
    kind = parts[0].lower()
    params: dict[str, Any] = {}
    for p in parts[1:]:
        if "=" in p:
            k, v = p.split("=", 1)
            params[k.strip().lower()] = _parse_value(v.strip())
    if kind == "weighted":
        params = _parse_weighted(spec[len("weighted"):].lstrip(","))
    return col.strip(), DistSpec(kind=kind, params=params)


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


def sample_with_dist(spec: DistSpec) -> Any:
    kind = spec.kind
    p = spec.params
    if kind == "normal":
        return random.gauss(float(p.get("mean", 0.0)), float(p.get("std", 1.0)))
    if kind == "poisson":
        lam = float(p.get("lambda", 1.0))
        # Knuth algorithm
        l = pow(2.718281828459045, -lam)
        k = 0
        prob = 1.0
        while prob > l:
            k += 1
            prob *= random.random()
        return k - 1
    if kind == "weighted":
        keys = list(p.keys())
        weights = list(p.values())
        return random.choices(keys, weights=weights, k=1)[0]
    return None
