import pytest

from datagen.config import DistSpec
from datagen.generator.dist import parse_dist_arg, sample_with_dist


def test_parse_and_sample_exponential():
    col, spec = parse_dist_arg("latency:exponential,rate=0.5")
    assert col == "latency"
    assert spec.kind == "exponential"
    value = sample_with_dist(spec)
    assert isinstance(value, float)
    assert value >= 0


def test_parse_and_sample_pareto():
    col, spec = parse_dist_arg("amount:pareto,alpha=1.3,xm=2")
    assert col == "amount"
    assert spec.kind == "pareto"
    value = sample_with_dist(spec)
    assert isinstance(value, float)
    assert value > 0


def test_parse_and_sample_zipf():
    col, spec = parse_dist_arg("rank:zipf,skew=1.8,n=50")
    assert col == "rank"
    assert spec.kind == "zipf"
    value = sample_with_dist(spec)
    assert isinstance(value, int)
    assert 1 <= value <= 50


def test_parse_and_sample_peak_time():
    col, spec = parse_dist_arg("created_at:peak,hours=9-11")
    assert col == "created_at"
    assert spec.kind == "peak"
    value = sample_with_dist(spec)
    assert isinstance(value, str)
    # ISO-ish timestamp string
    assert "T" in value


def test_weighted_still_works():
    spec = DistSpec(kind="weighted", params={"A": 0.7, "B": 0.3})
    value = sample_with_dist(spec)
    assert value in {"A", "B"}


def test_parse_peak_preserves_multiple_hour_ranges():
    _, spec = parse_dist_arg("created_at:peak,hours=9-11,18-20")
    assert spec.params["hours"] == "9-11,18-20"


def test_parse_dist_arg_rejects_missing_colon():
    with pytest.raises(ValueError, match="Invalid --dist token 'broken'"):
        parse_dist_arg("broken")


def test_parse_dist_arg_rejects_unknown_kind():
    with pytest.raises(ValueError, match="Unsupported distribution kind 'mystery'"):
        parse_dist_arg("age:mystery,mean=5")


def test_parse_dist_arg_rejects_empty_weighted_values():
    with pytest.raises(ValueError, match="requires at least one value=weight pair"):
        parse_dist_arg("tier:weighted")
