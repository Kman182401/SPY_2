import math

from spy2.portfolio.models import safe_float


def test_safe_float_rejects_nan_and_inf():
    assert safe_float(float("nan")) is None
    assert safe_float(float("inf")) is None
    assert safe_float(float("-inf")) is None


def test_safe_float_accepts_finite_values():
    out = safe_float("1.25")
    assert out is not None
    assert math.isfinite(out)
    assert out == 1.25
