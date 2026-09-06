import pytest

from strategies import MomentumConfig, momentum_return


def test_momentum_uses_first_and_last_adjusted_close() -> None:
    assert momentum_return([1.0, 1.1, 1.2]) == pytest.approx(0.2)


def test_momentum_rejects_incomplete_or_invalid_window() -> None:
    assert momentum_return([1.0]) is None
    assert momentum_return([0.0, 1.0]) is None
    assert momentum_return([1.0, None]) is None


def test_momentum_config_rejects_non_positive_values() -> None:
    with pytest.raises(ValueError):
        MomentumConfig(lookback_sessions=0)
