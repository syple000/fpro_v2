from __future__ import annotations

import pytest

from strategies import (
    MomentumConfig,
    momentum_return,
    select_momentum_targets,
    validate_target_weights,
)


def test_momentum_return_uses_only_two_endpoints() -> None:
    assert momentum_return(100.0, 125.0) == pytest.approx(0.25)
    assert momentum_return(None, 125.0) is None
    assert momentum_return(0.0, 125.0) is None


def test_select_momentum_targets_ranks_and_caps_weights() -> None:
    config = MomentumConfig(
        top_fraction=0.5,
        max_positions=3,
        gross_exposure=0.8,
        max_position_weight=0.3,
    )

    assert select_momentum_targets(
        {"D": -0.1, "B": 0.3, "A": 0.3, "C": None},
        config,
    ) == {"A": 0.3, "B": 0.3}


def test_positive_momentum_filter_can_return_cash() -> None:
    config = MomentumConfig(require_positive_momentum=True)

    assert select_momentum_targets({"A": 0.0, "B": -0.1}, config) == {}


def test_target_weights_are_validated_at_the_shared_boundary() -> None:
    assert validate_target_weights({"B": 0.4, "A": 0.5}) == {"A": 0.5, "B": 0.4}
    with pytest.raises(ValueError, match="权重之和"):
        validate_target_weights({"A": 0.6, "B": 0.5})
