from backtest.domain import AccountSnapshot, Holding, Side
from backtest.orders import create_orders, validate_target_weights


def test_complete_target_weights_create_sell_before_new_buy() -> None:
    account = AccountSnapshot(
        cash=5_000,
        dividend_receivable=0,
        market_value=5_000,
        total_equity=10_000,
        holdings=(Holding("OLD", 500, 500, 5_000),),
    )

    requests = create_orders({"NEW": 0.5}, account, {"OLD": 10.0, "NEW": 20.0})

    assert [(item.symbol, item.side, item.quantity) for item in requests] == [
        ("NEW", Side.BUY, 200),
        ("OLD", Side.SELL, 500),
    ]
def test_invalid_total_weight_is_rejected() -> None:
    try:
        validate_target_weights({"A": 0.6, "B": 0.5})
    except ValueError as exc:
        assert "不能超过 1" in str(exc)
    else:
        raise AssertionError("应拒绝总权重超过 1")
