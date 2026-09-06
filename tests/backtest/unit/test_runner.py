from backtest.runner import default_source_config
from models import ROUTE_SCHEMAS


def test_default_source_config_registers_every_logical_route() -> None:
    config = default_source_config()

    assert set(config.routes) == set(ROUTE_SCHEMAS)
    assert config.routes["market.intraday_bars"] == "qmt"
    assert config.routes["market.realtime_quotes"] == "qmt"
    assert all(
        source == "tushare"
        for route, source in config.routes.items()
        if route not in {"market.intraday_bars", "market.realtime_quotes"}
    )
