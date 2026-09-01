"""入门回测最常用的少量指标。"""

from __future__ import annotations

import math
import statistics
from collections import Counter
from typing import Any

from backtest.config import BacktestConfig
from backtest.engine import BacktestResult
from backtest.types import OrderStatus

ANNUAL_SESSIONS = 252


def _max_drawdown(values: list[float]) -> float:
    peak = values[0]
    worst = 0.0
    for value in values:
        peak = max(peak, value)
        worst = min(worst, value / peak - 1.0)
    return abs(worst)


def calculate_metrics(result: BacktestResult, config: BacktestConfig) -> dict[str, Any]:
    """计算收益、风险、换手和费用；无意义的比率返回 None。"""

    if not result.equity:
        raise ValueError("没有净值记录")
    values = [row.total_equity for row in result.equity]
    returns = [row.daily_return for row in result.equity if row.daily_return is not None]
    total_return = values[-1] / config.initial_cash - 1.0
    annualized_return = (
        (1 + total_return) ** (ANNUAL_SESSIONS / len(values)) - 1.0 if values[-1] > 0 else None
    )
    volatility = None
    sharpe = None
    if len(returns) >= 2:
        daily_std = statistics.stdev(returns)
        volatility = daily_std * math.sqrt(ANNUAL_SESSIONS)
        if daily_std > 0:
            sharpe = statistics.fmean(returns) / daily_std * math.sqrt(ANNUAL_SESSIONS)

    average_equity = statistics.fmean(values)
    traded_notional = sum(fill.notional for fill in result.fills)
    unfilled = [order for order in result.orders if order.status is OrderStatus.NOT_FILLED]
    blocked_reasons = Counter(order.reason.value for order in unfilled)
    return {
        "start_session": result.sessions[0].isoformat(),
        "end_session": result.sessions[-1].isoformat(),
        "session_count": len(result.sessions),
        "initial_equity": config.initial_cash,
        "final_equity": values[-1],
        "total_return": total_return,
        "annualized_return": annualized_return,
        "annualized_volatility": volatility,
        "sharpe": sharpe,
        "max_drawdown": _max_drawdown(values),
        "turnover": traded_notional / average_equity if average_equity else None,
        "order_count": len(result.orders),
        "trade_count": len(result.fills),
        "unfilled_order_count": len(unfilled),
        "blocked_order_reasons": dict(sorted(blocked_reasons.items())),
        "total_fees": sum(fill.total_fee for fill in result.fills),
        "slippage_cost": sum(fill.slippage_cost for fill in result.fills),
    }
