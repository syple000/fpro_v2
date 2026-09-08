"""从回测结果计算最常用的收益、风险和交易统计。"""

from __future__ import annotations

import math
import statistics
from collections import Counter
from typing import Any

from backtest.config import BacktestConfig
from backtest.domain import BacktestResult, OrderStatus

ANNUAL_SESSIONS = 252
_UNSUCCESSFUL = {
    OrderStatus.NOT_FILLED,
    OrderStatus.CANCELED,
    OrderStatus.EXPIRED,
}


def calculate_metrics(
    result: BacktestResult,
    config: BacktestConfig,
) -> dict[str, Any]:
    """计算收益、波动、回撤、换手、费用和订单统计。"""
    if not result.equity:
        raise ValueError("没有净值记录")

    values = [config.initial_cash, *(row.total_equity for row in result.equity)]
    returns = [row.daily_return for row in result.equity if row.daily_return is not None]
    final_equity = values[-1]
    total_return = final_equity / config.initial_cash - 1
    annualized_return = None
    if final_equity > 0:
        annualized_return = (1 + total_return) ** (
            ANNUAL_SESSIONS / len(result.sessions)
        ) - 1

    annualized_volatility = None
    sharpe = None
    if len(returns) >= 2:
        daily_standard_deviation = statistics.stdev(returns)
        annualized_volatility = daily_standard_deviation * math.sqrt(ANNUAL_SESSIONS)
        if daily_standard_deviation > 0:
            sharpe = (
                statistics.fmean(returns)
                / daily_standard_deviation
                * math.sqrt(ANNUAL_SESSIONS)
            )

    terminal_updates = [
        update
        for update in result.order_updates
        if update.status is not OrderStatus.SUBMITTED
    ]
    unsuccessful = [
        update for update in terminal_updates if update.status in _UNSUCCESSFUL
    ]
    blocked_reasons = Counter(update.reason.value for update in unsuccessful)
    average_equity = statistics.fmean(values)
    traded_notional = sum(fill.notional for fill in result.fills)
    metrics = {
        "start_session": result.sessions[0].isoformat(),
        "end_session": result.sessions[-1].isoformat(),
        "session_count": len(result.sessions),
        "initial_equity": config.initial_cash,
        "final_equity": final_equity,
        "total_return": total_return,
        "annualized_return": annualized_return,
        "annualized_volatility": annualized_volatility,
        "sharpe": sharpe,
        "max_drawdown": _max_drawdown(values),
        "turnover": traded_notional / average_equity if average_equity else None,
        "order_count": len(result.orders),
        "trade_count": len(result.fills),
        "unsuccessful_order_count": len(unsuccessful),
        "blocked_order_reasons": dict(sorted(blocked_reasons.items())),
        "total_fees": sum(fill.total_fee for fill in result.fills),
        "slippage_cost": sum(fill.slippage_cost for fill in result.fills),
    }
    coverage = result.market_data_coverage
    if coverage is not None:
        metrics["market_data_coverage"] = {
            "expected_events": coverage.expected_events,
            "events_with_bars": coverage.events_with_bars,
            "bar_count": coverage.bar_count,
            "symbols_with_bars": list(coverage.symbols_with_bars),
            "sessions_without_bars": [day.isoformat() for day in coverage.sessions_without_bars],
            "requested_symbols_without_bars": list(coverage.requested_symbols_without_bars),
        }
    return metrics


def _max_drawdown(values: list[float]) -> float:
    """计算净值从历史峰值到后续低点的最大跌幅绝对值。"""
    peak = values[0]
    worst = 0.0
    for value in values:
        peak = max(peak, value)
        worst = min(worst, value / peak - 1)
    return abs(worst)
