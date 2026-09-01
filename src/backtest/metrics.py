"""不掩盖无效分母的回测指标。"""

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from datetime import date
from typing import Any

from backtest.config import BacktestConfig
from backtest.engine import BacktestResult
from backtest.types import OrderSide, OrderStatus


def _safe_divide(numerator: float, denominator: float) -> float | None:
    if denominator == 0 or not math.isfinite(denominator):
        return None
    value = numerator / denominator
    return value if math.isfinite(value) else None


def _drawdown(equity: list[tuple[date, float]]) -> dict[str, Any]:
    peak_value = -math.inf
    peak_date: date | None = None
    worst = 0.0
    worst_peak: date | None = None
    trough: date | None = None
    peak_to_recover = 0.0
    for session, value in equity:
        if value > peak_value:
            peak_value = value
            peak_date = session
        drawdown = value / peak_value - 1.0 if peak_value > 0 else 0.0
        if drawdown < worst:
            worst = drawdown
            worst_peak = peak_date
            trough = session
            peak_to_recover = peak_value
    recovery: date | None = None
    if trough is not None:
        for session, value in equity:
            if session > trough and value >= peak_to_recover:
                recovery = session
                break
    duration = None
    if worst_peak is not None:
        end = recovery or equity[-1][0]
        duration = (end - worst_peak).days
    return {
        "max_drawdown": worst,
        "drawdown_start": worst_peak.isoformat() if worst_peak else None,
        "drawdown_trough": trough.isoformat() if trough else None,
        "drawdown_recovery": recovery.isoformat() if recovery else None,
        "drawdown_duration_calendar_days": duration,
    }


def _period_returns(result: BacktestResult) -> tuple[dict[str, float], dict[str, float]]:
    yearly_values: dict[int, list[float]] = defaultdict(list)
    monthly_values: dict[str, list[float]] = defaultdict(list)
    for row in result.equity:
        if row.daily_return is None:
            continue
        yearly_values[row.session.year].append(row.daily_return)
        monthly_values[row.session.strftime("%Y-%m")].append(row.daily_return)

    def compound(values: list[float]) -> float:
        total = 1.0
        for value in values:
            total *= 1.0 + value
        return total - 1.0

    yearly = {str(year): compound(values) for year, values in sorted(yearly_values.items())}
    monthly = {month: compound(values) for month, values in sorted(monthly_values.items())}
    return yearly, monthly


def _average_holding_days(result: BacktestResult) -> float | None:
    quantities: dict[str, int] = defaultdict(int)
    opened: dict[str, date] = {}
    durations: list[int] = []
    for fill in result.fills:
        if fill.side is OrderSide.BUY:
            if quantities[fill.symbol] == 0:
                opened[fill.symbol] = fill.filled_at.date()
            quantities[fill.symbol] += fill.quantity
        else:
            quantities[fill.symbol] = max(quantities[fill.symbol] - fill.quantity, 0)
            if quantities[fill.symbol] == 0 and fill.symbol in opened:
                durations.append((fill.filled_at.date() - opened.pop(fill.symbol)).days)
    return statistics.fmean(durations) if durations else None


def calculate_metrics(result: BacktestResult, config: BacktestConfig) -> dict[str, Any]:
    if not result.equity:
        raise ValueError("没有净值记录")
    equity = [(row.session, row.total_equity) for row in result.equity]
    returns = [row.daily_return for row in result.equity if row.daily_return is not None]
    initial = config.initial_cash
    final = equity[-1][1]
    total_return = final / initial - 1.0
    session_count = len(result.equity)
    annualized_return = (
        (final / initial) ** (config.annualization_sessions / session_count) - 1.0
        if final > 0 and session_count > 0
        else None
    )
    volatility = None
    sharpe = None
    sortino = None
    if len(returns) >= 2:
        daily_std = statistics.stdev(returns)
        volatility = daily_std * math.sqrt(config.annualization_sessions)
        daily_rf = (1 + config.risk_free_rate) ** (1 / config.annualization_sessions) - 1
        excess = [value - daily_rf for value in returns]
        if daily_std > 0:
            sharpe = statistics.fmean(excess) / daily_std * math.sqrt(
                config.annualization_sessions
            )
        downside = math.sqrt(statistics.fmean(min(value, 0.0) ** 2 for value in excess))
        if downside > 0:
            sortino = statistics.fmean(excess) / downside * math.sqrt(
                config.annualization_sessions
            )
    drawdown = _drawdown(equity)
    max_drawdown = abs(drawdown["max_drawdown"])
    calmar = (
        _safe_divide(annualized_return, max_drawdown)
        if annualized_return is not None
        else None
    )
    positives = [value for value in returns if value > 0]
    negatives = [value for value in returns if value < 0]
    nonzero_count = len(positives) + len(negatives)
    win_rate = len(positives) / nonzero_count if nonzero_count else None
    profit_loss_ratio = (
        statistics.fmean(positives) / abs(statistics.fmean(negatives))
        if positives and negatives
        else None
    )
    average_equity = statistics.fmean(row.total_equity for row in result.equity)
    traded_notional = sum(fill.notional for fill in result.fills)
    turnover = _safe_divide(traded_notional, average_equity)
    total_fees = sum(fill.total_fee for fill in result.fills)
    commission = sum(fill.commission for fill in result.fills)
    stamp_tax = sum(fill.stamp_tax for fill in result.fills)
    transfer_fee = sum(fill.transfer_fee for fill in result.fills)
    slippage = sum(fill.slippage_cost for fill in result.fills)
    rejected = [
        order
        for order in result.orders
        if order.status in {OrderStatus.REJECTED, OrderStatus.EXPIRED}
    ]
    reason_counts: dict[str, int] = defaultdict(int)
    for order in rejected:
        reason_counts[order.reason.value] += 1
    yearly, monthly = _period_returns(result)
    return {
        "start_session": result.sessions[0].isoformat(),
        "end_session": result.sessions[-1].isoformat(),
        "session_count": session_count,
        "initial_equity": initial,
        "final_equity": final,
        "total_return": total_return,
        "annualized_return": annualized_return,
        "annualized_volatility": volatility,
        "sharpe": sharpe,
        "sortino": sortino,
        **drawdown,
        "calmar": calmar,
        "daily_win_rate": win_rate,
        "daily_profit_loss_ratio": profit_loss_ratio,
        "average_holding_calendar_days": _average_holding_days(result),
        "turnover": turnover,
        "trade_count": len(result.fills),
        "order_count": len(result.orders),
        "rejected_or_expired_order_count": len(rejected),
        "blocked_order_reasons": dict(sorted(reason_counts.items())),
        "total_fees": total_fees,
        "commission": commission,
        "stamp_tax": stamp_tax,
        "transfer_fee": transfer_fee,
        "slippage_cost": slippage,
        "average_holding_count": statistics.fmean(row.holding_count for row in result.equity),
        "average_capital_utilization": statistics.fmean(
            row.market_value / row.total_equity if row.total_equity else 0.0
            for row in result.equity
        ),
        "stale_position_day_ratio": (
            sum(row.stale_position_count for row in result.equity)
            / max(sum(row.holding_count for row in result.equity), 1)
        ),
        "corporate_action_event_count": len(result.corporate_actions),
        "benchmark": config.benchmark,
        "benchmark_return": None,
        "excess_return": None,
        "annualization_sessions": config.annualization_sessions,
        "risk_free_rate": config.risk_free_rate,
        "yearly_returns": yearly,
        "monthly_returns": monthly,
    }
