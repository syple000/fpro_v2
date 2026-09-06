"""目标权重、订单规划和事前检查。"""

from __future__ import annotations

import math
from collections.abc import Mapping

from backtest.domain import AccountSnapshot, OrderRequest, Side

LOT_SIZE = 100


def validate_target_weights(weights: Mapping[str, float]) -> dict[str, float]:
    """校验完整目标组合，并按证券代码排序以保证确定性。"""
    result: dict[str, float] = {}
    for symbol, weight in weights.items():
        if not symbol:
            raise ValueError("目标权重的证券代码不能为空")
        if isinstance(weight, bool) or not isinstance(weight, (int, float)):
            raise ValueError(f"{symbol} 的目标权重必须是数字")
        if not math.isfinite(weight) or not 0 <= weight <= 1:
            raise ValueError(f"{symbol} 的目标权重必须位于 [0, 1]")
        result[symbol] = float(weight)
    if sum(result.values()) > 1 + 1e-9:
        raise ValueError("目标权重之和不能超过 1")
    return dict(sorted(result.items()))


def create_orders(
    weights: Mapping[str, float],
    account: AccountSnapshot,
    prices: Mapping[str, float],
) -> tuple[OrderRequest, ...]:
    """把完整目标权重与当前持仓之差直接转换成订单。"""
    targets = validate_target_weights(weights)
    holdings = {holding.symbol: holding for holding in account.holdings}
    result: list[OrderRequest] = []
    for symbol in sorted(set(targets) | set(holdings)):
        current = holdings[symbol].quantity if symbol in holdings else 0
        weight = targets.get(symbol, 0.0)
        price = prices.get(symbol)
        if weight == 0:
            target = 0
        elif price is None or not math.isfinite(price) or price <= 0:
            # 没有可靠价格时维持原持仓，不凭空交易。
            target = current
        else:
            target = math.floor(account.total_equity * weight / price / LOT_SIZE) * LOT_SIZE

        difference = target - current
        if difference == 0:
            continue
        side = Side.BUY if difference > 0 else Side.SELL
        quantity = abs(difference)
        # 买入和非清仓卖出使用整手；清仓允许卖出零股。
        if side is Side.BUY or target != 0:
            quantity = quantity // LOT_SIZE * LOT_SIZE
        if quantity > 0:
            result.append(OrderRequest(symbol, side, quantity, weight))
    return tuple(result)
