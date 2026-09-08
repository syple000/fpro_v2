"""集中定义当前支持的 A 股竞价申报数量规则。"""

from dataclasses import dataclass
from datetime import date
from typing import Literal

from backtest.errors import DataError


@dataclass(frozen=True, slots=True)
class QuantityRule:
    minimum: int
    step: int
    maximum: int

    def round_down(self, quantity: int) -> int:
        """向下取合法申报量；不足最低数量时不下单。"""
        quantity = min(quantity, self.maximum)
        return quantity // self.step * self.step if quantity >= self.minimum else 0

    def valid(self, quantity: int, *, liquidating: bool = False) -> bool:
        return 0 < quantity <= self.maximum and (
            liquidating or quantity == self.round_down(quantity)
        )


def quantity_rule(
    symbol: str,
    trading_date: date,
    order_type: Literal["market", "limit"] = "market",
) -> QuantityRule:
    """按交易代码的市场/板块、日期和订单类型取得申报规则。

    引擎目前只有市价数量订单；limit 仅供规则查询，不表示已经支持限价撮合。
    """
    if order_type not in {"market", "limit"}:
        raise ValueError(f"不支持的申报类型: {order_type}")
    code, _, exchange = symbol.partition(".")
    if len(code) != 6 or not code.isdigit():
        raise DataError(f"无法确定证券的申报数量规则: {symbol}")
    if exchange == "SH" and code.startswith(("688", "689")):
        if trading_date < date(2019, 7, 22):
            raise DataError("科创板开市前没有适用的申报规则")
        return QuantityRule(200, 1, 50_000 if order_type == "market" else 100_000)
    if exchange == "BJ":
        if trading_date < date(2021, 11, 15):
            raise DataError("北交所开市前的股转交易规则尚不支持")
        return QuantityRule(100, 1, 1_000_000)
    if exchange == "SZ" and code.startswith(("300", "301")):
        maximum = 1_000_000
        if trading_date >= date(2020, 8, 24):
            maximum = 150_000 if order_type == "market" else 300_000
        return QuantityRule(100, 100, maximum)
    if (exchange == "SH" and code.startswith("60")) or (exchange == "SZ" and code.startswith("00")):
        return QuantityRule(100, 100, 1_000_000)
    raise DataError(f"不支持该证券的申报数量规则: {symbol}")
