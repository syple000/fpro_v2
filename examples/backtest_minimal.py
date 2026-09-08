"""用四根虚构日线理解“策略 → 委托 → 后续成交 → 账户”。

在仓库根目录运行：.venv/bin/python examples/backtest_minimal.py
只在临时目录生成数据，不读取现有 dataset，也不访问网络。
建议先读 BuyThenSell 和 run_demo；下面的数据准备不属于策略逻辑。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from math import isclose
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa

from backtest import BacktestConfig, BacktestResult, Schedule, Side, Strategy, StrategyContext
from backtest.domain import AccountSnapshot, Order
from backtest.runner import default_source_config, run_backtest
from market_data import DataCatalog, DataReader
from tushare_data.schemas import TABLE_SCHEMAS
from tushare_data.storage import TushareDataStore

SYMBOL = "600000.SH"  # 只借用合法代码；下面的行情都是虚构数据。
SESSIONS = tuple(date(2026, 1, day) for day in (5, 6, 7, 8))


class BuyThenSell(Strategy):
    """第 1 日提交买单，第 3 日确认可卖后提交卖单。"""

    schedule = Schedule("day")

    def __init__(self) -> None:
        self.steps: list[Step] = []

    def on_event(self, context: StrategyContext) -> None:
        holding = context.account.holding(SYMBOL)
        order = None
        signal = "等待；不提交新订单"

        if context.event.session == SESSIONS[0]:
            signal = "买入 100 股"
            order = context.order(SYMBOL, Side.BUY, 100)
        elif context.event.session == SESSIONS[2]:
            assert holding is not None and holding.sellable_quantity == 100
            signal = "已满足 T+1，卖出 100 股"
            order = context.order(SYMBOL, Side.SELL, 100)

        # 只保存不可变快照，供运行结束后打印；不参与交易决策。
        self.steps.append(Step(context.event.at, context.account, signal, order))
        # 返回 None：本次只使用上面的数量订单，不再触发目标权重调仓。


def run_demo() -> BacktestResult:
    """准备四根日线，调用正式运行入口，打印并检查结果。"""
    strategy = BuyThenSell()
    config = BacktestConfig(
        start_date=SESSIONS[0],
        end_date=SESSIONS[-1],
        frequency="1d",  # 撮合周期；策略调用时间由上面的 Schedule 决定。
        symbols=(SYMBOL,),
        initial_cash=10_000.0,
    )
    # 使用默认滑点、佣金、印花税、过户费和成交量限制。
    with TemporaryDirectory(prefix="backtest_minimal_") as directory:
        root = Path(directory)
        prepare_data(root / "tushare")
        with DataCatalog(tushare_root=root / "tushare", qmt_root=root / "qmt") as catalog:
            reader = DataReader(catalog, sources=default_source_config())
            completed = run_backtest(reader=reader, config=config, strategy=strategy)

    check_result(completed.result, strategy.steps)
    print_result(completed.result, strategy.steps)
    return completed.result


@dataclass(frozen=True)
class Step:
    """一次策略调用时看到的账户，以及本次产生的信号和委托。"""

    at: datetime
    account: AccountSnapshot
    signal: str
    order: Order | None


def print_result(result: BacktestResult, steps: list[Step]) -> None:
    """把本示例的每日回放确认时间和成交记录时间分开显示。"""
    print("四根虚构日线；初始现金 10000 元；时区 Asia/Shanghai")
    print("模拟时钟到 16:05 才收到完整日线并确认成交；filled_at 记录该日开盘 09:30。")
    print("策略在本次撮合后调用；新委托等待后续 Bar，提交时账户不变。")

    for step in steps:
        print(f"\n模拟时钟：{step.at:%Y-%m-%d %H:%M %z}")
        # 本示例每天一根 Bar、收盘调用一次；当天成交都在此次调用前确认。
        for fill in result.fills:
            if fill.filled_at.date() != step.at.date():
                continue
            print(
                f"  本次回放确认：{fill.order_id} {fill.side} {fill.quantity} 股；"
                f"成交记录 filled_at={fill.filled_at:%Y-%m-%d %H:%M %z}"
            )
            print(
                f"    开盘价={fill.market_price:.4f}，含滑点成交价={fill.execution_price:.4f}；"
                f"成交额={fill.notional:.6f}"
            )
            print(
                f"    佣金={fill.commission:.6f}，印花税={fill.stamp_tax:.6f}，"
                f"过户费={fill.transfer_fee:.6f}"
            )
        holding = step.account.holding(SYMBOL)
        quantity = holding.quantity if holding else 0
        sellable = holding.sellable_quantity if holding else 0
        print(
            f"  策略看到的账户：现金={step.account.cash:.6f}，"
            f"持仓={quantity} 股，可卖={sellable} 股，"
            f"总权益={step.account.total_equity:.6f}"
        )
        print(f"  信号：{step.signal}")
        if step.order is not None:
            print(
                f"  新委托：{step.order.order_id} {step.order.side} "
                f"{step.order.quantity} 股，submitted_at={step.order.submitted_at:%H:%M}；"
                "等待后续 Bar"
            )

    final = result.equity[-1]
    print(f"\n自检通过：两笔成交，最终空仓，现金={final.cash:.6f} 元。")


def check_result(result: BacktestResult, steps: list[Step]) -> None:
    """核对成交先后、T+1 和扣除默认费用后的现金。"""
    assert result.sessions == SESSIONS
    assert len(result.orders) == len(result.fills) == 2
    buy, sell = result.fills
    assert (buy.side, buy.quantity, buy.filled_at.date()) == (Side.BUY, 100, SESSIONS[1])
    assert (sell.side, sell.quantity, sell.filled_at.date()) == (Side.SELL, 100, SESSIONS[3])
    for order, fill in zip(result.orders, result.fills, strict=True):
        assert order.submitted_at < fill.filled_at
    holdings = [step.account.holding(SYMBOL) for step in steps]
    quantities = [(item.quantity, item.sellable_quantity) if item else (0, 0) for item in holdings]
    assert quantities == [(0, 0), (100, 0), (100, 100), (0, 0)]
    # 买入扣 1000.5 + 5.010005；卖出收回 1099.45 - 5.5607195。
    assert isclose(result.equity[-1].cash, 10_088.3792755, rel_tol=0, abs_tol=1e-8)


def prepare_data(root: Path) -> None:
    """仅为演示生成正式 Schema 的本地数据；无需 API 密钥或已有数据库。"""
    store = TushareDataStore(root)

    def write(dataset: str, rows: list[dict[str, object]]) -> None:
        store.write(dataset, pa.Table.from_pylist(rows, schema=TABLE_SCHEMAS[dataset]))

    write(
        "stock_basic",
        [{"ts_code": SYMBOL, "list_date": date(2000, 1, 1), "exchange": "SSE", "curr_type": "CNY"}],
    )
    write(
        "trade_cal",
        [{"exchange": "SSE", "cal_date": day, "is_open": 1} for day in SESSIONS],
    )
    prices = [(10.0, 10.0), (10.0, 10.2), (10.4, 10.6), (11.0, 11.0)]
    daily_rows: list[dict[str, object]] = []
    limit_rows: list[dict[str, object]] = []
    previous_close = 10.0
    for day, (open_price, close) in zip(SESSIONS, prices, strict=True):
        daily_rows.append(
            {
                "ts_code": SYMBOL,
                "trade_date": day,
                "open": open_price,
                "high": max(open_price, close),
                "low": min(open_price, close),
                "close": close,
                "pre_close": previous_close,
                "vol": 1000.0,  # 原始单位“手”：读取后是 100000 股，默认容量足够。
                "amount": close * 100.0,
            }
        )
        limit_rows.append(
            {
                "ts_code": SYMBOL,
                "trade_date": day,
                "pre_close": previous_close,
                "up_limit": round(previous_close * 1.1, 2),
                "down_limit": round(previous_close * 0.9, 2),
            }
        )
        previous_close = close
    write("daily", daily_rows)
    write("stk_limit", limit_rows)
    # 此演示没有停牌、分红或送股事件；未写入的表由 DataCatalog 提供空表。


if __name__ == "__main__":
    run_demo()
