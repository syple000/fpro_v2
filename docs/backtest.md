# 回测引擎

这个回测只有一条同步流水线，支持 `1m/5m/15m/30m/60m/1d`：

```text
交易日开始
  → T+1 解锁
  → 公司行动和退市处理

每根完整 Bar
  → DataView 读取 open / close
  → Broker 用 open 撮合上一根 Bar 后提交的订单
  → Portfolio 应用成交
  → Portfolio 用 close 估值
  → Strategy 读取 DataView，返回目标权重
  → 目标权重转换成订单，交给 Broker 等待下一根 Bar

当天最后一根 Bar
  → 登记公司行动权益
  → 记录账户净值
```

回测结束后，待处理订单标记为 `EXPIRED`，然后计算指标并按需写文件。

## 最重要的边界

`Clock` 只根据交易日历和频率生成时间，不读取行情。

每个时间点由 `reader.at(event.at)` 创建 PIT `DataView`。当前 Bar、历史窗口、复权、文件裁剪、
缓存和批量查询全部属于 `market_data`。回测层不维护 `BarFeed`、`HistoricalData` 或第二套缓存。

引擎仍把当前查询结果转换成一个只有 `symbol / interval_start / open / close` 的 `Bar`，因为
Broker 和 Portfolio 需要明确的业务字段。这个对象不读取数据，也不保存历史。

## 撮合时间

一分钟示例：

```text
09:31 收到 [09:30, 09:31] 的完整 Bar
  → 更早的订单按 09:30 open 撮合
  → 策略在 09:31 产生新订单

09:32 收到 [09:31, 09:32] 的完整 Bar
  → 09:31 的新订单按 09:31 open 撮合
  → 再执行 09:32 的策略
```

因此策略不会使用产生信号的同一根 Bar 成交。成交记录的 `filled_at` 是下一根 Bar 的
`interval_start`，但回测是在该 Bar 完整后才确认结果。这是 Bar 级回测，不是逐笔仿真。

## 文件职责

| 文件 | 内容 |
| --- | --- |
| `config.py` | 回测参数和路径参数 |
| `clock.py` | Event、交易时间线、单向时钟 |
| `domain.py` | Bar、订单、成交、账户快照、最终结果 |
| `strategy.py` | 只有一个 `on_bar` 的策略接口 |
| `orders.py` | 目标权重校验及订单数量计算 |
| `broker.py` | 待处理订单、撮合、费用、滑点、成交限制 |
| `portfolio.py` | 现金、持仓、T+1、估值、分红状态 |
| `corporate_actions.py` | 股权登记、除权、派息、红股上市 |
| `universe.py` | 可选的默认股票池过滤 |
| `engine.py` | 按上述顺序调用各模块 |
| `runner.py` | 注册全部数据路由并组装运行 |
| `metrics.py` / `output.py` | 指标计算和结果写出 |

保留 `Broker`、`Portfolio` 和 `CorporateActionProcessor` 是因为它们分别保存订单、账户和公司
行动状态。时间线、目标权重转换、结果收集等无状态逻辑都使用普通函数或列表，不再建立包装类。

## 策略接口

```python
from backtest.clock import Event
from backtest.domain import AccountSnapshot
from backtest.strategy import Strategy
from market_data import DataView


class MyStrategy(Strategy):
    def on_bar(
        self,
        data: DataView,
        event: Event,
        account: AccountSnapshot,
    ) -> dict[str, float] | None:
        rows = data.market.bars(
            symbols=("000001.SZ",),
            frequency=event.frequency,
            start=event.interval_start,
            end=event.at,
            fields=("close",),
        ).table.to_pylist()
        if not rows or rows[0]["close"] is None:
            return None
        return {"000001.SZ": 0.5}
```

返回值含义：

- `None`：本次不调仓；
- `{}`：清空现有持仓；
- `{"000001.SZ": 0.5}`：完整目标组合，平安银行目标权重为 50%。

策略只能看到当前 `DataView` 和只读账户快照，不能创建未来 `DataView`，也不能直接修改账户。

内置动量策略直接向 `market_data` 请求历史：

```python
data.market.bars(
    symbols=candidates,
    frequency="1d",
    count=lookback_sessions + 1,
    fields=("close",),
    adjustment="forward",
)
```

## 当前交易规则

- 下一根 Bar 开盘价一次性撮合；
- 停牌不成交，涨停不买，跌停不卖；
- 买入使用 100 股整手，清仓允许零股；
- 买入受现金限制，卖出受持仓和 T+1 限制；
- 可选使用上一根 Bar 的成交量限制成交规模；
- 同批订单先卖后买；
- 计算滑点、佣金、印花税和过户费；
- 未完全成交的剩余部分不继续排队。

## 运行和验证

`run_from_storage` 已注册全部 routes，新策略不需要处理数据源注册：

```python
completed = run_from_storage(
    config=BacktestConfig(...),
    strategy=MyStrategy(),
    options=RunOptions(output_dir=...),
)
```

```bash
uv run --group dev pytest -q
uv run --group dev ruff check .
uv run --group dev pyright
```
