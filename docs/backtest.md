# 回测引擎

申报数量统一由 `backtest.trading_rules.quantity_rule` 按代码所属市场、板块、日期及申报类型读取。
当前撮合支持市价数量订单，目标权重超过单笔上限会拆单，直接委托越限则拒绝；科创板和北交所支持最低量以上逐股递增，清仓保留零股卖出。
规则依据：[上交所申报说明](https://edu.sse.com.cn/tib/qa/)、
[深交所创业板数量说明](https://www.szse.cn/www/investor/knowledge/t20200819_580804.html)、
[北交所交易规则](https://www.bse.cn/jygl_list/200028217.html)。股转市场、基金及限价撮合未纳入该实现。

这是一个面向 **A 股现金多头、Bar 级撮合**的简化回测引擎。策略调用时间可以配置，
撮合行情支持 `1m/5m/15m/30m/60m/1d`。限价单、做空、杠杆、夜盘、逐笔成交和自动聚合
新 Bar 周期尚未实现。调整市场时间配置不会自动增加其他市场的交易与结算规则。

## 第一次阅读：先跑四根 K 线

在已安装项目依赖的环境中，从仓库根目录运行：

```bash
.venv/bin/python examples/backtest_minimal.py
```

[最小示例](../examples/backtest_minimal.py) 自带四根合成日线，在临时目录中准备数据，
使用正式的数据读取层和回测入口；不需要本地行情库、行情账号或联网下载。
策略和运行入口放在文件前面，第一次阅读可以先跳过后面的数据准备函数。

| 时点 | 信号与成交 | 账户状态 |
| --- | --- | --- |
| 第一天收盘后 | 提交买入 100 股，尚未成交 | 持仓 0 |
| 第二天 | 买单按当天开盘价加滑点成交 | 持仓 100，当天可卖 0 |
| 第三天收盘后 | T+1 已解锁，提交卖出 100 股 | 持仓 100，可卖 100；提交卖单不立即扣持仓 |
| 第四天 | 卖单按当天开盘价减滑点成交 | 持仓 0，现金已扣除费用 |

日志会同时说明“当前模拟时钟”和成交记录的 `filled_at`。日线在 16:05 完整可见，
引擎此时确认此前订单按 09:30 开盘价成交；不能把这两种时间理解成同一个字段。
示例保留滑点、佣金、印花税和过户费，并自检最终持仓与现金。

默认参数下，最后一行应为：

```text
自检通过：两笔成交，最终空仓，现金=10088.379275 元。
```

建议阅读顺序：[最小示例](../examples/backtest_minimal.py) →
[策略接口](../src/backtest/strategy.py) → [Engine.run](../src/backtest/engine.py) →
[撮合](../src/backtest/broker.py)与[账户](../src/backtest/portfolio.py)。
需要修改调用规则时，再读 [schedule.py](../src/backtest/schedule.py)。

## 先弄清这些约定

| 容易误解的地方 | 当前行为 |
| --- | --- |
| `frequency` 是策略周期吗？ | 它是撮合和估值的行情周期；调用周期看 `Schedule`，研究周期看策略的数据查询 |
| `Schedule("week")` 固定在几点？ | 默认跟随最后一根 Bar：日线是 16:05，分钟线是 15:00；需要固定时间就显式设置 `at` |
| 返回 `None` 是没有交易吗？ | 只表示不触发目标权重调仓；通过 `context.order()` 提交的数量订单仍有效 |
| 返回空字典 `{}` 是什么？ | 请求把全部已有持仓的目标降为零，不等于跳过本次调用 |
| 权重字典只包含想调整的股票吗？ | 它描述完整目标组合，未列出的已有持仓也会以零为目标 |
| `context.order()` 会立即改变账户吗？ | 只提交委托；后续撮合成功才改变账户，当前回调的账户快照不会随提交而变化 |
| 未成交订单会一直挂着吗？ | 当前只尝试一次撮合；未成交及部分成交的剩余数量不继续排队 |
| `on_event()` 会收到开盘、收盘等所有事件吗？ | 只收到 `Schedule` 触发的策略事件；市场生命周期由引擎处理 |

例如已有 A、B 两只股票，返回 `{"A": 0.5}` 表示 A 的目标权重为 50%、B 的目标为零，
并不是只调整 A、保留 B。目标是否实际成交，仍受现金、T+1、行情与撮合规则限制。

## 市场周期与策略周期

回测使用一条同步事件循环。市场时间、撮合周期、策略调用时间分别配置：

| 配置 | 职责 | 例子 |
| --- | --- | --- |
| `BacktestConfig.market` | 交易所、时区、交易时段、日线可见及日终时间 | 默认 A 股时间 |
| `BacktestConfig.frequency` | 撮合和估值使用的 Bar 周期 | `1m/5m/15m/30m/60m/1d` |
| `Strategy.schedule` | 策略什么时候执行 | 每根 Bar、每 7 分钟、每天、每周、每月、指定日期时间 |

研究数据周期由策略查询自行决定。例如使用分钟线撮合，每月调仓，读取过去 20 个交易日的日线计算信号。

```text
交易日开始
  → T+1 解锁
  → 公司行动和退市处理

Bar 事件
  → DataView 读取 open / close
  → Broker 用 open 撮合上一根 Bar 后提交的订单
  → Portfolio 应用成交
  → Portfolio 用 close 估值
策略事件
  → Strategy.on_event(context)
  → 返回完整目标权重，或直接下单、撤单

交易日结束
  → 登记公司行动权益
  → 记录账户净值
```

同一时刻的顺序固定为：交易日开始 → Bar 撮合与估值 → 策略调用 → 交易日结束。
不调用策略的日期仍然处理 T+1、公司行动、撮合和每日净值。回测结束后，待处理订单标记为 `EXPIRED`。

## 最重要的边界

`market_timeline` 只生成市场生命周期和 Bar 事件，不判断周末、月末，也不读取策略。
`Schedule` 单独生成策略事件。`Clock` 只负责保证模拟时间不倒退。

默认市场在 09:25 开始日初处理，连续交易时段为 09:30–11:30、13:00–15:00；
日线在 16:05 可见，账户也在 16:05 完成日终登记。`MarketHours` 可以调整这些时间，
但配置必须匹配数据源的区间和可见性约定。当前时间配置支持同一自然日内的交易时段。

每个事件通过 `reader.at(event.at)` 创建 PIT `DataView`。当前 Bar、历史窗口、复权、文件裁剪、
缓存和批量查询全部属于 `market_data`。回测层不维护 `BarFeed`、`HistoricalData` 或第二套缓存。
PIT 表示“只读取模拟时点已经可见的数据”；时钟不倒退本身不能保证没有前视，数据读取仍须遵守这个约束。

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

策略调用必须落在 Bar 的开始、结束或市场生命周期事件上。比如 `5m` 撮合不能在 10:03
调用策略；配置时直接报错，需要改为 `1m` 撮合。这样避免 Bar 中途下单或撤单影响更早的开盘成交。
Broker 也会检查委托时间，开盘之后提交的订单不会回填到该根 Bar 的开盘。

## 文件职责

| 文件 | 内容 |
| --- | --- |
| `config.py` | 回测参数和路径参数 |
| `clock.py` | MarketHours、Event、市场时间线、单向时钟 |
| `schedule.py` | 先计算调用时间，再检查边界并构造策略事件 |
| `domain.py` | Bar、订单、成交、账户快照、最终结果 |
| `strategy.py` | Strategy、当前上下文、数量下单和撤单接口 |
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

## 保存与复现运行

向运行入口传入 `output_dir` 后，会生成 `config.json`、`metrics.json`、
`run_metadata.json` 和订单、订单更新、成交、每日净值四份 Parquet。
Parquet 的空表与非空表使用相同 schema，时间戳统一存为 UTC。

`run_metadata.json` 在回放前记录策略类型、参数、调度、源码摘要、Git 提交与未提交状态、
Python 和依赖版本、来源路由、实际加载的数据文件版本，以及启用的证券代码映射内容。
策略默认通过 `parameters()` 返回初始实例字段；使用 `slots` 或含有连接等复杂对象时，
应覆盖该方法，显式返回完整的策略配置和随机种子等输入。动量示例已显式列出全部参数。
配置可包含 dataclass、日期、时间、路径及 JSON 基本类型；不可序列化的值在运行前报错。

目录快照保留上次 `refresh()` 注册的 UUID 文件路径、顺序、大小、修改时间和 QMT 同步区间，
不会在运行结束时重新扫描 Manifest。自定义数据适配器需要实现 `snapshot_metadata()`，
返回可重新定位的数据版本。记录快照不会复制行情或源码：复现时须保留所列文件和对应代码；
存储清理已删掉的历史文件不能仅凭快照 ID 恢复，未提交代码也须自行保留。

## 公司行动时间语义

公司行动同时包含两类时间，职责不能混用：

- `visible_at` 只决定策略何时能从 `DataView` 查到公告；
- `record_date / ex_date / pay_date / listing_date` 决定账户何时登记和结算。

账户在回测开始时读取当前数据快照中的最终实施事实。登记日收盘保存持股数量，除权日确认
应收现金和待上市红股，随后在派息日和上市日完成结算。登记数量是账户内部状态，不会暴露给
策略，因此即使一条实施记录在登记日之后才可见，也不会产生策略前视。

特殊分配没有 `ex_date` 时不推测日期：现金在 `pay_date` 直接到账，股票在 `listing_date`
直接增加为可卖数量。只有存在实际登记权益时才校验结算字段；无持仓事件不会中断回测。
账户显式处理公司行动，所以撮合和估值必须使用不复权行情，避免重复计算分红送股。

## 策略接口

```python
from datetime import date, time

from backtest import BacktestConfig, Schedule, Strategy, StrategyContext, run_from_storage


class WeeklyStrategy(Strategy):
    schedule = Schedule("week", at=time(16, 5))

    def on_event(self, context: StrategyContext) -> dict[str, float] | None:
        rows = context.data.market.bars(
            symbols=("000001.SZ",),
            frequency="1d",
            count=20,
            fields=("close",),
            adjustment="forward",
        ).table.to_pylist()
        if len(rows) < 20:
            return None
        return {"000001.SZ": 0.5}


completed = run_from_storage(
    config=BacktestConfig(
        start_date=date(2026, 1, 5),
        end_date=date(2026, 1, 30),
        frequency="1d",
        symbols=("000001.SZ",),
    ),
    strategy=WeeklyStrategy(),
)
```

这个示例演示“周频调用、日线研究及撮合”的接口；显式在 16:05 等待当日日线可见，
返回固定权重，仅用于说明用法。它使用本地生产数据；首次上手请先运行上面的合成数据示例。

返回值含义：

- `None`：本次不触发目标权重调仓，已直接提交的数量订单仍有效；
- `{}`：清空现有持仓；
- `{"000001.SZ": 0.5}`：完整目标组合，平安银行目标权重为 50%。

目标数量根据调用时点可见的最新不复权收盘价计算。开盘前调用时只能使用此前可见的价格，
实际成交价格由后续撮合决定。策略上下文包含 `data`、`event`、只读 `account` 和 `pending_orders`。

`context.event` 的字段含义如下：

| 字段 | 含义 |
| --- | --- |
| `kind` | 引擎事件类型；传给策略回调时始终是 `"strategy"` |
| `at` | 本次调用的模拟时间，也是当前 `DataView` 的查询时点 |
| `session` | 事件所属交易日，用于账户结算与每日净值 |
| `frequency` | 撮合行情周期；周频策略使用日线撮合时仍为 `"1d"` |
| `interval_start` | 关联 Bar 的开始或当前市场边界；不是策略研究窗口的起点 |

在 Bar 结束时调用，`interval_start` 对应刚完成的那根 Bar；在 Bar 开始或日初、日终等
市场边界调用，该字段可能等于 `at`。策略查询过去 20 天等历史时，应自行指定 `count` 或
日期范围，不能把这个字段当作研究窗口。

按数量下单时，通过上下文提交即可：

```python
from datetime import time

from backtest import Schedule, Side, Strategy, StrategyContext


class QuantityStrategy(Strategy):
    schedule = Schedule("day", at=time(9, 25))

    def on_event(self, context: StrategyContext) -> None:
        for pending in context.pending_orders:
            context.cancel(pending.order_id)
        if context.account.holding("000001.SZ") is None:
            context.order("000001.SZ", Side.BUY, 100)
```

`order` 返回包含 `order_id` 的订单；`cancel` 成功返回 `True`，订单已结束或不存在时返回 `False`。
一次回调选择一种下单方式：直接下单后返回 `None`，或返回完整目标权重。
同时使用两种方式可能产生重复委托。数量订单与组合订单共用现有撮合、现金及 T+1 规则。

## 调度规则

| 声明 | 调用时点 |
| --- | --- |
| `Schedule()` | 每根撮合 Bar 结束后 |
| `Schedule("7m")` | 每段连续交易时段开始后每 7 分钟；需匹配更细的撮合 Bar |
| `Schedule("day")` | 每个交易日最后一根撮合 Bar 结束后 |
| `Schedule("week")` | 每周最后一个交易日的最后一根 Bar 结束后 |
| `Schedule("month", at=time(16, 5))` | 每月最后一个交易日 16:05 |
| `Schedule("day", at=time(14, 50))` | 每个交易日 14:50；需对应的分钟撮合周期 |
| `Schedule(times=(... ,))` | 用户列出的带时区日期时间，排序并去重 |

`Schedule("7m")` 在默认市场的实际调用时间为：

```text
上午：09:37、09:44、……、11:29
下午：13:07、13:14、……、14:59
```

午休后从下午交易时段重新计时。这个规则可配合 `1m` 撮合；配合 `5m` 会因 09:37 等
时刻不在 Bar 边界上而报错。调用周期独立配置，并不表示它可以忽略撮合数据的时间精度。

周末、月末通过下一交易日是否跨周、跨月判断，节假日由真实日历处理。
普通调度只读取回测区间的日历；周/月末调度额外取得下一交易日，缺失时明确报错。
回测提前结束不会把最后一天强行当作周期末。不再统一读取未来 40 天日历。

指定时间必须处于回测范围内并匹配市场事件或 Bar 边界；不会静默挪动时间。
更复杂的周期可预先生成 `times`，或使用默认逐 Bar 调用，在策略中根据当前数据决定是否交易。

内置动量策略声明 `Schedule("month", at=time(16, 5))`，继续向 `market_data` 请求日线历史：

```python
data.market.bars(
    symbols=candidates,
    frequency="1d",
    count=lookback_sessions + 1,
    fields=("close",),
    adjustment="forward",
)
```

因此该示例可以用日线或分钟线撮合，调用时都等待当日日线可见。
`run_monthly_momentum` 已移到 `strategies` 包，通用 `runner` 不导入具体策略。

旧策略迁移：将 `on_bar(data, event, account)` 改为 `on_event(context)`，原参数分别对应
`context.data`、`context.event`、`context.account`。删除策略中对 `event.is_month_end` 的判断，
改为声明 `schedule`。事件使用 `kind` 区分类型，不再包含 `is_month_end/is_session_end` 标志。

## 当前交易规则

下列规则适用于文档开头声明的 A 股现金多头范围：

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
    strategy=WeeklyStrategy(),
    options=RunOptions(output_dir=...),
)
```

```bash
uv run --group dev pytest -q
uv run --group dev ruff check .
uv run --group dev pyright
```
