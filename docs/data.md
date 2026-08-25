# `data`：回测数据可见性与读取接口

`data` 只解决两个问题：

1. 给定一个时间，当时能看到哪些数据；
2. 研究和回测如何通过同一个接口读取这些数据。

本文以 Tushare 为主要历史数据源。`DataCatalog` 只注册原始 Parquet 表；策略和回测只能使用
带 PIT 约束的 `DataReader`，不能直接访问原始表或 DuckDB connection。

当前 `DataCatalog` 的 `*_as_of` 宏是过渡实现，不代表本文定义的最终语义。

最常见的用法只有两步：先固定“站在哪个时间看”，再按业务领域取数。

```python
data = reader.at(as_of)

bars = data.market.bars(symbols=("000001.SZ",), frequency="1d", count=20)
income = data.fundamentals.statements(
    kind="income", symbols=("000001.SZ",), periods=8
)
status = data.market.status(symbols=("000001.SZ",))
```

调用方只需要理解 `market/fundamentals/corporate_actions/classification/calendar`，不需要理解
Tushare 表名、版本选择或 SQL。

## 一条核心规则

每次读取都绑定一个带时区的具体时间 `as_of`：

```python
datetime(2024, 4, 30, 9, 25, tzinfo=ZoneInfo("Asia/Shanghai"))
```

任何数据只有满足下面的条件才能返回：

```text
visible_at <= as_of
```

`as_of` 不接受纯 `date`，也不默认使用 `datetime.now()`。纯日期无法区分盘前、开盘、盘中和
盘后。`start/end` 可以使用接口规定的 `date` 或 `datetime`，但只表示数据范围，不能代替
`as_of`。

回测也是同一套规则，只是 `as_of` 由回测时钟提供，策略不能自行指定或修改。

## 默认时间约定

以下时间均为 `Asia/Shanghai`：

```text
PRE_OPEN_READY    09:25
MARKET_OPEN       09:30
DAILY_READY       16:05
DAILY_BASIC_READY 17:05
```

这些是根据 Tushare 更新时间设置的保守边界。它们属于可见性策略，修改后必须提升
`policy_version`，以便复现历史回测。

`next_session(D)` 表示严格晚于 D 的第一个交易日。周五公告的数据通常从下周一 09:25 可见。

## 所有数据的可见约束

| 数据集 | `visible_at` | 备注 |
| --- | --- | --- |
| QMT tick | `received_at` | 使用本系统实际收到该事件的时间，不使用行情自带时间冒充可见时间 |
| QMT 实时 bar | `max(interval_end, received_at)` | 只返回本系统已经收到的完整 K 线 |
| 历史分钟线 | `interval_end` | 仅适用于来源和区间语义已经验证的数据 |
| `daily.open` | 交易日 D 09:30 | 作为开盘事件，不作为完整日线 |
| 完整 `daily` | D 16:05 | 此前禁止返回当日最终 high/low/close/volume/amount |
| `daily_basic` | D 17:05 | 属于日终数据 |
| `moneyflow` | `next_session(D)` 09:25 | 缺少可靠更新时间，延迟到下一次盘前决策 |
| `stk_limit` | D 09:25 | D 日开盘起同时作为撮合约束 |
| `stock_st` | D 09:25 | D 日状态影响涨跌停规则 |
| `adj_factor` | D 09:25 | 只能用当时已可见的因子复权 |
| `suspend_d` 全日停复牌 | D 09:25 | D 日不得成交，不延迟到下一日 |
| `suspend_d` 日内停牌 | 停牌区间开始时间 | 不提前返回未来停牌区间 |
| `income`、`balancesheet`、`cashflow` | `next_session(f_ann_date)` 09:25 | 只认实际公告日 `f_ann_date` |
| `forecast`、`express`、`fina_audit` | `next_session(ann_date)` 09:25 | 公告日当天不可见 |
| `fina_indicator` | `next_session(ann_date)` 09:25 | 按 Tushare 已定义的指标口径使用 |
| 完整 `dividend` 实施记录 | `next_session(imp_ann_date)` 09:25 | 不用预案 `ann_date` 替补 |
| `sw_industry` 成员 | `in_date` 当日 09:25 | 只返回当前行业，不返回未来退出信息 |
| `trade_cal` | 不走普通 PIT 规则 | 作为回测引擎配置 |

D 表示记录的 `trade_date`、公告日期或生效日期。可见日期为空时，该记录不可见，不回退到其他
日期字段。

## 几个容易出错的地方

### 分钟线和日线

分钟线统一转换为半开区间 `[interval_start, interval_end)`，到 `interval_end` 才进入
`market.bars()`。QMT 的事件时间必须在导入时归一化；如果以后增加 Tushare 分钟表，必须先验证
其 `trade_time` 表示区间开始还是结束，不能由 Reader 临时猜测。

日线分成两个可见事件：

- D 日 09:30 后，`market.current()` 可以返回当日 `open`；
- D 日 16:05 后，`market.bars(frequency="1d")` 才能返回完整日线。

用 Tushare 历史日线回放 09:30 开盘事件属于回测事件模型，不代表实盘在 09:30 能从 Tushare
读取该行；实盘 `market.current()` 必须使用 QMT 等实时源。

看到某根 K 线或开盘价后生成的订单，只能参加后续撮合，不能成交在已经看完的 K 线或同一个
开盘事件中。

### 停牌、涨跌停和缺失行情

停牌不是下一交易日才知道：D 日全日停牌在 D 日盘前可见，撮合器在 D 日禁止成交。日内停牌
只有到停牌开始后才对策略可见。

策略读取当前状态，而不是整张原始停牌表：

```python
data.market.status(
    symbols=("000001.SZ",),
    fields=("suspended", "up_limit", "down_limit", "st_type"),
)
```

Tushare 在停牌期间没有日线。缺少日线只表示“没有行情数据”，不能直接推断为停牌，也不能
生成零成交 bar 或自动沿用前收盘价。

### 财报和修订

Tushare 财务数据大多只有公告日期，没有精确发布时间。因此公告日期为 D 的数据统一从
`next_session(D)` 09:25 可见。

三张报表只使用 `f_ann_date`；为空时不可见，不能回退 `ann_date`。同一报告期的版本选择顺序
必须是：

```text
先过滤 visible_at <= as_of
再按 ts_code、end_date、report_type、comp_type 选择最新版本
```

不能先选择今天看到的最终版本，再向过去过滤。需要统一计算口径的重要财务因子从 PIT 三张
报表计算；需要 Tushare 指标口径时直接使用 `fina_indicator`。

### 分红、复权和行业

严格 Reader 默认只返回 `imp_ann_date` 已公布的完整分红实施记录。若以后需要研究预案，应增加
独立的预案接口，只返回预案阶段已经知道的字段，不能提前返回后来补入的登记日和除权日。

价格以不复权日线为源。Reader 只能使用 `visible_at <= as_of` 的 `adj_factor` 现场复权，禁止
直接使用以今天最新因子生成的前复权历史。

`adjustment="forward"` 表示前复权。它与其他查询一样遵守统一的 PIT 规则，以 `as_of` 当时
最新可见因子为锚点：

```text
adjusted_price(t) = raw_price(t) * factor(t) / anchor_factor(as_of)
```

这里只调整 OHLC 和前收盘价；`volume/amount` 保留实际成交口径。结果元数据记录锚点日期和
因子；任一必要因子缺失时整个复权查询失败，调用方可以显式改查不复权价格，但 Reader 不返回
一张混合了复权和未复权行的表，也不自动改用今天的因子。

行业成员可以用 `in_date <= D < out_date` 计算 D 日有效分类，但返回结果不能包含未来
`out_date` 或当前快照的 `is_new`。

### 交易日历

交易日历用于驱动回测 session、处理节假日并计算 `next_session`，属于引擎配置，不作为普通
alpha 数据逐日解锁。策略只读取当前和历史 session；若要精确还原临时休市变更，需要另存
日历公告时间和版本。

## Tushare 数据使用前提

进入本地已发布快照的 Tushare 数据，视为已经由上游完成采集、清洗、校验和定版，是系统可以
直接使用的自有研究数据。`DataReader` 不在查询时重新评估或质疑数据质量，不做跨源复核，不因
首次采集时间或供应商身份降级结果，也不向策略返回 `exact/source_declared/approximate` 等质量
标签或相关警告。

Tushare 提供的公告日、生效日、交易日和版本字段是本系统构造 PIT 视图的权威输入。Reader 按
前文规则计算 `visible_at`、选择当时可见版本并直接返回结果，不猜测字段是否可靠，也不因为
存在其他数据源而拒绝使用。数据异常和跨源核验属于快照发布前的上游流程；一旦快照发布，
研究、回测和策略统一信任并使用该快照。

这里仍需保证的是查询语义和运行一致性，而不是再次审查数据质量：

- 回测启动时固定 `snapshot_id` 和 `policy_version`，运行中不自动刷新数据；
- Manifest 或 Parquet 更新不能改变一个正在运行的回测；
- 未登记可见性规则的新表，严格 Reader 拒绝读取；
- 空值或缺失不自动补零、前向填充、换表或切换数据源。

股票池也必须按历史时点构造。不能使用今天仍上市的股票列表回测过去；需要结合历史股票列表、
上市/退市区间和当日交易状态，避免幸存者偏差。

`snapshot_id` 在第一版只保证一次运行内一致；如果旧 Manifest 和数据文件没有归档，它不能承诺
未来任意时刻都能恢复该快照。

## 公共接口设计

成熟量化系统通常在内部统一处理历史请求，在策略侧按业务语义拆分接口：

- [Zipline DataPortal](https://zipline.ml4trading.io/_modules/zipline/data/data_portal.html)
  使用资产、结束时间、bar 数量、频率和字段定义历史窗口；
- [QuantConnect History](https://www.quantconnect.com/docs/v2/writing-algorithms/historical-data/history-requests)
  支持“最近 N 个样本”和“起止时间”两种查询，并用明确的数据类型区分行情、基本面和公司行动；
- [RQAlpha DataSource](https://github.com/ricequant/rqalpha/blob/master/rqalpha/data/base_data_source/data_source.py)
  把 `history_bars`、`current_snapshot` 和基本面读取分开。

本项目采用相同思路：公开接口按业务拆分，底层仍使用一个 PIT 查询规划器。

```text
DataReader.at(as_of) -> DataSnapshot
                          ├── market
                          ├── fundamentals
                          ├── corporate_actions
                          ├── classification
                          └── calendar
```

不公开 `query("tushare_table", ...)` 形式的万能策略接口。策略不需要知道源表名、可见日期列、
版本键或 SQL；这些信息由内部数据集注册表管理。

### 创建时间快照

研究代码先创建一个不可变快照，同一次计算中的所有读取共享同一个 `as_of`：

```python
from datetime import datetime
from zoneinfo import ZoneInfo

from data import DataCatalog, DataReader, SourceConfig


SHANGHAI = ZoneInfo("Asia/Shanghai")

with DataCatalog(
    tushare_root="data/tushare",
    qmt_root="data/qmt",
) as catalog:
    reader = DataReader(
        catalog,
        sources=SourceConfig(
            daily_bars="tushare",
            intraday_bars="qmt",
            fundamentals="tushare",
            realtime_quotes="qmt",
        ),
    )
    data = reader.at(
        datetime(2024, 4, 30, 9, 25, tzinfo=SHANGHAI)
    )
```

`DataReader.at()` 拒绝无时区 `datetime`，并把其他时区的 aware datetime 归一到
`Asia/Shanghai`。数据源在创建 Reader 时一次绑定，不允许策略逐次选择，也不在数据缺失时
静默切换来源。只使用 Tushare 时，`intraday_bars` 和 `realtime_quotes` 都设为 `None`，相应
方法明确报“数据源未配置”。

当前物理表与公共接口的关系如下：

| 公共接口 | 当前数据来源 |
| --- | --- |
| `market.bars(frequency="1d")` | `tushare.daily`；也可显式配置 `qmt.daily` 中的不复权日线 |
| `market.bars()` 分钟周期 | 当前只有已经实时接收并落盘的 `qmt.bars`；`tushare_data` 尚无分钟表 |
| `market.current()` | QMT tick/bar；无实时源时仅提供 Tushare 日线的开盘事件 |
| `market.daily_metrics()` | `tushare.daily_basic` |
| `market.moneyflow()` | `tushare.moneyflow` |
| `market.status()` | `tushare.suspend_d`、`stk_limit`、`stock_st` |
| `fundamentals.statements()` | `tushare.income`、`balancesheet`、`cashflow` |
| `fundamentals.indicators()` | `tushare.fina_indicator` |
| `fundamentals.disclosures()` | `tushare.forecast`、`express`、`fina_audit` |
| `corporate_actions.dividends()` | `tushare.dividend` |
| `market.bars(adjustment="forward")` | 不复权价格与 `tushare.adj_factor` |
| `classification.industry()` | `tushare.sw_industry` |
| `calendar.*` | `tushare.trade_cal` |

`qmt.financial` 和 `qmt.dividend_factors` 当前用于交叉验证，不作为第一版策略公共接口的数据源；
前者的字段和披露时间语义还需逐表登记，后者不能替代分红公告。`qmt.daily` 中已经下载好的
`front_ratio` 只用于数据校验，不能直接用于回测；`adjustment="forward"` 必须由不复权价格和
`as_of` 当时可见的因子计算。

### 公共参数

所有复数查询都使用仅限关键字参数，并共享以下约束：

- `symbols` 必须显式传入；空序列返回空结果，不解释为全市场；
- 全市场必须显式传 `ALL_SYMBOLS`，并同时提供 `limit` 或使用批量迭代接口；
- 重复或格式错误的证券代码直接报错；
- `fields=None` 表示该接口的全部公共字段；非空时只接受登记字段，不接受表达式或 SQL；
- 业务时间的 `start/end` 和报告范围均为左闭右开 `[start, end)`；
- `order` 只接受 `"asc"` 或 `"desc"`，不能传任意排序字段；
- `limit` 默认 `None`；非空时必须是正整数且不超过配置上限，布尔值不视为整数；
- 数据缺失返回空行或空表，不自动补数、换源或降低 PIT 规则。

每类结果的身份字段始终返回，不能被 `fields` 删除。例如 bar 始终包含
`symbol/interval_start/interval_end`，财报始终包含 `symbol/end_date/ann_date/f_ann_date`。

## 行情接口 `market`

### K 线 `market.bars()`

最近 N 根 K 线：

```python
bars = data.market.bars(
    symbols=("000001.SZ", "600000.SH"),
    frequency="1d",
    count=20,
    fields=("open", "high", "low", "close", "volume"),
    adjustment="forward",
    order="asc",
    limit=None,
)
```

时间范围查询：

```python
bars = data.market.bars(
    symbols=("000001.SZ",),
    frequency="1m",
    start=datetime(2024, 4, 30, 9, 30, tzinfo=SHANGHAI),
    end=datetime(2024, 4, 30, 11, 30, tzinfo=SHANGHAI),
    fields=("open", "high", "low", "close", "volume"),
    order="asc",
    limit=120,
)
```

接口约束：

- `frequency` 初始支持 `1m/5m/15m/30m/60m/1d`，只有已登记源才能读取；
- 第一版只读取源中明确记录的周期，不静默用 `1m` 数据合成其他周期；
- `count` 表示每只股票最近 N 根，而 `limit` 表示合并结果的全局行数上限；
- `count` 与 `start` 互斥；范围模式要求 `start`，`end` 默认 `as_of`；
- bar 的业务范围按 `interval_start` 判断，`end` 不得晚于 `as_of`；
- `adjustment` 只允许 `"none"` 和 `"forward"`，分别表示不复权和前复权，默认 `"none"`；
- `count=N` 时先对每个 symbol 选出最新 N 根，再按请求的输出顺序排序；
- `limit` 可以与 `count` 同时使用，但它会截断合并结果，可能使某些股票不足 N 根。

默认排序为：

```text
interval_end ASC, symbol ASC, interval_start ASC
```

`order="desc"` 时为：

```text
interval_end DESC, symbol ASC, interval_start DESC
```

### 当前行情 `market.current()`

```python
current = data.market.current(
    symbols=("000001.SZ", "600000.SH"),
    fields=("open", "last"),
    limit=100,
)
```

每个 symbol 最多返回一行，固定按 `symbol ASC` 排序。没有实时源时不伪造盘中 `last`；
Tushare 最终日线只能按前文规则提供已经发生的开盘事件。

### 市场状态 `market.status()`

```python
status = data.market.status(
    symbols=("000001.SZ", "600000.SH"),
    fields=("suspended", "st_type", "up_limit", "down_limit"),
    limit=100,
)
```

每个 symbol 最多一行，固定按 `symbol ASC` 排序。Reader 内部组合三张 Tushare 表；缺少其中一张
表的行保持对应状态未知，不能把缺失解释为 `False` 或普通股票。

### 日终指标和资金流

```python
metrics = data.market.daily_metrics(
    symbols=("000001.SZ",),
    start=date(2024, 1, 1),
    end=date(2024, 5, 1),
    fields=("turnover_rate", "pe", "pb", "total_mv"),
    order="asc",
    limit=100,
)

flows = data.market.moneyflow(
    symbols=("000001.SZ",),
    start=date(2024, 1, 1),
    end=date(2024, 5, 1),
    order="asc",
    limit=100,
)
```

二者默认按 `trade_date ASC, symbol ASC` 排序；倒序只反转 `trade_date`。这两个字段组成当前
两张源表的唯一业务键。

## 财务接口 `fundamentals`

### 三张报表 `fundamentals.statements()`

```python
income = data.fundamentals.statements(
    kind="income",
    symbols=("000001.SZ", "600000.SH"),
    report_start=date(2021, 1, 1),
    report_end=date(2024, 4, 1),
    periods=None,
    report_type="1",
    comp_type=None,
    fields=("revenue", "operate_profit", "n_income"),
    order="desc",
    limit=100,
)
```

`kind` 只允许 `"income"`、`"balance_sheet"` 和 `"cash_flow"`。如果只需要最近几个报告期：

```python
income = data.fundamentals.statements(
    kind="income",
    symbols=("000001.SZ", "600000.SH"),
    periods=8,
    fields=("revenue", "n_income"),
    order="desc",
)
```

`periods` 与 `report_start/report_end` 互斥；范围模式至少传 `report_start`，`report_end` 默认不设
上界。`periods` 是每只股票最近 N 个不同的 `end_date`，选完报告期后再返回符合
`report_type/comp_type` 的行；`limit` 仍是合并结果的全局上限。默认排序为：

```text
end_date DESC, symbol ASC, report_type ASC, comp_type ASC
```

`order="asc"` 只反转 `end_date`。在 SQL 中必须先过滤可见修订，再选每个业务键的最新版，最后
应用用户字段过滤和 `limit`。

### 财务指标 `fundamentals.indicators()`

```python
indicators = data.fundamentals.indicators(
    symbols=("000001.SZ",),
    periods=8,
    fields=("eps", "roe", "grossprofit_margin"),
    order="desc",
    limit=100,
)
```

参数与报表类似，默认按 `end_date DESC, symbol ASC` 排序。结果元数据必须标记其来自 Tushare
加工数据；需要严格复现的指标优先由 PIT 三张报表计算。

### 预告、快报和审计 `fundamentals.disclosures()`

```python
reports = data.fundamentals.disclosures(
    kind="forecast",
    symbols=("000001.SZ",),
    visible_start=datetime(2023, 1, 1, tzinfo=SHANGHAI),
    visible_end=data.as_of,
    order="asc",
    limit=100,
)
```

`kind` 只允许 `"forecast"`、`"express"` 和 `"audit"`。这里按信息进入策略视野的
`visible_at` 查询，而不是按报告期查询。`visible_end` 默认为 `as_of` 且不得晚于 `as_of`。
可见时间范围是两端都包含的 `[visible_start, visible_end]`，以保持
`visible_at <= as_of` 的核心语义。
默认排序为：

```text
visible_at ASC, symbol ASC, end_date ASC, ann_date ASC
```

## 公司行动接口 `corporate_actions`

```python
dividends = data.corporate_actions.dividends(
    symbols=("000001.SZ",),
    visible_start=datetime(2023, 1, 1, tzinfo=SHANGHAI),
    visible_end=data.as_of,
    order="asc",
    limit=100,
)
```

默认只返回已可见的完整实施记录，按
`visible_at ASC, symbol ASC, ex_date ASC, end_date ASC, ann_date ASC, div_proc ASC,
imp_ann_date ASC` 排序。预案将来使用独立方法，不能通过参数让完整实施字段提前出现。

`visible_end` 默认为 `as_of` 且不得晚于 `as_of`；可见时间范围两端都包含。

`adj_factor` 默认不作为策略需要直接处理的表，而由 `market.bars(adjustment="forward")` 使用。
研究确有需要时，可以提供只读的 `corporate_actions.adjustment_factors()`，但仍应用相同 PIT
规则和固定排序。

## 分类接口 `classification`

```python
industry = data.classification.industry(
    symbols=("000001.SZ", "600000.SH"),
    level=1,
    limit=100,
)
```

每只股票返回 `as_of` 时有效的一个行业，固定按 `symbol ASC` 排序。`level` 只允许 `1/2/3`；
返回类型不包含未来 `out_date` 和源表快照字段 `is_new`。

成熟量化系统通常还提供历史股票池接口，但当前仓库没有可靠的按日上市/退市股票列表。
在补充 Tushare 历史股票列表前，不提供会退化为“当前全部股票”的 `universe()`；调用方必须显式
传入 symbols，避免幸存者偏差。

## 日历接口 `calendar`

```python
sessions = data.calendar.sessions(
    start=date(2024, 1, 1),
    end=date(2024, 5, 1),
    exchange="SSE",
    order="asc",
    limit=100,
)

previous = data.calendar.previous_session(exchange="SSE")
```

`sessions()` 默认按 `cal_date ASC, exchange ASC` 排序。回测引擎内部还可以使用完整日历计算下一
session；策略公共接口初始只提供 `cal_date <= as_of` 本地日期的 session，不提供未来日历查询。

## 返回值、排序和截断

有限查询返回统一结果对象，底层数据使用 Arrow，调用方可以显式转换为 pandas：

```python
@dataclass(frozen=True, slots=True)
class QueryResult:
    table: pyarrow.Table
    as_of: datetime
    snapshot_id: str
    policy_version: int
    sources: tuple[str, ...]
    sort_keys: tuple[str, ...]
    truncated: bool

    def to_pandas(self) -> pandas.DataFrame: ...
```

各接口的默认排序汇总如下：

| 接口 | 默认排序 |
| --- | --- |
| `market.bars()` | `interval_end ASC, symbol ASC, interval_start ASC` |
| `market.current/status()` | `symbol ASC` |
| `market.daily_metrics/moneyflow()` | `trade_date ASC, symbol ASC` |
| `fundamentals.statements()` | `end_date DESC, symbol ASC, report_type ASC, comp_type ASC` |
| `fundamentals.indicators()` | `end_date DESC, symbol ASC` |
| `fundamentals.disclosures()` | `visible_at ASC, symbol ASC, end_date ASC, ann_date ASC` |
| `corporate_actions.dividends()` | `visible_at ASC, symbol ASC, ex_date ASC, end_date ASC, ann_date ASC, div_proc ASC, imp_ann_date ASC` |
| `classification.industry()` | `symbol ASC` |
| `calendar.sessions()` | `cal_date ASC, exchange ASC` |

排序必须显式写入 SQL，不能依赖 Parquet、Manifest 或 DuckDB 的物理行顺序。`order` 只反转表中
的主时间键，其余键始终作为稳定升序 tie-breaker；所有可空排序键显式使用 `NULLS LAST`。

`limit` 统一遵循：

1. 先应用 `visible_at <= as_of`；
2. 再选择当时可见的最新版本；
3. 再执行安全的业务参数过滤和字段投影；
4. 再按接口规定的稳定键排序；
5. 最后执行 `LIMIT limit + 1`，返回前 `limit` 行并据此设置 `truncated`。

`limit=None` 表示不做逻辑截断，不允许内部静默使用默认 limit。大结果使用相同查询对象的
`iter_batches(batch_size=...)`；第一版不提供 `offset`，需要分页时使用包含 `snapshot_id` 和
最后排序键的 keyset cursor。

## 回测接口

回测引擎从同一个 `DataReader` 创建绑定模拟时钟的快照：

```python
def on_event(context, data):
    assert data.as_of == context.now
    bars = data.market.bars(
        symbols=context.universe,
        frequency="1d",
        count=20,
    )
```

策略不能调用 `data.at()` 或修改 `data.as_of`：

```text
研究：调用方通过 DataReader.at(as_of) 创建快照
回测：引擎使用 clock.now 创建快照
实盘：引擎使用真实当前时间创建快照
```

三者复用完全相同的领域接口和 PIT 查询规划器。

## 内部查询模型

公共领域方法转换为内部 `ReadRequest`，策略不能直接构造 SQL：

```text
ReadRequest
    dataset
    as_of
    symbols
    business_range
    allowed_filters
    projection
    sort
    limit
```

执行顺序固定为：

```text
固定 snapshot 文件并取得读取 lease
-> 校验参数和数据集策略
-> 按 symbols/业务范围裁剪
-> 过滤 visible_at <= as_of
-> 在业务键内选择最新可见版本
-> 基于 as_of 可见数据计算前复权或安全派生字段
-> 应用用户 payload 过滤
-> 投影公共字段
-> 稳定排序
-> limit + 1
-> QueryResult
```

身份和业务范围过滤可以安全地下推到版本选择之前；依赖 payload 值的过滤必须在版本选择之后。
否则最新修订不符合条件时，查询可能错误地重新返回符合条件的旧版本。

`DataCatalog` 只负责文件和 DuckDB 生命周期；`DataReader` 及其领域 Reader 负责可见性、版本、
字段安全、排序和审计。策略代码不得持有 `catalog.connection`。

实现 `snapshot_id` 时必须同时固定 Manifest 中的文件列表，并让压缩/垃圾回收在所有读取 lease
释放前保留旧文件。只固定文件路径但允许后台删除文件，既不能保证一致读取，也不能保证查询
一定成功。

## 验收条件

最重要的性质是：

> 向数据集中加入任意 `visible_at > T` 的记录或修订，时点 T 的所有查询结果必须完全不变。

此外至少验证：

- 所有公共方法使用同一个快照 `as_of/snapshot_id/policy_version`；
- 相同参数的结果字段、排序和 `truncated` 完全稳定；
- `limit` 只在 PIT 过滤、版本选择和排序后执行；
- `count=N` 对每个 symbol 生效，`limit=N` 对合并结果生效；
- 分钟线在结束前不可见，当日日线最终字段在 16:05 前不可见；
- 看见开盘价后的订单不能成交在同一个开盘事件；
- D 日停牌证券在 D 日不能成交，状态缺失不自动变成 `False`；
- 财报先过滤可见版本，再选择最新修订；
- 复权不使用 `as_of` 之后的因子；
- 行业结果不泄漏未来退出日期；
- 缺失日线不会自动变成停牌或零成交 bar；
- 回测运行期间新增数据不会改变固定快照的结果。
