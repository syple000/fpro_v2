# `data`：面向回测和实盘的严格 PIT 读取层

`data` 位于 `tushare_data`、`qmt_receiver` 的存储之上。当前实现只把 Manifest
引用的 Parquet 文件注册成 DuckDB 原始视图，并提供少量 `*_as_of` 表宏；目标实现是在
`DataCatalog` 之上增加有类型的 `DataReader`，统一回测与实盘的查询入口，并默认阻止未来
数据进入策略。

原始视图仍用于数据核查和临时研究，但不作为策略接口。任何直接访问
`catalog.connection`、`tushare.<table>` 或 `qmt.<table>` 的代码，都不享有 PIT 保证。

## 设计结论

“传入日期表示当天开盘决策点，日期精度的数据默认只能看到更早日期”是合理的保守规则，
但需要补充以下约束：

1. `date` 不是“当日任意时间”，而是对应市场开盘事件发生后的逻辑决策点。中国股票市场
   默认解析为当日 `09:30 Asia/Shanghai`；非交易日没有开盘决策点，应拒绝日期输入。需要在
   非交易日查询时，调用方必须传入带时区的 `datetime`。
2. 开盘价在该决策点可见后，策略产生的订单不能再按同一个开盘价成交。若订单需要参加
   当日开盘撮合，决策时点必须在开盘事件之前，此时当日 `open` 不可见。数据可见性和撮合
   事件顺序必须共同满足这一规则。
3. Tick 是一次原子观测，不是尚未完成的 K 线。只要一条 tick 已经被系统接收，它携带的
   成交价、盘口和累计量都是当时真实可见值，应该完整返回；“当前区间只返回 `open`”只
   适用于 K 线。
4. 对只有日期、没有发布时间的数据，即使传入当日 `14:00`，也不能证明该数据已于
   `14:00` 发布。严格模式在整个当天都只接受日期早于当天的记录。这样会保守地延迟部分
   盘前公告，但不会把盘后公告提前到盘中。
5. 数据集必须逐一声明可见性策略，不能再按“所有市场表”或“所有公告表”使用同一个
   `<= as_of_date` 模板。未登记策略的新表应拒绝读取，而不是猜测可见日期。
6. 预先下载的前复权历史序列可能使用后来发生的除权事件重写更早价格。严格 Reader 不直接
   返回这类序列，而是从不复权价格和决策时点前可见的因子现场计算。

## 分层

```text
Tushare / QMT
      │
      ▼
源结构与固定 Arrow Schema
      │  tushare_data.records / qmt_receiver.records / qmt_protocol payload
      ▼
DataCatalog
      │  只注册 Manifest 当前文件和内部 SQL 原语
      ▼
可见性规划器
      │  解析决策时点、选择数据集策略、生成严格过滤与版本选择
      ▼
DataReader
      │  有类型、稳定排序、流式返回
      ▼
回测 / 实盘策略
```

`DataCatalog` 继续负责物理文件发现和 DuckDB 生命周期，不承担策略语义。
`DataReader` 组合一个 Catalog，并负责：

- 强制要求 `as_of`；
- 规范化时区和市场开盘时刻；
- 根据数据集注册表应用可见性策略；
- 做源数据版本选择和安全字段投影；
- 把 Arrow/DuckDB 行转换成明确结构；
- 以相同规则服务历史回测与实盘查询。

## 时间与区间约定

### 决策时点

公共接口接受 `date | datetime`：

- `date`：通过 `TradingClock` 解析为该市场当日开盘事件后的决策点。当前中国股票默认
  `09:30 Asia/Shanghai`，并用交易日历验证该日开市。
- `datetime`：必须带时区，表示该精确时刻；无时区 `datetime` 直接报错。
- 内部统一转换为 UTC Unix Epoch 微秒，同时保留本地市场日期。
- 不提供隐式 `datetime.now()` 默认值，防止回测遗漏 `as_of` 后读取到最新数据。

查询对象中的业务范围与决策时点必须分离：`start`/`end` 约束事件或报告范围，`as_of`
约束当时能知道什么。所有时间区间统一为左闭右开 `[start, end)`；日期报告范围也采用
左闭右开，避免连续批次重复边界。

### 比较规则

- 有真实观测时间的记录：`available_at <= decision_time`。
- 只有业务日期或公告日期的记录：`available_date < decision_local_date`。
- 版本表先过滤可见版本，再在业务键内选择最新版本；不能先选当前最新版再判断日期。
- `NULL` 可见日期不回退到其他日期，也不进入严格读取结果。
- 若同时有事件时间和接收时间，接收时间控制“系统何时知道”，事件时间用于业务排序；
  非空事件时间晚于决策时点的异常记录也必须排除。

“前一天可见”指日期严格早于决策所在的本地日，不是简单减去一个交易日。因此周六发布的
日期级公告可以在周一开盘读取，而周一日期的公告无论实际是盘前还是盘后，都要到周二才
进入严格接口。

## K 线和 Tick 的可见性

K 线使用半开区间 `[interval_start, interval_end)`，返回两个互斥结构，避免用可空字段
混淆“暂不可见”和“源值确实为空”：

```python
from dataclasses import dataclass
from datetime import datetime
from typing import TypeAlias


@dataclass(frozen=True, slots=True)
class OpenBar:
    source: str
    code: str
    period: str
    interval_start: datetime
    adjustment: str | None
    open: float | None


@dataclass(frozen=True, slots=True)
class CompletedBar:
    source: str
    code: str
    period: str
    interval_start: datetime
    interval_end: datetime
    adjustment: str | None
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: int | None
    amount: float | None


BarResult: TypeAlias = OpenBar | CompletedBar
```

- 已结束且已到达系统的更早区间返回 `CompletedBar`。
- 决策时点落在当前区间内时只返回 `OpenBar`。
- 尚未开始的区间不返回。
- 对实时 K 线，当前 `open` 还必须来自 `received_at <= decision_time` 的实际更新；不能从
  后来落盘的最终 K 线反推一个当时尚未成交的证券已经形成开盘价。
- 对只有最终日线的历史源，可按常见日线回测约定把当日 `open` 当作开盘事件值，但要标记
  为 `CONVENTIONAL_OPEN`。需要逐笔严格重放时，只能从当时已接收的 tick/bar 更新生成。

Tick 按 `received_at` 过滤后返回完整的 `qmt_protocol.QuoteEvent`。`event_time` 不能替代
`received_at`，否则延迟到达或时钟偏差会让策略提前看到尚未进入系统的数据。

QMT `daily.adjustment = 'front'` 以及任何以查询终点最新因子归一化的历史价格，在过去决策
时点都可能包含后来发生的公司行动。严格接口只把 `adjustment = 'none'` 当作源价格；如需
复权，由 Reader 使用可见日期早于 `D` 的因子计算，并把归一化锚点限制在决策时点以内。
原始前复权序列只能用于当前快照复核，PIT 质量标记为 `UNSAFE`。

## 各数据集默认可见性

下表是严格模式的初始策略。`D` 是决策时点的市场本地日期。

| 数据类别 | 数据集 | 严格可见条件 | 返回规则 |
| --- | --- | --- | --- |
| 实时 Tick | `qmt.ticks` | `received_at <= as_of`，且非空 `event_time <= as_of` | 返回完整 Tick |
| 实时 K 线更新 | `qmt.bars` | `received_at <= as_of` | 当前区间仅 `OpenBar`，更早区间可完整返回 |
| 历史日线 K 线 | `tushare.daily`、QMT `daily(adjustment='none')` | `trade_date < D`；`trade_date = D` 仅开放盘事件 | 更早日期返回完整 K 线，当日仅 `OpenBar`；预计算前复权序列禁用 |
| 日终横截面 | `daily_basic`、`moneyflow` | `trade_date < D` | 返回全部源字段 |
| 日级市场状态 | `suspend_d`、`stk_limit`、`stock_st` | 默认 `trade_date < D` | 保守延迟一天；日后可在验证盘前来源后单独升级策略 |
| 复权因子 | Tushare `adj_factor`、QMT `dividend_factors` | 默认业务日期 `< D` | 不凭生效日猜测盘前发布时间 |
| 三张财务报表 | `income`、`balancesheet`、`cashflow` | `f_ann_date < D` | 按代码、报告期、报表类型、公司类型选可见最新版，不回退 `ann_date` |
| 财务指标/公告 | `fina_indicator`、`forecast`、`express`、`fina_audit` | `ann_date < D` | 按各自业务键选择可见最新版 |
| 分红 | `dividend` | `imp_ann_date < D` | 只返回已发布实施公告的记录，不回退预案 `ann_date` |
| QMT 财务 | `qmt.financial` | `disclosure_date < D` | 按代码、表、报告期选择可见最新版 |
| 行业成员 | `sw_industry` | 默认读取 `D` 之前最后一个有效状态 | 不返回源表的未来 `out_date` 和当前快照 `is_new` |
| 交易日历 | `trade_cal` | 策略读取默认 `cal_date < D` | `TradingClock` 可把完整日历作为运行配置使用，但不得作为历史 alpha 特征 |

`adj_factor`、涨跌停、停牌、ST 和行业调整中有些信息实际上会在当日开盘前确定。当前源表只有
业务日期，没有可靠的公告时刻或采集时刻，因此先使用延迟一天的保守策略。将来只有在增加
公告时间、交易所生效时间或 `observed_at` 后，才能把某个数据集明确升级为“当日开盘可见”。

## 结构定义与归属

### Tushare

在 `tushare_data/records.py` 为 `TABLE_SCHEMAS` 的每张表定义不可变、带 `slots` 的记录类型：

```text
DailyRecord, DailyBasicRecord, AdjFactorRecord, SuspendRecord,
StockLimitRecord, StockStatusRecord, MoneyflowRecord, DividendRecord,
ForecastRecord, ExpressRecord, AuditRecord, IncomeStatement,
BalanceSheet, CashFlowStatement, FinancialIndicator,
SwIndustryMemberRecord, TradeCalendarRecord
```

字段名、Python 类型和可空性必须逐字段匹配 Arrow Schema。由于财务表最多有上百个字段，
记录文件应由一个受版本控制的生成脚本从 `TABLE_SCHEMAS` 生成，并提交生成结果；CI 通过
Schema 一致性测试防止结构漂移。不要在运行时动态生成类，否则静态类型检查和 IDE 无法识别
字段。

原始记录类型只描述源表，不自行决定可见性。策略只能经 `DataReader` 获得这些结构；当前
K 线、行业成员等需要裁剪字段的结果使用 `data.models` 中的安全结构，而不是把不可见字段
填成 `None` 后伪装成完整原始记录。

### QMT 与 `qmt_protocol` 的对齐情况

QMT 物理表并非全部等同于 `qmt_protocol`：

| QMT 表 | 当前对齐情况 | 规划 |
| --- | --- | --- |
| `ticks` | `quote` 的 24 个字段与 `TickQuote` 完全一致，信封可还原为 `QuoteEvent` | 直接复用 `QuoteEvent` |
| `bars` | `quote` 的 14 个字段与 `BarQuote` 完全一致，信封可还原为 `QuoteEvent` | 复用 payload；另外返回安全的 `OpenBar/CompletedBar` |
| `daily` | 从 `HistoryBar` 展平并增加代码、交易日、复权类型；当前 Schema 还遗漏了 `settlementPrice` | 在 `qmt_receiver.records` 定义存储记录，补齐字段并迁移/重拉 |
| `financial` | 外层是报告日、代码、表名、披露日和 `data_json`；写入时丢弃了协议记录必需的原始 `index`，当前主键还会覆盖同报告期旧披露版本 | Schema v2 保留原始 index，并把披露日期纳入版本键，再按 `dataset` 解码为对应 `FinancialRecord` |
| `dividend_factors` | 原始 `date/time` 已转换为 `ex_date/event_time`，并增加代码 | 使用 `QmtDividendFactorRecord`，不冒充原始 `DividendFactor` |

因此，`qmt_protocol` 继续只负责 agent/receiver 的网络和原始 XtData 契约；不要把 DuckDB
物理行塞入该包。与存储 Schema 对齐的 `QmtDailyBarRecord`、`QmtFinancialRecord` 和
`QmtDividendFactorRecord` 放在 `qmt_receiver/records.py`，其中的 payload 尽量复用
`qmt_protocol` 类型。`data.models` 只保存跨源读取所需的公共、安全结构。

实时 `bars` 目前以 `(code, period, event_time)` 去重并偏好最新 `received_at`，整理后会丢失
同一区间的早期更新。这无法严格重建任意盘中时点。实现读取接口前必须二选一：

- 将 bar 更新按 `received_at/seq` 作为版本保留，再由读取层选择决策时点前最后一个版本；或
- 单独保存不可变 `bar_open` 事件和最终 K 线。

初始实现建议保留版本，待数据量验证后再决定是否增加派生的 open/final 表。

## 公共读取接口草案

公共入口采用一个门面和按业务分类的 reader：

```python
from datetime import date

from data import DataCatalog, DataReader, ReadContext

with (
    DataCatalog(tushare_root="data/tushare", qmt_root="data/qmt") as catalog,
    DataReader(catalog, market="CN") as data,
):
    context = ReadContext(as_of=date(2024, 4, 30))

    bars = data.market.daily_bars(
        source="tushare",
        codes=("000001.SZ",),
        period="1d",
        start=date(2024, 4, 1),
        end=date(2024, 5, 1),
        context=context,
    )
    statements = data.fundamentals.income(
        codes=("000001.SZ",),
        report_start=date(2022, 1, 1),
        report_end=date(2024, 1, 1),
        context=context,
    )
```

建议的接口分组如下：

| Reader | 方法 | 主要返回类型 |
| --- | --- | --- |
| `QuoteReader` | `ticks`、`bars` | `QuoteEvent`、`BarResult` |
| `MarketReader` | `daily_bars` | `BarResult` |
| `MarketReader` | `daily_basic`、`moneyflow` | `DailyBasicRecord`、`MoneyflowRecord` |
| `MarketReader` | `adjustment_factors`、`limits`、`suspensions`、`stock_status` | `AdjFactorRecord`、`StockLimitRecord`、`SuspendRecord`、`StockStatusRecord` |
| `FundamentalReader` | `income`、`balance_sheets`、`cash_flows`、`indicators` | `IncomeStatement`、`BalanceSheet`、`CashFlowStatement`、`FinancialIndicator` |
| `FundamentalReader` | `forecasts`、`express_reports`、`audits` | `ForecastRecord`、`ExpressRecord`、`AuditRecord` |
| `FundamentalReader` | `qmt_financial` | `QmtFinancialRecord` |
| `CorporateActionReader` | `dividends`、`qmt_dividend_factors` | `DividendRecord`、`QmtDividendFactorRecord` |
| `ClassificationReader` | `sw_members` | 不含未来退出信息的 `SwMembership` |
| `CalendarReader` | `sessions`、`previous_session`、`session_bounds` | `Session` |

所有方法遵循以下约束：

- `context`/`as_of` 是仅限关键字的必填参数；
- `source` 必须显式选择，不在 Tushare/QMT 间静默回退或混合；
- 默认返回 `Iterator[T]` 或分批迭代器，避免回测一次把全市场多年记录实体化；
- 提供明确命名的 `fetch_all()` 便利方法和研究用 Arrow batch 方法，但二者使用完全相同的
  可见性规划器；
- 输出按业务时间、代码、版本键稳定排序；
- 空结果返回空迭代器，不通过降低可见性规则来“补数据”；
- 查询代码不得接受任意 SQL 片段，过滤参数全部绑定，避免策略绕过可见性谓词。

每次查询应附带或可查询以下审计信息：规范化后的决策时点、使用的数据集策略、源表、实际
可见截止日期/时间和 PIT 质量等级：

- `OBSERVED`：有 `received_at`，可精确重放系统实际收到的数据；
- `DATE_CONSERVATIVE`：只有源声明日期，统一延迟到下一本地日；
- `EFFECTIVE_ONLY`：只有生效日期，没有历史发布时间；
- `CONVENTIONAL_OPEN`：从最终历史 K 线投影出的开盘事件；
- `UNSAFE`：无法满足严格读取，公共 Reader 默认拒绝。

## Catalog 和 SQL 实现规划

现有 `*_as_of(DATE)` 使用包含当天的 `<=`，只适合原始研究，不符合新的开盘决策语义。
规划如下：

1. `DataCatalog` 保留原始视图注册；将可见性 SQL 收敛到内部、逐表登记的策略，而不是公开
   泛化的日期宏。
2. 日期级内部宏统一接收 `cutoff_date`，使用严格 `< cutoff_date`：
   财报只比较 `f_ann_date`，分红只比较 `imp_ann_date`，均不回退。
3. 版本表使用“先过滤、后 `row_number()`”的 SQL 形状，分区键由策略注册表明确声明。
4. Tick/bar 查询同时绑定 `received_at` 和事件区间；所有 UTC 微秒参数使用整数绑定，不在
   SQL 中依赖会话时区转换。
5. K 线 SQL 分成 `completed` 与 `current_open` 两个分支，最后映射为不同 Python 类型；
   不允许 `SELECT *` 后在 Python 中忘记清除当前区间的未来字段。
6. `sw_industry` 返回专用成员结构，SQL 不投影未来 `out_date` 和当前快照 `is_new`。
7. 原有 `*_as_of` 宏在 Reader 完成迁移后删除或改为明确的 `*_raw_until_eod` 名称，避免策略
   误用。测试和 `data-test` 同步迁移到 `DataReader`。

可见性策略注册表至少包含：源、表名、业务时间列、可见时间类型、版本业务键、版本排序、
允许返回的结构、PIT 质量和是否支持当前区间 open。新数据集没有完整注册信息时初始化失败。

## 分阶段实现计划

### 第一阶段：时间边界和失败关闭

- 实现 `TradingClock`、`ReadContext` 和带时区校验；
- 建立逐数据集 `AvailabilityPolicy` 注册表；
- 把日期级谓词从 `<= D` 改为 `< D`；
- 将现有 Catalog 宏标记为内部过渡接口；
- 为同日、前一日、周末公告、空可见日期建立边界测试。

### 第二阶段：源结构

- 生成并提交 `tushare_data.records`，增加 Arrow Schema 一致性测试；
- 增加 `qmt_receiver.records`；
- 补齐 QMT daily 的 `settlementPrice`；
- QMT financial 保留原始 index 和全部披露版本，定义兼容迁移或执行完整重拉；
- 为每种 `dataset` 建立到 `qmt_protocol.FinancialRecord` 的显式解码表。

### 第三阶段：分类 Reader

- 实现 `FundamentalReader`、`CorporateActionReader`、`ClassificationReader`；
- 再实现日终 `MarketReader`；
- 使用 DuckDB record batch 流式转换，添加稳定排序和批量代码过滤；
- 把数据复核工具迁移到相同的类型转换，但保留其“比较最新快照”用途与策略读取用途的区别。

### 第四阶段：行情与开盘事件

- 调整实时 bar 存储以保留区间版本或单独保存 open 事件；
- 实现完整 Tick、`OpenBar` 和 `CompletedBar`；
- 从不复权价格和当时可见因子实现 PIT 复权，拒绝直接读取预计算前复权历史；
- 为分钟、日、周等周期实现交易时段感知的 interval resolver；
- 回测撮合层增加事件序号，保证读取开盘价后的订单不能成交在同一开盘事件。

### 第五阶段：回测/实盘一致性

- 回测和实盘只依赖 `DataReader`，不直接持有 DuckDB connection；
- 使用同一组录制 tick/bar 分别驱动实时重放和历史回放，比较策略可见输入；
- 增加断线、迟到行情、事件时间晚于接收时间、重复 bar 更新和 Manifest refresh 测试；
- 度量全市场查询的批大小、峰值内存和 DuckDB 连接并发策略。一个 DuckDB connection 不在
  多个策略线程间无锁共享，每个执行线程使用独立 Reader/connection 或由上层串行调度。

## 验收条件

实现完成至少满足：

- 在 `as_of=date(D)` 和当日任意精确时间查询日期级非 K 线数据，都不会返回业务/公告日期
  为 `D` 的记录；
- 当日最终日线的 high、low、close、volume、amount 无法通过任何公共类型或 Arrow 入口泄漏；
- 当前 K 线返回类型没有 close/high/low 等属性；
- 过去 `as_of` 的复权价格不会使用该时点之后发生的除权因子；
- 已接收 Tick 的完整盘口可见，未接收或未来事件时间的 Tick 不可见；
- `f_ann_date`、`imp_ann_date` 为空时严格不可见，且不回退其他日期；
- 财务修订在修订实际公告日前返回旧版，公告后才返回新版；
- Reader 对未登记数据表和 `UNSAFE` 数据集失败关闭；
- 源 Schema 与记录结构逐字段一致；
- 回测事件测试证明使用开盘价生成的订单不会再次成交在该开盘价。

## 已知 PIT 边界

Tushare 当前存储不保存逐行 `observed_at`。如果供应商在同一公告日和同一版本键下静默改值，
旧值无法还原；日期延迟规则也无法修复这种源数据覆盖。严格 Reader 能避免已知日期导致的
未来函数，但不能把当前快照变成不存在的历史快照。

行业成员、交易日历、复权因子等只带生效日期的数据也有类似边界。需要完全精确的历史知识
状态时，必须从现在开始保存供应商采集时间和版本，或接入带真实发布时间的公告事件源。

## 当前低层用法

在 `DataReader` 完成前，Catalog 仍可用于数据核查：

```python
from data import DataCatalog

with DataCatalog(
    tushare_root="data/tushare",
    qmt_root="data/qmt",
) as catalog:
    raw = catalog.connection.execute(
        "SELECT * FROM tushare.cashflow WHERE ts_code = ?",
        ["000001.SZ"],
    ).fetch_arrow_table()
```

这段代码读取原始当前快照，不应直接放进回测或实盘策略。同步任务写入或整理文件后，对已存在
的 Catalog 调用 `refresh()`；新建对象会自动刷新，且只注册 Manifest 当前引用的文件。
