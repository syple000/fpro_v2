# `data`：研究、回测与实盘的数据读取接口

`data` 只解决两个问题：

1. 给定一个时间，当时能看到哪些数据；
2. 研究、回测和实盘如何通过同一个接口读取 Tushare/QMT 数据。

`DataCatalog` 注册 Tushare 历史数据和 QMT 历史/实时数据，`DataReader` 通过内置适配器提供
统一业务语义；策略只能使用
带 PIT 约束的 `DataReader`，不能直接访问原始表、行情连接或 DuckDB connection。

`DataCatalog` 只负责把 Manifest 当前引用的 Parquet 文件注册成原始物理视图，不提供另一套
`*_as_of` 查询入口；
PIT 语义统一由 adapter 和 `DataReader` 实现。

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
数据来自 Tushare 还是 QMT，也不需要理解源表名、版本选择或 SQL。

## 一条核心规则

每次读取都绑定一个带时区的具体时间 `as_of`：

```python
datetime(2024, 4, 30, 9, 25, tzinfo=ZoneInfo("Asia/Shanghai"))
```

任何通过 `DataReader` 返回的数据都属于 PIT 数据，只有满足下面的条件才能返回：

```text
visible_at <= as_of
```

`as_of` 不接受纯 `date`，也不默认使用 `datetime.now()`。纯日期无法区分盘前、开盘、盘中和
盘后。`start/end` 可以使用接口规定的 `date` 或 `datetime`，但只表示数据范围，不能代替
`as_of`。

研究、回测和实盘使用同一套规则。研究由调用方指定 `as_of`，回测由模拟时钟提供，实盘由
真实时钟提供；策略不能自行指定或修改引擎传入的时间。

## 默认时间约定

以下时间均为 `Asia/Shanghai`：

```text
PRE_OPEN_READY    09:25
MARKET_OPEN       09:30
DAILY_READY       16:05
DAILY_BASIC_READY 17:05
```

这些是 Tushare 内置适配器使用的时间边界。QMT 实时适配器使用 `received_at`，不同逻辑数据集
各自在适配器中实现明确的可见时间规则。

`next_session(D)` 表示严格晚于 D 的第一个交易日。周五公告的数据通常从下周一 09:25 可见。
该计算必须命中已发布交易日历；空日历或日历覆盖止于 D 时不回退为“下一个工作日”，而是抛出
`DataSourceUnavailableError`，避免把节假日误当成交易日。

## 内置数据源的可见约束

| 数据集 | `visible_at` | 备注 |
| --- | --- | --- |
| QMT tick | `received_at` | 使用本系统实际收到该事件的时间，不使用行情自带时间冒充可见时间 |
| QMT 实时 bar | `max(interval_end, received_at)` | 只返回本系统已经收到的完整 K 线 |
| 历史分钟线 | `interval_end` | 由适配器登记其区间语义 |
| `daily.open` | 交易日 D 09:30 | 作为开盘事件，不作为完整日线 |
| 完整 `daily` | D 16:05 | 此前禁止返回当日最终 high/low/close/volume/amount |
| `daily_basic` | D 17:05 | 属于日终数据 |
| `moneyflow` | `next_session(D)` 09:25 | 内置策略统一在下一次盘前开放 |
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
只有到停牌开始后才对策略可见；已知停牌区间结束后返回 `False`。非空但无法解析的停牌时段是
源数据错误，不能静默返回未知状态。

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
适配器在 Tushare 原始表中保留普通合并报表（report_type=1）和调整后合并报表（report_type=4）
排除调整前历史版本（report_type=5），再按 ts_code、end_date、comp_type 选择最新版本
```

相同公告日期下优先调整后合并报表。

不能先选择今天看到的最终版本，再向过去过滤。需要统一计算口径的重要财务因子从 PIT 三张
报表计算；需要 Tushare 指标口径时直接使用 `fina_indicator`。

### 分红、复权和行业

严格 Reader 默认只返回 `imp_ann_date` 已公布的完整分红实施记录。若以后需要研究预案，应增加
独立的预案接口，只返回预案阶段已经知道的字段，不能提前返回后来补入的登记日和除权日。

`adjustment="forward"` 表示平台统一的前复权输出语义，不规定数据源必须采用哪一种计算方式。
无论由哪个数据源实现，它都与其他查询一样遵守 PIT 规则，只能使用 `as_of` 时已经可见的数据。

Tushare 适配器在内部组合不复权日线与 `as_of` 当时可见的 `adj_factor` 现场计算：

```text
adjusted_price(t) = raw_price(t) * factor(t) / anchor_factor(as_of)
```

日线、当日因子和锚点因子在同一条 DuckDB 查询中完成关联，不生成中间 Arrow 表；任何行情缺少
当日因子、锚点因子或锚点为零时，整次查询明确失败。

QMT 原生 `front/front_ratio` 的价格锚点取决于查询区间，但当前历史表没有记录锚点、因子版本或
因子的历史可见时间，因此不能满足上述 PIT 语义。QMT 同步只新增 `adjustment="none"` 的原始
行情；已有 `front/front_ratio` 分区可保留用于离线复核，但 Reader 不读取它们，QMT 路由收到
`adjustment="forward"` 时明确抛出 `DataCapabilityNotSupportedError`。只有在存储补齐原始价格、
因子、因子生效日和可见时间后，才能重新启用 QMT 前复权。

> TODO：补充 QMT 前复权能力。生产数据继续只落地 `adjustment="none"` 的日线，并保存带版本和
> 可见时间的 QMT 除权因子；由 Adapter 按统一公式从不复权价格现场计算前复权价格，通过
> `market.bars(adjustment="forward")` 透出。同步或数据复核流程另行拉取同一股票、区间和锚点的
> QMT 原生 `front_ratio`，逐项校验计算结果的 OHLC 与前收盘价；成交量和成交额只校验保持实际
> 成交口径。原生 `front_ratio` 仅作为校验基准，不与生产日线增量拼接，也不替代因子版本的 PIT
> 约束。

Reader 只把 `adjustment` 语义传给当前行情适配器，不读取因子、不拼接数据源，也不要求配置
`corporate_actions.adjustment_factors` 路由。
适配器必须把结果归一到平台 bar Schema：只调整 OHLC 和前收盘价，
`volume/amount` 保留平台规定的实际成交口径。数据源不能完整满足该语义时，Reader 明确报
“数据源不支持前复权”，不能返回一张混合了复权和未复权行的表，也不能静默切换来源。

行业成员可以用 `in_date <= D < out_date` 计算 D 日有效分类，但返回结果不能包含未来
`out_date` 或当前数据中的 `is_new`。

### 交易日历

交易日历用于驱动回测 session、处理节假日并计算 `next_session`，属于引擎配置，不作为普通
alpha 数据逐日解锁。策略只读取当前和历史 session；若要精确还原临时休市变更，需要另存
日历公告时间和版本。凡是查询需要计算 `next_session`，日历必须至少覆盖到相关日期之后的第一
个开市日；覆盖不足属于来源不可用，不使用工作日近似。

## 已发布数据使用前提

进入任一已注册来源后，数据视为已经由上游完成采集、清洗、校验和定版，是系统
可以直接使用的自有数据。`DataReader` 不在查询时重新评估或质疑数据质量，不做跨源复核，不因
首次采集时间或供应商身份降级结果，也不向策略返回 `exact/source_declared/approximate` 等质量
标签或相关警告。

每个数据源适配器登记的公告日、生效日、交易日、接收时间和版本字段，是本系统构造 PIT 视图
的权威输入。Reader 按登记规则计算 `visible_at`、选择当时可见版本并直接返回结果，不猜测字段
是否可靠，也不因为存在其他数据源而拒绝使用。数据异常和跨源核验属于数据发布前的上游流程；
数据发布后，研究、回测、实盘和策略统一信任并使用它。

这里仍需保证的是查询语义和运行一致性，而不是再次审查数据质量：

- `DataReader.at(as_of)` 只固定 PIT 时间，不复制文件，也不另开数据库连接；
- `DataReader` 复用 `DataCatalog` 的单一连接，只有显式调用 `catalog.refresh()` 才重新加载 Manifest；
- 未登记可见性规则的新表，严格 Reader 拒绝读取；
- 空值或缺失不自动补零、前向填充、换表或切换数据源。

股票池也必须按历史时点构造。不能使用今天仍上市的股票列表回测过去；需要结合历史股票列表、
上市/退市区间和当日交易状态，避免幸存者偏差。

需要可复现回测时，调用方应在运行期间不调用 `catalog.refresh()`，并自行保留对应的 Manifest
和 Parquet 文件版本。当前 Reader 不提供物理文件归档或历史版本恢复。

## 公共接口设计

成熟量化系统通常在内部统一处理历史请求，在策略侧按业务语义拆分接口：

- [Zipline DataPortal](https://zipline.ml4trading.io/_modules/zipline/data/data_portal.html)
  使用资产、结束时间、bar 数量、频率和字段定义历史窗口；
- [QuantConnect History](https://www.quantconnect.com/docs/v2/writing-algorithms/historical-data/history-requests)
  支持“最近 N 个样本”和“起止时间”两种查询，并用明确的数据类型区分行情、基本面和公司行动；
- [RQAlpha DataSource](https://github.com/ricequant/rqalpha/blob/master/rqalpha/data/base_data_source/data_source.py)
  把 `history_bars`、`current_snapshot` 和基本面读取分开。

本项目采用相同思路：公开接口按业务拆分，`DataReader` 出口统一为平台数据模型，底层由 PIT
规则和 Tushare/QMT 内置适配器完成读取。

```text
策略 / 研究 / 回测 / 实盘
          |
DataReader.at(as_of) -> DataView -> 平台统一 Schema
          |
PIT 规则 + SourceConfig 路由
          |
          ├── TushareAdapter
          └── QmtAdapter
```

不公开 `query("tushare_table", ...)` 形式的万能策略接口。策略不需要知道源表名、可见日期列、
版本键或 SQL；这些信息由内部数据集注册表管理。

### 平台统一数据模型

`DataReader` 是数据抽象的唯一出口。源数据在进入 Reader 结果前必须转换为平台定义的公共
Schema；供应商原生结构不能穿透这个边界。例如 Tushare 的 `ts_code`、QMT 的 `code`、
复权标记和嵌套 `quote` 都由适配器边界处理，策略只看到平台的
`symbol/interval_start/interval_end/open/high/low/close/volume/amount` 等字段。

平台模型统一规定：

- 字段名称、Arrow 类型、可空性和身份键；
- 证券代码、时区、时间区间和交易日口径；
- 价格、成交量、成交额等单位以及复权后的字段含义；
- PIT 可见性、版本选择、默认排序、截断和空结果语义。

单位采用平台全局约定：

- 金额统一为人民币元，价格和每股金额为人民币元/股；
- 成交量、股本统一为股；送转比例表示每一原有股对应的新增股数；
- 百分比、收益率、利润率和增长率统一使用小数，`0.125` 表示 `12.5%`；
- PE、PB、周转率等倍数直接保留倍数值，`15.0` 表示 15 倍；
- 复权因子是无量纲相对值，只有同一序列内的相对关系有意义。

Reader 在统一入口根据 `CAPABILITY_SCHEMAS` 校验字段名称、顺序、类型和可空性；单位正确性由
适配器换算和对应数值测试保证。

公共 Schema 由 platform 集中定义和版本化，适配器只能提供字段映射，不能自行增加、删除或改变
公共字段。新增供应商专属字段必须先提升为平台业务字段并更新 Schema 版本，不能只对某一个
数据源开放。

同一个公共请求在 Tushare/QMT 间切换后，返回的字段集合、类型、单位、排序和业务含义必须保持
不变；允许
变化的只有来源本身提供的数值。`QueryResult.sources` 只用于平台审计和排障，不能改变公共表
结构，也不能成为策略分支条件。

### 内置数据源适配器

当前只有 `TushareAdapter` 和 `QmtAdapter`。两者以固定 `source_id` 注册，并声明能够实现的逻辑
能力，例如不复权日线、前复权日线、分钟线、实时行情或财务报表。适配器负责：

- 将原生表、文件或实时消息转换为平台内部标准记录；
- 提供该来源的 `visible_at` 规则；
- 用原生数据或派生计算实现请求的业务语义；
- 在能力不支持时明确失败，不向 Reader 返回半成品或供应商专属字段。

适配器直接返回符合平台 Schema 的 `pyarrow.Table`，最终 `QueryResult` 由 `DataReader` 构造。
返回表包含该能力 Schema 规定的 PIT 字段和版本键；Reader 会严格校验字段、类型、顺序与
可空性。等确实需要第三个数据源时，再从两个现有实现中提取公共扩展接口。

### 按逻辑数据集配置来源

`SourceConfig.routes` 是“平台逻辑数据集 -> source_id”的不可变映射。路由粒度不能停留在笼统的
`market` 或 `fundamentals`，每一类可独立读取或组合的数据都有自己的稳定键：

| 路由键 | 对应数据或接口 |
| --- | --- |
| `market.daily_bars` | `market.bars(frequency="1d")` |
| `market.intraday_bars` | `market.bars()` 的分钟周期 |
| `market.realtime_quotes` | `market.current()` |
| `market.daily_metrics` | `market.daily_metrics()` |
| `market.moneyflow` | `market.moneyflow()` |
| `market.suspensions` | `market.status()` 的停牌字段 |
| `market.price_limits` | `market.status()` 的涨跌停字段 |
| `market.st_status` | `market.status()` 的 ST 字段 |
| `fundamentals.income` | 利润表 |
| `fundamentals.balance_sheet` | 资产负债表 |
| `fundamentals.cashflow` | 现金流量表 |
| `fundamentals.indicators` | 财务指标 |
| `fundamentals.forecast`、`fundamentals.express`、`fundamentals.audit` | 业绩预告、快报和审计意见 |
| `corporate_actions.dividends` | 分红实施记录 |
| `corporate_actions.adjustment_factors` | 复权因子 |
| `classification.industry` | 行业分类和成员 |
| `calendar.sessions` | 交易日历 |

Reader 创建时校验所有已配置的 `source_id` 是否注册并支持对应基础数据集；每次查询再校验频率、
复权方式和字段组合等具体能力。配置允许只包含当前运行需要的数据集，以便构造轻量 Reader；
但是没有配置的路由没有默认来源，也不会从相邻类别推断来源或自动回退。每个路由只能绑定一个
`source_id`，不接受候选列表；未知路由键、重复路由或空 `source_id` 在构造配置时直接报错。

错误语义必须区分清楚：

- 请求依赖的路由未配置：抛出 `DataSourceNotConfiguredError`；
- 路由已配置，但适配器不支持请求能力且平台也无法用已配置依赖完成派生：抛出
  `DataCapabilityNotSupportedError`；
- 已发布数据的存储当前不可访问，或计算可见时间所需的交易日历覆盖不足：抛出
  `DataSourceUnavailableError`；
- 已发布数据正常可读，只是指定证券或时间范围内没有记录：返回符合平台 Schema 的空
  `QueryResult`。

数据源在 Reader 创建时绑定，策略不能逐次选源。一个公共请求依赖多个逻辑数据集时，Reader
只解析本次确实需要的路由：例如 `market.status(fields=("suspended",))` 只要求
`market.suspensions`。`market.bars()` 无论是否复权都只解析对应的日线或分钟线路由；Tushare
的 `daily + adj_factor` 是当前严格 PIT 前复权实现，QMT 对该选项明确报告能力不支持。
多路由组合结果的 `QueryResult.sources` 记录本次实际使用的全部 `source_id`，但公共表 Schema
不随来源数量变化。

### 创建 PIT 时间视图

研究代码先创建一个时间视图，同一次计算中的所有读取共享同一个 `as_of`：

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
            routes={
                "market.daily_bars": "qmt",
                "market.intraday_bars": "qmt",
                "market.realtime_quotes": "qmt",
                "market.suspensions": "tushare",
                "market.price_limits": "tushare",
                "market.st_status": "tushare",
                "fundamentals.income": "tushare",
                "fundamentals.balance_sheet": "tushare",
                "fundamentals.cashflow": "tushare",
                "calendar.sessions": "tushare",
            },
        ),
    )
    data = reader.at(
        datetime(2024, 4, 30, 9, 25, tzinfo=SHANGHAI)
    )
```

`DataReader.at()` 拒绝无时区 `datetime`，并把其他时区的 aware datetime 归一到
`Asia/Shanghai`。`at()` 不打开来源快照，也不需要关闭；它只是把时间和当前 Catalog 连接绑定到
领域接口。示例只配置需要的数据集；调用未配置的数据集会抛出
`DataSourceNotConfiguredError`。

当前内置适配器与公共接口的关系如下：

| 公共接口 | 内置实现 |
| --- | --- |
| `market.bars(frequency="1d", adjustment="none")` | Tushare `daily`；或 QMT `daily(adjustment="none")` |
| `market.bars(frequency="1d", adjustment="forward")` | Tushare `daily + adj_factor` 现场计算；QMT 当前明确不支持 |
| `market.bars()` 不复权分钟周期 | QMT 同步的 `intraday(adjustment="none")` 与已经实时接收的原始 `bars`；`tushare_data` 尚无分钟表 |
| `market.bars()` 前复权分钟周期 | 当前没有满足严格 PIT 语义的内置实现 |
| `market.current()` | QMT 当日 tick；无实时源时仅提供 Tushare 日线的开盘事件 |
| `market.daily_metrics()` | `tushare.daily_basic` |
| `market.moneyflow()` | `tushare.moneyflow` |
| `market.status()` | `tushare.suspend_d`、`stk_limit`、`stock_st` |
| `fundamentals.statements()` | `tushare.income`、`balancesheet`、`cashflow` |
| `fundamentals.indicators()` | `tushare.fina_indicator` |
| `fundamentals.disclosures()` | `tushare.forecast`、`express`、`fina_audit` |
| `corporate_actions.dividends()` | `tushare.dividend` |
| `classification.industry()` | `tushare.sw_industry` |
| `calendar.*` | `tushare.trade_cal` |

`qmt.financial` 和 `qmt.dividend_factors` 当前用于交叉验证，不作为第一版策略公共接口的数据源；
它们以后可以在完成平台 Schema 映射和可见性规则登记后成为对应能力的实现。QMT 目前只向
Reader 提供不复权历史行情和当日 tick；Tushare 适配器在内部使用 `daily + adj_factor` 实现
严格 PIT 前复权。

### 公共参数

所有复数查询都使用仅限关键字参数，并共享以下约束：

- `symbols` 必须显式传入；空序列返回空结果，不解释为全市场；
- 全市场必须显式传 `ALL_SYMBOLS`，并同时提供 `limit` 或使用批量迭代接口；
- 重复或格式错误的证券代码直接报错；
- `fields=None` 表示该接口的全部公共字段；非空时只接受登记字段，不接受表达式或 SQL；
- 业务时间的 `start/end` 和报告范围均为左闭右开 `[start, end)`；
- `order` 只接受 `"asc"` 或 `"desc"`，不能传任意排序字段；
- `limit` 默认 `None`；非空时必须是正整数且不超过配置上限，布尔值不视为整数；
- 路由已正确配置但筛选范围没有记录时返回空行或空表，不自动补数、换源或降低 PIT 规则；
- 路由未配置、来源不可用或能力不支持时抛出对应数据源错误，不解释为空数据。

每类结果的身份字段始终返回，不能被 `fields` 删除。例如 bar 始终包含
`symbol/interval_start/interval_end`，财报始终包含
`symbol/period_end/visible_at/announcement_date/actual_announcement_date/company_type`。

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
- `adjustment` 描述平台输出语义，不指定底层算法；所选适配器必须声明支持该语义；
- `count=N` 时先对每个 symbol 选出最新 N 根，再按请求的输出顺序排序；
- `limit` 可以与 `count` 同时使用，但它会截断合并结果，可能使某些股票不足 N 根。

查询条件会直接交给 DuckDB：

- 范围模式在 SQL 中直接应用 symbol、业务时间和 PIT 条件，由 DuckDB 对 Parquet 执行字段与过滤下推；
- `count=N` 在 SQL 中按 symbol 执行窗口截断，不再把全部结果返回 Arrow 后由 Reader 计数；
- `fields` 会下推为 DuckDB/Parquet 字段投影，`limit` 在不影响每 symbol 计数语义时下推为
  `limit + 1`，额外一行用于设置 `truncated`；
- 日指标、资金流、实时状态、财报 `periods`、复权因子和交易日历使用相同的 SQL 下推原则。

Catalog 只根据 Manifest 注册当前有效的 Parquet 文件，不参与查询范围规划。`count` 和
`periods` 查询为了保证精确语义，可能需要扫描历史候选行，但窗口计算会在 DuckDB 内完成。

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

每个 symbol 最多返回一行，固定按 `symbol ASC` 排序。没有实时源时不伪造盘中 `last`；QMT
只从 `as_of` 当日的 tick 中选择最新接收事件，不用分钟 bar 覆盖 tick，也不跨交易日返回上一
交易日快照。Tushare 最终日线只能按前文规则提供已经发生的开盘事件。

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

行情价格为人民币元/股，成交量为股，成交额为人民币元。每日指标的
`turnover_rate/turnover_rate_f/dv_ratio/dv_ttm` 使用小数；例如源数据的 `2.5%` 统一返回
`0.025`。`pe/pb/ps/volume_ratio` 等保留倍数口径。

## 财务接口 `fundamentals`

财务公共字段只由 `models/financial.py` 定义。Reader 不认识 `ts_code`、`f_ann_date`、
`free_cashflow` 等供应商原始字段；每个适配器必须先把它们映射为平台 Schema，Reader 再严格校验
字段名、顺序、Arrow 类型和非空约束。新增供应商不修改 `models` 或研究层调用。

平台财务单位固定为：金额使用人民币元，每股金额使用人民币元/股，百分比来源的利润率、回报率
和同比变化使用小数（例如 12.5% 返回 `0.125`）；流动比率和周转率等使用倍数。资产负债表的
`share_capital` 是股，不是人民币金额。供应商使用万元或百分数值时由适配器转换。原始公告日与
PIT 可见时间分开保存为 `announcement_date` 和 `visible_at`；`period_end` 表示报告期末。

### 三张报表 `fundamentals.statements()`

```python
income = data.fundamentals.statements(
    kind="income",
    symbols=("000001.SZ", "600000.SH"),
    report_start=date(2021, 1, 1),
    report_end=date(2024, 4, 1),
    periods=None,
    company_type="industrial",
    fields=("operating_revenue", "operating_profit", "net_income"),
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
    fields=("operating_revenue", "net_income_attributable_to_parent"),
    order="desc",
)
```

平台只公开合并口径报表，读取普通合并和后续调整后合并版本；Tushare 的
`report_type/end_type` 等源编码不进入公共 Schema。资产负债表中的 `money_cap` 映射为
`monetary_funds`（货币资金），不误称为现金及现金等价物。
`periods` 与 `report_start/report_end` 互斥；范围模式至少传 `report_start`，`report_end` 默认不设
上界。`periods` 是每只股票最近 N 个不同的 `period_end`，选完报告期后再返回符合
`company_type` 的行。`company_type` 使用平台枚举 `industrial/bank/insurance/securities`；`limit`
仍是合并结果的全局上限。默认排序为：

```text
period_end DESC, symbol ASC, company_type ASC
```

`order="asc"` 只反转 `period_end`。在 SQL 中必须先过滤可见修订，再选每个业务键的最新版，最后
应用用户字段过滤和 `limit`。

### 财务指标 `fundamentals.indicators()`

```python
indicators = data.fundamentals.indicators(
    symbols=("000001.SZ",),
    periods=8,
    fields=("basic_earnings_per_share", "return_on_equity", "gross_margin"),
    order="desc",
    limit=100,
)
```

参数与报表类似，默认按 `period_end DESC, symbol ASC` 排序。结果始终使用平台财务指标 Schema；
当前内置实现来自 Tushare，来源信息只保留在 `QueryResult.sources` 审计元数据中。需要统一计算
口径的指标由平台基于 PIT 三张报表计算，需要数据源既定指标口径时由所选适配器提供。

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
visible_at ASC, symbol ASC, period_end ASC, announcement_date ASC
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
返回类型不包含未来 `out_date` 和源表状态字段 `is_new`。

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
    sources: tuple[str, ...]
    truncated: bool

    def to_pandas(self) -> pandas.DataFrame: ...
```

`DataReader` 在构造 `QueryResult` 前按平台公共 Schema 做严格校验；缺少字段、多出供应商字段、
类型或单位无法归一时都视为适配器错误，不把不一致结果交给调用方。

各接口的默认排序汇总如下：

| 接口 | 默认排序 |
| --- | --- |
| `market.bars()` | `interval_end ASC, symbol ASC, interval_start ASC` |
| `market.current/status()` | `symbol ASC` |
| `market.daily_metrics/moneyflow()` | `trade_date ASC, symbol ASC` |
| `fundamentals.statements()` | `period_end DESC, symbol ASC, company_type ASC` |
| `fundamentals.indicators()` | `period_end DESC, symbol ASC` |
| `fundamentals.disclosures()` | `visible_at ASC, symbol ASC, period_end ASC, announcement_date ASC` |
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
5. 最后返回前 `limit` 行；存在更多结果时设置 `truncated=True`。

`limit=None` 表示不做逻辑截断，不允许内部静默使用默认 limit。大结果可以通过
`iter_batches(batch_size=...)` 分批消费；当前不提供 `offset` 或跨请求分页游标。

## 回测与实盘接口

回测引擎从同一个 `DataReader` 创建绑定模拟时钟的 PIT 时间视图：

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
研究：调用方通过 DataReader.at(as_of) 创建时间视图
回测：引擎使用 clock.now 创建时间视图
实盘：引擎使用真实当前时间创建时间视图
```

三者复用完全相同的领域接口、平台 Schema 和 PIT 查询规划器。回测与实盘可以绑定不同的
`SourceConfig`，但传给策略的 `data` 对象不能因此改变类型或字段语义。

## 内部查询模型

Reader 不把不同查询强行压进统一的 `AdapterRequest` 或无类型的 `parameters` 字典。
它在完成公共参数校验后，直接调用来源适配器的具名能力方法：

```text
market.bars(frequency="1d")
-> TushareAdapter.daily_bars(
       as_of=...,
       symbols=...,
       start=...,
       end=...,
       count=...,
       adjustment=...,
       order=...,
   )

fundamentals.statements(kind="cash_flow")
-> TushareAdapter.cash_flow_statements(
       as_of=...,
       symbols=...,
       report_start=...,
       report_end=...,
       periods=...,
       company_type=...,
       order=...,
   )
```

不同能力只声明自己真正需要的参数。例如 Tushare 日线没有 `frequency` 参数，
`previous_session()` 也不会通过 `sessions(open_only=True)` 复用入口。统一保留在有实际价值的
边界：Adapter 返回 Arrow 表，Reader 按逻辑数据集校验平台 Schema 并包装为 `QueryResult`。

执行顺序固定为：

```text
根据请求解析主逻辑数据集
-> 从 SourceConfig 解析主 source_id
-> 调用对应的 TushareAdapter 或 QmtAdapter
-> 适配器按 symbols/业务范围读取候选数据
-> 过滤 visible_at <= as_of
-> 在业务键内选择最新可见版本
-> 归一并校验平台公共 Schema
-> 应用字段投影
-> 稳定排序
-> limit
-> QueryResult
```

身份和业务范围过滤可以安全地下推到版本选择之前；依赖 payload 值的过滤必须在版本选择之后。
否则最新修订不符合条件时，查询可能错误地重新返回符合条件的旧版本。

`DataCatalog` 负责物理数据和唯一的 DuckDB 连接；为避免每次查询重新打开数千个小文件，
`trade_cal` 和 `sw_industry` 会在 `refresh()` 时同步物化到私有的 `data_internal` schema，
原始 `tushare` 视图仍保持不变。内置适配器负责来源接入和标准化；
`DataReader` 及其领域 Reader 负责来源路由、PIT、版本、平台 Schema 和排序。策略代码不得持有
`catalog.connection` 或数据源客户端。运行期间需要加载新 Manifest 时由外围流程显式调用
`catalog.refresh()`。

## 验收条件

最重要的性质是：

> 向数据集中加入任意 `visible_at > T` 的记录或修订，时点 T 的所有查询结果必须完全不变。

此外至少验证：

- 同一个时间视图的所有公共方法使用同一个 `as_of`；
- 每个逻辑数据集都能独立绑定当前支持的 Tushare 或 QMT，组合查询只读取本次需要的路由；
- 路由未配置、能力不支持、来源不可用和合法空结果分别产生约定的不同结果；
- 同一公共请求切换 Tushare/QMT 后，结果 Schema、类型、单位和排序完全一致；
- Tushare 前复权只使用 `as_of` 时可见的因子，QMT 在缺少因子可见性元数据时明确拒绝前复权；
- QMT 当前行情只使用当日 tick，分钟 bar 不得以不同聚合口径覆盖快照；
- 数据源缺少请求能力时明确失败，不返回供应商字段、不生成半成品且不静默换源；
- 适配器返回缺列、多列或错误类型时在 Reader 边界失败，不能生成 `QueryResult`；
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
- 不调用 `catalog.refresh()` 时，Reader 持续使用当前已注册的 Manifest 文件集合。
