# `data`：回测数据可见性与读取接口

`data` 只解决两个问题：

1. 给定一个时间，当时能看到哪些数据；
2. 研究和回测如何通过同一个接口读取这些数据。

本文以 Tushare 为主要历史数据源。`DataCatalog` 只注册原始 Parquet 表；策略和回测只能使用
带 PIT 约束的 `DataReader`，不能直接访问原始表或 DuckDB connection。

当前 `DataCatalog` 的 `*_as_of` 宏是过渡实现，不代表本文定义的最终语义。

## 一条核心规则

每次读取都绑定一个带时区的具体时间 `as_of`：

```python
datetime(2024, 4, 30, 9, 25, tzinfo=ZoneInfo("Asia/Shanghai"))
```

任何数据只有满足下面的条件才能返回：

```text
visible_at <= as_of
```

公共接口不接受纯 `date`，也不默认使用 `datetime.now()`。纯日期无法区分盘前、开盘、盘中和
盘后。`start/end` 只表示查询的数据范围，不能代替 `as_of`。

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
| 历史分钟线 | `interval_end` | 只返回已完成 K 线 |
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
| `fina_indicator` | `next_session(ann_date)` 09:25 | Tushare 加工数据，质量低于原始报表 |
| 完整 `dividend` 实施记录 | `next_session(imp_ann_date)` 09:25 | 不用预案 `ann_date` 替补 |
| `sw_industry` 成员 | `in_date` 当日 09:25 | 只返回当前行业，不返回未来退出信息 |
| `trade_cal` | 不走普通 PIT 规则 | 作为回测引擎配置 |

D 表示记录的 `trade_date`、公告日期或生效日期。可见日期为空时，该记录不可见，不回退到其他
日期字段。

## 几个容易出错的地方

### 分钟线和日线

分钟线统一转换为半开区间 `[interval_start, interval_end)`，到 `interval_end` 才进入
`history()`。Tushare 的 `trade_time` 必须在导入时验证并转换成区间结束时间，不能由 Reader
临时猜测。

日线分成两个可见事件：

- D 日 09:30 后，`current()` 可以返回当日 `open`；
- D 日 16:05 后，`history(frequency="1d")` 才能返回完整日线。

看到某根 K 线或开盘价后生成的订单，只能参加后续撮合，不能成交在已经看完的 K 线或同一个
开盘事件中。

### 停牌、涨跌停和缺失行情

停牌不是下一交易日才知道：D 日全日停牌在 D 日盘前可见，撮合器在 D 日禁止成交。日内停牌
只有到停牌开始后才对策略可见。

策略读取当前状态，而不是整张原始停牌表：

```python
data.is_suspended("000001.SZ")
data.price_limits("000001.SZ")
data.stock_status("000001.SZ")
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

不能先选择今天看到的最终版本，再向过去过滤。重要财务因子优先从 PIT 三张报表计算，避免
直接依赖可能被 Tushare 重新计算的 `fina_indicator`。

### 分红、复权和行业

严格 Reader 默认只返回 `imp_ann_date` 已公布的完整分红实施记录。若以后需要研究预案，应增加
独立的预案接口，只返回预案阶段已经知道的字段，不能提前返回后来补入的登记日和除权日。

价格以不复权日线为源。Reader 只能使用 `visible_at <= as_of` 的 `adj_factor` 现场复权，禁止
直接使用以今天最新因子生成的前复权历史。

行业成员可以用 `in_date <= D < out_date` 计算 D 日有效分类，但返回结果不能包含未来
`out_date` 或当前快照的 `is_new`。

### 交易日历

交易日历用于驱动回测 session、处理节假日并计算 `next_session`，属于引擎配置，不作为普通
alpha 数据逐日解锁。策略只读取当前和历史 session；若要精确还原临时休市变更，需要另存
日历公告时间和版本。

## Tushare 数据质量底线

可见时间只能防止日期已知的未来函数，不能恢复 Tushare 已经覆盖的旧值。同步层必须从现在
开始保留同一业务键的内容版本：

```text
dataset, business_key, payload_hash, first_seen_at, batch_id, is_backfill, payload
```

- 首次全量历史下载标记为 `is_backfill=true`，下载时间不能冒充历史可见时间；
- 后续发现同一业务键内容变化时保留新版本，不能覆盖旧版本；
- 首次采集以前发生的静默修订无法恢复，只能标记为近似 PIT；
- 回测启动时固定 `snapshot_id` 和 `policy_version`，运行中不自动刷新数据；
- 未登记可见性规则的新表，严格 Reader 拒绝读取。

股票池也必须按历史时点构造。不能使用今天仍上市的股票列表回测过去；需要结合历史股票列表、
上市/退市区间和当日交易状态，避免幸存者偏差。

空值和缺失数据统一失败关闭：不自动补零、不自动前向填充、不自动换表，也不静默切换数据源。

## 读取接口

### 研究

研究代码显式指定时间：

```python
from datetime import datetime
from zoneinfo import ZoneInfo

from data import DataCatalog, DataReader


SHANGHAI = ZoneInfo("Asia/Shanghai")

with DataCatalog(
    tushare_root="data/tushare",
    qmt_root="data/qmt",
) as catalog:
    data = DataReader(catalog).at(
        datetime(2024, 4, 30, 9, 25, tzinfo=SHANGHAI)
    )
    bars = data.history(
        symbols=("000001.SZ",),
        frequency="1d",
        count=20,
    )
    income = data.latest("income", symbols=("000001.SZ",))
```

初始接口只需要：

| 方法 | 返回内容 |
| --- | --- |
| `history(...)` | 已完成且可见的 K 线 |
| `current(...)` | 当前已发生的市场状态，例如当日开盘价 |
| `latest(dataset, ...)` | 财报、指标、行业等最新可见版本 |
| `events(dataset, ...)` | 截至 `as_of` 已可见的公告和公司行动 |
| `is_suspended(...)` | 当前是否停牌 |
| `price_limits(...)` | 当前涨跌停价格 |

所有方法共享同一个 `as_of`，批量和 DataFrame/Arrow 返回也必须经过同一套可见性过滤。

### 回测

回测引擎把 Reader 绑定到模拟时钟：

```python
def on_event(context, data):
    assert data.as_of == context.now
    bars = data.history(
        symbols=context.universe,
        frequency="1d",
        count=20,
    )
```

策略不能调用 `data.at()` 或修改 `data.as_of`：

```text
研究：调用方提供 as_of
回测：引擎提供 clock.now
实盘：引擎提供真实当前时间
```

三者使用相同的 `visible_at <= as_of` 规则。

## 实现与验收

`DataCatalog` 只负责文件和 DuckDB 生命周期；`DataReader` 负责计算 `visible_at`、过滤可见版本、
选择修订版本、隐藏未来字段并记录数据快照。策略代码不得持有 `catalog.connection`。

最重要的验收性质是：

> 向数据集中加入任意 `visible_at > T` 的记录或修订，时点 T 的所有查询结果必须完全不变。

此外至少验证：

- 分钟线在结束前不可见；
- 当日日线最终字段在 16:05 前不可见；
- 看见开盘价后的订单不能成交在同一个开盘事件；
- D 日停牌证券在 D 日不能成交；
- 财报先过滤可见版本，再选择最新修订；
- 复权不使用 `as_of` 之后的因子；
- 行业结果不泄漏未来退出日期；
- 缺失日线不会自动变成停牌或零成交 bar；
- 回测运行期间新增数据不会改变固定快照的结果。
