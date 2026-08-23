# QMT 数据协议

`qmt_protocol` 是 qmt-agent 与 qmt-receiver 共同依赖的结构包。协议只做两件事：准确描述 XtData 数据，以及描述中转所需的少量信封字段。这里没有 `XtDataFrame`、`TabularFrame` 或 `JsonValue`。

## 文件组织

| 文件 | 内容 |
| --- | --- |
| `base.py` | XtData 原始行、行情信封、类型别名和基础校验 |
| `requests.py` | HTTP 请求结构和请求参数校验 |
| `responses.py` | HTTP 返回结构及响应内部一致性校验 |

`qmt_agent` 和 `qmt_receiver` 不再各自定义协议模型。公共类型统一从 `qmt_protocol` 导入。

## 数据边界

```text
XtData dict / DataFrame
        │ 原字段名、原标量类型、原时间值
        ▼
qmt_protocol 具体模型
        │ HTTP JSON
        ▼
receiver 解析并派生存储字段
```

协议遵循以下规则：

- 已知字段逐一声明，未知字段拒绝，不静默丢弃。
- XtData 的字段名不改名，包括历史 K 线的 `settelementPrice` 拼写。
- agent 不修改 XtData `time` 的单位和数值类型。
- DataFrame 不作为通用二维结构传输，而是转为具体的行模型列表；原 index 保存为明确字段。
- 可缺失的 XtData 字段使用 `None`，不是任意 JSON 值。

这些结构根据当前东北证券 miniQMT 实机返回采样定义。若客户端升级后增加字段，接入边界会明确报错，便于同步更新协议。

## 基础模型

### 实时行情

`TickQuote` 对应 `get_full_tick`、全推和 tick 订阅。字段包括：

```text
time, stime, timetag, lastPrice, open, high, low, lastClose,
amount, volume, pvolume, stockStatus, openInt, transactionNum,
lastSettlementPrice, settlementPrice, pe,
askPrice, bidPrice, askVol, bidVol, volRatio, speed1Min, speed5Min
```

`BarQuote` 对应分钟线及更大周期的订阅：

```text
time, open, high, low, close, volume, amount,
settelementPrice, settlementPrice, openInterest, preClose,
suspendFlag, dr, totaldr
```

价格和金额使用 `int | float | None`，避免把 XtData 返回的整数强制改成浮点数；数量字段使用 `int | None`。

### 历史行情

- `HistoryTick(TickQuote)` 增加 `index: int | str`。
- `HistoryBar(BarQuote)` 增加 `index: int`。

实机日线 index 为 `YYYYMMDD` 整数，分钟线 index 为 `YYYYMMDDHHMMSS` 整数。响应结构是 `dict[code, list[HistoryTick | HistoryBar]]`，不再传递 DataFrame 的 columns/data 矩阵。

### 除权数据

`DividendFactor` 明确声明：

```text
date, time, interest, stockBonus, stockGift,
allotNum, allotPrice, gugai, dr
```

实机返回的 DataFrame index 是 `YYYYMMDD` 字符串，因此放入 `date`；真正的 XtData 毫秒时间戳来自 `time` 列。二者不会混用。

### 财务数据

八张 XtData 财务表分别有独立行模型，所有采样到的列都在模型中逐字段声明：

| XtData 表 | 行模型 | `FinancialData` 字段 |
| --- | --- | --- |
| `Balance` | `BalanceRecord` | `Balance` |
| `Income` | `IncomeRecord` | `Income` |
| `CashFlow` | `CashFlowRecord` | `CashFlow` |
| `Capital` | `CapitalRecord` | `Capital` |
| `Holdernum` | `HolderNumberRecord` | `Holdernum` |
| `Top10holder` | `Top10HolderRecord` | `Top10holder` |
| `Top10flowholder` | `Top10FlowHolderRecord` | `Top10flowholder` |
| `Pershareindex` | `PerShareIndexRecord` | `Pershareindex` |

每一行都有明确的 `index: int`，其余字段按 XtData 实际列定义为具体的 `float`、`str` 或可空版本。`FinancialData` 只负责把八种具体列表放在一个证券代码下，不包含通用字典行。

## 中转行情

`SequencedQuote` 是 agent 添加的最小信封：

| 字段 | 含义 |
| --- | --- |
| `seq` | agent 内单调递增序号 |
| `code` | XtData 回调中的证券代码 |
| `period` | 此订阅周期 |
| `source` | `market` 或 `stock` |
| `subscription` | 产生该回调的市场或证券订阅 |
| `received_at` | agent 收到回调的 Unix 微秒时间 |
| `quote` | `TickQuote` 或 `BarQuote` |

`QuoteEvent` 仅由 receiver 在落盘时产生，它在上述字段上增加：

- `event_time: int | None`：从原始 `quote.time` 推导出的 Unix 微秒时间；原行情没有时间时保持 `None`。
- `trading_date: date`：按中国市场时区从事件时间推导；缺少事件时间时使用 `received_at` 分区，但不丢弃行情。

## 请求模型

请求全部位于 `requests.py`：

```text
MarketRequest, MarketUnsubscribeRequest
StockRequest, StockSubscriptionRequest
SequencedQuoteRequest
HistoryDownloadRequest, HistoryQueryRequest
FinancialDownloadRequest, FinancialQueryRequest
DividendFactorsQueryRequest
```

请求层只处理输入边界：代码去空格并大写、重复项去重、时间格式和范围校验、数量上限校验。它不会过滤 XtData 返回的数据。

## 响应模型

响应全部位于 `responses.py`。主要形状如下：

```python
SnapshotResponse(
    data: dict[str, TickQuote],
    count: int,
)

LatestQuotesResponse(
    data: dict[str, TickQuote | BarQuote],
    periods: dict[str, XtDataPeriod],
    updated_at: dict[str, int],
)

HistoryQueryResponse(
    period: XtDataPeriod,
    data: dict[str, list[HistoryTick | HistoryBar]],
)

FinancialQueryResponse(
    data: dict[str, FinancialData],
)

DividendFactorsResponse(
    data: dict[str, list[DividendFactor]],
)
```

`LatestQuotesResponse` 要求 `data`、`periods`、`updated_at` 的代码集合完全一致。`SnapshotResponse.count` 和顺序行情的 `count` 必须与实际数据长度一致。结构不一致会直接失败，而不是修剪返回数据。

## 时间约定

时间只在有明确边界时转换：

- `quote.time` 和 `DividendFactor.time`：保留 XtData 原值，当前实机为毫秒时间戳。
- `received_at`：agent 生成的 Unix Epoch 微秒整数。
- `event_time`：receiver 从 `quote.time` 推导的 Unix Epoch 微秒整数。
- 历史行 `index`、财务行 `index`、除权 `date`：保留 XtData DataFrame index 的业务表示。

这样 agent 是透明中转，receiver 才是业务时间和存储语义的拥有者。
