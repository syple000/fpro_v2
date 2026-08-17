# QMT 行情数据协议

`qmt_protocol` 是 `qmt_agent` 与 `qmt_receiver` 唯一共用的数据契约。HTTP OpenAPI、
receiver 的运行时响应校验、Parquet 写入输入和 platform 队列事件都引用这里的 Pydantic
模型，不再各自维护裸 `dict[str, Any]`。

## 验证依据

字段定义同时依据：

1. [迅投 XtData 官方文档](https://dict.thinktrader.net/nativeApi/xtdata.html)中的接口、周期和字段表；
2. [迅投数据结构文档](https://dict.thinktrader.net/innerApi/data_structure.html?id=7zqjlm)；
3. 2026-08-17 在本机东北证券 miniQMT 上直接调用 `xtquant.xtdata` 的结果。

实机只调用行情接口：`get_full_tick`、`get_local_data`、`subscribe_quote`、
`subscribe_whole_quote`、`unsubscribe_quote`。没有调用账户、资产、持仓、委托、成交或
其他交易接口。

实测外层结构如下：

| XtData 接口 | Python 原始结构 |
| --- | --- |
| `get_full_tick` | `dict[str, dict[str, scalar/list]]` |
| `subscribe_whole_quote` | `dict[str, dict[str, scalar/list]]` |
| `subscribe_quote(..., period="tick")` | `dict[str, list[dict[str, scalar/list]]]` |
| `subscribe_quote(..., period="1m")` | `dict[str, list[dict[str, scalar]]]` |
| `get_local_data(..., period="1d"/"1m"/"tick")` | `dict[str, pandas.DataFrame]` |

SH 全推首包包含 26,585 个合约，所有合约具有相同的 22 个字段；字段和值类型与下文
`TickQuote` 一致。`get_full_tick` 的 `amount` 和 `settlementPrice` 实测可能是 `int`，而全推
回调和官方语义为 `float`，协议边界统一转换为 `float`。

## 链路

```text
XtData object
  -> XtDataGateway（校验、规范化为 TickQuote / BarQuote）
  -> QmtMarketService（只保存协议模型）
  -> FastAPI（按响应模型生成 OpenAPI 和 JSON）
  -> QmtAgentClient（从 JSON 严格反序列化为同一响应模型）
  -> QmtReceiver / QuoteParquetWriter（只接收 SequencedQuote）
  -> Queue[QuoteEvent]
```

协议信封使用 `extra="forbid"` 和严格类型。receiver 收到新增或拼错的信封字段时会拒绝
响应并抛出 `QmtAgentError`，不会悄悄忽略。

行情明细使用 `extra="allow"`，因为不同券商客户端版本和行情级别确实会扩展字段。已知
字段严格校验；未知字段保存在模型的 `__pydantic_extra__` 中并随 HTTP、Parquet
`quote.extra_json` 和队列事件完整传递，同时由 agent 记录 DEBUG 日志，便于后续补入正式定义。

## 行情明细

字段为可选并不表示类型不确定。实测 `get_full_tick`、全推回调和不同客户端版本的字段
集合不完全相同，因此“字段可以不存在”；字段一旦存在，值必须满足表中的确定类型。

### `TickQuote`

| 字段 | Python / JSON 类型 | 来源 |
| --- | --- | --- |
| `time` | `int`，毫秒时间戳 | 官方、实测 |
| `stime` | `str` | 新版官方结构 |
| `timetag` | `str` | 本机 `get_full_tick` 实测 |
| `lastPrice`, `open`, `high`, `low`, `lastClose` | `float` | 官方、实测 |
| `amount` | `float` | 官方；本机快照的 `int` 会规范为 `float` |
| `volume`, `pvolume` | `int` | 官方、实测 |
| `stockStatus`, `openInt` | `int` | 官方、实测 |
| `transactionNum` | `int` | 官方、回调实测 |
| `lastSettlementPrice`, `settlementPrice` | `float` | 官方、实测 |
| `askPrice`, `bidPrice` | `list[float]` | 官方、实测五档 |
| `askVol`, `bidVol` | `list[int]` | 官方、实测五档 |
| `pe` | `float` | 本机全推回调实测 |
| `volRatio` | `float` | 本机全推回调实测 |
| `speed1Min`, `speed5Min` | `float` | 本机全推回调实测 |

本机 `get_full_tick` 返回 `timetag`，但不返回 `transactionNum`、`pe`、`volRatio`、
`speed1Min`、`speed5Min`；全推和单股 tick 回调则返回后五项但没有 `timetag`。协议不伪造
缺失字段，HTTP 序列化会排除值为 `None` 的可选字段。

### `BarQuote`

| 字段 | Python / JSON 类型 | 来源 |
| --- | --- | --- |
| `time` | `int`，毫秒时间戳 | 官方、实测 |
| `open`, `high`, `low`, `close` | `float` | 官方、实测 |
| `volume` | `int` | 本机实测 |
| `amount` | `float` | 官方、实测 |
| `settelementPrice` | `float` | 官方拼写、本机历史 DataFrame 实测 |
| `settlementPrice` | `float` | 本机实时 K 线回调实测 |
| `openInterest` | `float` | 官方；本机 `int` 会规范为 `float` |
| `preClose` | `float` | 官方、实测 |
| `suspendFlag` | `int` | 官方、实测 |
| `dr`, `totaldr` | `float` | 本机实时 K 线回调实测 |

`settelementPrice` 是官方历史字段中长期存在的拼写，实时回调实测使用正确拼写
`settlementPrice`。协议同时保留两者，不能隐式改名或合并。

## HTTP 响应模型

| 接口 | 响应模型 |
| --- | --- |
| `GET /health` | `HealthResponse` |
| `GET /v1/subscriptions` | `SubscriptionStatus` |
| `POST/DELETE /v1/subscriptions/markets` | `MarketSubscriptionResponse` |
| `POST/DELETE /v1/subscriptions/stocks` | `StockSubscriptionResponse` |
| `POST /v1/snapshots/markets` | `SnapshotResponse` |
| `POST /v1/snapshots/stocks` | `SnapshotResponse` |
| `POST /v1/quotes/subscribed/markets` | `LatestQuotesResponse` |
| `POST /v1/quotes/subscribed/stocks` | `LatestQuotesResponse` |
| `POST /v1/quotes/subscribed/sequence` | `QuoteSequenceResponse` |
| `POST /v1/history/download` | `HistoryDownloadResponse` |
| `POST /v1/history/query` | `HistoryQueryResponse` |

### 状态和订阅结果

`QuoteSequenceStatus`：

| 字段 | 类型 |
| --- | --- |
| `oldest_seq`, `latest_seq` | `int | None` |
| `next_seq` | `int` |
| `size`, `capacity` | `int` |

`SubscriptionStatus`：

| 字段 | 类型 |
| --- | --- |
| `instance_id` | `str` |
| `markets`, `stocks` | `list[str]` |
| `stock_periods` | `dict[str, XtDataPeriod]` |
| `stock_count`, `stock_limit` | `int` |
| `quote_sequence` | `QuoteSequenceStatus` |

`HealthResponse` 在上述字段之外增加 `status: Literal["ok"]` 和 `version: str`。

`MarketSubscriptionResponse` 包含四个 `list[str]`：`subscribed`、`added`、`removed`、
`not_found`。

`StockSubscriptionResponse` 包含：

- `periods: dict[str, XtDataPeriod]`
- `subscribed`, `added`, `updated`, `removed`, `not_found: list[str]`
- `period_mismatches: dict[str, XtDataPeriod]`

### 快照、最新行情和顺序行情

`SnapshotResponse`：`data: dict[str, TickQuote]`、`count: int`；模型会校验 `count` 与
`data` 数量一致。

`LatestQuotesResponse`：

| 字段 | 类型 |
| --- | --- |
| `data` | `dict[str, TickQuote | BarQuote]` |
| `updated_at` | `dict[str, datetime]` |
| `periods` | `dict[str, XtDataPeriod]` |
| `missing`, `not_subscribed` | `list[str]` |

模型要求 `data`、`updated_at`、`periods` 使用相同的合约代码集合，并根据每个代码的
`period` 确定行情是 `TickQuote` 还是 `BarQuote`。

`SequencedQuote`：

| 字段 | 类型 |
| --- | --- |
| `seq` | `int` |
| `code` | `str` |
| `period` | `XtDataPeriod` |
| `source` | `Literal["market", "stock"]` |
| `subscription` | `str` |
| `received_at` | 有时区的 `datetime`；JSON 为 ISO 8601 字符串 |
| `quote` | 按 `period` 确定的 `TickQuote | BarQuote` |

`QuoteSequenceResponse`：`data: list[SequencedQuote]`、`count`、`requested_seq`、
`next_seq`、`oldest_seq`、`latest_seq` 均为 `int`。模型校验数量和序号边界。

HTTP 416 使用 `QuoteSequenceErrorResponse`：`detail: str`，以及可为空的
`requested_seq`、`oldest_seq`、`latest_seq`。

### 历史行情

每只合约的 pandas DataFrame 使用稳定的 split 编码 `HistoryFrame`：

| 字段 | 类型 |
| --- | --- |
| `index` | `list[JsonValue]` |
| `columns` | `list[str]` |
| `data` | `list[list[JsonValue]]` |

模型校验 index 行数、data 行数和 columns 行宽一致。单元值之所以是 `JsonValue`，是因为
调用方可通过 `fields` 动态选择列，tick 的盘口列本身又是列表；具体已知行情列的值类型仍
由上面的 `TickQuote` / `BarQuote` 表定义。

`HistoryQueryResponse.data` 是 `dict[str, HistoryFrame]`。
`HistoryDownloadResponse` 包含 `stocks: list[str]`、`period: XtDataPeriod`、
`mode: Literal["incremental", "full"]`、`completed: bool`。

## Receiver 队列事件

`QuoteEvent` 继承 `SequencedQuote` 的全部字段，并增加：

| 字段 | 类型 |
| --- | --- |
| `trading_date` | `datetime.date` |

因此 platform 应创建 `Queue[QuoteEvent]`，消费者使用属性访问，例如
`event.seq`、`event.quote.lastPrice`。Parquet 分为 `ticks`、`bars` 两张按交易日分区的表；
固定信封字段落入顶层列，行情主体保存在具有确定 schema 的 `quote` struct 中；
`received_at` 使用带 UTC 时区的 timestamp。未知的客户端扩展字段保存在
`quote.extra_json`，不会因动态字段未建列而丢失。

## 字段丢弃和日志规则

- 未定义的行情字段：不丢弃，保存在模型扩展字段中，并打印 DEBUG；
- 类型错误的 XtData 回调：拒绝整批，打印异常和原始 payload 的 DEBUG；
- 未定义的 HTTP 信封字段：receiver 拒绝响应并打印 DEBUG，不静默忽略；
- 请求中的重复代码、重复历史字段或空历史字段：清洗时打印 DEBUG；
- 非字符串 JSON key 的字符串化冲突、非法 UTF-8 替换、未知对象字符串回退：打印 DEBUG；
- 反订阅后的延迟回调或订阅失败前暂存回调：按竞态规则丢弃并打印 DEBUG。

生产环境可通过 `QMT_AGENT_LOG_LEVEL=debug` 查看这些记录。
