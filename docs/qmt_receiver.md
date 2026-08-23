# qmt-receiver

`qmt_receiver` 是供 platform 直接调用的普通 Python 组件，不启动 HTTP 服务、不创建线程，
也不持有消息队列。platform 决定何时及在哪个线程连续调用；receiver 每次只完成一批：

1. 调用 qmt-agent 的 quote sequence；
2. 把 tick 与 bar 分开、按交易日期写入 `parquet_store`；
3. 进入存储缓冲区后逐条 `queue.put(event)`。

## Platform 调用

```python
from queue import Queue

from qmt_receiver import QmtAgentClient, QmtDataStore, QmtReceiver, QuoteEvent

quote_queue: Queue[QuoteEvent] = Queue()  # Python Queue 本身是线程安全的

with (
    QmtAgentClient("http://127.0.0.1:8765") as client,
    QmtDataStore("data/qmt") as store,
):
    receiver = QmtReceiver(client, store, timeout_ms=30_000)

    while platform_is_running:
        result = receiver.receive(quote_queue)
```

`receive(queue)` 是 receiver 对 platform 暴露的接收方法。没有数据时，sequence 最多等待
`timeout_ms`，超时返回 `count=0`；receiver 不 sleep，下一次何时调用由 platform 决定。

如果 `next_seq` 已被环形缓存覆盖，receiver 按接口返回的边界进行试探：先请求
`oldest_seq - 1` 获取新边界，再从新 `oldest_seq + 1` 开始按 `+2`、`+4`……指数向缓存内部
移动，最大不超过 `latest_seq`，直到拿到可用批次。正常追到 `latest_seq + 1` 时，长轮询
超时会返回 HTTP 200 的空批次，只视为暂无新数据，不会产生 416 或回头重放缓存。

客户端方法返回 `qmt_protocol` 中定义的 Pydantic 模型，而不是裸字典。receiver 会在 HTTP
边界严格校验整个响应；字段缺失、类型错误或未知信封字段都会抛出 `QmtAgentError`。
最终写入队列的是 `QuoteEvent`，可通过 `event.seq`、`event.event_time`、`event.period`、
`event.quote` 和 `event.trading_date` 等属性访问。完整协议见
[QMT 行情数据协议](qmt_protocol.md)。

## qmt-agent 方法

`QmtAgentClient` 将 qmt-agent 的业务接口暴露为同步 Python 方法：

- `health()`、`subscriptions()`；
- `subscribe_markets()`、`unsubscribe_markets()`；
- `subscribe_stocks()`、`unsubscribe_stocks()`；
- `market_snapshot()`、`stock_snapshot()`；
- `market_quotes()`、`stock_quotes()`、`quote_sequence()`；
- `download_history()`、`query_history()`。
- `download_financial()`、`query_financial()`、`query_dividend_factors()`。

## 下载同步

`sync.py` 负责把 qmt-agent 下载接口和 `QmtDataStore` 串起来，对外直接暴露
`sync_daily()`、`sync_financial()`、`sync_dividend_factors()` 和 `sync_all()`：

- `daily`：日线，`adjustment` 区分 `none` 和 `front`；
- `financial`：财务报表，`disclosure_date` 来自 QMT `m_anntime`，原始字段完整保存在
  `data_json`；
- `dividend_factors`：除权事件和 `dr` 复权系数。

三类返回结构相互独立：历史行情使用 `HistoryFrame`，财务数据使用 `FinancialFrame`，除权
使用逐字段定义的 `DividendFactor` 列表。除权的 `event_time` 保留 XtData index 时间戳，落盘
时同时保存该微秒时间戳，并按中国市场时区派生 `ex_date` 分区。

```python
from qmt_receiver import QmtAgentClient, QmtDataStore, sync_all

with (
    QmtAgentClient() as client,
    QmtDataStore("data/qmt") as store,
):
    result = sync_all(
        client,
        store,
        ["000001.SZ", "600000.SH"],
        "20240101",
        "20241231",
    )
```

财务下载会补全抽样股票的本地数据，再按报告期读取指定区间。下载类数据每次写完都会立即
flush，并按业务主键去重合并受影响的日期分区。写入后 `DataCatalog` 会将三张表公开为
`qmt.daily`、`qmt.financial` 和 `qmt.dividend_factors`。

## Parquet

行情分成两张逻辑表，存储粒度仍为交易日：

- `ticks`：只保存 `period="tick"` 的行情；
- `bars`：保存 `1m`、`5m`、`1d` 等所有 K 线周期，通过 `period` 列区分周期。

两张表都按 `trading_date` 分区，并只按 `event_time` 排序。存储业务时间 `event_time` 和接收
时间 `received_at` 始终是 Unix Epoch 微秒 `int64`。缺少上游行情时间的实时记录会在
qmt-agent 中直接丢弃，不进入 receiver。目录结构示意为 `ticks/trading_date=.../` 和
`bars/trading_date=.../`（实际分区值由 `parquet_store` 做 URL 编码）。

两张表都有以下固定信封列：

| 列 | Arrow 类型 | 可空 |
| --- | --- | --- |
| `trading_date` | `date32` | 否 |
| `seq` | `int64` | 否 |
| `code`, `period`, `source`, `subscription` | `string` | 否 |
| `received_at` | `int64`，Unix Epoch 微秒 | 否 |
| `event_time` | `int64`，Unix Epoch 微秒 | 否 |
| `quote` | `struct` | 否 |

队列模型中的 `event_time` 是非空的原始行情发生时间，也是落盘业务键。`received_at` 是
qmt-agent 实际收到回调的时间。XtData 原始 `quote.time` 会在接入边界识别
秒/毫秒/微秒/纳秒并转成微秒。

行情主体不在顶层铺平，而是保存在 `quote` struct 中。`ticks.quote` 使用 `TickQuote` 的
schema：时间和计数类子字段为 `int64`，价格、金额和比率子字段为 `float64`，`stime`、
`timetag` 为 `string`，盘口价格/数量分别为 `list<float64>` / `list<int64>`。
`bars.quote` 使用 `BarQuote` 的 schema：`time`、`volume`、`suspendFlag` 为 `int64`，
OHLC、金额、结算价、持仓量及复权子字段为 `float64`。完整字段名与含义见
[QMT 行情数据协议](qmt_protocol.md)。

行情字段在不同接口和客户端版本中可能缺失，因此已知行情列允许为空，但类型固定。未知的
券商扩展字段不会丢弃，而是只保存在 `quote.extra_json`；已知字段不再存成 JSON。

新时间 Schema 不兼容旧数据目录，不做自动迁移；切换版本时应删除旧数据并重新接收。

`append_quotes()` 不会为每个接收批次强制 `flush`。数据进入 `ParquetStore` 缓冲区后即可写入
队列并推进 receiver 的内存 `next_seq`；达到 store 的行数或内存阈值时自动提交，store
`close()` 时会提交全部剩余数据。这样可以避免每个接收批次产生一个小 Parquet 文件。

Tick 以 `(code, event_time)` 去重；bar 因为混存多个周期，以
`(code, period, event_time)` 去重；冲突时保留 `received_at` 较大的记录。append 时不会立即
去重合并。`QmtReceiver` 每次创建时会扫描 tick/bar 的 Manifest，包含单文件分区内的重复键；
运行中需要手动整理时直接调用：

```python
result = store.compact_realtime()
```

返回值是 `ticks`、`bars` 各自实际整理的分区数。

## 测试 main

安装依赖：

```bash
uv sync --group qmt-receiver
```

实时接收：

```bash
uv run --group qmt-receiver qmt-receiver-test realtime
```

只接收一批：

```bash
uv run --group qmt-receiver qmt-receiver-test realtime --once
```

同步下载：

```bash
uv run --group qmt-receiver qmt-receiver-test sync \
  --stocks 000001.SZ 600000.SH \
  --start-time 20240101 \
  --end-time 20241231
```

可用 `--url` 和 `--data-dir` 覆盖默认参数；实时模式还支持 `--markets` 和 `--timeout-ms`。
