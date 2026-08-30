# qmt-receiver

`qmt_receiver` 是 platform 主动调用的数据接收组件。它不启动 HTTP 服务、不创建后台线程，也不持有平台队列。每次 `receive()` 完成一次顺序读取、落盘，并把具体事件直接返回。

## 实时接收

```python
from qmt_receiver import QmtAgentClient, QmtDataStore, QmtReceiver

with (
    QmtAgentClient("http://127.0.0.1:8765") as client,
    QmtDataStore("data/qmt") as store,
):
    receiver = QmtReceiver(client, store, timeout_ms=30_000)

    while platform_is_running:
        result = receiver.receive()
        for event in result.data:
            handle(event)
```

`ReceiveResult` 包含：

| 字段 | 含义 |
| --- | --- |
| `data` | 本批已经进入存储缓冲区的 `tuple[QuoteEvent, ...]` |
| `count` | `len(data)` 只读属性 |
| `next_seq` | 下一次应请求的序号 |
| `probes` | 缓存越界恢复时的试探次数 |
| `skipped` | 因 agent 环形缓存覆盖而跳过的条数 |

如 platform 已有线程安全队列，也可调用 `receiver.receive(queue)`。事件仍在 `result.data` 中返回，同时逐条执行 `queue.put(event)`；队列只是可选输出，不是 receiver 的必要依赖。

没有新数据时，agent 最多等待 `timeout_ms`，receiver 返回 `count == 0`，不自行 sleep。下一次调用时机由 platform 控制。

## 连续性和恢复

receiver 保存下一次应读取的 `seq`：

- 正常批次落盘成功后才推进 `next_seq`。
- 已追到 agent 最新序号时，长轮询超时返回空批次。
- 若所需序号已被环形缓存覆盖，receiver 根据 416 返回的 `oldest_seq`、`latest_seq` 从可用区间内指数试探，找到可读取窗口后继续。
- HTTP 响应会在客户端边界完整校验。字段缺失、类型错误或未知字段抛出 `QmtAgentError`，不会作为部分成功处理。

## 时间职责

agent 保留 XtData 原始 `quote.time`。receiver 落盘时才生成业务字段：

- 能识别的秒、毫秒、微秒或纳秒时间转换为 Unix Epoch 微秒 `event_time`。
- `trading_date` 按 `Asia/Shanghai` 从 `event_time` 推导。
- 原行情缺少 `time` 时，`event_time` 保持 `None`，使用 `received_at` 选择存储分区，数据仍然返回和落盘。

`QuoteEvent` 因而同时保留原始 `quote`、agent 接收时间和 receiver 派生时间。完整定义见 [QMT 数据协议](qmt_protocol.md)。

## Python 客户端

`QmtAgentClient` 返回 `qmt_protocol` 的 Pydantic 对象，而不是裸字典：

- `health()`、`subscriptions()`
- `subscribe_markets()`、`unsubscribe_markets()`
- `subscribe_stocks()`、`unsubscribe_stocks()`
- `market_snapshot()`、`stock_snapshot()`
- `market_quotes()`、`stock_quotes()`、`quote_sequence()`
- `download_history()`、`query_history()`
- `download_financial()`、`query_financial()`、`query_dividend_factors()`

`market_quotes()` 和 `stock_quotes()` 获取完整最新值缓存，没有证券过滤参数。客户端默认不读取系统或环境代理；注入自建 HTTP 客户端时由调用方决定代理设置。

## 下载同步

`sync.py` 将 agent 的直接查询结果写入 `QmtDataStore`：

- `sync_daily()`：同步 QMT 原生不复权日线；默认增量下载。
- `sync_intraday()`：按指定分钟周期同步 QMT 原生不复权历史 K 线。
- `sync_financial()`：同步八类具体财务记录。
- `sync_dividend_factors()`：同步具体 `DividendFactor` 记录。
- `sync_all()`：依次同步日线、1 分钟线、财务和除权因子，并接受关键字参数 `force=False`。

同步流程不再新增 `front/front_ratio` 行情。QMT 原生前复权缺少锚点及因子历史可见时间，不能
直接作为严格 PIT 的 Reader 输出；旧分区仍可由存储层读取以便离线复核。

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
        force=True,
    )
```

QMT 历史行情默认以 `incremental` 模式补齐本地缓存；`force=True` 改用 `full`，即使区间已经
下载过也再次下载。财务下载和除权因子查询没有增量/完整模式，本来就会在每次同步时请求，
所以 `force` 不改变这两类调用。`sync_all()` 默认包含 1 分钟线；其他分钟周期仍按实际需要调用
`sync_intraday()`。命令行 `qmt-receiver-test sync` 同样支持 `--force`。

历史、财务和除权返回在进入 receiver 时已经是具体行结构。存储层只做物理表映射，不再解析通用 DataFrame 或任意 JSON 单元。

## Parquet 表

| 表 | 分区 | 主键 |
| --- | --- | --- |
| `ticks` | `trading_date` | `(code, event_time)` |
| `bars` | `trading_date` | `(code, period, event_time)` |
| `daily` | `trade_date` | `(code, adjustment)` |
| `intraday` | `trading_date` | `(code, period, adjustment, event_time)` |
| `financial` | `report_date` | `(code, dataset)` |
| `dividend_factors` | `ex_date` | `code` |

实时表公共信封列为 `trading_date`、`seq`、`code`、`period`、`source`、`subscription`、`received_at`、`event_time` 和 `quote`。`quote` 是具体 Arrow struct：tick 和 bar 各自使用固定字段集合。

财务协议本身是八种具体记录；物理表为了让不同财务表共存，将具体记录序列化到 `data_json`，并另外保存用于查询、分区和去重的 `report_date`、`code`、`dataset`、`disclosure_date`。这只是存储布局，不是网络协议中的通用数据结构。

`append_quotes()` 不为每个接收批次强制 flush。数据先进入 `ParquetStore` 缓冲区，达到行数或内存阈值时提交，`close()` 提交剩余数据，避免大量小文件。启动 receiver 时会整理实时表已有重复键；运行中可手动调用：

```python
result = store.compact_realtime()
```

实时分区的 Manifest 会持久化压缩签名。未追加新行情且压缩配置未变化时，重复启动 receiver
或重复调用 `compact_realtime()` 会跳过已经整理过的分区；即使一个交易日数据量较大、整理后
仍有多个目标大小的文件，也不会再次做无效的全量读取和重写。新行情只会使其所属交易日分区
失效，其他历史分区保持跳过。

## 测试命令

```bash
uv sync --group qmt-receiver
uv run --group qmt-receiver qmt-receiver-test realtime
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
