# qmt-receiver

`qmt_receiver` 是供 platform 直接调用的普通 Python 组件，不启动 HTTP 服务、不创建线程，
也不持有消息队列。platform 决定何时及在哪个线程连续调用；receiver 每次只完成一批：

1. 调用 qmt-agent 的 quote sequence；
2. 按交易日期写入 `parquet_store`；
3. 写入成功后逐条 `queue.put(event)`。

## Platform 调用

```python
from queue import Queue

from qmt_receiver import QmtAgentClient, QmtReceiver, QuoteParquetWriter

quote_queue = Queue()  # Python Queue 本身是线程安全的，由 platform 创建和消费

with (
    QmtAgentClient("http://127.0.0.1:8765") as client,
    QuoteParquetWriter("data/qmt_receiver") as writer,
):
    receiver = QmtReceiver(client, writer, timeout_ms=30_000)

    while platform_is_running:
        result = receiver.receive(quote_queue)
```

`receive(queue)` 是 receiver 对 platform 暴露的接收方法。没有数据时，sequence 最多等待
`timeout_ms`，超时返回 `count=0`；receiver 不 sleep，下一次何时调用由 platform 决定。

如果 `next_seq` 已被环形缓存覆盖，receiver 按接口返回的边界进行试探：先请求
`oldest_seq - 1` 获取新边界，再从新 `oldest_seq + 1` 开始按 `+2`、`+4`……指数向缓存内部
移动，最大不超过 `latest_seq`，直到拿到可用批次。正常追到 `latest_seq + 1` 时只视为暂无
新数据，不会回头重放缓存。

## qmt-agent 方法

`QmtAgentClient` 将 qmt-agent 的业务接口暴露为同步 Python 方法：

- `health()`、`subscriptions()`；
- `subscribe_markets()`、`unsubscribe_markets()`；
- `subscribe_stocks()`、`unsubscribe_stocks()`；
- `market_snapshot()`、`stock_snapshot()`；
- `market_quotes()`、`stock_quotes()`、`quote_sequence()`；
- `download_history()`、`query_history()`。

## Parquet

逻辑表名为 `quotes`，按 `trading_date` 分区。日期优先使用 quote 的 `time`，无法解析时使用
agent 的 `received_at`，默认时区为 `Asia/Shanghai`。动态 quote 完整保存在 `quote_json`。

每批都会 `flush`；只有落盘成功后才写入队列和推进 receiver 的内存 `next_seq`。

## 测试 main

安装依赖：

```bash
uv sync --group qmt-receiver
```

运行：

```bash
uv run --group qmt-receiver qmt-receiver-test
```

测试 main 自己创建线程安全 `Queue` 并连续调用 `receive()`，启动时订阅 SH/SZ 全市场；每分钟
用 `000001.SZ` 调用一遍 qmt-agent 全部业务接口。只运行一轮：

```bash
uv run --group qmt-receiver qmt-receiver-test --once
```

可用 `--url`、`--data-dir` 和 `--timeout-ms` 覆盖默认参数。

