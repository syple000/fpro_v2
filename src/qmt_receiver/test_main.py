"""测试 main：全市场接收 quote，并每分钟调用一遍 qmt-agent 接口。"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from pathlib import Path
from queue import Empty, Queue
from time import monotonic
from typing import Any

from qmt_receiver import QmtAgentClient, QmtReceiver, QuoteParquetWriter

logger = logging.getLogger("qmt_receiver.test_main")


def _call(name: str, function: Callable[[], dict[str, Any]]) -> dict[str, Any] | None:
    started = monotonic()
    try:
        result = function()
    except Exception:
        logger.exception("%s 调用失败", name)
        return None
    logger.info(
        "%s 完成，耗时 %.1f ms，摘要=%s",
        name,
        (monotonic() - started) * 1_000,
        _summary(result),
    )
    return result


def _summary(result: dict[str, Any]) -> dict[str, Any]:
    summary = {
        key: result[key]
        for key in (
            "status",
            "count",
            "stock_count",
            "markets",
            "stocks",
            "subscribed",
            "added",
            "removed",
            "missing",
            "not_subscribed",
            "next_seq",
            "oldest_seq",
            "latest_seq",
            "completed",
        )
        if key in result
    }
    data = result.get("data")
    if isinstance(data, (dict, list)):
        summary["data_size"] = len(data)
    return summary


def check_all_interfaces(client: QmtAgentClient) -> None:
    """用 000001.SZ 调用全部接口，不中断 SH/SZ 全市场订阅。"""
    stock = "000001.SZ"
    _call("health", client.health)
    status = _call("subscriptions", client.subscriptions) or {}
    _call("subscribe markets", lambda: client.subscribe_markets(("SH", "SZ")))
    _call("unsubscribe market(no-op)", lambda: client.unsubscribe_markets(("TEST",)))
    _call("subscribe stock", lambda: client.subscribe_stocks((stock,), "tick"))
    _call("market snapshot", client.market_snapshot)
    _call("stock snapshot", lambda: client.stock_snapshot((stock,)))
    _call("market quotes", lambda: client.market_quotes((stock,)))
    _call("stock quotes", lambda: client.stock_quotes((stock,)))

    latest_status = _call("subscriptions before sequence", client.subscriptions) or status
    sequence = latest_status.get("quote_sequence", {})
    latest_seq = sequence.get("latest_seq") if isinstance(sequence, dict) else None
    _call(
        "quote sequence",
        lambda: client.quote_sequence(latest_seq or 1, limit=1, wait_ms=0),
    )
    _call(
        "history download",
        lambda: client.download_history((stock,), period="1d", mode="incremental"),
    )
    _call(
        "history query",
        lambda: client.query_history(
            (stock,),
            fields=("time", "open", "high", "low", "close", "volume"),
            period="1d",
            count=1,
        ),
    )
    _call("unsubscribe stock", lambda: client.unsubscribe_stocks((stock,), "tick"))


def drain_queue(queue: Queue[dict[str, Any]]) -> int:
    count = 0
    while True:
        try:
            queue.get_nowait()
        except Empty:
            return count
        count += 1


def run(base_url: str, data_dir: Path, once: bool, timeout_ms: int) -> None:
    quote_queue: Queue[dict[str, Any]] = Queue()
    with (
        QmtAgentClient(base_url) as client,
        QuoteParquetWriter(data_dir) as writer,
    ):
        client.subscribe_markets(("SH", "SZ"))
        receiver = QmtReceiver(client, writer, timeout_ms=timeout_ms)
        next_check = monotonic()

        while True:
            result = receiver.receive(quote_queue)
            consumed = drain_queue(quote_queue)
            if result.count or result.probes:
                logger.info(
                    "quote count=%s next_seq=%s probes=%s skipped=%s queue=%s",
                    result.count,
                    result.next_seq,
                    result.probes,
                    result.skipped,
                    consumed,
                )

            now = monotonic()
            if now >= next_check:
                check_all_interfaces(client)
                if once:
                    return
                next_check = now + 60


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8765")
    parser.add_argument("--data-dir", type=Path, default=Path("data/qmt_receiver"))
    parser.add_argument("--timeout-ms", type=int, default=30_000)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        run(args.url, args.data_dir, args.once, args.timeout_ms)
    except KeyboardInterrupt:
        logger.info("测试停止")


if __name__ == "__main__":
    main()
