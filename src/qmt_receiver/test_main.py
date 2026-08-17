"""测试 main：全市场接收 quote，并每分钟调用一遍 qmt-agent 接口。"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from pathlib import Path
from queue import Empty, Queue
from time import monotonic
from typing import TypeVar

from qmt_protocol import ProtocolModel, QuoteEvent
from qmt_receiver import QmtAgentClient, QmtReceiver, QuoteParquetWriter

logger = logging.getLogger("qmt_receiver.test_main")
ResponseModel = TypeVar("ResponseModel", bound=ProtocolModel)


def _call(name: str, function: Callable[[], ResponseModel]) -> ResponseModel | None:
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


def _summary(result: ProtocolModel) -> dict[str, object]:
    summary: dict[str, object] = {
        key: getattr(result, key)
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
        if key in type(result).model_fields
    }
    data = getattr(result, "data", None)
    if isinstance(data, (dict, list)):
        summary["data_size"] = len(data)
    return summary


def check_all_interfaces(client: QmtAgentClient) -> list[str]:
    """用 000001.SZ 调用全部接口，不中断 SH/SZ 全市场订阅。"""
    stock = "000001.SZ"
    failures: list[str] = []

    def check(name: str, function: Callable[[], ResponseModel]) -> ResponseModel | None:
        result = _call(name, function)
        if result is None:
            failures.append(name)
        return result

    check("health", client.health)
    status = check("subscriptions", client.subscriptions)
    check("subscribe markets", lambda: client.subscribe_markets(("SH", "SZ")))
    check("unsubscribe market(no-op)", lambda: client.unsubscribe_markets(("TEST",)))
    check("subscribe stock", lambda: client.subscribe_stocks((stock,), "tick"))
    check("market snapshot", client.market_snapshot)
    check("stock snapshot", lambda: client.stock_snapshot((stock,)))
    check("market quotes", lambda: client.market_quotes((stock,)))
    check("stock quotes", lambda: client.stock_quotes((stock,)))

    latest_status = check("subscriptions before sequence", client.subscriptions) or status
    latest_seq = latest_status.quote_sequence.latest_seq if latest_status is not None else None
    check(
        "quote sequence",
        lambda: client.quote_sequence(latest_seq or 1, limit=1, wait_ms=0),
    )
    check(
        "history download",
        lambda: client.download_history((stock,), period="1d", mode="incremental"),
    )
    check(
        "history query",
        lambda: client.query_history(
            (stock,),
            fields=("time", "open", "high", "low", "close", "volume"),
            period="1d",
            count=1,
        ),
    )
    check("unsubscribe stock", lambda: client.unsubscribe_stocks((stock,), "tick"))
    return failures


def drain_queue(queue: Queue[QuoteEvent]) -> int:
    count = 0
    while True:
        try:
            queue.get_nowait()
        except Empty:
            return count
        count += 1


def run(base_url: str, data_dir: Path, once: bool, timeout_ms: int) -> None:
    quote_queue: Queue[QuoteEvent] = Queue()
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
                failures = check_all_interfaces(client)
                if failures:
                    raise RuntimeError(f"接口巡检失败：{', '.join(failures)}")
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
