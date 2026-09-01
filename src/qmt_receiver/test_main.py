"""测试 main：运行实时接收或下载同步。"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from queue import Empty, Queue

from fpro_common import configure_beijing_logging
from qmt_protocol import QuoteEvent
from qmt_receiver import QmtAgentClient, QmtDataStore, QmtReceiver, sync_all

logger = logging.getLogger("qmt_receiver.test_main")


def drain_queue(queue: Queue[QuoteEvent]) -> int:
    count = 0
    while True:
        try:
            queue.get_nowait()
        except Empty:
            return count
        count += 1


def run_realtime(
    base_url: str,
    data_dir: Path,
    markets: list[str],
    once: bool,
    timeout_ms: int,
) -> None:
    quote_queue: Queue[QuoteEvent] = Queue()
    with QmtAgentClient(base_url) as client, QmtDataStore(data_dir) as store:
        store.compact_realtime()
        client.subscribe_markets(markets)
        receiver = QmtReceiver(client, store, timeout_ms=timeout_ms)
        while True:
            result = receiver.receive(quote_queue)
            consumed = drain_queue(quote_queue)
            logger.info(
                "quote count=%s next_seq=%s probes=%s skipped=%s queue=%s",
                result.count,
                result.next_seq,
                result.probes,
                result.skipped,
                consumed,
            )
            if once:
                return


def run_sync(
    base_url: str,
    data_dir: Path,
    stocks: list[str],
    start_time: str,
    end_time: str,
    force: bool,
) -> None:
    with QmtAgentClient(base_url) as client, QmtDataStore(data_dir) as store:
        result = sync_all(client, store, stocks, start_time, end_time, force=force)
    logger.info("sync 完成：%s", result)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("realtime", "sync"))
    parser.add_argument("--url", default="http://127.0.0.1:8765")
    parser.add_argument("--data-dir", type=Path, default=Path("dataset/qmt"))
    parser.add_argument("--markets", nargs="+", default=["SH", "SZ"])
    parser.add_argument("--stocks", nargs="+", default=["000001.SZ"])
    parser.add_argument("--start-time")
    parser.add_argument("--end-time")
    parser.add_argument("--timeout-ms", type=int, default=30_000)
    parser.add_argument("--once", action="store_true")
    parser.add_argument(
        "--force",
        action="store_true",
        help="sync 模式下忽略 checkpoint，重新查询并覆盖请求区间",
    )
    args = parser.parse_args()
    configure_beijing_logging(logging.INFO)
    try:
        if args.mode == "realtime":
            run_realtime(
                args.url,
                args.data_dir,
                args.markets,
                args.once,
                args.timeout_ms,
            )
        else:
            if args.start_time is None or args.end_time is None:
                parser.error("sync 模式必须提供 --start-time 和 --end-time")
            run_sync(
                args.url,
                args.data_dir,
                args.stocks,
                args.start_time,
                args.end_time,
                args.force,
            )
    except KeyboardInterrupt:
        logger.info("测试停止")


if __name__ == "__main__":
    main()
