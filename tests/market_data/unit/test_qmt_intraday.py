from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

import pytest

from market_data import DataCatalog, DataReader, SourceConfig
from qmt_protocol import BarQuote, HistoryBar, SequencedQuote
from qmt_receiver import QmtDataStore

TZ = ZoneInfo("Asia/Shanghai")
START = datetime(2026, 1, 5, 10, tzinfo=TZ)
SYMBOL = "000001.SZ"


@pytest.mark.parametrize(
    "frequency,period,minutes",
    [
        ("1m", "1m", 1),
        ("5m", "5m", 5),
        ("15m", "15m", 15),
        ("30m", "30m", 30),
        ("60m", "1h", 60),
    ],
)
@pytest.mark.parametrize("adjustment", ["none", "forward"])
@pytest.mark.parametrize("use_count", [False, True])
def test_intraday_reads_downloaded_ohlcv_and_never_falls_back_to_pushes(
    tmp_path: Path,
    frequency: str,
    period: Literal["1m", "5m", "15m", "30m", "1h"],
    minutes: int,
    adjustment: Literal["none", "forward"],
    use_count: bool,
) -> None:
    end = START + timedelta(minutes=minutes)
    with QmtDataStore(tmp_path / "qmt") as store:
        store.write_intraday(
            {
                SYMBOL: [
                    HistoryBar(
                        index=int(end.strftime("%Y%m%d%H%M%S")),
                        open=10,
                        high=11.2,
                        low=9.9,
                        close=11,
                        preClose=9.8,
                        volume=100,
                        amount=105_000,
                    )
                ]
            },
            period,
            "none",
        )
        store.append_quotes(
            [
                SequencedQuote(
                    seq=seq,
                    code=code,
                    period=period,
                    source="stock",
                    subscription=code,
                    received_at=int(received.timestamp() * 1_000_000),
                    quote=BarQuote(
                        time=int(label.timestamp() * 1_000),
                        open=10,
                        high=10.2,
                        low=10,
                        close=10.2,
                        volume=20,
                        amount=20_400,
                    ),
                )
                for seq, code, label, received in [
                    # 同一根 K 线的盘中快照与迟到版本，均不能覆盖历史数据。
                    (1, SYMBOL, end, START + timedelta(seconds=20)),
                    (2, SYMBOL, end, end + timedelta(seconds=20)),
                    # 更新的推送也不能补入 count 窗口；缺少历史的证券仍为空。
                    (3, SYMBOL, end + timedelta(minutes=minutes), end),
                    (4, "600000.SH", end, end),
                ]
            ]
        )
    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(catalog, sources=SourceConfig(routes={"market.intraday_bars": "qmt"}))
        before = reader.at(end - timedelta(seconds=1)).market.bars(
            symbols=(SYMBOL,),
            frequency=frequency,
            count=1,
            adjustment=adjustment,
        )
        assert before.table.num_rows == 0
        for as_of in (end, datetime(2026, 1, 5, 16, tzinfo=TZ)):
            data = reader.at(as_of)
            if use_count:
                result = data.market.bars(
                    symbols=(SYMBOL, "600000.SH"),
                    frequency=frequency,
                    count=1,
                    adjustment=adjustment,
                )
            else:
                result = data.market.bars(
                    symbols=(SYMBOL, "600000.SH"),
                    frequency=frequency,
                    start=START,
                    end=as_of,
                    adjustment=adjustment,
                )
            assert result.table.to_pylist() == [
                {
                    "symbol": SYMBOL,
                    "interval_start": START,
                    "interval_end": end,
                    "open": 10.0,
                    "high": 11.2,
                    "low": 9.9,
                    "close": 11.0,
                    "pre_close": 9.8,
                    "volume": 10_000.0,
                    "amount": 105_000.0,
                }
            ]


def test_push_only_store_stays_empty_after_interval_end(tmp_path: Path) -> None:
    with QmtDataStore(tmp_path / "qmt") as store:
        events = store.append_quotes(
            [
                SequencedQuote(
                    seq=1,
                    code=SYMBOL,
                    period="1m",
                    source="stock",
                    subscription=SYMBOL,
                    received_at=int(START.timestamp() * 1_000_000),
                    quote=BarQuote(
                        time=int((START + timedelta(minutes=1)).timestamp() * 1_000), close=10
                    ),
                )
            ]
        )
        assert len(events) == 1
    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(catalog, sources=SourceConfig(routes={"market.intraday_bars": "qmt"}))
        assert (
            reader.at(START + timedelta(days=1))
            .market.bars(
                symbols=(SYMBOL,),
                frequency="1m",
                count=10,
            )
            .table.num_rows
            == 0
        )
        assert catalog.connection.execute("SELECT count(*) FROM qmt.bars").fetchone() == (1,)
