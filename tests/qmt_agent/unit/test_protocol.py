from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from qmt_protocol import (
    BarQuote,
    HistoryFrame,
    QuoteSequenceResponse,
    SequencedQuote,
    TickQuote,
)


def test_tick_model_matches_real_xtdata_snapshot_and_callback_types() -> None:
    quote = TickQuote.model_validate(
        {
            "time": 1786944183000,
            "timetag": "20260817 13:23:03",
            "lastPrice": 11.09,
            "open": 11.2,
            "high": 11.22,
            "low": 11.07,
            "lastClose": 11.11,
            # get_full_tick 实测为 int；协议统一成 float。
            "amount": 687998800,
            "volume": 619280,
            "pvolume": 61928005,
            "stockStatus": 3,
            "openInt": 13,
            "transactionNum": 61684,
            "lastSettlementPrice": 11.11,
            "settlementPrice": 0,
            "pe": 0.0,
            "askPrice": [11.09, 11.1],
            "bidPrice": [11.08, 11.07],
            "askVol": [12332, 100],
            "bidVol": [5781, 200],
            "volRatio": 0.0,
            "speed1Min": 0.0,
            "speed5Min": 0.0,
        }
    )

    assert quote.amount == 687998800.0
    assert quote.settlementPrice == 0.0
    assert quote.askPrice == [11.09, 11.1]


def test_bar_model_covers_real_callback_extensions_and_both_settlement_spellings() -> None:
    quote = BarQuote.model_validate(
        {
            "time": 1786944300000,
            "open": 11.09,
            "high": 11.09,
            "low": 11.08,
            "close": 11.08,
            "volume": 662,
            "amount": 733967.0,
            "settelementPrice": 0.0,
            "settlementPrice": 0.0,
            "openInterest": 13,
            "dr": 1.0,
            "totaldr": 107.62245531327493,
            "preClose": 11.11,
            "suspendFlag": 0,
        }
    )

    assert quote.openInterest == 13.0
    assert quote.settelementPrice == quote.settlementPrice == 0.0


def test_sequence_selects_quote_model_from_period_without_guessing() -> None:
    record = SequencedQuote(
        seq=1,
        code="000001.SZ",
        period="1m",
        source="stock",
        subscription="000001.SZ",
        received_at=datetime(2026, 8, 17, tzinfo=UTC),
        quote=BarQuote(close=11.08),
    )

    assert isinstance(record.quote, BarQuote)


def test_history_frame_rejects_rows_that_do_not_match_columns() -> None:
    with pytest.raises(ValidationError, match="行宽"):
        HistoryFrame(index=[20260817], columns=["open", "close"], data=[[11.2]])


def test_sequence_response_rejects_inconsistent_count() -> None:
    with pytest.raises(ValidationError, match="count"):
        QuoteSequenceResponse(
            data=[],
            count=1,
            requested_seq=1,
            next_seq=2,
            oldest_seq=1,
            latest_seq=1,
        )


def test_sequence_parses_aware_iso_time_and_rejects_naive_time() -> None:
    common = {
        "seq": 1,
        "code": "000001.SZ",
        "period": "tick",
        "source": "market",
        "subscription": "SZ",
        "quote": TickQuote(lastPrice=10.0),
    }

    parsed = SequencedQuote.model_validate(
        {**common, "received_at": "2026-08-17T05:51:05.934481+00:00"}
    )
    assert parsed.received_at.tzinfo is not None

    with pytest.raises(ValidationError, match="必须包含时区"):
        SequencedQuote.model_validate({**common, "received_at": "2026-08-17T05:51:05"})
