from __future__ import annotations

import pytest
from pydantic import ValidationError

from qmt_protocol import (
    BarQuote,
    DividendFactor,
    FinancialFrame,
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
    assert quote.time == 1_786_944_183_000_000
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

    assert quote.time == 1_786_944_300_000_000
    assert quote.openInterest == 13.0
    assert quote.settelementPrice == quote.settlementPrice == 0.0


def test_sequence_selects_quote_model_from_period_without_guessing() -> None:
    record = SequencedQuote(
        seq=1,
        code="000001.SZ",
        period="1m",
        source="stock",
        subscription="000001.SZ",
        received_at=1_786_838_400_000_000,
        quote=BarQuote(time=1_786_838_400_000, close=11.08),
    )

    assert isinstance(record.quote, BarQuote)


def test_history_frame_rejects_rows_that_do_not_match_columns() -> None:
    with pytest.raises(ValidationError, match="行宽"):
        HistoryFrame(index=[20260817], columns=["open", "close"], data=[[11.2]])


def test_financial_and_dividend_have_distinct_business_structures() -> None:
    financial = FinancialFrame(
        index=[20241231],
        columns=["m_anntime", "tot_assets"],
        data=[[20250331, 100.0]],
    )
    factor = DividendFactor(event_time=1_717_200_000_000, interest=0.1, dr=0.99)

    assert not isinstance(financial, HistoryFrame)
    assert factor.event_time == 1_717_200_000_000_000


def test_sequence_rejects_realtime_quote_without_business_time() -> None:
    with pytest.raises(ValidationError, match="event_time"):
        SequencedQuote(
            seq=1,
            code="000001.SZ",
            period="tick",
            source="market",
            subscription="SH",
            received_at=1_786_838_400_000_000,
            quote=TickQuote(lastPrice=10.0),
        )


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


def test_sequence_response_accepts_empty_long_poll_without_advancing_cursor() -> None:
    empty = QuoteSequenceResponse(
        data=[],
        count=0,
        requested_seq=1,
        next_seq=1,
    )
    caught_up = QuoteSequenceResponse(
        data=[],
        count=0,
        requested_seq=11,
        next_seq=11,
        oldest_seq=1,
        latest_seq=10,
    )

    assert empty.oldest_seq is None
    assert caught_up.latest_seq == 10


def test_sequence_accepts_only_int64_microseconds() -> None:
    common = {
        "seq": 1,
        "code": "000001.SZ",
        "period": "tick",
        "source": "market",
        "subscription": "SZ",
        "quote": TickQuote(time=1_786_944_183_000, lastPrice=10.0),
    }

    parsed = SequencedQuote.model_validate(
        {
            **common,
            "received_at": 1_786_923_065_934_481,
            "quote": TickQuote(time=1_786_944_183_000, lastPrice=10.0),
        }
    )
    assert parsed.received_at == 1_786_923_065_934_481
    assert parsed.quote.time == 1_786_944_183_000_000
    assert parsed.event_time == 1_786_944_183_000_000

    for invalid in (
        "2026-08-17T05:51:05+00:00",
        1.0,
        True,
        2**63,
    ):
        with pytest.raises(ValidationError, match="Unix Epoch 微秒整数|int64"):
            SequencedQuote.model_validate({**common, "received_at": invalid})
