from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from qmt_agent.models import (
    DividendFactorsRequest,
    FinancialQueryRequest,
    FinancialRequest,
    HistoryDownloadRequest,
    HistoryQueryRequest,
    MarketRequest,
    MarketUnsubscribeRequest,
    SequencedQuoteRequest,
    StockRequest,
    StockSubscriptionRequest,
    SubscribedQuoteRequest,
)


@pytest.mark.parametrize(
    ("model", "payload"),
    [
        (MarketRequest, {"markets": ["SH"]}),
        (MarketUnsubscribeRequest, {"markets": ["SH"]}),
        (StockRequest, {"stocks": ["000001.SZ"]}),
        (
            StockSubscriptionRequest,
            {"stocks": ["000001.SZ"], "period": "1m"},
        ),
        (SubscribedQuoteRequest, {"stocks": ["000001.SZ"]}),
        (SequencedQuoteRequest, {"seq": 1, "limit": 100}),
        (HistoryDownloadRequest, {"stocks": ["000001.SZ"]}),
        (HistoryQueryRequest, {"stocks": ["000001.SZ"]}),
        (FinancialRequest, {"stocks": ["000001.SZ"]}),
        (FinancialQueryRequest, {"stocks": ["000001.SZ"]}),
        (DividendFactorsRequest, {"stocks": ["000001.SZ"]}),
    ],
)
def test_every_request_model_rejects_unknown_fields(
    model: type[BaseModel], payload: dict[str, Any]
) -> None:
    with pytest.raises(ValidationError) as error:
        model.model_validate({**payload, "unexpected": "value"})

    assert any(item["type"] == "extra_forbidden" for item in error.value.errors())


@pytest.mark.parametrize(
    ("model", "payload", "error_type"),
    [
        (MarketRequest, {"markets": "SH"}, "list_type"),
        (StockRequest, {"stocks": [123]}, "string_type"),
        (SequencedQuoteRequest, {"seq": "1"}, "int_type"),
        (
            HistoryQueryRequest,
            {"stocks": ["000001.SZ"], "fill_data": 1},
            "bool_type",
        ),
    ],
)
def test_request_models_do_not_coerce_wrong_types(
    model: type[BaseModel], payload: dict[str, Any], error_type: str
) -> None:
    with pytest.raises(ValidationError) as error:
        model.model_validate(payload)

    assert any(item["type"] == error_type for item in error.value.errors())


@pytest.mark.parametrize(
    "model",
    [
        HistoryDownloadRequest,
        HistoryQueryRequest,
        FinancialRequest,
        FinancialQueryRequest,
        DividendFactorsRequest,
    ],
)
def test_history_models_reject_impossible_dates_and_reversed_ranges(
    model: type[BaseModel],
) -> None:
    with pytest.raises(ValidationError, match="有效日期"):
        model.model_validate({"stocks": ["000001.SZ"], "start_time": "20250230"})
    with pytest.raises(ValidationError, match="不能晚于"):
        model.model_validate(
            {
                "stocks": ["000001.SZ"],
                "start_time": "20250202",
                "end_time": "20250201",
            }
        )


@pytest.mark.parametrize(
    "model",
    [
        HistoryDownloadRequest,
        HistoryQueryRequest,
        FinancialRequest,
        FinancialQueryRequest,
        DividendFactorsRequest,
    ],
)
def test_date_only_end_time_includes_the_whole_day(model: type[BaseModel]) -> None:
    request = model.model_validate(
        {
            "stocks": ["000001.SZ"],
            "start_time": "20250201150000",
            "end_time": "20250201",
        }
    )

    assert request.model_dump()["end_time"] == "20250201"


def test_financial_request_deduplicates_tables() -> None:
    request = FinancialRequest.model_validate(
        {
            "stocks": ["000001.SZ"],
            "tables": ["Balance", "Income", "Balance"],
        }
    )

    assert request.tables == ["Balance", "Income"]
