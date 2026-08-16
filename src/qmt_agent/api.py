"""qmt-agent 的 HTTP 接口。"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from qmt_agent import __version__
from qmt_agent.config import Settings
from qmt_agent.gateway import MarketDataGateway, QmtGatewayError, XtDataGateway
from qmt_agent.models import (
    HistoryDownloadRequest,
    HistoryQueryRequest,
    MarketRequest,
    MarketUnsubscribeRequest,
    SequencedQuoteRequest,
    StockRequest,
    StockSubscriptionRequest,
    SubscribedQuoteRequest,
)
from qmt_agent.serialization import to_jsonable
from qmt_agent.service import (
    QmtMarketService,
    QuoteSequenceOutOfRangeError,
    SubscriptionLimitError,
    SubscriptionPeriodConflictError,
)


def create_app(
    gateway: MarketDataGateway | None = None,
    settings: Settings | None = None,
) -> FastAPI:
    configured_settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        active_gateway = gateway or XtDataGateway()
        app.state.market_service = QmtMarketService(
            active_gateway,
            max_stock_subscriptions=configured_settings.max_stock_subscriptions,
            quote_buffer_capacity=configured_settings.quote_buffer_capacity,
        )
        try:
            yield
        finally:
            app.state.market_service.close()

    app = FastAPI(
        title="QMT Agent",
        description="东北证券 miniQMT 行情 HTTP 服务",
        version=__version__,
        lifespan=lifespan,
    )

    @app.exception_handler(QmtGatewayError)
    async def handle_qmt_error(_: Request, exc: QmtGatewayError) -> JSONResponse:
        return JSONResponse(status_code=502, content={"detail": str(exc)})

    @app.exception_handler(SubscriptionLimitError)
    async def handle_limit_error(_: Request, exc: SubscriptionLimitError) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(SubscriptionPeriodConflictError)
    async def handle_period_conflict(
        _: Request, exc: SubscriptionPeriodConflictError
    ) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(QuoteSequenceOutOfRangeError)
    async def handle_quote_sequence_error(
        _: Request, exc: QuoteSequenceOutOfRangeError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=416,
            content={
                "detail": str(exc),
                "requested_seq": exc.requested_seq,
                "oldest_seq": exc.oldest_seq,
                "latest_seq": exc.latest_seq,
            },
        )

    def service(request: Request) -> QmtMarketService:
        return request.app.state.market_service

    @app.get("/health", summary="健康检查")
    def health(request: Request) -> dict[str, Any]:
        return {"status": "ok", "version": __version__, **service(request).status()}

    @app.get("/v1/subscriptions", summary="查看当前订阅")
    def subscriptions(request: Request) -> dict[str, Any]:
        return service(request).status()

    @app.post("/v1/subscriptions/markets", summary="全市场订阅")
    def subscribe_markets(
        request: Request, body: MarketRequest | None = None
    ) -> dict[str, Any]:
        markets = body.markets if body is not None else ["SH", "SZ"]
        return service(request).subscribe_markets(markets)

    @app.delete("/v1/subscriptions/markets", summary="全市场取消订阅")
    def unsubscribe_markets(
        request: Request, body: MarketUnsubscribeRequest | None = None
    ) -> dict[str, Any]:
        markets = body.markets if body is not None else None
        return service(request).unsubscribe_markets(markets)

    @app.post("/v1/subscriptions/stocks", summary="按列表订阅")
    def subscribe_stocks(
        request: Request, body: StockSubscriptionRequest
    ) -> dict[str, Any]:
        return service(request).subscribe_stocks(body.stocks, body.period)

    @app.delete("/v1/subscriptions/stocks", summary="按列表取消订阅")
    def unsubscribe_stocks(
        request: Request, body: StockSubscriptionRequest
    ) -> dict[str, Any]:
        return service(request).unsubscribe_stocks(body.stocks, body.period)

    @app.post("/v1/snapshots/markets", summary="获取全市场快照截面")
    def market_snapshot(
        request: Request, body: MarketRequest | None = None
    ) -> JSONResponse:
        markets = body.markets if body is not None else ["SH", "SZ"]
        result = service(request).get_market_snapshot(markets)
        return JSONResponse(content=to_jsonable({"data": result, "count": len(result)}))

    @app.post("/v1/snapshots/stocks", summary="按列表获取行情快照")
    def stock_snapshot(request: Request, body: StockRequest) -> JSONResponse:
        result = service(request).get_stock_snapshot(body.stocks)
        return JSONResponse(content=to_jsonable({"data": result, "count": len(result)}))

    @app.post("/v1/quotes/subscribed/markets", summary="获取全市场订阅的最新数据")
    def market_quotes(
        request: Request, body: SubscribedQuoteRequest | None = None
    ) -> JSONResponse:
        stocks = body.stocks if body is not None else None
        result = service(request).get_market_quotes(stocks)
        return JSONResponse(content=to_jsonable(result))

    @app.post("/v1/quotes/subscribed/stocks", summary="获取单股订阅的最新数据")
    def stock_quotes(
        request: Request, body: SubscribedQuoteRequest | None = None
    ) -> JSONResponse:
        stocks = body.stocks if body is not None else None
        result = service(request).get_stock_quotes(stocks)
        return JSONResponse(content=to_jsonable(result))

    @app.post("/v1/quotes/subscribed/sequence", summary="按序获取订阅行情")
    def sequenced_quotes(request: Request, body: SequencedQuoteRequest) -> JSONResponse:
        result = service(request).get_subscribed_quote_sequence(
            body.seq, body.limit, body.stocks
        )
        return JSONResponse(content=to_jsonable(result))

    @app.post("/v1/history/download", summary="按列表下载增量或全量历史数据")
    def download_history(request: Request, body: HistoryDownloadRequest) -> dict[str, Any]:
        return service(request).download_history(
            body.stocks,
            body.period,
            body.start_time,
            body.end_time,
            incrementally=body.mode == "incremental",
        )

    @app.post("/v1/history/query", summary="按列表获取历史数据")
    def query_history(request: Request, body: HistoryQueryRequest) -> JSONResponse:
        result = service(request).get_history(
            body.stocks,
            body.fields,
            body.period,
            body.start_time,
            body.end_time,
            body.count,
            body.dividend_type,
            body.fill_data,
        )
        return JSONResponse(content=to_jsonable({"data": result}))

    return app
