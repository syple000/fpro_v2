"""可由 platform 直接调用的 QMT 行情接收组件。"""

from qmt_protocol import (
    BarQuote,
    HealthResponse,
    HistoryDownloadResponse,
    HistoryFrame,
    HistoryQueryResponse,
    LatestQuotesResponse,
    MarketSubscriptionResponse,
    QuoteEvent,
    QuoteSequenceResponse,
    QuoteSequenceStatus,
    SequencedQuote,
    SnapshotResponse,
    StockSubscriptionResponse,
    SubscriptionStatus,
    TickQuote,
)
from qmt_receiver.client import QmtAgentClient, QmtAgentError
from qmt_receiver.receiver import QmtReceiver, ReceiveResult
from qmt_receiver.storage import (
    BAR_SCHEMA,
    BAR_TABLE,
    TICK_SCHEMA,
    TICK_TABLE,
    QuoteParquetWriter,
)

__all__ = [
    "BarQuote",
    "BAR_SCHEMA",
    "BAR_TABLE",
    "HealthResponse",
    "HistoryDownloadResponse",
    "HistoryFrame",
    "HistoryQueryResponse",
    "LatestQuotesResponse",
    "MarketSubscriptionResponse",
    "QmtAgentClient",
    "QmtAgentError",
    "QmtReceiver",
    "QuoteEvent",
    "QuoteParquetWriter",
    "QuoteSequenceResponse",
    "QuoteSequenceStatus",
    "ReceiveResult",
    "SequencedQuote",
    "SnapshotResponse",
    "StockSubscriptionResponse",
    "SubscriptionStatus",
    "TickQuote",
    "TICK_SCHEMA",
    "TICK_TABLE",
]
