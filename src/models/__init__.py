"""平台统一业务模型。"""

from types import MappingProxyType

from models.data import (
    ADJUSTMENT_FACTOR_SCHEMA,
    BAR_SCHEMA,
    CURRENT_SCHEMA,
    DAILY_METRICS_SCHEMA,
    DIVIDEND_SCHEMA,
    INDUSTRY_SCHEMA,
    MONEYFLOW_SCHEMA,
    PRICE_LIMIT_SCHEMA,
    SESSION_SCHEMA,
    ST_STATUS_SCHEMA,
    STATUS_SCHEMA,
    STOCK_SCHEMA,
    SUSPENSION_SCHEMA,
    QueryResult,
)
from models.financial import (
    AUDIT_SCHEMA,
    BALANCE_SHEET_SCHEMA,
    CASH_FLOW_STATEMENT_SCHEMA,
    EXPRESS_SCHEMA,
    FINANCIAL_INDICATOR_SCHEMA,
    FORECAST_SCHEMA,
    INCOME_STATEMENT_SCHEMA,
)

ROUTE_SCHEMAS = MappingProxyType(
    {
        "market.daily_bars": BAR_SCHEMA,
        "market.intraday_bars": BAR_SCHEMA,
        "market.realtime_quotes": CURRENT_SCHEMA,
        "market.daily_metrics": DAILY_METRICS_SCHEMA,
        "market.moneyflow": MONEYFLOW_SCHEMA,
        "market.suspensions": SUSPENSION_SCHEMA,
        "market.price_limits": PRICE_LIMIT_SCHEMA,
        "market.st_status": ST_STATUS_SCHEMA,
        "fundamentals.income": INCOME_STATEMENT_SCHEMA,
        "fundamentals.balance_sheet": BALANCE_SHEET_SCHEMA,
        "fundamentals.cashflow": CASH_FLOW_STATEMENT_SCHEMA,
        "fundamentals.indicators": FINANCIAL_INDICATOR_SCHEMA,
        "fundamentals.forecast": FORECAST_SCHEMA,
        "fundamentals.express": EXPRESS_SCHEMA,
        "fundamentals.audit": AUDIT_SCHEMA,
        "corporate_actions.dividends": DIVIDEND_SCHEMA,
        "corporate_actions.adjustment_factors": ADJUSTMENT_FACTOR_SCHEMA,
        "classification.industry": INDUSTRY_SCHEMA,
        "reference.stocks": STOCK_SCHEMA,
        "calendar.sessions": SESSION_SCHEMA,
    }
)

__all__ = [
    "ADJUSTMENT_FACTOR_SCHEMA",
    "AUDIT_SCHEMA",
    "BAR_SCHEMA",
    "BALANCE_SHEET_SCHEMA",
    "CASH_FLOW_STATEMENT_SCHEMA",
    "CURRENT_SCHEMA",
    "DAILY_METRICS_SCHEMA",
    "DIVIDEND_SCHEMA",
    "EXPRESS_SCHEMA",
    "FINANCIAL_INDICATOR_SCHEMA",
    "FORECAST_SCHEMA",
    "INDUSTRY_SCHEMA",
    "INCOME_STATEMENT_SCHEMA",
    "MONEYFLOW_SCHEMA",
    "PRICE_LIMIT_SCHEMA",
    "SESSION_SCHEMA",
    "STOCK_SCHEMA",
    "ST_STATUS_SCHEMA",
    "STATUS_SCHEMA",
    "SUSPENSION_SCHEMA",
    "ROUTE_SCHEMAS",
    "QueryResult",
]
