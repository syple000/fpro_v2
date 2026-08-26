"""供应商无关的财务报表、指标和披露 Arrow Schema。

金额使用人民币元，每股金额使用人民币元/股，比率、利润率和同比变化使用小数。
"""

from __future__ import annotations

import pyarrow as pa

from models.data import SHANGHAI_TIMESTAMP

_SYMBOL = pa.field("symbol", pa.string(), nullable=False)
_PERIOD_END = pa.field("period_end", pa.date32(), nullable=False)
_VISIBLE_AT = pa.field("visible_at", SHANGHAI_TIMESTAMP, nullable=False)
_ANNOUNCEMENT_DATE = pa.field("announcement_date", pa.date32())

_STATEMENT_IDENTITY = (
    _SYMBOL,
    _PERIOD_END,
    _VISIBLE_AT,
    _ANNOUNCEMENT_DATE,
    pa.field("actual_announcement_date", pa.date32(), nullable=False),
    pa.field("company_type", pa.string()),
)

_DISCLOSURE_IDENTITY = (
    _SYMBOL,
    _VISIBLE_AT,
    _PERIOD_END,
    _ANNOUNCEMENT_DATE,
)


def _float_fields(*names: str) -> tuple[pa.Field[pa.DataType], ...]:
    return tuple(pa.field(name, pa.float64()) for name in names)


INCOME_STATEMENT_SCHEMA = pa.schema(
    [
        *_STATEMENT_IDENTITY,
        *_float_fields(
            "basic_earnings_per_share",
            "diluted_earnings_per_share",
        ),
        *_float_fields(
            "total_revenue",
            "operating_revenue",
            "interest_income",
            "earned_premiums",
            "fee_and_commission_income",
            "net_fee_and_commission_income",
            "net_other_operating_income",
            "other_operating_income",
            "fair_value_change_gain",
            "investment_income",
            "associates_and_joint_ventures_income",
            "foreign_exchange_gain",
            "total_operating_costs",
            "operating_costs",
            "interest_expenses",
            "fee_and_commission_expenses",
            "taxes_and_surcharges",
            "selling_expenses",
            "administrative_expenses",
            "financial_expenses",
            "asset_impairment_losses",
            "research_and_development_expenses",
            "operating_profit",
            "non_operating_income",
            "non_operating_expenses",
            "total_profit",
            "income_tax_expenses",
            "net_income",
            "net_income_attributable_to_parent",
            "minority_interest_income",
            "other_comprehensive_income",
            "total_comprehensive_income",
            "comprehensive_income_attributable_to_parent",
            "comprehensive_income_attributable_to_minority",
            "earnings_before_interest_and_tax",
            "earnings_before_interest_tax_depreciation_and_amortization",
            "continuing_operations_net_income",
        ),
    ]
)

BALANCE_SHEET_SCHEMA = pa.schema(
    [
        *_STATEMENT_IDENTITY,
        pa.field("share_capital", pa.float64()),
        *_float_fields(
            "capital_reserve",
            "surplus_reserve",
            "special_reserve",
            "retained_earnings",
            "treasury_stock",
            "cash_and_cash_equivalents",
            "trading_financial_assets",
            "derivative_financial_assets",
            "notes_receivable",
            "accounts_receivable",
            "prepayments",
            "interest_receivable",
            "dividends_receivable",
            "other_receivables",
            "inventories",
            "other_current_assets",
            "total_current_assets",
            "available_for_sale_financial_assets",
            "held_to_maturity_investments",
            "long_term_equity_investments",
            "investment_property",
            "fixed_assets",
            "construction_in_progress",
            "construction_materials",
            "intangible_assets",
            "goodwill",
            "long_term_deferred_expenses",
            "deferred_tax_assets",
            "other_non_current_assets",
            "total_non_current_assets",
            "total_assets",
            "short_term_borrowings",
            "trading_financial_liabilities",
            "derivative_financial_liabilities",
            "notes_payable",
            "accounts_payable",
            "contract_liabilities",
            "employee_benefits_payable",
            "taxes_payable",
            "interest_payable",
            "dividends_payable",
            "other_payables",
            "current_portion_of_non_current_liabilities",
            "other_current_liabilities",
            "total_current_liabilities",
            "long_term_borrowings",
            "bonds_payable",
            "long_term_payables",
            "specific_payables",
            "deferred_tax_liabilities",
            "other_non_current_liabilities",
            "total_non_current_liabilities",
            "total_liabilities",
            "equity_attributable_to_parent",
            "minority_interests",
            "total_equity",
            "total_liabilities_and_equity",
        ),
    ]
)

CASH_FLOW_STATEMENT_SCHEMA = pa.schema(
    [
        *_STATEMENT_IDENTITY,
        *_float_fields(
            "cash_received_from_sales_and_services",
            "tax_refunds_received",
            "other_operating_cash_receipts",
            "total_operating_cash_inflows",
            "cash_paid_for_goods_and_services",
            "cash_paid_to_employees",
            "taxes_paid",
            "other_operating_cash_payments",
            "total_operating_cash_outflows",
            "net_operating_cash_flow",
            "cash_received_from_investment_disposals",
            "cash_received_from_investment_returns",
            "cash_received_from_asset_disposals",
            "cash_received_from_subsidiary_disposals",
            "other_investing_cash_receipts",
            "total_investing_cash_inflows",
            "cash_paid_for_asset_acquisitions",
            "cash_paid_for_investments",
            "cash_paid_for_subsidiary_acquisitions",
            "other_investing_cash_payments",
            "total_investing_cash_outflows",
            "net_investing_cash_flow",
            "cash_received_from_capital_contributions",
            "cash_received_from_borrowings",
            "cash_received_from_bond_issuance",
            "other_financing_cash_receipts",
            "total_financing_cash_inflows",
            "cash_paid_for_debt_repayments",
            "cash_paid_for_dividends_and_interest",
            "other_financing_cash_payments",
            "total_financing_cash_outflows",
            "net_financing_cash_flow",
            "effect_of_exchange_rate_changes",
            "net_change_in_cash_and_cash_equivalents",
            "cash_and_cash_equivalents_at_beginning",
            "cash_and_cash_equivalents_at_end",
            "net_income",
            "asset_impairment_provisions",
            "depreciation_of_fixed_assets",
            "amortization_of_intangible_assets",
            "amortization_of_long_term_deferred_expenses",
            "financial_expenses",
            "investment_losses",
            "free_cash_flow",
        ),
    ]
)

FINANCIAL_INDICATOR_SCHEMA = pa.schema(
    [
        _SYMBOL,
        _PERIOD_END,
        _VISIBLE_AT,
        _ANNOUNCEMENT_DATE,
        *_float_fields(
            "basic_earnings_per_share",
            "diluted_earnings_per_share",
            "net_assets_per_share",
            "operating_cash_flow_per_share",
            "capital_reserve_per_share",
            "surplus_reserve_per_share",
            "retained_earnings_per_share",
        ),
        pa.field("gross_profit", pa.float64()),
        *_float_fields(
            "gross_margin",
            "net_profit_margin",
            "return_on_equity",
            "weighted_return_on_equity",
            "return_on_assets",
            "return_on_invested_capital",
        ),
        *_float_fields(
            "current_ratio",
            "quick_ratio",
            "cash_ratio",
        ),
        pa.field("debt_to_assets", pa.float64()),
        *_float_fields(
            "asset_turnover",
            "inventory_turnover",
            "accounts_receivable_turnover",
        ),
        *_float_fields(
            "revenue_year_over_year",
            "net_income_year_over_year",
            "operating_cash_flow_year_over_year",
        ),
    ]
)

FORECAST_SCHEMA = pa.schema(
    [
        *_DISCLOSURE_IDENTITY,
        pa.field("forecast_type", pa.string()),
        *_float_fields(
            "net_income_change_lower_bound",
            "net_income_change_upper_bound",
        ),
        *_float_fields(
            "net_income_lower_bound",
            "net_income_upper_bound",
            "prior_period_net_income",
        ),
        pa.field("first_announcement_date", pa.date32()),
        pa.field("summary", pa.string()),
        pa.field("change_reason", pa.string()),
    ]
)

EXPRESS_SCHEMA = pa.schema(
    [
        *_DISCLOSURE_IDENTITY,
        *_float_fields(
            "operating_revenue",
            "operating_profit",
            "total_profit",
            "net_income_attributable_to_parent",
            "total_assets",
            "equity_attributable_to_parent",
        ),
        pa.field("diluted_earnings_per_share", pa.float64()),
        pa.field("diluted_return_on_equity", pa.float64()),
        pa.field("prior_period_net_income", pa.float64()),
        pa.field("net_assets_per_share", pa.float64()),
        *_float_fields(
            "revenue_year_over_year",
            "operating_profit_year_over_year",
            "total_profit_year_over_year",
            "net_income_year_over_year",
            "earnings_per_share_year_over_year",
            "return_on_equity_year_over_year",
            "total_assets_growth",
            "equity_year_over_year",
            "net_assets_per_share_growth",
        ),
        pa.field("summary", pa.string()),
        pa.field("is_audited", pa.bool_()),
        pa.field("remark", pa.string()),
    ]
)

AUDIT_SCHEMA = pa.schema(
    [
        *_DISCLOSURE_IDENTITY,
        pa.field("audit_opinion", pa.string()),
        pa.field("audit_fees", pa.float64()),
        pa.field("audit_firm", pa.string()),
        pa.field("signing_accountants", pa.string()),
    ]
)
