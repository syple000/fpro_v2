"""Tushare 与 QMT 到平台统一字段的内置适配器。"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from typing import Literal
from zoneinfo import ZoneInfo

import duckdb
import pyarrow as pa
import pyarrow.compute as pc

from data.catalog import DataCatalog
from data.errors import (
    DataCapabilityNotSupportedError,
    DataSourceUnavailableError,
)
from models import (
    ADJUSTMENT_FACTOR_SCHEMA,
    AUDIT_SCHEMA,
    BALANCE_SHEET_SCHEMA,
    BAR_SCHEMA,
    CASH_FLOW_STATEMENT_SCHEMA,
    CURRENT_SCHEMA,
    DAILY_METRICS_SCHEMA,
    DIVIDEND_SCHEMA,
    EXPRESS_SCHEMA,
    FINANCIAL_INDICATOR_SCHEMA,
    FORECAST_SCHEMA,
    INCOME_STATEMENT_SCHEMA,
    INDUSTRY_SCHEMA,
    MONEYFLOW_SCHEMA,
    PRICE_LIMIT_SCHEMA,
    SESSION_SCHEMA,
    ST_STATUS_SCHEMA,
    SUSPENSION_SCHEMA,
    DataCapability,
)

_TZ = "Asia/Shanghai"
_CALENDAR_COVERAGE_ERROR = "__FPRO_CALENDAR_COVERAGE_ERROR__"
_SUSPENSION_TIMING_ERROR = "__FPRO_SUSPENSION_TIMING_ERROR__"
_FORWARD_BAR_SCHEMA = pa.schema(
    [*BAR_SCHEMA, pa.field("__invalid_factor", pa.bool_(), nullable=False)]
)

_TUSHARE_COMPANY_TYPES = (
    ("1", "industrial"),
    ("2", "bank"),
    ("3", "insurance"),
    ("4", "securities"),
)
_PLATFORM_TO_TUSHARE_COMPANY_TYPE = {
    platform: source for source, platform in _TUSHARE_COMPANY_TYPES
}
_TUSHARE_CAPABILITIES = frozenset(
    {
        DataCapability.DAILY_BARS,
        DataCapability.REALTIME_QUOTES,
        DataCapability.DAILY_METRICS,
        DataCapability.MONEYFLOW,
        DataCapability.SUSPENSIONS,
        DataCapability.PRICE_LIMITS,
        DataCapability.ST_STATUS,
        DataCapability.INCOME,
        DataCapability.BALANCE_SHEET,
        DataCapability.CASHFLOW,
        DataCapability.INDICATORS,
        DataCapability.FORECAST,
        DataCapability.EXPRESS,
        DataCapability.AUDIT,
        DataCapability.DIVIDENDS,
        DataCapability.ADJUSTMENT_FACTORS,
        DataCapability.INDUSTRY,
        DataCapability.SESSIONS,
    }
)

_QMT_CAPABILITIES = frozenset(
    {
        DataCapability.DAILY_BARS,
        DataCapability.INTRADAY_BARS,
        DataCapability.REALTIME_QUOTES,
    }
)

_INCOME_SOURCE_FIELDS = {
    "basic_earnings_per_share": "basic_eps",
    "diluted_earnings_per_share": "diluted_eps",
    "total_revenue": "total_revenue",
    "operating_revenue": "revenue",
    "interest_income": "int_income",
    "earned_premiums": "prem_earned",
    "fee_and_commission_income": "comm_income",
    "net_fee_and_commission_income": "n_commis_income",
    "net_other_operating_income": "n_oth_income",
    "other_operating_income": "oth_b_income",
    "fair_value_change_gain": "fv_value_chg_gain",
    "investment_income": "invest_income",
    "associates_and_joint_ventures_income": "ass_invest_income",
    "foreign_exchange_gain": "forex_gain",
    "total_operating_costs": "total_cogs",
    "operating_costs": "oper_cost",
    "interest_expenses": "int_exp",
    "fee_and_commission_expenses": "comm_exp",
    "taxes_and_surcharges": "biz_tax_surchg",
    "selling_expenses": "sell_exp",
    "administrative_expenses": "admin_exp",
    "financial_expenses": "fin_exp",
    "asset_impairment_losses": "assets_impair_loss",
    "research_and_development_expenses": "rd_exp",
    "operating_profit": "operate_profit",
    "non_operating_income": "non_oper_income",
    "non_operating_expenses": "non_oper_exp",
    "total_profit": "total_profit",
    "income_tax_expenses": "income_tax",
    "net_income": "n_income",
    "net_income_attributable_to_parent": "n_income_attr_p",
    "minority_interest_income": "minority_gain",
    "other_comprehensive_income": "oth_compr_income",
    "total_comprehensive_income": "t_compr_income",
    "comprehensive_income_attributable_to_parent": "compr_inc_attr_p",
    "comprehensive_income_attributable_to_minority": "compr_inc_attr_m_s",
    "earnings_before_interest_and_tax": "ebit",
    "earnings_before_interest_tax_depreciation_and_amortization": "ebitda",
    "continuing_operations_net_income": "continued_net_profit",
}

_BALANCE_SHEET_SOURCE_FIELDS = {
    "share_capital": "total_share",
    "capital_reserve": "cap_rese",
    "surplus_reserve": "surplus_rese",
    "special_reserve": "special_rese",
    "retained_earnings": "undistr_porfit",
    "treasury_stock": "treasury_share",
    "monetary_funds": "money_cap",
    "trading_financial_assets": "trad_asset",
    "derivative_financial_assets": "deriv_assets",
    "notes_receivable": "notes_receiv",
    "accounts_receivable": "accounts_receiv",
    "prepayments": "prepayment",
    "interest_receivable": "int_receiv",
    "dividends_receivable": "div_receiv",
    "other_receivables": "oth_rcv_total",
    "inventories": "inventories",
    "other_current_assets": "oth_cur_assets",
    "total_current_assets": "total_cur_assets",
    "available_for_sale_financial_assets": "fa_avail_for_sale",
    "held_to_maturity_investments": "htm_invest",
    "long_term_equity_investments": "lt_eqt_invest",
    "investment_property": "invest_real_estate",
    "fixed_assets": "fix_assets_total",
    "construction_in_progress": "cip_total",
    "construction_materials": "const_materials",
    "intangible_assets": "intan_assets",
    "goodwill": "goodwill",
    "long_term_deferred_expenses": "lt_amor_exp",
    "deferred_tax_assets": "defer_tax_assets",
    "other_non_current_assets": "oth_nca",
    "total_non_current_assets": "total_nca",
    "total_assets": "total_assets",
    "short_term_borrowings": "st_borr",
    "trading_financial_liabilities": "trading_fl",
    "derivative_financial_liabilities": "deriv_liab",
    "notes_payable": "notes_payable",
    "accounts_payable": "acct_payable",
    "contract_liabilities": "contract_liab",
    "employee_benefits_payable": "payroll_payable",
    "taxes_payable": "taxes_payable",
    "interest_payable": "int_payable",
    "dividends_payable": "div_payable",
    "other_payables": "oth_pay_total",
    "current_portion_of_non_current_liabilities": "non_cur_liab_due_1y",
    "other_current_liabilities": "oth_cur_liab",
    "total_current_liabilities": "total_cur_liab",
    "long_term_borrowings": "lt_borr",
    "bonds_payable": "bond_payable",
    "long_term_payables": "lt_payable",
    "specific_payables": "specific_payables",
    "deferred_tax_liabilities": "defer_tax_liab",
    "other_non_current_liabilities": "oth_ncl",
    "total_non_current_liabilities": "total_ncl",
    "total_liabilities": "total_liab",
    "equity_attributable_to_parent": "total_hldr_eqy_exc_min_int",
    "minority_interests": "minority_int",
    "total_equity": "total_hldr_eqy_inc_min_int",
    "total_liabilities_and_equity": "total_liab_hldr_eqy",
}

_CASH_FLOW_SOURCE_FIELDS = {
    "cash_received_from_sales_and_services": "c_fr_sale_sg",
    "tax_refunds_received": "recp_tax_rends",
    "other_operating_cash_receipts": "c_fr_oth_operate_a",
    "total_operating_cash_inflows": "c_inf_fr_operate_a",
    "cash_paid_for_goods_and_services": "c_paid_goods_s",
    "cash_paid_to_employees": "c_paid_to_for_empl",
    "taxes_paid": "c_paid_for_taxes",
    "other_operating_cash_payments": "oth_cash_pay_oper_act",
    "total_operating_cash_outflows": "st_cash_out_act",
    "net_operating_cash_flow": "n_cashflow_act",
    "cash_received_from_investment_disposals": "c_disp_withdrwl_invest",
    "cash_received_from_investment_returns": "c_recp_return_invest",
    "cash_received_from_asset_disposals": "n_recp_disp_fiolta",
    "cash_received_from_subsidiary_disposals": "n_recp_disp_sobu",
    "other_investing_cash_receipts": "oth_recp_ral_inv_act",
    "total_investing_cash_inflows": "stot_inflows_inv_act",
    "cash_paid_for_asset_acquisitions": "c_pay_acq_const_fiolta",
    "cash_paid_for_investments": "c_paid_invest",
    "cash_paid_for_subsidiary_acquisitions": "n_disp_subs_oth_biz",
    "other_investing_cash_payments": "oth_pay_ral_inv_act",
    "total_investing_cash_outflows": "stot_out_inv_act",
    "net_investing_cash_flow": "n_cashflow_inv_act",
    "cash_received_from_capital_contributions": "c_recp_cap_contrib",
    "cash_received_from_borrowings": "c_recp_borrow",
    "cash_received_from_bond_issuance": "proc_issue_bonds",
    "other_financing_cash_receipts": "oth_cash_recp_ral_fnc_act",
    "total_financing_cash_inflows": "stot_cash_in_fnc_act",
    "cash_paid_for_debt_repayments": "c_prepay_amt_borr",
    "cash_paid_for_dividends_and_interest": "c_pay_dist_dpcp_int_exp",
    "other_financing_cash_payments": "oth_cashpay_ral_fnc_act",
    "total_financing_cash_outflows": "stot_cashout_fnc_act",
    "net_financing_cash_flow": "n_cash_flows_fnc_act",
    "effect_of_exchange_rate_changes": "eff_fx_flu_cash",
    "net_change_in_cash_and_cash_equivalents": "n_incr_cash_cash_equ",
    "cash_and_cash_equivalents_at_beginning": "c_cash_equ_beg_period",
    "cash_and_cash_equivalents_at_end": "c_cash_equ_end_period",
    "net_income": "net_profit",
    "asset_impairment_provisions": "prov_depr_assets",
    "depreciation_of_fixed_assets": "depr_fa_coga_dpba",
    "amortization_of_intangible_assets": "amort_intang_assets",
    "amortization_of_long_term_deferred_expenses": "lt_amort_deferred_exp",
    "financial_expenses": "finan_exp",
    "investment_losses": "invest_loss",
    "free_cash_flow": "free_cashflow",
}

_INDICATOR_SOURCE_FIELDS = {
    "basic_earnings_per_share": "eps",
    "diluted_earnings_per_share": "dt_eps",
    "net_assets_per_share": "bps",
    "operating_cash_flow_per_share": "ocfps",
    "capital_reserve_per_share": "capital_rese_ps",
    "surplus_reserve_per_share": "surplus_rese_ps",
    "retained_earnings_per_share": "undist_profit_ps",
    "gross_profit": "gross_margin",
    "gross_margin": "grossprofit_margin",
    "net_profit_margin": "netprofit_margin",
    "return_on_equity": "roe",
    "weighted_return_on_equity": "roe_waa",
    "return_on_assets": "roa",
    "return_on_invested_capital": "roic",
    "current_ratio": "current_ratio",
    "quick_ratio": "quick_ratio",
    "cash_ratio": "cash_ratio",
    "debt_to_assets": "debt_to_assets",
    "asset_turnover": "assets_turn",
    "inventory_turnover": "inv_turn",
    "accounts_receivable_turnover": "ar_turn",
    "revenue_year_over_year": "or_yoy",
    "net_income_year_over_year": "netprofit_yoy",
    "operating_cash_flow_year_over_year": "ocf_yoy",
}

_INDICATOR_PERCENT_FIELDS = (
    "gross_margin",
    "net_profit_margin",
    "return_on_equity",
    "weighted_return_on_equity",
    "return_on_assets",
    "return_on_invested_capital",
    "debt_to_assets",
    "revenue_year_over_year",
    "net_income_year_over_year",
    "operating_cash_flow_year_over_year",
)

_FORECAST_SOURCE_FIELDS = {
    "forecast_type": "type",
    "net_income_change_lower_bound": "p_change_min",
    "net_income_change_upper_bound": "p_change_max",
    "net_income_lower_bound": "net_profit_min",
    "net_income_upper_bound": "net_profit_max",
    "prior_period_net_income": "last_parent_net",
    "first_announcement_date": "first_ann_date",
    "summary": "summary",
    "change_reason": "change_reason",
}

_EXPRESS_SOURCE_FIELDS = {
    "operating_revenue": "revenue",
    "operating_profit": "operate_profit",
    "total_profit": "total_profit",
    "net_income_attributable_to_parent": "n_income",
    "total_assets": "total_assets",
    "equity_attributable_to_parent": "total_hldr_eqy_exc_min_int",
    "diluted_earnings_per_share": "diluted_eps",
    "diluted_return_on_equity": "diluted_roe",
    "prior_period_net_income": "yoy_net_profit",
    "net_assets_per_share": "bps",
    "revenue_year_over_year": "yoy_sales",
    "operating_profit_year_over_year": "yoy_op",
    "total_profit_year_over_year": "yoy_tp",
    "net_income_year_over_year": "yoy_dedu_np",
    "earnings_per_share_year_over_year": "yoy_eps",
    "return_on_equity_year_over_year": "yoy_roe",
    "total_assets_growth": "growth_assets",
    "equity_year_over_year": "yoy_equity",
    "net_assets_per_share_growth": "growth_bps",
    "summary": "perf_summary",
    "is_audited": "is_audit",
    "remark": "remark",
}

_EXPRESS_PERCENT_FIELDS = (
    "diluted_return_on_equity",
    "revenue_year_over_year",
    "operating_profit_year_over_year",
    "total_profit_year_over_year",
    "net_income_year_over_year",
    "earnings_per_share_year_over_year",
    "return_on_equity_year_over_year",
    "total_assets_growth",
    "equity_year_over_year",
    "net_assets_per_share_growth",
)

_AUDIT_SOURCE_FIELDS = {
    "audit_opinion": "audit_result",
    "audit_fees": "audit_fees",
    "audit_firm": "audit_agency",
    "signing_accountants": "audit_sign",
}


class TushareAdapter:
    """把已发布的 Tushare Parquet 数据归一为平台字段。"""

    capabilities = _TUSHARE_CAPABILITIES

    def __init__(self, catalog: DataCatalog) -> None:
        self._connection = catalog.connection

    def daily_bars(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        start: date | datetime | None,
        end: date | datetime,
        count: int | None,
        adjustment: Literal["none", "forward"],
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        direction = _sql_direction(order, default="asc")
        if count is None:
            assert start is not None
            params = _query_parameters(
                as_of=as_of,
                as_of_date=as_of.date(),
                symbols=symbols,
                start=start,
                end=end,
                start_date=_local_date(start),
                end_date=_local_date(end),
                fetch_limit=fetch_limit,
            )
            query = f"""
                SELECT ts_code AS symbol,
                       {_day_time("trade_date", "09:30")} AS interval_start,
                       {_day_time("trade_date", "15:00")} AS interval_end,
                       open, high, low, close, pre_close,
                       CAST(vol * 100.0 AS DOUBLE) AS volume,
                       CAST(amount * 1000.0 AS DOUBLE) AS amount
                FROM tushare.daily
                WHERE trade_date IS NOT NULL
                  AND trade_date <= $as_of_date
                  AND trade_date >= $start_date
                  AND trade_date <= $end_date
                  AND {_day_time("trade_date", "16:05")} <= $as_of
                  AND ($symbols IS NULL OR ts_code IN (SELECT unnest($symbols)))
                  AND {_day_time("trade_date", "09:30")} >= $start
                  AND {_day_time("trade_date", "09:30")} < $end
                ORDER BY interval_end {direction}, symbol, interval_start {direction}
                LIMIT $fetch_limit
            """
        else:
            params = _query_parameters(
                as_of=as_of,
                as_of_date=as_of.date(),
                symbols=symbols,
                count=count,
                fetch_limit=fetch_limit,
            )
            query = f"""
                SELECT ts_code AS symbol,
                       {_day_time("trade_date", "09:30")} AS interval_start,
                       {_day_time("trade_date", "15:00")} AS interval_end,
                       open, high, low, close, pre_close,
                       CAST(vol * 100.0 AS DOUBLE) AS volume,
                       CAST(amount * 1000.0 AS DOUBLE) AS amount
                FROM tushare.daily
                WHERE trade_date IS NOT NULL
                  AND trade_date <= $as_of_date
                  AND {_day_time("trade_date", "16:05")} <= $as_of
                  AND ($symbols IS NULL OR ts_code IN (SELECT unnest($symbols)))
                QUALIFY row_number() OVER (
                    PARTITION BY ts_code ORDER BY trade_date DESC
                ) <= $count
                ORDER BY interval_end {direction}, symbol, interval_start {direction}
                LIMIT $fetch_limit
            """

        if adjustment == "none":
            return _fetch(self._connection, query, params, BAR_SCHEMA, columns)
        return self._forward_daily_bars(
            raw_bars_sql=query,
            params=params,
            direction=direction,
            columns=columns,
        )

    def _forward_daily_bars(
        self,
        *,
        raw_bars_sql: str,
        params: Mapping[str, object],
        direction: str,
        columns: tuple[str, ...] | None,
    ) -> pa.Table:
        query = f"""
            WITH raw_bars AS MATERIALIZED (
                {raw_bars_sql}
            ), raw_symbols AS (
                SELECT DISTINCT symbol
                FROM raw_bars
            ), visible_factors AS (
                SELECT f.ts_code AS symbol,
                       f.trade_date,
                       f.adj_factor AS factor
                FROM tushare.adj_factor f
                SEMI JOIN raw_symbols s ON s.symbol = f.ts_code
                WHERE f.trade_date IS NOT NULL
                  AND f.trade_date <= $as_of_date
                  AND {_day_time("f.trade_date", "09:25")} <= $as_of
            ), anchors AS (
                SELECT symbol,
                       arg_max(factor, trade_date) AS anchor
                FROM visible_factors
                GROUP BY symbol
            )
            SELECT b.symbol,
                   b.interval_start,
                   b.interval_end,
                   b.open * f.factor / a.anchor AS open,
                   b.high * f.factor / a.anchor AS high,
                   b.low * f.factor / a.anchor AS low,
                   b.close * f.factor / a.anchor AS close,
                   b.pre_close * f.factor / a.anchor AS pre_close,
                   b.volume,
                   b.amount,
                   f.factor IS NULL OR a.anchor IS NULL OR a.anchor = 0
                       AS __invalid_factor
            FROM raw_bars b
            LEFT JOIN visible_factors f
              ON f.symbol = b.symbol
             AND f.trade_date = CAST(
                 timezone('{_TZ}', b.interval_start) AS DATE
             )
            LEFT JOIN anchors a ON a.symbol = b.symbol
            ORDER BY b.interval_end {direction},
                     b.symbol,
                     b.interval_start {direction}
        """
        adjusted = _fetch(self._connection, query, params, _FORWARD_BAR_SCHEMA)
        if pc.any(adjusted.column("__invalid_factor")).as_py():
            raise DataCapabilityNotSupportedError("Tushare 不能为全部行情提供 PIT 前复权因子")
        return _project_table(adjusted.select(BAR_SCHEMA.names), columns)

    def current(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        params = _query_parameters(
            as_of=as_of,
            trade_date=as_of.date(),
            symbols=symbols,
            fetch_limit=fetch_limit,
        )
        query = f"""
            SELECT
                ts_code AS symbol,
                {_day_time("trade_date", "09:30")} AS event_time,
                open,
                CASE WHEN {_day_time("trade_date", "16:05")} <= $as_of THEN high END AS high,
                CASE WHEN {_day_time("trade_date", "16:05")} <= $as_of THEN low END AS low,
                CASE WHEN {_day_time("trade_date", "16:05")} <= $as_of THEN close END AS last,
                pre_close,
                CASE WHEN {_day_time("trade_date", "16:05")} <= $as_of
                     THEN CAST(vol * 100.0 AS DOUBLE) END AS volume,
                CASE WHEN {_day_time("trade_date", "16:05")} <= $as_of
                     THEN CAST(amount * 1000.0 AS DOUBLE) END AS amount
            FROM tushare.daily
            WHERE trade_date = $trade_date
              AND {_day_time("trade_date", "09:30")} <= $as_of
              AND ($symbols IS NULL OR ts_code IN (SELECT unnest($symbols)))
            QUALIFY row_number() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) = 1
            ORDER BY symbol
            LIMIT $fetch_limit
        """
        return _fetch(self._connection, query, params, CURRENT_SCHEMA, columns)

    def daily_metrics(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        start: date,
        end: date,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        direction = _sql_direction(order, default="asc")
        params = _query_parameters(
            as_of=as_of,
            symbols=symbols,
            start=start,
            end=end,
            fetch_limit=fetch_limit,
        )
        query = f"""
            SELECT ts_code AS symbol, trade_date, close,
                   turnover_rate * 0.01 AS turnover_rate,
                   turnover_rate_f * 0.01 AS turnover_rate_f,
                   volume_ratio, pe, pe_ttm, pb, ps, ps_ttm,
                   dv_ratio * 0.01 AS dv_ratio,
                   dv_ttm * 0.01 AS dv_ttm,
                   total_share * 10000.0 AS total_share,
                   float_share * 10000.0 AS float_share,
                   free_share * 10000.0 AS free_share,
                   total_mv * 10000.0 AS total_mv,
                   circ_mv * 10000.0 AS circ_mv
            FROM tushare.daily_basic
            WHERE trade_date IS NOT NULL
              AND {_day_time("trade_date", "17:05")} <= $as_of
              AND ($symbols IS NULL OR ts_code IN (SELECT unnest($symbols)))
              AND trade_date >= $start
              AND trade_date < $end
            ORDER BY trade_date {direction}, symbol
            LIMIT $fetch_limit
        """
        return _fetch(self._connection, query, params, DAILY_METRICS_SCHEMA, columns)

    def moneyflow(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        start: date,
        end: date,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        direction = _sql_direction(order, default="asc")
        params = _query_parameters(
            as_of=as_of,
            symbols=symbols,
            start=start,
            end=end,
            fetch_limit=fetch_limit,
        )
        query = f"""
            SELECT ts_code AS symbol, trade_date,
                   CAST(buy_sm_vol * 100.0 AS DOUBLE) AS buy_sm_volume,
                   CAST(buy_sm_amount * 10000.0 AS DOUBLE) AS buy_sm_amount,
                   CAST(sell_sm_vol * 100.0 AS DOUBLE) AS sell_sm_volume,
                   CAST(sell_sm_amount * 10000.0 AS DOUBLE) AS sell_sm_amount,
                   CAST(buy_md_vol * 100.0 AS DOUBLE) AS buy_md_volume,
                   CAST(buy_md_amount * 10000.0 AS DOUBLE) AS buy_md_amount,
                   CAST(sell_md_vol * 100.0 AS DOUBLE) AS sell_md_volume,
                   CAST(sell_md_amount * 10000.0 AS DOUBLE) AS sell_md_amount,
                   CAST(buy_lg_vol * 100.0 AS DOUBLE) AS buy_lg_volume,
                   CAST(buy_lg_amount * 10000.0 AS DOUBLE) AS buy_lg_amount,
                   CAST(sell_lg_vol * 100.0 AS DOUBLE) AS sell_lg_volume,
                   CAST(sell_lg_amount * 10000.0 AS DOUBLE) AS sell_lg_amount,
                   CAST(buy_elg_vol * 100.0 AS DOUBLE) AS buy_elg_volume,
                   CAST(buy_elg_amount * 10000.0 AS DOUBLE) AS buy_elg_amount,
                   CAST(sell_elg_vol * 100.0 AS DOUBLE) AS sell_elg_volume,
                   CAST(sell_elg_amount * 10000.0 AS DOUBLE) AS sell_elg_amount,
                   CAST(net_mf_vol * 100.0 AS DOUBLE) AS net_volume,
                   CAST(net_mf_amount * 10000.0 AS DOUBLE) AS net_amount
            FROM tushare.moneyflow
            WHERE trade_date IS NOT NULL
              AND {_next_session_time("trade_date")} <= $as_of
              AND ($symbols IS NULL OR ts_code IN (SELECT unnest($symbols)))
              AND trade_date >= $start
              AND trade_date < $end
            ORDER BY trade_date {direction}, symbol
            LIMIT $fetch_limit
        """
        return _fetch(self._connection, query, params, MONEYFLOW_SCHEMA, columns)

    def suspensions(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        params = _query_parameters(
            as_of=as_of,
            trade_date=as_of.date(),
            symbols=symbols,
            fetch_limit=fetch_limit,
        )
        query = f"""
            WITH source AS MATERIALIZED (
                SELECT *
                FROM tushare.suspend_d
                WHERE trade_date = $trade_date
                  AND ($symbols IS NULL OR ts_code IN (SELECT unnest($symbols)))
            ), suspensions AS (
                SELECT *,
                       CASE WHEN suspend_timing IS NOT NULL THEN coalesce(
                           timezone(
                               '{_TZ}',
                               CAST(trade_date AS DATE) + CAST(try_strptime(
                                   regexp_extract(
                                       suspend_timing,
                                       '([0-2][0-9]:[0-5][0-9])',
                                       1
                                   ),
                                   '%H:%M'
                               ) AS TIME)
                           ),
                           CAST(error('{_SUSPENSION_TIMING_ERROR}') AS TIMESTAMPTZ)
                       ) END AS interval_start,
                       CASE WHEN suspend_timing IS NOT NULL THEN coalesce(
                           timezone(
                               '{_TZ}',
                               CAST(trade_date AS DATE) + CAST(try_strptime(
                                   regexp_extract(
                                       suspend_timing,
                                       '[0-2][0-9]:[0-5][0-9][^0-2]*([0-2][0-9]:[0-5][0-9])',
                                       1
                                   ),
                                   '%H:%M'
                               ) AS TIME)
                           ),
                           CAST(error('{_SUSPENSION_TIMING_ERROR}') AS TIMESTAMPTZ)
                       ) END AS interval_end
                FROM source
            )
            SELECT ts_code AS symbol,
                   CASE
                       WHEN suspend_type = 'S' AND suspend_timing IS NULL THEN TRUE
                       WHEN suspend_type = 'S' AND interval_end > $as_of THEN TRUE
                       WHEN suspend_type IN ('S', 'R') THEN FALSE
                   END AS suspended
            FROM suspensions
            WHERE (
                  (suspend_timing IS NULL AND {_day_time("trade_date", "09:25")} <= $as_of)
                  OR (suspend_timing IS NOT NULL AND interval_start <= $as_of)
              )
            QUALIFY row_number() OVER (
                PARTITION BY ts_code ORDER BY interval_start DESC NULLS LAST, suspend_type
            ) = 1
            ORDER BY symbol
            LIMIT $fetch_limit
        """
        return _fetch(self._connection, query, params, SUSPENSION_SCHEMA, columns)

    def price_limits(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        params = _query_parameters(
            as_of=as_of,
            trade_date=as_of.date(),
            symbols=symbols,
            fetch_limit=fetch_limit,
        )
        query = f"""
            SELECT ts_code AS symbol, up_limit, down_limit
            FROM tushare.stk_limit
            WHERE trade_date = $trade_date
              AND {_day_time("trade_date", "09:25")} <= $as_of
              AND ($symbols IS NULL OR ts_code IN (SELECT unnest($symbols)))
            ORDER BY symbol
            LIMIT $fetch_limit
        """
        return _fetch(self._connection, query, params, PRICE_LIMIT_SCHEMA, columns)

    def st_status(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        params = _query_parameters(
            as_of=as_of,
            trade_date=as_of.date(),
            symbols=symbols,
            fetch_limit=fetch_limit,
        )
        query = f"""
            SELECT ts_code AS symbol, coalesce(type_name, type) AS st_type
            FROM tushare.stock_st
            WHERE trade_date = $trade_date
              AND {_day_time("trade_date", "09:25")} <= $as_of
              AND ($symbols IS NULL OR ts_code IN (SELECT unnest($symbols)))
            QUALIFY row_number() OVER (PARTITION BY ts_code ORDER BY type NULLS LAST) = 1
            ORDER BY symbol
            LIMIT $fetch_limit
        """
        return _fetch(self._connection, query, params, ST_STATUS_SCHEMA, columns)

    def statements(
        self,
        *,
        kind: Literal["income", "balance_sheet", "cash_flow"],
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        report_start: date | None,
        report_end: date | None,
        company_type: str | None,
        periods: int | None,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        """按平台报表种类分派到 Tushare 的对应原始表。"""
        if kind == "income":
            method = self.income_statements
        elif kind == "balance_sheet":
            method = self.balance_sheets
        elif kind == "cash_flow":
            method = self.cash_flow_statements
        else:
            raise DataCapabilityNotSupportedError(f"Tushare 不支持财报种类 {kind!r}")
        return method(
            as_of=as_of,
            symbols=symbols,
            report_start=report_start,
            report_end=report_end,
            company_type=company_type,
            periods=periods,
            order=order,
            fetch_limit=fetch_limit,
            columns=columns,
        )

    def income_statements(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        report_start: date | None,
        report_end: date | None,
        company_type: str | None,
        periods: int | None,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        return self._statements(
            table="income",
            schema=INCOME_STATEMENT_SCHEMA,
            source_fields=_INCOME_SOURCE_FIELDS,
            expressions={},
            as_of=as_of,
            symbols=symbols,
            report_start=report_start,
            report_end=report_end,
            company_type=company_type,
            periods=periods,
            order=order,
            fetch_limit=fetch_limit,
            columns=columns,
        )

    def balance_sheets(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        report_start: date | None,
        report_end: date | None,
        company_type: str | None,
        periods: int | None,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        return self._statements(
            table="balancesheet",
            schema=BALANCE_SHEET_SCHEMA,
            source_fields=_BALANCE_SHEET_SOURCE_FIELDS,
            expressions={
                "other_receivables": "coalesce(oth_rcv_total, oth_receiv)",
                "fixed_assets": "coalesce(fix_assets_total, fix_assets)",
                "construction_in_progress": "coalesce(cip_total, cip)",
                "other_payables": "coalesce(oth_pay_total, oth_payable)",
            },
            as_of=as_of,
            symbols=symbols,
            report_start=report_start,
            report_end=report_end,
            company_type=company_type,
            periods=periods,
            order=order,
            fetch_limit=fetch_limit,
            columns=columns,
        )

    def cash_flow_statements(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        report_start: date | None,
        report_end: date | None,
        company_type: str | None,
        periods: int | None,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        return self._statements(
            table="cashflow",
            schema=CASH_FLOW_STATEMENT_SCHEMA,
            source_fields=_CASH_FLOW_SOURCE_FIELDS,
            expressions={},
            as_of=as_of,
            symbols=symbols,
            report_start=report_start,
            report_end=report_end,
            company_type=company_type,
            periods=periods,
            order=order,
            fetch_limit=fetch_limit,
            columns=columns,
        )

    def _statements(
        self,
        *,
        table: str,
        schema: pa.Schema,
        source_fields: Mapping[str, str],
        expressions: Mapping[str, str],
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        report_start: date | None,
        report_end: date | None,
        company_type: str | None,
        periods: int | None,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        select_fields = _mapped_select(schema, 6, source_fields, expressions)
        source_company_type: str | None = None
        if company_type is not None:
            source_company_type = _PLATFORM_TO_TUSHARE_COMPANY_TYPE[company_type]
        direction = _sql_direction(order, default="desc")
        params = _query_parameters(
            as_of=as_of,
            symbols=symbols,
            start=report_start,
            end=report_end,
            company_type=source_company_type,
            periods=periods,
            fetch_limit=fetch_limit,
        )
        query = f"""
            WITH visible AS MATERIALIZED (
                SELECT *,
                       {_next_session_time("f_ann_date")} AS visible_at
                FROM tushare.{table}
                WHERE end_date IS NOT NULL
                  AND f_ann_date IS NOT NULL
                  AND report_type IN ('1', '4')
                  AND ($symbols IS NULL OR ts_code IN (SELECT unnest($symbols)))
                  AND ($start IS NULL OR end_date >= $start)
                  AND ($end IS NULL OR end_date < $end)
            ), latest AS (
                SELECT *
                FROM visible
                WHERE visible_at <= $as_of
                QUALIFY row_number() OVER (
                    PARTITION BY ts_code, end_date, comp_type
                    ORDER BY f_ann_date DESC NULLS LAST,
                             ann_date DESC NULLS LAST,
                             CASE report_type WHEN '4' THEN 1 ELSE 2 END,
                             try_cast(update_flag AS INTEGER) DESC NULLS LAST
                ) = 1
            ), ranked AS (
                SELECT *,
                       dense_rank() OVER (
                           PARTITION BY ts_code ORDER BY end_date DESC
                       ) AS period_rank
                FROM latest
            )
            SELECT ts_code AS symbol,
                   end_date AS period_end,
                   visible_at,
                   ann_date AS announcement_date,
                   f_ann_date AS actual_announcement_date,
                   CASE comp_type
                       WHEN '1' THEN 'industrial'
                       WHEN '2' THEN 'bank'
                       WHEN '3' THEN 'insurance'
                       WHEN '4' THEN 'securities'
                   END AS company_type,
                   {select_fields}
            FROM ranked
            WHERE ($periods IS NULL OR period_rank <= $periods)
              AND ($company_type IS NULL OR comp_type = $company_type)
            ORDER BY period_end {direction}, symbol, company_type
            LIMIT $fetch_limit
        """
        return _fetch(self._connection, query, params, schema, columns)

    def financial_indicators(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        report_start: date | None,
        report_end: date | None,
        periods: int | None,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        expressions = _scale_expressions(
            _INDICATOR_SOURCE_FIELDS,
            _INDICATOR_PERCENT_FIELDS,
            0.01,
        )
        select_fields = _mapped_select(
            FINANCIAL_INDICATOR_SCHEMA,
            4,
            _INDICATOR_SOURCE_FIELDS,
            expressions,
        )
        direction = _sql_direction(order, default="desc")
        params = _query_parameters(
            as_of=as_of,
            symbols=symbols,
            start=report_start,
            end=report_end,
            periods=periods,
            fetch_limit=fetch_limit,
        )
        query = f"""
            WITH visible AS MATERIALIZED (
                SELECT *,
                       {_next_session_time("ann_date")} AS visible_at
                FROM tushare.fina_indicator
                WHERE end_date IS NOT NULL
                  AND ann_date IS NOT NULL
                  AND ($symbols IS NULL OR ts_code IN (SELECT unnest($symbols)))
                  AND ($start IS NULL OR end_date >= $start)
                  AND ($end IS NULL OR end_date < $end)
            ), latest AS (
                SELECT *
                FROM visible
                WHERE visible_at <= $as_of
                QUALIFY row_number() OVER (
                    PARTITION BY ts_code, end_date
                    ORDER BY ann_date DESC NULLS LAST,
                             try_cast(update_flag AS INTEGER) DESC NULLS LAST
                ) = 1
            )
            SELECT ts_code AS symbol,
                   end_date AS period_end,
                   visible_at,
                   ann_date AS announcement_date,
                   {select_fields}
            FROM latest
            QUALIFY $periods IS NULL OR dense_rank() OVER (
                PARTITION BY ts_code ORDER BY end_date DESC
            ) <= $periods
            ORDER BY period_end {direction}, symbol
            LIMIT $fetch_limit
        """
        return _fetch(
            self._connection,
            query,
            params,
            FINANCIAL_INDICATOR_SCHEMA,
            columns,
        )

    def disclosures(
        self,
        *,
        kind: Literal["forecast", "express", "audit"],
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        visible_start: datetime | None,
        visible_end: datetime,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        """按平台披露种类分派到 Tushare 的对应原始表。"""
        if kind == "forecast":
            method = self.forecasts
        elif kind == "express":
            method = self.express_reports
        elif kind == "audit":
            method = self.audit_reports
        else:
            raise DataCapabilityNotSupportedError(f"Tushare 不支持披露种类 {kind!r}")
        return method(
            as_of=as_of,
            symbols=symbols,
            visible_start=visible_start,
            visible_end=visible_end,
            order=order,
            fetch_limit=fetch_limit,
            columns=columns,
        )

    def forecasts(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        visible_start: datetime | None,
        visible_end: datetime,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        expressions = _scale_expressions(
            _FORECAST_SOURCE_FIELDS,
            ("net_income_change_lower_bound", "net_income_change_upper_bound"),
            0.01,
        )
        expressions.update(
            _scale_expressions(
                _FORECAST_SOURCE_FIELDS,
                (
                    "net_income_lower_bound",
                    "net_income_upper_bound",
                    "prior_period_net_income",
                ),
                10_000.0,
            )
        )
        return self._disclosures(
            table="forecast",
            schema=FORECAST_SCHEMA,
            source_fields=_FORECAST_SOURCE_FIELDS,
            expressions=expressions,
            has_update_flag=True,
            as_of=as_of,
            symbols=symbols,
            visible_start=visible_start,
            visible_end=visible_end,
            order=order,
            fetch_limit=fetch_limit,
            columns=columns,
        )

    def express_reports(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        visible_start: datetime | None,
        visible_end: datetime,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        expressions = _scale_expressions(
            _EXPRESS_SOURCE_FIELDS,
            _EXPRESS_PERCENT_FIELDS,
            0.01,
        )
        expressions["is_audited"] = "CAST(is_audit AS BOOLEAN)"
        return self._disclosures(
            table="express",
            schema=EXPRESS_SCHEMA,
            source_fields=_EXPRESS_SOURCE_FIELDS,
            expressions=expressions,
            has_update_flag=True,
            as_of=as_of,
            symbols=symbols,
            visible_start=visible_start,
            visible_end=visible_end,
            order=order,
            fetch_limit=fetch_limit,
            columns=columns,
        )

    def audit_reports(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        visible_start: datetime | None,
        visible_end: datetime,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        return self._disclosures(
            table="fina_audit",
            schema=AUDIT_SCHEMA,
            source_fields=_AUDIT_SOURCE_FIELDS,
            expressions={},
            has_update_flag=False,
            as_of=as_of,
            symbols=symbols,
            visible_start=visible_start,
            visible_end=visible_end,
            order=order,
            fetch_limit=fetch_limit,
            columns=columns,
        )

    def _disclosures(
        self,
        *,
        table: str,
        schema: pa.Schema,
        source_fields: Mapping[str, str],
        expressions: Mapping[str, str],
        has_update_flag: bool,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        visible_start: datetime | None,
        visible_end: datetime,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None,
    ) -> pa.Table:
        visible_at = _next_session_time("ann_date")
        update_order = (
            ", try_cast(update_flag AS INTEGER) DESC NULLS LAST" if has_update_flag else ""
        )
        select_fields = _mapped_select(schema, 4, source_fields, expressions)
        direction = _sql_direction(order, default="asc")
        params = _query_parameters(
            as_of=as_of,
            symbols=symbols,
            start=visible_start,
            end=visible_end,
            fetch_limit=fetch_limit,
        )
        query = f"""
            WITH visible AS MATERIALIZED (
                SELECT *,
                       {visible_at} AS visible_at
                FROM tushare.{table}
                WHERE end_date IS NOT NULL
                  AND ann_date IS NOT NULL
                  AND ($symbols IS NULL OR ts_code IN (SELECT unnest($symbols)))
            )
            SELECT ts_code AS symbol, visible_at,
                   end_date AS period_end, ann_date AS announcement_date,
                   {select_fields}
            FROM visible
            WHERE visible_at <= $as_of
              AND ($start IS NULL OR visible_at >= $start)
              AND visible_at <= $end
            QUALIFY row_number() OVER (
                PARTITION BY ts_code, end_date
                ORDER BY ann_date DESC NULLS LAST {update_order}
            ) = 1
            ORDER BY visible_at {direction}, symbol, period_end, announcement_date
            LIMIT $fetch_limit
        """
        return _fetch(self._connection, query, params, schema, columns)

    def dividends(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        visible_start: datetime | None,
        visible_end: datetime,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        visible_at = _next_session_time("imp_ann_date")
        direction = _sql_direction(order, default="asc")
        params = _query_parameters(
            as_of=as_of,
            symbols=symbols,
            start=visible_start,
            end=visible_end,
            fetch_limit=fetch_limit,
        )
        query = f"""
            WITH visible AS MATERIALIZED (
                SELECT *,
                       {visible_at} AS visible_at
                FROM tushare.dividend
                WHERE imp_ann_date IS NOT NULL
                  AND ($symbols IS NULL OR ts_code IN (SELECT unnest($symbols)))
            )
            SELECT ts_code AS symbol, visible_at, end_date, ann_date, div_proc,
                   stk_div AS stock_dividend, stk_bo_rate AS stock_bonus_rate,
                   stk_co_rate AS stock_conversion_rate, cash_div AS cash_dividend,
                   cash_div_tax AS cash_dividend_before_tax, record_date, ex_date, pay_date,
                   div_listdate AS listing_date, imp_ann_date AS implementation_ann_date,
                   base_date, base_share * 10000.0 AS base_share
            FROM visible
            WHERE visible_at <= $as_of
              AND ($start IS NULL OR visible_at >= $start)
              AND visible_at <= $end
            ORDER BY visible_at {direction}, symbol, ex_date, end_date, ann_date,
                     div_proc, implementation_ann_date
            LIMIT $fetch_limit
        """
        return _fetch(self._connection, query, params, DIVIDEND_SCHEMA, columns)

    def adjustment_factors(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        start: date | None,
        end: date | None,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        direction = _sql_direction(order, default="asc")
        params = _query_parameters(
            as_of=as_of,
            symbols=symbols,
            start=start,
            end=end,
            fetch_limit=fetch_limit,
        )
        query = f"""
            SELECT ts_code AS symbol, trade_date, adj_factor AS factor
            FROM tushare.adj_factor
            WHERE trade_date IS NOT NULL
              AND {_day_time("trade_date", "09:25")} <= $as_of
              AND ($symbols IS NULL OR ts_code IN (SELECT unnest($symbols)))
              AND ($start IS NULL OR trade_date >= $start)
              AND ($end IS NULL OR trade_date < $end)
            ORDER BY trade_date {direction}, symbol
            LIMIT $fetch_limit
        """
        return _fetch(self._connection, query, params, ADJUSTMENT_FACTOR_SCHEMA, columns)

    def industry(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        level: Literal[1, 2, 3],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        params = _query_parameters(
            as_of=as_of,
            as_of_date=as_of.date(),
            symbols=symbols,
            fetch_limit=fetch_limit,
        )
        query = f"""
            SELECT ts_code AS symbol, CAST({level} AS TINYINT) AS level,
                   l{level}_code AS industry_code, l{level}_name AS industry_name
            FROM data_internal.sw_industry
            WHERE {_day_time("in_date", "09:25")} <= $as_of
              AND (out_date IS NULL OR out_date > $as_of_date)
              AND ($symbols IS NULL OR ts_code IN (SELECT unnest($symbols)))
            QUALIFY row_number() OVER (
                PARTITION BY ts_code ORDER BY in_date DESC NULLS LAST
            ) = 1
            ORDER BY symbol
            LIMIT $fetch_limit
        """
        return _fetch(self._connection, query, params, INDUSTRY_SCHEMA, columns)

    def sessions(
        self,
        *,
        as_of: datetime,
        start: date,
        end: date,
        exchange: str | None,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        direction = _sql_direction(order, default="asc")
        params = _query_parameters(
            as_of_date=as_of.date(),
            start=start,
            end=end,
            exchange=exchange,
            fetch_limit=fetch_limit,
        )
        query = f"""
            SELECT cal_date,
                   exchange,
                   CAST(is_open AS BOOLEAN) AS is_open,
                   pretrade_date AS previous_session
            FROM data_internal.trade_cal
            WHERE cal_date <= $as_of_date
              AND cal_date >= $start
              AND cal_date < $end
              AND ($exchange IS NULL OR exchange = $exchange)
            ORDER BY cal_date {direction}, exchange
            LIMIT $fetch_limit
        """
        return _fetch(self._connection, query, params, SESSION_SCHEMA, columns)

    def previous_session(
        self,
        *,
        end: date,
        exchange: str,
    ) -> pa.Table:
        query = """
            SELECT cal_date,
                   exchange
            FROM data_internal.trade_cal
            WHERE cal_date < $end
              AND CAST(is_open AS BOOLEAN)
              AND exchange = $exchange
            ORDER BY cal_date DESC
            LIMIT 1
        """
        return _fetch(
            self._connection,
            query,
            {"end": end, "exchange": exchange},
            pa.schema([SESSION_SCHEMA.field("cal_date"), SESSION_SCHEMA.field("exchange")]),
        )


class QmtAdapter:
    """把 QMT 下载历史行情和已接收实时事件归一为平台字段。"""

    capabilities = _QMT_CAPABILITIES

    def __init__(self, catalog: DataCatalog) -> None:
        self._connection = catalog.connection

    def daily_bars(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        start: date | datetime | None,
        end: date | datetime,
        count: int | None,
        adjustment: Literal["none", "forward"],
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        if adjustment == "forward":
            raise DataCapabilityNotSupportedError(
                "QMT 历史复权行情缺少 PIT 因子可见性，暂不支持 adjustment='forward'"
            )
        direction = _sql_direction(order, default="asc")
        if count is None:
            assert start is not None
            params = _query_parameters(
                as_of=as_of,
                as_of_date=as_of.date(),
                symbols=symbols,
                start=start,
                end=end,
                start_date=_local_date(start),
                end_date=_local_date(end),
                fetch_limit=fetch_limit,
            )
            query = f"""
                SELECT code AS symbol,
                       {_day_time("trade_date", "09:30")} AS interval_start,
                       {_day_time("trade_date", "15:00")} AS interval_end,
                       open, high, low, close, preClose AS pre_close,
                       {_qmt_share_volume("volume")} AS volume,
                       amount
                FROM qmt.daily
                WHERE adjustment = 'none'
                  AND trade_date <= $as_of_date
                  AND trade_date >= $start_date
                  AND trade_date <= $end_date
                  AND ($symbols IS NULL OR code IN (SELECT unnest($symbols)))
                  AND {_day_time("trade_date", "16:05")} <= $as_of
                  AND {_day_time("trade_date", "09:30")} >= $start
                  AND {_day_time("trade_date", "09:30")} < $end
                ORDER BY interval_end {direction}, symbol, interval_start {direction}
                LIMIT $fetch_limit
            """
        else:
            params = _query_parameters(
                as_of=as_of,
                as_of_date=as_of.date(),
                symbols=symbols,
                count=count,
                fetch_limit=fetch_limit,
            )
            query = f"""
                SELECT code AS symbol,
                       {_day_time("trade_date", "09:30")} AS interval_start,
                       {_day_time("trade_date", "15:00")} AS interval_end,
                       open, high, low, close, preClose AS pre_close,
                       {_qmt_share_volume("volume")} AS volume,
                       amount
                FROM qmt.daily
                WHERE adjustment = 'none'
                  AND trade_date <= $as_of_date
                  AND ($symbols IS NULL OR code IN (SELECT unnest($symbols)))
                  AND {_day_time("trade_date", "16:05")} <= $as_of
                QUALIFY row_number() OVER (
                    PARTITION BY code ORDER BY trade_date DESC
                ) <= $count
                ORDER BY interval_end {direction}, symbol, interval_start {direction}
                LIMIT $fetch_limit
            """
        return _fetch(self._connection, query, params, BAR_SCHEMA, columns)

    def intraday_bars(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        frequency: str,
        start: date | datetime | None,
        end: date | datetime,
        count: int | None,
        adjustment: Literal["none", "forward"],
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        period = {
            "1m": ("1m", 1),
            "5m": ("5m", 5),
            "15m": ("15m", 15),
            "30m": ("30m", 30),
            "60m": ("1h", 60),
        }.get(frequency)
        if period is None:
            raise DataCapabilityNotSupportedError(f"QMT 不支持分钟周期 {frequency!r}")
        if adjustment == "forward":
            raise DataCapabilityNotSupportedError(
                "QMT 历史复权行情缺少 PIT 因子可见性，暂不支持 adjustment='forward'"
            )
        qmt_period, minutes = period
        start_expr = _epoch_time("event_time")
        end_expr = f"{start_expr} + INTERVAL '{minutes} minutes'"
        qmt_adjustment = "none"
        direction = _sql_direction(order, default="asc")
        as_of_us = _epoch_us(as_of)
        assert as_of_us is not None
        params = _query_parameters(
            period=qmt_period,
            adjustment=qmt_adjustment,
            as_of=as_of,
            as_of_date=as_of.date(),
            as_of_us=as_of_us,
            latest_start_us=as_of_us - minutes * 60 * 1_000_000,
            symbols=symbols,
            start=start,
            end=end,
            start_us=_epoch_us(start),
            end_us=_epoch_us(end),
            start_date=_local_date(start) if start is not None else None,
            end_date=_local_date(end),
            count=count,
            fetch_limit=fetch_limit,
        )
        query = f"""
            WITH candidates AS (
                SELECT code,
                       event_time,
                       open, high, low, close, preClose,
                       CAST(volume * 100.0 AS DOUBLE) AS volume,
                       amount,
                       CAST(NULL AS BIGINT) AS received_at,
                       CAST(0 AS BIGINT) AS seq
                FROM qmt.intraday
                WHERE period = $period
                  AND adjustment = $adjustment
                  AND trading_date <= $as_of_date
                  AND ($symbols IS NULL OR code IN (SELECT unnest($symbols)))
                  AND ($start_date IS NULL OR trading_date >= $start_date)
                  AND trading_date <= $end_date
                  AND event_time <= $latest_start_us
                  AND ($start_us IS NULL OR event_time >= $start_us)
                  AND ($end_us IS NULL OR event_time < $end_us)

                UNION ALL

                SELECT code,
                       event_time,
                       quote.open, quote.high, quote.low, quote.close, quote.preClose,
                       {_qmt_share_volume("quote.volume")} AS volume,
                       quote.amount,
                       received_at,
                       seq
                FROM qmt.bars
                WHERE period = $period
                  AND trading_date <= $as_of_date
                  AND ($symbols IS NULL OR code IN (SELECT unnest($symbols)))
                  AND ($start_date IS NULL OR trading_date >= $start_date)
                  AND trading_date <= $end_date
                  AND event_time IS NOT NULL
                  AND received_at <= $as_of_us
                  AND event_time <= $latest_start_us
                  AND ($start_us IS NULL OR event_time >= $start_us)
                  AND ($end_us IS NULL OR event_time < $end_us)
            ), latest AS (
                SELECT *
                FROM candidates
                QUALIFY row_number() OVER (
                    PARTITION BY code, event_time
                    ORDER BY received_at DESC NULLS LAST, seq DESC
                ) = 1
            ), platform_bars AS (
                SELECT code AS symbol,
                       {start_expr} AS interval_start,
                       {end_expr} AS interval_end,
                       open, high, low, close,
                       preClose AS pre_close,
                       volume,
                       amount,
                       event_time
                FROM latest
            )
            SELECT symbol,
                   interval_start,
                   interval_end,
                   open, high, low, close, pre_close, volume, amount
            FROM platform_bars
            WHERE interval_end <= $as_of
              AND ($start IS NULL OR interval_start >= $start)
              AND ($end IS NULL OR interval_start < $end)
            QUALIFY $count IS NULL OR row_number() OVER (
                PARTITION BY symbol ORDER BY event_time DESC
            ) <= $count
            ORDER BY interval_end {direction}, symbol, interval_start {direction}
            LIMIT $fetch_limit
        """
        return _fetch(self._connection, query, params, BAR_SCHEMA, columns)

    def current(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        as_of_us = _epoch_us(as_of)
        assert as_of_us is not None
        params = _query_parameters(
            as_of_date=as_of.date(),
            as_of_us=as_of_us,
            symbols=symbols,
            fetch_limit=fetch_limit,
        )
        query = f"""
            SELECT code AS symbol,
                   {_epoch_time("event_time")} AS event_time,
                   quote.open AS open,
                   quote.high AS high,
                   quote.low AS low,
                   quote.lastPrice AS last,
                   quote.lastClose AS pre_close,
                   {_qmt_share_volume("quote.volume", "quote.pvolume")} AS volume,
                   quote.amount AS amount
            FROM qmt.ticks
            WHERE trading_date = $as_of_date
              AND received_at <= $as_of_us
              AND ($symbols IS NULL OR code IN (SELECT unnest($symbols)))
            QUALIFY row_number() OVER (
                PARTITION BY code ORDER BY received_at DESC, seq DESC
            ) = 1
            ORDER BY code
            LIMIT $fetch_limit
        """
        return _fetch(self._connection, query, params, CURRENT_SCHEMA, columns)


def _fetch(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    params: Mapping[str, object],
    schema: pa.Schema | None = None,
    columns: tuple[str, ...] | None = None,
) -> pa.Table:
    output_schema = schema
    if columns is not None:
        if schema is None:
            raise ValueError("字段投影需要平台 Schema")
        unknown = set(columns) - set(schema.names)
        if unknown:
            raise ValueError(f"投影包含未知平台字段: {unknown}")
        selected = ", ".join(_quote(name) for name in columns)
        query = f"SELECT {selected} FROM ({query}) AS platform_result"
        output_schema = pa.schema(schema.field(name) for name in columns)
    try:
        table = connection.execute(query, params).to_arrow_table()
    except duckdb.Error as exc:
        message = str(exc)
        if _CALENDAR_COVERAGE_ERROR in message:
            raise DataSourceUnavailableError("交易日历未覆盖计算可见时间所需的下一交易日") from exc
        if _SUSPENSION_TIMING_ERROR in message:
            raise DataSourceUnavailableError("Tushare 停牌时段格式无效") from exc
        raise DataSourceUnavailableError("读取已发布数据失败") from exc
    return _coerce_schema(table, output_schema) if output_schema is not None else table


def _coerce_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
    if table.schema.names != schema.names:
        raise ValueError(f"平台字段不匹配: 期望 {schema.names}，实际 {table.schema.names}")
    arrays = [pc.cast(table.column(field.name), field.type) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def _project_table(table: pa.Table, columns: tuple[str, ...] | None) -> pa.Table:
    return table if columns is None else table.select(columns)


def _mapped_select(
    schema: pa.Schema,
    identity_count: int,
    source_fields: Mapping[str, str],
    expressions: Mapping[str, str] | None = None,
) -> str:
    """按平台 Schema 顺序生成供应商字段映射。"""
    targets = schema.names[identity_count:]
    if set(source_fields) != set(targets):
        raise ValueError(f"平台字段映射不完整: {set(targets) ^ set(source_fields)}")
    overrides = expressions or {}
    unknown_overrides = set(overrides) - set(targets)
    if unknown_overrides:
        raise ValueError(f"平台字段表达式未知: {unknown_overrides}")
    return ", ".join(
        f"{overrides.get(target, _quote(source_fields[target]))} AS {_quote(target)}"
        for target in targets
    )


def _scale_expressions(
    source_fields: dict[str, str],
    targets: tuple[str, ...],
    scale: float,
) -> dict[str, str]:
    return {target: f"{_quote(source_fields[target])} * {scale}" for target in targets}


def _qmt_share_volume(volume: str, raw_volume: str | None = None) -> str:
    value = f"coalesce({raw_volume}, {volume} * 100.0)" if raw_volume else f"{volume} * 100.0"
    return f"CAST({value} AS DOUBLE)"


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _day_time(column: str, clock: str) -> str:
    return f"timezone('{_TZ}', CAST({column} AS DATE) + TIME '{clock}')"


def _epoch_time(column: str) -> str:
    return f"to_timestamp({column} / 1000000.0)"


def _next_session_time(column: str) -> str:
    next_date = (
        "coalesce((SELECT min(cal_date) FROM data_internal.trade_cal "
        f"WHERE is_open = 1 AND cal_date > {column}), "
        f"CAST(error('{_CALENDAR_COVERAGE_ERROR}') AS DATE))"
    )
    return f"timezone('{_TZ}', CAST({next_date} AS DATE) + TIME '09:25')"


def _query_parameters(**values: object | None) -> dict[str, object]:
    """构造 DuckDB 具名参数；保留 None，让可选条件可以直接写在 SQL 里。"""
    return {
        name: list(value) if isinstance(value, tuple) else value for name, value in values.items()
    }


def _epoch_us(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        instant = value
    elif isinstance(value, date):
        instant = datetime(value.year, value.month, value.day, tzinfo=ZoneInfo(_TZ))
    else:
        raise TypeError(f"无法转换为微秒时间戳: {value!r}")
    return int(instant.timestamp() * 1_000_000)


def _local_date(value: date | datetime) -> date:
    if isinstance(value, datetime):
        return value.astimezone(ZoneInfo(_TZ)).date()
    return value


def _sql_direction(value: object, *, default: str) -> str:
    order = default if value is None else value
    if order == "asc":
        return "ASC"
    if order == "desc":
        return "DESC"
    raise ValueError(f"无效 SQL 排序方向: {order!r}")
