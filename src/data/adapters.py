"""Tushare 与 QMT 到平台统一字段的内置适配器。"""

from __future__ import annotations

from datetime import datetime
from typing import cast

import duckdb
import pyarrow as pa
import pyarrow.compute as pc

from data.catalog import CatalogSnapshot, DataCatalog
from data.errors import DataCapabilityNotSupportedError, DataSourceUnavailableError
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
    SourceRequest,
    SourceSnapshot,
)

_TZ = "Asia/Shanghai"

_TUSHARE_COMPANY_TYPES = (
    ("1", "industrial"),
    ("2", "bank"),
    ("3", "insurance"),
    ("4", "securities"),
)
_PLATFORM_TO_TUSHARE_COMPANY_TYPE = {
    platform: source for source, platform in _TUSHARE_COMPANY_TYPES
}
_COMPANY_TYPE_SQL = (
    "CASE comp_type "
    + " ".join(f"WHEN '{source}' THEN '{platform}'" for source, platform in _TUSHARE_COMPANY_TYPES)
    + " END"
)

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
    "cash_and_cash_equivalents": "money_cap",
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
    """把已发布的 Tushare Parquet 快照归一为平台字段。"""

    source_id = "tushare"

    def __init__(self, catalog: DataCatalog) -> None:
        self._catalog = catalog

    def capabilities(self) -> frozenset[DataCapability]:
        return _TUSHARE_CAPABILITIES

    def open_snapshot(self, as_of: datetime) -> SourceSnapshot:
        try:
            handle = self._catalog.open_snapshot(self.source_id)
        except (OSError, ValueError, duckdb.Error) as exc:
            raise DataSourceUnavailableError("无法打开 Tushare 已发布快照") from exc
        return SourceSnapshot(self.source_id, handle.snapshot_id, handle)

    def read(self, request: SourceRequest, snapshot: SourceSnapshot) -> pa.Table:
        connection = _connection(snapshot, self.source_id)
        readers = {
            DataCapability.DAILY_BARS: self._daily_bars,
            DataCapability.REALTIME_QUOTES: self._current,
            DataCapability.DAILY_METRICS: self._daily_metrics,
            DataCapability.MONEYFLOW: self._moneyflow,
            DataCapability.SUSPENSIONS: self._suspensions,
            DataCapability.PRICE_LIMITS: self._price_limits,
            DataCapability.ST_STATUS: self._st_status,
            DataCapability.INCOME: self._statements,
            DataCapability.BALANCE_SHEET: self._statements,
            DataCapability.CASHFLOW: self._statements,
            DataCapability.INDICATORS: self._indicators,
            DataCapability.FORECAST: self._disclosures,
            DataCapability.EXPRESS: self._disclosures,
            DataCapability.AUDIT: self._disclosures,
            DataCapability.DIVIDENDS: self._dividends,
            DataCapability.ADJUSTMENT_FACTORS: self._adjustment_factors,
            DataCapability.INDUSTRY: self._industry,
            DataCapability.SESSIONS: self._sessions,
        }
        try:
            reader = readers[request.dataset]
        except KeyError:
            raise DataCapabilityNotSupportedError(
                f"Tushare 不支持逻辑数据集 {request.dataset!r}"
            ) from None
        return reader(connection, request)

    def _daily_bars(
        self,
        connection: duckdb.DuckDBPyConnection,
        request: SourceRequest,
    ) -> pa.Table:
        if request.parameters["frequency"] != "1d":
            raise DataCapabilityNotSupportedError("Tushare 当前只支持平台日线")
        params: list[object] = [request.as_of]
        symbol_sql = _symbol_filter("ts_code", request.symbols, params)
        start_sql = _range_filter(
            _day_time("trade_date", "09:30"),
            request.parameters.get("start"),
            request.parameters.get("end"),
            params,
        )
        query = f"""
            SELECT
                ts_code AS symbol,
                {_day_time("trade_date", "09:30")} AS interval_start,
                {_day_time("trade_date", "15:00")} AS interval_end,
                open, high, low, close, pre_close,
                CAST(vol * 100.0 AS DOUBLE) AS volume,
                CAST(amount * 1000.0 AS DOUBLE) AS amount
            FROM tushare.daily
            WHERE trade_date IS NOT NULL
              AND {_day_time("trade_date", "16:05")} <= ?
              {symbol_sql}
              {start_sql}
        """
        return _fetch(connection, query, params, BAR_SCHEMA)

    def _current(
        self,
        connection: duckdb.DuckDBPyConnection,
        request: SourceRequest,
    ) -> pa.Table:
        params: list[object] = [request.as_of.date(), request.as_of]
        symbol_sql = _symbol_filter("ts_code", request.symbols, params)
        query = f"""
            SELECT
                ts_code AS symbol,
                {_day_time("trade_date", "09:30")} AS event_time,
                open,
                CASE WHEN {_day_time("trade_date", "16:05")} <= ? THEN high END AS high,
                CASE WHEN {_day_time("trade_date", "16:05")} <= ? THEN low END AS low,
                CASE WHEN {_day_time("trade_date", "16:05")} <= ? THEN close END AS last,
                pre_close,
                CASE WHEN {_day_time("trade_date", "16:05")} <= ?
                     THEN CAST(vol * 100.0 AS DOUBLE) END AS volume,
                CASE WHEN {_day_time("trade_date", "16:05")} <= ?
                     THEN CAST(amount * 1000.0 AS DOUBLE) END AS amount
            FROM tushare.daily
            WHERE trade_date = ?
              AND {_day_time("trade_date", "09:30")} <= ?
              {symbol_sql}
            QUALIFY row_number() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) = 1
        """
        # The five repeated comparisons precede the date/as-of WHERE parameters.
        params = [request.as_of] * 5 + params
        return _fetch(connection, query, params, CURRENT_SCHEMA)

    def _daily_metrics(
        self,
        connection: duckdb.DuckDBPyConnection,
        request: SourceRequest,
    ) -> pa.Table:
        params: list[object] = [request.as_of]
        symbol_sql = _symbol_filter("ts_code", request.symbols, params)
        range_sql = _range_filter(
            "trade_date",
            request.parameters.get("start"),
            request.parameters.get("end"),
            params,
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
              AND {_day_time("trade_date", "17:05")} <= ?
              {symbol_sql}
              {range_sql}
        """
        return _fetch(connection, query, params, DAILY_METRICS_SCHEMA)

    def _moneyflow(
        self,
        connection: duckdb.DuckDBPyConnection,
        request: SourceRequest,
    ) -> pa.Table:
        params: list[object] = [request.as_of]
        symbol_sql = _symbol_filter("ts_code", request.symbols, params)
        range_sql = _range_filter(
            "trade_date",
            request.parameters.get("start"),
            request.parameters.get("end"),
            params,
        )
        pairs = (
            ("buy_sm_vol", "buy_sm_volume", 100.0),
            ("buy_sm_amount", "buy_sm_amount", 10000.0),
            ("sell_sm_vol", "sell_sm_volume", 100.0),
            ("sell_sm_amount", "sell_sm_amount", 10000.0),
            ("buy_md_vol", "buy_md_volume", 100.0),
            ("buy_md_amount", "buy_md_amount", 10000.0),
            ("sell_md_vol", "sell_md_volume", 100.0),
            ("sell_md_amount", "sell_md_amount", 10000.0),
            ("buy_lg_vol", "buy_lg_volume", 100.0),
            ("buy_lg_amount", "buy_lg_amount", 10000.0),
            ("sell_lg_vol", "sell_lg_volume", 100.0),
            ("sell_lg_amount", "sell_lg_amount", 10000.0),
            ("buy_elg_vol", "buy_elg_volume", 100.0),
            ("buy_elg_amount", "buy_elg_amount", 10000.0),
            ("sell_elg_vol", "sell_elg_volume", 100.0),
            ("sell_elg_amount", "sell_elg_amount", 10000.0),
            ("net_mf_vol", "net_volume", 100.0),
            ("net_mf_amount", "net_amount", 10000.0),
        )
        values = ", ".join(
            f"CAST({source} * {scale} AS DOUBLE) AS {target}" for source, target, scale in pairs
        )
        query = f"""
            SELECT ts_code AS symbol, trade_date, {values}
            FROM tushare.moneyflow
            WHERE trade_date IS NOT NULL
              AND {_next_session_time("trade_date")} <= ?
              {symbol_sql}
              {range_sql}
        """
        return _fetch(connection, query, params, MONEYFLOW_SCHEMA)

    def _suspensions(
        self,
        connection: duckdb.DuckDBPyConnection,
        request: SourceRequest,
    ) -> pa.Table:
        interval_start = (
            "timezone('Asia/Shanghai', CAST(trade_date AS DATE) + "
            "CAST(try_strptime(regexp_extract(suspend_timing, "
            "'([0-2][0-9]:[0-5][0-9])', 1), '%H:%M') AS TIME))"
        )
        interval_end = (
            "timezone('Asia/Shanghai', CAST(trade_date AS DATE) + "
            "CAST(try_strptime(regexp_extract(suspend_timing, "
            "'[0-2][0-9]:[0-5][0-9][^0-2]*([0-2][0-9]:[0-5][0-9])', 1), "
            "'%H:%M') AS TIME))"
        )
        params: list[object] = [request.as_of, request.as_of.date(), request.as_of, request.as_of]
        symbol_sql = _symbol_filter("ts_code", request.symbols, params)
        query = f"""
            SELECT ts_code AS symbol,
                   CASE WHEN suspend_type = 'S'
                                  AND (suspend_timing IS NULL OR {interval_end} > ?)
                             THEN TRUE
                        WHEN suspend_type = 'R' THEN FALSE END AS suspended
            FROM tushare.suspend_d
            WHERE trade_date = ?
              AND ((suspend_timing IS NULL AND {_day_time("trade_date", "09:25")} <= ?)
                   OR (suspend_timing IS NOT NULL AND {interval_start} <= ?))
              {symbol_sql}
            QUALIFY row_number() OVER (
                PARTITION BY ts_code ORDER BY suspend_timing DESC NULLS LAST, suspend_type
            ) = 1
        """
        return _fetch(connection, query, params, SUSPENSION_SCHEMA)

    def _price_limits(
        self,
        connection: duckdb.DuckDBPyConnection,
        request: SourceRequest,
    ) -> pa.Table:
        params: list[object] = [request.as_of.date(), request.as_of]
        symbol_sql = _symbol_filter("ts_code", request.symbols, params)
        query = f"""
            SELECT ts_code AS symbol, up_limit, down_limit
            FROM tushare.stk_limit
            WHERE trade_date = ? AND {_day_time("trade_date", "09:25")} <= ?
              {symbol_sql}
        """
        return _fetch(connection, query, params, PRICE_LIMIT_SCHEMA)

    def _st_status(
        self,
        connection: duckdb.DuckDBPyConnection,
        request: SourceRequest,
    ) -> pa.Table:
        params: list[object] = [request.as_of.date(), request.as_of]
        symbol_sql = _symbol_filter("ts_code", request.symbols, params)
        query = f"""
            SELECT ts_code AS symbol, coalesce(type_name, type) AS st_type
            FROM tushare.stock_st
            WHERE trade_date = ? AND {_day_time("trade_date", "09:25")} <= ?
              {symbol_sql}
            QUALIFY row_number() OVER (PARTITION BY ts_code ORDER BY type NULLS LAST) = 1
        """
        return _fetch(connection, query, params, ST_STATUS_SCHEMA)

    def _statements(
        self,
        connection: duckdb.DuckDBPyConnection,
        request: SourceRequest,
    ) -> pa.Table:
        table, schema, source_fields = {
            DataCapability.INCOME: (
                "income",
                INCOME_STATEMENT_SCHEMA,
                _INCOME_SOURCE_FIELDS,
            ),
            DataCapability.BALANCE_SHEET: (
                "balancesheet",
                BALANCE_SHEET_SCHEMA,
                _BALANCE_SHEET_SOURCE_FIELDS,
            ),
            DataCapability.CASHFLOW: (
                "cashflow",
                CASH_FLOW_STATEMENT_SCHEMA,
                _CASH_FLOW_SOURCE_FIELDS,
            ),
        }[request.dataset]
        expressions: dict[str, str] = {}
        if request.dataset == DataCapability.BALANCE_SHEET:
            expressions = {
                "other_receivables": "coalesce(oth_rcv_total, oth_receiv)",
                "fixed_assets": "coalesce(fix_assets_total, fix_assets)",
                "construction_in_progress": "coalesce(cip_total, cip)",
                "other_payables": "coalesce(oth_pay_total, oth_payable)",
            }
        select_fields = _mapped_select(schema, 6, source_fields, expressions)
        params: list[object] = [request.as_of]
        symbol_sql = _symbol_filter("ts_code", request.symbols, params)
        range_sql = _range_filter(
            "end_date",
            request.parameters.get("report_start"),
            request.parameters.get("report_end"),
            params,
        )
        company_type_sql = ""
        if request.parameters.get("company_type") is not None:
            company_type_sql = "AND comp_type = ?"
            company_type = cast(str, request.parameters["company_type"])
            params.append(_PLATFORM_TO_TUSHARE_COMPANY_TYPE[company_type])
        query = f"""
            WITH visible AS (
                SELECT *
                FROM tushare.{table}
                WHERE end_date IS NOT NULL AND f_ann_date IS NOT NULL
                  AND report_type = '1'
                  AND {_next_session_time("f_ann_date")} <= ?
                  {symbol_sql}
                  {range_sql}
            ), latest AS (
                SELECT * FROM visible
                QUALIFY row_number() OVER (
                    PARTITION BY ts_code, end_date, comp_type
                    ORDER BY f_ann_date DESC NULLS LAST, ann_date DESC NULLS LAST,
                             try_cast(update_flag AS INTEGER) DESC NULLS LAST
                ) = 1
            )
            SELECT ts_code AS symbol, end_date AS period_end,
                   {_next_session_time("f_ann_date")} AS visible_at,
                   ann_date AS announcement_date,
                   f_ann_date AS actual_announcement_date,
                   {_COMPANY_TYPE_SQL} AS company_type,
                   {select_fields}
            FROM latest
            WHERE TRUE {company_type_sql}
        """
        return _fetch(connection, query, params, schema)

    def _indicators(
        self,
        connection: duckdb.DuckDBPyConnection,
        request: SourceRequest,
    ) -> pa.Table:
        expressions = _scale_expressions(
            _INDICATOR_SOURCE_FIELDS,
            _INDICATOR_PERCENT_FIELDS,
            0.01,
        )
        params: list[object] = [request.as_of]
        symbol_sql = _symbol_filter("ts_code", request.symbols, params)
        range_sql = _range_filter(
            "end_date",
            request.parameters.get("report_start"),
            request.parameters.get("report_end"),
            params,
        )
        query = f"""
            SELECT ts_code AS symbol, end_date AS period_end,
                   {_next_session_time("ann_date")} AS visible_at,
                   ann_date AS announcement_date,
                   {
            _mapped_select(
                FINANCIAL_INDICATOR_SCHEMA,
                4,
                _INDICATOR_SOURCE_FIELDS,
                expressions,
            )
        }
            FROM tushare.fina_indicator
            WHERE end_date IS NOT NULL AND ann_date IS NOT NULL
              AND {_next_session_time("ann_date")} <= ?
              {symbol_sql}
              {range_sql}
            QUALIFY row_number() OVER (
                PARTITION BY ts_code, end_date
                ORDER BY ann_date DESC NULLS LAST,
                         try_cast(update_flag AS INTEGER) DESC NULLS LAST
            ) = 1
        """
        return _fetch(connection, query, params, FINANCIAL_INDICATOR_SCHEMA)

    def _disclosures(
        self,
        connection: duckdb.DuckDBPyConnection,
        request: SourceRequest,
    ) -> pa.Table:
        table, schema, source_fields = {
            DataCapability.FORECAST: ("forecast", FORECAST_SCHEMA, _FORECAST_SOURCE_FIELDS),
            DataCapability.EXPRESS: ("express", EXPRESS_SCHEMA, _EXPRESS_SOURCE_FIELDS),
            DataCapability.AUDIT: ("fina_audit", AUDIT_SCHEMA, _AUDIT_SOURCE_FIELDS),
        }[request.dataset]
        visible = _next_session_time("ann_date")
        params: list[object] = [request.as_of]
        symbol_sql = _symbol_filter("ts_code", request.symbols, params)
        visible_sql = _inclusive_range_filter(
            visible,
            request.parameters.get("visible_start"),
            request.parameters.get("visible_end"),
            params,
        )
        update_order = (
            ", try_cast(update_flag AS INTEGER) DESC NULLS LAST"
            if request.dataset != DataCapability.AUDIT
            else ""
        )
        expressions: dict[str, str] = {}
        if request.dataset == DataCapability.FORECAST:
            expressions = _scale_expressions(
                source_fields,
                ("net_income_change_lower_bound", "net_income_change_upper_bound"),
                0.01,
            )
            expressions.update(
                _scale_expressions(
                    source_fields,
                    (
                        "net_income_lower_bound",
                        "net_income_upper_bound",
                        "prior_period_net_income",
                    ),
                    10_000.0,
                )
            )
        elif request.dataset == DataCapability.EXPRESS:
            expressions = _scale_expressions(
                source_fields,
                _EXPRESS_PERCENT_FIELDS,
                0.01,
            )
            expressions["is_audited"] = "CAST(is_audit AS BOOLEAN)"
        query = f"""
            SELECT ts_code AS symbol, {visible} AS visible_at,
                   end_date AS period_end, ann_date AS announcement_date,
                   {_mapped_select(schema, 4, source_fields, expressions)}
            FROM tushare.{table}
            WHERE end_date IS NOT NULL AND ann_date IS NOT NULL AND {visible} <= ?
              {symbol_sql}
              {visible_sql}
            QUALIFY row_number() OVER (
                PARTITION BY ts_code, end_date
                ORDER BY ann_date DESC NULLS LAST {update_order}
            ) = 1
        """
        return _fetch(connection, query, params, schema)

    def _dividends(
        self,
        connection: duckdb.DuckDBPyConnection,
        request: SourceRequest,
    ) -> pa.Table:
        visible = _next_session_time("imp_ann_date")
        params: list[object] = [request.as_of]
        symbol_sql = _symbol_filter("ts_code", request.symbols, params)
        visible_sql = _inclusive_range_filter(
            visible,
            request.parameters.get("visible_start"),
            request.parameters.get("visible_end"),
            params,
        )
        query = f"""
            SELECT ts_code AS symbol, {visible} AS visible_at, end_date, ann_date, div_proc,
                   stk_div AS stock_dividend, stk_bo_rate AS stock_bonus_rate,
                   stk_co_rate AS stock_conversion_rate, cash_div AS cash_dividend,
                   cash_div_tax AS cash_dividend_before_tax, record_date, ex_date, pay_date,
                   div_listdate AS listing_date, imp_ann_date AS implementation_ann_date,
                   base_date, base_share * 10000.0 AS base_share
            FROM tushare.dividend
            WHERE imp_ann_date IS NOT NULL AND {visible} <= ?
              {symbol_sql}
              {visible_sql}
        """
        return _fetch(connection, query, params, DIVIDEND_SCHEMA)

    def _adjustment_factors(
        self,
        connection: duckdb.DuckDBPyConnection,
        request: SourceRequest,
    ) -> pa.Table:
        params: list[object] = [request.as_of]
        symbol_sql = _symbol_filter("ts_code", request.symbols, params)
        range_sql = _range_filter(
            "trade_date",
            request.parameters.get("start"),
            request.parameters.get("end"),
            params,
        )
        query = f"""
            SELECT ts_code AS symbol, trade_date, adj_factor AS factor
            FROM tushare.adj_factor
            WHERE trade_date IS NOT NULL
              AND {_day_time("trade_date", "09:25")} <= ?
              {symbol_sql}
              {range_sql}
        """
        return _fetch(connection, query, params, ADJUSTMENT_FACTOR_SCHEMA)

    def _industry(
        self,
        connection: duckdb.DuckDBPyConnection,
        request: SourceRequest,
    ) -> pa.Table:
        level = request.parameters["level"]
        params: list[object] = [request.as_of, request.as_of.date()]
        symbol_sql = _symbol_filter("ts_code", request.symbols, params)
        query = f"""
            SELECT ts_code AS symbol, CAST({level} AS TINYINT) AS level,
                   l{level}_code AS industry_code, l{level}_name AS industry_name
            FROM tushare.sw_industry
            WHERE {_day_time("in_date", "09:25")} <= ?
              AND (out_date IS NULL OR out_date > ?)
              {symbol_sql}
            QUALIFY row_number() OVER (
                PARTITION BY ts_code ORDER BY in_date DESC NULLS LAST
            ) = 1
        """
        return _fetch(connection, query, params, INDUSTRY_SCHEMA)

    def _sessions(
        self,
        connection: duckdb.DuckDBPyConnection,
        request: SourceRequest,
    ) -> pa.Table:
        params: list[object] = [request.as_of.date()]
        range_sql = _range_filter(
            "cal_date",
            request.parameters.get("start"),
            request.parameters.get("end"),
            params,
        )
        exchange_sql = ""
        if request.parameters.get("exchange") is not None:
            exchange_sql = "AND exchange = ?"
            params.append(request.parameters["exchange"])
        query = f"""
            SELECT cal_date, exchange, CAST(is_open AS BOOLEAN) AS is_open,
                   pretrade_date AS previous_session
            FROM tushare.trade_cal
            WHERE cal_date <= ? {range_sql} {exchange_sql}
        """
        return _fetch(connection, query, params, SESSION_SCHEMA)


class QmtAdapter:
    """把 QMT 下载日线和已接收实时事件归一为平台字段。"""

    source_id = "qmt"

    def __init__(self, catalog: DataCatalog) -> None:
        self._catalog = catalog

    def capabilities(self) -> frozenset[DataCapability]:
        return _QMT_CAPABILITIES

    def open_snapshot(self, as_of: datetime) -> SourceSnapshot:
        try:
            handle = self._catalog.open_snapshot(self.source_id)
        except (OSError, ValueError, duckdb.Error) as exc:
            raise DataSourceUnavailableError("无法打开 QMT 已发布快照") from exc
        return SourceSnapshot(self.source_id, handle.snapshot_id, handle)

    def read(self, request: SourceRequest, snapshot: SourceSnapshot) -> pa.Table:
        connection = _connection(snapshot, self.source_id)
        if request.dataset == DataCapability.DAILY_BARS:
            return self._daily_bars(connection, request)
        if request.dataset == DataCapability.INTRADAY_BARS:
            return self._intraday_bars(connection, request)
        if request.dataset == DataCapability.REALTIME_QUOTES:
            return self._current(connection, request)
        raise DataCapabilityNotSupportedError(f"QMT 不支持逻辑数据集 {request.dataset!r}")

    def _daily_bars(
        self,
        connection: duckdb.DuckDBPyConnection,
        request: SourceRequest,
    ) -> pa.Table:
        if request.parameters["frequency"] != "1d":
            raise DataCapabilityNotSupportedError("QMT 下载日线只支持 1d")
        adjustment = request.parameters["adjustment"]
        qmt_adjustment = "none" if adjustment == "none" else "front_ratio"
        params: list[object] = [qmt_adjustment, request.as_of]
        symbol_sql = _symbol_filter("code", request.symbols, params)
        range_sql = _range_filter(
            _day_time("trade_date", "09:30"),
            request.parameters.get("start"),
            request.parameters.get("end"),
            params,
        )
        query = f"""
            SELECT code AS symbol,
                   {_day_time("trade_date", "09:30")} AS interval_start,
                   {_day_time("trade_date", "15:00")} AS interval_end,
                   open, high, low, close, preClose AS pre_close,
                   {_qmt_share_volume("volume")} AS volume, amount
            FROM qmt.daily
            WHERE adjustment = ? AND {_day_time("trade_date", "16:05")} <= ?
              {symbol_sql}
              {range_sql}
        """
        return _fetch(connection, query, params, BAR_SCHEMA)

    def _intraday_bars(
        self,
        connection: duckdb.DuckDBPyConnection,
        request: SourceRequest,
    ) -> pa.Table:
        frequency = cast(str, request.parameters["frequency"])
        minutes = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "60m": 60}.get(frequency)
        if minutes is None:
            raise DataCapabilityNotSupportedError(f"QMT 不支持分钟周期 {frequency!r}")
        start_expr = _epoch_time("event_time")
        end_expr = f"{start_expr} + INTERVAL '{minutes} minutes'"
        received_expr = _epoch_time("received_at")
        params: list[object] = [frequency, request.as_of, request.as_of]
        symbol_sql = _symbol_filter("code", request.symbols, params)
        range_sql = _range_filter(
            start_expr,
            request.parameters.get("start"),
            request.parameters.get("end"),
            params,
        )
        query = f"""
            SELECT code AS symbol, {start_expr} AS interval_start, {end_expr} AS interval_end,
                   quote.open AS open, quote.high AS high, quote.low AS low,
                   quote.close AS close, quote.preClose AS pre_close,
                   {_qmt_share_volume("quote.volume")} AS volume, quote.amount AS amount
            FROM qmt.bars
            WHERE period = ? AND event_time IS NOT NULL
              AND {received_expr} <= ? AND {end_expr} <= ?
              {symbol_sql}
              {range_sql}
            QUALIFY row_number() OVER (
                PARTITION BY code, period, event_time
                ORDER BY received_at DESC, seq DESC
            ) = 1
        """
        return _fetch(connection, query, params, BAR_SCHEMA)

    def _current(
        self,
        connection: duckdb.DuckDBPyConnection,
        request: SourceRequest,
    ) -> pa.Table:
        params: list[object] = [request.as_of]
        tick_symbols = _symbol_filter("code", request.symbols, params)
        params.append(request.as_of)
        bar_symbols = _symbol_filter("code", request.symbols, params)
        query = f"""
            WITH candidates AS (
                SELECT code AS symbol, event_time, received_at, seq,
                       quote.open AS open, quote.high AS high, quote.low AS low,
                       quote.lastPrice AS last, quote.lastClose AS pre_close,
                       {_qmt_share_volume("quote.volume", "quote.pvolume")} AS volume,
                       quote.amount AS amount
                FROM qmt.ticks
                WHERE {_epoch_time("received_at")} <= ? {tick_symbols}
                UNION ALL
                SELECT code AS symbol, event_time, received_at, seq,
                       quote.open AS open, quote.high AS high, quote.low AS low,
                       quote.close AS last, quote.preClose AS pre_close,
                       {_qmt_share_volume("quote.volume")} AS volume, quote.amount AS amount
                FROM qmt.bars
                WHERE {_epoch_time("received_at")} <= ? {bar_symbols}
            )
            SELECT symbol, {_epoch_time("event_time")} AS event_time,
                   open, high, low, last, pre_close, volume, amount
            FROM candidates
            QUALIFY row_number() OVER (
                PARTITION BY symbol ORDER BY received_at DESC, seq DESC
            ) = 1
        """
        return _fetch(connection, query, params, CURRENT_SCHEMA)


def _connection(snapshot: SourceSnapshot, expected_source: str) -> duckdb.DuckDBPyConnection:
    if snapshot.source_id != expected_source or not isinstance(snapshot.handle, CatalogSnapshot):
        raise DataSourceUnavailableError(f"{expected_source} 快照句柄无效")
    return snapshot.handle.connection


def _fetch(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    params: list[object],
    schema: pa.Schema | None = None,
) -> pa.Table:
    try:
        table = connection.execute(query, params).to_arrow_table()
    except duckdb.Error as exc:
        raise DataSourceUnavailableError("读取已发布数据快照失败") from exc
    return _coerce_schema(table, schema) if schema is not None else table


def _coerce_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
    if table.schema.names != schema.names:
        raise ValueError(f"平台字段不匹配: 期望 {schema.names}，实际 {table.schema.names}")
    arrays = [pc.cast(table.column(field.name), field.type) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def _mapped_select(
    schema: pa.Schema,
    identity_count: int,
    source_fields: dict[str, str],
    expressions: dict[str, str] | None = None,
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
    # 已发布日历优先；空日历时只退化为工作日，便于读取尚未同步日历的轻量数据集。
    fallback = f"{column} + CASE dayofweek({column}) WHEN 5 THEN 3 WHEN 6 THEN 2 ELSE 1 END"
    next_date = (
        "coalesce((SELECT min(cal_date) FROM tushare.trade_cal "
        f"WHERE is_open = 1 AND cal_date > {column}), {fallback})"
    )
    return f"timezone('{_TZ}', CAST({next_date} AS DATE) + TIME '09:25')"


def _symbol_filter(
    column: str,
    symbols: tuple[str, ...] | None,
    params: list[object],
) -> str:
    if symbols is None:
        return ""
    params.append(list(symbols))
    return f"AND {column} IN (SELECT unnest(?))"


def _range_filter(
    expression: str,
    start: object,
    end: object,
    params: list[object],
) -> str:
    clauses: list[str] = []
    if start is not None:
        clauses.append(f"{expression} >= ?")
        params.append(start)
    if end is not None:
        clauses.append(f"{expression} < ?")
        params.append(end)
    return "" if not clauses else "AND " + " AND ".join(clauses)


def _inclusive_range_filter(
    expression: str,
    start: object,
    end: object,
    params: list[object],
) -> str:
    clauses: list[str] = []
    if start is not None:
        clauses.append(f"{expression} >= ?")
        params.append(start)
    if end is not None:
        clauses.append(f"{expression} <= ?")
        params.append(end)
    return "" if not clauses else "AND " + " AND ".join(clauses)
