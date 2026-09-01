"""随机抽样并比较 Tushare 与 QMT 的日线、财务和除权数据。"""

from __future__ import annotations

import json
import math
import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

from market_data import DataCatalog
from qmt_protocol import DividendFactor, HistoryBar, HistoryQuote
from qmt_receiver import QmtAgentClient, QmtDataStore
from qmt_receiver.sync import SyncResult, sync_all


@dataclass(frozen=True, slots=True)
class Difference:
    check: str
    code: str
    date: str
    field: str
    tushare: object
    qmt: object


@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    compared: int
    differences: tuple[Difference, ...]

    @property
    def passed(self) -> bool:
        return not self.differences


@dataclass(frozen=True, slots=True)
class ValidationReport:
    stocks: tuple[str, ...]
    sync: SyncResult
    checks: tuple[CheckResult, ...]

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)


_FINANCIAL_FIELDS = {
    "Balance": (
        "balancesheet",
        {
            "cash_equivalents": "money_cap",
            "tradable_fin_assets": "trad_asset",
            "derivative_fin_assets": "deriv_assets",
            "bill_receivable": "notes_receiv",
            "account_receivable": "accounts_receiv",
            "advance_payment": "prepayment",
            "int_rcv": "int_receiv",
            "other_receivable": ("oth_receiv", "oth_rcv_total"),
            "inventories": "inventories",
            "other_current_assets": "oth_cur_assets",
            "total_current_assets": "total_cur_assets",
            "fin_assets_avail_for_sale": "fa_avail_for_sale",
            "held_to_mty_invest": "htm_invest",
            "long_term_eqy_invest": "lt_eqt_invest",
            "invest_real_estate": "invest_real_estate",
            "fix_assets": ("fix_assets", "fix_assets_total"),
            "constru_in_process": ("cip", "cip_total"),
            "construction_materials": "const_materials",
            "intang_assets": "intan_assets",
            "goodwill": "goodwill",
            "long_deferred_expense": "lt_amor_exp",
            "deferred_tax_assets": "defer_tax_assets",
            "total_non_current_assets": "total_nca",
            "tot_assets": "total_assets",
            "shortterm_loan": "st_borr",
            "tradable_fin_liab": "trading_fl",
            "empl_ben_payable": "payroll_payable",
            "taxes_surcharges_payable": "taxes_payable",
            "int_payable": "int_payable",
            "bonds_payable": "bond_payable",
            "deferred_tax_liab": "defer_tax_liab",
            "tot_liab": "total_liab",
            "cap_stk": "total_share",
            "cap_rsrv": "cap_rese",
            "surplus_rsrv": "surplus_rese",
            "undistributed_profit": "undistr_porfit",
            "dividend_payable": "div_payable",
            "other_payable": ("oth_payable", "oth_pay_total"),
            "non_current_liability_in_one_year": "non_cur_liab_due_1y",
            "other_current_liability": "oth_cur_liab",
            "longterm_account_payable": "lt_payable",
            "accounts_payable": "acct_payable",
            "advance_peceipts": "contract_liab",
            "total_current_liability": "total_cur_liab",
            "notes_payable": "notes_payable",
            "long_term_loans": "lt_borr",
            "grants_received": "specific_payables",
            "other_non_current_liabilities": "oth_ncl",
            "non_current_liabilities": "total_ncl",
            "specific_reserves": "special_rese",
            "minority_int": "minority_int",
            "tot_shrhldr_eqy_excl_min_int": "total_hldr_eqy_exc_min_int",
            "total_equity": "total_hldr_eqy_inc_min_int",
            "tot_liab_shrhldr_eqy": "total_liab_hldr_eqy",
        },
    ),
    "Income": (
        "income",
        {
            "revenue_inc": "revenue",
            "revenue": "total_revenue",
            "earned_premium": "prem_earned",
            "total_expense": "oper_cost",
            "research_expenses": "rd_exp",
            "change_income_fair_value": "fv_value_chg_gain",
            "int_inc": "int_income",
            "handling_chrg_comm_inc": "comm_income",
            "less_handling_chrg_comm_exp": "comm_exp",
            "other_bus_cost": "other_bus_cost",
            "plus_net_gain_fx_trans": "forex_gain",
            "inc_tax": "income_tax",
            "net_profit_excl_min_int_inc": "n_income_attr_p",
            "less_int_exp": "int_exp",
            "other_bus_inc": "oth_b_income",
            "less_taxes_surcharges_ops": "biz_tax_surchg",
            "sale_expense": "sell_exp",
            "less_gerl_admin_exp": "admin_exp",
            "financial_expense": "fin_exp",
            "less_impair_loss_assets": "assets_impair_loss",
            "plus_net_invest_inc": "invest_income",
            "incl_inc_invest_assoc_jv_entp": "ass_invest_income",
            "oper_profit": "operate_profit",
            "plus_non_oper_rev": "non_oper_income",
            "less_non_oper_exp": "non_oper_exp",
            "tot_profit": "total_profit",
            "net_profit_incl_min_int_inc": "n_income",
            "minority_int_inc": "minority_gain",
            "s_fa_eps_basic": "basic_eps",
            "s_fa_eps_diluted": "diluted_eps",
            "total_income": "t_compr_income",
            "total_income_minority": "compr_inc_attr_m_s",
        },
    ),
    "CashFlow": (
        "cashflow",
        {
            "net_profit": "net_profit",
            "cash_received_ori_ins_contract_pre": "prem_fr_orig_contr",
            "net_cash_received_rei_ope": "n_reinsur_prem",
            "net_increase_insured_funds": "n_incr_insured_dep",
            "cash_for_payment_original_insurance": "c_pay_claims_orig_inco",
            "cash_payment_policy_dividends": "pay_comm_insur_plcy",
            "cash_paid_for_investments": "c_paid_invest",
            "cash_paid_by_subsidiaries": "n_disp_subs_oth_biz",
            "other_cash_recp_ral_oper_act": "c_fr_oth_operate_a",
            "goods_sale_and_service_render_cash": "c_fr_sale_sg",
            "tax_levy_refund": "recp_tax_rends",
            "stot_cash_inflows_oper_act": "c_inf_fr_operate_a",
            "goods_and_services_cash_paid": "c_paid_goods_s",
            "net_incr_clients_loan_adv": "n_incr_clt_loan_adv",
            "net_incr_dep_cbob": "n_incr_dep_cbob",
            "handling_chrg_paid": "pay_handling_chrg",
            "cash_pay_beh_empl": "c_paid_to_for_empl",
            "pay_all_typ_tax": "c_paid_for_taxes",
            "other_cash_pay_ral_oper_act": "oth_cash_pay_oper_act",
            "stot_cash_outflows_oper_act": "st_cash_out_act",
            "net_cash_flows_oper_act": "n_cashflow_act",
            "cash_recp_disp_withdrwl_invest": "c_disp_withdrwl_invest",
            "cash_recp_return_invest": "c_recp_return_invest",
            "net_cash_recp_disp_fiolta": "n_recp_disp_fiolta",
            "other_cash_recp_ral_inv_act": "oth_recp_ral_inv_act",
            "stot_cash_inflows_inv_act": "stot_inflows_inv_act",
            "cash_pay_acq_const_fiolta": "c_pay_acq_const_fiolta",
            "other_cash_pay_ral_inv_act": "oth_pay_ral_inv_act",
            "stot_cash_outflows_inv_act": "stot_out_inv_act",
            "net_cash_flows_inv_act": "n_cashflow_inv_act",
            "cash_recp_cap_contrib": "c_recp_cap_contrib",
            "cash_recp_borrow": "c_recp_borrow",
            "proc_issue_bonds": "proc_issue_bonds",
            "other_cash_recp_ral_fnc_act": "oth_cash_recp_ral_fnc_act",
            "stot_cash_inflows_fnc_act": "stot_cash_in_fnc_act",
            "cash_prepay_amt_borr": "c_prepay_amt_borr",
            "cash_pay_dist_dpcp_int_exp": "c_pay_dist_dpcp_int_exp",
            "other_cash_pay_ral_fnc_act": "oth_cashpay_ral_fnc_act",
            "stot_cash_outflows_fnc_act": "stot_cashout_fnc_act",
            "net_cash_flows_fnc_act": "n_cash_flows_fnc_act",
            "eff_fx_flu_cash": "eff_fx_flu_cash",
            "net_incr_cash_cash_equ": "n_incr_cash_cash_equ",
            "cash_cash_equ_beg_period": "c_cash_equ_beg_period",
            "cash_cash_equ_end_period": "c_cash_equ_end_period",
        },
    ),
    "Pershareindex": (
        "fina_indicator",
        {
            "s_fa_ocfps": "ocfps",
            "s_fa_bps": "bps",
            "s_fa_eps_basic": "eps",
            "s_fa_eps_diluted": "dt_eps",
            "s_fa_undistributedps": "undist_profit_ps",
            "s_fa_surpluscapitalps": "capital_rese_ps",
            "du_return_on_equity": "roe",
            "sales_gross_profit": "grossprofit_margin",
        },
    ),
}


def validate_sample(
    client: QmtAgentClient,
    *,
    tushare_root: str | Path,
    qmt_root: str | Path,
    start_date: date,
    end_date: date,
    sample_size: int = 20,
    seed: int = 0,
) -> ValidationReport:
    """从 Tushare 日线随机抽股票，拉取 QMT 数据并完成四类比较。"""
    with DataCatalog(tushare_root=tushare_root, qmt_root=qmt_root) as catalog:
        stocks = sample_stocks(
            catalog.connection,
            start_date=start_date,
            end_date=end_date,
            sample_size=sample_size,
            seed=seed,
        )

    with QmtDataStore(qmt_root) as store:
        sync_result = sync_all(
            client,
            store,
            stocks,
            start_date.strftime("%Y%m%d"),
            end_date.strftime("%Y%m%d"),
            force=True,
        )
    native_front = client.query_history(
        stocks,
        period="1d",
        start_time=start_date.strftime("%Y%m%d"),
        end_time=end_date.strftime("%Y%m%d"),
        dividend_type="front_ratio",
        fill_data=False,
    ).data
    all_factors = client.query_dividend_factors(stocks).data

    with DataCatalog(tushare_root=tushare_root, qmt_root=qmt_root) as catalog:
        checks = (
            compare_daily(catalog.connection, stocks, start_date, end_date),
            compare_qmt_front_ratio(
                catalog.connection,
                stocks,
                start_date,
                end_date,
                native_front,
                all_factors,
            ),
            compare_financial(catalog.connection, stocks, start_date, end_date),
            compare_dividends(catalog.connection, stocks, start_date, end_date),
        )
    return ValidationReport(tuple(stocks), sync_result, checks)


def sample_stocks(
    connection: duckdb.DuckDBPyConnection,
    *,
    start_date: date,
    end_date: date,
    sample_size: int,
    seed: int = 0,
) -> list[str]:
    if sample_size < 1:
        raise ValueError("sample_size 必须大于等于 1")
    rows = connection.execute(
        "SELECT DISTINCT ts_code FROM tushare.daily "
        "WHERE trade_date BETWEEN ? AND ? ORDER BY ts_code",
        [start_date, end_date],
    ).fetchall()
    population = [row[0] for row in rows]
    if not population:
        raise ValueError("指定区间没有可抽样的 Tushare 日线数据")
    return sorted(random.Random(seed).sample(population, min(sample_size, len(population))))


def compare_daily(
    connection: duckdb.DuckDBPyConnection,
    stocks: list[str],
    start_date: date,
    end_date: date,
) -> CheckResult:
    tushare_rows = _fetch(
        connection,
        "SELECT ts_code, trade_date, open, high, low, close, vol, amount "
        f"FROM tushare.daily WHERE {_stock_filter('ts_code', stocks)} "
        "AND trade_date BETWEEN ? AND ?",
        [*stocks, start_date, end_date],
    )
    qmt_rows = _fetch(
        connection,
        "SELECT code, trade_date, adjustment, open, high, low, close, volume, amount "
        f"FROM qmt.daily WHERE {_stock_filter('code', stocks)} "
        "AND trade_date BETWEEN ? AND ?",
        [*stocks, start_date, end_date],
    )
    factor_rows = _fetch(
        connection,
        "SELECT ts_code, trade_date, adj_factor FROM tushare.adj_factor "
        f"WHERE {_stock_filter('ts_code', stocks)} AND trade_date <= ?",
        [*stocks, end_date],
    )

    tushare = {(row["ts_code"], row["trade_date"]): row for row in tushare_rows}
    qmt = {(row["code"], row["trade_date"], row["adjustment"]): row for row in qmt_rows}
    has_front_ratio = any(adjustment == "front_ratio" for _, _, adjustment in qmt)
    factors = {(row["ts_code"], row["trade_date"]): row["adj_factor"] for row in factor_rows}
    latest_factors: dict[str, tuple[date, float]] = {}
    for row in factor_rows:
        code = row["ts_code"]
        value = row["adj_factor"]
        if value is not None and (
            code not in latest_factors or row["trade_date"] > latest_factors[code][0]
        ):
            latest_factors[code] = (row["trade_date"], value)

    differences: list[Difference] = []
    compared = 0
    for key in sorted(tushare.keys() | {(code, day) for code, day, _ in qmt}):
        ts_row = tushare.get(key)
        none_row = qmt.get((*key, "none"))
        front_row = qmt.get((*key, "front_ratio"))
        if ts_row is None or none_row is None:
            differences.append(_missing("daily_none", key, ts_row, none_row))
        else:
            for field in ("open", "high", "low", "close"):
                compared += _compare(
                    differences, "daily_none", key, field, ts_row[field], none_row[field]
                )
            compared += _compare(
                differences,
                "daily_none",
                key,
                "volume",
                ts_row["vol"],
                none_row["volume"],
                atol=0.500001,
            )
            compared += _compare(
                differences,
                "daily_none",
                key,
                "amount",
                ts_row["amount"],
                _scaled(none_row["amount"], 1_000),
            )

        if has_front_ratio:
            factor = factors.get(key)
            latest = latest_factors.get(key[0])
            if ts_row is None or front_row is None or factor is None or latest is None:
                differences.append(_missing("daily_front", key, ts_row, front_row))
                continue
            for field in ("open", "high", "low", "close"):
                expected = _scaled_product(ts_row[field], factor, latest[1])
                compared += _compare(
                    differences,
                    "daily_front",
                    key,
                    field,
                    expected,
                    front_row[field],
                    rtol=1e-5,
                    atol=0.005001,
                )
    return CheckResult("daily", compared, tuple(differences))


def compare_qmt_front_ratio(
    connection: duckdb.DuckDBPyConnection,
    stocks: list[str],
    start_date: date,
    end_date: date,
    native_front: Mapping[str, Sequence[HistoryQuote]],
    factors: Mapping[str, Sequence[DividendFactor]],
) -> CheckResult:
    """用 QMT 未复权行情和事件 dr 复现本次实拉的原生 front_ratio。"""
    raw_rows = _fetch(
        connection,
        "SELECT code, trade_date, open, high, low, close, preClose, volume, amount "
        f"FROM qmt.daily WHERE {_stock_filter('code', stocks)} "
        "AND adjustment = 'none' AND trade_date BETWEEN ? AND ?",
        [*stocks, start_date, end_date],
    )
    native = {
        (code, _history_date(record.index)): record
        for code, records in native_front.items()
        for record in records
        if isinstance(record, HistoryBar)
    }
    event_factors = {
        code: tuple((_factor_date(item), item.dr) for item in records)
        for code, records in factors.items()
    }
    differences: list[Difference] = []
    compared = 0
    for raw in raw_rows:
        key = (raw["code"], raw["trade_date"])
        front = native.get(key)
        if front is None:
            differences.append(_missing("qmt_front_ratio", key, raw, None))
            continue
        divisor = 1.0
        invalid_factor = False
        for ex_date, factor in event_factors.get(key[0], ()):
            if ex_date <= key[1]:
                continue
            if factor is None or factor <= 0:
                invalid_factor = True
                break
            divisor *= factor
        if invalid_factor:
            differences.append(
                Difference(
                    "qmt_front_ratio",
                    key[0],
                    key[1].isoformat(),
                    "dr",
                    "positive",
                    "invalid",
                )
            )
            continue
        for field, native_field in (
            ("open", "open"),
            ("high", "high"),
            ("low", "low"),
            ("close", "close"),
            ("preClose", "preClose"),
        ):
            compared += _compare(
                differences,
                "qmt_front_ratio",
                key,
                field,
                _divided(raw[field], divisor),
                getattr(front, native_field),
                rtol=1e-12,
                atol=1e-12,
            )
        for field in ("volume", "amount"):
            compared += _compare(
                differences,
                "qmt_front_ratio",
                key,
                field,
                raw[field],
                getattr(front, field),
            )
    return CheckResult("qmt_front_ratio", compared, tuple(differences))


def compare_financial(
    connection: duckdb.DuckDBPyConnection,
    stocks: list[str],
    start_date: date,
    end_date: date,
) -> CheckResult:
    qmt_rows = _fetch(
        connection,
        "SELECT code, dataset, report_date, data_json FROM qmt.financial "
        f"WHERE {_stock_filter('code', stocks)} AND report_date BETWEEN ? AND ? "
        "QUALIFY row_number() OVER (PARTITION BY code, dataset, report_date "
        "ORDER BY disclosure_date DESC NULLS LAST) = 1",
        [*stocks, start_date, end_date],
    )
    qmt = {
        (row["code"], row["dataset"].lower(), row["report_date"]): json.loads(row["data_json"])
        for row in qmt_rows
    }

    differences: list[Difference] = []
    compared = 0
    for qmt_table, (tushare_table, fields) in _FINANCIAL_FIELDS.items():
        if tushare_table == "fina_indicator":
            query = (
                "SELECT * FROM tushare.fina_indicator "
                f"WHERE {_stock_filter('ts_code', stocks)} AND end_date BETWEEN ? AND ? "
                "QUALIFY row_number() OVER (PARTITION BY ts_code, end_date "
                "ORDER BY ann_date DESC NULLS LAST, "
                "try_cast(update_flag AS INTEGER) DESC NULLS LAST) = 1"
            )
        else:
            query = (
                f"SELECT * FROM tushare.{tushare_table} "
                f"WHERE {_stock_filter('ts_code', stocks)} AND end_date BETWEEN ? AND ? "
                "AND report_type = '1' "
                "QUALIFY row_number() OVER (PARTITION BY ts_code, end_date "
                "ORDER BY f_ann_date DESC NULLS LAST, "
                "try_cast(update_flag AS INTEGER) DESC NULLS LAST) = 1"
            )
        tushare_rows = _fetch(connection, query, [*stocks, start_date, end_date])
        tushare = {(row["ts_code"], row["end_date"]): row for row in tushare_rows}
        qmt_for_table = {
            (code, report_date): row
            for (code, table, report_date), row in qmt.items()
            if table == qmt_table.lower()
        }
        for key in sorted(tushare.keys() | qmt_for_table.keys()):
            ts_row = tushare.get(key)
            qmt_row = qmt_for_table.get(key)
            if ts_row is None or qmt_row is None:
                differences.append(_missing(f"financial_{qmt_table}", key, ts_row, qmt_row))
                continue
            for qmt_field, tushare_fields in fields.items():
                if qmt_field not in qmt_row:
                    continue
                candidates = (
                    (tushare_fields,)
                    if isinstance(tushare_fields, str)
                    else tushare_fields
                )
                qmt_value = qmt_row[qmt_field]
                values = [ts_row[field] for field in candidates if ts_row[field] is not None]
                # QMT 在新旧报表格式间会把同一项目放进不同字段。仅在 QMT
                # 实际提供值时，从等价的 Tushare 字段中选择数值最接近的一项；
                # QMT 为空时仍用主字段判断是否存在真实的数据覆盖缺口。
                if (
                    isinstance(qmt_value, (int, float))
                    and not isinstance(qmt_value, bool)
                    and values
                    and all(
                        isinstance(value, (int, float)) and not isinstance(value, bool)
                        for value in values
                    )
                ):
                    tushare_value = min(values, key=lambda value: abs(value - qmt_value))
                else:
                    tushare_value = ts_row[candidates[0]]
                atol = (
                    0.005001
                    if qmt_field in {"s_fa_eps_basic", "s_fa_eps_diluted"}
                    else 1e-4
                )
                compared += _compare(
                    differences,
                    f"financial_{qmt_table}",
                    key,
                    candidates[0],
                    tushare_value,
                    qmt_value,
                    rtol=1e-6,
                    atol=atol,
                    null_equals_zero=True,
                )
    return CheckResult("financial", compared, tuple(differences))


def compare_dividends(
    connection: duckdb.DuckDBPyConnection,
    stocks: list[str],
    start_date: date,
    end_date: date,
) -> CheckResult:
    tushare_rows = _fetch(
        connection,
        "WITH latest_plan AS ("
        "SELECT ts_code, end_date, ann_date, ex_date, cash_div_tax, "
        "stk_bo_rate, stk_co_rate, stk_div FROM tushare.dividend "
        f"WHERE {_stock_filter('ts_code', stocks)} AND div_proc = '实施' "
        "QUALIFY row_number() OVER ("
        "PARTITION BY ts_code, end_date, ann_date "
        "ORDER BY imp_ann_date DESC NULLS LAST) = 1"
        ") SELECT ts_code, ex_date, "
        "sum(coalesce(cash_div_tax, 0)) AS cash_div_tax, "
        "sum(coalesce(stk_bo_rate, 0)) AS stk_bo_rate, "
        "sum(coalesce(stk_co_rate, 0)) AS stk_co_rate, "
        "sum(coalesce(stk_div, 0)) AS stk_div "
        "FROM latest_plan WHERE ex_date BETWEEN ? AND ? "
        "GROUP BY ts_code, ex_date",
        [*stocks, start_date, end_date],
    )
    qmt_rows = _fetch(
        connection,
        "SELECT * FROM qmt.dividend_factors "
        f"WHERE {_stock_filter('code', stocks)} AND ex_date BETWEEN ? AND ?",
        [*stocks, start_date, end_date],
    )
    tushare = {(row["ts_code"], row["ex_date"]): row for row in tushare_rows}
    qmt = {(row["code"], row["ex_date"]): row for row in qmt_rows}
    differences: list[Difference] = []
    compared = 0
    for key in sorted(tushare.keys() | qmt.keys()):
        ts_row = tushare.get(key)
        qmt_row = qmt.get(key)
        if ts_row is None or qmt_row is None:
            differences.append(_missing("dividend", key, ts_row, qmt_row))
            continue
        for tushare_field, qmt_field in (
            ("cash_div_tax", "interest"),
            ("stk_bo_rate", "stockBonus"),
            ("stk_co_rate", "stockGift"),
        ):
            compared += _compare(
                differences,
                "dividend",
                key,
                tushare_field,
                _zero(ts_row[tushare_field]),
                _zero(qmt_row[qmt_field]),
            )
        qmt_stock_div = _sum(qmt_row["stockBonus"], qmt_row["stockGift"])
        compared += _compare(
            differences,
            "dividend",
            key,
            "stk_div",
            _zero(ts_row["stk_div"]),
            _zero(qmt_stock_div),
        )
    return CheckResult("dividend", compared, tuple(differences))


def _fetch(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    parameters: Sequence[object],
) -> list[dict[str, Any]]:
    return connection.execute(query, parameters).to_arrow_table().to_pylist()


def _stock_filter(column: str, stocks: list[str]) -> str:
    if not stocks:
        raise ValueError("stocks 不能为空")
    return f"{column} IN ({', '.join('?' for _ in stocks)})"


def _compare(
    differences: list[Difference],
    check: str,
    key: tuple[str, date],
    field: str,
    tushare: object,
    qmt: object,
    *,
    rtol: float = 1e-6,
    atol: float = 1e-6,
    null_equals_zero: bool = False,
) -> int:
    if tushare is None and qmt is None:
        return 0
    if null_equals_zero:
        if tushare is None and isinstance(qmt, (int, float)) and qmt == 0:
            tushare = 0.0
        elif qmt is None and isinstance(tushare, (int, float)) and tushare == 0:
            qmt = 0.0
    equal = (
        isinstance(tushare, (int, float))
        and not isinstance(tushare, bool)
        and isinstance(qmt, (int, float))
        and not isinstance(qmt, bool)
        and math.isclose(float(tushare), float(qmt), rel_tol=rtol, abs_tol=atol)
    )
    if not equal:
        differences.append(Difference(check, key[0], key[1].isoformat(), field, tushare, qmt))
    return 1


def _missing(
    check: str,
    key: tuple[str, date],
    tushare: object,
    qmt: object,
) -> Difference:
    return Difference(
        check,
        key[0],
        key[1].isoformat(),
        "__row__",
        "present" if tushare is not None else "missing",
        "present" if qmt is not None else "missing",
    )


def _scaled(value: object, divisor: float) -> float | None:
    return float(value) / divisor if isinstance(value, (int, float)) else None


def _divided(value: object, divisor: float) -> float | None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    return float(value) / divisor


def _history_date(index: int) -> date:
    try:
        return date.fromisoformat(f"{str(index)[:4]}-{str(index)[4:6]}-{str(index)[6:8]}")
    except ValueError as exc:
        raise ValueError(f"QMT 日线包含无效 index: {index!r}") from exc


def _factor_date(factor: DividendFactor) -> date:
    text = factor.date.strip().replace("-", "")
    try:
        return datetime.strptime(text[:8], "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError(f"QMT 除权因子包含无效 date: {factor.date!r}") from exc


def _scaled_product(value: object, factor: object, latest: object) -> float | None:
    if (
        not isinstance(value, (int, float))
        or not isinstance(factor, (int, float))
        or not isinstance(latest, (int, float))
    ):
        return None
    return float(value) * float(factor) / float(latest)


def _sum(left: object, right: object) -> float | None:
    values = [value for value in (left, right) if isinstance(value, (int, float))]
    return sum(float(value) for value in values) if values else None


def _zero(value: object) -> object:
    return 0.0 if value is None else value
