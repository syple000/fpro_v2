"""QMT 原始数据与中转行情的数据结构。"""

from __future__ import annotations

from datetime import date
from typing import Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, model_validator

from fpro_common import require_utc_us

XtDataPeriod: TypeAlias = Literal[
    "tick",
    "1m",
    "5m",
    "15m",
    "30m",
    "1h",
    "1d",
    "1w",
    "1mon",
    "1q",
    "1hy",
    "1y",
]
QuoteSource: TypeAlias = Literal["market", "stock"]
HistoryMode: TypeAlias = Literal["incremental", "full"]
DividendType: TypeAlias = Literal["none", "front", "back", "front_ratio", "back_ratio"]
FinancialTable: TypeAlias = Literal[
    "Balance",
    "Income",
    "CashFlow",
    "Capital",
    "Holdernum",
    "Top10holder",
    "Top10flowholder",
    "Pershareindex",
]
FinancialReportType: TypeAlias = Literal["report_time", "announce_time"]
XtNumber: TypeAlias = int | float


class ProtocolModel(BaseModel):
    """HTTP 信封和中转元数据使用严格、封闭的结构。"""

    model_config = ConfigDict(extra="forbid", strict=True, validate_default=True)


class QuoteModel(BaseModel):
    """XtData 行情字段。

    已知字段不改名、不改时间单位，也不把整数价格强制转换为浮点数。
    未定义字段会在接入边界报错，避免协议与当前 XtData 版本悄悄偏离。
    """

    model_config = ConfigDict(extra="forbid", strict=True, validate_default=True)


class TickQuote(QuoteModel):
    """``get_full_tick``、全推和 tick 订阅返回的单条行情。"""

    time: int | None = None
    stime: str | None = None
    timetag: str | None = None
    lastPrice: XtNumber | None = None
    open: XtNumber | None = None
    high: XtNumber | None = None
    low: XtNumber | None = None
    lastClose: XtNumber | None = None
    amount: XtNumber | None = None
    volume: int | None = None
    pvolume: int | None = None
    stockStatus: int | None = None
    openInt: int | None = None
    transactionNum: int | None = None
    lastSettlementPrice: XtNumber | None = None
    settlementPrice: XtNumber | None = None
    pe: XtNumber | None = None
    askPrice: list[XtNumber] | None = None
    bidPrice: list[XtNumber] | None = None
    askVol: list[int] | None = None
    bidVol: list[int] | None = None
    volRatio: XtNumber | None = None
    speed1Min: XtNumber | None = None
    speed5Min: XtNumber | None = None


class BarQuote(QuoteModel):
    """分钟线、日线等非 tick 订阅返回的单条 K 线。"""

    time: int | None = None
    open: XtNumber | None = None
    high: XtNumber | None = None
    low: XtNumber | None = None
    close: XtNumber | None = None
    volume: int | None = None
    amount: XtNumber | None = None
    # XtData 历史数据沿用错误拼写，实时 K 线使用正确拼写；两者都原样保留。
    settelementPrice: XtNumber | None = None
    settlementPrice: XtNumber | None = None
    openInterest: XtNumber | None = None
    preClose: XtNumber | None = None
    suspendFlag: int | None = None
    dr: XtNumber | None = None
    totaldr: XtNumber | None = None


QuotePayload: TypeAlias = TickQuote | BarQuote


def quote_model_for_period(period: XtDataPeriod) -> type[TickQuote] | type[BarQuote]:
    return TickQuote if period == "tick" else BarQuote


def validate_quote(period: XtDataPeriod, value: object) -> QuotePayload:
    """按订阅周期选择行情结构，避免联合类型猜测。"""
    model = quote_model_for_period(period)
    if isinstance(value, model):
        return value
    return model.model_validate(value)


class HistoryTick(TickQuote):
    """历史 tick DataFrame 的一行；``index`` 原样保留 DataFrame index。"""

    index: int | str


class HistoryBar(BarQuote):
    """历史 K 线 DataFrame 的一行；index 为 ``YYYYMMDD[HHMMSS]`` 整数。"""

    index: int


HistoryQuote: TypeAlias = HistoryTick | HistoryBar


class DividendFactor(ProtocolModel):
    """除权 DataFrame 的一行。

    实机返回的 index 是 ``YYYYMMDD`` 日期，真正的毫秒时间戳位于 ``time`` 列。
    """

    date: str
    time: XtNumber
    interest: XtNumber | None = None
    stockBonus: XtNumber | None = None
    stockGift: XtNumber | None = None
    allotNum: XtNumber | None = None
    allotPrice: XtNumber | None = None
    gugai: XtNumber | None = None
    dr: XtNumber | None = None


class SequencedQuote(ProtocolModel):
    """agent 为一条原始订阅行情增加的可靠中转信封。"""

    seq: int = Field(ge=1)
    code: str
    period: XtDataPeriod
    source: QuoteSource
    subscription: str
    received_at: int
    quote: QuotePayload

    @model_validator(mode="before")
    @classmethod
    def select_quote_model(cls, value: object) -> object:
        if not isinstance(value, dict) or "period" not in value or "quote" not in value:
            return value
        converted = dict(value)
        converted["quote"] = validate_quote(converted["period"], converted["quote"])
        try:
            converted["received_at"] = require_utc_us(converted.get("received_at"))
        except (TypeError, ValueError) as exc:
            raise ValueError(str(exc)) from exc
        return converted


class QuoteEvent(SequencedQuote):
    """receiver 派生时间并落盘后返回给 platform 的行情事件。"""

    event_time: int | None
    trading_date: date


class BalanceRecord(ProtocolModel):
    """Balance DataFrame 的一行。"""

    index: int
    m_timetag: str | None = None
    m_anntime: str | None = None
    internal_shoule_recv: float | None = None
    fixed_capital_clearance: float | None = None
    should_pay_money: float | None = None
    settlement_payment: float | None = None
    receivable_premium: float | None = None
    accounts_receivable_reinsurance: float | None = None
    reinsurance_contract_reserve: float | None = None
    dividends_payable: float | None = None
    tax_rebate_for_export: float | None = None
    subsidies_receivable: float | None = None
    deposit_receivable: float | None = None
    apportioned_cost: float | None = None
    profit_and_current_assets_with_deal: float | None = None
    current_assets_one_year: float | None = None
    long_term_receivables: float | None = None
    other_long_term_investments: float | None = None
    original_value_of_fixed_assets: float | None = None
    net_value_of_fixed_assets: float | None = None
    depreciation_reserves_of_fixed_assets: float | None = None
    productive_biological_assets: float | None = None
    public_welfare_biological_assets: float | None = None
    oil_and_gas_assets: float | None = None
    development_expenditure: float | None = None
    right_of_split_share_distribution: float | None = None
    other_non_mobile_assets: float | None = None
    handling_fee_and_commission: float | None = None
    other_payables: float | None = None
    margin_payable: float | None = None
    internal_accounts_payable: float | None = None
    advance_cost: float | None = None
    insurance_contract_reserve: float | None = None
    broker_buying_and_selling_securities: float | None = None
    acting_underwriting_securities: float | None = None
    international_ticket_settlement: float | None = None
    domestic_ticket_settlement: float | None = None
    deferred_income: float | None = None
    short_term_bonds_payable: float | None = None
    long_term_deferred_income: float | None = None
    undetermined_investment_losses: float | None = None
    quasi_distribution_of_cash_dividends: float | None = None
    provisions_not: float | None = None
    cust_bank_dep: float | None = None
    provisions: float | None = None
    less_tsy_stk: float | None = None
    cash_equivalents: float | None = None
    loans_to_oth_banks: float | None = None
    tradable_fin_assets: float | None = None
    derivative_fin_assets: float | None = None
    bill_receivable: float | None = None
    account_receivable: float | None = None
    advance_payment: float | None = None
    int_rcv: float | None = None
    other_receivable: float | None = None
    red_monetary_cap_for_sale: float | None = None
    agency_bus_assets: float | None = None
    inventories: float | None = None
    other_current_assets: float | None = None
    total_current_assets: float | None = None
    loans_and_adv_granted: float | None = None
    fin_assets_avail_for_sale: float | None = None
    held_to_mty_invest: float | None = None
    long_term_eqy_invest: float | None = None
    invest_real_estate: float | None = None
    accumulated_depreciation: float | None = None
    fix_assets: float | None = None
    constru_in_process: float | None = None
    construction_materials: float | None = None
    long_term_liabilities: float | None = None
    intang_assets: float | None = None
    goodwill: float | None = None
    long_deferred_expense: float | None = None
    deferred_tax_assets: float | None = None
    total_non_current_assets: float | None = None
    tot_assets: float | None = None
    shortterm_loan: float | None = None
    borrow_central_bank: float | None = None
    loans_oth_banks: float | None = None
    tradable_fin_liab: float | None = None
    derivative_fin_liab: float | None = None
    notes_payable: float | None = None
    accounts_payable: float | None = None
    advance_peceipts: float | None = None
    fund_sales_fin_assets_rp: float | None = None
    empl_ben_payable: float | None = None
    taxes_surcharges_payable: float | None = None
    int_payable: float | None = None
    dividend_payable: float | None = None
    other_payable: float | None = None
    non_current_liability_in_one_year: float | None = None
    other_current_liability: float | None = None
    total_current_liability: float | None = None
    long_term_loans: float | None = None
    bonds_payable: float | None = None
    longterm_account_payable: float | None = None
    grants_received: float | None = None
    deferred_tax_liab: float | None = None
    other_non_current_liabilities: float | None = None
    non_current_liabilities: float | None = None
    tot_liab: float | None = None
    cap_stk: float | None = None
    cap_rsrv: float | None = None
    specific_reserves: float | None = None
    surplus_rsrv: float | None = None
    prov_nom_risks: float | None = None
    undistributed_profit: float | None = None
    cnvd_diff_foreign_curr_stat: float | None = None
    tot_shrhldr_eqy_excl_min_int: float | None = None
    minority_int: float | None = None
    total_equity: float | None = None
    tot_liab_shrhldr_eqy: float | None = None
    m_quarter: float | None = None
    cash_deposits_central_bank: float | None = None
    asset_dep_oth_banks_fin_inst: float | None = None
    precious_metals: float | None = None
    rcv_invest: float | None = None
    oth_assets: float | None = None
    liab_dep_oth_banks_fin_inst: float | None = None
    agency_bus_liab: float | None = None
    oth_liab: float | None = None
    unconfirmed_invest_loss: float | None = None
    tot_shrhldr_eqy_incl_min_int: float | None = None
    inventory_depreciation_reserve: float | None = None
    current_ratio: float | None = None
    total_equity_and_liabilities: float | None = None
    m_stateTypeCode: float | None = None
    m_source: float | None = None
    m_coverPeriod: float | None = None
    m_industryCode: float | None = None
    m_balanceCurrency: float | None = None
    m_cashAdepositsCentralBank: float | None = None
    m_nobleMetal: float | None = None
    m_depositsOtherFinancialInstitutions: float | None = None
    m_currentInvestment: float | None = None
    m_redemptoryMonetaryCapitalSale: float | None = None
    m_netAmountSubrogation: float | None = None
    m_refundableDeposits: float | None = None
    m_netAmountLoanPledged: float | None = None
    m_fixedTimeDeposit: float | None = None
    m_netLongtermDebtInvestments: float | None = None
    m_permanentInvestment: float | None = None
    m_depositForcapitalRecognizance: float | None = None
    m_netBalConstructionProgress: float | None = None
    m_separateAccountAssets: float | None = None
    m_capitalInvicariousBussiness: float | None = None
    m_otherAssets: float | None = None
    m_depositsWithBanksOtherFinancialIns: float | None = None
    m_indemnityPayable: float | None = None
    m_policyDividendPayable: float | None = None
    m_guaranteeInvestmentFunds: float | None = None
    m_premiumsReceivedAdvance: float | None = None
    m_insuranceLiabilities: float | None = None
    m_liabilitiesIndependentAccounts: float | None = None
    m_liabilitiesVicariousBusiness: float | None = None
    m_otherLiablities: float | None = None
    m_capitalPremium: float | None = None
    m_petainedProfit: float | None = None
    m_provisionTransactionRisk: float | None = None
    m_otherReserves: float | None = None


class IncomeRecord(ProtocolModel):
    """Income DataFrame 的一行。"""

    index: int
    m_timetag: str | None = None
    m_anntime: str | None = None
    revenue_inc: float | None = None
    earned_premium: float | None = None
    real_estate_sales_income: float | None = None
    total_operating_cost: float | None = None
    real_estate_sales_cost: float | None = None
    research_expenses: float | None = None
    surrender_value: float | None = None
    net_payments: float | None = None
    net_withdrawal_ins_con_res: float | None = None
    policy_dividend_expenses: float | None = None
    reinsurance_cost: float | None = None
    change_income_fair_value: float | None = None
    futures_loss: float | None = None
    trust_income: float | None = None
    subsidize_revenue: float | None = None
    other_business_profits: float | None = None
    net_profit_excl_merged_int_inc: float | None = None
    int_inc: float | None = None
    handling_chrg_comm_inc: float | None = None
    less_handling_chrg_comm_exp: float | None = None
    other_bus_cost: float | None = None
    plus_net_gain_fx_trans: float | None = None
    il_net_loss_disp_noncur_asset: float | None = None
    inc_tax: float | None = None
    unconfirmed_invest_loss: float | None = None
    net_profit_excl_min_int_inc: float | None = None
    less_int_exp: float | None = None
    other_bus_inc: float | None = None
    revenue: float | None = None
    total_expense: float | None = None
    less_taxes_surcharges_ops: float | None = None
    sale_expense: float | None = None
    less_gerl_admin_exp: float | None = None
    financial_expense: float | None = None
    less_impair_loss_assets: float | None = None
    plus_net_invest_inc: float | None = None
    incl_inc_invest_assoc_jv_entp: float | None = None
    oper_profit: float | None = None
    plus_non_oper_rev: float | None = None
    less_non_oper_exp: float | None = None
    tot_profit: float | None = None
    net_profit_incl_min_int_inc: float | None = None
    net_profit_incl_min_int_inc_after: float | None = None
    minority_int_inc: float | None = None
    s_fa_eps_basic: float | None = None
    s_fa_eps_diluted: float | None = None
    total_income: float | None = None
    total_income_minority: float | None = None
    other_compreh_inc: float | None = None
    m_stateTypeCode: float | None = None
    m_source: float | None = None
    m_coverPeriod: float | None = None
    m_industryCode: float | None = None
    m_currency: float | None = None
    m_netinterestIncome: float | None = None
    m_netFeesCommissions: float | None = None
    m_insuranceBusiness: float | None = None
    m_separatePremium: float | None = None
    m_asideReservesUndueLiabilities: float | None = None
    m_paymentsInsuranceClaims: float | None = None
    m_amortizedCompensationExpenses: float | None = None
    m_netReserveInsuranceLiability: float | None = None
    m_policyReserve: float | None = None
    m_amortizeInsuranceReserve: float | None = None
    m_nsuranceFeesCommissionExpenses: float | None = None
    m_operationAdministrativeExpense: float | None = None
    m_amortizedReinsuranceExpenditure: float | None = None
    m_netProfitLossdisposalNonassets: float | None = None
    m_otherItemsAffectingNetProfit: float | None = None
    m_quarter: float | None = None
    net_int_inc: float | None = None
    net_handling_chrg_comm_inc: float | None = None
    net_inc_other_ops: float | None = None
    plus_net_gain_chg_fv: float | None = None
    plus_net_inc_other_bus: float | None = None
    oper_exp: float | None = None
    tot_compreh_inc: float | None = None
    tot_compreh_inc_min_shrhldr: float | None = None
    tot_compreh_inc_parent_comp: float | None = None
    actual_ann_dt: float | None = None
    operating_revenue: float | None = None
    cost_of_goods_sold: float | None = None


class CashFlowRecord(ProtocolModel):
    """CashFlow DataFrame 的一行。"""

    index: int
    m_timetag: str | None = None
    m_anntime: str | None = None
    cash_received_ori_ins_contract_pre: float | None = None
    net_cash_received_rei_ope: float | None = None
    net_increase_insured_funds: float | None = None
    net_increase_in_disposal: float | None = None
    cash_for_interest: float | None = None
    net_increase_in_repurchase_funds: float | None = None
    cash_for_payment_original_insurance: float | None = None
    cash_payment_policy_dividends: float | None = None
    disposal_other_business_units: float | None = None
    cash_received_from_pledges: float | None = None
    cash_paid_for_investments: float | None = None
    net_increase_in_pledged_loans: float | None = None
    cash_paid_by_subsidiaries: float | None = None
    increase_in_cash_paid: float | None = None
    cass_received_sub_abs: float | None = None
    cass_received_sub_investments: float | None = None
    minority_shareholder_profit_loss: float | None = None
    unrecognized_investment_losses: float | None = None
    ncrease_deferred_income: float | None = None
    projected_liability: float | None = None
    increase_operational_payables: float | None = None
    reduction_outstanding_amounts_less: float | None = None
    reduction_outstanding_amounts_more: float | None = None
    goods_sale_and_service_render_cash: float | None = None
    net_incr_dep_cob: float | None = None
    net_incr_loans_central_bank: float | None = None
    net_incr_fund_borr_ofi: float | None = None
    tax_levy_refund: float | None = None
    cash_paid_invest: float | None = None
    other_cash_recp_ral_oper_act: float | None = None
    stot_cash_inflows_oper_act: float | None = None
    goods_and_services_cash_paid: float | None = None
    net_incr_clients_loan_adv: float | None = None
    net_incr_dep_cbob: float | None = None
    handling_chrg_paid: float | None = None
    cash_pay_beh_empl: float | None = None
    pay_all_typ_tax: float | None = None
    other_cash_pay_ral_oper_act: float | None = None
    stot_cash_outflows_oper_act: float | None = None
    net_cash_flows_oper_act: float | None = None
    cash_recp_disp_withdrwl_invest: float | None = None
    cash_recp_return_invest: float | None = None
    net_cash_recp_disp_fiolta: float | None = None
    other_cash_recp_ral_inv_act: float | None = None
    stot_cash_inflows_inv_act: float | None = None
    cash_pay_acq_const_fiolta: float | None = None
    stot_cash_outflows_inv_act: float | None = None
    net_cash_flows_inv_act: float | None = None
    cash_recp_cap_contrib: float | None = None
    cash_recp_borrow: float | None = None
    proc_issue_bonds: float | None = None
    other_cash_recp_ral_fnc_act: float | None = None
    stot_cash_inflows_fnc_act: float | None = None
    cash_prepay_amt_borr: float | None = None
    cash_pay_dist_dpcp_int_exp: float | None = None
    other_cash_pay_ral_fnc_act: float | None = None
    stot_cash_outflows_fnc_act: float | None = None
    net_cash_flows_fnc_act: float | None = None
    eff_fx_flu_cash: float | None = None
    net_incr_cash_cash_equ: float | None = None
    cash_cash_equ_beg_period: float | None = None
    cash_cash_equ_end_period: float | None = None
    net_profit: float | None = None
    plus_prov_depr_assets: float | None = None
    depr_fa_coga_dpba: float | None = None
    amort_intang_assets: float | None = None
    amort_lt_deferred_exp: float | None = None
    decr_deferred_exp: float | None = None
    incr_acc_exp: float | None = None
    loss_disp_fiolta: float | None = None
    loss_scr_fa: float | None = None
    loss_fv_chg: float | None = None
    fin_exp: float | None = None
    invest_loss: float | None = None
    decr_deferred_inc_tax_assets: float | None = None
    incr_deferred_inc_tax_liab: float | None = None
    decr_inventories: float | None = None
    decr_oper_payable: float | None = None
    others: float | None = None
    im_net_cash_flows_oper_act: float | None = None
    conv_debt_into_cap: float | None = None
    conv_corp_bonds_due_within_1y: float | None = None
    fa_fnc_leases: float | None = None
    end_bal_cash: float | None = None
    less_beg_bal_cash: float | None = None
    plus_end_bal_cash_equ: float | None = None
    less_beg_bal_cash_equ: float | None = None
    im_net_incr_cash_cash_equ: float | None = None
    m_quarter: float | None = None
    net_incr_int_handling_chrg: float | None = None
    net_cash_deal_subcompany: float | None = None
    cash_from_mino_s_invest_sub: float | None = None
    fix_intan_other_asset_dispo_cash_payment: float | None = None
    other_cash_pay_ral_inv_act: float | None = None
    m_stateTypeCode: float | None = None
    m_source: float | None = None
    m_coverPeriod: float | None = None
    m_industryCode: float | None = None
    m_currency: float | None = None
    m_cashSellingProvidingServices: float | None = None
    m_netDecreaseUnwindingFunds: float | None = None
    m_netReductionPurchaseRebates: float | None = None
    m_netIncreaseDepositsBanks: float | None = None
    m_netCashReinsuranceBusiness: float | None = None
    m_netReductionDeposInveFunds: float | None = None
    m_netIncreaseUnwindingFunds: float | None = None
    m_netReductionAmountBorrowedFunds: float | None = None
    m_netReductionSaleRepurchaseProceeds: float | None = None
    m_investmentPaidInCash: float | None = None
    m_paymentOtherCashRelated: float | None = None
    m_cashOutFlowsInvesactivities: float | None = None
    m_absorbCashEquityInv: float | None = None
    m_otherImpactsOnCash: float | None = None
    m_addOperatingReceivableItems: float | None = None


class CapitalRecord(ProtocolModel):
    """Capital DataFrame 的一行。"""

    index: int
    m_timetag: str | None = None
    m_anntime: str | None = None
    total_capital: float | None = None
    circulating_capital: float | None = None
    restrict_circulating_capital: float | None = None
    freeFloatCapital: float | None = None
    m_quarter: float | None = None


class HolderNumberRecord(ProtocolModel):
    """Holdernum DataFrame 的一行。"""

    index: int
    declareDate: str | None = None
    endDate: str | None = None
    shareholder: float | None = None
    shareholderA: float | None = None
    shareholderB: float | None = None
    shareholderH: float | None = None
    shareholderFloat: float | None = None
    shareholderOther: float | None = None


class Top10HolderRecord(ProtocolModel):
    """Top10holder DataFrame 的一行。"""

    index: int
    declareDate: str | None = None
    endDate: str | None = None
    quantity: float | None = None
    ratio: float | None = None
    rank: float | None = None
    name: str | None = None
    type: str | None = None
    reason: str | None = None
    nature: str | None = None


class Top10FlowHolderRecord(ProtocolModel):
    """Top10flowholder DataFrame 的一行。"""

    index: int
    declareDate: str | None = None
    endDate: str | None = None
    quantity: float | None = None
    ratio: float | None = None
    rank: float | None = None
    name: str | None = None
    type: str | None = None
    reason: str | None = None
    nature: str | None = None


class PerShareIndexRecord(ProtocolModel):
    """Pershareindex DataFrame 的一行。"""

    index: int
    m_timetag: str | None = None
    m_anntime: str | None = None
    s_fa_ocfps: float | None = None
    s_fa_bps: float | None = None
    s_fa_eps_basic: float | None = None
    s_fa_eps_diluted: float | None = None
    s_fa_undistributedps: float | None = None
    s_fa_surpluscapitalps: float | None = None
    adjusted_earnings_per_share: float | None = None
    du_return_on_equity: float | None = None
    sales_gross_profit: float | None = None
    inc_revenue_rate: float | None = None
    du_profit_rate: float | None = None
    inc_net_profit_rate: float | None = None
    adjusted_net_profit_rate: float | None = None
    inc_total_revenue_annual: float | None = None
    inc_net_profit_to_shareholders_annual: float | None = None
    adjusted_profit_to_profit_annual: float | None = None
    equity_roe: float | None = None
    net_roe: float | None = None
    total_roe: float | None = None
    gross_profit: float | None = None
    net_profit: float | None = None
    actual_tax_rate: float | None = None
    pre_pay_operate_income: float | None = None
    sales_cash_flow: float | None = None
    gear_ratio: float | None = None
    inventory_turnover: float | None = None
    m_quarter: float | None = None
    s_fa_fcfeps: float | None = None
    s_fa_retainedps: float | None = None
    s_fa_fcffps: float | None = None
    s_fa_ebitps: float | None = None
    s_fa_cfps: float | None = None
    s_fa_grps: float | None = None
    s_fa_surplusreserveps: float | None = None
    s_fa_orps: float | None = None
    inc_revenue: float | None = None
    inc_gross_profit: float | None = None
    inc_profit_before_tax: float | None = None
    du_profit: float | None = None
    inc_net_profit: float | None = None
    adjusted_net_profit: float | None = None


FinancialRecord: TypeAlias = (
    BalanceRecord
    | IncomeRecord
    | CashFlowRecord
    | CapitalRecord
    | HolderNumberRecord
    | Top10HolderRecord
    | Top10FlowHolderRecord
    | PerShareIndexRecord
)


class FinancialData(ProtocolModel):
    """一只合约按 XtData 表名组织的财务记录。"""

    Balance: list[BalanceRecord] | None = None
    Income: list[IncomeRecord] | None = None
    CashFlow: list[CashFlowRecord] | None = None
    Capital: list[CapitalRecord] | None = None
    Holdernum: list[HolderNumberRecord] | None = None
    Top10holder: list[Top10HolderRecord] | None = None
    Top10flowholder: list[Top10FlowHolderRecord] | None = None
    Pershareindex: list[PerShareIndexRecord] | None = None
