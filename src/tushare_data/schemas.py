"""Tushare 落盘字段定义。

这里故意把字段逐一列出，并在请求 Tushare 时也传入同一份字段列表。
这样上游新增字段时不会悄悄改变本地 Parquet Schema。
每个字段上方的中文注释来自 Tushare 官方输出参数表；标为 quicksync 实测的字段
是官方参数表暂未列出、但代理接口确实返回的扩展字段。
"""

from __future__ import annotations

import pyarrow as pa

# 下方注释中的钟点是北京时间发布时间；落盘为 Unix Epoch 微秒 int64。
# partition_date：visible_at 对应的北京时间日历日期，仅用于物理分区。
VISIBLE_TIME_TYPE = pa.int64()

# visible_at：使用 trade_date，当日 16:00（收盘后）可见。
DAILY_FIELDS = (
    # 股票代码
    "ts_code",
    # 交易日期
    "trade_date",
    # 开盘价
    "open",
    # 最高价
    "high",
    # 最低价
    "low",
    # 收盘价
    "close",
    # 昨收价【除权价】
    "pre_close",
    # 涨跌额
    "change",
    # 涨跌幅（%） 【基于除权后的昨收计算的涨跌幅：（今收-除权昨收）/除权昨收 】
    "pct_chg",
    # 成交量 （手）
    "vol",
    # 成交额 （千元）
    "amount",
    # 盘后成交量 （手）
    "ah_vol",
    # 盘后成交额 （千元）
    "ah_amount",
)

# visible_at：使用 trade_date，当日 17:00（每日指标发布后）可见。
# 官方新增 limit_status，但 quicksync 实测仍会省略该列，因此暂不纳入固定 Schema。
DAILY_BASIC_FIELDS = (
    # TS股票代码
    "ts_code",
    # 交易日期
    "trade_date",
    # 当日收盘价
    "close",
    # 换手率 (成交量/无限售流通股数)
    "turnover_rate",
    # 换手率（自由流通股）(成交量/自由流通股数)
    "turnover_rate_f",
    # 量比 VOL/MA
    "volume_ratio",
    # 市盈率（总市值/净利润， 亏损的PE为空）
    "pe",
    # 市盈率（ 总市值/净利润TTM，亏损的PE为空）
    "pe_ttm",
    # 市净率（总市值/(净资产-其他权益工具)）
    "pb",
    # 市销率 (总市值/营业收入(最新年报))
    "ps",
    # 市销率（TTM）(总市值/营业收入TTM)
    "ps_ttm",
    # 股息率 （%），除息日发生在去年期间的派现
    "dv_ratio",
    # 股息率（TTM）（%），除息日在近12个月且分红报告期在12个月以内的派现
    "dv_ttm",
    # 总股本 （万股）
    "total_share",
    # 流通股本 （万股）
    "float_share",
    # 自由流通股本 （万）
    "free_share",
    # 总市值 （万元）
    "total_mv",
    # 流通市值（万元）
    "circ_mv",
)

# visible_at：使用 trade_date，当日 09:20 可见。
ADJ_FACTOR_FIELDS = (
    # 股票代码
    "ts_code",
    # 交易日期
    "trade_date",
    # 复权因子
    "adj_factor",
)

# visible_at：使用 trade_date，当日 09:30 可见。
SUSPEND_D_FIELDS = (
    # TS代码
    "ts_code",
    # 停复牌日期（覆盖从停牌到覆盖期间的连续日期）
    "trade_date",
    # 日内停牌时间段（日内停牌才有值，否则为空值）
    "suspend_timing",
    # 停复牌类型：S-停牌，R-复牌
    "suspend_type",
)

# visible_at：使用 trade_date，当日 08:45（开盘前涨跌停价发布后）可见。
STK_LIMIT_FIELDS = (
    # 交易日期
    "trade_date",
    # TS股票代码
    "ts_code",
    # 昨日收盘价
    "pre_close",
    # 涨停价
    "up_limit",
    # 跌停价
    "down_limit",
)

# visible_at：使用 trade_date，当日 09:20（开盘前风险警示名单发布后）可见。
STOCK_ST_FIELDS = (
    # 股票代码
    "ts_code",
    # 股票名称
    "name",
    # 交易日期
    "trade_date",
    # 类型
    "type",
    # 类型名称
    "type_name",
)

# visible_at：使用 trade_date，当日 19:00（盘后资金流数据发布后）可见。
MONEYFLOW_FIELDS = (
    # TS代码
    "ts_code",
    # 交易日期
    "trade_date",
    # 小单买入量（手）
    "buy_sm_vol",
    # 小单买入金额（万元）
    "buy_sm_amount",
    # 小单卖出量（手）
    "sell_sm_vol",
    # 小单卖出金额（万元）
    "sell_sm_amount",
    # 中单买入量（手）
    "buy_md_vol",
    # 中单买入金额（万元）
    "buy_md_amount",
    # 中单卖出量（手）
    "sell_md_vol",
    # 中单卖出金额（万元）
    "sell_md_amount",
    # 大单买入量（手）
    "buy_lg_vol",
    # 大单买入金额（万元）
    "buy_lg_amount",
    # 大单卖出量（手）
    "sell_lg_vol",
    # 大单卖出金额（万元）
    "sell_lg_amount",
    # 特大单买入量（手）
    "buy_elg_vol",
    # 特大单买入金额（万元）
    "buy_elg_amount",
    # 特大单卖出量（手）
    "sell_elg_vol",
    # 特大单卖出金额（万元）
    "sell_elg_amount",
    # 净流入量（手）
    "net_mf_vol",
    # 净流入额（万元）
    "net_mf_amount",
)

# visible_at：优先使用实施公告日 imp_ann_date；未实施时使用 ann_date，避免提前看到实施字段。
DIVIDEND_FIELDS = (
    # TS代码
    "ts_code",
    # 分红年度
    "end_date",
    # 公告日(预案，决案)
    "ann_date",
    # 实施进度
    "div_proc",
    # 每股送转
    "stk_div",
    # 每股送股比例
    "stk_bo_rate",
    # 每股转增比例
    "stk_co_rate",
    # 每股分红（税后）
    "cash_div",
    # 每股分红（税前）
    "cash_div_tax",
    # 股权登记日
    "record_date",
    # 除权除息日
    "ex_date",
    # 派息日
    "pay_date",
    # 红股上市日
    "div_listdate",
    # 实施公告日
    "imp_ann_date",
    # 基准日
    "base_date",
    # 基准股本（万）
    "base_share",
)

# visible_at：使用 ann_date，公告日结束时可见。
FORECAST_FIELDS = (
    # TS股票代码
    "ts_code",
    # 公告日期
    "ann_date",
    # 报告期
    "end_date",
    # 业绩预告类型(预增/预减/扭亏/首亏/续亏/续盈/略增/略减)
    "type",
    # 预告净利润变动幅度下限（%）
    "p_change_min",
    # 预告净利润变动幅度上限（%）
    "p_change_max",
    # 预告净利润下限（万元）
    "net_profit_min",
    # 预告净利润上限（万元）
    "net_profit_max",
    # 上年同期归属母公司净利润
    "last_parent_net",
    # 首次公告日
    "first_ann_date",
    # 业绩预告摘要
    "summary",
    # 业绩变动原因
    "change_reason",
    # 更新标识（quicksync 实测返回字段）
    "update_flag",
)

# visible_at：使用 ann_date，公告日结束时可见。
EXPRESS_FIELDS = (
    # TS股票代码
    "ts_code",
    # 公告日期
    "ann_date",
    # 报告期
    "end_date",
    # 营业收入(元)
    "revenue",
    # 营业利润(元)
    "operate_profit",
    # 利润总额(元)
    "total_profit",
    # 净利润(元)
    "n_income",
    # 总资产(元)
    "total_assets",
    # 股东权益合计(不含少数股东权益)(元)
    "total_hldr_eqy_exc_min_int",
    # 每股收益(摊薄)(元)
    "diluted_eps",
    # 净资产收益率(摊薄)(%)
    "diluted_roe",
    # 去年同期修正后净利润
    "yoy_net_profit",
    # 每股净资产
    "bps",
    # 同比增长率:营业收入
    "yoy_sales",
    # 同比增长率:营业利润
    "yoy_op",
    # 同比增长率:利润总额
    "yoy_tp",
    # 同比增长率:归属母公司股东的净利润
    "yoy_dedu_np",
    # 同比增长率:基本每股收益
    "yoy_eps",
    # 同比增减:加权平均净资产收益率
    "yoy_roe",
    # 比年初增长率:总资产
    "growth_assets",
    # 比年初增长率:归属母公司的股东权益
    "yoy_equity",
    # 比年初增长率:归属于母公司股东的每股净资产
    "growth_bps",
    # 去年同期营业收入
    "or_last_year",
    # 去年同期营业利润
    "op_last_year",
    # 去年同期利润总额
    "tp_last_year",
    # 去年同期净利润
    "np_last_year",
    # 去年同期每股收益
    "eps_last_year",
    # 期初净资产
    "open_net_assets",
    # 期初每股净资产
    "open_bps",
    # 业绩简要说明
    "perf_summary",
    # 是否审计： 1是 0否
    "is_audit",
    # 备注
    "remark",
    # 更新标识（quicksync 实测返回字段）
    "update_flag",
)

# visible_at：使用 ann_date，公告日结束时可见。
FINA_AUDIT_FIELDS = (
    # TS股票代码
    "ts_code",
    # 公告日期
    "ann_date",
    # 报告期
    "end_date",
    # 审计结果
    "audit_result",
    # 审计总费用（元）
    "audit_fees",
    # 会计事务所
    "audit_agency",
    # 签字会计师
    "audit_sign",
)

# visible_at：使用 cal_date，当日 00:00；交易日历属于预先公布信息。
TRADE_CAL_FIELDS = (
    # 交易所 SSE上交所 SZSE深交所
    "exchange",
    # 日历日期
    "cal_date",
    # 是否交易 0休市 1交易
    "is_open",
    # 上一个交易日
    "pretrade_date",
)

# visible_at：优先使用 f_ann_date（实际公告日），缺失时使用 ann_date，公告日结束时可见。
# 官方新增的 9 个利润表字段目前会被 quicksync 省略，待代理实际返回后再扩展固定 Schema。
INCOME_FIELDS = (
    # TS代码
    "ts_code",
    # 公告日期
    "ann_date",
    # 实际公告日期
    "f_ann_date",
    # 报告期
    "end_date",
    # 报告类型 见底部表
    "report_type",
    # 公司类型(1一般工商业2银行3保险4证券)
    "comp_type",
    # 报告期类型
    "end_type",
    # 基本每股收益
    "basic_eps",
    # 稀释每股收益
    "diluted_eps",
    # 营业总收入
    "total_revenue",
    # 营业收入
    "revenue",
    # 利息收入
    "int_income",
    # 已赚保费
    "prem_earned",
    # 手续费及佣金收入
    "comm_income",
    # 手续费及佣金净收入
    "n_commis_income",
    # 其他经营净收益
    "n_oth_income",
    # 加:其他业务净收益
    "n_oth_b_income",
    # 保险业务收入
    "prem_income",
    # 减:分出保费
    "out_prem",
    # 提取未到期责任准备金
    "une_prem_reser",
    # 其中:分保费收入
    "reins_income",
    # 代理买卖证券业务净收入
    "n_sec_tb_income",
    # 证券承销业务净收入
    "n_sec_uw_income",
    # 受托客户资产管理业务净收入
    "n_asset_mg_income",
    # 其他业务收入
    "oth_b_income",
    # 加:公允价值变动净收益
    "fv_value_chg_gain",
    # 加:投资净收益
    "invest_income",
    # 其中:对联营企业和合营企业的投资收益
    "ass_invest_income",
    # 加:汇兑净收益
    "forex_gain",
    # 营业总成本
    "total_cogs",
    # 减:营业成本
    "oper_cost",
    # 减:利息支出
    "int_exp",
    # 减:手续费及佣金支出
    "comm_exp",
    # 减:营业税金及附加
    "biz_tax_surchg",
    # 减:销售费用
    "sell_exp",
    # 减:管理费用
    "admin_exp",
    # 减:财务费用
    "fin_exp",
    # 减:资产减值损失
    "assets_impair_loss",
    # 退保金
    "prem_refund",
    # 赔付总支出
    "compens_payout",
    # 提取保险责任准备金
    "reser_insur_liab",
    # 保户红利支出
    "div_payt",
    # 分保费用
    "reins_exp",
    # 营业支出
    "oper_exp",
    # 减:摊回赔付支出
    "compens_payout_refu",
    # 减:摊回保险责任准备金
    "insur_reser_refu",
    # 减:摊回分保费用
    "reins_cost_refund",
    # 其他业务成本
    "other_bus_cost",
    # 营业利润
    "operate_profit",
    # 加:营业外收入
    "non_oper_income",
    # 减:营业外支出
    "non_oper_exp",
    # 其中:减:非流动资产处置净损失
    "nca_disploss",
    # 利润总额
    "total_profit",
    # 所得税费用
    "income_tax",
    # 净利润(含少数股东损益)
    "n_income",
    # 净利润(不含少数股东损益)
    "n_income_attr_p",
    # 少数股东损益
    "minority_gain",
    # 其他综合收益
    "oth_compr_income",
    # 综合收益总额
    "t_compr_income",
    # 归属于母公司(或股东)的综合收益总额
    "compr_inc_attr_p",
    # 归属于少数股东的综合收益总额
    "compr_inc_attr_m_s",
    # 息税前利润
    "ebit",
    # 息税折旧摊销前利润
    "ebitda",
    # 保险业务支出
    "insurance_exp",
    # 年初未分配利润
    "undist_profit",
    # 可分配利润
    "distable_profit",
    # 研发费用
    "rd_exp",
    # 财务费用:利息费用
    "fin_exp_int_exp",
    # 财务费用:利息收入
    "fin_exp_int_inc",
    # 盈余公积转入
    "transfer_surplus_rese",
    # 住房周转金转入
    "transfer_housing_imprest",
    # 其他转入
    "transfer_oth",
    # 调整以前年度损益
    "adj_lossgain",
    # 提取法定盈余公积
    "withdra_legal_surplus",
    # 提取法定公益金
    "withdra_legal_pubfund",
    # 提取企业发展基金
    "withdra_biz_devfund",
    # 提取储备基金
    "withdra_rese_fund",
    # 提取任意盈余公积金
    "withdra_oth_ersu",
    # 职工奖金福利
    "workers_welfare",
    # 可供股东分配的利润
    "distr_profit_shrhder",
    # 应付优先股股利
    "prfshare_payable_dvd",
    # 应付普通股股利
    "comshare_payable_dvd",
    # 转作股本的普通股股利
    "capit_comstock_div",
    # 持续经营净利润
    "continued_net_profit",
    # 更新标识
    "update_flag",
)

# visible_at：优先使用 f_ann_date（实际公告日），缺失时使用 ann_date，公告日结束时可见。
# 官方新增的 6 个资产负债表字段目前会被 quicksync 省略，待代理实际返回后再扩展固定 Schema。
BALANCESHEET_FIELDS = (
    # TS股票代码
    "ts_code",
    # 公告日期
    "ann_date",
    # 实际公告日期
    "f_ann_date",
    # 报告期
    "end_date",
    # 报表类型
    "report_type",
    # 公司类型(1一般工商业2银行3保险4证券)
    "comp_type",
    # 报告期类型
    "end_type",
    # 期末总股本
    "total_share",
    # 资本公积金
    "cap_rese",
    # 未分配利润
    "undistr_porfit",
    # 盈余公积金
    "surplus_rese",
    # 专项储备
    "special_rese",
    # 货币资金
    "money_cap",
    # 交易性金融资产
    "trad_asset",
    # 应收票据
    "notes_receiv",
    # 应收账款
    "accounts_receiv",
    # 其他应收款
    "oth_receiv",
    # 预付款项
    "prepayment",
    # 应收股利
    "div_receiv",
    # 应收利息
    "int_receiv",
    # 存货
    "inventories",
    # 待摊费用
    "amor_exp",
    # 一年内到期的非流动资产
    "nca_within_1y",
    # 结算备付金
    "sett_rsrv",
    # 拆出资金
    "loanto_oth_bank_fi",
    # 应收保费
    "premium_receiv",
    # 应收分保账款
    "reinsur_receiv",
    # 应收分保合同准备金
    "reinsur_res_receiv",
    # 买入返售金融资产
    "pur_resale_fa",
    # 其他流动资产
    "oth_cur_assets",
    # 流动资产合计
    "total_cur_assets",
    # 可供出售金融资产
    "fa_avail_for_sale",
    # 持有至到期投资
    "htm_invest",
    # 长期股权投资
    "lt_eqt_invest",
    # 投资性房地产
    "invest_real_estate",
    # 定期存款
    "time_deposits",
    # 其他资产
    "oth_assets",
    # 长期应收款
    "lt_rec",
    # 固定资产
    "fix_assets",
    # 在建工程
    "cip",
    # 工程物资
    "const_materials",
    # 固定资产清理
    "fixed_assets_disp",
    # 生产性生物资产
    "produc_bio_assets",
    # 油气资产
    "oil_and_gas_assets",
    # 无形资产
    "intan_assets",
    # 研发支出
    "r_and_d",
    # 商誉
    "goodwill",
    # 长期待摊费用
    "lt_amor_exp",
    # 递延所得税资产
    "defer_tax_assets",
    # 发放贷款及垫款
    "decr_in_disbur",
    # 其他非流动资产
    "oth_nca",
    # 非流动资产合计
    "total_nca",
    # 现金及存放中央银行款项
    "cash_reser_cb",
    # 存放同业和其它金融机构款项
    "depos_in_oth_bfi",
    # 贵金属
    "prec_metals",
    # 衍生金融资产
    "deriv_assets",
    # 应收分保未到期责任准备金
    "rr_reins_une_prem",
    # 应收分保未决赔款准备金
    "rr_reins_outstd_cla",
    # 应收分保寿险责任准备金
    "rr_reins_lins_liab",
    # 应收分保长期健康险责任准备金
    "rr_reins_lthins_liab",
    # 存出保证金
    "refund_depos",
    # 保户质押贷款
    "ph_pledge_loans",
    # 存出资本保证金
    "refund_cap_depos",
    # 独立账户资产
    "indep_acct_assets",
    # 其中：客户资金存款
    "client_depos",
    # 其中：客户备付金
    "client_prov",
    # 其中:交易席位费
    "transac_seat_fee",
    # 应收款项类投资
    "invest_as_receiv",
    # 资产总计
    "total_assets",
    # 长期借款
    "lt_borr",
    # 短期借款
    "st_borr",
    # 向中央银行借款
    "cb_borr",
    # 吸收存款及同业存放
    "depos_ib_deposits",
    # 拆入资金
    "loan_oth_bank",
    # 交易性金融负债
    "trading_fl",
    # 应付票据
    "notes_payable",
    # 应付账款
    "acct_payable",
    # 预收款项
    "adv_receipts",
    # 卖出回购金融资产款
    "sold_for_repur_fa",
    # 应付手续费及佣金
    "comm_payable",
    # 应付职工薪酬
    "payroll_payable",
    # 应交税费
    "taxes_payable",
    # 应付利息
    "int_payable",
    # 应付股利
    "div_payable",
    # 其他应付款
    "oth_payable",
    # 预提费用
    "acc_exp",
    # 递延收益
    "deferred_inc",
    # 应付短期债券
    "st_bonds_payable",
    # 应付分保账款
    "payable_to_reinsurer",
    # 保险合同准备金
    "rsrv_insur_cont",
    # 代理买卖证券款
    "acting_trading_sec",
    # 代理承销证券款
    "acting_uw_sec",
    # 一年内到期的非流动负债
    "non_cur_liab_due_1y",
    # 其他流动负债
    "oth_cur_liab",
    # 流动负债合计
    "total_cur_liab",
    # 应付债券
    "bond_payable",
    # 长期应付款
    "lt_payable",
    # 专项应付款
    "specific_payables",
    # 预计负债
    "estimated_liab",
    # 递延所得税负债
    "defer_tax_liab",
    # 递延收益-非流动负债
    "defer_inc_non_cur_liab",
    # 其他非流动负债
    "oth_ncl",
    # 非流动负债合计
    "total_ncl",
    # 同业和其它金融机构存放款项
    "depos_oth_bfi",
    # 衍生金融负债
    "deriv_liab",
    # 吸收存款
    "depos",
    # 代理业务负债
    "agency_bus_liab",
    # 其他负债
    "oth_liab",
    # 预收保费
    "prem_receiv_adva",
    # 存入保证金
    "depos_received",
    # 保户储金及投资款
    "ph_invest",
    # 未到期责任准备金
    "reser_une_prem",
    # 未决赔款准备金
    "reser_outstd_claims",
    # 寿险责任准备金
    "reser_lins_liab",
    # 长期健康险责任准备金
    "reser_lthins_liab",
    # 独立账户负债
    "indept_acc_liab",
    # 其中:质押借款
    "pledge_borr",
    # 应付赔付款
    "indem_payable",
    # 应付保单红利
    "policy_div_payable",
    # 负债合计
    "total_liab",
    # 减:库存股
    "treasury_share",
    # 一般风险准备
    "ordin_risk_reser",
    # 外币报表折算差额
    "forex_differ",
    # 未确认的投资损失
    "invest_loss_unconf",
    # 少数股东权益
    "minority_int",
    # 股东权益合计(不含少数股东权益)
    "total_hldr_eqy_exc_min_int",
    # 股东权益合计(含少数股东权益)
    "total_hldr_eqy_inc_min_int",
    # 负债及股东权益总计
    "total_liab_hldr_eqy",
    # 长期应付职工薪酬
    "lt_payroll_payable",
    # 其他综合收益
    "oth_comp_income",
    # 其他权益工具
    "oth_eqt_tools",
    # 其他权益工具(优先股)
    "oth_eqt_tools_p_shr",
    # 融出资金
    "lending_funds",
    # 应收款项
    "acc_receivable",
    # 应付短期融资款
    "st_fin_payable",
    # 应付款项
    "payables",
    # 持有待售的资产
    "hfs_assets",
    # 持有待售的负债
    "hfs_sales",
    # 以摊余成本计量的金融资产
    "cost_fin_assets",
    # 以公允价值计量且其变动计入其他综合收益的金融资产
    "fair_value_fin_assets",
    # 在建工程(合计)(元)
    "cip_total",
    # 其他应付款(合计)(元)
    "oth_pay_total",
    # 长期应付款(合计)(元)
    "long_pay_total",
    # 债权投资(元)
    "debt_invest",
    # 其他债权投资(元)
    "oth_debt_invest",
    # 合同资产
    "contract_assets",
    # 合同负债
    "contract_liab",
    # 应收票据及应收账款
    "accounts_receiv_bill",
    # 应付票据及应付账款
    "accounts_pay",
    # 其他应收款(合计)（元）
    "oth_rcv_total",
    # 固定资产(合计)(元)
    "fix_assets_total",
    # 更新标识
    "update_flag",
)

# visible_at：优先使用 f_ann_date（实际公告日），缺失时使用 ann_date，公告日结束时可见。
CASHFLOW_FIELDS = (
    # TS股票代码
    "ts_code",
    # 公告日期
    "ann_date",
    # 实际公告日期
    "f_ann_date",
    # 报告期
    "end_date",
    # 公司类型(1一般工商业2银行3保险4证券)
    "comp_type",
    # 报表类型
    "report_type",
    # 报告期类型
    "end_type",
    # 净利润
    "net_profit",
    # 财务费用
    "finan_exp",
    # 销售商品、提供劳务收到的现金
    "c_fr_sale_sg",
    # 收到的税费返还
    "recp_tax_rends",
    # 客户存款和同业存放款项净增加额
    "n_depos_incr_fi",
    # 向中央银行借款净增加额
    "n_incr_loans_cb",
    # 向其他金融机构拆入资金净增加额
    "n_inc_borr_oth_fi",
    # 收到原保险合同保费取得的现金
    "prem_fr_orig_contr",
    # 保户储金净增加额
    "n_incr_insured_dep",
    # 收到再保业务现金净额
    "n_reinsur_prem",
    # 处置交易性金融资产净增加额
    "n_incr_disp_tfa",
    # 收取利息和手续费净增加额
    "ifc_cash_incr",
    # 处置可供出售金融资产净增加额
    "n_incr_disp_faas",
    # 拆入资金净增加额
    "n_incr_loans_oth_bank",
    # 回购业务资金净增加额
    "n_cap_incr_repur",
    # 收到其他与经营活动有关的现金
    "c_fr_oth_operate_a",
    # 经营活动现金流入小计
    "c_inf_fr_operate_a",
    # 购买商品、接受劳务支付的现金
    "c_paid_goods_s",
    # 支付给职工以及为职工支付的现金
    "c_paid_to_for_empl",
    # 支付的各项税费
    "c_paid_for_taxes",
    # 客户贷款及垫款净增加额
    "n_incr_clt_loan_adv",
    # 存放央行和同业款项净增加额
    "n_incr_dep_cbob",
    # 支付原保险合同赔付款项的现金
    "c_pay_claims_orig_inco",
    # 支付手续费的现金
    "pay_handling_chrg",
    # 支付保单红利的现金
    "pay_comm_insur_plcy",
    # 支付其他与经营活动有关的现金
    "oth_cash_pay_oper_act",
    # 经营活动现金流出小计
    "st_cash_out_act",
    # 经营活动产生的现金流量净额
    "n_cashflow_act",
    # 收到其他与投资活动有关的现金
    "oth_recp_ral_inv_act",
    # 收回投资收到的现金
    "c_disp_withdrwl_invest",
    # 取得投资收益收到的现金
    "c_recp_return_invest",
    # 处置固定资产、无形资产和其他长期资产收回的现金净额
    "n_recp_disp_fiolta",
    # 处置子公司及其他营业单位收到的现金净额
    "n_recp_disp_sobu",
    # 投资活动现金流入小计
    "stot_inflows_inv_act",
    # 购建固定资产、无形资产和其他长期资产支付的现金
    "c_pay_acq_const_fiolta",
    # 投资支付的现金
    "c_paid_invest",
    # 取得子公司及其他营业单位支付的现金净额
    "n_disp_subs_oth_biz",
    # 支付其他与投资活动有关的现金
    "oth_pay_ral_inv_act",
    # 质押贷款净增加额
    "n_incr_pledge_loan",
    # 投资活动现金流出小计
    "stot_out_inv_act",
    # 投资活动产生的现金流量净额
    "n_cashflow_inv_act",
    # 取得借款收到的现金
    "c_recp_borrow",
    # 发行债券收到的现金
    "proc_issue_bonds",
    # 收到其他与筹资活动有关的现金
    "oth_cash_recp_ral_fnc_act",
    # 筹资活动现金流入小计
    "stot_cash_in_fnc_act",
    # 企业自由现金流量
    "free_cashflow",
    # 偿还债务支付的现金
    "c_prepay_amt_borr",
    # 分配股利、利润或偿付利息支付的现金
    "c_pay_dist_dpcp_int_exp",
    # 其中:子公司支付给少数股东的股利、利润
    "incl_dvd_profit_paid_sc_ms",
    # 支付其他与筹资活动有关的现金
    "oth_cashpay_ral_fnc_act",
    # 筹资活动现金流出小计
    "stot_cashout_fnc_act",
    # 筹资活动产生的现金流量净额
    "n_cash_flows_fnc_act",
    # 汇率变动对现金的影响
    "eff_fx_flu_cash",
    # 现金及现金等价物净增加额
    "n_incr_cash_cash_equ",
    # 期初现金及现金等价物余额
    "c_cash_equ_beg_period",
    # 期末现金及现金等价物余额
    "c_cash_equ_end_period",
    # 吸收投资收到的现金
    "c_recp_cap_contrib",
    # 其中:子公司吸收少数股东投资收到的现金
    "incl_cash_rec_saims",
    # 未确认投资损失
    "uncon_invest_loss",
    # 加:资产减值准备
    "prov_depr_assets",
    # 固定资产折旧、油气资产折耗、生产性生物资产折旧
    "depr_fa_coga_dpba",
    # 无形资产摊销
    "amort_intang_assets",
    # 长期待摊费用摊销
    "lt_amort_deferred_exp",
    # 待摊费用减少
    "decr_deferred_exp",
    # 预提费用增加
    "incr_acc_exp",
    # 处置固定、无形资产和其他长期资产的损失
    "loss_disp_fiolta",
    # 固定资产报废损失
    "loss_scr_fa",
    # 公允价值变动损失
    "loss_fv_chg",
    # 投资损失
    "invest_loss",
    # 递延所得税资产减少
    "decr_def_inc_tax_assets",
    # 递延所得税负债增加
    "incr_def_inc_tax_liab",
    # 存货的减少
    "decr_inventories",
    # 经营性应收项目的减少
    "decr_oper_payable",
    # 经营性应付项目的增加
    "incr_oper_payable",
    # 其他
    "others",
    # 经营活动产生的现金流量净额(间接法)
    "im_net_cashflow_oper_act",
    # 债务转为资本
    "conv_debt_into_cap",
    # 一年内到期的可转换公司债券
    "conv_copbonds_due_within_1y",
    # 融资租入固定资产
    "fa_fnc_leases",
    # 现金及现金等价物净增加额(间接法)
    "im_n_incr_cash_equ",
    # 拆出资金净增加额
    "net_dism_capital_add",
    # 代理买卖证券收到的现金净额(元)
    "net_cash_rece_sec",
    # 信用减值损失
    "credit_impa_loss",
    # 使用权资产折旧
    "use_right_asset_dep",
    # 其他资产减值损失
    "oth_loss_asset",
    # 现金的期末余额
    "end_bal_cash",
    # 减:现金的期初余额
    "beg_bal_cash",
    # 加:现金等价物的期末余额
    "end_bal_cash_equ",
    # 减:现金等价物的期初余额
    "beg_bal_cash_equ",
    # 更新标志(1最新）
    "update_flag",
)

# visible_at：使用 ann_date，公告日结束时可见；end_date 只表示报告期，不能用于可见时间。
FINA_INDICATOR_FIELDS = (
    # TS代码
    "ts_code",
    # 公告日期
    "ann_date",
    # 报告期
    "end_date",
    # 基本每股收益
    "eps",
    # 稀释每股收益
    "dt_eps",
    # 每股营业总收入
    "total_revenue_ps",
    # 每股营业收入
    "revenue_ps",
    # 每股资本公积
    "capital_rese_ps",
    # 每股盈余公积
    "surplus_rese_ps",
    # 每股未分配利润
    "undist_profit_ps",
    # 非经常性损益
    "extra_item",
    # 扣除非经常性损益后的净利润（扣非净利润）
    "profit_dedt",
    # 毛利
    "gross_margin",
    # 流动比率
    "current_ratio",
    # 速动比率
    "quick_ratio",
    # 保守速动比率
    "cash_ratio",
    # 存货周转天数
    "invturn_days",
    # 应收账款周转天数
    "arturn_days",
    # 存货周转率
    "inv_turn",
    # 应收账款周转率
    "ar_turn",
    # 流动资产周转率
    "ca_turn",
    # 固定资产周转率
    "fa_turn",
    # 总资产周转率
    "assets_turn",
    # 经营活动净收益
    "op_income",
    # 价值变动净收益
    "valuechange_income",
    # 利息费用
    "interst_income",
    # 折旧与摊销
    "daa",
    # 息税前利润
    "ebit",
    # 息税折旧摊销前利润
    "ebitda",
    # 企业自由现金流量
    "fcff",
    # 股权自由现金流量
    "fcfe",
    # 无息流动负债
    "current_exint",
    # 无息非流动负债
    "noncurrent_exint",
    # 带息债务
    "interestdebt",
    # 净债务
    "netdebt",
    # 有形资产
    "tangible_asset",
    # 营运资金
    "working_capital",
    # 营运流动资本
    "networking_capital",
    # 全部投入资本
    "invest_capital",
    # 留存收益
    "retained_earnings",
    # 期末摊薄每股收益
    "diluted2_eps",
    # 每股净资产
    "bps",
    # 每股经营活动产生的现金流量净额
    "ocfps",
    # 每股留存收益
    "retainedps",
    # 每股现金流量净额
    "cfps",
    # 每股息税前利润
    "ebit_ps",
    # 每股企业自由现金流量
    "fcff_ps",
    # 每股股东自由现金流量
    "fcfe_ps",
    # 销售净利率
    "netprofit_margin",
    # 销售毛利率
    "grossprofit_margin",
    # 销售成本率
    "cogs_of_sales",
    # 销售期间费用率
    "expense_of_sales",
    # 净利润/营业总收入
    "profit_to_gr",
    # 销售费用/营业总收入
    "saleexp_to_gr",
    # 管理费用/营业总收入
    "adminexp_of_gr",
    # 财务费用/营业总收入
    "finaexp_of_gr",
    # 资产减值损失/营业总收入
    "impai_ttm",
    # 营业总成本/营业总收入
    "gc_of_gr",
    # 营业利润/营业总收入
    "op_of_gr",
    # 息税前利润/营业总收入
    "ebit_of_gr",
    # 净资产收益率
    "roe",
    # 加权平均净资产收益率
    "roe_waa",
    # 净资产收益率(扣除非经常损益)
    "roe_dt",
    # 总资产报酬率
    "roa",
    # 总资产净利润
    "npta",
    # 投入资本回报率
    "roic",
    # 年化净资产收益率
    "roe_yearly",
    # 年化总资产报酬率
    "roa2_yearly",
    # 平均净资产收益率(增发条件)
    "roe_avg",
    # 经营活动净收益/利润总额
    "opincome_of_ebt",
    # 价值变动净收益/利润总额
    "investincome_of_ebt",
    # 营业外收支净额/利润总额
    "n_op_profit_of_ebt",
    # 所得税/利润总额
    "tax_to_ebt",
    # 扣除非经常损益后的净利润/净利润
    "dtprofit_to_profit",
    # 销售商品提供劳务收到的现金/营业收入
    "salescash_to_or",
    # 经营活动产生的现金流量净额/营业收入
    "ocf_to_or",
    # 经营活动产生的现金流量净额/经营活动净收益
    "ocf_to_opincome",
    # 资本支出/折旧和摊销
    "capitalized_to_da",
    # 资产负债率
    "debt_to_assets",
    # 权益乘数
    "assets_to_eqt",
    # 权益乘数(杜邦分析)
    "dp_assets_to_eqt",
    # 流动资产/总资产
    "ca_to_assets",
    # 非流动资产/总资产
    "nca_to_assets",
    # 有形资产/总资产
    "tbassets_to_totalassets",
    # 带息债务/全部投入资本
    "int_to_talcap",
    # 归属于母公司的股东权益/全部投入资本
    "eqt_to_talcapital",
    # 流动负债/负债合计
    "currentdebt_to_debt",
    # 非流动负债/负债合计
    "longdeb_to_debt",
    # 经营活动产生的现金流量净额/流动负债
    "ocf_to_shortdebt",
    # 产权比率
    "debt_to_eqt",
    # 归属于母公司的股东权益/负债合计
    "eqt_to_debt",
    # 归属于母公司的股东权益/带息债务
    "eqt_to_interestdebt",
    # 有形资产/负债合计
    "tangibleasset_to_debt",
    # 有形资产/带息债务
    "tangasset_to_intdebt",
    # 有形资产/净债务
    "tangibleasset_to_netdebt",
    # 经营活动产生的现金流量净额/负债合计
    "ocf_to_debt",
    # 经营活动产生的现金流量净额/带息债务
    "ocf_to_interestdebt",
    # 经营活动产生的现金流量净额/净债务
    "ocf_to_netdebt",
    # 已获利息倍数(EBIT/利息费用)
    "ebit_to_interest",
    # 长期债务与营运资金比率
    "longdebt_to_workingcapital",
    # 息税折旧摊销前利润/负债合计
    "ebitda_to_debt",
    # 营业周期
    "turn_days",
    # 年化总资产净利率
    "roa_yearly",
    # 总资产净利率(杜邦分析)
    "roa_dp",
    # 固定资产合计
    "fixed_assets",
    # 扣除财务费用前营业利润
    "profit_prefin_exp",
    # 非营业利润
    "non_op_profit",
    # 营业利润／利润总额
    "op_to_ebt",
    # 非营业利润／利润总额
    "nop_to_ebt",
    # 经营活动产生的现金流量净额／营业利润
    "ocf_to_profit",
    # 货币资金／流动负债
    "cash_to_liqdebt",
    # 货币资金／带息流动负债
    "cash_to_liqdebt_withinterest",
    # 营业利润／流动负债
    "op_to_liqdebt",
    # 营业利润／负债合计
    "op_to_debt",
    # 年化投入资本回报率
    "roic_yearly",
    # 固定资产合计周转率
    "total_fa_trun",
    # 利润总额／营业收入
    "profit_to_op",
    # 经营活动单季度净收益
    "q_opincome",
    # 价值变动单季度净收益
    "q_investincome",
    # 扣除非经常损益后的单季度净利润
    "q_dtprofit",
    # 每股收益(单季度)
    "q_eps",
    # 销售净利率(单季度)
    "q_netprofit_margin",
    # 销售毛利率(单季度)
    "q_gsprofit_margin",
    # 销售期间费用率(单季度)
    "q_exp_to_sales",
    # 净利润／营业总收入(单季度)
    "q_profit_to_gr",
    # 销售费用／营业总收入 (单季度)
    "q_saleexp_to_gr",
    # 管理费用／营业总收入 (单季度)
    "q_adminexp_to_gr",
    # 财务费用／营业总收入 (单季度)
    "q_finaexp_to_gr",
    # 资产减值损失／营业总收入(单季度)
    "q_impair_to_gr_ttm",
    # 营业总成本／营业总收入 (单季度)
    "q_gc_to_gr",
    # 营业利润／营业总收入(单季度)
    "q_op_to_gr",
    # 净资产收益率(单季度)
    "q_roe",
    # 净资产单季度收益率(扣除非经常损益)
    "q_dt_roe",
    # 总资产净利润(单季度)
    "q_npta",
    # 经营活动净收益／利润总额(单季度)
    "q_opincome_to_ebt",
    # 价值变动净收益／利润总额(单季度)
    "q_investincome_to_ebt",
    # 扣除非经常损益后的净利润／净利润(单季度)
    "q_dtprofit_to_profit",
    # 销售商品提供劳务收到的现金／营业收入(单季度)
    "q_salescash_to_or",
    # 经营活动产生的现金流量净额／营业收入(单季度)
    "q_ocf_to_sales",
    # 经营活动产生的现金流量净额／经营活动净收益(单季度)
    "q_ocf_to_or",
    # 基本每股收益同比增长率(%)
    "basic_eps_yoy",
    # 稀释每股收益同比增长率(%)
    "dt_eps_yoy",
    # 每股经营活动产生的现金流量净额同比增长率(%)
    "cfps_yoy",
    # 营业利润同比增长率(%)
    "op_yoy",
    # 利润总额同比增长率(%)
    "ebt_yoy",
    # 归属母公司股东的净利润同比增长率(%)
    "netprofit_yoy",
    # 归属母公司股东的净利润-扣除非经常损益同比增长率(%)
    "dt_netprofit_yoy",
    # 经营活动产生的现金流量净额同比增长率(%)
    "ocf_yoy",
    # 净资产收益率(摊薄)同比增长率(%)
    "roe_yoy",
    # 每股净资产相对年初增长率(%)
    "bps_yoy",
    # 资产总计相对年初增长率(%)
    "assets_yoy",
    # 归属母公司的股东权益相对年初增长率(%)
    "eqt_yoy",
    # 营业总收入同比增长率(%)
    "tr_yoy",
    # 营业收入同比增长率(%)
    "or_yoy",
    # 营业总收入同比增长率(%)(单季度)
    "q_gr_yoy",
    # 营业总收入环比增长率(%)(单季度)
    "q_gr_qoq",
    # 营业收入同比增长率(%)(单季度)
    "q_sales_yoy",
    # 营业收入环比增长率(%)(单季度)
    "q_sales_qoq",
    # 营业利润同比增长率(%)(单季度)
    "q_op_yoy",
    # 营业利润环比增长率(%)(单季度)
    "q_op_qoq",
    # 净利润同比增长率(%)(单季度)
    "q_profit_yoy",
    # 净利润环比增长率(%)(单季度)
    "q_profit_qoq",
    # 归属母公司股东的净利润同比增长率(%)(单季度)
    "q_netprofit_yoy",
    # 归属母公司股东的净利润环比增长率(%)(单季度)
    "q_netprofit_qoq",
    # 净资产同比增长率
    "equity_yoy",
    # 研发投入合计
    "rd_exp",
    # 更新标识
    "update_flag",
)

# visible_at：原始 in_date/out_date 分别生成 IN/OUT 事件，使用对应事件日的 09:00。
INDEX_MEMBER_ALL_FIELDS = (
    # 一级行业代码
    "l1_code",
    # 一级行业名称
    "l1_name",
    # 二级行业代码
    "l2_code",
    # 二级行业名称
    "l2_name",
    # 三级行业代码
    "l3_code",
    # 三级行业名称
    "l3_name",
    # 成分股票代码
    "ts_code",
    # 成分股票名称
    "name",
    # 纳入日期
    "in_date",
    # 剔除日期
    "out_date",
    # 是否最新Y是N否
    "is_new",
)

DATE_FIELDS = {
    "trade_date",
    "end_date",
    "ann_date",
    "f_ann_date",
    "record_date",
    "ex_date",
    "pay_date",
    "div_listdate",
    "imp_ann_date",
    "base_date",
    "first_ann_date",
}

STRING_FIELDS = {
    "ts_code",
    "report_type",
    "comp_type",
    "end_type",
    "update_flag",
    "suspend_timing",
    "suspend_type",
    "div_proc",
    "name",
    "type",
    "type_name",
    "summary",
    "change_reason",
    "perf_summary",
    "remark",
    "audit_result",
    "audit_agency",
    "audit_sign",
}

INTEGER_FIELDS = {
    "buy_sm_vol",
    "sell_sm_vol",
    "buy_md_vol",
    "sell_md_vol",
    "buy_lg_vol",
    "sell_lg_vol",
    "buy_elg_vol",
    "sell_elg_vol",
    "net_mf_vol",
    "is_audit",
}


def _source_schema(fields: tuple[str, ...]) -> pa.Schema:
    """把固定的 Tushare 字段表转换为固定 Arrow Schema。"""
    arrow_fields: list[pa.Field[pa.DataType]] = [
        # visible_at 对应的北京时间日期，只用于物理分区。
        pa.field("partition_date", pa.date32(), nullable=False),
        # Tushare 股票代码。
        pa.field("ts_code", pa.string(), nullable=False),
        # 数据可安全用于研究的 UTC Unix Epoch 微秒时间戳。
        pa.field("visible_at", VISIBLE_TIME_TYPE, nullable=False),
    ]
    for name in fields:
        if name == "ts_code":
            continue
        if name in DATE_FIELDS:
            data_type = pa.date32()
        elif name in STRING_FIELDS:
            data_type = pa.string()
        elif name in INTEGER_FIELDS:
            data_type = pa.int64()
        else:
            data_type = pa.float64()
        arrow_fields.append(pa.field(name, data_type))
    return pa.schema(arrow_fields)


DAILY_SCHEMA = _source_schema(DAILY_FIELDS)
DAILY_BASIC_SCHEMA = _source_schema(DAILY_BASIC_FIELDS)
ADJ_FACTOR_SCHEMA = _source_schema(ADJ_FACTOR_FIELDS)
SUSPEND_D_SCHEMA = _source_schema(SUSPEND_D_FIELDS)
STK_LIMIT_SCHEMA = _source_schema(STK_LIMIT_FIELDS)
STOCK_ST_SCHEMA = _source_schema(STOCK_ST_FIELDS)
MONEYFLOW_SCHEMA = _source_schema(MONEYFLOW_FIELDS)
DIVIDEND_SCHEMA = _source_schema(DIVIDEND_FIELDS)
FORECAST_SCHEMA = _source_schema(FORECAST_FIELDS)
EXPRESS_SCHEMA = _source_schema(EXPRESS_FIELDS)
FINA_AUDIT_SCHEMA = _source_schema(FINA_AUDIT_FIELDS)
INCOME_SCHEMA = _source_schema(INCOME_FIELDS)
BALANCESHEET_SCHEMA = _source_schema(BALANCESHEET_FIELDS)
CASHFLOW_SCHEMA = _source_schema(CASHFLOW_FIELDS)
FINA_INDICATOR_SCHEMA = _source_schema(FINA_INDICATOR_FIELDS)

# 申万行业成员数据拆成 IN/OUT 事件。若把当前接口返回的未来 out_date 直接放在
# in_date 那一行，历史回测会提前知道股票将在哪一天被移出行业。
SW_INDUSTRY_SCHEMA = pa.schema(
    [
        # 事件可见时间对应的北京时间日期，只用于物理分区。
        pa.field("partition_date", pa.date32(), nullable=False),
        # 成分股票的 Tushare 代码。
        pa.field("ts_code", pa.string(), nullable=False),
        # 事件可安全用于研究的 UTC Unix Epoch 微秒时间戳。
        pa.field("visible_at", VISIBLE_TIME_TYPE, nullable=False),
        # 股票纳入或移出申万行业的事件日期。
        pa.field("event_date", pa.date32(), nullable=False),
        # IN 表示纳入，OUT 表示移出。
        pa.field("event_type", pa.string(), nullable=False),
        # 申万一级行业代码。
        pa.field("l1_code", pa.string()),
        # 申万一级行业名称。
        pa.field("l1_name", pa.string()),
        # 申万二级行业代码。
        pa.field("l2_code", pa.string()),
        # 申万二级行业名称。
        pa.field("l2_name", pa.string()),
        # 申万三级行业代码。
        pa.field("l3_code", pa.string()),
        # 申万三级行业名称。
        pa.field("l3_name", pa.string()),
        # 成分股票名称。
        pa.field("stock_name", pa.string()),
    ]
)

TRADE_CAL_SCHEMA = pa.schema(
    [
        # 交易所代码：SSE、SZSE 或 BSE。
        pa.field("exchange", pa.string(), nullable=False),
        # 日历信息可见的 UTC Unix Epoch 微秒时间戳。
        pa.field("visible_at", VISIBLE_TIME_TYPE, nullable=False),
        # 日历日期，也是该表的物理分区字段。
        pa.field("cal_date", pa.date32(), nullable=False),
        # 是否开市：1 开市，0 休市。
        pa.field("is_open", pa.int8(), nullable=False),
        # 当前日期之前最近一个交易日。
        pa.field("pretrade_date", pa.date32()),
    ]
)

TABLE_SCHEMAS = {
    "daily": DAILY_SCHEMA,
    "daily_basic": DAILY_BASIC_SCHEMA,
    "adj_factor": ADJ_FACTOR_SCHEMA,
    "suspend_d": SUSPEND_D_SCHEMA,
    "stk_limit": STK_LIMIT_SCHEMA,
    "stock_st": STOCK_ST_SCHEMA,
    "moneyflow": MONEYFLOW_SCHEMA,
    "dividend": DIVIDEND_SCHEMA,
    "forecast": FORECAST_SCHEMA,
    "express": EXPRESS_SCHEMA,
    "fina_audit": FINA_AUDIT_SCHEMA,
    "income": INCOME_SCHEMA,
    "balancesheet": BALANCESHEET_SCHEMA,
    "cashflow": CASHFLOW_SCHEMA,
    "fina_indicator": FINA_INDICATOR_SCHEMA,
    "sw_industry": SW_INDUSTRY_SCHEMA,
    "trade_cal": TRADE_CAL_SCHEMA,
}

SOURCE_FIELDS = {
    "daily": DAILY_FIELDS,
    "daily_basic": DAILY_BASIC_FIELDS,
    "adj_factor": ADJ_FACTOR_FIELDS,
    "suspend_d": SUSPEND_D_FIELDS,
    "stk_limit": STK_LIMIT_FIELDS,
    "stock_st": STOCK_ST_FIELDS,
    "moneyflow": MONEYFLOW_FIELDS,
    "dividend": DIVIDEND_FIELDS,
    "forecast": FORECAST_FIELDS,
    "express": EXPRESS_FIELDS,
    "fina_audit": FINA_AUDIT_FIELDS,
    "income": INCOME_FIELDS,
    "balancesheet": BALANCESHEET_FIELDS,
    "cashflow": CASHFLOW_FIELDS,
    "fina_indicator": FINA_INDICATOR_FIELDS,
    "sw_industry": INDEX_MEMBER_ALL_FIELDS,
    "trade_cal": TRADE_CAL_FIELDS,
}

TABLE_PARTITION_BY = {
    dataset: "cal_date" if dataset == "trade_cal" else "partition_date" for dataset in TABLE_SCHEMAS
}

TABLE_SORT_BY = {
    dataset: "exchange" if dataset == "trade_cal" else "ts_code" for dataset in TABLE_SCHEMAS
}
