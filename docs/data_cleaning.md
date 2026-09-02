# `data_cleaning`：检测、修复、发布

`data_cleaning` 把 Tushare 采集数据发布成 Reader 可以直接使用的版本。
整个流程只有三步，三步之间通过报告文件衔接：

```text
dataset/tushare
      ↓ detect：逐数据集、逐项检查
quality/issues/detected.jsonl
      ↓ repair：读取检测报告，自动补丁或定向重拉
quality/issues/repaired.jsonl
      ↓ publish：逐数据集合并修复、复检、发布
dataset/tushare_published/current
```

不会在回测时跳过异常行。有未解决问题的数据集整体标记为
`UNAVAILABLE`，回测读取它时立即报错。

## 一、具体怎么用

所有命令都在项目根目录执行。首次使用先安装依赖：

```bash
uv sync --group data-cleaning
```

### 1. 检测指定数据集和区间

例如，只检查 `daily` 在 2024-07-01 到 2024-07-31 的数据：

```bash
uv run --group data-cleaning data-cleaning detect \
  --input dataset/tushare \
  --datasets daily \
  --start 2024-07-01 \
  --through 2024-07-31 \
  --output quality/issues/daily-202407-detected.jsonl
```

- `detect` 只读，不修改任何数据；
- `--datasets` 可以写多个，例如 `daily adj_factor stk_limit`；
- `--start` 和 `--through` 都包含当天；
- 局部报告用于排查问题，因为带 `--start`，不能直接发布。

终端会输出每一个检查项，包括通过项：

```text
检测结果：
daily (52000 行)
  [通过] manifest_v1 - Manifest 格式正确
  [通过] schema_v1 - Parquet Schema 与登记 Schema 一致
  [失败] daily_ohlc_v1 - 开高低收关系正确：0 个自动，1 个人工
{"datasets":["daily"],"issues":1,"manual":1,"auto_fix":0,"output":"..."}
```

`manual` 大于 0 时退出码为 1，表示“成功检出待处理问题”，不是程序崩溃。

### 2. 修复必须读取这份检测报告

```bash
export TUSHARE_TOKEN='你的 token'

uv run --group data-cleaning data-cleaning repair \
  --input dataset/tushare \
  --issues quality/issues/daily-202407-detected.jsonl \
  --output quality/issues/daily-202407-repaired.jsonl
```

`repair` 不再接受 `--datasets`、`--start` 或 `--through`，因为它必须严格使用
检测报告中已经记录的范围。

对每个问题，只有三种明确结果：

- `自动补丁`：正确值唯一，记录补丁，由 `publish` 合并到发布数据；
- `自动重拉`：问题能定位到数据集和日期，从 Tushare 强制重拉后复检；
- `待人工`：无法确定唯一修法，不自动猜测。

只有存在“自动重拉”时才需要 `TUSHARE_TOKEN`。如果报告中只有自动补丁或人工问题，
可以不设 token。

输出示例：

```text
修复计划：
  [自动重拉] daily daily_ohlc_v1 - 重拉 daily 对应交易日
...
待人工干预：0
```

如果重拉两轮后问题仍在，或者问题本身无法自动处理，终端会逐条输出：

```text
待人工干预：1
  <issue_id> | daily | daily_ohlc_v1 | <原因>
```

### 3. 正式发布前做全量检测和修复

发布报告不能带 `--start`。推荐不写 `--datasets`，一次检查所有数据集：

```bash
uv run --group data-cleaning data-cleaning detect \
  --input dataset/tushare \
  --through 2026-08-22 \
  --output quality/issues/20260822-detected.jsonl
```

再让修复命令读取这份报告：

```bash
export TUSHARE_TOKEN='你的 token'

uv run --group data-cleaning data-cleaning repair \
  --input dataset/tushare \
  --issues quality/issues/20260822-detected.jsonl \
  --output quality/issues/20260822-repaired.jsonl
```

### 4. 按数据集合并修复并发布

```bash
uv run --group data-cleaning data-cleaning publish \
  --input dataset/tushare \
  --issues quality/issues/20260822-repaired.jsonl \
  --output-root dataset/tushare_published \
  --release 20260822-v1
```

`publish` 对每个数据集分别执行：

1. 检查该数据集是否还有未解决人工问题；
2. 合并该数据集的确定性自动补丁和人工 `PATCH`；
3. 未修改分区使用硬链接，修改分区重写 Parquet；
4. 只复检该数据集；
5. 复检通过标记 `AVAILABLE`，否则标记 `UNAVAILABLE`。

终端会逐数据集输出发布结果：

```text
发布结果：
  [AVAILABLE] daily: 3200000 行，未解决 0 个
  [UNAVAILABLE] adj_factor: 2100000 行，未解决 1 个
```

它同时生成：

```text
dataset/tushare_published/
├── current -> releases/20260822-v1
└── releases/
    └── 20260822-v1/
        ├── daily/...
        ├── adj_factor/...
        └── release.json
```

### 5. 回测只读发布版本

```bash
uv run --group backtest backtest-momentum \
  --start 2017-01-01 \
  --end 2026-08-22 \
  --tushare-dir dataset/tushare_published/current \
  --qmt-dir dataset/qmt
```

正式回测不要直接读 `dataset/tushare`，因为它是未经发布门禁的采集层。

## 二、检测报告里有什么

`jsonl` 文件每行是一个 JSON：

- 第一行 `kind=report`：检测范围、数据集、行数和输入指纹；
- 中间的 `kind=check`：每个检查项的 `PASS` / `WARN` / `FAIL` 和问题数；
- 后面的 `kind=issue`：只记录失败问题的分区、主键、观测值和修复建议。

查看报告：

```bash
sed -n '1,30p' quality/issues/20260822-repaired.jsonl
```

这样既能看到“检查了什么”，也能看到“哪些检查没通过”。

## 三、所有数据集都检查什么

### 每个数据集都执行的通用检查

| 检查 ID | 检查内容 | 检出后的修复方式 |
| --- | --- | --- |
| `dataset_empty_v1` | 全量发布时数据集不能为空 | 待人工确认应采集范围 |
| `manifest_v1` | Manifest 是合法 JSON，`files` 列表格式正确 | 待人工恢复或重建分区 |
| `manifest_file_missing_v1` | Manifest 引用的 Parquet 都存在 | 待人工恢复或重建分区 |
| `parquet_read_v1` | Parquet 文件可读 | 待人工从原数据恢复 |
| `schema_v1` | Parquet Schema 与该数据集的登记 Schema 一致 | 待人工核对版本并重建 |
| `partition_value_v1` | 分区路径日期与记录分区字段一致 | 待人工核对后重建 |
| `duplicate_key_v1` | 分区内业务主键不重复 | 可定位日期时自动重拉，否则待人工 |
| `required_value_v1` | Schema 必填字段不为空 | 可定位日期时自动重拉，否则待人工 |
| `finite_float_v1` | 浮点数不包含 `NaN` 或无穷值 | 关键字段重拉；其他可空字段自动补为 `null` |

### 检查结果的级别

- `PASS`：没有发现问题；
- `WARN`：发现需要重拉或人工复核的异常，但它可能是合法公司行动、历史修订或特殊市场规则，不阻止发布；
- `FAIL`：确定性错误，自动重拉后仍存在时阻止对应数据集发布。

检测问题还保留原有修复方式：`AUTO_FIX` 表示有唯一修正值，`MANUAL` 表示不能直接改值。
`WARNING + MANUAL` 会列入待人工清单，但不会把数据集标记为不可用。

### 需要交易日连续性的数据集

`daily`、`daily_basic`、`adj_factor`、`stk_limit`、`stock_st` 和 `moneyflow`
额外执行：

| 检查 ID | 检查内容 | 修复方式 |
| --- | --- | --- |
| `missing_market_partition_v1` | 不缺 `trade_cal` 中已知开市日的分区 | 自动重拉缺失日期 |
| `closed_market_partition_v1` | 日级市场数据不能出现在休市日 | 自动重拉；仍存在则人工核对日历 |

检查范围会一直延伸到 `--through`，所以最新一段数据整体没拉到也能发现，不再只检查实际数据首尾之间的洞。

### 每个数据集的专有检查

| 数据集 | 检查 ID | 检查内容 | 级别与处理 |
| --- | --- | --- | --- |
| `stock_basic` | `stock_basic_identity_v1` | `ts_code`、`symbol`、交易所和代码后缀一致 | FAIL，重拉上市日期 |
| `stock_basic` | `stock_basic_lifecycle_v1` | 上市状态合法，上市日不晚于退市日，退市状态有退市日 | FAIL，重拉上市日期 |
| `daily` | `daily_missing_v1` | `open/high/low/close/pre_close/vol/amount` 完整 | 自动重拉该交易日 |
| `daily` | `daily_range_v1` | 价格为正，成交量和成交额非负 | 自动重拉该交易日 |
| `daily` | `daily_ohlc_v1` | `high ≥ open/low/close`，`low ≤ open/high/close` | 自动重拉该交易日 |
| `daily` | `daily_close_consistency_v1` | `change` 和 `pct_chg` 独立推导的收盘价一致 | 唯一结果生成自动补丁 |
| `daily` | `daily_arithmetic_v1` | `close≈pre_close+change`，`pct_chg≈change/pre_close×100` | FAIL，自动重拉 |
| `daily` | `daily_volume_amount_v1` | 成交量和成交额不能只有一个为零 | FAIL，自动重拉 |
| `daily_basic` | `daily_basic_range_v1` | 股本、市值、换手率等应为非负数 | FAIL，自动重拉 |
| `daily_basic` | `daily_basic_share_order_v1` | `total_share ≥ float_share ≥ free_share` | WARN，复核股本口径 |
| `daily_basic` | `daily_basic_market_value_v1` | `total_mv≈close×total_share`，`circ_mv≈close×float_share` | WARN，复核单位与舍入 |
| `daily_basic` | `daily_basic_daily_match_v1` | 与 `daily` 同日覆盖且收盘价相同 | WARN，交叉复核 |
| `adj_factor` | `adj_factor_positive_v1` | 复权因子大于 0 | 自动重拉该交易日 |
| `adj_factor` | `adj_factor_daily_coverage_v1` | 每条日线都有同代码同日复权因子 | FAIL，自动重拉因子 |
| `adj_factor` | `adj_factor_continuity_v1` | `round(昨日close×昨日factor÷今日factor, 2)≈今日pre_close` | WARN，重拉后复核公司行动 |
| `adj_factor` | `adj_factor_decrease_v1` | 因子下降需检查公司行动、代码变更或历史修订 | WARN，不能按单调性直接判错 |
| `adj_factor` | `adj_factor_without_daily_v1` | 因子没有同日日线 | WARN，区分停牌、退市和历史代码 |
| `suspend_d` | `suspend_value_v1` | 类型只能为 S/R，复牌不能带日内停牌时间段 | FAIL，自动重拉 |
| `suspend_d` | `suspend_daily_conflict_v1` | 全日停牌证券不能仍有同日日线 | FAIL，重拉后交叉复核 |
| `stk_limit` | `stk_limit_partition_missing_v1` | 整个分区的涨停价或跌停价不能整列为空 | 自动重拉该交易日 |
| `stk_limit` | `stk_limit_missing_v1` | 每行涨停价和跌停价完整；`pre_close` 对部分非股票标的允许为空 | 自动重拉该交易日 |
| `stk_limit` | `stk_limit_order_v1` | `down_limit ≤ pre_close ≤ up_limit`；无涨跌幅限制允许 0/99999.99 哨兵值 | 自动重拉该交易日 |
| `stk_limit` | `stk_limit_daily_match_v1` | 与日线昨收一致，OHLC 不越过限制价格 | WARN，交叉复核 |
| `stock_st` | `stock_st_value_v1` | 证券名称、类型和类型名称完整 | FAIL，自动重拉 |
| `moneyflow` | `moneyflow_range_v1` | 买卖各档量额非负；净流入允许为负 | FAIL，自动重拉 |
| `moneyflow` | `moneyflow_daily_coverage_v1` | 资金流向有对应同日日线 | WARN，交叉复核；不强制净流入等于各档相减 |
| `dividend` | `dividend_value_v1` | 实施进度合法，送转、派现和基准股本非负，税后不高于税前 | FAIL，自动重拉公告日 |
| `dividend` | `dividend_stock_ratio_v1` | `stk_div≈stk_bo_rate+stk_co_rate` | FAIL，自动重拉公告日 |
| `dividend` | `dividend_date_order_v1` | 实施记录具备实施公告、登记、除权日期且顺序合理 | WARN，复核实施公告 |
| `forecast` | `forecast_value_v1` | 预告类型和更新标识合法 | FAIL，自动重拉公告日 |
| `forecast` | `forecast_range_v1` | 比例、利润下限不高于上限 | FAIL，自动重拉公告日 |
| `forecast` | `forecast_date_order_v1` | 报告期为季度末且首次公告不晚于本次公告 | WARN，复核供应商历史日期 |
| `express` | `express_value_v1` | 报告期、公告日和更新标识合法 | FAIL，自动重拉公告日 |
| `express` | `express_audit_flag_v1` | 审计标识不是官方文档中的 0/1 | WARN，兼容并复核供应商新口径 |
| `express` | `express_growth_v1` | 收入、利润和 EPS 同比可由本期及同期值重算 | WARN，复核修订口径 |
| `fina_audit` | `fina_audit_value_v1` | 日期、费用、年度审计意见和事务所合理 | WARN，复核审计记录 |
| `income` | `income_value_v1` | 实际公告日、季度末、报表类型、公司类型和更新标识合法 | FAIL，自动重拉公告日 |
| `income` | `income_equation_v1` | 利润总额、净利润、少数股东损益和综合收益勾稽 | WARN，按报表口径复核 |
| `balancesheet` | `balancesheet_value_v1` | 实际公告日、季度末、报表类型、公司类型和更新标识合法 | FAIL，自动重拉公告日 |
| `balancesheet` | `balancesheet_equation_v1` | 资产=负债及权益，负债与含少数股东权益勾稽 | WARN，按报表口径复核 |
| `cashflow` | `cashflow_value_v1` | 实际公告日、季度末、报表类型、公司类型和更新标识合法 | FAIL，自动重拉公告日 |
| `cashflow` | `cashflow_equation_v1` | 经营/投资/筹资净额、现金净增加额和期末余额勾稽 | WARN，按报表口径复核 |
| `fina_indicator` | `fina_indicator_value_v1` | 公告日、季度末和更新标识合法 | FAIL，自动重拉公告日 |
| `sw_industry` | `sw_industry_value_v1` | 三级层级字段、纳入剔除日期和最新标识合法 | WARN，复核行业历史 |
| `sw_industry` | `sw_industry_mapping_v1` | 代码名称映射唯一，同股有效区间不重叠且最多一个当前行业 | WARN，复核更名和换版 |
| `trade_cal` | `trade_calendar_value_v1` | 交易所是 SSE/SZSE，`is_open` 是 0/1 | 自动重拉该日期 |
| `trade_cal` | `calendar_exchange_coverage_v1` | 每个日期都覆盖 SSE 和 SZSE；Tushare 不提供独立 BSE 日历行 | 自动重拉该日期 |
| `trade_cal` | `calendar_date_coverage_v1` | 请求范围内每个自然日都有日历记录 | FAIL，自动重拉缺失日 |
| `trade_cal` | `calendar_pretrade_v1` | `pretrade_date` 指向该市场此前最近开市日 | WARN，重拉后复核 |

### 每个数据集的明确检查入口

| 数据集 | 检查入口 | 当前检查 |
| --- | --- | --- |
| `stock_basic` | `check_stock_basic` | 身份和上市生命周期 |
| `daily` | `check_daily` | 完整性、范围、OHLC、算术和量额关系 |
| `daily_basic` | `check_daily_basic` | 范围、股本顺序和市值恒等式 |
| `adj_factor` | `check_adj_factor` | 正数；序列及跨表检查由 `check_cross_dataset_consistency` 执行 |
| `suspend_d` | `check_suspend_d` | 类型和日内时间段 |
| `stk_limit` | `check_stk_limit` | 完整性和价格顺序 |
| `stock_st` | `check_stock_st` | 状态字段完整性 |
| `moneyflow` | `check_moneyflow` | 分档量额范围 |
| `dividend` | `check_dividend` | 数值、送转合计和实施日期 |
| `forecast` | `check_forecast` | 类型、上下限和公告日期 |
| `express` | `check_express` | 公告字段和同比指标 |
| `fina_audit` | `check_fina_audit` | 日期、费用和审计信息 |
| `income` | `check_income` | 公告字段和利润勾稽 |
| `balancesheet` | `check_balancesheet` | 公告字段和资产负债权益勾稽 |
| `cashflow` | `check_cashflow` | 公告字段和现金流勾稽 |
| `fina_indicator` | `check_fina_indicator` | 公告、报告期和版本标识 |
| `sw_industry` | `check_sw_industry` | 层级、区间和最新标识 |
| `trade_cal` | `check_trade_cal` | 日历值和市场覆盖；序列检查由 `check_trade_calendar_series` 执行 |

跨表检查只在相关数据集同时包含于一次 `detect` 时执行。因此正式发布前建议不传
`--datasets`，一次检查全部数据集；局部修复时再用 `--datasets` 缩小范围。

## 四、人工干预怎么写

只有 `repair` 仍然输出待人工问题时，才需要写决策文件。先查看问题：

```bash
rg '"kind":"issue"' quality/issues/20260822-repaired.jsonl
```

`quality/decisions.jsonl` 每行写一个 JSON 决策。

### 已知正确值：`PATCH`

```json
{"issue_id":"daily:daily_ohlc_v1:...","action":"PATCH","expected":{"close":12.0},"values":{"close":10.5},"reason":"交易所历史行情核对"}
```

- `issue_id`：从报告原样复制；
- `expected`：当前错误值，从 `observed` 复制，用作安全锁；
- `values`：要写入的正确值；
- `reason`：核对依据。

### 确认规则误报：`ACCEPT`

```json
{"issue_id":"daily:daily_ohlc_v1:...","action":"ACCEPT","reason":"交易所与第二数据源一致，确认为特殊行情"}
```

Manifest、Schema、主键、必填值、非有限数、空数据集和分区缺口等硬性问题
不能用 `ACCEPT` 绕过。

写了决策文件时，在发布命令加上：

```text
--decisions quality/decisions.jsonl
```

无法确认时不写决策即可。该数据集会发布为 `UNAVAILABLE`，不会混入回测。

## 五、常见问题

- `detect` 或 `repair` 退出码是 1：检出了 `MANUAL` 问题，报告已正常生成。
- `repair` 提示缺少 token：报告包含自动重拉项，设置 `TUSHARE_TOKEN` 后重试。
- `repair` 或 `publish` 提示 Manifest 指纹不匹配：检测后原数据变了，重新执行 `detect`。
- `publish` 提示报告带 `start`：使用不带 `--start` 的全量报告。
- `publish` 提示版本已存在：发布版本不可覆盖，改用新名称，例如 `20260822-v2`。
- 回测提示 `DataSourceUnavailableError`：查看
  `dataset/tushare_published/current/release.json` 中该数据集的 `open_issue_ids`。

## 六、与 `data_crosscheck` 的边界

`data_crosscheck` 抽样比较 Tushare 和 QMT，用于发现两个来源的观测差异；
`data_cleaning detect` 是对待发布数据的全量、确定性检查。

交叉检查的差异不能直接自动修数据。只有经过语义核对、确认为源数据问题后，
才转成 `data_cleaning` 的人工 Issue。
