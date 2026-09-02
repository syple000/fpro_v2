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
- 中间的 `kind=check`：每个检查项的 `PASS` / `FAIL` 和问题数；
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

### 需要交易日连续性的数据集

`daily`、`daily_basic`、`adj_factor`、`stk_limit`、`stock_st` 和 `moneyflow`
额外执行：

| 检查 ID | 检查内容 | 修复方式 |
| --- | --- | --- |
| `missing_market_partition_v1` | 不缺 `trade_cal` 中已知开市日的分区 | 自动重拉缺失日期 |

### 有专有业务检查的数据集

| 数据集 | 检查 ID | 检查内容 | 修复方式 |
| --- | --- | --- | --- |
| `daily` | `daily_missing_v1` | `open/high/low/close/pre_close/vol/amount` 完整 | 自动重拉该交易日 |
| `daily` | `daily_range_v1` | 价格为正，成交量和成交额非负 | 自动重拉该交易日 |
| `daily` | `daily_ohlc_v1` | `high ≥ open/low/close`，`low ≤ open/high/close` | 自动重拉该交易日 |
| `daily` | `daily_close_consistency_v1` | `change` 和 `pct_chg` 独立推导的收盘价一致 | 唯一结果生成自动补丁 |
| `adj_factor` | `adj_factor_positive_v1` | 复权因子大于 0 | 自动重拉该交易日 |
| `stk_limit` | `stk_limit_partition_missing_v1` | 整个分区的昨收、涨停、跌停不能整列为空 | 自动重拉该交易日 |
| `stk_limit` | `stk_limit_missing_v1` | 每行昨收、涨停、跌停完整 | 自动重拉该交易日 |
| `stk_limit` | `stk_limit_order_v1` | `down_limit ≤ pre_close ≤ up_limit` | 自动重拉该交易日 |
| `trade_cal` | `trade_calendar_value_v1` | 交易所是 SSE/SZSE/BSE，`is_open` 是 0/1 | 自动重拉该日期 |
| `trade_cal` | `calendar_exchange_coverage_v1` | 每个日期都覆盖 SSE、SZSE 和 BSE | 自动重拉该日期 |

### 每个数据集的明确检查入口

| 数据集 | 检查入口 | 当前检查 |
| --- | --- | --- |
| `stock_basic` | `check_stock_basic` | 通用检查 |
| `daily` | `check_daily` | 通用、交易日缺口、daily 四项业务检查 |
| `daily_basic` | `check_daily_basic` | 通用、交易日缺口 |
| `adj_factor` | `check_adj_factor` | 通用、交易日缺口、因子为正 |
| `suspend_d` | `check_suspend_d` | 通用检查 |
| `stk_limit` | `check_stk_limit` | 通用、交易日缺口、涨跌停三项业务检查 |
| `stock_st` | `check_stock_st` | 通用、交易日缺口 |
| `moneyflow` | `check_moneyflow` | 通用、交易日缺口 |
| `dividend` | `check_dividend` | 通用检查 |
| `forecast` | `check_forecast` | 通用检查 |
| `express` | `check_express` | 通用检查 |
| `fina_audit` | `check_fina_audit` | 通用检查 |
| `income` | `check_income` | 通用检查 |
| `balancesheet` | `check_balancesheet` | 通用检查 |
| `cashflow` | `check_cashflow` | 通用检查 |
| `fina_indicator` | `check_fina_indicator` | 通用检查 |
| `sw_industry` | `check_sw_industry` | 通用检查 |
| `trade_cal` | `check_trade_cal` | 通用、日历值和三市覆盖 |

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
