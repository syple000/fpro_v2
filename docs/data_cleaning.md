# `data_cleaning`：离线数据检测、修复和发布

`data_cleaning` 把 Tushare 采集数据变成 Reader 可以直接信任的版本化数据。
它不在查询时跳过异常行，而是在回测前完成下面的闭环：

```text
dataset/tushare
      ↓ detect
稳定的问题报告
      ↓ repair（可选：按数据集和日期定向重拉）
      ↓ publish（自动修正 + 人工决策 + 完整复检）
dataset/tushare_published/current
      ↓
DataCatalog → DataReader
```

## 第一次使用：照着做即可

下面的命令都在项目根目录执行。先安装本模块的依赖：

```bash
uv sync --group data-cleaning
```

先记住两件事：

- `repair` 只是向 Tushare 定向重拉原数据；
- 可以确定正确值的自动修正，在 `publish` 时才写入发布目录。

### 第 1 步：先用一天的数据练习检测

`detect` 只读，不会修改原数据。下面只检查 `daily` 的一天：

```bash
uv run --group data-cleaning data-cleaning detect \
  --input dataset/tushare \
  --datasets daily \
  --start 2024-07-23 \
  --through 2024-07-23 \
  --output quality/issues/practice.jsonl
```

这些参数的意思是：

- `--input`：待检查的采集层目录；
- `--datasets`：只检查指定数据集，可以空格分隔多个名称；
- `--start`：检查区间的开始日；
- `--through`：检查到哪一天，包含该日；
- `--output`：问题报告保存位置，父目录会自动创建。

命令最后会输出一行摘要，例如：

```json
{"datasets":["daily"],"issues":2,"manual":1,"auto_fix":1,"output":"quality/issues/practice.jsonl"}
```

- `issues`：共发现多少个问题；
- `manual`：还需重拉或人工决策的问题数；
- `auto_fix`：已经能算出唯一正确值，发布时会自动修正的问题数。

当 `manual` 大于 0 时，命令退出码是 `1`。这表示“成功检出需处理的问题”，
不是命令崩溃，报告已经正常生成。

查看报告前 10 行：

```bash
sed -n '1,10p' quality/issues/practice.jsonl
```

第一行是本次检测的范围和数据指纹，后续每行是一个问题。问题中最常用的
字段是 `issue_id`、`dataset`、`partition`、`key`、`fix_mode` 和 `suggested`。

### 第 2 步：让程序尝试自动恢复原数据

如果有 `manual` 问题，先运行 `repair`。它会将问题合并成尽可能少的
“数据集 + 日期区间”请求，重拉后自动复检：

```bash
export TUSHARE_TOKEN='你的 token'

uv run --group data-cleaning data-cleaning repair \
  --input dataset/tushare \
  --datasets daily \
  --start 2024-07-23 \
  --through 2024-07-23 \
  --output quality/issues/practice-after-repair.jsonl
```

`repair` 会更新 `dataset/tushare` 中当前生效的采集版本，但不会更新已发布版本。
如果重拉后 `manual` 变成 0，就不需要写人工决策。

### 第 3 步：生成可发布的全量报告

前两步带有 `--start`，只用于定位和修复局部问题，不能直接发布。正式发布前，
要从数据起点检查到指定日期：

```bash
uv run --group data-cleaning data-cleaning detect \
  --input dataset/tushare \
  --through 2026-08-22 \
  --output quality/issues/20260822-full.jsonl
```

这里故意不写 `--start` 和 `--datasets`，表示检查所有已登记数据集的全部有效历史。
如果这次仍有 `manual`，最省事的做法是让 `repair` 处理整份报告范围：

```bash
export TUSHARE_TOKEN='你的 token'

uv run --group data-cleaning data-cleaning repair \
  --input dataset/tushare \
  --through 2026-08-22 \
  --output quality/issues/20260822-full.jsonl
```

它不会把所有历史数据重拉一遍，只会重拉问题建议中的数据集和日期区间。
这份新报告没有 `--start` 限制，可以直接交给 `publish`。如果重拉后仍有
`manual`，按本文的“人工决策”一节处理；不想人工判断时也可以直接发布，
有问题的数据集会是 `UNAVAILABLE`，不会混入回测。

### 第 4 步：发布清洗版本

如果全量报告的 `manual` 是 0，直接发布，不需要决策文件：

```bash
uv run --group data-cleaning data-cleaning publish \
  --input dataset/tushare \
  --issues quality/issues/20260822-full.jsonl \
  --output-root dataset/tushare_published \
  --release 20260822-v1
```

`publish` 会应用 `auto_fix`、完整复检、写入不可变版本，最后将 `current`
原子切换到新版本。发布成功后检查：

```bash
readlink dataset/tushare_published/current
python -m json.tool dataset/tushare_published/current/release.json | sed -n '1,120p'
```

每个数据集应该是 `AVAILABLE`。如果某个数据集是 `UNAVAILABLE`，
`open_issue_ids` 会列出它仍然没有解决的问题。该数据集会被隔离，其他已通过的
数据集仍然可用。

### 第 5 步：回测只读发布版本

```bash
uv run --group backtest backtest-momentum \
  --start 2017-01-01 \
  --end 2026-08-22 \
  --tushare-dir dataset/tushare_published/current \
  --qmt-dir dataset/qmt
```

不要在正式回测中把 `--tushare-dir` 指向 `dataset/tushare`：那是还没有经过发布门禁的
采集层。如果回测依赖的数据集被隔离，Reader 会立即报
`DataSourceUnavailableError`，不会跳过坏行后继续计算。

### 日常最短流程

已经熟悉后，只需记住：

```text
局部 repair → 不带 --start 的全量 detect → publish → 回测读 current
```

## 原则

- `dataset/tushare` 是采集层；清洗不直接修改其 Parquet；
- 只有正确值唯一且可复检时才自动修正；
- 能用原供应商恢复的问题优先定向重拉；
- 不插值价格、不前向填充行情和复权因子、不为财务报表强行“平账”；
- 未解决问题只阻断对应数据集，不阻断无关数据集；
- Reader 不读取问题清单，只读取已复检的 Parquet 和 `release.json`。

## 三个命令（参数参考）

### 1. 检测

检测全部数据集：

```bash
uv run --group data-cleaning data-cleaning detect \
  --input dataset/tushare \
  --through 2026-08-22 \
  --output quality/issues/20260822.jsonl
```

只检查特定数据集和日期区间：

```bash
uv run --group data-cleaning data-cleaning detect \
  --input dataset/tushare \
  --datasets daily stk_limit adj_factor \
  --start 2024-07-01 \
  --through 2024-07-31 \
  --output quality/issues/202407.jsonl
```

`detect` 为只读操作。未发现阻断问题时退出码为 0，存在 `MANUAL` 问题时为 1。

### 2. 自动修复

`repair` 先检测，再从问题中提取可定位的 `REFETCH` 建议，按数据集合并连续
日期，强制重拉后重新检测。默认最多两轮，问题不再变化时提前停止。

```bash
export TUSHARE_TOKEN='你的 token'

uv run --group data-cleaning data-cleaning repair \
  --input dataset/tushare \
  --datasets stk_limit adj_factor \
  --start 2024-07-23 \
  --through 2024-07-23 \
  --output quality/issues/repair-20240723.jsonl
```

也可以直接使用采集层的定向同步能力：

```bash
uv run --group tushare-data tushare-data-test \
  --mode sync_all \
  --datasets daily adj_factor \
  --start-date 20200318 \
  --end-date 20200318 \
  --data-dir dataset/tushare \
  --force
```

`force` 不会先清空旧数据。新数据完整写入后，存储层按业务主键保留新版本。

### 3. 发布

`publish` 必须使用不带 `--start` 的全量报告。发布前会校验报告中的 Manifest
指纹，防止将旧问题清单应用到已更新数据。

```bash
uv run --group data-cleaning data-cleaning publish \
  --input dataset/tushare \
  --issues quality/issues/20260822.jsonl \
  --output-root dataset/tushare_published \
  --release 20260822-v1
```

只有确实写了人工决策文件时，才加上：

```text
--decisions quality/decisions.jsonl
```

发布成功后：

```text
dataset/tushare_published/
├── current -> releases/20260822-v1
└── releases/
    └── 20260822-v1/
        ├── daily/...
        ├── adj_factor/...
        └── release.json
```

未修改分区用硬链复用活跃 Parquet；有自动修正或 `PATCH` 的分区重写。
只有复检通过的数据集会对 Reader 开放。

## 已实现检查

所有数据集执行：

- Manifest 格式和引用文件存在性；
- Parquet Schema 与 Tushare 登记 Schema 一致；
- 分区路径与记录分区日期一致；
- 分区内业务主键不重复；
- 非空 Schema 字段不为空；
- 浮点数不包含 `NaN` 或无穷值；
- 稠密交易数据集不缺已知交易日分区。

交易关键数据额外执行：

- `daily`：关键字段完整、价格为正、成交量额非负、OHLC 关系正确；
- `daily`：`pre_close + change` 与 `pct_chg` 独立指向同一收盘价时，
  可自动修正不一致的 `close`；
- `adj_factor`：因子为正数；
- `stk_limit`：关键价格完整且 `down_limit <= pre_close <= up_limit`；
- `trade_cal`：交易所和开市标记合法，每日覆盖 SSE、SZSE 和 BSE。

关键行情、复权和涨跌停字段中的非有限数会建议重拉；其他可空数值字段
中的非有限数在发布时确定性归一为 `null`。

## 人工决策

只有定向重拉之后仍然有 `MANUAL` 问题，才需要这一步。先从最新的全量报告
中找到问题行：

```bash
rg '"kind":"issue"' quality/issues/20260822-full.jsonl
```

每个问题有三种处理方式。

### 方式 1：已从可信来源确认正确值

用编辑器打开 `quality/decisions.jsonl`，每个决策写成单独一行 JSON：

```json
{"issue_id":"daily:daily_ohlc_v1:...","action":"PATCH","expected":{"close":12.0},"values":{"close":10.5},"reason":"交易所历史行情核对"}
```

- `issue_id`：从问题行原样复制；
- `expected`：当前错误值，从问题的 `observed` 中复制；
- `values`：经人工核对后要写入的正确值；
- `reason`：记录正确值的核对来源。

`expected` 是安全锁。如果采集层已经发生变化，它与当前值不同，补丁就会拒绝应用，
避免把旧决策误用到新数据上。

### 方式 2：确认这是规则误报，原值正确

```json
{"issue_id":"daily:daily_ohlc_v1:...","action":"ACCEPT","reason":"交易所数据与第二数据源一致，确认为特殊行情"}
```

`ACCEPT` 只适用于可以解释的业务规则。Manifest、Schema、重复主键、必填值、
非有限数、空数据集和分区缺口等硬性问题不能用 `ACCEPT` 绕过。

### 方式 3：暂时无法确认

不写决策即可，发布时该数据集会保持 `UNAVAILABLE`。如果需要显式留下“待重拉”
的审计记录，可以写：

```json
{"issue_id":"daily:daily_ohlc_v1:...","action":"REFETCH","reason":"暂无可信数据源，不发布该数据集"}
```

写完决策后，在发布命令中增加：

```text
--decisions quality/decisions.jsonl
```

## 常见问题

- `detect` 或 `repair` 退出码是 1：查看摘要中的 `manual`。大于 0 时是正常的检测结果。
- `repair` 提示没有 token：先执行 `export TUSHARE_TOKEN='你的 token'`。
- `publish` 提示报告带有 `start`：重新执行不带 `--start` 的全量 `detect` 或 `repair`。
- `publish` 提示 Manifest 指纹不匹配：原数据在报告生成后被更新了，重新生成全量报告。
- `publish` 提示版本已存在：发布版本不可覆盖，将 `--release` 改为新名称，例如 `20260822-v2`。
- 回测提示 `DataSourceUnavailableError`：查看 `release.json` 中该数据集的 `open_issue_ids`，
  修复后以新的 `--release` 重新发布。

## 发布门禁

`release.json` 对每个数据集记录 `AVAILABLE` / `UNAVAILABLE`、行数和未解决
`issue_id`。`DataCatalog` 初始化时只读取一次该状态：

- 请求依赖的数据集可用：正常扫描 Parquet；
- 任一实际依赖不可用：立即抛出 `DataSourceUnavailableError`；
- `adj_factor` 不可用不影响未复权 `daily`，但会阻断前复权查询。

当前隔离粒度是数据集级，没有静默删行或部分结果。

## 与 `data_crosscheck` 的边界

`data_crosscheck` 抽样比较 Tushare 和 QMT，只证明两个来源的观测是否一致。
交叉检查差异经语义核对并确认为源数据问题后，再转换为本模块的人工 Issue。

`data_cleaning detect` 是全量、确定性的发布门禁；`data_crosscheck` 是抽样、跨源的
补充证据，二者不相互替代。
