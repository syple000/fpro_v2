# `data_cleaning`：原位检测与可回滚修复

数据只有一份，始终位于 `dataset/tushare`。检测不复制数据；修复也只改有问题的日期分区。

```text
dataset/tushare
├── daily/...                 # MarketData 实际读取的数据
├── adj_factor/...
└── _quality/
    ├── status.json           # 最近一次全量检测的质量状态
    ├── detections/*.jsonl    # 每次检测和复检的完整记录
    └── repairs/<repair-id>/
        ├── journal.json      # 改了什么、当前是否已提交
        └── backup/...        # 修改前的分区，可用于回滚
```

不再有 `publish`、`releases` 或 `current`。`MarketData` 直接读取
`dataset/tushare` 中各分区 Manifest 当前指向的 Parquet。

## 最短使用流程

### 1. 检测

```bash
uv run --group data-cleaning data-cleaning detect \
  --input dataset/tushare \
  --through 2026-08-22
```

这条命令会检查全部数据集和全部历史。报告自动写入：

```text
dataset/tushare/_quality/detections/<时间>-<数据指纹>.jsonl
```

不需要自己指定输出文件。终端最后会打印实际报告路径。
终端只按数据集汇总行数、检查数、错误数和告警数；每条检查的完整结果都在 JSONL 中。

只排查某个范围时可以加：

```bash
--datasets daily adj_factor --start 2024-07-01 --through 2024-07-31
```

局部检测也会留下日志，但不会覆盖代表整库状态的 `_quality/status.json`。

### 2. 修复

把上一步打印的报告路径传给修复命令：

```bash
export TUSHARE_TOKEN='你的 token'

uv run --group data-cleaning data-cleaning repair \
  --input dataset/tushare \
  --issues dataset/tushare/_quality/detections/<报告>.jsonl
```

修复的固定顺序是：

1. 核对报告中的 Manifest 指纹，防止拿旧报告改新数据；
2. 创建 `repairs/<repair-id>/journal.json`；
3. 只备份即将修改的分区，Parquet 优先使用硬链接，不复制全库；
4. 对可恢复错误定向重拉，对确定性修正原位打补丁；
5. 使用完全相同的范围重新检测并记录报告；
6. 把事务标记为 `COMMITTED`。

任何一步抛出异常，程序都会自动恢复备份，并把事务记为 `ROLLED_BACK`。

只有需要访问上游重拉时才需要 `TUSHARE_TOKEN`。默认最多重拉两轮；两轮后仍存在的
ERROR 会保留给人工处理，不会被悄悄忽略。

### 3. 回滚

修复结束会打印 `repair-id`。需要撤销时：

```bash
uv run --group data-cleaning data-cleaning rollback \
  --input dataset/tushare \
  --repair-id <repair-id>
```

回滚前会再次核对修复后的指纹。如果该数据后来又被同步或修复过，命令会拒绝覆盖后续
修改；连续撤销多次修复时要按最新到最旧的顺序执行。回滚成功后重新执行一次全量
`detect` 即可更新质量状态。

## 到底会怎样修改数据

修复不会重新写全部 Parquet。

- 普通行情错误：只备份并重拉对应的数据集和日期；
- Manifest、文件、Schema 损坏：日期能从分区路径识别时，先备份坏分区，再删除该分区
  并重拉；日期无法识别时才要求人工处理；
- 可空、非关键浮点字段中的 `NaN/Infinity`：原位改成 `null`；
- 同一公司、同一报告期已有正常正式版本时，删除公告日期早于报告期且标记为旧版的
  `fina_indicator` 记录；没有正式版本作为证据时绝不自动删除；
- `open/high/low/close/pre_close/vol/amount` 等核心行情值：绝不根据其他字段猜值，
  只允许重拉或人工核对后补丁；
- 没有问题的分区：完全不读写其 Parquet，也不做发布副本。

一次修改前后的证据都在同一事务目录中。`journal.json` 的常见状态是：

- `PREPARED`：备份和修改尚未完成；
- `COMMITTED`：修复和复检已完成，可以显式回滚；
- `ROLLED_BACK`：已恢复到修复前。

`journal.json` 的 `operations` 还会记录每次重拉的表、日期和返回行数，以及每个补丁的
问题 ID、定位键、修改前观察值、修改后值和理由。完整旧分区仍以备份为准，可直接回滚。

## 人工补丁

只有已经从交易所公告、另一权威源或人工复核中得到唯一正确值时，才写人工补丁。
人工文件每行一个 JSON，只允许 `PATCH`：

```json
{"issue_id":"报告中的问题 ID","action":"PATCH","expected":{"close":12.0},"values":{"close":10.5,"change":0.5,"pct_chg":5.0},"reason":"已核对交易所历史行情"}
```

然后执行：

```bash
data-cleaning repair --issues <报告> --decisions <补丁.jsonl>
```

`expected` 是原值保护：当前数据只要与它不同，补丁就拒绝执行并自动回滚。系统不提供
“接受 ERROR”开关；确定性错误必须修好，或者保持不可用。

## 检查结果怎么理解

- `PASS`：该项未发现问题；
- `WARN`：可能是合法市场事件或数据口径差异，保留记录但不阻止读取；
- `FAIL`：确定性错误，修复前不能认为数据通过。

每个数据集都会执行这些基础检查：

- Manifest 格式、引用文件存在、Parquet 可读；
- Schema 与代码登记一致；
- 分区日期与行内日期一致；
- 主键不重复、必填字段不为空；
- 浮点值不是 `NaN` 或无穷。

业务检查只保留能回答“数据是否可用于回测”的规则：

- 日线价格范围、OHLC 关系、涨跌额/涨跌幅算术；
- 交易日历自然日和 SSE/SZSE 必需覆盖（BSE 记录存在时也校验）；
- 开市日分区覆盖；
- 复权因子为正、每条日线都有同日因子；
- 涨跌停价格顺序；
- 财务公告日期、报告期和版本字段；
- 其他数据集的必要字段取值和日期顺序。

经验性关系（财务简化勾稽、复权因子升降、停牌与日线冲突、历史行业映射等）不作为
数据损坏检测。它们依赖口径或市场事件，不能作为自动改写源数据的依据。

证券在本地历史中的第一条日线允许没有 `pre_close`：此前没有可用价格就无法连接前一日
收益，回测会跳过这个端点，不猜值也不制造告警。

## `MarketData` 如何生效

修复通过 ParquetStore 原子替换目标分区的 Manifest。`DataCatalog` 初始化或调用
`refresh()` 时，只读取每个 Manifest 当前列出的文件，因此无需发布或切换路径，修复后
自然生效。

最近一次“全部数据集、全部历史”的检测会写 `_quality/status.json`：

- 有 ERROR 的数据集，`require_available()` 会拒绝使用；
- 只有 WARNING 的数据集仍可读；
- 如果检测后 Manifest 又变化，状态指纹会过期，所有 Tushare 数据集都会先被判为
  不可用，直到重新全量检测。

回测保持默认路径即可：

```bash
uv run --group backtest backtest-momentum \
  --start 2017-01-01 \
  --end 2026-08-22 \
  --tushare-dir dataset/tushare \
  --qmt-dir dataset/qmt
```

这样只有一个事实来源：采集、检测、修复和回测看到的是同一份 Manifest 数据。
