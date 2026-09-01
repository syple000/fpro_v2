# `data_cleaning`：离线数据清洗与发布

`data_cleaning` 负责把已采集的原始数据检出、修正并发布为 Reader 可以直接信任的数据。
第一阶段只处理 Tushare 离线数据，首个发布范围截至 `2026-08-22`；实时行情质量控制不在本阶段
范围内。

模块名称使用 `data_cleaning`，代码、测试和文档分别放在：

```text
src/data_cleaning/
tests/data_cleaning/
docs/data_cleaning.md
```

## 目标与边界

数据清洗遵守以下规则：

1. `dataset/tushare` 是原始数据，只读且永不原地修正；
2. 检出与修正分开运行，检测命令不得修改任何数据；
3. 修正只分为确定性的自动修正和需要人工决策的修正；
4. 未解决的阻断问题只使对应数据类型不可用，不阻塞无关数据类型；
5. 修正结果物化为普通 Parquet，Reader 不在查询时执行清洗或关联补丁；
6. 每个发布版本都记录输入、规则、人工决策、截止日期和校验结果；
7. 相同输入、规则和决策必须生成相同结果，并且可以切回旧发布版本。

`data_cleaning` 不负责采集 Tushare/QMT 数据，不改变 `DataReader` 的 PIT 语义，也不在财务口径
不明确时主动“平账”。

## 总体流程

```text
dataset/tushare
       │
       ▼
data_cleaning detect
       │
       ├── AUTO_FIX：存在唯一、可验证的修正结果
       └── MANUAL：必须重新采集、人工补丁或人工确认
       │
       ▼
quality/decisions.jsonl
       │
       ▼
data_cleaning publish
       │
       ├── 构建候选数据
       ├── 应用修正
       ├── 完整复检
       └── 按数据类型判定可用性
       │
       ▼
dataset/tushare_published/current
       │
       ▼
DataCatalog → DataReader
```

公开命令只保留 `detect` 和 `publish`。`publish` 内部完成构建、修正、复检和原子发布，不再暴露
额外的中间命令。

## 目录规划

代码使用直观的职责拆分，不建设通用规则引擎或任务 DAG：

```text
src/data_cleaning/
├── __init__.py
├── __main__.py          # detect、publish 命令
├── models.py            # Issue、Decision、DatasetRelease
├── detector.py          # 执行检测规则并生成问题清单
├── cleaner.py           # 应用自动修正和人工决策
├── publisher.py         # 物化、复检和原子发布
└── rules/
    ├── __init__.py
    ├── common.py        # Schema、主键、日期、有限数等通用规则
    ├── market.py        # 行情、交易约束和复权规则
    ├── reference.py     # 日历、证券主数据和行业规则
    └── fundamentals.py  # 财报、指标和公司行动规则

tests/data_cleaning/
├── unit/
└── integration/
```

运行产生的数据与人工决策分开存放：

```text
quality/
├── issues/
│   └── 20260822.jsonl   # detect 自动生成，不人工编辑
└── decisions.jsonl      # 人工决策，纳入版本控制

dataset/tushare_published/
├── current -> releases/20260822-v1
└── releases/
    └── 20260822-v1/
        ├── daily/...
        ├── daily_basic/...
        ├── adj_factor/...
        ├── ...
        └── release.json
```

`current` 只指向已经完成复检的发布版本。未修改的不可变 Parquet 文件可以使用硬链接复用，发生
修正或包含截止日之后数据的分区必须重新写入。

## 核心模型

### Issue

每条检测规则输出统一的问题记录：

```python
@dataclass(frozen=True)
class Issue:
    issue_id: str
    dataset: str
    partition: str | None
    key: dict[str, object]
    rule_id: str
    fix_mode: Literal["AUTO_FIX", "MANUAL"]
    observed: dict[str, object]
    suggested: dict[str, object] | None
    message: str
```

`issue_id` 由数据类型、业务主键、规则编号和观测值确定性生成。原始值变化后，旧决策不会误用
到新的问题上。

### Decision

人工决策只允许三种动作：

```python
@dataclass(frozen=True)
class Decision:
    issue_id: str
    action: Literal["PATCH", "REFETCH", "ACCEPT"]
    expected: dict[str, object] | None
    values: dict[str, object] | None
    reason: str
```

- `PATCH`：明确提供修正值；
- `REFETCH`：要求重新采集，重新采集完成前问题仍未解决；
- `ACCEPT`：人工确认观测值符合数据定义，不需要修改。

`PATCH` 必须包含 `expected` 原值。实际原值与其不一致时拒绝应用，避免旧补丁覆盖已经更新的
供应商数据。

### DatasetRelease

第一阶段只按数据类型控制可用性：

```python
@dataclass(frozen=True)
class DatasetRelease:
    dataset: str
    status: Literal["AVAILABLE", "UNAVAILABLE"]
    row_count: int
    open_issue_ids: tuple[str, ...]
```

不增加行级质量状态，也不让 Reader 在每次查询时判断分区质量。需要更细粒度隔离时再扩展，
不提前增加复杂度。

## 检出

检测命令示例：

```bash
uv run python -m data_cleaning detect \
  --input dataset/tushare \
  --through 2026-08-22 \
  --output quality/issues/20260822.jsonl
```

`detect` 必须：

- 仅读取 Manifest 当前引用的 Parquet 文件；
- 逐个数据类型运行已启用规则；
- 输出 `AUTO_FIX` 和 `MANUAL` 问题；
- 按数据类型、分区、主键和规则稳定排序；
- 对相同输入生成相同的 `issue_id` 和输出内容；
- 不创建发布数据，不修改原始 Manifest。

第一版不引入 `WARNING` 等更多状态。尚不能明确判断对错的现象不启用为发布规则，保留在规则
研究清单中，待字段语义确认后再加入。

## 修正与发布

发布命令示例：

```bash
uv run python -m data_cleaning publish \
  --input dataset/tushare \
  --through 2026-08-22 \
  --issues quality/issues/20260822.jsonl \
  --decisions quality/decisions.jsonl \
  --release 20260822-v1
```

`publish` 按以下固定顺序运行：

1. 校验问题清单与当前原始 Manifest 是否匹配；
2. 在临时目录构建候选发布版本；
3. 对未修改文件复用不可变 Parquet，对有变化的分区重新写入；
4. 应用所有 `AUTO_FIX`；
5. 应用匹配当前观测值的人工 `PATCH` 或 `ACCEPT`；
6. 将 `REFETCH` 和没有决策的 `MANUAL` 问题保留为未解决；
7. 过滤各数据类型业务日期晚于 `2026-08-22` 的记录；
8. 对候选数据重新运行完整检测，而不是只相信修正函数；
9. 生成 `release.json`，逐个数据类型记录可用性；
10. 原子提交发布目录并更新 `current`。

自动修正生成了新问题，或者人工补丁与 `expected` 不匹配时，对应数据类型必须标记为
`UNAVAILABLE`。

## 发布状态与 Reader

`release.json` 是发布结果的唯一状态入口：

```json
{
  "release_id": "20260822-v1",
  "validated_through": "2026-08-22",
  "ruleset_version": 1,
  "datasets": {
    "daily": {
      "status": "AVAILABLE",
      "row_count": 0,
      "open_issue_ids": []
    },
    "adj_factor": {
      "status": "UNAVAILABLE",
      "row_count": 0,
      "open_issue_ids": [
        "adj_factor:920627.BJ:isolated_jump_v1"
      ]
    }
  }
}
```

可用性规则只有一条：

```text
没有未解决的 MANUAL 问题且复检通过 → AVAILABLE
否则                                  → UNAVAILABLE
```

`DataCatalog` 初始化时读取一次发布状态。可用数据仍直接注册为普通 Parquet 视图；访问不可用
数据时立即抛出 `DataSourceUnavailableError`。该检查只需要一次常量集合查询，不增加 Parquet
扫描、SQL JOIN、UDF 或逐行处理。

不同能力按其实际依赖决定是否可用。例如：

- 未复权日线只依赖 `daily`；
- 前复权日线同时依赖 `daily` 和 `adj_factor`；
- 涨跌停状态依赖 `stk_limit`；
- 综合交易状态还依赖 `stock_st` 和 `suspend_d`。

因此 `adj_factor` 不可用时仍可读取未复权日线，但前复权查询必须失败。

## 规则分类

### 通用规则

所有数据类型都执行：

- Arrow Schema 与登记 Schema 一致；
- 业务主键非空且不重复；
- Manifest 可解析且只引用存在的 Parquet 文件；
- 日期字段合法，发布范围字段不晚于截止日；
- 浮点字段不存在 `NaN` 或无穷值；
- 已确认定义的枚举值位于允许集合；
- 分区值与记录中的分区字段一致。

### 自动修正规则

只有同时满足以下条件的规则才允许标记为 `AUTO_FIX`：

- 正确结果唯一；
- 规则是确定性的；
- 不依赖主观阈值猜测；
- 可以检查修正前的预期值；
- 修正后能由独立规则重新验证。

允许自动处理的典型情况包括：

- 确定的类型、日期、时区和单位标准化；
- 存在明确版本排序时的完全重复记录去重；
- 多个独立字段共同确定唯一正确值；
- 供应商重新采集后原始数据自然恢复。

禁止自动插值价格或复权因子、前向填充缺失交易数据、裁剪异常价格、用上一条记录覆盖当前
异常、对多个供应商取平均，以及为了满足恒等式而修改财务报表。

## 数据类型实施顺序

### 第一批：交易关键数据

优先覆盖：

- `daily`；
- `adj_factor`；
- `stk_limit`；
- `stock_st`；
- `suspend_d`；
- `trade_cal`。

截至 `2026-08-22` 已发现问题的初始处置原则：

| 问题 | 初始处置 |
| --- | --- |
| `603005.SH` 在 `2020-03-18` 的日线收盘价不一致 | 仅在多个独立字段唯一指向同一结果且修正后全部价格规则通过时 `AUTO_FIX` |
| `stk_limit` 在 `2024-07-23` 的关键字段整分区为空 | `MANUAL/REFETCH`，禁止根据比例自行生成涨跌停价 |
| `stock_st` 缺少完整交易日 | `MANUAL/REFETCH`，禁止默认前向填充 |
| `adj_factor` 存在单日跳变后立即恢复 | `MANUAL`，核对公司行动或重新采集 |
| 缺少 BSE 交易日历 | `MANUAL`，明确数据来源，禁止把 SSE 记录伪装为 Tushare BSE 原始数据 |

### 第二批：研究指标和分类

覆盖：

- `daily_basic`；
- `moneyflow`；
- `stock_basic`；
- `sw_industry`。

`free_share > float_share`、moneyflow 与日线成交额差异、多个行业记录同时有效等现象，在字段
定义和历史版本语义确认之前不自动修正。

### 第三批：财务与公司行动

覆盖：

- `income`；
- `balancesheet`；
- `cashflow`；
- `fina_indicator`；
- `forecast`；
- `express`；
- `fina_audit`；
- `dividend`。

第一版只对明确的结构、主键、日期、枚举和版本错误启用阻断规则。财务恒等式差异和供应商
口径差异只用于人工核验，不自动修改金额。

## 实施阶段

### 阶段一：框架和通用规则

- 建立 `src/data_cleaning` 包和两个公开命令；
- 实现三种核心模型与稳定 JSONL 编解码；
- 接入 Manifest、Schema、主键、日期和有限数检查；
- 验证检测过程只读且结果确定。

### 阶段二：交易关键数据

- 实现六个交易关键数据类型的规则；
- 完成自动修正和 `decisions.jsonl` 应用；
- 实现候选目录、分区重写、截止日过滤和完整复检；
- 生成第一个 `20260822-v1` 候选版本。

### 阶段三：发布与读取集成

- 实现 `release.json` 和按数据类型的可用状态；
- 原子更新 `current`；
- 让 `DataCatalog` 读取发布状态；
- 验证不可用依赖明确失败、可用数据仍直接扫描 Parquet。

### 阶段四：扩展其余数据类型

- 加入研究指标、分类、财务和公司行动规则；
- 对尚不明确的规则先补充数据定义和测试样本；
- 只有经过确认的规则才能进入发布门禁。

## 验收标准

首个发布版本必须满足：

- `dataset/tushare` 的文件内容和 Manifest 完全未改变；
- 相同输入、规则版本和人工决策可以重复生成相同结果；
- 所有自动修正问题在候选数据复检中消失；
- `AVAILABLE` 数据类型不存在未解决的人工问题；
- 输出 Schema 与 Tushare 登记 Schema 一致，主键不重复；
- 发布数据不包含 `2026-08-22` 之后的记录；
- `release.json` 可以解释每个数据类型为何可用或不可用；
- Reader 不读取问题清单或人工决策文件；
- 可用数据查询不增加 SQL JOIN、UDF 或逐行清洗；
- 切换 `current` 到旧发布目录即可回滚。

第一阶段完成的标志是生成一个截至 `2026-08-22` 的可审计发布版本，并让现有 PIT Reader 对
可用数据类型直接读取该版本、对不可用依赖明确失败。
