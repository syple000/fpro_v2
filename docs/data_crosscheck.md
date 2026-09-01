# Tushare/QMT 数据交叉检查

`data_crosscheck` 的职责是比较两个独立数据源对同一业务事实的观测，发现缺失、
数值和口径差异。它是数据质量体系中的“跨源交叉检查”，不是数据清洗器，也不是
发布门禁。

跨源差异只表示 Tushare 与 QMT 不一致，不表示任一方已被判定为错误。本模块不会
用 QMT 覆盖 Tushare，不会生成自动补丁，也不直接将数据集标记为可用或不可用。差异
需要结合字段定义、交易所或其他证据人工定性；确认为源数据问题后，再进入
`data_cleaning` 的 `MANUAL` Issue 和 `PATCH` / `REFETCH` / `ACCEPT` 流程。

## 当前实现

当前实现从 Tushare 日线股票池中做可重复随机抽样，直接调用
`qmt_receiver.sync_all(..., force=True)` 强制下载并落地样本股票，然后完成四类比较：

- QMT 不复权日线与 Tushare `daily`；
- 本次实拉的 QMT 原生 `front_ratio` 与“QMT 不复权日线 ÷ 后续事件 `dr` 连乘”；
- QMT 财务表与 Tushare `income`、`balancesheet`、`cashflow`、`fina_indicator` 的同义字段；
- QMT 除权事件与 Tushare 已实施分红的税前现金分红、送股和转增比例。

原生 `front_ratio` 只保存在本次校验响应中，不写入生产 Parquet。复核会另外拉取样本股票的
完整 QMT 除权事件序列：对每根 K 线，将其 OHLC 和前收盘价除以所有 `ex_date` 晚于 K 线日期的
`dr` 连乘，再与原生值比较；成交量和成交额必须原样相等。生产 Reader 则把因子锚定在查询
`as_of`，并要求 meta 中的因子同步区间完整覆盖 K 线到锚点。

2026-08-30 的实机验证覆盖 21 只股票、4,839 根日线，其中 2,140 根跨越至少一次除权事件；
五个价格字段的最大绝对误差为 `7.11e-15`，4,839 根 K 线的成交量和成交额全部保持一致。分钟线
使用相同的日期边界和计算公式；同步数据中不保存原生复权分钟线。

QMT 与 Tushare 的股票日线成交量都以手为单位；QMT 用整数手、Tushare 可保留不足一手的
小数，因此允许半手取整误差。QMT 成交额按元转换为 Tushare 的千元。前复权价格允许半分
钱的输出精度误差，更大的差异仍保留，用于识别两家供应商复权因子的口径差异。

财务字段按报告期对齐；QMT `m_anntime` 只表示一个披露日，没有 Tushare
`ann_date/f_ann_date` 两套日期，因此不用于伪造“实际公告日”。同一报告期使用 QMT 最新
披露快照，对比 Tushare `f_ann_date` 对应的最新版本。新旧会计报表中
固定资产、在建工程、其他应收和其他应付字段会使用明确的等价候选字段；`NULL` 与数值零
视为相同，EPS 允许 QMT 两位小数带来的半个最小单位误差。供应商语义不同或 Tushare 代理
没有对应字段的项目不进入自动比较。

QMT 除权接口只返回事件时间戳和因子，没有预案公告日、实施公告日；存储层按中国市场时区
从时间戳派生除权日。复核时先按 Tushare
`imp_ann_date` 选择每个已实施方案的最新版本，再汇总同一股票、同一除权日的多个方案，和
QMT 的单日汇总事件比较。`allotNum/allotPrice/gugai/dr` 暂无可直接等价的 Tushare 分红字段，
只落原值，不做牵强映射。

miniQMT 登录并启动 qmt-agent 后运行：

```bash
uv sync --group data-crosscheck
uv run --group data-crosscheck data-crosscheck \
  --tushare-dir dataset/tushare \
  --qmt-dir dataset/qmt \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --sample-size 20 \
  --seed 0
```

命令输出 JSON 报告。`checks[].compared` 是实际比较的字段数，`differences` 给出代码、日期、
字段和两边的值；未发现差异时退出码为 0，存在差异时为 1。`passed=true`
只表示本次抽样的已比较字段没有差异，不表示已完成全市场数据质量认证。相同 `seed`
和股票池会得到相同样本；QMT 数据源本身更新后，比较结果仍可能变化。

## 边界和限制

- 这是抽样验证，不代替 Schema、主键、分区、日期、有限数和全量完整性检查；
- 抽样母集来自 Tushare `daily`，因此无法单独证明 Tushare 股票池本身完整；
- 当前命令会强制拉取并写入 QMT 样本数据，不是纯只读操作；
- 两个供应商的相同值也不构成业务真值证明，关键差异仍需要第三方证据或人工核对；
- 交叉检查报告是人工复核证据，不直接作为 Reader 运行时输入。

QMT 接口和字段以[迅投 XtData 官方文档](https://dict.thinktrader.net/nativeApi/xtdata.html)
为准。不同券商版本新增的字段不会丢失，但只有已建立明确同义关系的字段进入自动数值比较。
