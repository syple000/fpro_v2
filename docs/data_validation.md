# Tushare/QMT 数据复核

`data_validation` 从 Tushare 日线股票池中做可重复随机抽样，直接调用 `qmt_receiver.sync_all()`
下载并落地样本股票，然后完成三类比较：

- QMT 不复权日线与 Tushare `daily`；
- QMT 等比前复权（`front_ratio`）日线与
  `daily × adj_factor / 区间末最新 adj_factor`；
- QMT 财务表与 Tushare `income`、`balancesheet`、`cashflow`、`fina_indicator` 的同义字段；
- QMT 除权事件与 Tushare 已实施分红的税前现金分红、送股和转增比例。

QMT 与 Tushare 的股票日线成交量都以手为单位；QMT 用整数手、Tushare 可保留不足一手的
小数，因此允许半手取整误差。QMT 成交额按元转换为 Tushare 的千元。前复权价格允许半分
钱的输出精度误差，更大的差异仍保留，用于识别两家供应商复权因子的口径差异。

财务字段按报告期对齐；QMT `m_anntime` 只表示一个披露日，没有 Tushare
`ann_date/f_ann_date` 两套日期，因此不用于伪造“实际公告日”。同一报告期使用 QMT 最新
披露快照，对比 Tushare `coalesce(f_ann_date, ann_date)` 对应的最新版本。新旧会计报表中
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
uv sync --group data-validation
uv run --group data-validation data-validation \
  --tushare-dir data/tushare \
  --qmt-dir data/qmt \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --sample-size 20 \
  --seed 0
```

命令输出 JSON 报告。`checks[].compared` 是实际比较的字段数，`differences` 给出代码、日期、
字段和两边的值；全部通过时退出码为 0，存在差异时为 1。相同 `seed` 和股票池会得到相同
样本。

QMT 接口和字段以[迅投 XtData 官方文档](https://dict.thinktrader.net/nativeApi/xtdata.html)
为准。不同券商版本新增的字段不会丢失，但只有已建立明确同义关系的字段进入自动数值比较。
