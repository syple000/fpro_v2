# DuckDB 研究读取层

`data` 是独立于 `tushare_data` 和 `qmt_receiver` 的只读包。它不复制 Parquet，
只把每个分区 `_manifest.json` 当前引用的文件注册到 DuckDB。

## 使用

```bash
uv sync --group data
```

```python
from data import DataCatalog

with DataCatalog(
    tushare_root="data/tushare",
    qmt_root="data/qmt",
) as catalog:
    con = catalog.connection
    cashflow = con.execute("""
        SELECT *
        FROM tushare.cashflow_as_of(DATE '2024-04-30')
        WHERE end_date = DATE '2023-12-31'
    """).fetch_arrow_table()
```

`tushare.<table>` 和 `qmt.<table>` 是原始视图。每个业务同时提供
`<table>_as_of(...)` 表宏：

- Tushare 宏接受 `DATE`，按日末 EOD 语义解释。盘前研究应传上一个交易日。
- 三张财务报表只按实际公告日 `f_ann_date` 选择当时最新版本；不回退到
  `ann_date`。
- 预告、快报、财务指标和审计意见按 `ann_date` 选择当时最新版本。
- 分红只按实施公告日 `imp_ann_date` 返回已经实施公告的记录；没有实施公告日的
  预案和决案记录不会由该宏提前暴露。
- 申万行业返回当日有效成员，并屏蔽未来 `out_date` 与当前快照 `is_new`。
- QMT 宏接受 UTC Unix Epoch 微秒整数，使用 `received_at <= as_of_us`。

```sql
SELECT * FROM tushare.daily_as_of(DATE '2024-04-30');
SELECT * FROM tushare.dividend_as_of(DATE '2024-04-30');
SELECT * FROM tushare.sw_industry_as_of(DATE '2024-04-30');
SELECT * FROM qmt.ticks_as_of(1714464000000000);
```

QMT 还公开用于跨源复核的 `qmt.daily`、`qmt.financial` 和
`qmt.dividend_factors`。这些历史表按自身业务日期查询，不使用实时行情的 `received_at`
宏。

同步任务写入或整理文件后，对已存在的 `DataCatalog` 调用 `refresh()`。
新建对象会自动执行一次刷新。目录中未被 Manifest 引用的 Parquet 文件不会对外可见。

可以直接运行内置的 `test_main` 查看指定股票的财务 PIT 和 QMT Tick：

```bash
uv run --group data data-test \
  --tushare-dir data/tushare \
  --qmt-dir data/qmt \
  --as-of 2024-04-30 \
  --ts-code 000001.SZ \
  --limit 10
```

也可以使用 `python -m data.test_main` 传入同样的参数。
