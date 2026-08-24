# tushare_data

`tushare_data` 通过 quicksync/Tushare 按全市场粒度拉取数据，再写入
`parquet_store`。存储层只保留 Tushare 接口字段；PIT 可见性由独立的
[`data`](data.md) DuckDB 读取层处理。

本模块不落盘 `partition_date`、`visible_at`、`observed_at` 或行级采集时间。
归一化只负责固定字段校验、空值清理和 Arrow 类型转换。

## 拉取与完成区间

`sync_all(pro, store, start_date, end_date, force=False)` 按 API 的
`start_date/end_date` 滑动拉取，
并在每个块完整落盘后才提交 `<root>/_meta/sync_all/*.json` 完成区间。
默认只补未完成区间；`force=True` 会在本次调用中忽略已有完成区间，重新抓取整个请求区间，
但不会预先清空 checkpoint。这样强制重抓失败时，原有完成状态仍然有效；成功块照常写盘并提交。
`sync_inc` 重新拉取固定回看窗口，不改变 `sync_all` 断点。

API 游标和存储分区是两个独立概念。同步层不会根据请求区间过滤返回行：
例如按 `ann_date=2024-04-18` 拉到 `f_ann_date=2025-04-29` 的修订版时，
该行依然会写入它的 `end_date` 分区。

所有分页请求都按实际返回行数推进 `offset`。`fina_audit` 是唯一个需要先
拉股票列表再逐股请求的业务：每只股票使用完整待同步日期区间，每完成 30 只
股票就写盘一次；其余表优先使用全市场接口。

## 并发和限速

并发只放在数据表这一层：`sync_all` 和 `sync_inc` 先同步交易日历，再用线程池
同时运行其余数据表的同步函数。每个数据表内部仍是直观的顺序请求和分页。

所有数据表共用同一个 `TushareProClient`，真实 HTTP 请求统一由它的
`RequestLimiter` 控制。默认配置是每分钟 120 次、最多 3 个在途请求，可通过
命令行的 `--requests-per-minute` 和 `--max-concurrency` 按 quicksync 套餐调整。
线程池只负责让多个数据表同时推进，不重复实现请求限速。

除 `fina_audit` 按 30 只股票分批写盘外，每个数据块会在本块全部请求成功和
校验完成后才写盘。`fina_audit` 的 `sync_all` 日期 checkpoint 仍在完整待同步
日期区间处理完后提交。一个数据集失败时，其他独立数据集可以完成并提交各自
checkpoint，重跑 `sync_all` 只补未完成区间。

## 自然分区和版本主键

下表的主键是“分区内主键”，表级唯一性等价于 `分区字段 + 分区内主键`。

| 业务 | 分区 | 分区内主键 |
| --- | --- | --- |
| `daily`、`daily_basic`、`adj_factor`、`stk_limit`、`moneyflow` | `trade_date` | `ts_code` |
| `suspend_d` | `trade_date` | `ts_code, suspend_type, suspend_timing` |
| `stock_st` | `trade_date` | `ts_code, type` |
| `income`、`balancesheet`、`cashflow` | `end_date` | `ts_code, report_type, comp_type, ann_date, f_ann_date` |
| `fina_indicator`、`forecast`、`express`、`fina_audit` | `end_date` | `ts_code, ann_date` |
| `dividend` | `end_date` | `ts_code, ann_date, div_proc, imp_ann_date` |
| `sw_industry` | `in_date` | `ts_code` |
| `trade_cal` | `cal_date` | `exchange` |

主键组件按 null-safe 语义去重，因此 `f_ann_date`、`imp_ann_date` 和
`suspend_timing` 可以为空。`update_flag` 不进入主键；同一版本键冲突时，
`update_flag=1` 优先，其次由 Manifest 提交顺序决定后写胜出。

三张财务报表将 `ann_date/f_ann_date` 放入存储主键，是为了保留源数据声明的
每个公告版本。DuckDB `as_of` 查询则使用不包公告日的业务键选择截止当日
`f_ann_date` 最新版本；`f_ann_date` 缺失时不会回退到 `ann_date`，两者不能混用。

分红原始表保留预案、决案和实施等生命周期记录。DuckDB `dividend_as_of` 只按
`imp_ann_date` 暴露已经发布实施公告的记录，不使用 `ann_date` 替补，以免预案行中
后来反填的登记日、除权日或派息日形成未来函数。

`TushareDataStore.write()` 会追加数据、刷新文件，再整理本次触及的每个分区。
空输入不会创建或清空分区。

## 申万行业

`sw_industry` 原样保留 `index_member_all` 的 `in_date/out_date` 区间，不在落盘时
拆成派生的 IN/OUT 事件。某日有效成员由读取层使用下列区间条件计算：

```sql
in_date <= as_of AND (out_date IS NULL OR out_date > as_of)
```

## 运行

```bash
uv sync --group tushare-data
export TUSHARE_TOKEN='你的 token'
uv run --group tushare-data tushare-data-test \
  --mode sync_all \
  --start-date 20170101 \
  --end-date 20260822 \
  --data-dir data/tushare
```

需要无视已有完成区间、完整重抓本次区间时增加 `--force`。`sync_inc` 的计划窗口本来就会
每次重抓，因此也接受 `--force`，但不会扩大或改变滚动窗口。

后续持续刷新：

```bash
uv run --group tushare-data tushare-data-test \
  --mode sync_inc \
  --current-date 20260822 \
  --data-dir data/tushare
```

`create_pro_client` 为 Tushare SDK 安装按工作线程复用连接的直连 HTTP 传输；它不读取
`HTTP_PROXY`、`HTTPS_PROXY`、`ALL_PROXY` 或 Windows 系统代理，也不会清除进程环境，
因此不影响同一进程中的其他网络客户端。单次请求读取超时为 120 秒。

## 迁移

新 Schema 与旧的 `partition_date/visible_at` 目录不兼容。不在原目录内混写：
应在新的 Tushare root 执行完整重拉，校验后再切换读取层路径。新 root 不应复用
旧目录的 `sync_all` 完成区间。

## PIT 能力边界

本设计还原的是 Tushare 原始日期字段声明的版本可见性。不保存 `observed_at`
意味着：如果供应商在相同公告日、相同版本标记下静默改值，旧值不可还原。
这是本存储边界的有意约束，不再通过额外行字段扩展。
