# tushare_data

`tushare_data` 通过 quicksync 代理调用 Tushare SDK，按全市场粒度获取指定历史区间，再写入现有
`parquet_store`。股票数据以 `partition_date` 分区、分区内按 `ts_code` 排序；交易日历以
`cal_date` 分区、分区内按 `exchange` 排序。

目录层级固定为“数据类别 → 日期分区”。例如 `daily`、`income`、`dividend` 各自拥有独立
表目录，目录下面才是对应的 `partition_date` 分区；不同类别的数据不会写入同一个日期目录。
`parquet_store` 为保留分区值类型会进行 URL 编码，实际路径示例为
`daily/partition_date=value%3A2024-04-30/`。

## 获取粒度

能使用全市场接口的数据均不再循环股票：

| 本地表 | Tushare 接口 | 全市场请求条件 | 可见时间（北京时间语义） |
| --- | --- | --- | --- |
| `daily` | `daily` | `trade_date` | 交易日 16:00 |
| `daily_basic` | `daily_basic` | `trade_date` | 交易日 17:00 |
| `adj_factor` | `adj_factor` | `trade_date` | 交易日 09:20 |
| `suspend_d` | `suspend_d` | `trade_date` | 交易日 09:30 |
| `stk_limit` | `stk_limit` | `trade_date` | 交易日 08:45 |
| `stock_st` | `stock_st` | `trade_date` | 交易日 09:20 |
| `moneyflow` | `moneyflow` | `trade_date` | 交易日 19:00 |
| `dividend` | `dividend` | 分别按 `ann_date`、`imp_ann_date` | 优先实施公告日结束，否则预案公告日结束 |
| `forecast` | `forecast_vip` | 公告日期区间 | 公告日结束 |
| `express` | `express_vip` | 公告日期区间 | 公告日结束 |
| `income` | `income_vip` | 公告日期区间 | 实际公告日结束 |
| `balancesheet` | `balancesheet_vip` | 公告日期区间 | 实际公告日结束 |
| `cashflow` | `cashflow_vip` | 公告日期区间 | 实际公告日结束 |
| `fina_indicator` | `fina_indicator_vip` | 公告日期区间 | 公告日结束 |
| `sw_industry` | `index_member_all` | 全市场当前和历史成员 | 纳入/移出日 09:00 |
| `trade_cal` | `trade_cal` | 上交所、深交所、北交所 | 日历日 00:00 |
| `fina_audit` | `fina_audit` | 无全市场接口，逐股票兜底 | 公告日结束 |

交易日数据会先同步三家交易所日历，只请求至少一家交易所开市的日期。所有接口都使用
`limit/offset` 连续分页，不能因为第一页刚好达到上限就判断结束。申万成员接口每页 2,000 行，
其余接口每页 5,000 行；实际服务端返回上限与请求值不一致时，也按真实返回行数推进 offset。

`fina_audit` 是唯一保留的逐股票接口。同步器先用三种上市状态的全市场 `stock_basic` 获取代码
清单，再逐只拉审计意见。这个数据集请求量明显高于其余表，因此 `sync_inc` 只在每周一执行。

## 时间与字段约定

表格中的钟点用于判断一条记录何时可以安全进入研究和回测。代码按 `Asia/Shanghai` 解释这些
市场时间，再转换成 Unix Epoch 微秒，`visible_at` 统一以 Arrow `int64` 传递和落盘。日志时间
和 Parquet 分区目录可使用北京时间，其余真实时间瞬间都用 UTC Epoch 微秒。

`partition_date` 是 `visible_at` 对应的北京时间日历日期，使用 `date32`，只负责物理分区。
行情数据中它等于 `trade_date`；财报、分红和行业事件没有统一交易日字段，因此分别等于实际
公告日、实施公告日或事件日。这样不会把周末公告错误挪到相邻交易日。

`trade_date`、`ann_date`、`end_date` 等也是市场日历标签，使用 `date32`；它们不是某一时区的
零点时间戳。每张表的完整字段定义和 `visible_at` 来源都直接写在
`src/tushare_data/schemas.py` 对应字段列表旁。上游缺少任一固定字段会立即报错，不会静默改变
本地 Schema。

公告接口没有时分秒，所以保守地在北京时间公告日 `23:59:59.999999` 后可见。利润表、
资产负债表和现金流量表优先使用 `f_ann_date`，不能用报告期 `end_date` 倒填。分红实施记录中
含有登记日、除权日等后续信息，因此完整实施记录优先以 `imp_ann_date` 为可见日，防止在预案
公告时提前看到未来实施字段。申万成员的 `in_date/out_date` 被拆成独立 `IN/OUT` 事件，避免
历史 `IN` 行提前带出未来移出日。

字段核对来源包括 Tushare 官方的[日线](https://tushare.pro/document/1?doc_id=27)、
[每日指标](https://tushare.pro/document/2?doc_id=32)、
[复权因子](https://tushare.pro/document/2?doc_id=28)、
[每日涨跌停价格](https://tushare.pro/document/2?doc_id=183)、
[ST 股票列表](https://tushare.pro/document/2?doc_id=397)、
[资金流向](https://tushare.pro/document/2?doc_id=170)、
[利润表](https://tushare.pro/document/2?doc_id=33)、
[资产负债表](https://tushare.pro/document/2?doc_id=36)、
[现金流量表](https://tushare.pro/document/2?doc_id=44)、
[财务指标](https://tushare.pro/document/2?doc_id=79)、
[业绩预告](https://tushare.pro/document/2?doc_id=45)和
[分红送股](https://tushare.pro/document/2?doc_id=103)。

固定 Schema 采用“官方文档定义且 quicksync 实际返回”的字段。2026-08-19 实测时，官方已经
列出但代理仍会省略的字段包括：`daily_basic.limit_status`，利润表的
`net_after_nr_lp_correct`、`credit_impa_loss` 等 9 项，以及资产负债表的
`oth_eq_invest`、`receiv_financing`、`use_right_assets` 等 6 项。这些列暂不强行补 null，
否则严格缺列检查会让历史同步失败。待代理实际返回后再按新 Schema 重拉。

反过来，`forecast.update_flag` 和 `express.update_flag` 虽未出现在当前官方输出参数表中，
但 quicksync 会实际返回，因此保留。`schemas.py` 中每个源字段和本地派生字段上方都有中文
说明；官方 `int` 字段使用 Arrow `int64`，日期标签使用 `date32`，真实时间瞬间仍使用 Unix
Epoch 微秒 `int64`。

## 限速

[quicksync 参考文档](https://run.quicksync.cn/code/YWQ0OTNhYTA2ZDM0Nzc5OTE0MzNlMzdkZmIwNDA3YzQ5ZjIzMmUxM2JlZGEyOTdlOTA1YzdjODA=)
给出的频率上限是基础版每分钟 120 次、标准版 600 次、极速版 1,200 次；异常流量可能被临时
降到每分钟 60 次或 0 次。代码默认每分钟 120 次、最大并发 1。所有分页和所有数据集共享同一
个线程安全限流器，购买更高额度后才应显式提高：

```python
pro = create_pro_client(
    token,
    requests_per_minute=600,
    max_concurrency=1,
)
```

连接超时、临时断线和 HTTP 响应体中途截断默认最多重试 3 次，等待时间依次为 1、2、4 秒；
每次重试仍重新经过同一个限流器。字段错误、权限错误和接口参数错误不会重试。

## 增量规则

`TushareDataStore` 对同步层只提供 `write(dataset, data)` 和 `read(...)` 两个业务方法。
`write` 在内部校验固定 Schema、按日期拆分非空数据，并用当天完整截面覆盖原日期分区；同步代码
不需要知道 `parquet_store` 的 replace、分区和排序细节。

### sync_all：一次性历史回填

`sync_all(pro, store, start_date, end_date)` 为每张表持久化已经成功完成的日期闭区间。行情和分红
每 31 个自然日提交一个断点，公告区间和逐股审计每 366 天提交一个断点；每块数据先写 Parquet，
成功后才原子更新元数据。发生断网或进程退出时，重新执行同一命令会从失败块开始，已经完成的
日期不会再次请求。所有缺失区间和分块都严格按日期升序执行。

元数据位于 `<data-dir>/_meta/sync_all/<dataset>.json`，例如：

```json
{
  "completed_ranges": [
    {"start_date": "2017-01-01", "end_date": "2022-12-31"}
  ],
  "dataset": "daily",
  "updated_at": 1787155200000000,
  "version": 1
}
```

`updated_at` 是 UTC Unix Epoch 微秒整数。左右扩展历史范围时，程序只补尚未完成的区间，并在
区间相邻后自动合并。接口成功但返回 0 行也算完成，因为 `sync_all` 只负责短时间内完成一次
初始回填，不考虑后续修正。若要强制重新执行完整回填，需要同时删除相应数据目录和
`_meta/sync_all` 中对应的 JSON。

### sync_inc：持续修正

`sync_inc` 不读取也不更新上述断点。同一区间再次执行会重新请求；有返回的完整全市场日期截面
直接替换对应 Parquet 分区，接口返回空数据时不清空已有分区。迟到、补录和修订数据都由滚动
回看窗口处理，不会因为 `sync_all` 已经标记完成而被跳过。

例如日常行情任务可以始终传入最近 5 个交易日；如果某天首次请求暂时没有拿全，后续任务会再次
刷新该分区。这里的“增量”由较小的滚动区间实现，逻辑简单，也能自然接住 Tushare 的迟到数据。

`sync_inc(pro, store, current_date)` 已把滚动窗口和定期任务固定下来：

| 数据类型 | 普通增量窗口 | 定期深度刷新 |
| --- | --- | --- |
| 日线、涨跌停、停复牌、ST | 最近 5 个交易日 | 无 |
| 每日指标、资金流、复权因子 | 最近 10 个交易日 | 无 |
| 三张财务报表、财务指标 | 最近 10 个自然日公告 | 每月 1 日回看 3 年，约 12 个报告期 |
| 业绩预告、业绩快报 | 最近 180 个自然日 | 无 |
| 审计意见 | 非定期日跳过 | 每周一回看 180 个自然日 |
| 分红 | 最近 30 个自然日的预案和实施公告 | 每月 1 日回看 2 年 |
| 申万行业 | 非定期日跳过 | 每周一刷新完整成员快照 |
| 交易日历 | 回看 60 个自然日 | 同时刷新未来 366 个自然日 |

所有普通数据窗口都截止到传入的 `current_date`；交易日历是唯一例外，因为未来休市安排可能
调整。周一和每月 1 日直接由 `current_date` 判断，不保存额外调度状态。如果任务每天运行，
自然会覆盖这些定期刷新日。

行情类按 31 个自然日组成一批提交给存储层，`parquet_store` 再拆成每日分区。每个日期分区
内部按 `ts_code` 排序，同一天全市场横截面可以直接读取，不需要扫描数千个股票目录。

交易日历会先合并 SSE、SZSE、BSE，再把同一天的三家交易所记录一起交给 `write`。申万行业
接口本身返回全量成员快照，因此每次会生成完整的历史 `IN/OUT` 事件，再按事件日期覆盖已有
分区。存储层本身不包含任何交易日历或行业分类的特殊分支。

同步层不判断某个日期的数据现在是否理应存在，也不裁剪今天或未来日期。调用方传到哪一天就请求
到哪一天；尚未产生数据时接口通常返回空表，本次写入 0 行即可。`visible_at` 只约束已经返回的
记录何时可用于研究，不参与决定是否发起请求。

本版本不兼容旧的股票分区 Schema，也不做迁移；按约定直接删除旧 Tushare 数据目录后重拉。

## 运行验证

安装依赖：

```bash
uv sync --group tushare-data
```

Token 只放在当前进程环境中，不写入代码：

```bash
export TUSHARE_TOKEN='你的 token'
uv run --group tushare-data tushare-data-test \
  --mode sync_all \
  --start-date 20210101 \
  --end-date 20221231 \
  --data-dir data/tushare
```

随后用同一个目录扩展范围：

```bash
uv run --group tushare-data tushare-data-test \
  --mode sync_all \
  --start-date 20170101 \
  --end-date 20260819 \
  --data-dir data/tushare
```

第二次只会请求 `2017--2020` 和 `2023--2026` 两侧尚未完成的区间，已经完成的 2021--2022
会直接跳过。如果中途退出，再次执行相同命令会继续尚未完成的分块。后续数据修订交给
`sync_inc`，不会让 `sync_all` 重拉已完成历史。

验证自动增量规则：

```bash
uv run --group tushare-data tushare-data-test \
  --mode sync_inc \
  --current-date 20260819 \
  --data-dir data/tushare
```

程序中直接调用：

```python
import os
from datetime import UTC, datetime

from fpro_common import datetime_to_utc_us
from tushare_data import TushareDataStore, create_pro_client, sync_all, sync_inc

pro = create_pro_client(os.environ["TUSHARE_TOKEN"])
# 客户端业务方法支持直接按顺序传参，签名里没有隐藏的 *args / **kwargs。
sample_daily = pro.daily("20240102", "ts_code,trade_date,close", 5_000, 0)
with TushareDataStore("data/tushare") as store:
    result = sync_all(pro, store, "20170101", "20260819")
    incremental_result = sync_inc(pro, store, "20260819")
    visible_by_noon = store.read(
        "daily",
        datetime(2024, 1, 10, tzinfo=UTC).date(),
        ts_code="000001.SZ",
        visible_end=datetime_to_utc_us(datetime(2024, 1, 10, 4, 0, tzinfo=UTC)),
    )
```

`TushareProClient` 是唯一的具体业务客户端。每个接口都把业务筛选条件、`fields`、`limit`
和 `offset` 逐项写在方法签名中，既可按位置传参，也可按参数名传参；没有公开的 `*args`
或 `**kwargs`。动态 Tushare SDK 只在客户端内部的 `query` 边界做一次运行时检查，限流和
重试也封装在客户端内部，业务同步、测试入口和存储链路不传播 `Any`。
