# tushare_data

`tushare_data` 通过 quicksync 代理调用 Tushare SDK，把单只 A 股的历史数据写入现有
`parquet_store`。股票表以 `ts_code` 分区；交易日历以 `exchange` 分区。所有表都以
`visible_at` 升序保存。

## 已实现的数据

| 本地表 | Tushare 接口 | 内容 | 可见时间 |
| --- | --- | --- | --- |
| `daily` | `daily` | 未复权日 K 线 | 交易日 16:00 |
| `daily_basic` | `daily_basic` | 估值、换手率、股本、市值 | 交易日 17:00 |
| `adj_factor` | `adj_factor` | 复权因子 | 交易日 09:20 |
| `suspend_d` | `suspend_d` | 停牌、复牌 | 事件日 09:30 |
| `stk_limit` | `stk_limit` | 每日涨停价、跌停价 | 交易日 08:45 |
| `stock_st` | `stock_st` | 每日 ST、风险警示状态（2016 年起） | 交易日 09:20 |
| `moneyflow` | `moneyflow` | 小、中、大、特大单资金流向 | 交易日 19:00 |
| `dividend` | `dividend` | 分红送转、登记日、除权日、实施日 | 公告日结束 |
| `forecast` | `forecast` | 业绩预告及其更新版本 | 公告日结束 |
| `express` | `express` | 业绩快报 | 公告日结束 |
| `fina_audit` | `fina_audit` | 财务审计意见 | 公告日结束 |
| `income` | `income` | 利润表及其修订版本 | 实际公告日结束 |
| `balancesheet` | `balancesheet` | 资产负债表及其修订版本 | 实际公告日结束 |
| `cashflow` | `cashflow` | 现金流量表及其修订版本 | 实际公告日结束 |
| `fina_indicator` | `fina_indicator` | 财务分析指标 | 公告日结束 |
| `sw_industry` | `index_member_all` | 申万一级、二级、三级行业成员变更 | 纳入/移出日 09:00 |
| `trade_cal` | `trade_cal` | 上交所、深交所、北交所交易日历 | 日历日 00:00 |

表格中的钟点是上游数据在中国市场的北京时间发布时间；代码先按 `Asia/Shanghai` 解释，
随后转换为 Unix Epoch 微秒，所有 `visible_at` 均以 Arrow `int64` 落盘。K 线不会在当天开盘前
可见，财报不会按报告期 `end_date` 倒填，而是优先使用 `f_ann_date`。申万成员记录被拆成
`IN` 和 `OUT` 两种事件，
所以历史的 `IN` 行不会包含未来才知道的 `out_date`。财务和分红接口只给公告日期、不提供
时分秒，因此先保守地解释为北京时间公告日 `23:59:59.999999`，再转成 UTC，避免盘中回测
提前使用当天晚些时候才发布的公告。

`trade_date`、`ann_date`、`end_date` 等字段是市场日历标签，继续使用 `date32`，不强行伪装
成某一时区的零点。只有表示真实时间瞬间的 `visible_at` 使用 Unix Epoch 微秒 `int64`。

字段在 `src/tushare_data/schemas.py` 中逐一固定定义。拉取时会把同一字段列表显式传给
Tushare；上游缺少任何已定义字段会直接报错，不会静默改变 Parquet Schema。除标识、日期、
枚举字符串外，Tushare 财务数值统一存为 `float64`，以兼容不同公司类型中大量的空值。

字段定义核对来源：Tushare 官方的 [日线](https://tushare.pro/document/2?doc_id=27)、
[每日指标](https://tushare.pro/document/2?doc_id=32)、
[复权因子](https://tushare.pro/document/2?doc_id=28)、
[停复牌](https://tushare.pro/document/2?doc_id=214)、
[利润表](https://tushare.pro/document/2?doc_id=33)、
[资产负债表](https://tushare.pro/document/2?doc_id=36)、
[现金流量表](https://tushare.pro/document/2?doc_id=44)、
[分红送股](https://tushare.pro/document/2?doc_id=103)、
[财务指标](https://tushare.pro/document/2?doc_id=79) 和
[申万行业成分](https://tushare.pro/document/2?doc_id=335)。除文档核对外，测试入口还会把
固定字段列表真正传给接口，以实际返回结果验证字段存在性和类型转换。

补充的量化数据主要解决两类问题：`stk_limit`、`stock_st`、`trade_cal` 与停复牌数据共同
描述当日是否允许交易以及价格边界；`forecast`、`express`、`fina_audit` 补齐正式财报之前
和审计环节的信息时间线。`moneyflow` 属于常用的交易行为因子。暂不把融资融券、龙虎榜、
大宗交易等市场级数据放进单股票必选集合，因为它们通常需要按交易日全市场批量拉取，适合
另建市场级同步任务，避免每只股票重复请求同一批数据。

新增字段定义还核对了 Tushare 官方的
[每日涨跌停价格](https://tushare.pro/document/2?doc_id=183)、
[ST 股票列表](https://tushare.pro/document/2?doc_id=397)、
[个股资金流向](https://tushare.pro/document/2?doc_id=170)、
[业绩预告](https://tushare.pro/document/2?doc_id=45)、
[业绩快报](https://tushare.pro/document/2?doc_id=46)、
[财务审计意见](https://tushare.pro/document/2?doc_id=80) 和
[交易日历](https://tushare.pro/document/2?doc_id=26)。

## 请求限制

[quicksync 参考文档](https://run.quicksync.cn/code/YWQ0OTNhYTA2ZDM0Nzc5OTE0MzNlMzdkZmIwNDA3YzQ5ZjIzMmUxM2JlZGEyOTdlOTA1YzdjODA=)
给出的频率上限是基础版每分钟 120 次、标准版 600 次、极速版 1200 次；异常流量可能临时
降至每分钟 60 次或 0 次，次日恢复。文档只明确了每分钟频率，没有给出固定并发数，因此代码
采用最稳妥的默认值：每分钟 120 次、最大并发 1。

`create_pro_client` 创建的所有接口共享同一个线程安全限流器。即使以后并行调用不同同步函数，
请求开始时间也会按频率均匀错开，同时受最大在途请求数约束。购买了更高速率的套餐时才应显式
调整：

```python
pro = create_pro_client(
    token,
    requests_per_minute=600,
    max_concurrency=1,
)
```

命令行对应参数是 `--requests-per-minute` 和 `--max-concurrency`。如果代理返回动态降速提示，
本次程序不会猜测新的额度；应停止任务，按服务端提示降低参数后续跑。已完成区间不会重拉。

## 增量规则

`_sync_ranges` 表记录 `(dataset, partition)` 已成功请求的闭区间；股票表的 partition 是
`ts_code`，交易日历是 `exchange`。下一次请求先计算尚未覆盖的
区间，只调用这些区间。成功但返回 0 行的区间也会记录，因此“没有停牌/没有公告”不会导致
反复请求。左右两段已有数据之间存在空洞时，也只补这个空洞。
请求结束日晚于今天时会自动截到上海时区的今天，未来区间不会被误记成已同步。
如果当天还没到该表的可见时间（例如 `daily` 的 16:00、`daily_basic` 的 17:00），当天也
不会标成完成；公告类接口因为没有精确发布时间，要到次日才确认前一天区间。

为避免触发 Tushare 单次返回行数上限，行情类缺失区间最多按 10 年一段请求，财务类最多
按 2 年一段请求。`fina_indicator` 的接口日期参数实际按报告期过滤，代码会向前多取 550 天
的报告期，再按公告 `visible_at` 裁回目标区间，避免漏掉次年披露的上年度年报指标。

`dividend` 和 `index_member_all` 的单股票接口没有日期区间参数。代码只在存在未覆盖区间时
调用一次全历史接口，然后仅把缺失区间内的数据写入本地。由于上游限制，这两个接口在扩展
历史范围时仍会返回一次全历史源数据，但落盘时只写缺失区间并按整行去重。

## 实际接口验证

先安装模块依赖：

```bash
uv sync --group tushare-data
```

Token 不写入代码，通过环境变量运行全部接口测试：

```bash
export TUSHARE_TOKEN='你的 token'
uv run --group tushare-data tushare-data-test \
  --ts-code 000001.SZ \
  --start-date 20240101 \
  --end-date 20241231 \
  --data-dir data/tushare
```

验证“先 2021--2022，再扩展到 2017--2026”的增量行为时，两次必须使用同一个目录：

```bash
uv run --group tushare-data tushare-data-test \
  --ts-code 000001.SZ --start-date 20210101 --end-date 20221231 \
  --data-dir data/tushare

uv run --group tushare-data tushare-data-test \
  --ts-code 000001.SZ --start-date 20170101 --end-date 20260817 \
  --data-dir data/tushare
```

第二次对支持日期区间的接口只请求 `2017--2020` 和 `2023--2026` 两侧缺失区间。程序不会把
周末、休市日或 0 行公告区间误判成缺口，因为覆盖元数据记录的是“请求成功的日期区间”，
而不是返回数据的最小/最大日期。第三次执行相同命令时各表“本次拉取”应为 `0`。

再次执行相同命令时，各表日志中的“本次拉取”应为 `0`。也可只验证部分接口：

```bash
uv run --group tushare-data tushare-data-test --datasets daily,income,sw_industry
```

程序中直接调用：

```python
import os
from datetime import UTC, datetime

from fpro_common import datetime_to_utc_us
from tushare_data import TushareDataStore, create_pro_client, sync_all

pro = create_pro_client(
    os.environ["TUSHARE_TOKEN"],
    requests_per_minute=120,
    max_concurrency=1,
)
with TushareDataStore("data/tushare") as store:
    result = sync_all(pro, store, "000001.SZ", "20100101", "20241231")
    known_at_noon = store.read(
        "daily",
        "000001.SZ",
        as_of=datetime_to_utc_us(datetime(2024, 1, 10, 4, 0, tzinfo=UTC)),
    )
```

新时间 Schema 不兼容旧数据目录，不做自动迁移；切换版本时应删除旧数据并重新拉取。
