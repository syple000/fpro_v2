# qmt-agent 模块

一个尽量小而直接的东北证券 miniQMT 行情 HTTP 服务。它负责：

- 全市场热订阅、热取消订阅；
- 合约列表按周期热订阅、热取消订阅，去重后总数严格不超过 50；
- 获取全市场最新快照截面；
- 按合约列表获取最新快照；
- 获取订阅收到的最新行情，或按全局递增序号顺序消费行情；
- 按合约列表增量或全量下载历史数据，并查询本地历史数据。

服务只运行单进程。订阅状态、最新行情和顺序行情循环缓存保存在内存中，重启后需要
重新订阅，行情序号也会从 1 重新开始。

所有响应、XtData 行情明细和顺序行情均使用共享 Pydantic 协议模型，完整字段和类型见
[QMT 行情数据协议](qmt_protocol.md)。Swagger/OpenAPI 会展示每个接口的响应结构，
不再只有请求结构。

## Windows 启动

前提：

1. 已安装 [uv](https://docs.astral.sh/uv/)；
2. 快捷方式位于 `%USERPROFILE%\Desktop\东北证券NET专业版.lnk`；
3. `xtquant` 位于 `C:\Program Files\东北证券NET专业版\bin.x64\Lib\site-packages`。

双击 `scripts\qmt_agent\start_qmt_agent.cmd`。脚本直接把上述两个路径传给启动器，
校验后关闭正在运行的旧 qmt-agent 和该安装目录下的 miniQMT，然后启动 miniQMT，
等待 60 秒，再启动 qmt-agent。启动器根据传入的 `qmt-bin` 配置客户端 Python 包和
DLL 目录；不要直接用普通 Python 环境启动。

启动器从 miniQMT 启动前开始阻止 Windows 因空闲自动休眠，并在 qmt-agent 退出时恢复
系统默认电源行为。该设置不会强制点亮显示器，也不会阻止用户通过合盖、电源按钮或系统
菜单主动休眠。

默认路径为：

- `QMT bin.x64`：`C:\Program Files\东北证券NET专业版\bin.x64`
- Python 包：`C:\Program Files\东北证券NET专业版\bin.x64\Lib\site-packages`
- 快捷方式：`%USERPROFILE%\Desktop\东北证券NET专业版.lnk`

启动器不会搜索目录或注册表。进程检测只匹配 qmt-agent 启动命令，以及明确传入的
`bin.x64` 目录中的 `XtMiniQmt.exe`；不会关闭同目录的其他 QMT 组件，也不会按
`python.exe` 等通用进程名关闭程序。如果安装位置发生变化，直接覆盖参数：

```bat
scripts\qmt_agent\start_qmt_agent.cmd ^
  --qmt-bin "C:\Program Files\东北证券NET专业版\bin.x64" ^
  --qmt-shortcut "%USERPROFILE%\Desktop\东北证券NET专业版.lnk"
```

可通过环境变量修改监听参数：

```powershell
$env:QMT_AGENT_HOST = "127.0.0.1"
$env:QMT_AGENT_PORT = "8765"
$env:QMT_AGENT_LOG_LEVEL = "info"
$env:QMT_AGENT_MAX_SUBSCRIPTIONS = "50"
$env:QMT_AGENT_QUOTE_BUFFER_CAPACITY = "10000"
.\scripts\qmt_agent\start_qmt_agent.cmd
```

`QMT_AGENT_MAX_SUBSCRIPTIONS` 可在 1 到 50 之间调整。
`QMT_AGENT_QUOTE_BUFFER_CAPACITY` 是顺序行情循环缓存最多保留的行情条数，默认 10000，
必须大于等于 1。

默认只监听本机，避免行情接口意外暴露到局域网。启动后可访问：

- 健康检查：<http://127.0.0.1:8765/health>
- Swagger 接口文档：<http://127.0.0.1:8765/docs>

miniQMT 必须已经登录并连接行情服务器。Python 启动器只负责启动客户端，不能代替人工登录、验证码或客户端侧的连接配置。

## 接口

所有代码会去除首尾空格并转为大写。合约代码使用 `000001.SZ` 格式，市场代码使用 `SH`、`SZ` 等 miniQMT 支持的市场代码。

所有请求体都使用严格校验：未定义字段、错误字段类型、不支持的枚举值、无效日期和起止
时间倒置都会返回 HTTP 422，不会静默忽略字段或自动转换类型。

### 全市场订阅

```http
POST /v1/subscriptions/markets
Content-Type: application/json

{"markets":["SH","SZ"]}
```

请求体可省略，默认订阅 `SH` 和 `SZ`。每个市场独立调用一次
`subscribe_whole_quote([market])` 并维护自己的订阅号。重复订阅是
幂等的；新增市场只创建缺少的订阅，不会取消或重建已有市场订阅。
响应只返回当前订阅范围和本次新增的市场；XtData 订阅号由 agent 内部维护，
不会暴露给调用方。

### 全市场取消订阅

```http
DELETE /v1/subscriptions/markets
Content-Type: application/json

{"markets":["SH","SZ"]}
```

省略请求体时取消当前全部市场订阅。
传入市场列表时只取消明确指定且已经存在的市场订阅，不影响其他市场。

### 按列表订阅

```http
POST /v1/subscriptions/stocks
Content-Type: application/json

{"stocks":["000001.SZ","600000.SH"],"period":"1m"}
```

`period` 必填，支持 `tick`、`1m`、`5m`、`15m`、`30m`、`1h`、`1d`、
`1w`、`1mon`、`1q`、`1hy`、`1y`。服务会按照 XtData 官方接口逐只调用
`subscribe_quote(stock_code, period=..., count=0, callback=...)`；`count=0` 表示只订阅
实时数据，不额外请求历史部分。

请求会和已有列表合并，只为尚未订阅的合约创建订阅。同一合约再次使用不同 `period`
订阅时返回 HTTP 409，已有订阅不会被取消或替换；如需修改周期，调用方必须先使用原周期
显式取消，再发起新周期订阅。
响应中的 `periods` 是当前周期映射。XtData 订阅号由 agent 内部维护，
不会暴露给调用方。
官方建议单股订阅不超过 50，因此服务默认并强制将上限设为 50，超过时返回 HTTP 409，
原订阅保持不变。更多合约应使用全市场全推订阅。

批量请求按代码逐项执行。某一项创建失败时请求返回错误，但不会为了回滚而取消本次已经
成功创建的订阅，更不会触碰请求前已有的订阅；调用方可通过 `GET /v1/subscriptions`
确认实际状态后重试缺少部分。

### 按列表取消订阅

```http
DELETE /v1/subscriptions/stocks
Content-Type: application/json

{"stocks":["000001.SZ"],"period":"1m"}
```

取消列表订阅必须传入原 `period`，只取消股票和周期均匹配的订阅。不存在的股票通过
`not_found` 返回；股票存在但周期不匹配时通过 `period_mismatches` 返回，不会调用
`unsubscribe_quote`。

### 全市场快照截面

```http
POST /v1/snapshots/markets
Content-Type: application/json

{"markets":["SH","SZ"]}
```

该接口直接调用 `xtdata.get_full_tick`。返回数据可能较大，应按实际频率调用，不建议高频轮询全市场。

### 按列表获取快照

```http
POST /v1/snapshots/stocks
Content-Type: application/json

{"stocks":["000001.SZ","600000.SH"]}
```

该接口同样直接调用 `xtdata.get_full_tick`，不会占用列表订阅额度。

### 获取订阅最新数据

全市场订阅和单股订阅使用不同的回调结构、订阅范围和缓存，因此通过两个接口分别读取。

读取全市场订阅收到的最新 tick：

```http
POST /v1/quotes/subscribed/markets
Content-Type: application/json

{"stocks":["600000.SH"]}
```

读取单股订阅收到的最新行情：

```http
POST /v1/quotes/subscribed/stocks
Content-Type: application/json

{"stocks":["000001.SZ"]}
```

两个接口的请求体均可省略，此时返回各自订阅范围内已经收到的全部最新行情。全市场结果
可能较大，建议通过 `stocks` 指定所需合约。响应中的 `missing` 表示该类型已经订阅、但尚未
收到行情；`not_subscribed` 只按当前接口对应的订阅类型判断。例如某合约只有单股订阅，
请求全市场接口时仍会出现在 `not_subscribed` 中。

`periods` 标明行情周期。同一合约同时存在全市场 tick 和单股周期订阅时，两个接口分别返回
各自缓存的数据，不再隐式覆盖。服务不提供合并市场和单股行情的兼容接口；调用方应根据
订阅类型明确使用 `/v1/quotes/subscribed/markets` 或 `/v1/quotes/subscribed/stocks`。

这些最新数据接口每个合约只返回最后一条；完整的有序数据通过下节的顺序接口读取。

### 按序获取订阅行情

每条回调行情都会被分配一个进程内全局递增的 `seq` 并写入循环缓存。列表订阅的一次回调
即使包含多条行情，也会逐条入队，不会只保留最后一条。只有缓存超过
`QMT_AGENT_QUOTE_BUFFER_CAPACITY` 时才会淘汰最旧记录。

从指定序号开始读取：

```http
POST /v1/quotes/subscribed/sequence
Content-Type: application/json

{"seq":1201,"limit":100,"wait_ms":30000}
```

`limit` 默认 100，范围为 1 到 1000，表示本次扫描的连续序号窗口大小。响应示例：

```json
{
  "data": [
    {
      "seq": 1201,
      "code": "000001.SZ",
      "period": "1m",
      "source": "stock",
      "subscription": "000001.SZ",
      "received_at": "2026-08-16T01:02:03+00:00",
      "quote": {"close": 10.2}
    }
  ],
  "count": 1,
  "requested_seq": 1201,
  "next_seq": 1301,
  "oldest_seq": 1000,
  "latest_seq": 1350
}
```

可选的 `stocks` 只筛选当前序号窗口中的返回项，不改变全局序号，也不改变窗口推进规则：

```json
{"seq":1201,"limit":100,"stocks":["000001.SZ"]}
```

调用方下一次应使用响应的 `next_seq`。如果请求序号小于 `oldest_seq`（数据已被循环缓存
覆盖），或大于 `latest_seq`（数据尚未到达），接口返回 HTTP 416，并在响应中明确给出
`requested_seq`、`oldest_seq` 和 `latest_seq`。缓存尚无数据时，最旧和最新序号均为 `null`。

`wait_ms` 默认 0，范围为 0 到 30000。当请求的正好是下一条待到达序号时，服务最多等待
该时长；数据一到即返回，超时仍返回 HTTP 416。该参数让实时接收方无需固定间隔轮询，
同时避免空闲时忙循环。

`GET /v1/subscriptions` 和 `GET /health` 的 `quote_sequence` 字段会返回当前
`oldest_seq`、`latest_seq`、下一待分配序号 `next_seq`、`size` 和 `capacity`，上游可据此
选择首次读取序号。状态还包含本次 agent 进程唯一的 `instance_id`，用于识别进程重启后
从 1 重新开始的新序列。这里的行情 `seq` 与 XtData 内部订阅号无关。

### 下载历史数据

增量下载会从本地已有数据之后继续下载：

```http
POST /v1/history/download
Content-Type: application/json

{
  "stocks":["000001.SZ","600000.SH"],
  "period":"1d",
  "mode":"incremental"
}
```

全量下载可指定起止时间；留空表示完整范围：

```json
{
  "stocks":["000001.SZ"],
  "period":"1m",
  "start_time":"20250101",
  "end_time":"20251231",
  "mode":"full"
}
```

下载接口是同步接口，数据完成落盘后才返回。大量合约或长时间范围建议拆成多个请求。
部分券商内置的旧版 xtquant 不支持批量接口的 `incrementally` 参数；agent 检测到该特定
`TypeError` 时会自动去掉参数重试，其他参数错误仍会正常返回。

### 查询历史数据

```http
POST /v1/history/query
Content-Type: application/json

{
  "stocks":["000001.SZ","600000.SH"],
  "fields":["time","open","high","low","close","volume"],
  "period":"1d",
  "start_time":"20250101",
  "end_time":"20251231",
  "count":-1,
  "dividend_type":"none",
  "fill_data":true
}
```

K 线中的 pandas DataFrame 会返回为 `{"index": [...], "columns": [...], "data": [...]}`，numpy 数值和数组会转换为普通 JSON；`NaN`、正负无穷会转换为 `null`。

### 查看订阅状态

```http
GET /v1/subscriptions
```

`stock_periods` 返回每个列表订阅合约当前使用的周期。状态接口不返回 XtData 订阅号；
订阅与取消订阅所需的底层关联由 agent 封装。

## XtData 接口对应关系

本服务只封装以下官方行情接口：

| HTTP 能力 | XtData 接口 | 使用方式 |
| --- | --- | --- |
| 全市场订阅 | `subscribe_whole_quote` | 每个市场独立订阅，接收 tick 全推数据 |
| 股票列表订阅 | `subscribe_quote` | 逐合约调用，显式传 `period`，实时订阅使用 `count=0` |
| 取消订阅 | `unsubscribe_quote` | agent 使用内部保存的订阅号取消订阅 |
| 市场/股票快照 | `get_full_tick` | 分别传市场代码或合约代码列表 |
| 历史下载 | `download_history_data2` | 使用官方批量同步下载接口 |
| 历史查询 | `get_local_data` | 下载完成后从本地文件快速批量读取，不产生隐式订阅 |

## 本地开发

Linux 或没有 miniQMT 的机器仍可安装依赖并运行完整测试，因为测试使用假的行情网关：

```bash
uv sync --group qmt-agent
uv run --group qmt-agent pytest tests/qmt_agent
uv run ruff check .
```

qmt-agent 的测试按职责隔离：

- `tests/qmt_agent/unit`：业务规则、序列化、网关调用和 Windows 启动器；
- `tests/qmt_agent/integration`：FastAPI 接口和组件组合；
- `tests/qmt_agent/stress`：并发按需订阅、50 上限竞争和高频行情读写。

可分别运行：

```bash
uv run --group qmt-agent pytest tests/qmt_agent/unit
uv run --group qmt-agent pytest tests/qmt_agent/integration
uv run --group qmt-agent pytest tests/qmt_agent/stress
```

压力测试验证最终状态和并发不变量，不使用依赖机器性能的耗时阈值。

真实服务必须在能导入东北证券客户端 `xtquant` 的 Windows 环境中运行。

miniQMT 接口行为以[迅投 XtData 官方文档](https://dict.thinktrader.net/nativeApi/xtdata.html)为准。
