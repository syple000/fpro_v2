# qmt-agent 模块

一个尽量小而直接的东北证券 miniQMT 行情 HTTP 服务。它负责：

- 全市场热订阅、热取消订阅；
- 合约列表热订阅、热取消订阅，去重后总数严格不超过 300；
- 获取全市场最新快照截面；
- 按合约列表获取最新快照；
- 获取订阅收到的最新行情；
- 按合约列表增量或全量下载历史数据，并查询本地历史数据。

服务只运行单进程。订阅状态和最新行情保存在内存中，重启后需要重新订阅。

## Windows 启动

前提：

1. 已安装 [uv](https://docs.astral.sh/uv/)；
2. 桌面存在东北证券 miniQMT 客户端快捷方式；
3. 客户端目录包含 `bin.x64\Lib\site-packages\xtquant`。

双击 `scripts\qmt_agent\start_qmt_agent.cmd`，脚本会依次完成：

1. 由 uv 运行 `scripts/qmt_agent/start_qmt_agent.py`；
2. 阻止 Windows 因空闲自动进入睡眠；
3. 查找并启动桌面的 miniQMT 快捷方式；
4. 等待客户端初始化；
5. 将客户端自带的 `xtquant` 和 DLL 目录传给 agent；
6. 在同一个 Python 进程中启动服务。

防休眠状态只在 agent 运行期间有效，agent 退出时自动恢复。它不会阻止显示器按系统设置熄屏，也不会阻止用户主动点击睡眠、关机或重启。

脚本默认会自动查找类似
`C:\Program Files\东北证券NET专业版\bin.x64` 的目录。如果自动查找失败，可在
命令行中显式指定：

```bat
scripts\qmt_agent\start_qmt_agent.cmd ^
  --qmt-bin "C:\Program Files\东北证券NET专业版\bin.x64" ^
  --qmt-shortcut "%USERPROFILE%\Desktop\东北证券NET专业版.lnk"
```

每次启动前，Python 启动器都会检查旧的 qmt-agent 和 miniQMT 客户端。若发现正在运行的实例，会先请求客户端正常关闭；超时后终止残留进程，确认完全退出后再重新启动客户端和 agent。客户端按安装目录匹配，agent 按启动命令匹配，不会按 `python.exe` 进程名批量结束其他 Python 程序。如果 Windows 拒绝关闭进程，启动器会报错停止，必要时可用管理员身份运行 `scripts\qmt_agent\start_qmt_agent.cmd`。

可通过环境变量修改监听参数：

```powershell
$env:QMT_AGENT_HOST = "127.0.0.1"
$env:QMT_AGENT_PORT = "8765"
$env:QMT_AGENT_LOG_LEVEL = "info"
.\scripts\qmt_agent\start_qmt_agent.cmd
```

默认只监听本机，避免行情接口意外暴露到局域网。启动后可访问：

- 健康检查：<http://127.0.0.1:8765/health>
- Swagger 接口文档：<http://127.0.0.1:8765/docs>

miniQMT 必须已经登录并连接行情服务器。Python 启动器只负责启动客户端，不能代替人工登录、验证码或客户端侧的连接配置。

## 接口

所有代码会去除首尾空格并转为大写。合约代码使用 `000001.SZ` 格式，市场代码使用 `SH`、`SZ` 等 miniQMT 支持的市场代码。

### 全市场订阅

```http
POST /v1/subscriptions/markets
Content-Type: application/json

{"markets":["SH","SZ"]}
```

请求体可省略，默认订阅 `SH` 和 `SZ`。重复订阅是幂等的。添加或移除市场会在服务运行期间重建对应订阅，无需重启。

### 全市场取消订阅

```http
DELETE /v1/subscriptions/markets
Content-Type: application/json

{"markets":["SH","SZ"]}
```

省略请求体时取消当前全部市场订阅。

### 按列表订阅

```http
POST /v1/subscriptions/stocks
Content-Type: application/json

{"stocks":["000001.SZ","600000.SH"]}
```

列表订阅采用 miniQMT 的全推接口统一维护。请求会和已有列表合并；去重后的总数超过 300 时返回 HTTP 409，原订阅保持不变。

### 按列表取消订阅

```http
DELETE /v1/subscriptions/stocks
Content-Type: application/json

{"stocks":["000001.SZ"]}
```

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

### 获取订阅数据

不传 `stocks` 时返回当前订阅范围内已经收到的全部最新行情；如果开启了全市场订阅，响应中也会包含对应市场的行情：

```http
POST /v1/quotes/subscribed
Content-Type: application/json

{}
```

也可以只取指定的已订阅合约：

```json
{"stocks":["000001.SZ"]}
```

响应中的 `missing` 表示已订阅但尚未收到行情，`not_subscribed` 表示请求了未订阅的合约。服务只保留每个合约的最新一条行情，不积压无界队列。全市场结果可能较大，实际调用时建议在 `stocks` 中指定所需合约。

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

## 本地开发

Linux 或没有 miniQMT 的机器仍可安装依赖并运行完整测试，因为测试使用假的行情网关：

```bash
uv sync --extra qmt-agent
uv run --extra qmt-agent pytest tests/qmt_agent
uv run ruff check .
```

qmt-agent 的测试按职责隔离：

- `tests/qmt_agent/unit`：业务规则、序列化、网关调用和 Windows 启动器；
- `tests/qmt_agent/integration`：FastAPI 接口和组件组合；
- `tests/qmt_agent/stress`：并发订阅、300 上限竞争、热切换失败和高频行情读写。

可分别运行：

```bash
uv run --extra qmt-agent pytest tests/qmt_agent/unit
uv run --extra qmt-agent pytest tests/qmt_agent/integration
uv run --extra qmt-agent pytest tests/qmt_agent/stress
```

压力测试验证最终状态和并发不变量，不使用依赖机器性能的耗时阈值。

真实服务必须在能导入东北证券客户端 `xtquant` 的 Windows 环境中运行。

miniQMT 接口行为以[迅投 XtData 官方文档](https://dict.thinktrader.net/nativeApi/xtdata.html)为准。
