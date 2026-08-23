# qmt-agent

`qmt_agent` 是东北证券 miniQMT 的薄 HTTP 中转层。它负责调用 XtData、维护订阅和内存缓存、按共享协议返回数据；不解释行情业务，也不修改 XtData 返回值。

## 模块边界

```text
FastAPI api.py
      │ 请求模型 / 响应模型
      ▼
QmtMarketService service.py ── 订阅、最新值缓存、顺序环形缓存
      │
      ▼
XtDataGateway gateway.py ────── XtData 调用与具体结构校验
```

- `gateway.py` 是唯一直接接触 `xtquant.xtdata` 的位置。字典直接校验，DataFrame 逐行转成具体模型。
- `service.py` 维护订阅生命周期、50 个单股订阅上限、最新值缓存和可靠顺序缓存。
- `api.py` 只把 HTTP 请求转给 service，并返回其协议对象。
- `serialization.py` 只处理 pandas/numpy 标量和 DataFrame index，不定义业务结构。
- 所有公共模型位于 `qmt_protocol/base.py`、`requests.py`、`responses.py`。

服务使用单进程内存状态。重启后需要重新订阅，行情序号从 1 重新开始。

## 订阅和缓存

全市场订阅和单股订阅分别缓存，并且每份缓存按实际订阅隔离：

- 每次 XtData 回调原样进入顺序缓存。
- 最新值缓存只对同一回调中的同一代码做覆盖，保留最后一条。
- GET 最新行情返回对应订阅类型的完整缓存，不接受证券列表，也不做代码、市场或时间过滤。
- 取消某个订阅时只删除该订阅自己的缓存，不根据代码后缀猜测归属。
- 同一证券不能同时使用多个单股订阅周期；必须先按原周期取消，再订阅新周期。
- 单股订阅去重后总数最多 50，可通过配置降低上限。

顺序缓存为固定容量环形缓冲区。每条记录带单调递增 `seq`、来源订阅、周期和接收时间。读取始终返回从指定序号开始的完整连续窗口，不按证券筛选；请求已被覆盖的序号时返回 HTTP 416 和当前边界。

## 数据获取

快照、历史、财务和除权接口都是最短路径转发：

1. 请求模型校验输入。
2. service 原参数调用 gateway。
3. gateway 原参数调用 XtData。
4. XtData 结果转为具体协议模型并直接返回。

agent 不做结果集补齐、业务日期转换、代码过滤、行情时间归一化或存储转换。具体结构见 [QMT 数据协议](qmt_protocol.md)。

## HTTP 接口

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/health` | 健康状态及订阅摘要 |
| `GET` | `/v1/subscriptions` | 当前订阅和顺序缓存状态 |
| `POST` | `/v1/subscriptions/markets` | 订阅市场；无请求体时为 SH、SZ |
| `DELETE` | `/v1/subscriptions/markets` | 取消指定市场；无请求体时取消全部 |
| `POST` | `/v1/subscriptions/stocks` | 按证券和周期订阅 |
| `DELETE` | `/v1/subscriptions/stocks` | 按证券和原周期取消订阅 |
| `POST` | `/v1/snapshots/markets` | 转发多个市场的 `get_full_tick` |
| `POST` | `/v1/snapshots/stocks` | 转发证券列表的 `get_full_tick` |
| `GET` | `/v1/quotes/subscribed/markets` | 完整市场订阅最新值缓存 |
| `GET` | `/v1/quotes/subscribed/stocks` | 完整单股订阅最新值缓存 |
| `POST` | `/v1/quotes/subscribed/sequence` | 按序读取订阅行情，可长轮询 |
| `POST` | `/v1/history/download` | 下载本地历史数据 |
| `POST` | `/v1/history/query` | 查询本地历史数据 |
| `POST` | `/v1/financial/download` | 下载本地财务数据 |
| `POST` | `/v1/financial/query` | 查询本地财务数据 |
| `POST` | `/v1/dividend-factors/query` | 查询除权因子 |

旧版两个最新行情 POST 路径暂时保留为隐藏兼容入口；请求体不参与筛选。新调用统一使用 GET。

请求使用严格类型。代码会去除首尾空格并大写，重复请求项会去重；未知字段、错误类型、不支持的枚举、无效日期及倒置时间范围返回 HTTP 422。合约代码形如 `000001.SZ`，市场代码形如 `SH`。

### 常用请求

```http
POST /v1/subscriptions/stocks
Content-Type: application/json

{"stocks":["000001.SZ"],"period":"1m"}
```

```http
POST /v1/quotes/subscribed/sequence
Content-Type: application/json

{"seq":1,"limit":1000,"wait_ms":30000}
```

```http
POST /v1/history/query
Content-Type: application/json

{
  "stocks":["000001.SZ"],
  "fields":[],
  "period":"1d",
  "start_time":"20240101",
  "end_time":"20241231",
  "count":-1,
  "dividend_type":"none",
  "fill_data":true
}
```

`start_time` 和 `end_time` 接受空字符串、`YYYYMMDD` 或 `YYYYMMDDhhmmss`。返回中的原始 XtData `time` 不在 agent 中转换。

## XtData 对应关系

| gateway 操作 | XtData 调用 |
| --- | --- |
| 全市场订阅 | `subscribe_whole_quote` |
| 单股订阅 | `subscribe_quote` |
| 取消订阅 | `unsubscribe_quote` |
| 快照 | `get_full_tick` |
| 历史下载 | `download_history_data2`；兼容旧客户端不含 `incrementally` 的签名 |
| 历史查询 | `get_local_data` |
| 财务下载 | `download_financial_data2` |
| 财务查询 | `get_financial_data` |
| 除权查询 | `get_divid_factors` |

XtData 调用由一个不可重入锁串行保护，避免客户端并发不稳定；行情回调只做校验、追加顺序缓存和更新最新值，异常会记录但不会破坏已有缓存。

## Windows 启动

前提：已安装 [uv](https://docs.astral.sh/uv/)，miniQMT 已安装并能正常登录。默认路径是：

- `bin.x64`：`C:\Program Files\东北证券NET专业版\bin.x64`
- 快捷方式：`%USERPROFILE%\Desktop\东北证券NET专业版.lnk`

双击 `scripts\qmt_agent\start_qmt_agent.cmd`。启动器会校验明确路径、关闭由相同命令启动的旧 agent 和该安装目录下的 miniQMT、启动客户端并等待初始化，然后启动服务。它还会为两个进程清理代理环境并在运行期间阻止系统因空闲自动休眠。

路径变化时显式传参：

```bat
scripts\qmt_agent\start_qmt_agent.cmd ^
  --qmt-bin "C:\Program Files\东北证券NET专业版\bin.x64" ^
  --qmt-shortcut "%USERPROFILE%\Desktop\东北证券NET专业版.lnk"
```

监听和容量配置：

```powershell
$env:QMT_AGENT_HOST = "127.0.0.1"
$env:QMT_AGENT_PORT = "8765"
$env:QMT_AGENT_LOG_LEVEL = "info"
$env:QMT_AGENT_MAX_SUBSCRIPTIONS = "50"
$env:QMT_AGENT_QUOTE_BUFFER_CAPACITY = "10000"
.\scripts\qmt_agent\start_qmt_agent.cmd
```

默认仅监听本机。启动后访问 <http://127.0.0.1:8765/health> 和 <http://127.0.0.1:8765/docs>。启动器不能代替 miniQMT 的人工登录、验证码或行情连接配置。

## 测试

```bash
uv sync --group qmt-agent
uv run --group qmt-agent pytest tests/qmt_agent
```

测试使用假的行情网关，因此没有 miniQMT 的环境也可以运行。真实服务必须运行在能够导入东北证券 `xtquant` 的 Windows 环境中。
