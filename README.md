# fpro_v2 量化系统

这是一个模块化量化系统。各模块独立运行、独立维护边界，根项目只负责统一的 Python 版本、依赖锁定和工程规范。

## 当前模块

- `fpro_common`：全项目共用的微秒时间戳转换、北京时间日志格式等少量基础规则。
- `data`：基于 DuckDB 的统一读取层，由 `DataCatalog` 管理物理快照、`DataReader` 提供 PIT 查询。
- `data_validation`：从 Tushare 随机抽股，拉取 QMT 日线、财务和除权数据并交叉复核。
- `qmt_agent`：东北证券 miniQMT 行情接入服务，代码位于 `src/qmt_agent`。
- `qmt_receiver`：供 platform 调用的实时接收、下载同步和 Parquet 存储组件，代码位于
  `src/qmt_receiver`。
- `parquet_store`：本地、单进程的通用不可变 Parquet 存储，代码位于 `src/parquet_store`。
- `tushare_data`：通过 quicksync/Tushare 增量拉取股票主数据、日线、财报、分红复权、申万行业和停复牌，
  代码位于 `src/tushare_data`。

qmt-agent 的接口、Windows 启动方式和开发说明见 [docs/qmt_agent.md](docs/qmt_agent.md)。
WSL 实时接收组件的调用和测试说明见 [docs/qmt_receiver.md](docs/qmt_receiver.md)。
两端共用的响应、行情字段和队列事件类型见
[docs/qmt_protocol.md](docs/qmt_protocol.md)。
Parquet 存储的接口和最简示例见 [docs/parquet_store.md](docs/parquet_store.md)。
Tushare 历史数据字段、增量规则和验证方式见 [docs/tushare_data.md](docs/tushare_data.md)。
统一 PIT Reader 见 [docs/data.md](docs/data.md)。
跨源随机抽样复核见 [docs/data_validation.md](docs/data_validation.md)。
回测系统的架构、业务规则、风险点和实施顺序见 [docs/backtest.md](docs/backtest.md)。

## 时间规范

所有表示时间瞬间的业务字段统一使用 Unix Epoch 微秒整数：Python/JSON 为 `int`，Arrow 为
`int64`。整数本身不携带时区，语义固定为从 `1970-01-01T00:00:00Z` 起经过的微秒。交易日、
报告期和公告日等纯日历标签仍使用 `date` / `date32`。只有计算中国市场数据分区和打印日志
记录时间时转换到北京时间；超时、限流和耗时使用 monotonic clock。

## 目录约定

```text
src/<module>/                  模块实现
scripts/<module>/              模块启动及运维脚本
docs/<module>.md               模块文档
tests/<module>/unit/           纯单元测试
tests/<module>/integration/    模块接口与组件集成测试
tests/<module>/stress/         并发和压力下的正确性测试
```

新增模块时应沿用这个边界，不要把模块专属脚本或测试继续堆到项目根目录。
更具体的测试放置规则见 [tests/README.md](tests/README.md)。
