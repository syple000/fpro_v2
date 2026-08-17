# fpro_v2 量化系统

这是一个模块化量化系统。各模块独立运行、独立维护边界，根项目只负责统一的 Python 版本、依赖锁定和工程规范。

## 当前模块

- `qmt_agent`：东北证券 miniQMT 行情接入服务，代码位于 `src/qmt_agent`。
- `qmt_receiver`：供 platform 调用的行情接收、Parquet 落盘和队列投递组件，代码位于
  `src/qmt_receiver`。
- `parquet_store`：本地、单进程的通用不可变 Parquet 存储，代码位于 `src/parquet_store`。

qmt-agent 的接口、Windows 启动方式和开发说明见 [docs/qmt_agent.md](docs/qmt_agent.md)。
WSL 实时接收组件的调用和测试说明见 [docs/qmt_receiver.md](docs/qmt_receiver.md)。
两端共用的响应、行情字段和队列事件类型见
[docs/qmt_protocol.md](docs/qmt_protocol.md)。
Parquet 存储的接口和最简示例见 [docs/parquet_store.md](docs/parquet_store.md)。

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
