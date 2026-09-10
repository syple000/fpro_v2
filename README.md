# fpro_v2 量化系统

这是一个模块化量化系统。各模块独立运行、独立维护边界，根项目只负责统一的 Python 版本、依赖锁定和工程规范。

## 当前模块

- `fpro_common`：全项目共用的微秒时间戳转换、北京时间日志格式等少量基础规则。
- `market_data`：基于 DuckDB 的统一读取层，由 `DataCatalog` 管理物理快照、`DataReader` 提供 PIT 查询。
- `data_crosscheck`：Tushare/QMT 数据交叉检查；抽样比较日线、财务和除权数据，
  只报告差异，不代替全量清洗和发布门禁。
- `data_cleaning`：Tushare 离线数据的全量检测、定向重拉修复、人工决策和版本发布。
- `backtest`：入门级日频 A 股回测，覆盖 PIT 股票池、次日开盘成交、T+1、费用和公司行动。
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
统一 PIT Reader 见 [docs/market_data.md](docs/market_data.md)。
Tushare/QMT 数据交叉检查见 [docs/data_crosscheck.md](docs/data_crosscheck.md)。
Tushare 离线数据清洗和发布见 [docs/data_cleaning.md](docs/data_cleaning.md)。
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

## Python 类型检查

以项目根目录打开 VS Code，使用项目 `.venv` 中的 Python 3.11。工作区配置让 Pylance 检查整个
项目；`pyproject.toml` 统一登记 `src` 导入路径、`standard` 检查规则，以及源码、脚本、测试和示例的
检查范围。命令行 Pyright 也会使用 `.venv`，无需依赖终端是否激活虚拟环境：

```bash
.venv/bin/pyright
```

工作区默认解释器路径适用于 Linux / WSL；Windows 中选择 `.venv\Scripts\python.exe`，
命令行使用 `.venv\Scripts\pyright.exe`。开发环境需要安装 `dev` 和所开发模块的依赖组；
`dev` 包含 `pyarrow-stubs`，用于补充 Arrow 的类型信息。

如果工作区曾选择其他解释器，新增的默认路径不会替换已保存的选择；执行
`Python: Select Interpreter` 选择项目 `.venv`，再执行 `Python: Restart Language Server` 刷新诊断。
这是 [VS Code 默认解释器设置](https://code.visualstudio.com/docs/python/settings-reference#_general-python-settings)
的既定行为。Pylance 使用编辑器选择的解释器，命令行 Pyright 使用 `venvPath` / `venv`，两者应指向
同一环境；不要通过关闭缺失导入或类型错误诊断来处理环境问题。
