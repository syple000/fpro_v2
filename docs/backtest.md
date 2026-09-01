# 日频 A 股回测

`backtest` 是一个面向当前月度动量研究的日频回测闭环。它使用 `market_data` 的 PIT 查询，按
未复权价格成交和记账，并保留 A 股日频研究真正需要的交易约束。

当前实现刻意不建设通用事件总线、订单生命周期框架、因子平台或依赖注入系统。主要数据流是：

```text
MarketData.session(date)
        ↓
Strategy.on_close(session_data, portfolio_snapshot)
        ↓ 完整目标权重
下一交易日 ExecutionEngine.execute_open(...)
        ↓ 不可变订单结果和成交
Portfolio.apply_fill(...)
        ↓
EquitySnapshot
```

## 运行

```bash
uv run --group backtest backtest-momentum \
  --start 2017-01-01 \
  --end 2026-08-22 \
  --tushare-dir dataset/tushare \
  --qmt-dir dataset/qmt \
  --output-dir runs
```

默认只哈希较小的 Manifest，并记录 Manifest 引用的数据文件路径和大小。需要逐个读取并哈希
全部输入 Parquet 时显式增加：

```bash
--audit-data-hashes
```

## 代码结构

```text
src/backtest/
├── config.py              # 业务配置和运行路径选项
├── types.py               # 订单意图、最终结果、成交、持仓和净值
├── data.py                # PIT 数据加载、按日释放和有限历史
├── strategy.py            # 策略协议和只读账户快照
├── execution.py           # 开盘可交易检查、资金分配、费用与滑点
├── portfolio.py           # 现金、持仓、T+1 和账户不变量
├── corporate_actions.py   # 分红、送转、股份上市和退市核销
├── engine.py              # 单向日频主循环
├── metrics.py             # 收益、风险、换手和交易统计
├── report.py              # 单文件 HTML 报告
├── artifacts.py           # 精简结果产物和可选数据审计
└── runner.py              # 月度动量正式运行入口

src/strategies/
└── momentum.py            # 月度中期横截面动量
```

## 组件边界

### `DataPortal` 与 `SessionData`

`DataPortal` 固定本次运行的数据快照，一次载入日线和公司行动，再按模拟日期释放数据。策略只拿到
绑定当前 session 的 `SessionData`，不能访问 `DataReader` 或尚未释放的日线。

`SessionData` 提供：

- 当前 session 和序号；
- 是否月末；
- 当前 PIT 候选股票池；
- 单股票截至当前时点的有限历史；
- 当前已释放收盘价。

动量计算属于策略，不属于数据基础设施。

### `Strategy`

策略在收盘接收只读数据和账户快照，返回一组完整目标权重：

```python
class Strategy(Protocol):
    strategy_id: str

    def on_close(
        self,
        data: SessionData,
        portfolio: PortfolioView,
    ) -> Mapping[str, float] | None: ...
```

返回 `None` 表示当天不调仓；返回字典表示完整目标组合，未出现的当前持仓目标为零。权重必须有限、
位于 `[0, 1]`，总和不能超过 1。

策略对象自行持有研究状态，不通过可变 Context 或私有命令缓冲区与引擎通信。

### `ExecutionEngine`

执行层读取订单、开盘行情、交易状态、现金和可卖数量，输出 `OrderResult` 与 `Fill`。它不持有或
修改 `Portfolio`，因此执行计算和账户记账保持单向关系。

每个订单只在目标开盘尝试一次。最终状态只有：

- `FILLED`；
- `PARTIALLY_FILLED`；
- `NOT_FILLED`。

未成交部分通过明确原因表达，例如停牌、涨跌停、容量、资金不足或回测结束，不维护瞬时的
`NEW/ACCEPTED/EXPIRED` 状态链。

### `Portfolio`

账户只接受已经算好的成交和公司行动。它维护：

- 现金；
- 总持仓与可卖持仓；
- 待上市红股；
- 分红应收款；
- 成本、已实现盈亏和最后估值价格。

当前同步回测没有并发订单，也没有跨多个开盘存活的订单，因此不维护冻结现金或冻结持仓。

## 每日顺序

### 盘前 09:25

1. 昨日以前买入的股份解除 T+1；
2. 应用除权、派息和红股上市事件；
3. 撤销受公司行动影响的待执行目标；
4. 核销已经退市且没有后续估值的持仓。

### 开盘 09:30

1. 读取待执行证券的停牌和涨跌停状态；
2. 先计算卖出，所得现金可用于同一批买入；
3. 按上一交易日成交量限制容量；
4. 对全部买单按资金比例缩放并按整数手取整；
5. 计算滑点、佣金、印花税和过户费；
6. 账户按确定性顺序应用成交。

### 收盘 16:05

1. 释放当日日线；
2. 用未复权收盘价估值；
3. 捕获公司行动登记日权益；
4. 记录净值；
5. 调用策略并生成下一交易日开盘订单。

信号看见当日收盘后，最早只能在下一交易日开盘成交。

## 股票池和信号

候选池使用当时可见的 `stock_basic` 历史状态，默认要求：

- CNY；
- BSE、SSE 或 SZSE；
- 至少上市 250 个交易日；
- 非 ST；
- 当日存在已释放日线。

默认策略在月末计算过去第 120 至第 20 个交易日之间的总收益。总收益指数通过每日
`close / pre_close` 链接，策略只读取已经释放的历史。

## A 股交易规则

当前实现保留：

- 股票买入后下一交易日才可卖；
- 买入按 100 股整数手，清仓允许卖出零股尾数；
- 停牌禁止成交；
- 开盘触及涨停不买、触及跌停不卖；
- 默认滑点 5 bps，并按价格最小单位取整；
- 默认单只股票不超过上一交易日成交量的 10%；
- 卖出先于买入，买单资金不足时按比例缩放；
- 佣金、印花税和过户费按成交日期计算。

## 公司行动

账户使用未复权价格，因此显式处理：

- 登记日权益快照；
- 除息日确认应收股利；
- 派息日应收转现金；
- 送转股份增加总持仓；
- 红股上市后转为可卖；
- 退市且无法继续估值时核销为零。

不能同时使用前复权价格计算账户收益并再次增加分红或股份。配股、换股、吸收合并等没有完整
数据和明确记账规则的事件仍不在当前能力内。

## 配置

`BacktestConfig` 只包含会影响结果的参数：日期、初始资金、年化口径、费用、执行、股票池和公司
行动规则。

`RunOptions` 只包含基础设施选项：Tushare/QMT 路径、输出路径和是否全量哈希数据。改变输出目录
不会改变确定性 run ID。

## 结果产物

默认运行生成：

```text
runs/<run_id>/
├── config.json
├── run_options.json
├── environment.json
├── data_snapshot.json
├── strategy.json
├── metrics.json
├── orders.parquet
├── fills.parquet
├── corporate_actions.parquet
├── equity.parquet
└── report.html
```

默认不再生成通用事件流水、每次订单瞬时状态和每日逐股票持仓快照。订单文件直接保存一次开盘尝试
后的最终结果。

Manifest 模式能够检测 Manifest 变化，但不承诺在源数据删除后恢复历史输入。审计模式额外保存
每个活动数据文件的 SHA-256，同样只验证内容，不复制或保留源文件。

## 当前限制

- 只支持日频开盘执行；
- 只支持现金多头账户；
- 没有指数行情，因此不计算基准与超额收益；
- 日线无法模拟盘口排队和盘中成交路径；
- 2017 年以前的上市交易日龄按自然日近似换算；
- 未覆盖的数据源外公司行动无法自动发现。

## 验证重点

测试至少覆盖：

- 收盘信号只能下一交易日开盘成交；
- T+1；
- 停牌和涨跌停拒绝原因；
- 执行计算不会直接修改账户；
- 费用政策生效日；
- 分红应收、派息和红股上市分离；
- 无效风险指标分母返回 `None`；
- 账户不变量始终成立。
