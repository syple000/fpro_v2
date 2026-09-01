"""无外部前端依赖的单文件 HTML 报告。"""

from __future__ import annotations

import html
from collections.abc import Mapping, Sequence
from typing import Any

from backtest.engine import BacktestResult


def _percent(value: Any) -> str:
    return "—" if value is None else f"{float(value):.2%}"


def _number(value: Any, digits: int = 2) -> str:
    return "—" if value is None else f"{float(value):,.{digits}f}"


def _line_chart(values: Sequence[float], *, width: int = 1000, height: int = 300) -> str:
    if not values:
        return ""
    sample_step = max(len(values) // width, 1)
    sampled = list(values[::sample_step])
    if sampled[-1] != values[-1]:
        sampled.append(values[-1])
    low, high = min(sampled), max(sampled)
    span = high - low or 1.0
    points = []
    for index, value in enumerate(sampled):
        x = index / max(len(sampled) - 1, 1) * width
        y = height - (value - low) / span * (height - 20) - 10
        points.append(f"{x:.1f},{y:.1f}")
    return (
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="净值曲线">'
        f'<polyline points="{" ".join(points)}" fill="none" stroke="#2563eb" '
        'stroke-width="2" vector-effect="non-scaling-stroke" />'
        f'<text x="8" y="20">最高 {_number(high)}</text>'
        f'<text x="8" y="{height - 8}">最低 {_number(low)}</text>'
        "</svg>"
    )


def _metric_card(label: str, value: str) -> str:
    return f'<div class="card"><span>{html.escape(label)}</span><strong>{value}</strong></div>'


def render_report(
    *,
    result: BacktestResult,
    metrics: Mapping[str, Any],
    strategy: Mapping[str, Any],
) -> str:
    """生成包含净值、风险、年度收益和限制说明的中文报告。"""

    cards = "".join(
        (
            _metric_card("累计收益", _percent(metrics["total_return"])),
            _metric_card("年化收益", _percent(metrics["annualized_return"])),
            _metric_card("最大回撤", _percent(metrics["max_drawdown"])),
            _metric_card("Sharpe", _number(metrics["sharpe"])),
            _metric_card("年化波动", _percent(metrics["annualized_volatility"])),
            _metric_card("换手率", _percent(metrics["turnover"])),
            _metric_card("期末权益", _number(metrics["final_equity"])),
            _metric_card("交易费用", _number(metrics["total_fees"])),
        )
    )
    yearly_rows = "".join(
        f"<tr><td>{html.escape(year)}</td><td>{_percent(value)}</td></tr>"
        for year, value in metrics["yearly_returns"].items()
    )
    blocked_rows = (
        "".join(
            f"<tr><td>{html.escape(reason)}</td><td>{count}</td></tr>"
            for reason, count in metrics["blocked_order_reasons"].items()
        )
        or '<tr><td colspan="2">无</td></tr>'
    )
    warnings = "".join(f"<li>{html.escape(item)}</li>" for item in result.warnings)
    strategy_rows = "".join(
        f"<tr><td>{html.escape(str(key))}</td><td>{html.escape(str(value))}</td></tr>"
        for key, value in strategy.items()
        if key != "rebalance_log"
    )
    chart = _line_chart([item.total_equity for item in result.equity])
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>回测报告 · {html.escape(result.run_id)}</title>
<style>
:root {{ color-scheme: light; font-family: system-ui, sans-serif; color: #172033; }}
body {{ margin: 0 auto; max-width: 1180px; padding: 28px; background: #f6f8fb; }}
h1, h2 {{ margin-top: 1.4em; }}
.subtle {{ color: #64748b; }}
.cards {{ display: grid; grid-template-columns: repeat(auto-fit,minmax(160px,1fr)); gap: 12px; }}
.card, section {{ background: white; border: 1px solid #e2e8f0;
border-radius: 10px; padding: 16px; }}
.card span {{ display: block; color: #64748b; font-size: 13px; }}
.card strong {{ display: block; margin-top: 7px; font-size: 23px; }}
section {{ margin-top: 16px; }}
svg {{ width: 100%; height: auto; background: linear-gradient(#fff,#f8fafc); }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border-bottom: 1px solid #e2e8f0; padding: 8px; text-align: left; }}
.columns {{ display: grid; grid-template-columns: repeat(auto-fit,minmax(300px,1fr)); gap: 16px; }}
code {{ word-break: break-all; }}
</style>
</head>
<body>
<h1>月度中期动量回测报告</h1>
<p class="subtle">运行 ID：<code>{html.escape(result.run_id)}</code> ·
{metrics["start_session"]} 至 {metrics["end_session"]} · {metrics["session_count"]} 个交易日</p>
<div class="cards">{cards}</div>
<section><h2>账户净值</h2>{chart}</section>
<div class="columns">
<section><h2>年度收益</h2><table><tr><th>年份</th><th>收益</th></tr>{yearly_rows}</table></section>
<section><h2>未成交/拒单</h2><table><tr><th>原因</th><th>数量</th></tr>{blocked_rows}</table></section>
</div>
<div class="columns">
<section><h2>交易与账户</h2><table>
<tr><td>订单数</td><td>{metrics["order_count"]}</td></tr>
<tr><td>成交数</td><td>{metrics["trade_count"]}</td></tr>
<tr><td>佣金</td><td>{_number(metrics["commission"])}</td></tr>
<tr><td>印花税</td><td>{_number(metrics["stamp_tax"])}</td></tr>
<tr><td>过户费</td><td>{_number(metrics["transfer_fee"])}</td></tr>
<tr><td>滑点成本</td><td>{_number(metrics["slippage_cost"])}</td></tr>
<tr><td>平均持仓数</td><td>{_number(metrics["average_holding_count"])}</td></tr>
<tr><td>平均资金利用率</td><td>{_percent(metrics["average_capital_utilization"])}</td></tr>
<tr><td>stale 持仓日占比</td><td>{_percent(metrics["stale_position_day_ratio"])}</td></tr>
</table></section>
<section><h2>策略参数</h2><table>{strategy_rows}</table></section>
</div>
<section><h2>口径与限制</h2>
<p>信号在月末 16:05 使用当时已释放的日线生成，订单最早下一交易日 09:30 撮合。
动量收益通过每日 <code>close / pre_close</code> 链接为 PIT 总收益指数；
账户成交和估值始终使用未复权价格。</p>
<p>最大回撤区间：{metrics["drawdown_start"]} → {metrics["drawdown_trough"]}；
恢复日：{metrics["drawdown_recovery"] or "尚未恢复"}。</p>
<ul>{warnings}</ul>
</section>
</body>
</html>
"""
