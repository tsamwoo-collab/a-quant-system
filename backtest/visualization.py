"""
回测可视化模块
"""
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def render_equity_curve(equity_df: pd.DataFrame, baseline_df: pd.DataFrame = None):
    """绘制净值曲线"""
    fig = go.Figure()

    # 策略净值
    fig.add_trace(go.Scatter(
        x=equity_df.index,
        y=equity_df['total_equity'],
        name='策略净值',
        line=dict(color='#1f77b4', width=2)
    ))

    # 基准对比
    if baseline_df is not None:
        fig.add_trace(go.Scatter(
            x=baseline_df.index,
            y=baseline_df['total_equity'],
            name='基准净值',
            line=dict(color='#ff7f0e', width=1, dash='dash')
        ))

    fig.update_layout(
        title='📈 净值曲线',
        xaxis_title='日期',
        yaxis_title='净值',
        hovermode='x unified',
        height=400
    )

    return fig


def render_drawdown_chart(equity_df: pd.DataFrame):
    """绘制回撤图"""
    equity_df = equity_df.copy()
    equity_df['cummax'] = equity_df['total_equity'].cummax()
    equity_df['drawdown'] = (equity_df['total_equity'] - equity_df['cummax']) / equity_df['cummax'] * 100

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=equity_df.index,
        y=equity_df['drawdown'],
        fill='tozeroy',
        fillcolor='rgba(255, 0, 0, 0.3)',
        line=dict(color='red', width=1),
        name='回撤'
    ))

    fig.update_layout(
        title='📉 回撤分析',
        xaxis_title='日期',
        yaxis_title='回撤 (%)',
        height=300
    )

    return fig


def render_monthly_returns(returns_series: pd.Series):
    """绘制月度收益热力图"""
    # 计算月度收益
    monthly = returns_series.resample('M').apply(lambda x: (1 + x).prod() - 1)

    monthly_df = monthly.to_frame('returns')
    monthly_df['year'] = monthly_df.index.year
    monthly_df['month'] = monthly_df.index.month

    pivot = monthly_df.pivot(index='year', columns='month', values='returns')

    # 转换为百分比
    pivot_pct = pivot * 100

    month_names = ['1月', '2月', '3月', '4月', '5月', '6月',
                   '7月', '8月', '9月', '10月', '11月', '12月']

    fig = go.Figure(data=go.Heatmap(
        z=pivot_pct.values,
        x=month_names[:len(pivot_pct.columns)],
        y=pivot_pct.index.astype(str),
        colorscale='RdYlGn',
        text=pivot_pct.round(2).values,
        texttemplate='%{text:.2f}%',
        textfont={"size": 10},
        colorbar=dict(title='收益率 (%)')
    ))

    fig.update_layout(
        title='📊 月度收益热力图',
        xaxis_title='月份',
        yaxis_title='年份',
        height=400
    )

    return fig


def render_trade_analysis(trades_df: pd.DataFrame):
    """绘制交易分析图"""
    if trades_df.empty:
        return None

    sell_trades = trades_df[trades_df['action'] == '卖出'].copy()

    if sell_trades.empty:
        return None

    # 盈亏分布
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('盈亏分布', '累计盈亏'),
        specs=[[{'type': 'histogram'}, {'type': 'scatter'}]]
    )

    # 盈亏直方图
    fig.add_trace(
        go.Histogram(
            x=sell_trades['pnl_pct'],
            nbinsx=20,
            marker_color='green',
            name='盈亏分布'
        ),
        row=1, col=1
    )

    # 累计盈亏曲线
    sell_trades = sell_trades.sort_values('date')
    sell_trades['cumulative_pnl'] = sell_trades['pnl'].cumsum()

    fig.add_trace(
        go.Scatter(
            x=sell_trades['date'],
            y=sell_trades['cumulative_pnl'],
            mode='lines+markers',
            name='累计盈亏',
            line=dict(color='blue')
        ),
        row=1, col=2
    )

    fig.update_layout(
        title='📋 交易分析',
        height=350,
        showlegend=False
    )

    return fig


def render_metrics_cards(metrics: dict):
    """生成性能指标卡片数据"""
    cards = []

    cards.append({
        'title': '💰 总收益',
        'value': f"{metrics.get('total_return', 0):.2f}%",
        'color': 'green' if metrics.get('total_return', 0) > 0 else 'red'
    })

    cards.append({
        'title': '📉 最大回撤',
        'value': f"{metrics.get('max_drawdown', 0):.2f}%",
        'color': 'orange' if metrics.get('max_drawdown', 0) > -10 else 'red'
    })

    cards.append({
        'title': '📊 夏普比率',
        'value': f"{metrics.get('sharpe_ratio', 0):.2f}",
        'color': 'green' if metrics.get('sharpe_ratio', 0) > 1 else 'orange'
    })

    cards.append({
        'title': '🎯 胜率',
        'value': f"{metrics.get('win_rate', 0):.1f}%",
        'color': 'green' if metrics.get('win_rate', 0) > 50 else 'orange'
    })

    cards.append({
        'title': '📈 交易次数',
        'value': f"{metrics.get('total_trades', 0)}",
        'color': 'blue'
    })

    cards.append({
        'title': '💎 盈亏比',
        'value': f"{metrics.get('profit_loss_ratio', 0):.2f}",
        'color': 'green' if metrics.get('profit_loss_ratio', 0) > 1.5 else 'orange'
    })

    return cards
