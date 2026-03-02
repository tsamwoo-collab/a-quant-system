"""
Streamlit Dashboard - A股量化信号系统 (含交易闭环)
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# 导入追踪模块
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from tracking import get_tracker

# 页面配置
st.set_page_config(
    page_title="A股量化信号系统",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 自定义CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
    .signal-buy { color: #00c853; font-weight: bold; font-size: 1.1rem; }
    .signal-sell { color: #d50000; font-weight: bold; font-size: 1.1rem; }
    .signal-hold { color: #ffab00; font-size: 1.1rem; }
    .action-buy { background-color: #c8e6c9 !important; }
    .action-sell { background-color: #ffcdd2 !important; }
    .action-add { background-color: #b2dfdb !important; }
    .action-reduce { background-color: #ffe0b2 !important; }
    .priority-high { border-left: 4px solid #d50000; }
    .priority-medium { border-left: 4px solid #ff9800; }
    .priority-low { border-left: 4px solid #4caf50; }
</style>
""", unsafe_allow_html=True)


def generate_mock_signals(num_stocks=50):
    """生成模拟信号数据"""
    np.random.seed(int(datetime.now().timestamp()) % 10000)

    stock_names = [
        ('000001', '平安银行'), ('000002', '万科A'), ('600036', '招商银行'),
        ('600519', '贵州茅台'), ('000858', '五粮液'), ('600000', '浦发银行'),
        ('601318', '中国平安'), ('000333', '美的集团'), ('002594', '比亚迪'),
        ('600276', '恒瑞医药'), ('000651', '格力电器'), ('600887', '伊利股份'),
        ('601012', '隆基绿能'), ('300750', '宁德时代'), ('688981', '中芯国际'),
        ('600030', '中信证券'), ('601166', '兴业银行'), ('000725', '京东方A'),
        ('002415', '海康威视'), ('600031', '三一重工'), ('601888', '中国中免'),
        ('002142', '宁波银行'), ('601390', '中国中铁'), ('601668', '中国建筑'),
        ('601398', '工商银行'), ('601288', '农业银行'), ('601939', '建设银行'),
        ('600016', '民生银行'), ('000063', '中兴通讯'), ('002304', '洋河股份'),
        ('600585', '海螺水泥'), ('600089', '特变电工'), ('601899', '紫金矿业'),
        ('600900', '长江电力'), ('000876', '新希望'), ('600745', '闻泰科技'),
        ('000100', 'TCL科技'), ('002027', '分众传媒'), ('603259', '药明康德'),
        ('600809', '山西汾酒'), ('000568', '泸州老窖'), ('002352', '顺丰控股'),
        ('300059', '东方财富'), ('000166', '申万宏源'), ('600104', '上汽集团'),
        ('000776', '广发证券'), ('002475', '立讯精密'), ('601018', '宁波港'),
        ('600048', '保利发展'), ('000069', '华侨城A'), ('002459', '晶澳科技')
    ]

    data = []
    for i in range(min(num_stocks, len(stock_names))):
        symbol, name = stock_names[i]

        # 生成信号（模拟变化）
        base_signal = np.random.randn() * 0.6
        signal_change = np.random.randn() * 0.3
        signal = np.clip(base_signal + signal_change, -1, 1)

        # 确定信号等级
        if signal >= 0.7:
            level = '强买入'
        elif signal >= 0.3:
            level = '买入'
        elif signal >= -0.3:
            level = '持有'
        elif signal >= -0.7:
            level = '卖出'
        else:
            level = '强卖出'

        # 生成各层级得分
        macro_score = np.clip(np.random.randn() * 0.3, -1, 1)
        meso_score = np.clip(np.random.randn() * 0.4, -1, 1)
        micro_score = np.clip(np.random.randn() * 0.5, -1, 1)

        # 生成因子值
        factors = {
            'shibor_on': np.random.randn() * 0.5,
            'north_flow_net': np.random.randn() * 0.6,
            'market_volume_ratio': np.random.randn() * 0.4,
            'industry_relative_strength': np.random.randn() * 0.5,
            'industry_rank': np.random.randn() * 0.4,
            'turnover_extreme': np.random.randn() * 0.3,
            'momentum_20d': np.random.randn() * 0.5,
            'bias': np.random.randn() * 0.4,
        }

        data.append({
            'symbol': symbol,
            'name': name,
            'signal': signal,
            'signal_level': level,
            'macro_score': macro_score,
            'meso_score': meso_score,
            'micro_score': micro_score,
            'factors': factors
        })

    df = pd.DataFrame(data)
    df = df.sort_values('signal', ascending=False).reset_index(drop=True)
    return df


def render_sidebar():
    """渲染侧边栏"""
    st.sidebar.title("⚙️ 设置")

    # 股票池选择
    stock_pool = st.sidebar.radio(
        "股票池",
        ["沪深300", "中证500", "自定义"],
        horizontal=True
    )

    # 股票数量
    num_stocks = st.sidebar.slider("分析股票数量", 10, 100, 50)

    # 信号阈值
    col1, col2 = st.sidebar.columns(2)
    with col1:
        buy_threshold = st.slider("买入阈值", 0.0, 1.0, 0.3, 0.1)
    with col2:
        sell_threshold = st.slider("卖出阈值", -1.0, 0.0, -0.3, 0.1)

    # 交易设置
    st.sidebar.subheader("🔄 交易闭环")
    enable_tracking = st.sidebar.checkbox("启用持仓追踪", value=True)
    simulate_change = st.sidebar.checkbox("模拟信号变化", value=True,
                                         help="模拟每天信号变化以测试交易信号")

    # 因子权重
    st.sidebar.subheader("因子权重")
    macro_weight = st.slider("宏观因子", 0.0, 1.0, 0.2, 0.1)
    meso_weight = st.slider("中观因子", 0.0, 1.0, 0.3, 0.1)
    micro_weight = round(1.0 - macro_weight - meso_weight, 1)
    st.sidebar.caption(f"微观因子: {micro_weight}")

    return {
        'stock_pool': stock_pool,
        'num_stocks': num_stocks,
        'buy_threshold': buy_threshold,
        'sell_threshold': sell_threshold,
        'enable_tracking': enable_tracking,
        'simulate_change': simulate_change,
        'weights': {'macro': macro_weight, 'meso': meso_weight, 'micro': micro_weight}
    }


def render_trade_panel(tracker, settings):
    """渲染交易面板"""
    st.subheader("📋 今日交易信号")

    # 获取汇总
    summary = tracker.get_daily_summary()

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("当前持仓", summary['total_positions'], delta_color="normal")
    with col2:
        st.metric("买入信号", summary['buy_signals'], delta_color="normal")
    with col3:
        st.metric("加仓信号", summary['add_signals'])
    with col4:
        st.metric("减仓信号", summary['reduce_signals'])
    with col5:
        st.metric("清仓信号", summary['sell_signals'], delta_color="inverse")

    st.divider()

    # 交易信号列表
    signals_df = tracker.get_trade_signals()

    if signals_df.empty:
        st.info("📊 暂无交易信号。点击下方「刷新数据」模拟新的一天数据，生成交易信号。")
        return

    # 按动作分类显示
    buy_df = signals_df[signals_df['action'].isin(['买入', '加仓'])]
    sell_df = signals_df[signals_df['action'].isin(['卖出', '清仓', '减仓'])]

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 🟢 买入信号")
        if not buy_df.empty:
            for _, row in buy_df.iterrows():
                priority_class = f"priority-{'high' if row['priority'] >= 3 else 'medium' if row['priority'] >= 2 else 'low'}"
                attribution = row.get('attribution', {})
                attribution_html = render_attribution(attribution)

                st.markdown(f"""
                <div class="action-buy {priority_class}" style="padding: 12px; margin: 8px 0; border-radius: 8px;">
                    <div style="font-size: 1.1em; margin-bottom: 8px;">
                        <strong>{row['action']}</strong> <strong>{row['symbol']}</strong> {row['name']}
                    </div>
                    <div style="margin-bottom: 6px;">
                        <strong>信号:</strong> {row['prev_signal']:.2f} → <strong>{row['current_signal']:.2f}</strong>
                    </div>
                    <div style="margin-bottom: 6px;">
                        <small>{row['reason']}</small>
                    </div>
                    {attribution_html}
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("暂无买入信号")

    with col2:
        st.markdown("### 🔴 卖出信号")
        if not sell_df.empty:
            for _, row in sell_df.iterrows():
                priority_class = f"priority-{'high' if row['priority'] >= 3 else 'medium' if row['priority'] >= 2 else 'low'}"
                bg_class = "action-sell" if row['action'] in ['卖出', '清仓'] else "action-reduce"
                attribution = row.get('attribution', {})
                attribution_html = render_attribution(attribution)

                st.markdown(f"""
                <div class="{bg_class} {priority_class}" style="padding: 12px; margin: 8px 0; border-radius: 8px;">
                    <div style="font-size: 1.1em; margin-bottom: 8px;">
                        <strong>{row['action']}</strong> <strong>{row['symbol']}</strong> {row['name']}
                    </div>
                    <div style="margin-bottom: 6px;">
                        <strong>信号:</strong> {row['prev_signal']:.2f} → <strong>{row['current_signal']:.2f}</strong>
                    </div>
                    <div style="margin-bottom: 6px;">
                        <small>{row['reason']}</small>
                    </div>
                    {attribution_html}
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("暂无卖出信号")


def render_attribution(attribution: dict) -> str:
    """渲染归因分析"""
    if not attribution or 'display_items' not in attribution:
        return ""

    items = attribution['display_items']

    html_parts = ["<div style='margin-top: 10px; padding: 8px; background: rgba(255,255,255,0.5); border-radius: 5px;'>"]
    html_parts.append("<div style='font-size: 0.85em; margin-bottom: 6px;'><strong>📊 动能归因:</strong></div>")

    for item in items:
        # 箭头符号
        if item['change'] > 0:
            arrow = "➡️"
            change_str = f"+{item['change']:.2f}"
        elif item['change'] < 0:
            arrow = "➡️"
            change_str = f"{item['change']:.2f}"
        else:
            arrow = "➡️"
            change_str = "0.00"

        # 高亮主驱动力
        if item['is_driver']:
            item_html = f"""
            <div style="margin-bottom: 4px;">
                {item['icon']} <strong>{item['name']}</strong>:
                {item['prev_score']:.2f} {arrow} {item['current_score']:.2f}
                <span style="color: {'green' if item['change'] > 0 else 'red'}">({change_str})</span>
                <span style="font-weight: bold; color: {'green' if item['change'] > 0 else 'red'}">({item['status']})</span>
            </div>
            """
        else:
            item_html = f"""
            <div style="margin-bottom: 4px; opacity: 0.8;">
                {item['icon']} {item['name']}:
                {item['prev_score']:.2f} {arrow} {item['current_score']:.2f}
                <span style="color: {'green' if item['change'] > 0 else 'red'}">({change_str})</span>
                <span>({item['status']})</span>
            </div>
            """

        html_parts.append(item_html)

    html_parts.append("</div>")
    return ''.join(html_parts)


def render_positions(tracker):
    """渲染持仓列表"""
    st.subheader("💼 当前持仓")

    positions_df = tracker.get_positions()

    if positions_df.empty:
        st.info("暂无持仓")
        return

    # 计算盈亏（模拟）
    positions_df['pnl'] = np.random.uniform(-0.1, 0.15, len(positions_df))
    positions_df['pnl_pct'] = (positions_df['pnl'] * 100).round(2)

    # 显示
    display_df = positions_df[['symbol', 'name', 'entry_signal', 'current_signal', 'pnl_pct']]
    display_df.columns = ['代码', '名称', '买入信号', '当前信号', '盈亏%']

    # 盈亏颜色
    def color_pnl(val):
        if val > 0:
            return 'color: #00c853'
        elif val < 0:
            return 'color: #d50000'
        return ''

    styled_df = display_df.style.applymap(color_pnl, subset=['盈亏%'])
    st.dataframe(styled_df, use_container_width=True)


def render_signal_changes(tracker):
    """渲染信号变化"""
    st.subheader("📊 信号变化榜")

    if 'prev_signals_df' not in st.session_state:
        st.info("暂无历史数据对比")
        return

    changes_df = tracker.get_signal_changes(st.session_state.signals_df)

    if changes_df.empty:
        st.info("暂无变化数据")
        return

    # 显示变化最大的前10名
    top_rises = changes_df.head(10)
    top_falls = changes_df.tail(10)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 🔼 信号上升 Top 10")
        for _, row in top_rises.iterrows():
            st.markdown(f"""
            **{row['symbol']} {row['name']}**
            <small>{row['prev_signal']:.2f} → {row['current_signal']:.2f}
            (<span style="color:green">+{row['change']:.2f}</span>)</small>
            """, unsafe_allow_html=True)

    with col2:
        st.markdown("#### 🔽 信号下降 Top 10")
        for _, row in top_falls.iterrows():
            st.markdown(f"""
            **{row['symbol']} {row['name']}**
            <small>{row['prev_signal']:.2f} → {row['current_signal']:.2f}
            (<span style="color:red">{row['change']:.2f}</span>)</small>
            """, unsafe_allow_html=True)


def render_signal_overview(signals_df, settings):
    """渲染信号总览"""
    buy_threshold = settings['buy_threshold']
    sell_threshold = settings['sell_threshold']

    # 统计信息
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        strong_buy = (signals_df['signal'] >= 0.7).sum()
        st.metric("强买入", strong_buy)
    with col2:
        buy = ((signals_df['signal'] >= buy_threshold) & (signals_df['signal'] < 0.7)).sum()
        st.metric("买入", buy)
    with col3:
        hold = ((signals_df['signal'] < buy_threshold) & (signals_df['signal'] >= sell_threshold)).sum()
        st.metric("持有", hold)
    with col4:
        sell = ((signals_df['signal'] < sell_threshold) & (signals_df['signal'] >= -0.7)).sum()
        st.metric("卖出", sell)
    with col5:
        strong_sell = (signals_df['signal'] < -0.7).sum()
        st.metric("强卖出", strong_sell)

    st.divider()

    # 可视化
    col1, col2 = st.columns(2)

    with col1:
        fig = px.histogram(
            signals_df,
            x='signal',
            nbins=30,
            title='信号分布',
            labels={'signal': '信号值'},
            color_discrete_sequence=['#1f77b4']
        )
        fig.add_vline(x=buy_threshold, line_dash="dash", line_color="green")
        fig.add_vline(x=sell_threshold, line_dash="dash", line_color="red")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        level_counts = signals_df['signal_level'].value_counts()
        fig = px.pie(
            values=level_counts.values,
            names=level_counts.index,
            title='信号等级分布',
            color=level_counts.index,
            color_discrete_map={
                '强买入': '#00c853',
                '买入': '#64dd17',
                '持有': '#ffab00',
                '卖出': '#ff6d00',
                '强卖出': '#d50000'
            }
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # 信号表格
    st.subheader("📋 完整信号列表")
    display_df = signals_df[['symbol', 'name', 'signal', 'signal_level',
                             'macro_score', 'meso_score', 'micro_score']].copy()
    display_df['signal'] = display_df['signal'].round(3)
    st.dataframe(display_df, use_container_width=True, height=300)


def render_stock_detail(signals_df):
    """渲染个股分析"""
    st.subheader("🔍 个股详情")

    stock_options = [f"{row['symbol']} - {row['name']}"
                     for _, row in signals_df.iterrows()]
    selected = st.selectbox("选择股票", stock_options)

    if selected:
        symbol = selected.split(' - ')[0]
        stock_data = signals_df[signals_df['symbol'] == symbol].iloc[0]

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("综合信号", f"{stock_data['signal']:.3f}")
        with col2:
            st.markdown(f"<p class='signal-{stock_data['signal_level'].lower().replace('强', 'strong-').replace('买', '-buy').replace('卖', '-sell')}'>{stock_data['signal_level']}</p>",
                       unsafe_allow_html=True)
        with col3:
            st.metric("宏观得分", f"{stock_data['macro_score']:.3f}")
        with col4:
            st.metric("中观得分", f"{stock_data['meso_score']:.3f}")

        st.divider()

        # 因子雷达图
        st.subheader("因子雷达图")
        factors = stock_data['factors']
        if factors:
            categories = list(factors.keys())
            values = list(factors.values())
            display_values = [np.clip(v, -1, 1) for v in values]

            fig = go.Figure(data=go.Scatterpolar(
                r=display_values,
                theta=categories,
                fill='toself',
                marker_color='#1f77b4'
            ))
            fig.update_layout(
                polar=dict(radialaxis=dict(visible=True, range=[-1, 1])),
                showlegend=False,
                title="因子值雷达图"
            )
            st.plotly_chart(fig, use_container_width=True)


# ============= 主程序 =============

def main():
    """主函数"""
    st.markdown('<div class="main-header">📈 A股量化信号系统</div>', unsafe_allow_html=True)

    # 侧边栏
    settings = render_sidebar()

    # 获取追踪器
    tracker = get_tracker()

    # 生成信号
    signals_df = generate_mock_signals(settings['num_stocks'])

    # 如果启用了变化模拟，随机调整一些信号
    if settings['simulate_change'] and 'prev_signals_df' in st.session_state:
        # 随机选择一些股票进行信号变化
        n_changes = int(len(signals_df) * 0.3)  # 30%的股票信号变化
        change_indices = np.random.choice(len(signals_df), n_changes, replace=False)
        for idx in change_indices:
            change = np.random.randn() * 0.4  # 信号变化幅度
            signals_df.at[idx, 'signal'] = np.clip(signals_df.at[idx, 'signal'] + change, -1, 1)
            # 更新信号等级
            sig = signals_df.at[idx, 'signal']
            if sig >= 0.7:
                signals_df.at[idx, 'signal_level'] = '强买入'
            elif sig >= 0.3:
                signals_df.at[idx, 'signal_level'] = '买入'
            elif sig >= -0.3:
                signals_df.at[idx, 'signal_level'] = '持有'
            elif sig >= -0.7:
                signals_df.at[idx, 'signal_level'] = '卖出'
            else:
                signals_df.at[idx, 'signal_level'] = '强卖出'

    # 保存当前信号到session
    st.session_state.signals_df = signals_df

    # 更新追踪器
    if settings['enable_tracking']:
        trade_signals = tracker.update_signals(signals_df)

    # 刷新按钮
    col1, col2, col3 = st.columns([1, 1, 3])
    with col1:
        if st.button("🔄 刷新数据", use_container_width=True):
            st.session_state.prev_signals_df = signals_df.copy()
            st.rerun()
    with col2:
        if st.button("🗑️ 清空持仓", use_container_width=True):
            tracker.positions.clear()
            tracker.signal_history.clear()
            tracker.trade_signals.clear()
            st.rerun()

    st.divider()

    # 标签页
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📋 交易信号", "💼 持仓管理", "📊 信号总览",
        "🔍 个股分析", "📈 信号变化"
    ])

    with tab1:
        if settings['enable_tracking']:
            render_trade_panel(tracker, settings)
        else:
            st.info("请在侧边栏启用「持仓追踪」功能")

    with tab2:
        if settings['enable_tracking']:
            render_positions(tracker)
        else:
            st.info("请在侧边栏启用「持仓追踪」功能")

    with tab3:
        render_signal_overview(signals_df, settings)

    with tab4:
        render_stock_detail(signals_df)

    with tab5:
        render_signal_changes(tracker)


if __name__ == "__main__":
    main()
