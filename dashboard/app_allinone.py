"""
A股量化信号系统
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import sys
import os

# 添加项目根目录到path
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# 添加dashboard目录到path
_dashboard_dir = os.path.dirname(os.path.abspath(__file__))
if _dashboard_dir not in sys.path:
    sys.path.insert(0, _dashboard_dir)

# 导入实盘持仓模块
try:
    from real_positions import RealPositionTracker, get_real_tracker
except ImportError:
    RealPositionTracker = None
    get_real_tracker = None

# ==================== 持仓追踪模块 ====================

class PositionTracker:
    """持仓追踪器 - 捕捉临界突破"""

    def __init__(self):
        self.positions = {}
        self.signal_history = {}
        self.factor_history = {}
        self.level_scores_history = {}
        self.trade_signals = []

        # 交易阈值
        self.BUY_THRESHOLD = 0.30
        self.SELL_THRESHOLD = -0.30

    def update_signals(self, current_signals: pd.DataFrame):
        """更新信号并生成交易建议"""
        today = datetime.now().strftime('%Y-%m-%d')
        trade_signals = []

        for _, row in current_signals.iterrows():
            symbol = row['symbol']
            current_signal = row['signal']

            if symbol not in self.signal_history:
                self.signal_history[symbol] = []
            if symbol not in self.factor_history:
                self.factor_history[symbol] = {}
            if symbol not in self.level_scores_history:
                self.level_scores_history[symbol] = {'macro': [], 'meso': [], 'micro': []}

            prev_signal = self.signal_history[symbol][-1] if self.signal_history[symbol] else None

            trade_signal = self._generate_crossing_signal(symbol, current_signal, prev_signal, row)

            if trade_signal:
                trade_signals.append(trade_signal)

                if trade_signal['action'] in ['买入', '加仓']:
                    self.positions[symbol] = {
                        'entry_signal': current_signal,
                        'entry_date': today,
                        'name': row.get('name', ''),
                        'status': '持有',
                        'attribution': trade_signal.get('attribution', {})
                    }
                elif trade_signal['action'] in ['卖出', '清仓']:
                    if symbol in self.positions:
                        del self.positions[symbol]

            # 更新历史
            self.signal_history[symbol].append(current_signal)
            if len(self.signal_history[symbol]) > 30:
                self.signal_history[symbol] = self.signal_history[symbol][-30:]

            factors = row.get('factors', {})
            for factor_name, factor_value in factors.items():
                if factor_name not in self.factor_history[symbol]:
                    self.factor_history[symbol][factor_name] = []
                self.factor_history[symbol][factor_name].append(factor_value)

            self.level_scores_history[symbol]['macro'].append(row.get('macro_score', 0))
            self.level_scores_history[symbol]['meso'].append(row.get('meso_score', 0))
            self.level_scores_history[symbol]['micro'].append(row.get('micro_score', 0))

        self.trade_signals.extend(trade_signals)
        return trade_signals

    def _generate_crossing_signal(self, symbol: str, current_signal: float, prev_signal: Optional[float], row_data: pd.Series) -> Optional[Dict]:
        """生成跨越雷池型交易信号"""
        if prev_signal is None:
            return None

        attribution = self._analyze_attribution(symbol, row_data, prev_signal, current_signal)

        buy_crossing = (prev_signal < self.BUY_THRESHOLD) and (current_signal >= self.BUY_THRESHOLD)
        sell_crossing = (prev_signal > self.SELL_THRESHOLD) and (current_signal <= self.SELL_THRESHOLD)

        action = None
        reason = ""
        priority = 0
        crossing_info = {}

        in_position = symbol in self.positions

        # 买入跨越
        if buy_crossing and not in_position:
            action = "买入"
            priority = 3
            crossing_info = {'type': 'buy_crossing', 'threshold': self.BUY_THRESHOLD}
            reason = f"🚨 跨越买入红线！信号 {prev_signal:.2f} → {current_signal:.2f}"

        # 卖出跨越
        elif sell_crossing and in_position:
            action = "清仓"
            priority = 3
            crossing_info = {'type': 'sell_crossing', 'threshold': self.SELL_THRESHOLD}
            reason = f"⚠️ 跌破卖出红线！信号 {prev_signal:.2f} → {current_signal:.2f}"

        # 减仓
        elif in_position and (current_signal < prev_signal) and (current_signal < 0):
            if (prev_signal - current_signal) > 0.2:
                action = "减仓"
                priority = 2
                reason = f"📉 信号明显恶化: {prev_signal:.2f} → {current_signal:.2f}"

        # 急剧下跌
        elif in_position and (prev_signal - current_signal) > 0.5:
            action = "清仓"
            priority = 4
            reason = f"🔥 信号急剧下跌！紧急止损: {prev_signal:.2f} → {current_signal:.2f}"

        if action:
            return {
                'symbol': symbol,
                'name': row_data.get('name', ''),
                'action': action,
                'current_signal': current_signal,
                'prev_signal': prev_signal,
                'signal_change': current_signal - prev_signal,
                'reason': reason,
                'priority': priority,
                'attribution': attribution,
                'crossing_info': crossing_info,
                'timestamp': datetime.now()
            }

        return None

    def _analyze_attribution(self, symbol: str, row_data: pd.Series, prev_signal: float, current_signal: float) -> Dict:
        """动能归因分析"""
        current_macro = row_data.get('macro_score', 0)
        current_meso = row_data.get('meso_score', 0)
        current_micro = row_data.get('micro_score', 0)

        history = self.level_scores_history.get(symbol, {'macro': [], 'meso': [], 'micro': []})
        prev_macro = history['macro'][-1] if history['macro'] else 0
        prev_meso = history['meso'][-1] if history['meso'] else 0
        prev_micro = history['micro'][-1] if history['micro'] else 0

        macro_change = current_macro - prev_macro
        meso_change = current_meso - prev_meso
        micro_change = current_micro - prev_micro

        changes = {'macro': macro_change, 'meso': meso_change, 'micro': micro_change}
        sorted_changes = sorted(changes.items(), key=lambda x: abs(x[1]), reverse=True)
        primary_driver = sorted_changes[0][0] if sorted_changes else 'macro'

        level_names = {'macro': '宏观环境', 'meso': '中观行业', 'micro': '微观量价'}

        attribution = {
            'primary_driver': primary_driver,
            'driver_name': level_names[primary_driver],
            'changes': changes,
            'current_scores': {'macro': current_macro, 'meso': current_meso, 'micro': current_micro},
            'prev_scores': {'macro': prev_macro, 'meso': prev_meso, 'micro': prev_micro},
            'display_items': []
        }

        for level, name in [('macro', '宏观环境'), ('meso', '中观行业'), ('micro', '微观量价')]:
            prev_score = attribution['prev_scores'][level]
            curr_score = attribution['current_scores'][level]
            change = attribution['changes'][level]

            if abs(change) < 0.05:
                icon = '⚪️'
                status_text = '无显著变化'
            elif change > 0.15:
                icon = '🟢'
                status_text = '主驱动力' if level == primary_driver else '同步上升'
            elif change > 0:
                icon = '🟡'
                status_text = '企稳'
            elif change < -0.15:
                icon = '🔴'
                status_text = '主要拖累' if level == primary_driver else '同步走弱'
            else:
                icon = '🟠'
                status_text = '持续承压'

            attribution['display_items'].append({
                'level': level, 'name': name, 'icon': icon,
                'prev_score': round(prev_score, 2), 'current_score': round(curr_score, 2),
                'change': round(change, 2), 'status': status_text, 'is_driver': level == primary_driver
            })

        return attribution

    def get_positions(self) -> pd.DataFrame:
        """获取当前持仓"""
        if not self.positions:
            return pd.DataFrame()

        data = []
        for symbol, pos in self.positions.items():
            current_signal = self.signal_history[symbol][-1] if symbol in self.signal_history else 0
            data.append({
                'symbol': symbol,
                'name': pos.get('name', ''),
                'entry_signal': pos.get('entry_signal', 0),
                'entry_date': pos.get('entry_date', ''),
                'current_signal': current_signal,
                'status': pos.get('status', '持有'),
                'attribution': pos.get('attribution', {})
            })

        return pd.DataFrame(data)

    def get_trade_signals(self, limit: int = 20) -> pd.DataFrame:
        """获取最近的交易信号"""
        if not self.trade_signals:
            return pd.DataFrame()

        signals = sorted(self.trade_signals, key=lambda x: (-x['priority'], x['timestamp']))
        df = pd.DataFrame(signals[-limit:])
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values(['priority', 'timestamp'], ascending=[False, False])

        return df

    def get_signal_changes(self, current_signals: pd.DataFrame) -> pd.DataFrame:
        """获取信号变化列表"""
        changes = []

        for _, row in current_signals.iterrows():
            symbol = row['symbol']
            current = row['signal']

            if symbol in self.signal_history and self.signal_history[symbol]:
                prev = self.signal_history[symbol][-1]
                change = current - prev

                buy_cross = (prev < self.BUY_THRESHOLD) and (current >= self.BUY_THRESHOLD)
                sell_cross = (prev > self.SELL_THRESHOLD) and (current <= self.SELL_THRESHOLD)

                if buy_cross or sell_cross or abs(change) >= 0.1:
                    changes.append({
                        'symbol': symbol, 'name': row.get('name', ''),
                        'prev_signal': round(prev, 3), 'current_signal': round(current, 3),
                        'change': round(change, 3), 'buy_crossing': buy_cross, 'sell_crossing': sell_cross
                    })

        df = pd.DataFrame(changes)
        if not df.empty:
            df = df.sort_values('change', ascending=False)

        return df

    def get_daily_summary(self) -> Dict:
        """获取每日汇总"""
        positions_df = self.get_positions()
        signals_df = self.get_trade_signals()

        return {
            'total_positions': len(positions_df) if not positions_df.empty else 0,
            'buy_signals': len(signals_df[signals_df['action'] == '买入']) if not signals_df.empty else 0,
            'sell_signals': len(signals_df[signals_df['action'] == '清仓']) if not signals_df.empty else 0,
            'reduce_signals': len(signals_df[signals_df['action'] == '减仓']) if not signals_df.empty else 0,
        }


def get_tracker() -> PositionTracker:
    """获取追踪器单例 - 使用 session_state 保持状态"""
    if 'tracker' not in st.session_state:
        st.session_state.tracker = PositionTracker()
    return st.session_state.tracker


# ==================== Dashboard 主程序 ====================

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
    .action-buy { background-color: #c8e6c9 !important; }
    .action-sell { background-color: #ffcdd2 !important; }
    .action-add { background-color: #b2dfdb !important; }
    .action-reduce { background-color: #ffe0b2 !important; }
    .priority-high { border-left: 4px solid #d50000; }
    .priority-medium { border-left: 4px solid #ff9800; }
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
        ('000776', '广发证券'), ('002475', '立讯精密'), ('601018', '宁波港')
    ]

    data = []
    for i in range(min(num_stocks, len(stock_names))):
        symbol, name = stock_names[i]

        # 生成信号
        base_signal = np.random.randn() * 0.6
        signal_change = np.random.randn() * 0.3
        signal = np.clip(base_signal + signal_change, -1, 1)

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

        macro_score = np.clip(np.random.randn() * 0.3, -1, 1)
        meso_score = np.clip(np.random.randn() * 0.4, -1, 1)
        micro_score = np.clip(np.random.randn() * 0.5, -1, 1)

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
            'symbol': symbol, 'name': name, 'signal': signal, 'signal_level': level,
            'macro_score': macro_score, 'meso_score': meso_score, 'micro_score': micro_score,
            'factors': factors
        })

    df = pd.DataFrame(data)
    df = df.sort_values('signal', ascending=False).reset_index(drop=True)
    return df


def render_sidebar():
    """渲染侧边栏"""
    st.sidebar.title("⚙️ 设置")

    stock_pool = st.sidebar.radio("股票池", ["沪深300", "中证500", "自定义"], horizontal=True)
    num_stocks = st.sidebar.slider("分析股票数量", 10, 100, 50)

    col1, col2 = st.sidebar.columns(2)
    with col1:
        buy_threshold = st.slider("买入阈值", 0.0, 1.0, 0.3, 0.1)
    with col2:
        sell_threshold = st.slider("卖出阈值", -1.0, 0.0, -0.3, 0.1)

    st.sidebar.subheader("🔄 交易闭环")
    enable_tracking = st.sidebar.checkbox("启用持仓追踪", value=True)
    simulate_change = st.sidebar.checkbox("模拟信号变化", value=True)

    st.sidebar.subheader("因子权重")
    macro_weight = st.slider("宏观因子", 0.0, 1.0, 0.2, 0.1)
    meso_weight = st.slider("中观因子", 0.0, 1.0, 0.3, 0.1)
    micro_weight = round(1.0 - macro_weight - meso_weight, 1)
    st.sidebar.caption(f"微观因子: {micro_weight}")

    return {
        'stock_pool': stock_pool, 'num_stocks': num_stocks,
        'buy_threshold': buy_threshold, 'sell_threshold': sell_threshold,
        'enable_tracking': enable_tracking, 'simulate_change': simulate_change,
        'weights': {'macro': macro_weight, 'meso': meso_weight, 'micro': micro_weight}
    }


def render_attribution(attribution: dict) -> str:
    """渲染归因分析"""
    if not attribution or 'display_items' not in attribution:
        return ""

    items = attribution['display_items']

    html_parts = ["<div style='margin-top: 10px; padding: 8px; background: rgba(255,255,255,0.5); border-radius: 5px;'>"]
    html_parts.append("<div style='font-size: 0.85em; margin-bottom: 6px;'><strong>📊 动能归因:</strong></div>")

    for item in items:
        if item['change'] > 0:
            change_str = f"+{item['change']:.2f}"
        else:
            change_str = f"{item['change']:.2f}"

        if item['is_driver']:
            item_html = f"""
            <div style="margin-bottom: 4px;">
                {item['icon']} <strong>{item['name']}</strong>:
                {item['prev_score']:.2f} ➡️ {item['current_score']:.2f}
                <span style="color: {'green' if item['change'] > 0 else 'red'}">({change_str})</span>
                <span style="font-weight: bold; color: {'green' if item['change'] > 0 else 'red'}">({item['status']})</span>
            </div>
            """
        else:
            item_html = f"""
            <div style="margin-bottom: 4px; opacity: 0.8;">
                {item['icon']} {item['name']}:
                {item['prev_score']:.2f} ➡️ {item['current_score']:.2f}
                <span style="color: {'green' if item['change'] > 0 else 'red'}">({change_str})</span>
                <span>({item['status']})</span>
            </div>
            """

        html_parts.append(item_html)

    html_parts.append("</div>")
    return ''.join(html_parts)


def render_trade_panel(tracker, settings):
    """渲染交易面板"""
    st.subheader("📋 今日交易信号")

    summary = tracker.get_daily_summary()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("当前持仓", summary['total_positions'])
    with col2:
        st.metric("买入信号", summary['buy_signals'])
    with col3:
        st.metric("减仓信号", summary['reduce_signals'])
    with col4:
        st.metric("清仓信号", summary['sell_signals'])

    st.divider()

    signals_df = tracker.get_trade_signals()

    if signals_df.empty:
        st.info("📊 暂无交易信号。点击下方「刷新数据」模拟新的一天数据，生成交易信号。")
        return

    buy_df = signals_df[signals_df['action'].isin(['买入', '加仓'])]
    sell_df = signals_df[signals_df['action'].isin(['清仓', '减仓'])]

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
                bg_class = "action-sell" if row['action'] == '清仓' else "action-reduce"
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


def render_positions(tracker):
    """渲染持仓列表"""
    st.subheader("💼 当前持仓")

    positions_df = tracker.get_positions()

    if positions_df.empty:
        st.info("暂无持仓")
        return

    positions_df['pnl'] = np.random.uniform(-0.1, 0.15, len(positions_df))
    positions_df['pnl_pct'] = (positions_df['pnl'] * 100).round(2)

    display_df = positions_df[['symbol', 'name', 'entry_signal', 'current_signal', 'pnl_pct']]
    display_df.columns = ['代码', '名称', '买入信号', '当前信号', '盈亏%']

    def color_pnl(val):
        if val > 0:
            return 'color: #00c853'
        elif val < 0:
            return 'color: #d50000'
        return ''

    styled_df = display_df.style.applymap(color_pnl, subset=['盈亏%'])
    st.dataframe(styled_df, use_container_width=True)


# ==================== VIP轨：实盘持仓管理 ====================

def render_trade_input():
    """渲染交易录入界面"""
    st.subheader("📝 交易录入")

    # 初始化状态
    if 'add_success' not in st.session_state:
        st.session_state.add_success = False
    if 'last_added_stock' not in st.session_state:
        st.session_state.last_added_stock = None

    # 显示添加成功提示
    if st.session_state.add_success and st.session_state.last_added_stock:
        st.markdown("---")
        st.success(f"✅ **添加成功！**")
        st.info(f"📊 {st.session_state.last_added_stock['symbol']} - {st.session_state.last_added_stock['name']}\n"
                f"   买入价: ¥{st.session_state.last_added_stock['price']:.2f} × {st.session_state.last_added_stock['quantity']}股")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("➕ 添加下一个", use_container_width=True, key="add_next"):
                st.session_state.add_success = False
                st.session_state.last_added_stock = None
                st.rerun()
        with col2:
            if st.button("✅ 完成", use_container_width=True, key="add_done"):
                st.session_state.add_success = False
                st.session_state.last_added_stock = None
                st.rerun()
        return

    # 交易录入表单
    with st.form("trade_input_form"):
        col1, col2, col3 = st.columns(3)
        with col1:
            symbol = st.text_input("股票代码 *", placeholder="如: 600519", max_chars=10, key="input_symbol")
            name = st.text_input("股票名称 *", placeholder="如: 贵州茅台", max_chars=20, key="input_name")
        with col2:
            price = st.number_input("买入均价 *", min_value=0.01, step=0.01, format="%.2f", key="input_price")
            quantity = st.number_input("买入数量 *", min_value=100, step=100, value=1000, key="input_quantity")
        with col3:
            trade_date = st.date_input("操作日期", value=datetime.now().date(), key="input_date")

        col1, col2 = st.columns(2)
        with col1:
            submit = st.form_submit_button("➕ 添加持仓", use_container_width=True)
        with col2:
            clear_btn = st.form_submit_button("🗑️ 清空", use_container_width=True)

        if clear_btn:
            st.rerun()

        if submit:
            if symbol and name and price > 0 and quantity > 0:
                if get_real_tracker is None:
                    st.error("❌ 实盘持仓模块未加载")
                    return
                real_tracker = get_real_tracker()
                date_str = trade_date.strftime('%Y-%m-%d')
                if real_tracker.add_position(symbol, name, price, quantity, date_str):
                    # 保存成功信息到 session_state
                    st.session_state.add_success = True
                    st.session_state.last_added_stock = {
                        'symbol': symbol,
                        'name': name,
                        'price': price,
                        'quantity': quantity,
                        'date': date_str
                    }
                    st.rerun()
            else:
                st.warning("⚠️ 请填写所有必填字段")


def render_real_positions(signals_df):
    """渲染VIP轨持仓健康度体检"""
    st.subheader("💼 VIP轨 - 持仓健康度体检")

    if get_real_tracker is None:
        st.warning("⚠️ 实盘持仓模块未加载，请检查 `dashboard/real_positions.py` 是否存在")
        return

    real_tracker = get_real_tracker()
    positions_df = real_tracker.get_positions()

    if positions_df.empty:
        st.info("📭 暂无实盘持仓，请先在上方「交易录入」中添加持仓")
        return

    # 生成模拟当前价格
    current_prices = {}
    for _, row in positions_df.iterrows():
        symbol = row['代码']
        # 模拟当前价格（实际应该从真实数据源获取）
        base_price = row['买入价']
        change = np.random.uniform(-0.15, 0.20)
        current_prices[symbol] = round(base_price * (1 + change), 2)

    # 生成健康度报告
    health_report = real_tracker.get_health_report(signals_df, current_prices)

    if health_report.empty:
        return

    # 汇总卡片
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        total_count = len(health_report)
        st.metric("持仓数量", total_count)
    with col2:
        avg_health = health_report['健康度'].mean()
        health_color = '🟢' if avg_health >= 70 else '🟡' if avg_health >= 50 else '🔴'
        st.metric("平均健康度", f"{health_color} {avg_health:.0f}")
    with col3:
        total_pnl = health_report['盈亏%'].sum()
        pnl_color = '🟢' if total_pnl > 0 else '🔴'
        st.metric("总盈亏%", f"{pnl_color} {total_pnl:.2f}%")
    with col4:
        profit_count = (health_report['盈亏%'] > 0).sum()
        st.metric("盈利数", f"🟢 {profit_count}")
    with col5:
        loss_count = (health_report['盈亏%'] < 0).sum()
        st.metric("亏损数", f"🔴 {loss_count}")

    st.divider()

    # 健康度报告表格
    st.markdown("### 📊 持仓体检报告")

    # 按健康度着色
    def color_health(val):
        if val >= 80:
            return 'background-color: #c8e6c9; color: #1b5e20'
        elif val >= 65:
            return 'background-color: #fff9c4; color: #f57f17'
        elif val >= 50:
            return 'background-color: #ffe0b2; color: #e65100'
        else:
            return 'background-color: #ffcdd2; color: #b71c1c'

    def color_pnl(val):
        if val > 0:
            return 'color: #00c853; font-weight: bold'
        elif val < 0:
            return 'color: #d50000; font-weight: bold'
        return ''

    def color_signal(val):
        if val >= 0.3:
            return 'background-color: #c8e6c9'
        elif val <= -0.3:
            return 'background-color: #ffcdd2'
        return ''

    # 准备显示列
    display_cols = ['代码', '名称', '买入价', '现价', '盈亏%', '市值',
                    '持仓天数', '信号', '健康度', '健康评级', '建议']

    styled_df = health_report[display_cols].style
    styled_df = styled_df.applymap(color_health, subset=['健康度'])
    styled_df = styled_df.applymap(color_pnl, subset=['盈亏%'])
    styled_df = styled_df.applymap(color_signal, subset=['信号'])

    st.dataframe(styled_df, use_container_width=True, height=400)

    # 持仓操作
    st.divider()
    st.markdown("### 🔧 持仓操作")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### 卖出持仓")
        sell_symbol = st.selectbox("选择要卖出的股票",
                                   options=[""] + list(health_report['代码'].unique()),
                                   key="sell_select",
                                   label_visibility="collapsed")

        if sell_symbol and st.button("🗑️ 卖出持仓", key="sell_btn", use_container_width=True):
            real_tracker.remove_position(sell_symbol)
            st.success(f"✅ 已卖出: {sell_symbol}")
            st.rerun()

    with col2:
        st.markdown("#### 快捷操作")
        if st.button("🔄 刷新健康度", use_container_width=True):
            st.rerun()


def render_signal_overview(signals_df, settings):
    """渲染信号总览"""
    buy_threshold = settings['buy_threshold']
    sell_threshold = settings['sell_threshold']

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

    col1, col2 = st.columns(2)

    with col1:
        fig = px.histogram(signals_df, x='signal', nbins=30, title='信号分布')
        fig.add_vline(x=buy_threshold, line_dash="dash", line_color="green")
        fig.add_vline(x=sell_threshold, line_dash="dash", line_color="red")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        level_counts = signals_df['signal_level'].value_counts()
        fig = px.pie(values=level_counts.values, names=level_counts.index, title='信号等级分布')
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    st.subheader("📋 完整信号列表")
    display_df = signals_df[['symbol', 'name', 'signal', 'signal_level']].copy()
    display_df['signal'] = display_df['signal'].round(3)
    st.dataframe(display_df, use_container_width=True, height=300)


def render_stock_detail(signals_df):
    """渲染个股分析"""
    st.subheader("🔍 个股详情")

    stock_options = [f"{row['symbol']} - {row['name']}" for _, row in signals_df.iterrows()]
    selected = st.selectbox("选择股票", stock_options)

    if selected:
        symbol = selected.split(' - ')[0]
        stock_data = signals_df[signals_df['symbol'] == symbol].iloc[0]

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("综合信号", f"{stock_data['signal']:.3f}")
        with col2:
            st.markdown(f"<strong>{stock_data['signal_level']}</strong>", unsafe_allow_html=True)
        with col3:
            st.metric("宏观得分", f"{stock_data['macro_score']:.3f}")
        with col4:
            st.metric("中观得分", f"{stock_data['meso_score']:.3f}")

        st.divider()

        factors = stock_data['factors']
        if factors:
            categories = list(factors.keys())
            values = list(factors.values())
            display_values = [np.clip(v, -1, 1) for v in values]

            fig = go.Figure(data=go.Scatterpolar(
                r=display_values, theta=categories, fill='toself', marker_color='#1f77b4'
            ))
            fig.update_layout(polar=dict(radialaxis=dict(visible=True, range=[-1, 1])), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)


# ============= 主程序 =============

def main():
    """主函数"""
    st.markdown('<div class="main-header">📈 A股量化信号系统</div>', unsafe_allow_html=True)

    # 初始化 session state
    if 'day_count' not in st.session_state:
        st.session_state.day_count = 0
    if 'signals_df' not in st.session_state:
        st.session_state.signals_df = generate_mock_signals(50)
        st.session_state.day_count = 1

    settings = render_sidebar()
    tracker = get_tracker()

    # 使用当前信号
    signals_df = st.session_state.signals_df

    # 按钮区域
    col1, col2, col3 = st.columns([1, 1, 3])

    with col1:
        refresh_clicked = st.button("🔄 刷新数据", key="refresh_button", use_container_width=True)
    with col2:
        clear_clicked = st.button("🗑️ 清空持仓", key="clear_button", use_container_width=True)
    with col3:
        st.write(f"📅 第 **{st.session_state.day_count}** 个交易日")

    # 处理刷新按钮点击
    if refresh_clicked:
        with st.spinner("正在生成新信号..."):
            # 生成新的"今天"的信号
            new_signals = generate_mock_signals(settings['num_stocks'])
            # 模拟部分股票跨越阈值
            for idx in range(len(new_signals)):
                if np.random.random() < 0.20:  # 20%概率生成跨越信号
                    # 生成买入跨越
                    if np.random.random() < 0.5:
                        new_signals.at[idx, 'signal'] = np.random.uniform(0.35, 0.8)
                        new_signals.at[idx, 'signal_level'] = '强买入' if new_signals.at[idx, 'signal'] >= 0.7 else '买入'
                    # 生成卖出跨越
                    else:
                        new_signals.at[idx, 'signal'] = np.random.uniform(-0.8, -0.35)
                        new_signals.at[idx, 'signal_level'] = '强卖出' if new_signals.at[idx, 'signal'] <= -0.7 else '卖出'
            st.session_state.signals_df = new_signals
            st.session_state.day_count += 1
        st.success(f"✅ 已更新到第 {st.session_state.day_count} 天")
        st.rerun()

    # 处理清空按钮点击
    if clear_clicked:
        tracker.positions.clear()
        tracker.signal_history.clear()
        tracker.trade_signals.clear()
        st.warning("🗑️ 持仓已清空")
        st.rerun()

    # 更新追踪器
    if settings['enable_tracking']:
        tracker.update_signals(signals_df)

    st.divider()

    # 双轨制标签页
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📋 普通轨-交易信号",
        "💼 普通轨-持仓管理",
        "💎 VIP轨-实盘持仓",
        "📊 信号总览",
        "🎯 双层回测"
    ])

    with tab1:
        st.markdown("### 🔄 普通轨 - 信号筛选")
        st.caption("针对全市场股票，按照信号强度筛选潜在机会")
        st.divider()
        if settings['enable_tracking']:
            render_trade_panel(tracker, settings)
        else:
            st.info("请在侧边栏启用「持仓追踪」功能")

    with tab2:
        st.markdown("### 🔄 普通轨 - 系统持仓")
        st.caption("系统自动捕捉的信号持仓（模拟持仓）")
        st.divider()
        if settings['enable_tracking']:
            render_positions(tracker)
        else:
            st.info("请在侧边栏启用「持仓追踪」功能")

    with tab3:
        st.markdown("### 💎 VIP轨 - 实盘持仓管理")
        st.caption("您的实盘持仓健康度体检，基于真实交易数据")
        st.divider()

        subtab1, subtab2 = st.tabs(["📝 交易录入", "💼 持仓体检"])

        with subtab1:
            render_trade_input()

        with subtab2:
            render_real_positions(signals_df)

    with tab4:
        render_signal_overview(signals_df, settings)

    with tab5:
        render_backtest_interface()


# ==================== 回测界面 ====================

def render_backtest_interface():
    """渲染回测界面"""
    st.markdown("### 🎯 双层回测系统")
    st.caption("第一层：向量化筛选 → 第二层：循环回测")

    st.divider()

    # 数据源选择
    data_source = st.radio(
        "📊 选择数据源",
        ["模拟数据", "真实数据"],
        horizontal=True,
        help="模拟数据用于快速验证，真实数据使用已下载的市场数据"
    )

    # 真实数据配置
    real_db_path = None
    real_db_info = {}

    if data_source == "真实数据":
        import glob
        import duckdb
        import os

        # 数据库选择
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        data_dir = os.path.join(project_root, "data")

        # 扫描可用的数据库
        db_files = glob.glob(os.path.join(data_dir, "*.duckdb"))

        # 数据库选项
        db_options = {}
        # 按优先级排序数据库（沪深300优先）
        db_list = []  # [(display_name, db_file), ...]

        for db_file in db_files:
            db_name = os.path.basename(db_file)
            if db_name == "quant.duckdb":
                continue  # 跳过模拟数据库

            # 友好的显示名称
            if "cs300_2020_bull" in db_name:
                display_name = "🚀 沪深300 (2020牛市·进攻测试)"
                priority = 0  # 最高优先级
            elif "cs300_2018_full" in db_name:
                display_name = "🎯 沪深300 (2018年盲测·225只)"
                priority = 1
            elif "cs300_2018" in db_name:
                display_name = "沪深300 (2018年·32只)"
                priority = 2
            elif "cs300" in db_name and "2years" in db_name:
                display_name = "🌟 沪深300 (2年)"
                priority = 3
            elif "real_market" in db_name:
                display_name = "测试数据 (少量)"
                priority = 4
            else:
                display_name = db_name.replace(".duckdb", "")
                priority = 5

            db_list.append((display_name, db_file, priority))

        # 按优先级排序
        db_list.sort(key=lambda x: x[2])

        # 添加 Tushare 本地数据库（如果存在）
        tushare_db_path = os.path.join(data_dir, "tushare_db.duckdb")
        if os.path.exists(tushare_db_path):
            db_list.insert(0, ("📊 Tushare本地数据库 (实时)", tushare_db_path, -1))

        if db_list:
            # 提取显示名称列表和路径字典
            db_options = {name: path for name, path, _ in db_list}
            db_names = [name for name, _, _ in db_list]

            # 默认选择第一个（沪深300）
            selected_db = st.selectbox(
                "选择数据库",
                options=db_names,
                index=0,  # 默认选择第一个
                help="选择要用于回测的数据库"
            )
            real_db_path = db_options[selected_db]

            # 显示数据库信息
            if os.path.exists(real_db_path):
                try:
                    conn = duckdb.connect(real_db_path)

                    # 检测数据库格式（Tushare格式 vs 原始格式）
                    # 通过尝试查询 trade_date 来判断
                    try:
                        test_result = conn.execute("SELECT trade_date FROM daily_quotes LIMIT 1").fetchone()
                        is_tushare_format = True
                        date_col = 'trade_date'
                        symbol_col = 'ts_code'
                    except:
                        is_tushare_format = False
                        date_col = 'date'
                        symbol_col = 'symbol'

                    count = conn.execute(f"SELECT COUNT(*) FROM daily_quotes").fetchone()[0]
                    symbol_count = conn.execute(f"SELECT COUNT(DISTINCT {symbol_col}) FROM daily_quotes").fetchone()[0]
                    date_range = conn.execute(f"""
                        SELECT
                            MIN({date_col}) as start_date,
                            MAX({date_col}) as end_date
                        FROM daily_quotes
                    """).fetchone()
                    conn.close()

                    real_db_info = {
                        'count': count,
                        'symbol_count': symbol_count,
                        'start_date': str(date_range[0]),
                        'end_date': str(date_range[1]),
                        'is_tushare_format': is_tushare_format
                    }

                    st.info(f"📊 数据库状态: **{count:,} 条记录**, **{symbol_count} 只股票** | 时间范围: {date_range[0]} 至 {date_range[1]}")
                except Exception as e:
                    st.warning(f"⚠️ 数据库查询失败: {e}")
            else:
                st.error("❌ 数据库文件不存在")
                real_db_path = None

            st.caption("💡 提示: 下载更多数据请运行 `python3 scripts/download_cs300.py`")
        else:
            st.warning("⚠️ 未找到真实数据，请先下载数据")
            st.code("python3 scripts/download_cs300.py")
            real_db_path = None

    # 回测配置
    with st.expander("⚙️ 回测配置", expanded=True):
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            # 策略选择
            strategy_options = {
                "trend": "趋势跟踪 (+14%)",
                "momentum": "20日动量 (原策略)",
                "multifactor": "多因子融合 (推荐)",
                "meanreversion": "均值回归 (胜率69%)",
                "trend_plus": "趋势增强",
                "adaptive": "自适应Hurst (+31%)",
                "adaptive_adx": "自适应ADX ⚡ (+62%)",
                "adaptive_adx_rest": "自适应ADX+空仓 💎 (59%, 1.73盈亏比)"
            }
            strategy = st.selectbox(
                "信号策略",
                options=list(strategy_options.keys()),
                format_func=lambda x: strategy_options[x],
                index=0  # 默认选择趋势跟踪
            )

            if data_source == "模拟数据":
                n_stocks = st.number_input("回测股票数", min_value=50, max_value=500, value=100, step=50)
                n_days = st.number_input("回测天数", min_value=100, max_value=500, value=200, step=50)
            else:
                st.info(f"真实数据模式")
                n_stocks = 100
                n_days = 200

        with col2:
            buy_threshold = st.slider("买入阈值", 0.0, 1.0, 0.30, 0.05, help="信号强度阈值，越高越严格")
            sell_threshold = st.slider("卖出阈值", -1.0, 0.0, -0.3, 0.05)

        with col3:
            initial_cash = st.number_input("初始资金", min_value=10000, max_value=1000000, value=100000, step=10000)
            max_positions = st.slider("最大持仓数", 1, 10, 5)

        with col4:
            position_size = st.number_input("单股金额", min_value=5000, max_value=100000, value=20000, step=5000)
            if data_source == "模拟数据":
                min_signals = st.number_input("最小信号次数", min_value=5, max_value=50, value=10)

    # 止盈止损配置
    with st.expander("🛡️ 止盈止损设置", expanded=False):
        use_dynamic_stops = st.checkbox(
            "启用动态止盈止损",
            value=True,
            help="根据盈亏情况动态平仓，保护利润减少损失"
        )

        if use_dynamic_stops:
            col1, col2, col3 = st.columns(3)
            with col1:
                initial_stop_loss = st.slider(
                    "初始止损 (%)",
                    0.01, 0.20, 0.08, 0.01,
                    help="亏损达到此比例时止损（牛市建议8%）"
                )
            with col2:
                trailing_stop = st.slider(
                    "追踪止盈 (%)",
                    0.01, 0.20, 0.08, 0.01,
                    help="从最高价回撤此比例时止盈（放宽到8%让利润奔跑）"
                )
            with col3:
                take_profit = st.slider(
                    "目标止盈 (%)",
                    0.05, 0.50, 0.30, 0.05,
                    help="盈利达到此比例后开始追踪止盈（提高到30%更长时间保护）"
                )

            st.caption("💡 牛市建议: 初始止损8% + 追踪止盈8% + 目标止盈30% → 让利润充分奔跑")
        else:
            initial_stop_loss = 0.05
            trailing_stop = 0.03
            take_profit = 0.15

    # 动态仓位管理选项
    with st.expander("💰 动态仓位管理 (实验性)", expanded=False):
        st.warning("⚠️ 回测显示动态仓位降低了总收益，建议保持关闭")

        use_dynamic_position = st.checkbox(
            "启用动态仓位管理",
            value=False,  # 默认关闭
            help="根据趋势强度动态调整每笔交易的仓位大小"
        )

        if use_dynamic_position:
            col1, col2, col3 = st.columns(3)
            with col1:
                min_position_mult = st.slider(
                    "最小仓位倍数",
                    0.1, 1.0, 0.5, 0.1,
                    help="趋势弱时使用倍数（如0.5表示一半仓位）"
                )
            with col2:
                max_position_mult = st.slider(
                    "最大仓位倍数",
                    1.0, 3.0, 2.0, 0.1,
                    help="趋势强时使用倍数（如2.0表示双倍仓位）"
                )
            with col3:
                st.metric(
                    "仓位范围",
                    f"{min_position_mult}x ~ {max_position_mult}x"
                )

            st.caption("💡 强趋势时加仓，弱趋势时减仓，提高资金利用效率")
        else:
            min_position_mult = 1.0
            max_position_mult = 1.0

    # 策略说明
    strategy_descriptions = {
        "trend": "基于均线交叉，回测表现最佳（+14%），简单有效",
        "momentum": "基于20日价格动量，简单直接但震荡市表现差",
        "multifactor": "融合动量、趋势、波动率、RSI多因子",
        "meanreversion": "基于布林带均值回归，胜率69%但盈亏比0.85",
        "trend_plus": "多重趋势确认，过度优化反而表现更差",
        "adaptive": "根据市场状态自动切换策略，适应性更强"
    }

    st.caption(f"💡 策略说明: {strategy_descriptions.get(strategy, '')}")

    # 启动按钮
    col1, col2, col3 = st.columns([1, 1, 3])
    with col1:
        start_backtest = st.button("🚀 开始回测", use_container_width=True, type="primary")
    with col2:
        if st.button("📊 生成新数据", use_container_width=True):
            if 'backtest_data' in st.session_state:
                del st.session_state.backtest_data
            st.rerun()

    # 执行回测
    if start_backtest:
        try:
            if data_source == "真实数据":
                # 使用真实数据回测（内联版本，避免导入问题）
                with st.spinner("🔄 正在执行真实数据回测..."):
                    try:
                        # 导入必要的模块
                        import duckdb
                        import os

                        # 使用用户选择的数据库
                        db_path = real_db_path

                        if not db_path or not os.path.exists(db_path):
                            st.error("❌ 数据库文件不存在，请先选择有效的数据库")
                            st.code("python3 scripts/download_cs300.py")
                            return

                        conn = duckdb.connect(db_path)

                        # 检测数据库格式（Tushare格式 vs 原始格式）
                        # 通过尝试查询 trade_date 来判断
                        try:
                            test_result = conn.execute("SELECT trade_date FROM daily_quotes LIMIT 1").fetchone()
                            is_tushare_format = True
                            date_col = 'trade_date'
                            symbol_col = 'ts_code'
                        except:
                            is_tushare_format = False
                            date_col = 'date'
                            symbol_col = 'symbol'

                        # 检查数据
                        count = conn.execute(f"SELECT COUNT(*) FROM daily_quotes").fetchone()[0]
                        symbol_count = conn.execute(f"SELECT COUNT(DISTINCT {symbol_col}) FROM daily_quotes").fetchone()[0]

                        if count == 0:
                            st.error("❌ 数据库中没有数据，请先下载数据")
                            st.code("python3 scripts/download_cs300.py")
                            conn.close()
                            return

                        # 加载数据（根据格式选择列）
                        if is_tushare_format:
                            # Tushare格式需要转换
                            quotes = conn.execute(f"""
                                SELECT {date_col} as date, {symbol_col} as symbol, close
                                FROM daily_quotes
                                ORDER BY {date_col}, {symbol_col}
                            """).fetchdf()
                            # 转换日期格式 YYYYMMDD -> YYYY-MM-DD
                            quotes['date'] = pd.to_datetime(quotes['date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
                        else:
                            # 原始格式直接使用
                            quotes = conn.execute(f"""
                                SELECT {date_col} as date, {symbol_col} as symbol, close
                                FROM daily_quotes
                                ORDER BY {date_col}, {symbol_col}
                            """).fetchdf()

                        conn.close()

                        # 获取数据时间范围
                        date_range = (quotes['date'].min(), quotes['date'].max())
                        real_db_info = {
                            'count': count,
                            'symbol_count': symbol_count,
                            'start_date': str(date_range[0]),
                            'end_date': str(date_range[1]),
                            'is_tushare_format': is_tushare_format
                        }

                        # 转换为价格矩阵
                        price_df = quotes.pivot(index='date', columns='symbol', values='close')
                        price_df = price_df.ffill().bfill()

                        # 使用选择的策略生成信号
                        try:
                            from backtest.signal_strategies import get_strategy
                            signal_generator = get_strategy(strategy)
                            st.info(f"📈 使用策略: {signal_generator.name}")
                            signal_df = signal_generator.generate(price_df)
                        except Exception as e:
                            import traceback
                            st.error(f"❌ 策略加载失败: {strategy}")
                            st.code(f"错误: {str(e)}\n\n{traceback.format_exc()}")
                            st.warning("将使用20日动量策略作为替代")

                            # 回退到简单动量策略
                            signal_df = pd.DataFrame(index=price_df.index, columns=price_df.columns)
                            for stock in price_df.columns:
                                prices = price_df[stock]
                                momentum_20 = prices.pct_change(20)
                                mean = momentum_20.mean()
                                std = momentum_20.std()
                                if std > 0:
                                    normalized = (momentum_20 - mean) / std
                                    signal_df[stock] = normalized.clip(-1, 1)
                                else:
                                    signal_df[stock] = 0
                            signal_df = signal_df.fillna(0)

                        # 执行回测
                        if use_dynamic_stops:
                            # 使用带止盈止损的回测
                            try:
                                from backtest.stop_loss import run_backtest_with_stops, calculate_metrics_with_stop_loss

                                st.info("🛡️ 使用动态止盈止损模式")

                                result = run_backtest_with_stops(
                                    price_df=price_df,
                                    signal_df=signal_df,
                                    buy_threshold=buy_threshold,
                                    sell_threshold=sell_threshold,
                                    initial_cash=initial_cash,
                                    position_size=position_size,
                                    max_positions=max_positions,
                                    use_dynamic_stops=True,
                                    initial_stop_loss=initial_stop_loss,
                                    trailing_stop_pct=trailing_stop,
                                    take_profit_pct=take_profit
                                )

                                equity_df = result['equity_df']
                                trades_df = result['trades']

                                # 添加 stop_price 列（如果有）
                                if 'stop_price' not in trades_df.columns:
                                    trades_df['stop_price'] = None

                                # 计算指标
                                metrics = calculate_metrics_with_stop_loss(
                                    equity_df, trades_df, initial_cash
                                )

                                # 确保所有必需的指标都存在
                                required_metrics = {
                                    'total_return': 0,
                                    'max_drawdown': 0,
                                    'sharpe_ratio': 0,
                                    'win_rate': 0,
                                    'profit_loss_ratio': 0,
                                    'total_trades': 0
                                }
                                for key, default_val in required_metrics.items():
                                    if key not in metrics or metrics[key] is None:
                                        metrics[key] = default_val

                                # 验证数据完整性
                                if equity_df.empty or 'total_equity' not in equity_df.columns:
                                    raise ValueError("回测数据无效：equity_df为空或缺少total_equity列")

                                # 清理数据中的NaN和Infinity（防止前端格式化错误）
                                import numpy as np
                                equity_df = equity_df.replace([np.inf, -np.inf], np.nan).fillna(method='ffill').fillna(method='bfill')
                                if equity_df['total_equity'].isna().any():
                                    equity_df['total_equity'] = equity_df['total_equity'].fillna(initial_cash)

                                # 保存止盈止损回测结果（止盈止损配置放在config中）
                                st.session_state.backtest_data = {
                                    'equity_df': equity_df,
                                    'trades_df': trades_df,
                                    'metrics': metrics,
                                    'config': {
                                        'data_source': '真实数据',
                                        'strategy': strategy,
                                        'buy_threshold': buy_threshold,
                                        'sell_threshold': sell_threshold,
                                        'max_positions': max_positions,
                                        'use_dynamic_stops': True,
                                        'initial_stop_loss': initial_stop_loss,
                                        'trailing_stop': trailing_stop,
                                        'take_profit': take_profit,
                                        'data_info': f'{symbol_count}只股票, {count:,}条记录 | {real_db_info.get("start_date", "")} 至 {real_db_info.get("end_date", "")}'
                                    }
                                }

                                # 确保显示值有效
                                display_return = metrics.get('total_return', 0)
                                if not isinstance(display_return, (int, float)) or (isinstance(display_return, float) and (np.isnan(display_return) or np.isinf(display_return))):
                                    display_return = 0

                                st.success(f"✅ 止盈止损回测完成！总收益: {display_return:.2f}%")
                                if metrics.get('stop_loss_count', 0) > 0:
                                    st.info(f"🛡️ 止盈止损触发: {metrics['stop_loss_count']}次")

                                # 止盈止损回测成功，跳过标准回测
                                use_dynamic_stops = False  # 设为False以跳过后续标准回测

                            except Exception as e:
                                import traceback
                                st.error(f"❌ 止盈止损回测失败: {e}")
                                st.code(traceback.format_exc())
                                st.warning("使用标准回测模式")
                                use_dynamic_stops = False

                        # 标准回测模式（仅在未使用止盈止损或止盈止损失败时执行）
                        if not use_dynamic_stops and 'backtest_data' not in st.session_state:
                            # 执行回测（标准模式）
                            dates = price_df.index
                            symbols = price_df.columns.tolist()

                            cash = initial_cash
                            positions = {}
                            equity_curve = []
                            trades = []

                            # 获取动态仓位倍数（如果启用）
                            position_multipliers = None
                            if use_dynamic_position and strategy == 'trend':
                                try:
                                    from backtest.signal_strategies import TrendFollowingStrategy
                                    temp_strategy = get_strategy(strategy)
                                    if hasattr(temp_strategy, 'get_position_size'):
                                        position_multipliers = temp_strategy.get_position_size(price_df, position_size)
                                        st.info(f"💰 启用动态仓位管理: {min_position_mult}x ~ {max_position_mult}x")
                                except Exception as e:
                                    st.warning(f"动态仓位计算失败: {e}，使用固定仓位")
                                    position_multipliers = None

                            # 创建进度条
                            progress_bar = st.progress(0)
                            progress_text = st.empty()

                            for i, date in enumerate(dates):
                                # 更新进度
                                progress = (i + 1) / len(dates)
                                progress_bar.progress(progress)
                                progress_text.text(f"🔄 回测进度: {i+1}/{len(dates)} ({progress*100:.1f}%)")

                                daily_prices = price_df.loc[date]
                                daily_signals = signal_df.loc[date]

                                prev_signals = daily_signals * 0 if i == 0 else signal_df.iloc[i - 1]

                                # 检测跨越
                                buy_list, sell_list = [], []
                                for stock in symbols:
                                    if pd.isna(daily_prices[stock]):
                                        continue

                                    prev_sig = prev_signals.get(stock, 0)
                                    curr_sig = daily_signals.get(stock, 0)

                                    if prev_sig < buy_threshold and curr_sig >= buy_threshold:
                                        buy_list.append((stock, curr_sig))
                                    elif prev_sig > sell_threshold and curr_sig <= sell_threshold:
                                        sell_list.append(stock)

                                # 执行卖出
                                for stock in sell_list:
                                    if stock in positions:
                                        pos = positions[stock]
                                        pnl = (daily_prices[stock] - pos['entry_price']) * pos['shares']
                                        trades.append({
                                            'date': date, 'stock': stock, 'action': '卖出',
                                            'price': daily_prices[stock], 'pnl': pnl,
                                            'entry_price': pos['entry_price'], 'shares': pos['shares'],
                                            'position_mult': pos.get('position_mult', 1.0)
                                        })
                                        cash += pos['shares'] * daily_prices[stock]
                                        del positions[stock]

                                # 执行买入（支持动态仓位）
                                buy_list.sort(key=lambda x: x[1], reverse=True)
                                for stock, signal_strength in buy_list:
                                    if len(positions) < max_positions and cash >= position_size * 0.5:
                                        price = daily_prices[stock]

                                        # 计算动态仓位大小
                                        if position_multipliers is not None and date in position_multipliers.index:
                                            mult = position_multipliers.loc[date, stock]
                                            mult = max(min_position_mult, min(max_position_mult, mult))
                                        else:
                                            mult = 1.0

                                        actual_position_size = position_size * mult
                                        shares = int((actual_position_size / price) / 100) * 100

                                        if shares >= 100 and cash >= shares * price:
                                            positions[stock] = {
                                                'shares': shares,
                                                'entry_price': price,
                                                'position_mult': mult
                                            }
                                            cash -= shares * price
                                            trades.append({
                                                'date': date, 'stock': stock, 'action': '买入',
                                                'price': price, 'shares': shares, 'position_mult': mult
                                            })

                                # 计算净值
                                position_value = sum(
                                    pos['shares'] * daily_prices.get(s, pos['entry_price'])
                                    for s, pos in positions.items()
                                )
                                equity_curve.append({
                                    'date': date,
                                    'cash': cash,
                                    'position_value': position_value,
                                    'total_equity': cash + position_value
                                })

                            # 清理进度条（循环结束后）
                            progress_bar.empty()
                            progress_text.empty()

                            # 转换结果
                            equity_df = pd.DataFrame(equity_curve)
                            equity_df.set_index('date', inplace=True)

                            trades_df = pd.DataFrame(trades) if trades else pd.DataFrame()

                            # 计算指标
                            final_equity = equity_df['total_equity'].iloc[-1]
                            total_return = (final_equity - initial_cash) / initial_cash * 100

                            equity_df['cummax'] = equity_df['total_equity'].cummax()
                            equity_df['drawdown'] = (equity_df['total_equity'] - equity_df['cummax']) / equity_df['cummax']
                            max_drawdown = equity_df['drawdown'].min() * 100

                            daily_returns = equity_df['total_equity'].pct_change().dropna()
                            if len(daily_returns) > 0 and daily_returns.std() > 0:
                                sharpe_ratio = (daily_returns.mean() * 252) / (daily_returns.std() * np.sqrt(252))
                            else:
                                sharpe_ratio = 0

                            if not trades_df.empty and 'pnl' in trades_df.columns:
                                sell_trades = trades_df[trades_df['action'] == '卖出']
                                if len(sell_trades) > 0:
                                    win_rate = (sell_trades['pnl'] > 0).sum() / len(sell_trades) * 100
                                    avg_win = sell_trades[sell_trades['pnl'] > 0]['pnl'].mean() if (sell_trades['pnl'] > 0).any() else 0
                                    avg_loss = sell_trades[sell_trades['pnl'] < 0]['pnl'].mean() if (sell_trades['pnl'] < 0).any() else 0
                                    profit_loss_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0
                                    total_trades = len(sell_trades)
                                else:
                                    win_rate = 0
                                    profit_loss_ratio = 0
                                    total_trades = 0
                            else:
                                win_rate = 0
                                profit_loss_ratio = 0
                                total_trades = 0

                            metrics = {
                                'total_return': round(total_return, 2),
                                'max_drawdown': round(max_drawdown, 2),
                                'sharpe_ratio': round(sharpe_ratio, 2),
                                'win_rate': round(win_rate, 1),
                                'profit_loss_ratio': round(profit_loss_ratio, 2),
                                'total_trades': total_trades
                            }

                            # 保存结果
                            st.session_state.backtest_data = {
                                'equity_df': equity_df,
                                'trades_df': trades_df,
                                'metrics': metrics,
                                'config': {
                                    'data_source': '真实数据',
                                    'strategy': strategy,
                                    'buy_threshold': buy_threshold,
                                    'sell_threshold': sell_threshold,
                                    'max_positions': max_positions,
                                    'use_dynamic_position': use_dynamic_position,
                                    'min_position_mult': min_position_mult,
                                    'max_position_mult': max_position_mult,
                                    'data_info': f'{symbol_count}只股票, {count:,}条记录 | {real_db_info.get("start_date", "")} 至 {real_db_info.get("end_date", "")}'
                                }
                            }

                            st.success(f"✅ 回测完成！总收益: {total_return:.2f}%")

                    except Exception as e:
                        st.error(f"❌ 回测失败: {e}")
                        import traceback
                        st.code(traceback.format_exc())

            else:
                # 使用模拟数据
                with st.spinner("🔄 正在执行双层回测..."):
                    # 第一层：向量化筛选
                    st.info("📊 第一层：生成数据和筛选中...")

                    # 生成模拟数据
                np.random.seed(42)
                dates = pd.date_range(start='2023-01-01', periods=n_days, freq='D')
                symbols = [f'{i:06d}' for i in range(600000, 600000 + n_stocks)]

                # 生成价格数据
                base_prices = np.random.uniform(10, 100, n_stocks)
                price_changes = np.random.normal(0, 0.02, (n_days, n_stocks))
                price_changes[0, :] = 0
                prices = base_prices * (1 + price_changes).cumprod(axis=0)
                price_df = pd.DataFrame(prices, index=dates, columns=symbols)

                # 生成信号数据（与价格相关）
                signal_quality = np.random.uniform(-0.05, 0.25, n_stocks)
                future_returns = price_changes.copy()
                future_returns = np.nan_to_num(future_returns, nan=0)

                signals = np.random.randn(n_days, n_stocks) * 0.3
                for i in range(n_stocks):
                    signals[:, i] += future_returns[:, i] * signal_quality[i]
                signals = np.clip(signals, -1, 1)
                signal_df = pd.DataFrame(signals, index=dates, columns=symbols)

                # 向量化筛选
                future_returns_df = price_df.pct_change().shift(-1)

                results = []
                for stock in symbols:
                    stock_signals = signal_df[stock]
                    stock_returns = future_returns_df[stock]

                    buy_mask = stock_signals >= buy_threshold
                    buy_count = buy_mask.sum()
                    buy_avg_return = stock_returns[buy_mask].mean()

                    sell_mask = stock_signals <= sell_threshold
                    sell_count = sell_mask.sum()
                    sell_avg_return = stock_returns[sell_mask].mean()

                    signal_quality = (buy_avg_return - sell_avg_return) if buy_count > 0 and sell_count > 0 else 0

                    results.append({
                        '股票代码': stock,
                        '买入信号次数': int(buy_count),
                        '买入平均收益': round(buy_avg_return * 100, 3) if buy_count > 0 else 0,
                        '卖出信号次数': int(sell_count),
                        '卖出平均收益': round(sell_avg_return * 100, 3) if sell_count > 0 else 0,
                        '信号质量': round(signal_quality * 100, 3),
                        '总信号数': int(buy_count + sell_count)
                    })

                effectiveness_df = pd.DataFrame(results)
                valid_mask = effectiveness_df['总信号数'] >= min_signals
                effectiveness_df = effectiveness_df[valid_mask].sort_values('信号质量', ascending=False)
                top_stocks_df = effectiveness_df.head(100).copy()
                valid_stocks = top_stocks_df['股票代码'].tolist()

                st.success(f"✅ 第一层完成：从 {n_stocks} 只股票中筛选出 {len(valid_stocks)} 只优质股票")

                # 第二层：循环回测
                # 创建进度条
                progress_bar = st.progress(0)
                progress_text = st.empty()

                signal_subset = signal_df[valid_stocks]
                price_subset = price_df[valid_stocks]

                cash = initial_cash
                positions = {}
                equity_curve = []
                trades = []

                for i, date in enumerate(dates):
                    # 更新进度
                    progress = (i + 1) / len(dates)
                    progress_bar.progress(progress)
                    progress_text.text(f"🎯 第二层回测: {i+1}/{len(dates)} ({progress*100:.1f}%)")
                    daily_signals = signal_subset.loc[date]
                    daily_prices = price_subset.loc[date]

                    prev_signals = daily_signals * 0 if i == 0 else signal_subset.iloc[i - 1]

                    # 检测跨越
                    buy_list, sell_list = [], []
                    for stock in valid_stocks:
                        prev_sig = prev_signals.get(stock, 0)
                        curr_sig = daily_signals.get(stock, 0)

                        if prev_sig < buy_threshold and curr_sig >= buy_threshold:
                            buy_list.append((stock, curr_sig))
                        elif prev_sig > sell_threshold and curr_sig <= sell_threshold:
                            sell_list.append(stock)

                    # 执行卖出
                    for stock in sell_list:
                        if stock in positions:
                            pos = positions[stock]
                            pnl = (daily_prices[stock] - pos['entry_price']) * pos['shares']
                            trades.append({
                                'date': date, 'stock': stock, 'action': '卖出',
                                'price': daily_prices[stock], 'pnl': pnl
                            })
                            cash += pos['shares'] * daily_prices[stock]
                            del positions[stock]

                    # 执行买入（按信号强度排序）
                    buy_list.sort(key=lambda x: x[1], reverse=True)
                    for stock, _ in buy_list:
                        if len(positions) < max_positions and cash >= position_size:
                            price = daily_prices[stock]
                            shares = int((position_size / price) / 100) * 100
                            if shares >= 100:
                                positions[stock] = {'shares': shares, 'entry_price': price}
                                cash -= shares * price
                                trades.append({
                                    'date': date, 'stock': stock, 'action': '买入',
                                    'price': price, 'shares': shares
                                })

                    # 计算净值
                    position_value = sum(
                        pos['shares'] * daily_prices.get(s, pos['entry_price'])
                        for s, pos in positions.items()
                    )
                    equity_curve.append({
                        'date': date,
                        'cash': cash,
                        'position_value': position_value,
                        'total_equity': cash + position_value
                    })

                # 清理进度条
                progress_bar.empty()
                progress_text.empty()

                equity_df = pd.DataFrame(equity_curve)
                equity_df.set_index('date', inplace=True)

                trades_df = pd.DataFrame(trades) if trades else pd.DataFrame()

                # 计算指标
                final_equity = equity_df['total_equity'].iloc[-1]
                total_return = (final_equity - initial_cash) / initial_cash * 100

                equity_df['cummax'] = equity_df['total_equity'].cummax()
                equity_df['drawdown'] = (equity_df['total_equity'] - equity_df['cummax']) / equity_df['cummax']
                max_drawdown = equity_df['drawdown'].min() * 100

                # 计算日收益率
                equity_df['daily_return'] = equity_df['total_equity'].pct_change()

                # 夏普比率（年化）
                daily_returns = equity_df['daily_return'].dropna()
                if len(daily_returns) > 0 and daily_returns.std() > 0:
                    sharpe_ratio = (daily_returns.mean() * 252) / (daily_returns.std() * np.sqrt(252))
                else:
                    sharpe_ratio = 0

                if not trades_df.empty:
                    sell_trades = trades_df[trades_df['action'] == '卖出']
                    win_rate = (sell_trades['pnl'] > 0).sum() / len(sell_trades) * 100 if len(sell_trades) > 0 else 0
                    total_trades = len(sell_trades)

                    # 盈亏比 = 平均盈利 / 平均亏损的绝对值
                    win_trades = sell_trades[sell_trades['pnl'] > 0]['pnl']
                    loss_trades = sell_trades[sell_trades['pnl'] < 0]['pnl']

                    avg_win = win_trades.mean() if len(win_trades) > 0 else 0
                    avg_loss = loss_trades.mean() if len(loss_trades) > 0 else 0

                    if avg_loss != 0:
                        profit_loss_ratio = abs(avg_win / avg_loss)
                    else:
                        profit_loss_ratio = 0
                else:
                    win_rate = 0
                    total_trades = 0
                    profit_loss_ratio = 0

                metrics = {
                    'total_return': round(total_return, 2),
                    'max_drawdown': round(max_drawdown, 2),
                    'sharpe_ratio': round(sharpe_ratio, 2),
                    'win_rate': round(win_rate, 1),
                    'profit_loss_ratio': round(profit_loss_ratio, 2),
                    'total_trades': total_trades
                }

                # 保存结果
                st.session_state.backtest_data = {
                    'equity_df': equity_df,
                    'trades_df': trades_df,
                    'effectiveness_df': top_stocks_df,
                    'metrics': metrics,
                    'config': {'n_stocks': n_stocks, 'n_days': n_days}
                }

                st.success(f"✅ 回测完成！总收益: {total_return:.2f}%")

        except Exception as e:
            st.error(f"❌ 回测失败: {e}")
            import traceback
            st.code(traceback.format_exc())

    # 显示回测结果
    if 'backtest_data' in st.session_state:
        st.divider()

        data = st.session_state.backtest_data
        metrics = data['metrics']

        # 确保所有必需的指标都存在且有效
        def safe_metric(key, default=0):
            """安全获取指标值，确保返回有效数字"""
            import math
            val = metrics.get(key, default)
            if val is None:
                return default
            # 检查是否为有效数字
            if isinstance(val, (int, float)):
                if math.isnan(val) or math.isinf(val):
                    return default
                return val
            return default

        total_return = safe_metric('total_return')
        max_drawdown = safe_metric('max_drawdown')
        sharpe_ratio = safe_metric('sharpe_ratio')
        win_rate = safe_metric('win_rate')
        profit_loss_ratio = safe_metric('profit_loss_ratio')
        total_trades = int(safe_metric('total_trades'))

        # 性能指标卡片
        try:
            col1, col2, col3, col4, col5, col6 = st.columns(6)
            with col1:
                color = '🟢' if total_return > 0 else '🔴'
                st.metric(f"{color} 总收益", str(round(total_return, 2)) + "%")
            with col2:
                dd_color = '🟢' if max_drawdown > -10 else '🟠' if max_drawdown > -20 else '🔴'
                st.metric(f"{dd_color} 最大回撤", str(round(max_drawdown, 2)) + "%")
            with col3:
                sr_color = '🟢' if sharpe_ratio > 1 else '🟠' if sharpe_ratio > 0 else '🔴'
                st.metric(f"{sr_color} 夏普比率", str(round(sharpe_ratio, 2)))
            with col4:
                wr_color = '🟢' if win_rate > 50 else '🟠' if win_rate > 40 else '🔴'
                st.metric(f"{wr_color} 胜率", str(round(win_rate, 1)) + "%")
            with col5:
                pl_color = '🟢' if profit_loss_ratio > 1.5 else '🟠' if profit_loss_ratio > 1 else '🔴'
                st.metric(f"{pl_color} 盈亏比", str(round(profit_loss_ratio, 2)))
            with col6:
                st.metric("📈 交易次数", int(total_trades))
        except Exception as e:
            st.error(f"显示指标时出错: {e}")
            st.write(f"调试: total_return={total_return} ({type(total_return)})")

        st.divider()

        # 结果展示
        tab1, tab2, tab3 = st.tabs(["📈 净值曲线", "📋 交易明细", "🎯 筛选结果"])

        with tab1:
            equity_df = data['equity_df'].copy()
            # 确保所有数值列都是有效的 float 类型
            import numpy as np
            for col in equity_df.columns:
                if equity_df[col].dtype in ['float64', 'object']:
                    equity_df[col] = pd.to_numeric(equity_df[col], errors='coerce').fillna(0)

            # 确保 index 是 datetime 类型
            if not isinstance(equity_df.index, pd.DatetimeIndex):
                try:
                    equity_df.index = pd.to_datetime(equity_df.index)
                except:
                    pass

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=equity_df.index,
                y=equity_df['total_equity'],
                name='净值',
                line=dict(color='#1f77b4', width=2)
            ))
            fig.update_layout(
                title='净值曲线',
                xaxis_title='日期',
                yaxis_title='净值',
                height=400
            )
            st.plotly_chart(fig)

        with tab2:
            trades_df = data['trades_df']
            if trades_df.empty:
                st.info("暂无交易记录")
            else:
                # 确保所有数值列都是有效的类型
                import numpy as np
                for col in trades_df.columns:
                    if trades_df[col].dtype in ['float64', 'object']:
                        trades_df[col] = pd.to_numeric(trades_df[col], errors='coerce').fillna(0)
                st.dataframe(trades_df, height=400)

        with tab3:
            if 'effectiveness_df' in data:
                # 模拟数据：显示筛选结果
                effectiveness_df = data['effectiveness_df']
                st.dataframe(effectiveness_df, height=400)
            else:
                # 真实数据：显示回测配置信息
                st.info("📊 真实数据回测模式")
                if 'metrics' in data:
                    st.subheader("回测配置")
                    config = data.get('config', {})
                    metrics = data['metrics']

                    config_col1, config_col2 = st.columns(2)
                    with config_col1:
                        initial_cash_val = metrics.get('initial_cash', 0)
                        if not isinstance(initial_cash_val, (int, float)) or (isinstance(initial_cash_val, float) and (np.isnan(initial_cash_val) or np.isinf(initial_cash_val))):
                            initial_cash_val = 100000
                        st.metric("初始资金", "¥" + str(int(initial_cash_val)))
                        st.metric("数据来源", str(config.get('data_source', '真实数据')))
                    with config_col2:
                        buy_threshold = config.get('buy_threshold', 0.3)
                        if not isinstance(buy_threshold, (int, float)) or (isinstance(buy_threshold, float) and (np.isnan(buy_threshold) or np.isinf(buy_threshold))):
                            buy_threshold = 0.3
                        sell_threshold = config.get('sell_threshold', -0.3)
                        if not isinstance(sell_threshold, (int, float)) or (isinstance(sell_threshold, float) and (np.isnan(sell_threshold) or np.isinf(sell_threshold))):
                            sell_threshold = -0.3
                        st.metric("买入阈值", str(round(buy_threshold, 2)))
                        st.metric("卖出阈值", str(round(sell_threshold, 2)))

                    if 'data_info' in config:
                        st.info(f"📈 数据范围: {config['data_info']}")

                    # 显示策略信息
                    strategy_map = {
                        'momentum': '20日动量',
                        'multifactor': '多因子融合',
                        'trend': '趋势跟踪',
                        'trend_plus': '趋势增强',
                        'meanreversion': '均值回归',
                        'adaptive': '自适应切换'
                    }
                    strategy_key = config.get('strategy', 'trend')
                    strategy_display = strategy_map.get(strategy_key, '趋势跟踪')
                    st.caption(f"💡 信号策略: {strategy_display}")

                    # 显示动态仓位管理信息
                    if config.get('use_dynamic_position', False):
                        st.info(f"💰 动态仓位管理: {config.get('min_position_mult', 1.0)}x ~ {config.get('max_position_mult', 2.0)}x")
                    else:
                        st.caption("💰 仓位管理: 固定仓位")


if __name__ == "__main__":
    main()
