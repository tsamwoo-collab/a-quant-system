"""
持仓追踪与交易信号模块 - 捕捉"跨越雷池"瞬间
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class PositionTracker:
    """持仓追踪器 - 捕捉临界突破"""

    def __init__(self):
        self.positions = {}  # {symbol: position_data}
        self.signal_history = {}  # {symbol: [signal1, signal2, ...]}
        self.factor_history = {}  # {symbol: {factor_name: [value1, value2, ...]}}
        self.level_scores_history = {}  # {symbol: {'macro': [...], 'meso': [...], 'micro': [...]}}
        self.trade_signals = []

        # 交易阈值
        self.BUY_THRESHOLD = 0.30    # 买入红线
        self.SELL_THRESHOLD = -0.30  # 卖出红线

    def update_signals(self, current_signals: pd.DataFrame):
        """更新信号并生成交易建议"""
        today = datetime.now().strftime('%Y-%m-%d')
        trade_signals = []

        for _, row in current_signals.iterrows():
            symbol = row['symbol']
            current_signal = row['signal']
            current_level = row['signal_level']

            # 获取历史数据
            if symbol not in self.signal_history:
                self.signal_history[symbol] = []
            if symbol not in self.factor_history:
                self.factor_history[symbol] = {}
            if symbol not in self.level_scores_history:
                self.level_scores_history[symbol] = {'macro': [], 'meso': [], 'micro': []}

            prev_signal = self.signal_history[symbol][-1] if self.signal_history[symbol] else None

            # 生成带归因的交易信号
            trade_signal = self._generate_crossing_signal(
                symbol, current_signal, prev_signal, row
            )

            if trade_signal:
                trade_signals.append(trade_signal)

                # 更新持仓
                if trade_signal['action'] in ['买入', '加仓']:
                    self.positions[symbol] = {
                        'entry_signal': current_signal,
                        'entry_date': today,
                        'entry_type': trade_signal['action'],
                        'status': '持有',
                        'name': row.get('name', ''),
                        'attribution': trade_signal.get('attribution', {}),
                        'crossing_info': trade_signal.get('crossing_info', {})
                    }
                elif trade_signal['action'] in ['卖出', '清仓']:
                    if symbol in self.positions:
                        # 更新持仓为已卖出，保留历史记录
                        self.positions[symbol]['exit_signal'] = current_signal
                        self.positions[symbol]['exit_date'] = today
                        self.positions[symbol]['exit_reason'] = trade_signal['reason']
                        # 从活跃持仓中移除
                        del self.positions[symbol]

            # 更新历史数据
            self.signal_history[symbol].append(current_signal)
            if len(self.signal_history[symbol]) > 30:
                self.signal_history[symbol] = self.signal_history[symbol][-30:]

            # 更新因子历史
            factors = row.get('factors', {})
            for factor_name, factor_value in factors.items():
                if factor_name not in self.factor_history[symbol]:
                    self.factor_history[symbol][factor_name] = []
                self.factor_history[symbol][factor_name].append(factor_value)
                if len(self.factor_history[symbol][factor_name]) > 30:
                    self.factor_history[symbol][factor_name] = self.factor_history[symbol][factor_name][-30:]

            # 更新层级得分历史
            self.level_scores_history[symbol]['macro'].append(row.get('macro_score', 0))
            self.level_scores_history[symbol]['meso'].append(row.get('meso_score', 0))
            self.level_scores_history[symbol]['micro'].append(row.get('micro_score', 0))

        self.trade_signals.extend(trade_signals)
        return trade_signals

    def _generate_crossing_signal(
        self,
        symbol: str,
        current_signal: float,
        prev_signal: Optional[float],
        row_data: pd.Series
    ) -> Optional[Dict]:
        """
        生成"跨越雷池"型交易信号

        核心逻辑：
        - 买入触发：昨天 < 0.30，今天 >= 0.30
        - 卖出触发：昨天 > -0.30，今天 <= -0.30

        忽略：
        - 昨天就高、今天仍然高的（不触发）
        - 一直在谷底的（不触发）
        """
        if prev_signal is None:
            return None

        # 归因分析
        attribution = self._analyze_attribution(symbol, row_data, prev_signal, current_signal)

        # 判断是否跨越阈值
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
            priority = 3  # 高优先级
            crossing_info = {
                'type': 'buy_crossing',
                'threshold': self.BUY_THRESHOLD,
                'prev_below': True,
                'curr_above': True,
                'gap': current_signal - prev_signal
            }
            reason = f"🚨 跨越买入红线！信号 {prev_signal:.2f} → {current_signal:.2f}"

        # 卖出跨越（持有中）
        elif sell_crossing and in_position:
            action = "清仓"
            priority = 3  # 高优先级
            crossing_info = {
                'type': 'sell_crossing',
                'threshold': self.SELL_THRESHOLD,
                'prev_above': True,
                'curr_below': True,
                'gap': prev_signal - current_signal
            }
            reason = f"⚠️ 跌破卖出红线！信号 {prev_signal:.2f} → {current_signal:.2f}"

        # 减仓（持有中，信号恶化但未跌破红线）
        elif in_position and (current_signal < prev_signal) and (current_signal < 0):
            # 只有当信号明显恶化时才减仓
            if (prev_signal - current_signal) > 0.2:
                action = "减仓"
                priority = 2
                crossing_info = {
                    'type': 'deterioration',
                    'decline': prev_signal - current_signal
                }
                reason = f"📉 信号明显恶化: {prev_signal:.2f} → {current_signal:.2f}"

        # 特殊：急剧下跌，强制清仓
        elif in_position and (prev_signal - current_signal) > 0.5:
            action = "清仓"
            priority = 4  # 最高优先级
            crossing_info = {
                'type': 'crash',
                'decline': prev_signal - current_signal
            }
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

    def _analyze_attribution(
        self,
        symbol: str,
        row_data: pd.Series,
        prev_signal: float,
        current_signal: float
    ) -> Dict:
        """动能归因分析"""
        # 获取当前各层级得分
        current_macro = row_data.get('macro_score', 0)
        current_meso = row_data.get('meso_score', 0)
        current_micro = row_data.get('micro_score', 0)

        # 获取历史层级得分
        history = self.level_scores_history.get(symbol, {'macro': [], 'meso': [], 'micro': []})
        prev_macro = history['macro'][-1] if history['macro'] else 0
        prev_meso = history['meso'][-1] if history['meso'] else 0
        prev_micro = history['micro'][-1] if history['micro'] else 0

        # 计算各层级变化
        macro_change = current_macro - prev_macro
        meso_change = current_meso - prev_meso
        micro_change = current_micro - prev_micro

        # 确定主要驱动力（变化最大的层级）
        changes = {
            'macro': macro_change,
            'meso': meso_change,
            'micro': micro_change
        }

        # 按绝对值排序
        sorted_changes = sorted(changes.items(), key=lambda x: abs(x[1]), reverse=True)
        primary_driver = sorted_changes[0][0] if sorted_changes else 'macro'

        # 确定驱动力的状态
        def get_change_status(change):
            if abs(change) < 0.05:
                return 'neutral', '无显著变化'
            elif change > 0.2:
                return 'strong_up', '强势上升'
            elif change > 0:
                return 'up', '企稳回升'
            elif change < -0.2:
                return 'strong_down', '强势恶化'
            else:
                return 'down', '持续走弱'

        driver_status, driver_desc = get_change_status(changes[primary_driver])

        # 生成归因描述
        level_names = {
            'macro': '宏观环境',
            'meso': '中观行业',
            'micro': '微观量价'
        }

        attribution = {
            'primary_driver': primary_driver,
            'driver_name': level_names[primary_driver],
            'driver_status': driver_status,
            'driver_desc': driver_desc,
            'changes': changes,
            'current_scores': {
                'macro': current_macro,
                'meso': current_meso,
                'micro': current_micro
            },
            'prev_scores': {
                'macro': prev_macro,
                'meso': prev_meso,
                'micro': prev_micro
            },
            'display_items': []
        }

        # 生成展示项
        for level, name in [('macro', '宏观环境'), ('meso', '中观行业'), ('micro', '微观量价')]:
            prev_score = attribution['prev_scores'][level]
            curr_score = attribution['current_scores'][level]
            change = attribution['changes'][level]

            # 判断状态
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
                'level': level,
                'name': name,
                'icon': icon,
                'prev_score': round(prev_score, 2),
                'current_score': round(curr_score, 2),
                'change': round(change, 2),
                'status': status_text,
                'is_driver': level == primary_driver
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
                'entry_signal': pos['entry_signal'],
                'entry_date': pos['entry_date'],
                'current_signal': current_signal,
                'status': pos['status'],
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

                # 只显示有跨越的
                buy_cross = (prev < self.BUY_THRESHOLD) and (current >= self.BUY_THRESHOLD)
                sell_cross = (prev > self.SELL_THRESHOLD) and (current <= self.SELL_THRESHOLD)

                if buy_cross or sell_cross or abs(change) >= 0.1:
                    changes.append({
                        'symbol': symbol,
                        'name': row.get('name', ''),
                        'prev_signal': round(prev, 3),
                        'current_signal': round(current, 3),
                        'change': round(change, 3),
                        'buy_crossing': buy_cross,
                        'sell_crossing': sell_cross
                    })

        df = pd.DataFrame(changes)
        if not df.empty:
            df = df.sort_values('change', ascending=False)

        return df

    def get_daily_summary(self) -> Dict:
        """获取每日汇总"""
        positions_df = self.get_positions()
        signals_df = self.get_trade_signals()

        # 统计跨越次数
        crossing_signals = [s for s in self.trade_signals
                          if s.get('crossing_info', {}).get('type') in ['buy_crossing', 'sell_crossing']]

        return {
            'total_positions': len(positions_df) if not positions_df.empty else 0,
            'buy_signals': len(signals_df[signals_df['action'] == '买入']) if not signals_df.empty else 0,
            'sell_signals': len(signals_df[signals_df['action'] == '清仓']) if not signals_df.empty else 0,
            'reduce_signals': len(signals_df[signals_df['action'] == '减仓']) if not signals_df.empty else 0,
            'crossing_count': len(crossing_signals),
        }


# 全局追踪器
_tracker = None

def get_tracker() -> PositionTracker:
    """获取追踪器单例"""
    global _tracker
    if _tracker is None:
        _tracker = PositionTracker()
    return _tracker
