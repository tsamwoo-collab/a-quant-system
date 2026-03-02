"""
实盘持仓管理模块
支持手动录入交易信息，对实盘持仓进行健康度体检
"""
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json


class RealPositionTracker:
    """实盘持仓追踪器"""

    def __init__(self, storage_path: str = "data/real_positions.json"):
        self.storage_path = storage_path
        self.positions: Dict[str, Dict] = {}
        self.load()

    def add_position(self, symbol: str, name: str, price: float, quantity: int, trade_date: str) -> bool:
        """添加持仓"""
        try:
            self.positions[symbol] = {
                'symbol': symbol,
                'name': name,
                'entry_price': float(price),
                'quantity': int(quantity),
                'entry_date': trade_date,
                'total_cost': float(price) * int(quantity),
                'status': '持有'
            }
            self.save()
            return True
        except Exception as e:
            st.error(f"添加失败: {e}")
            return False

    def remove_position(self, symbol: str) -> bool:
        """卖出持仓"""
        if symbol in self.positions:
            del self.positions[symbol]
            self.save()
            return True
        return False

    def update_position(self, symbol: str, current_price: float) -> Dict:
        """更新持仓价格并计算盈亏"""
        if symbol not in self.positions:
            return {}

        pos = self.positions[symbol]
        entry_price = pos['entry_price']
        quantity = pos['quantity']

        # 计算盈亏
        pnl = (current_price - entry_price) * quantity
        pnl_pct = ((current_price - entry_price) / entry_price) * 100
        market_value = current_price * quantity

        pos.update({
            'current_price': current_price,
            'pnl': pnl,
            'pnl_pct': pnl_pct,
            'market_value': market_value
        })

        return pos

    def get_positions(self) -> pd.DataFrame:
        """获取所有持仓"""
        if not self.positions:
            return pd.DataFrame()

        data = []
        for symbol, pos in self.positions.items():
            data.append({
                '代码': symbol,
                '名称': pos['name'],
                '买入价': pos['entry_price'],
                '数量': pos['quantity'],
                '成本': pos['total_cost'],
                '买入日期': pos['entry_date'],
                '状态': pos['status']
            })

        df = pd.DataFrame(data)
        return df

    def get_health_report(self, signals_df: pd.DataFrame, current_prices: Dict[str, float]) -> pd.DataFrame:
        """生成持仓健康度体检报告

        Args:
            signals_df: 当前所有股票的信号数据
            current_prices: {symbol: current_price} 当前价格字典

        Returns:
            健康度报告DataFrame
        """
        if not self.positions:
            return pd.DataFrame()

        reports = []

        for symbol, pos in self.positions.items():
            # 获取当前价格
            current_price = current_prices.get(symbol, pos['entry_price'])

            # 更新持仓信息
            updated = self.update_position(symbol, current_price)

            # 获取信号信息
            stock_signal = signals_df[signals_df['symbol'] == symbol]
            if not stock_signal.empty:
                signal_row = stock_signal.iloc[0]
                current_signal = signal_row['signal']
                signal_level = signal_row['signal_level']
                macro_score = signal_row.get('macro_score', 0)
                meso_score = signal_row.get('meso_score', 0)
                micro_score = signal_row.get('micro_score', 0)
            else:
                current_signal = 0
                signal_level = '未知'
                macro_score = 0
                meso_score = 0
                micro_score = 0

            # 计算健康度得分
            health_score = self._calculate_health_score(updated, current_signal)

            # 生成建议
            advice = self._generate_advice(updated, current_signal, health_score)

            # 持仓天数
            entry_date = datetime.strptime(pos['entry_date'], '%Y-%m-%d')
            hold_days = (datetime.now() - entry_date).days

            reports.append({
                '代码': symbol,
                '名称': pos['name'],
                '买入价': pos['entry_price'],
                '现价': current_price,
                '盈亏%': round(updated.get('pnl_pct', 0), 2),
                '市值': round(updated.get('market_value', 0), 2),
                '持仓天数': hold_days,
                '信号': round(current_signal, 3),
                '信号等级': signal_level,
                '宏观得分': round(macro_score, 3),
                '中观得分': round(meso_score, 3),
                '微观得分': round(micro_score, 3),
                '健康度': health_score,
                '健康评级': self._get_health_level(health_score),
                '建议': advice
            })

        df = pd.DataFrame(reports)
        if not df.empty:
            df = df.sort_values('健康度', ascending=False)
        return df

    def _calculate_health_score(self, pos: Dict, signal: float) -> float:
        """计算健康度得分 (0-100)

        综合考虑：
        - 盈亏情况 (30%)
        - 信号强度 (40%)
        - 持仓时长 (15%)
        - 中观微观表现 (15%)
        """
        score = 50  # 基础分

        # 盈亏得分 (0-30分)
        pnl_pct = pos.get('pnl_pct', 0)
        if pnl_pct > 20:
            score += 30
        elif pnl_pct > 10:
            score += 25
        elif pnl_pct > 5:
            score += 20
        elif pnl_pct > 0:
            score += 15
        elif pnl_pct > -5:
            score += 10
        elif pnl_pct > -10:
            score += 5
        else:
            score += 0

        # 信号得分 (0-40分)
        if signal >= 0.7:
            score += 40
        elif signal >= 0.3:
            score += 30
        elif signal >= 0:
            score += 20
        elif signal >= -0.3:
            score += 10
        else:
            score += 0

        # 持仓天数 (0-15分) - 持仓越久风险越高
        entry_date = datetime.strptime(pos['entry_date'], '%Y-%m-%d')
        hold_days = (datetime.now() - entry_date).days
        if hold_days < 30:
            score += 15
        elif hold_days < 60:
            score += 12
        elif hold_days < 90:
            score += 8
        else:
            score += 5

        return min(100, max(0, score))

    def _get_health_level(self, score: float) -> str:
        """获取健康等级"""
        if score >= 80:
            return '🟢 优秀'
        elif score >= 65:
            return '🟡 良好'
        elif score >= 50:
            return '🟠 一般'
        else:
            return '🔴 较差'

    def _generate_advice(self, pos: Dict, signal: float, health_score: float) -> str:
        """生成操作建议"""
        pnl_pct = pos.get('pnl_pct', 0)

        # 紧急止损
        if pnl_pct < -10:
            return "🔴 建议止损：亏损超过10%"
        elif signal <= -0.3:
            return "🔴 建议清仓：信号转负"

        # 减仓建议
        if pnl_pct < -5 or signal < 0:
            return "🟠 建议减仓：盈利缩水/信号走弱"

        # 持有建议
        if health_score >= 70:
            return "🟢 继续持有：健康度良好"
        elif signal >= 0.3:
            return "🟢 继续持有：信号强势"
        else:
            return "🟡 观望：等待信号明确"

    def save(self):
        """保存到文件"""
        import os
        os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
        with open(self.storage_path, 'w', encoding='utf-8') as f:
            json.dump(self.positions, f, ensure_ascii=False, indent=2)

    def load(self):
        """从文件加载"""
        import os
        if os.path.exists(self.storage_path):
            try:
                with open(self.storage_path, 'r', encoding='utf-8') as f:
                    self.positions = json.load(f)
            except:
                self.positions = {}


def get_real_tracker() -> RealPositionTracker:
    """获取实盘追踪器单例"""
    if 'real_tracker' not in st.session_state:
        st.session_state.real_tracker = RealPositionTracker()
    return st.session_state.real_tracker
