"""
持仓管理模块 - 用于跟踪模拟盘持仓
"""
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional

class PositionManager:
    """持仓管理器"""

    def __init__(self, positions_file: str = "data/positions.json"):
        self.positions_file = positions_file
        self.positions = {}
        self.load_positions()

    def load_positions(self):
        """加载持仓数据"""
        import os
        if os.path.exists(self.positions_file):
            try:
                with open(self.positions_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.positions = {k: v for k, v in data.items() if v.get('status') == 'open'}
            except Exception as e:
                print(f"⚠️ 加载持仓失败: {e}")
                self.positions = {}

    def save_positions(self):
        """保存持仓数据"""
        import os
        os.makedirs(os.path.dirname(self.positions_file), exist_ok=True)
        with open(self.positions_file, 'w', encoding='utf-8') as f:
            json.dump(self.positions, f, ensure_ascii=False, indent=2)

    def add_position(self, symbol: str, entry_price: float, shares: int,
                     entry_date: str, signal_strength: float = 0.5):
        """添加持仓"""
        self.positions[symbol] = {
            'symbol': symbol,
            'entry_price': entry_price,
            'shares': shares,
            'entry_date': entry_date,
            'signal_strength': signal_strength,
            'highest_price': entry_price,
            'status': 'open',
            'exit_reason': None,
            'exit_date': None,
            'exit_price': None
        }
        self.save_positions()

    def close_position(self, symbol: str, exit_price: float, reason: str):
        """平仓"""
        if symbol in self.positions:
            pos = self.positions[symbol]
            pos['status'] = 'closed'
            pos['exit_price'] = exit_price
            pos['exit_date'] = datetime.now().strftime("%Y-%m-%d")
            pos['exit_reason'] = reason
            self.save_positions()

    def update_highest_price(self, symbol: str, current_price: float):
        """更新最高价"""
        if symbol in self.positions and self.positions[symbol]['status'] == 'open':
            if current_price > self.positions[symbol]['highest_price']:
                self.positions[symbol]['highest_price'] = current_price
                self.save_positions()

    def check_stop_conditions(self, symbol: str, current_price: float,
                               initial_stop: float = 0.08, trailing_stop: float = 0.08,
                               take_profit: float = 0.30):
        """检查止盈止损条件

        Returns:
            dict: {'should_close': bool, 'reason': str, 'pnl_pct': float}
        """
        if symbol not in self.positions or self.positions[symbol]['status'] != 'open':
            return {'should_close': False, 'reason': '无持仓'}

        pos = self.positions[symbol]
        entry_price = pos['entry_price']
        highest_price = pos['highest_price']

        # 计算盈亏
        pnl_pct = (current_price - entry_price) / entry_price
        max_pnl_pct = (highest_price - entry_price) / entry_price

        # 1. 初始止损
        if pnl_pct <= -initial_stop:
            return {
                'should_close': True,
                'reason': f'初始止损({pnl_pct*100:.1f}%)',
                'pnl_pct': pnl_pct
            }

        # 2. 追踪止盈（盈利后）
        if max_pnl_pct > 0.03:  # 盈利3%以上才启用
            trailing_stop_price = highest_price * (1 - trailing_stop)
            if current_price <= trailing_stop_price:
                return {
                    'should_close': True,
                    'reason': f'追踪止盈(利润{max_pnl_pct*100:.1f}%)',
                    'pnl_pct': pnl_pct
                }

        # 3. 目标止盈
        if max_pnl_pct > take_profit:
            protect_pct = 0.05 + (max_pnl_pct - take_profit) * 0.3
            protect_stop = highest_price * (1 - protect_pct)
            if current_price <= protect_stop:
                return {
                    'should_close': True,
                    'reason': f'目标止盈(利润{max_pnl_pct*100:.1f}%)',
                    'pnl_pct': pnl_pct
                }

        return {
            'should_close': False,
            'reason': '持仓中',
            'pnl_pct': pnl_pct
        }

    def get_positions_summary(self, prices: Dict[str, float] = None) -> List[Dict]:
        """获取持仓摘要"""
        summary = []

        for symbol, pos in self.positions.items():
            if pos['status'] == 'open':
                current_price = prices.get(symbol, pos['entry_price'])
                pnl_pct = (current_price - pos['entry_price']) / pos['entry_price']

                summary.append({
                    'symbol': symbol,
                    'entry_price': pos['entry_price'],
                    'current_price': current_price,
                    'shares': pos['shares'],
                    'entry_date': pos['entry_date'],
                    'pnl_pct': pnl_pct,
                    'pnl_amount': (current_price - pos['entry_price']) * pos['shares'],
                    'highest_price': pos['highest_price'],
                    'signal_strength': pos['signal_strength']
                })

        return sorted(summary, key=lambda x: x['pnl_pct'], reverse=True)


def process_trading_signals(signals: dict, position_manager: PositionManager,
                              initial_stop: float = 0.08, trailing_stop: float = 0.08,
                              take_profit: float = 0.30) -> dict:
    """
    处理交易信号并更新持仓

    Returns:
        dict: 更新后的信号信息
    """
    import os

    # 获取最新价格
    prices = {}
    for sig in signals['buy_signals']:
        prices[sig['symbol']] = sig['price']
    for sig in signals['sell_signals']:
        prices[sig['symbol']] = sig['price']

    # 检查现有持仓的止盈止损
    positions_to_close = []
    for symbol in list(position_manager.positions.keys()):
        if position_manager.positions[symbol]['status'] == 'open':
            current_price = prices.get(symbol)
            if current_price:
                check = position_manager.check_stop_conditions(
                    symbol, current_price, initial_stop, trailing_stop, take_profit
                )
                if check['should_close']:
                    positions_to_close.append({
                        'symbol': symbol,
                        'reason': check['reason'],
                        'pnl_pct': check['pnl_pct'],
                        'current_price': current_price
                    })

    # 更新返回的信号信息
    enhanced_signals = signals.copy()
    enhanced_signals['positions_summary'] = []
    enhanced_signals['close_signals'] = []

    # 添加平仓信号
    for close_sig in positions_to_close:
        position_manager.close_position(close_sig['symbol'], close_sig['current_price'], close_sig['reason'])
        enhanced_signals['close_signals'].append({
            'symbol': close_sig['symbol'],
            'reason': close_sig['reason'],
            'pnl_pct': close_sig['pnl_pct']
        })

    # 获取当前持仓摘要
    if prices:
        enhanced_signals['positions_summary'] = position_manager.get_positions_summary(prices)

    return enhanced_signals
