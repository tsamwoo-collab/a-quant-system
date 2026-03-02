"""
第二层：循环回测层
对筛选出的优质股票进行精确交易模拟
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime


class LoopBacktester:
    """循环回测器"""

    def __init__(self,
                 initial_cash: float = 100000,
                 max_positions: int = 5,
                 position_size: float = 20000,
                 buy_threshold: float = 0.3,
                 sell_threshold: float = -0.3,
                 commission: float = 0.0003):  # 万分之三
        self.initial_cash = initial_cash
        self.max_positions = max_positions
        self.position_size = position_size
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold
        self.commission = commission

    def run(self, valid_stocks: List[str], signal_df: pd.DataFrame,
             price_df: pd.DataFrame) -> Dict:
        """
        运行循环回测

        Args:
            valid_stocks: 第一层筛选的股票列表
            signal_df: 信号矩阵
            price_df: 价格矩阵

        Returns:
            回测结果字典
        """
        # 只处理有效股票
        signal_subset = signal_df[valid_stocks].copy()
        price_subset = price_df[valid_stocks].copy()

        # 对齐日期
        common_index = signal_subset.index.intersection(price_subset.index)
        signal_subset = signal_subset.loc[common_index]
        price_subset = price_subset.loc[common_index]

        # 初始化
        cash = self.initial_cash
        positions = {}  # {stock: {'shares': int, 'entry_price': float}}
        equity_curve = []
        trades = []

        # 逐日循环
        for i, date in enumerate(common_index):
            daily_signals = signal_subset.loc[date]
            daily_prices = price_subset.loc[date]

            # 获取昨日信号
            if i == 0:
                prev_signals = pd.Series(0, index=valid_stocks)
            else:
                prev_signals = signal_subset.iloc[i - 1]

            # 检测交易信号
            buy_signals, sell_signals = self._detect_crossings(
                prev_signals, daily_signals, valid_stocks
            )

            # 执行卖出
            for stock in sell_signals:
                if stock in positions:
                    trade = self._execute_sell(stock, daily_prices[stock], date, positions)
                    trades.append(trade)

            # 执行买入
            buy_candidates = self._prioritize_buys(buy_signals, daily_signals)
            for stock in buy_candidates:
                if len(positions) < self.max_positions and cash >= self.position_size:
                    trade = self._execute_buy(stock, daily_prices[stock], date, cash, positions)
                    if trade:
                        trades.append(trade)
                        cash = trade['cash_after']

            # 计算当日净值
            position_value = sum(
                pos['shares'] * daily_prices.get(stock, pos['entry_price'])
                for stock, pos in positions.items()
            )
            total_equity = cash + position_value

            equity_curve.append({
                'date': date,
                'cash': cash,
                'position_value': position_value,
                'total_equity': total_equity,
                'positions_count': len(positions)
            })

        # 转换为DataFrame
        equity_df = pd.DataFrame(equity_curve)
        equity_df.set_index('date', inplace=True)

        trades_df = pd.DataFrame(trades) if trades else pd.DataFrame()

        # 计算性能指标
        metrics = self._calculate_metrics(equity_df, trades_df)

        return {
            'equity_curve': equity_df,
            'trades': trades_df,
            'metrics': metrics,
            'final_positions': positions
        }

    def _detect_crossings(self, prev_signals: pd.Series, curr_signals: pd.Series,
                         stocks: List[str]) -> tuple:
        """检测跨越信号"""
        buy_signals = []
        sell_signals = []

        for stock in stocks:
            if stock not in curr_signals:
                continue

            prev_sig = prev_signals.get(stock, 0)
            curr_sig = curr_signals[stock]

            # 买入跨越
            if prev_sig < self.buy_threshold and curr_sig >= self.buy_threshold:
                buy_signals.append(stock)

            # 卖出跨越
            elif prev_sig > self.sell_threshold and curr_sig <= self.sell_threshold:
                sell_signals.append(stock)

        return buy_signals, sell_signals

    def _prioritize_buys(self, buy_signals: List[str], signals: pd.Series) -> List[str]:
        """按信号强度排序买入候选"""
        signal_values = [(s, signals.get(s, 0)) for s in buy_signals]
        signal_values.sort(key=lambda x: x[1], reverse=True)
        return [s[0] for s in signal_values]

    def _execute_buy(self, stock: str, price: float, date, cash: float,
                     positions: Dict) -> Optional[Dict]:
        """执行买入"""
        if price <= 0 or np.isnan(price):
            return None

        # 计算买入数量
        max_shares = int((self.position_size / price) / 100) * 100  # 整手

        if max_shares < 100:
            return None

        # 计算手续费
        cost = max_shares * price
        commission = cost * self.commission
        total_cost = cost + commission

        if total_cost > cash:
            return None

        # 更新持仓
        positions[stock] = {
            'shares': max_shares,
            'entry_price': price,
            'entry_date': date
        }

        return {
            'date': date,
            'stock': stock,
            'action': '买入',
            'price': price,
            'shares': max_shares,
            'amount': cost,
            'commission': commission,
            'cash_after': cash - total_cost
        }

    def _execute_sell(self, stock: str, price: float, date,
                      positions: Dict) -> Dict:
        """执行卖出"""
        if stock not in positions:
            return None

        pos = positions[stock]
        shares = pos['shares']
        entry_price = pos['entry_price']

        # 计算手续费
        proceeds = shares * price
        commission = proceeds * self.commission
        net_proceeds = proceeds - commission

        # 计算盈亏
        pnl = (price - entry_price) * shares - commission
        pnl_pct = ((price - entry_price) / entry_price) * 100

        # 移除持仓
        del positions[stock]

        return {
            'date': date,
            'stock': stock,
            'action': '卖出',
            'price': price,
            'shares': shares,
            'entry_price': entry_price,
            'amount': proceeds,
            'commission': commission,
            'pnl': pnl,
            'pnl_pct': pnl_pct
        }

    def _calculate_metrics(self, equity_df: pd.DataFrame, trades_df: pd.DataFrame) -> Dict:
        """计算性能指标"""
        if equity_df.empty:
            return {}

        final_equity = equity_df['total_equity'].iloc[-1]
        total_return = (final_equity - self.initial_cash) / self.initial_cash

        # 计算日收益率
        equity_df['daily_return'] = equity_df['total_equity'].pct_change()

        # 最大回撤
        equity_df['cummax'] = equity_df['total_equity'].cummax()
        equity_df['drawdown'] = (equity_df['total_equity'] - equity_df['cummax']) / equity_df['cummax']
        max_drawdown = equity_df['drawdown'].min()

        # 夏普比率
        daily_returns = equity_df['daily_return'].dropna()
        if len(daily_returns) > 0 and daily_returns.std() > 0:
            sharpe_ratio = (daily_returns.mean() * 252) / (daily_returns.std() * np.sqrt(252))
        else:
            sharpe_ratio = 0

        # 胜率
        if not trades_df.empty and 'pnl' in trades_df.columns:
            sell_trades = trades_df[trades_df['action'] == '卖出']
            win_rate = (sell_trades['pnl'] > 0).sum() / len(sell_trades) if len(sell_trades) > 0 else 0
            avg_win = sell_trades[sell_trades['pnl'] > 0]['pnl'].mean() if (sell_trades['pnl'] > 0).any() else 0
            avg_loss = sell_trades[sell_trades['pnl'] < 0]['pnl'].mean() if (sell_trades['pnl'] < 0).any() else 0
        else:
            win_rate = 0
            avg_win = 0
            avg_loss = 0

        # 交易统计
        total_trades = len(trades_df[trades_df['action'] == '卖出']) if not trades_df.empty else 0

        return {
            'initial_cash': self.initial_cash,
            'final_equity': round(final_equity, 2),
            'total_return': round(total_return * 100, 2),
            'max_drawdown': round(max_drawdown * 100, 2),
            'sharpe_ratio': round(sharpe_ratio, 2),
            'win_rate': round(win_rate * 100, 1),
            'total_trades': total_trades,
            'avg_win': round(avg_win, 2),
            'avg_loss': round(avg_loss, 2),
            'profit_loss_ratio': round(abs(avg_win / avg_loss), 2) if avg_loss != 0 else 0
        }
