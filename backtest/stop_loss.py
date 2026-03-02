"""
动态止盈止损模块
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional


class PositionTracker:
    """持仓跟踪器，用于管理动态止盈止损"""

    def __init__(
        self,
        initial_stop_loss: float = 0.05,      # 初始止损 5%
        trailing_stop_pct: float = 0.03,       # 追踪止盈 3%
        take_profit_pct: float = 0.15,         # 目标止盈 15%
        profit_protect_pct: float = 0.05,      # 利润保护 5%
        max_profit_protect: float = 0.30       # 最大利润保护 30%
    ):
        """
        Args:
            initial_stop_loss: 初始止损比例（从入场价算）
            trailing_stop_pct: 追踪止盈比例（从最高价算）
            take_profit_pct: 目标止盈比例（达到后开始保护）
            profit_protect_pct: 利润保护比例（盈利后的最小回撤）
            max_profit_protect: 最大利润保护比例
        """
        self.initial_stop_loss = initial_stop_loss
        self.trailing_stop_pct = trailing_stop_pct
        self.take_profit_pct = take_profit_pct
        self.profit_protect_pct = profit_protect_pct
        self.max_profit_protect = max_profit_protect

    def check_position(
        self,
        current_price: float,
        entry_price: float,
        highest_price: float,
        shares: int,
        entry_date: pd.Timestamp
    ) -> Dict:
        """检查持仓是否需要平仓

        Args:
            current_price: 当前价格
            entry_price: 入场价
            highest_price: 持仓期间最高价
            shares: 持仓数量
            entry_date: 入场日期

        Returns:
            {
                'should_close': bool, 是否需要平仓
                'close_reason': str, 平仓原因
                'stop_price': float, 止损/止盈价格
            }
        """
        current_pnl_pct = (current_price - entry_price) / entry_price
        max_pnl_pct = (highest_price - entry_price) / entry_price

        should_close = False
        close_reason = ""
        stop_price = entry_price

        # 1. 初始止损（亏损）
        if current_pnl_pct <= -self.initial_stop_loss:
            should_close = True
            close_reason = "初始止损"
            stop_price = entry_price * (1 - self.initial_stop_loss)

        # 2. 追踪止盈（盈利后）
        elif current_pnl_pct > 0:
            # 计算追踪止盈价
            if max_pnl_pct > self.take_profit_pct:
                # 达到目标止盈，开始保护利润
                protect_pct = min(
                    self.max_profit_protect,
                    self.profit_protect_pct + (max_pnl_pct - self.take_profit_pct) * 0.5
                )
                trailing_stop = highest_price * (1 - protect_pct)

                if current_price <= trailing_stop:
                    should_close = True
                    close_reason = f"追踪止盈(利润{max_pnl_pct*100:.1f}%)"
                    stop_price = trailing_stop

            # 小幅追踪止盈（还未达到目标）
            elif max_pnl_pct > 0.03:
                trailing_stop = highest_price * (1 - self.trailing_stop_pct)
                if current_price <= trailing_stop:
                    should_close = True
                    close_reason = f"追踪止盈(利润{max_pnl_pct*100:.1f}%)"
                    stop_price = trailing_stop

        return {
            'should_close': should_close,
            'close_reason': close_reason if should_close else "",
            'stop_price': stop_price,
            'current_pnl_pct': current_pnl_pct,
            'max_pnl_pct': max_pnl_pct
        }


def run_backtest_with_stops(
    price_df: pd.DataFrame,
    signal_df: pd.DataFrame,
    buy_threshold: float = 0.3,
    sell_threshold: float = -0.3,
    initial_cash: float = 100000,
    position_size: float = 20000,
    max_positions: int = 5,
    # 止盈止损参数
    use_dynamic_stops: bool = True,
    initial_stop_loss: float = 0.05,
    trailing_stop_pct: float = 0.03,
    take_profit_pct: float = 0.15
) -> Dict:
    """执行带动态止盈止损的回测"""

    dates = price_df.index
    symbols = price_df.columns.tolist()

    cash = initial_cash
    positions = {}  # {symbol: {shares, entry_price, highest_price, entry_date, stop_price}}
    equity_curve = []
    trades = []

    for i, date in enumerate(dates):
        daily_prices = price_df.loc[date]
        daily_signals = signal_df.loc[date]

        if i == 0:
            prev_signals = pd.Series(0, index=symbols)
        else:
            prev_signals = signal_df.iloc[i - 1]

        # 检查平仓（信号触发 + 止盈止损）
        sell_list = []
        for stock in list(positions.keys()):
            pos = positions[stock]

            # 1. 信号触发卖出
            prev_sig = prev_signals.get(stock, 0)
            curr_sig = daily_signals.get(stock, 0)
            signal_sell = prev_sig > sell_threshold and curr_sig <= sell_threshold

            # 2. 动态止盈止损检查
            stop_sell = False
            stop_reason = ""

            if use_dynamic_stops:
                price = daily_prices[stock]
                tracker = PositionTracker(
                    initial_stop_loss=initial_stop_loss,
                    trailing_stop_pct=trailing_stop_pct,
                    take_profit_pct=take_profit_pct
                )
                stop_check = tracker.check_position(
                    current_price=price,
                    entry_price=pos['entry_price'],
                    highest_price=pos['highest_price'],
                    shares=pos['shares'],
                    entry_date=pos['entry_date']
                )

                if stop_check['should_close']:
                    stop_sell = True
                    stop_reason = stop_check['close_reason']
                    pos['stop_price'] = stop_check['stop_price']

            # 执行卖出
            if signal_sell or stop_sell:
                price = daily_prices[stock]
                pnl = (price - pos['entry_price']) * pos['shares']

                reason = "信号卖出" if signal_sell else stop_reason
                trades.append({
                    'date': date,
                    'stock': stock,
                    'action': '卖出',
                    'price': price,
                    'entry_price': pos['entry_price'],
                    'shares': pos['shares'],
                    'pnl': pnl,
                    'reason': reason
                })
                cash += pos['shares'] * price
                del positions[stock]

        # 检测买入信号
        buy_list = []
        for stock in symbols:
            if pd.isna(daily_prices[stock]) or stock in positions:
                continue

            prev_sig = prev_signals.get(stock, 0)
            curr_sig = daily_signals.get(stock, 0)

            if prev_sig < buy_threshold and curr_sig >= buy_threshold:
                buy_list.append((stock, curr_sig))

        # 执行买入（按信号强度排序）
        buy_list.sort(key=lambda x: x[1], reverse=True)
        for stock, _ in buy_list:
            if len(positions) < max_positions and cash >= position_size:
                price = daily_prices[stock]
                shares = int((position_size / price) / 100) * 100

                if shares >= 100 and cash >= shares * price:
                    positions[stock] = {
                        'shares': shares,
                        'entry_price': price,
                        'highest_price': price,
                        'entry_date': date,
                        'stop_price': price * (1 - initial_stop_loss) if use_dynamic_stops else None
                    }
                    cash -= shares * price
                    trades.append({
                        'date': date,
                        'stock': stock,
                        'action': '买入',
                        'price': price,
                        'shares': shares,
                        'reason': '信号买入'
                    })

        # 更新最高价
        for stock in positions:
            price = daily_prices.get(stock, positions[stock]['entry_price'])
            if price and price > positions[stock]['highest_price']:
                positions[stock]['highest_price'] = price

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

    # 转换为DataFrame
    equity_df = pd.DataFrame(equity_curve)
    equity_df.set_index('date', inplace=True)

    trades_df = pd.DataFrame(trades) if trades else pd.DataFrame()

    return {
        'equity_df': equity_df,
        'trades': trades_df,
        'positions_count': len(positions),
        'trades_count': len(trades)
    }


def calculate_metrics_with_stop_loss(
    equity_df: pd.DataFrame,
    trades_df: pd.DataFrame,
    initial_cash: float
) -> Dict:
    """计算回测指标（含止盈止损分析）"""

    if equity_df.empty:
        return {}

    final_equity = equity_df['total_equity'].iloc[-1]
    total_return = (final_equity - initial_cash) / initial_cash * 100

    # 最大回撤
    equity_df['cummax'] = equity_df['total_equity'].cummax()
    equity_df['drawdown'] = (equity_df['total_equity'] - equity_df['cummax']) / equity_df['cummax']
    max_drawdown = equity_df['drawdown'].min() * 100

    # 夏普比率
    daily_returns = equity_df['total_equity'].pct_change().dropna()
    if len(daily_returns) > 0 and daily_returns.std() > 0:
        sharpe_ratio = (daily_returns.mean() * 252) / (daily_returns.std() * np.sqrt(252))
    else:
        sharpe_ratio = 0

    # 交易分析
    if not trades_df.empty and 'action' in trades_df.columns:
        sell_trades = trades_df[trades_df['action'] == '卖出']

        if len(sell_trades) > 0:
            win_rate = (sell_trades['pnl'] > 0).sum() / len(sell_trades) * 100

            win_trades = sell_trades[sell_trades['pnl'] > 0]['pnl']
            loss_trades = sell_trades[sell_trades['pnl'] < 0]['pnl']

            avg_win = win_trades.mean() if len(win_trades) > 0 else 0
            avg_loss = loss_trades.mean() if len(loss_trades) > 0 else 0

            profit_loss_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0
            total_trades = len(sell_trades)

            # 分析平仓原因
            if 'reason' in sell_trades.columns:
                reason_stats = sell_trades['reason'].value_counts()
                stop_loss_trades = sell_trades[sell_trades['reason'].str.contains('止损|止盈', na=False)]
                stop_loss_count = len(stop_loss_trades)
            else:
                reason_stats = None
                stop_loss_count = 0
        else:
            win_rate = 0
            profit_loss_ratio = 0
            total_trades = 0
            reason_stats = None
            stop_loss_count = 0
    else:
        win_rate = 0
        profit_loss_ratio = 0
        total_trades = 0
        reason_stats = None
        stop_loss_count = 0

    # 确保所有值都是有效数字（处理NaN和Infinity）
    import math
    def safe_round(value, decimals=2):
        """安全四舍五入，处理NaN和Infinity"""
        if value is None or math.isnan(value) or math.isinf(value):
            return 0.0
        return round(value, decimals)

    return {
        'initial_cash': initial_cash,
        'final_equity': safe_round(final_equity, 2),
        'total_return': safe_round(total_return, 2),
        'max_drawdown': safe_round(max_drawdown, 2),
        'sharpe_ratio': safe_round(sharpe_ratio, 2),
        'win_rate': safe_round(win_rate, 1),
        'profit_loss_ratio': safe_round(profit_loss_ratio, 2),
        'total_trades': total_trades if total_trades is not None else 0,
        'stop_loss_count': stop_loss_count if stop_loss_count is not None else 0
    }
