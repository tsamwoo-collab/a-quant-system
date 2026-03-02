"""
真实数据回测模块
使用已下载的真实市场数据进行回测
"""
import pandas as pd
import numpy as np
import duckdb
from datetime import datetime, timedelta
from typing import Dict, List


class RealDataBacktest:
    """真实数据回测器"""

    def __init__(self, db_path: str = "data/real_market.duckdb",
                 initial_cash: float = 100000,
                 max_positions: int = 5,
                 position_size: float = 20000,
                 buy_threshold: float = 0.3,
                 sell_threshold: float = -0.3):
        self.db_path = db_path
        self.initial_cash = initial_cash
        self.max_positions = max_positions
        self.position_size = position_size
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold

    def load_data(self) -> tuple:
        """从数据库加载真实数据

        Returns:
            (price_df, stock_list)
        """
        conn = duckdb.connect(self.db_path)

        # 获取股票列表
        stock_list = conn.execute("SELECT symbol, name FROM stock_list").fetchdf()

        # 获取日线数据
        quotes = conn.execute("""
            SELECT date, symbol, close, volume
            FROM daily_quotes
            ORDER BY date, symbol
        """).fetchdf()

        conn.close()

        if quotes.empty:
            raise ValueError("数据库中没有日线数据，请先运行批量下载")

        # 转换为价格矩阵
        price_df = quotes.pivot(index='date', columns='symbol', values='close')

        # 填充缺失值
        price_df = price_df.fillna(method='ffill').fillna(method='bfill')

        print(f"✅ 加载数据成功:")
        print(f"   - 日期范围: {price_df.index.min()} 至 {price_df.index.max()}")
        print(f"   - 股票数量: {len(price_df.columns)}")
        print(f"   - 数据点数: {price_df.count().sum():.0f}")

        return price_df, stock_list

    def generate_signals(self, price_df: pd.DataFrame) -> pd.DataFrame:
        """基于价格数据生成模拟信号

        使用简单的动量策略生成信号
        """
        signal_df = pd.DataFrame(index=price_df.index, columns=price_df.columns)

        for stock in price_df.columns:
            prices = price_df[stock].dropna()

            # 20日动量
            momentum_20 = prices.pct_change(20)

            # 标准化到 [-1, 1]
            mean = momentum_20.mean()
            std = momentum_20.std()

            if std > 0:
                normalized = (momentum_20 - mean) / std
                # 限制范围
                normalized = normalized.clip(-1, 1)
            else:
                normalized = pd.Series(0, index=prices.index)

            signal_df[stock] = normalized

        # 填充缺失值
        signal_df = signal_df.fillna(0)

        print(f"✅ 信号生成完成")
        print(f"   - 信号范围: [{signal_df.min().min():.2f}, {signal_df.max().max():.2f}]")

        return signal_df

    def run(self, use_real_data: bool = True) -> Dict:
        """运行回测

        Args:
            use_real_data: 是否使用真实数据

        Returns:
            回测结果字典
        """
        if use_real_data:
            print("=" * 60)
            print("🎯 真实数据回测")
            print("=" * 60)

            # 加载真实数据
            price_df, stock_list = self.load_data()

            # 生成信号
            signal_df = self.generate_signals(price_df)
        else:
            # 使用模拟数据（原有逻辑）
            from backtest.layer1_filter import VectorizedFilter
            layer1 = VectorizedFilter()
            signal_df, price_df = layer1.generate_synthetic_data(n_stocks=100, n_days=200)
            stock_list = pd.DataFrame({
                'symbol': signal_df.columns,
                'name': [f'股票{s}' for s in signal_df.columns]
            })

        # 执行回测
        return self._execute_backtest(price_df, signal_df, stock_list)

    def _execute_backtest(self, price_df: pd.DataFrame, signal_df: pd.DataFrame,
                          stock_list: pd.DataFrame) -> Dict:
        """执行回测逻辑"""
        dates = price_df.index
        symbols = price_df.columns.tolist()

        cash = self.initial_cash
        positions = {}
        equity_curve = []
        trades = []

        print(f"\n开始回测...")
        print(f"  日期范围: {dates[0]} 至 {dates[-1]}")
        print(f"  股票数量: {len(symbols)}")

        for i, date in enumerate(dates):
            if i % 50 == 0:
                print(f"  进度: {i+1}/{len(dates)} ({(i+1)/len(dates)*100:.0f}%)")

            daily_prices = price_df.loc[date]
            daily_signals = signal_df.loc[date]

            # 获取昨日信号
            if i == 0:
                prev_signals = pd.Series(0, index=symbols)
            else:
                prev_signals = signal_df.iloc[i - 1]

            # 检测跨越
            buy_list, sell_list = [], []
            for stock in symbols:
                if pd.isna(daily_prices[stock]):
                    continue

                prev_sig = prev_signals.get(stock, 0)
                curr_sig = daily_signals.get(stock, 0)

                # 买入跨越
                if prev_sig < self.buy_threshold and curr_sig >= self.buy_threshold:
                    buy_list.append((stock, curr_sig))
                # 卖出跨越
                elif prev_sig > self.sell_threshold and curr_sig <= self.sell_threshold:
                    sell_list.append(stock)

            # 执行卖出
            for stock in sell_list:
                if stock in positions and stock in daily_signals.index:
                    price = daily_prices[stock]
                    pos = positions[stock]
                    pnl = (price - pos['entry_price']) * pos['shares']
                    trades.append({
                        'date': date,
                        'stock': stock,
                        'action': '卖出',
                        'price': price,
                        'entry_price': pos['entry_price'],
                        'shares': pos['shares'],
                        'pnl': pnl
                    })
                    cash += pos['shares'] * price
                    del positions[stock]

            # 执行买入（按信号强度排序）
            buy_list.sort(key=lambda x: x[1], reverse=True)
            for stock, _ in buy_list:
                if len(positions) < self.max_positions and cash >= self.position_size:
                    if stock in daily_signals.index:
                        price = daily_prices[stock]
                        shares = int((self.position_size / price) / 100) * 100
                        if shares >= 100:
                            positions[stock] = {'shares': shares, 'entry_price': price}
                            cash -= shares * price
                            trades.append({
                                'date': date,
                                'stock': stock,
                                'action': '买入',
                                'price': price,
                                'shares': shares
                            })

            # 计算净值
            position_value = 0
            for stock, pos in positions.items():
                if stock in daily_signals.index:
                    position_value += pos['shares'] * daily_prices[stock]
                else:
                    position_value += pos['shares'] * pos['entry_price']

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

        # 计算指标
        metrics = self._calculate_metrics(equity_df, trades_df)

        return {
            'equity_df': equity_df,
            'trades': trades_df,
            'metrics': metrics
        }

    def _calculate_metrics(self, equity_df: pd.DataFrame, trades_df: pd.DataFrame) -> Dict:
        """计算性能指标"""
        final_equity = equity_df['total_equity'].iloc[-1]
        total_return = (final_equity - self.initial_cash) / self.initial_cash * 100

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

        # 胜率和盈亏比
        if not trades_df.empty and 'action' in trades_df.columns:
            sell_trades = trades_df[trades_df['action'] == '卖出']
            if 'pnl' in sell_trades.columns:
                win_rate = (sell_trades['pnl'] > 0).sum() / len(sell_trades) * 100 if len(sell_trades) > 0 else 0

                win_trades = sell_trades[sell_trades['pnl'] > 0]['pnl']
                loss_trades = sell_trades[sell_trades['pnl'] < 0]['pnl']

                avg_win = win_trades.mean() if len(win_trades) > 0 else 0
                avg_loss = loss_trades.mean() if len(loss_trades) > 0 else 0

                profit_loss_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0
            else:
                win_rate = 0
                profit_loss_ratio = 0

            total_trades = len(sell_trades)
        else:
            win_rate = 0
            profit_loss_ratio = 0
            total_trades = 0

        return {
            'initial_cash': self.initial_cash,
            'final_equity': round(final_equity, 2),
            'total_return': round(total_return, 2),
            'max_drawdown': round(max_drawdown, 2),
            'sharpe_ratio': round(sharpe_ratio, 2),
            'win_rate': round(win_rate, 1),
            'profit_loss_ratio': round(profit_loss_ratio, 2),
            'total_trades': total_trades
        }


def run_real_backtest() -> Dict:
    """便捷函数：运行真实数据回测"""
    backtester = RealDataBacktest()
    return backtester.run(use_real_data=True)
