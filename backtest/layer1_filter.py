"""
第一层：向量化筛选层
快速验证信号有效性，筛选优质股票池
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple


class VectorizedFilter:
    """向量化筛选器"""

    def __init__(self, buy_threshold: float = 0.3, sell_threshold: float = -0.3):
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold

    def filter(self, signal_df: pd.DataFrame, price_df: pd.DataFrame,
               min_signals: int = 10, top_n: int = 200) -> Tuple[List[str], pd.DataFrame]:
        """
        筛选有效股票

        Args:
            signal_df: 信号矩阵 [日期 x 股票]
            price_df: 价格矩阵 [日期 x 股票]
            min_signals: 最小信号触发次数
            top_n: 返回前N只股票

        Returns:
            (valid_stocks, effectiveness_df)
        """
        # 确保数据对齐
        common_index = signal_df.index.intersection(price_df.index)
        common_cols = signal_df.columns.intersection(price_df.columns)
        signal_aligned = signal_df.loc[common_index, common_cols]
        price_aligned = price_df.loc[common_index, common_cols]

        # 1. 计算次日收益率（向量化）
        future_returns = price_aligned.pct_change().shift(-1)

        # 2. 统计每只股票的信号表现
        results = []

        for stock in common_cols:
            stock_signals = signal_aligned[stock]
            stock_returns = future_returns[stock]

            # 买入信号统计
            buy_signal_mask = stock_signals >= self.buy_threshold
            buy_count = buy_signal_mask.sum()
            buy_avg_return = stock_returns[buy_signal_mask].mean()

            # 卖出信号统计
            sell_signal_mask = stock_signals <= self.sell_threshold
            sell_count = sell_signal_mask.sum()
            sell_avg_return = stock_returns[sell_signal_mask].mean()

            # 持有期间统计
            hold_mask = (stock_signals > self.sell_threshold) & (stock_signals < self.buy_threshold)
            hold_avg_return = stock_returns[hold_mask].mean()

            # 信号质量评分
            # 买入信号盈利 - 卖出信号亏损（理想情况）
            signal_quality = (buy_avg_return - sell_avg_return) if buy_count > 0 and sell_count > 0 else 0

            results.append({
                '股票代码': stock,
                '买入信号次数': int(buy_count),
                '买入平均收益': round(buy_avg_return * 100, 3) if buy_count > 0 else 0,
                '卖出信号次数': int(sell_count),
                '卖出平均收益': round(sell_avg_return * 100, 3) if sell_count > 0 else 0,
                '持有平均收益': round(hold_avg_return * 100, 3) if not np.isnan(hold_avg_return) else 0,
                '信号质量': round(signal_quality * 100, 3),
                '总信号数': int(buy_count + sell_count)
            })

        effectiveness_df = pd.DataFrame(results)

        # 3. 筛选有效股票
        # 条件：总信号次数 >= min_signals
        valid_mask = effectiveness_df['总信号数'] >= min_signals

        # 按信号质量排序
        effectiveness_df = effectiveness_df[valid_mask].sort_values('信号质量', ascending=False)

        # 取前N只
        top_stocks_df = effectiveness_df.head(top_n).copy()

        # 添加评级
        top_stocks_df['评级'] = top_stocks_df['信号质量'].apply(self._get_rating)

        return top_stocks_df['股票代码'].tolist(), top_stocks_df

    def _get_rating(self, quality: float) -> str:
        """根据信号质量返回评级"""
        if quality >= 0.5:
            return '★★★★★'
        elif quality >= 0.3:
            return '★★★★☆'
        elif quality >= 0.1:
            return '★★★☆☆'
        elif quality >= 0:
            return '★★☆☆☆'
        else:
            return '★☆☆☆☆'

    def get_crossing_signals(self, signal_df: pd.DataFrame) -> pd.DataFrame:
        """
        检测跨越信号（向量化）

        Returns:
            交易信号矩阵 [+1=买入, -1=卖出, 0=无操作]
        """
        signal_shifted = signal_df.shift(1)

        # 买入跨越：昨天<阈值 → 今天≥阈值
        buy_crossing = (signal_shifted < self.buy_threshold) & (signal_df >= self.buy_threshold)

        # 卖出跨越：昨天>阈值 → 今天≤阈值
        sell_crossing = (signal_shifted > self.sell_threshold) & (signal_df <= self.sell_threshold)

        # 交易信号矩阵
        trade_signals = pd.DataFrame(0, index=signal_df.index, columns=signal_df.columns, dtype=int)
        trade_signals[buy_crossing] = 1
        trade_signals[sell_crossing] = -1

        return trade_signals

    def generate_synthetic_data(self, n_stocks: int = 500, n_days: int = 500,
                                start_date: str = '2023-01-01') -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        生成模拟数据用于测试

        Returns:
            (signal_df, price_df)
        """
        np.random.seed(42)

        dates = pd.date_range(start=start_date, periods=n_days, freq='D')
        symbols = [f'{i:06d}' for i in range(600000, 600000 + n_stocks)]

        # 生成模拟价格（随机游走）
        base_prices = np.random.uniform(10, 100, n_stocks)
        price_changes = np.random.normal(0, 0.02, (n_days, n_stocks))
        price_changes[0, :] = 0  # 第一天无变化

        prices = base_prices * (1 + price_changes).cumprod(axis=0)
        price_df = pd.DataFrame(prices, index=dates, columns=symbols)

        # 生成模拟信号（与价格有一定相关性）
        # 好股票：信号领先价格上涨
        # 差股票：信号不领先
        signal_quality = np.random.uniform(-0.1, 0.3, n_stocks)  # 每只股票的信号质量

        signals = np.random.randn(n_days, n_stocks) * 0.3

        # 让信号与未来收益相关
        future_returns = price_changes.copy()
        future_returns = np.nan_to_num(future_returns, nan=0)

        # 调整信号，使高质量股票的信号更有预测性
        for i in range(n_stocks):
            signals[:, i] += future_returns[:, i] * signal_quality[i]

        # 限制信号范围
        signals = np.clip(signals, -1, 1)
        signal_df = pd.DataFrame(signals, index=dates, columns=symbols)

        return signal_df, price_df
