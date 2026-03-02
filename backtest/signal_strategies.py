"""
多因子信号策略模块
结合动量、波动率、成交量、技术指标等生成高质量交易信号
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple


class SignalStrategy:
    """信号策略基类"""

    def __init__(self, name: str):
        self.name = name

    def generate(self, price_df: pd.DataFrame, volume_df: pd.DataFrame = None) -> pd.DataFrame:
        """生成信号

        Args:
            price_df: 价格数据 (日期 x 股票)
            volume_df: 成交量数据 (可选)

        Returns:
            信号矩阵 (日期 x 股票), 值范围 [-1, 1]
        """
        raise NotImplementedError


class MomentumStrategy(SignalStrategy):
    """简单动量策略（原有）"""

    def __init__(self, period: int = 20):
        super().__init__("20日动量")
        self.period = period

    def generate(self, price_df: pd.DataFrame, volume_df: pd.DataFrame = None) -> pd.DataFrame:
        signal_df = pd.DataFrame(index=price_df.index, columns=price_df.columns)

        for stock in price_df.columns:
            prices = price_df[stock]
            momentum = prices.pct_change(self.period)
            mean = momentum.mean()
            std = momentum.std()

            if std > 0:
                normalized = (momentum - mean) / std
                signal_df[stock] = normalized.clip(-1, 1)
            else:
                signal_df[stock] = 0

        return signal_df.fillna(0)


class MultiFactorStrategy(SignalStrategy):
    """多因子融合策略 ⭐ 改进版"""

    def __init__(
        self,
        momentum_weight: float = 0.25,
        trend_weight: float = 0.45,  # 提高趋势权重
        volatility_weight: float = 0.15,
        volume_weight: float = 0.15
    ):
        super().__init__("多因子融合")
        self.momentum_weight = momentum_weight
        self.trend_weight = trend_weight
        self.volatility_weight = volatility_weight
        self.volume_weight = volume_weight

    def generate(self, price_df: pd.DataFrame, volume_df: pd.DataFrame = None) -> pd.DataFrame:
        """生成多因子信号"""
        signals = {}

        for stock in price_df.columns:
            prices = price_df[stock]
            signal = self._calculate_stock_signal(prices, volume_df)
            signals[stock] = signal

        return pd.DataFrame(signals, index=price_df.index)

    def _calculate_stock_signal(
        self,
        prices: pd.Series,
        volume_df: pd.DataFrame = None
    ) -> pd.Series:
        """计算单只股票的信号"""
        # 1. 动量因子 (短期 + 中期)
        momentum_5 = prices.pct_change(5)
        momentum_20 = prices.pct_change(20)
        momentum_score = (
            self._normalize(momentum_5) * 0.3 +
            self._normalize(momentum_20) * 0.7
        )

        # 2. 趋势因子 (MACD + 均线) - 提高权重
        ema_12 = prices.ewm(span=12).mean()
        ema_26 = prices.ewm(span=26).mean()
        macd = ema_12 - ema_26
        signal_line = macd.ewm(span=9).mean()
        macd_hist = macd - signal_line

        # 均线趋势
        ma_20 = prices.rolling(20).mean()
        ma_60 = prices.rolling(60).mean()
        ma_trend = self._normalize((ma_20 - ma_60) / ma_60)

        # 综合趋势评分
        trend_score = (self._normalize(macd_hist) * 0.5 + ma_trend * 0.5)

        # 3. 波动率因子 (低波动率时信号更可靠)
        returns = prices.pct_change()
        volatility = returns.rolling(20).std()
        vol_score = -self._normalize(volatility)  # 低波动率 = 高分

        # 4. RSI因子 (避免超买超卖)
        rsi = self._calculate_rsi(prices, period=14)
        rsi_score = (rsi - 50) / 50  # 转换到 [-1, 1]

        # 综合评分（调整权重）
        final_score = (
            momentum_score * self.momentum_weight +
            trend_score * self.trend_weight +           # 提高趋势权重
            vol_score * self.volatility_weight +
            rsi_score * 0.15  # RSI权重
        )

        return final_score.clip(-1, 1)

    def _normalize(self, series: pd.Series) -> pd.Series:
        """标准化到 [-1, 1]"""
        mean = series.mean()
        std = series.std()
        if std > 0:
            return (series - mean) / std
        return pd.Series(0, index=series.index)

    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """计算RSI指标"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

        rs = gain / (loss + 1e-8)  # 避免除零
        rsi = 100 - (100 / (1 + rs))
        return rsi


class TrendFollowingStrategy(SignalStrategy):
    """趋势跟踪策略（适合震荡市）"""

    def __init__(self, fast_period: int = 10, slow_period: int = 30,
                 use_dynamic_position: bool = True):
        super().__init__("趋势跟踪")
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.use_dynamic_position = use_dynamic_position

    def generate(self, price_df: pd.DataFrame, volume_df: pd.DataFrame = None) -> pd.DataFrame:
        signal_df = pd.DataFrame(index=price_df.index, columns=price_df.columns)

        for stock in price_df.columns:
            prices = price_df[stock]

            # 快慢均线
            fast_ma = prices.rolling(self.fast_period).mean()
            slow_ma = prices.rolling(self.slow_period).mean()

            # 均线多头排列
            ma_score = self._normalize((fast_ma - slow_ma) / slow_ma)

            # 价格相对均线位置
            price_vs_ma = self._normalize((prices - slow_ma) / slow_ma)

            # 综合信号
            signal = (ma_score * 0.6 + price_vs_ma * 0.4).clip(-1, 1)
            signal_df[stock] = signal

        return signal_df.fillna(0)

    def get_position_size(self, price_df: pd.DataFrame, base_size: float = 20000) -> pd.DataFrame:
        """获取动态仓位大小

        根据趋势强度调整仓位：
        - 强趋势：增加仓位（1.5x）
        - 弱趋势：减少仓位（0.5x）
        - 无趋势：标准仓位（1.0x）

        Returns:
            DataFrame with position size multiplier for each stock and date
        """
        position_multiplier = pd.DataFrame(index=price_df.index, columns=price_df.columns)

        for stock in price_df.columns:
            prices = price_df[stock]

            # 计算趋势强度指标
            fast_ma = prices.rolling(self.fast_period).mean()
            slow_ma = prices.rolling(self.slow_period).mean()

            # 1. 均线斜率（趋势方向）
            ma_slope = slow_ma.diff(5)

            # 2. 价格与均线的距离（趋势强度）
            price_distance = (prices - slow_ma) / slow_ma

            # 3. 动量确认
            momentum = prices.pct_change(20)

            # 综合趋势强度 (0-1)
            trend_strength = (
                self._normalize(ma_slope.abs()) * 0.3 +
                self._normalize(price_distance.abs()) * 0.4 +
                self._normalize(momentum.abs()) * 0.3
            )

            # 转换为仓位倍数 (0.5 ~ 2.0)
            multiplier = 0.5 + trend_strength * 1.5
            multiplier = multiplier.clip(0.5, 2.0)

            position_multiplier[stock] = multiplier

        return position_multiplier.fillna(1.0)

    def _normalize(self, series: pd.Series) -> pd.Series:
        mean = series.mean()
        std = series.std()
        if std > 0:
            return (series - mean) / std
        return pd.Series(0, index=series.index)


class MeanReversionStrategy(SignalStrategy):
    """均值回归策略（适合震荡市）"""

    def __init__(self, period: int = 20, std_threshold: float = 2):
        super().__init__("均值回归")
        self.period = period
        self.std_threshold = std_threshold

    def generate(self, price_df: pd.DataFrame, volume_df: pd.DataFrame = None) -> pd.DataFrame:
        signal_df = pd.DataFrame(index=price_df.index, columns=price_df.columns)

        for stock in price_df.columns:
            prices = price_df[stock]

            # 布林带
            sma = prices.rolling(self.period).mean()
            std = prices.rolling(self.period).std()
            upper_band = sma + self.std_threshold * std
            lower_band = sma - self.std_threshold * std

            # 价格偏离度
            deviation = (prices - sma) / (self.std_threshold * std + 1e-8)

            # 反向信号：价格过高看空，过低看多
            signal = -deviation.clip(-1, 1)
            signal_df[stock] = signal

        return signal_df.fillna(0)


class AdaptiveStrategy(SignalStrategy):
    """自适应策略（根据市场状态切换）"""

    def __init__(self):
        super().__init__("自适应多策略")

    def generate(self, price_df: pd.DataFrame, volume_df: pd.DataFrame = None) -> pd.DataFrame:
        # 计算市场状态（趋势 vs 震荡）
        market_trend = self._detect_market_regime(price_df)

        signal_df = pd.DataFrame(index=price_df.index, columns=price_df.columns)

        for i, date in enumerate(price_df.index):
            regime = market_trend.iloc[i] if i < len(market_trend) else 0.5

            # 趋势市使用趋势策略，震荡市使用均值回归
            if regime > 0.6:  # 强趋势
                strategy = TrendFollowingStrategy()
            elif regime < 0.4:  # 震荡
                strategy = MeanReversionStrategy()
            else:  # 中性
                strategy = MultiFactorStrategy()

            # 使用当前日期之前的数据生成信号
            if i > 30:  # 确保有足够历史数据
                hist_prices = price_df.iloc[:i+1]
                temp_signal = strategy.generate(hist_prices, volume_df)
                signal_df.loc[date] = temp_signal.loc[date]

        return signal_df.fillna(0)

    def _detect_market_regime(self, price_df: pd.DataFrame) -> pd.Series:
        """检测市场状态（趋势 vs 震荡）"""
        # 计算市场平均走势
        market_avg = price_df.mean(axis=1)
        returns = market_avg.pct_change()

        # 趋势强度：用Hurst指数近似
        # > 0.5 趋势，< 0.5 震荡
        trend_strength = returns.rolling(60).apply(lambda x: self._hurst(x))

        return trend_strength.fillna(0.5)

    def _hurst(self, series):
        """简化版Hurst指数"""
        try:
            var = series.var()
            if var == 0:
                return 0.5
            return (series.iloc[-1] - series.iloc[0]) / (len(series) * var ** 0.5) + 0.5
        except:
            return 0.5


class AdaptiveStrategyDelayed(SignalStrategy):
    """自适应策略 - 延迟版（压力测试）"""

    def __init__(self, delay_days: int = 3):
        super().__init__(f"自适应延迟({delay_days}天)")
        self.delay_days = delay_days

    def generate(self, price_df: pd.DataFrame, volume_df: pd.DataFrame = None) -> pd.DataFrame:
        # 计算市场状态（带延迟）
        market_trend = self._detect_market_regime(price_df)

        signal_df = pd.DataFrame(index=price_df.index, columns=price_df.columns)

        for i, date in enumerate(price_df.index):
            regime = market_trend.iloc[i] if i < len(market_trend) else 0.5

            # 趋势市使用趋势策略，震荡市使用均值回归
            if regime > 0.6:  # 强趋势
                strategy = TrendFollowingStrategy()
            elif regime < 0.4:  # 震荡
                strategy = MeanReversionStrategy()
            else:  # 中性
                strategy = MultiFactorStrategy()

            # 使用当前日期之前的数据生成信号
            if i > 30:  # 确保有足够历史数据
                hist_prices = price_df.iloc[:i+1]
                temp_signal = strategy.generate(hist_prices, volume_df)
                signal_df.loc[date] = temp_signal.loc[date]

        return signal_df.fillna(0)

    def _detect_market_regime(self, price_df: pd.DataFrame) -> pd.Series:
        """检测市场状态（带延迟的Hurst指数）"""
        market_avg = price_df.mean(axis=1)
        returns = market_avg.pct_change()

        # 计算趋势强度并强制延迟
        trend_strength = returns.rolling(60).apply(lambda x: self._hurst(x))

        # 【核心修改】强制延迟 N 天
        # 第i天使用第i-N天的状态（模拟信号滞后）
        delayed = trend_strength.shift(self.delay_days)

        # 前N天没有历史数据，使用即时计算的值作为初始状态
        # 从第N+1天开始，使用N天前的状态（真正产生延迟效果）
        for i in range(min(self.delay_days, len(delayed))):
            if pd.isna(delayed.iloc[i]):
                delayed.iloc[i] = trend_strength.iloc[i]

        return delayed

    def _hurst(self, series):
        """简化版Hurst指数"""
        try:
            var = series.var()
            if var == 0:
                return 0.5
            return (series.iloc[-1] - series.iloc[0]) / (len(series) * var ** 0.5) + 0.5
        except:
            return 0.5


class AdaptiveStrategyMacro(SignalStrategy):
    """自适应策略 - 宏观指标版（压力测试）"""

    def __init__(self, use_volume: bool = True, volume_threshold: float = 1.2):
        super().__init__(f"自适应宏观指标(量比>{volume_threshold})")
        self.use_volume = use_volume
        self.volume_threshold = volume_threshold

    def generate(self, price_df: pd.DataFrame, volume_df: pd.DataFrame = None) -> pd.DataFrame:
        # 计算市场状态（基于成交量）
        market_trend = self._detect_market_regime(price_df, volume_df)

        signal_df = pd.DataFrame(index=price_df.index, columns=price_df.columns)

        for i, date in enumerate(price_df.index):
            regime = market_trend.iloc[i] if i < len(market_trend) else 0.5

            # 趋势市使用趋势策略，震荡市使用均值回归
            if regime > 0.6:  # 强趋势（放量）
                strategy = TrendFollowingStrategy()
            elif regime < 0.4:  # 震荡（缩量）
                strategy = MeanReversionStrategy()
            else:  # 中性
                strategy = MultiFactorStrategy()

            # 使用当前日期之前的数据生成信号
            if i > 30:  # 确保有足够历史数据
                hist_prices = price_df.iloc[:i+1]
                hist_volume = volume_df.iloc[:i+1] if volume_df is not None else None
                temp_signal = strategy.generate(hist_prices, hist_volume)
                signal_df.loc[date] = temp_signal.loc[date]

        return signal_df.fillna(0)

    def _detect_market_regime(self, price_df: pd.DataFrame, volume_df: pd.DataFrame = None) -> pd.Series:
        """检测市场状态（基于成交量的宏观指标）"""
        if volume_df is None:
            # 如果没有成交量数据，回退到Hurst指数
            market_avg = price_df.mean(axis=1)
            returns = market_avg.pct_change()
            return returns.rolling(60).apply(lambda x: self._hurst(x)).fillna(0.5)

        # 【核心修改】用成交量替换Hurst指数
        # 计算市场总成交额
        market_volume = volume_df.sum(axis=1)

        # 计算20日平均成交量
        volume_ma20 = market_volume.rolling(20).mean()

        # 计算量比（当日成交量 / 20日均量）
        volume_ratio = market_volume / volume_ma20

        # 【判断逻辑】
        # 量比 > 1.2 → 放量 → 趋势市 → regime = 0.8
        # 量比 < 0.8 → 缩量 → 震荡市 → regime = 0.2
        # 量比 0.8-1.2 → 中性 → regime = 0.5

        regime = pd.Series(0.5, index=price_df.index)
        regime[volume_ratio > self.volume_threshold] = 0.8  # 放量 = 趋势
        regime[volume_ratio < (1 / self.volume_threshold)] = 0.2  # 缩量 = 震荡

        return regime

    def _hurst(self, series):
        """简化版Hurst指数（备用）"""
        try:
            var = series.var()
            if var == 0:
                return 0.5
            return (series.iloc[-1] - series.iloc[0]) / (len(series) * var ** 0.5) + 0.5
        except:
            return 0.5


class AdaptiveStrategyADX(SignalStrategy):
    """自适应策略 - 基于ADX指标（快速版）"""

    def __init__(self, adx_period: int = 14, trend_threshold: float = 25,
                 weak_threshold: float = 20, rest_when_weak: bool = False):
        super().__init__(f"自适应ADX(>{trend_threshold}趋势)")
        self.adx_period = adx_period
        self.trend_threshold = trend_threshold
        self.weak_threshold = weak_threshold
        self.rest_when_weak = rest_when_weak  # 震荡时是否空仓

    def generate(self, price_df: pd.DataFrame, volume_df: pd.DataFrame = None) -> pd.DataFrame:
        # 计算市场ADX
        market_adx = self._calculate_market_adx(price_df)

        signal_df = pd.DataFrame(index=price_df.index, columns=price_df.columns)

        for i, date in enumerate(price_df.index):
            adx = market_adx.iloc[i] if i < len(market_adx) else 20

            # 基于ADX选择策略
            if adx >= self.trend_threshold:
                # 强趋势市 → 趋势跟踪
                strategy = TrendFollowingStrategy()
            elif adx <= self.weak_threshold:
                # 震荡市
                if self.rest_when_weak:
                    # 空仓休息
                    signal_df.loc[date] = 0
                    continue
                else:
                    # 均值回归
                    strategy = MeanReversionStrategy()
            else:
                # 中性区间 → 多因子
                strategy = MultiFactorStrategy()

            # 使用当前日期之前的数据生成信号
            if i > 30:
                hist_prices = price_df.iloc[:i+1]
                temp_signal = strategy.generate(hist_prices, volume_df)
                signal_df.loc[date] = temp_signal.loc[date]

        return signal_df.fillna(0)

    def _calculate_market_adx(self, price_df: pd.DataFrame) -> pd.Series:
        """计算市场平均ADX（使用个股AD X的平均值）"""
        # 计算每只股票的ADX，然后取平均
        adx_values = pd.Series(index=price_df.index, dtype=float)

        for stock in price_df.columns:
            stock_adx = self._calculate_adx(price_df[stock], self.adx_period)
            adx_values = adx_values.add(stock_adx, fill_value=0)

        # 取平均值
        market_adx = adx_values / len(price_df.columns)
        return market_adx.fillna(20)

    def _calculate_adx(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """计算ADX指标（标准版）"""
        # 对于单列价格数据，用价格变化代替高低价
        # 计算涨跌幅
        change = prices.diff()

        # +DM 和 -DM
        pos_dm = change.where(change > 0, 0)
        neg_dm = (-change).where(change < 0, 0)

        # TR (简化版：用绝对变化代替)
        tr = change.abs()

        # 平滑处理 (使用Wilder平滑)
        def wilder_smooth(series, period):
            alpha = 1.0 / period
            smoothed = series.copy()
            for i in range(period, len(series)):
                smoothed.iloc[i] = alpha * series.iloc[i] + (1 - alpha) * smoothed.iloc[i-1]
            return smoothed

        # 计算平滑后的TR, +DM, -DM
        atr = wilder_smooth(tr, period)
        pos_di_smooth = wilder_smooth(pos_dm, period)
        neg_di_smooth = wilder_smooth(neg_dm, period)

        # +DI 和 -DI
        pos_di = 100 * pos_di_smooth / atr
        neg_di = 100 * neg_di_smooth / atr

        # DX 和 ADX
        dx = 100 * abs(pos_di - neg_di) / (pos_di + neg_di)
        adx = wilder_smooth(dx, period)

        return adx.fillna(20)

    def _hurst(self, series):
        """不需要Hurst，保留兼容性"""
        return 0.5


class AdaptiveStrategyInverted(SignalStrategy):
    """自适应策略 - 反相延迟版（极端压力测试）"""

    def __init__(self):
        super().__init__("自适应反相延迟 (极端测试)")

    def generate(self, price_df: pd.DataFrame, volume_df: pd.DataFrame = None) -> pd.DataFrame:
        # 计算市场状态
        market_trend = self._detect_market_regime(price_df)

        signal_df = pd.DataFrame(index=price_df.index, columns=price_df.columns)

        for i, date in enumerate(price_df.index):
            regime = market_trend.iloc[i] if i < len(market_trend) else 0.5

            # 【核心修改】反相策略选择
            if regime > 0.6:  # 原版趋势 → 反相使用均值回归
                strategy = MeanReversionStrategy()
            elif regime < 0.4:  # 原版震荡 → 反相使用趋势
                strategy = TrendFollowingStrategy()
            else:  # 中性保持不变
                strategy = MultiFactorStrategy()

            # 使用当前日期之前的数据生成信号
            if i > 30:
                hist_prices = price_df.iloc[:i+1]
                temp_signal = strategy.generate(hist_prices, volume_df)
                signal_df.loc[date] = temp_signal.loc[date]

        return signal_df.fillna(0)

    def _detect_market_regime(self, price_df: pd.DataFrame) -> pd.Series:
        """检测市场状态（与原版相同）"""
        market_avg = price_df.mean(axis=1)
        returns = market_avg.pct_change()
        trend_strength = returns.rolling(60).apply(lambda x: self._hurst(x))
        return trend_strength.fillna(0.5)

    def _hurst(self, series):
        """简化版Hurst指数"""
        try:
            var = series.var()
            if var == 0:
                return 0.5
            return (series.iloc[-1] - series.iloc[0]) / (len(series) * var ** 0.5) + 0.5
        except:
            return 0.5


def get_strategy(strategy_name: str = "multifactor") -> SignalStrategy:
    """获取信号策略实例"""
    strategies = {
        "momentum": MomentumStrategy(),
        "multifactor": MultiFactorStrategy(),
        "trend": TrendFollowingStrategy(),
        "trend_plus": TrendPlusStrategy(),
        "meanreversion": MeanReversionStrategy(),
        "adaptive": AdaptiveStrategy(),
        "adaptive_adx": AdaptiveStrategyADX(adx_period=14, trend_threshold=25),
        "adaptive_adx_rest": AdaptiveStrategyADX(adx_period=14, trend_threshold=25, rest_when_weak=True),
    }
    return strategies.get(strategy_name, MultiFactorStrategy())


def list_strategies() -> Dict[str, str]:
    """列出可用策略"""
    return {
        "momentum": "20日动量 (原策略)",
        "multifactor": "多因子融合 (推荐)",
        "trend": "趋势跟踪 (+14%)",
        "trend_plus": "趋势增强",
        "meanreversion": "均值回归 (高胜率69%)",
        "adaptive": "自适应Hurst (+31%)",
        "adaptive_adx": "自适应ADX ⚡ (+62%)",
        "adaptive_adx_rest": "自适应ADX+空仓 💎 (59%, 1.73盈亏比)"
    }


class TrendPlusStrategy(SignalStrategy):
    """趋势增强策略（基于回测优化）"""

    def __init__(self, fast_period: int = 10, slow_period: int = 30):
        super().__init__("趋势增强")
        self.fast_period = fast_period
        self.slow_period = slow_period

    def generate(self, price_df: pd.DataFrame, volume_df: pd.DataFrame = None) -> pd.DataFrame:
        signal_df = pd.DataFrame(index=price_df.index, columns=price_df.columns)

        for stock in price_df.columns:
            prices = price_df[stock]

            # 多重趋势确认
            # 1. 均线趋势
            fast_ma = prices.rolling(self.fast_period).mean()
            slow_ma = prices.rolling(self.slow_period).mean()
            ma_trend = (fast_ma - slow_ma) / slow_ma

            # 2. 价格位置
            price_vs_ma = (prices - slow_ma) / slow_ma

            # 3. 动量确认
            momentum_20 = prices.pct_change(20)
            momentum_signal = self._normalize(momentum_20)

            # 4. 趋势强度（ADX简化版）
            price_diff = prices.diff().abs()
            trend_strength = self._normalize(price_diff.rolling(14).mean())

            # 综合信号（趋势为主）
            signal = (
                self._normalize(ma_trend) * 0.35 +
                self._normalize(price_vs_ma) * 0.25 +
                momentum_signal * 0.25 +
                trend_strength * 0.15
            ).clip(-1, 1)

            signal_df[stock] = signal

        return signal_df.fillna(0)

    def _normalize(self, series: pd.Series) -> pd.Series:
        mean = series.mean()
        std = series.std()
        if std > 0:
            return (series - mean) / std
        return pd.Series(0, index=series.index)
