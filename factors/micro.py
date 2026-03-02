"""
微观因子计算 - 个股层面
"""
import pandas as pd
import numpy as np
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class MicroFactors:
    """微观（个股）因子计算器"""

    def __init__(self, data_fetcher, data_storage):
        self.fetcher = data_fetcher
        self.storage = data_storage

    def calculate_for_stock(self, symbol: str) -> pd.Series:
        """
        计算单个股票的所有微观因子

        Args:
            symbol: 股票代码

        Returns:
            因子值序列
        """
        factors = {}

        # 获取日线数据
        daily_data = self.fetcher.get_stock_daily(symbol, days=60)
        if daily_data is None or daily_data.empty:
            logger.warning(f"股票 {symbol} 无日线数据")
            return pd.Series()

        # 换手率极值
        turnover = self.calc_turnover_extreme(daily_data)
        if turnover is not None:
            factors['turnover_extreme'] = turnover

        # 20日动量
        momentum = self.calc_momentum_20d(daily_data)
        if momentum is not None:
            factors['momentum_20d'] = momentum

        # 均线偏离度
        bias = self.calc_bias(daily_data)
        if bias is not None:
            factors['bias'] = bias

        # 龙虎榜净买入（需要额外数据）
        lhb = self.calc_longhubang_net_buy(symbol)
        if lhb is not None:
            factors['longhubang_net_buy'] = lhb

        # 两融净买入（需要额外数据）
        margin = self.calc_margin_net_buy(symbol)
        if margin is not None:
            factors['margin_net_buy'] = margin

        return pd.Series(factors)

    def calc_turnover_extreme(self, data: pd.DataFrame, window: int = 20) -> Optional[float]:
        """
        计算换手率极值因子

        逻辑: 换手率是否处于极端高位（可能是反转信号）
        """
        try:
            if '换手率' not in data.columns:
                return None

            turnover = data['换手率'].dropna()
            if len(turnover) < window:
                return None

            recent = turnover.iloc[-1]
            mean = turnover.iloc[-window:].mean()
            std = turnover.iloc[-window:].std()

            if std == 0:
                return 0

            # 换手率过高可能是反转信号（负因子）
            zscore = (recent - mean) / std

            # 换手率极高时给予负信号（可能见顶）
            if zscore > 2:
                factor = -1
            elif zscore < -2:
                factor = 1  # 换手率极低可能表示底部
            else:
                factor = -zscore / 2  # 正常范围内，换手率越高越积极

            return np.clip(factor, -1, 1)

        except Exception as e:
            logger.error(f"计算换手率因子失败: {e}")
            return None

    def calc_momentum_20d(self, data: pd.DataFrame, period: int = 20) -> Optional[float]:
        """
        计算20日动量因子

        逻辑: 过去20日涨跌幅
        """
        try:
            if len(data) < period + 1:
                return None

            momentum = (data['close'].iloc[-1] / data['close'].iloc[-period-1] - 1) * 100

            # 标准化到 -1 到 1
            # 假设 ±20% 是极端情况
            factor = np.clip(momentum / 20, -1, 1)
            return factor

        except Exception as e:
            logger.error(f"计算动量因子失败: {e}")
            return None

    def calc_bias(self, data: pd.DataFrame, period: int = 20) -> Optional[float]:
        """
        计算均线偏离度(BIAS)因子

        逻辑: 价格偏离均线的程度
        正偏离过大可能超买，负偏离过大可能超卖
        """
        try:
            if len(data) < period:
                return None

            ma = data['close'].rolling(period).mean()
            current_price = data['close'].iloc[-1]
            current_ma = ma.iloc[-1]

            if current_ma == 0 or pd.isna(current_ma):
                return None

            # BIAS = (价格 - 均线) / 均线
            bias = (current_price - current_ma) / current_ma

            # 计算历史BIAS的统计特征
            bias_series = ((data['close'] - ma) / ma).dropna()
            mean_bias = bias_series.mean()
            std_bias = bias_series.std()

            if std_bias == 0:
                return 0

            # 标准化
            zscore = (bias - mean_bias) / std_bias

            # 过度正偏离可能是负信号（反转），过度负偏离可能是正信号（反弹）
            factor = -np.clip(zscore / 2, -1, 1)
            return factor

        except Exception as e:
            logger.error(f"计算BIAS因子失败: {e}")
            return None

    def calc_longhubang_net_buy(self, symbol: str, days: int = 5) -> Optional[float]:
        """
        计算龙虎榜净买入因子

        逻辑: 近期龙虎榜净买入金额
        """
        try:
            # 获取龙虎榜数据
            lhb_data = self.fetcher.get_longhubang()
            if lhb_data is None or lhb_data.empty:
                return None

            # 筛选该股票的数据
            if '代码' in lhb_data.columns:
                stock_lhb = lhb_data[lhb_data['代码'] == symbol]
            else:
                # 列名可能不同，尝试其他方式
                return None

            if stock_lhb.empty:
                return None

            # 计算净买入
            if 'net_buy' in stock_lhb.columns:
                net_buy = stock_lhb['net_buy'].sum()
                # 标准化：假设 ±1亿 是极端情况
                factor = np.clip(net_buy / 100000000, -1, 1)
                return factor

            return None

        except Exception as e:
            logger.error(f"计算龙虎榜因子失败: {e}")
            return None

    def calc_margin_net_buy(self, symbol: str) -> Optional[float]:
        """
        计算两融净买入因子

        逻辑: 融资融券净变化
        融资增加表示看多，融券增加表示看空
        """
        try:
            # AkShare的两融数据是市场层面的，个股数据可能需要其他接口
            # 这里使用一个简化的实现

            margin_data = self.fetcher.get_margin_trading(symbol)
            if margin_data is None or margin_data.empty:
                return None

            # 计算融资余额变化
            if '融资余额' in margin_data.columns:
                recent = margin_data['融资余额'].iloc[-1]
                prev = margin_data['融资余额'].iloc[0]

                if prev == 0:
                    return 0

                change = (recent - prev) / prev

                # 标准化
                factor = np.clip(change * 10, -1, 1)
                return factor

            return None

        except Exception as e:
            logger.error(f"计算两融因子失败: {e}")
            return None

    def calc_pe_pb_percentile(self, symbol: str) -> pd.Series:
        """
        计算PE/PB分位数因子

        逻辑: 当前估值在历史中的位置
        低估值给予正因子
        """
        try:
            financial = self.fetcher.get_stock_financial(symbol)
            if financial is None or financial.empty:
                return pd.Series()

            factors = {}

            # PE 分位数
            if 'pe' in financial.columns:
                pe_series = financial['pe'].dropna()
                if len(pe_series) > 0:
                    current_pe = pe_series.iloc[-1]
                    pe_percentile = (pe_series <= current_pe).sum() / len(pe_series)
                    # 低估值是正因子
                    factors['pe_percentile'] = 0.5 - pe_percentile

            # PB 分位数
            if 'pb' in financial.columns:
                pb_series = financial['pb'].dropna()
                if len(pb_series) > 0:
                    current_pb = pb_series.iloc[-1]
                    pb_percentile = (pb_series <= current_pb).sum() / len(pb_series)
                    factors['pb_percentile'] = 0.5 - pb_percentile

            return pd.Series(factors)

        except Exception as e:
            logger.error(f"计算PE/PB因子失败: {e}")
            return pd.Series()

    def calc_roe_trend(self, symbol: str) -> Optional[float]:
        """
        计算ROE趋势因子

        逻辑: ROE的上升趋势表示盈利能力改善
        """
        try:
            financial = self.fetcher.get_stock_financial(symbol)
            if financial is None or financial.empty or 'roe' not in financial.columns:
                return None

            roe_series = financial['roe'].dropna()
            if len(roe_series) < 4:
                return None

            # 计算趋势
            recent_roe = roe_series.iloc[-1]
            avg_roe = roe_series.mean()

            if avg_roe == 0:
                return 0

            # ROE高于平均且稳定为正因子
            factor = np.clip((recent_roe - avg_roe) / abs(avg_roe), -1, 1)
            return factor

        except Exception as e:
            logger.error(f"计算ROE趋势因子失败: {e}")
            return None

    def get_factor_interpretation(self, factor_name: str, value: float) -> str:
        """获取因子解释"""
        interpretations = {
            'turnover_extreme': {
                'positive': '换手率适中，筹码稳定',
                'negative': '换手率过高，注意风险',
                'neutral': '换手率正常'
            },
            'momentum_20d': {
                'positive': '近期表现强势，动能充足',
                'negative': '近期表现疲弱，动能不足',
                'neutral': '近期表现平稳'
            },
            'bias': {
                'positive': '价格低于均线，可能超卖',
                'negative': '价格高于均线，可能超买',
                'neutral': '价格围绕均线波动'
            },
            'longhubang_net_buy': {
                'positive': '龙虎榜资金净流入',
                'negative': '龙虎榜资金净流出',
                'neutral': '龙虎榜无进出'
            },
            'margin_net_buy': {
                'positive': '融资余额增加',
                'negative': '融资余额减少',
                'neutral': '两融余额平稳'
            }
        }

        interp = interpretations.get(factor_name, {})
        if value > 0.3:
            return interp.get('positive', '')
        elif value < -0.3:
            return interp.get('negative', '')
        else:
            return interp.get('neutral', '')
