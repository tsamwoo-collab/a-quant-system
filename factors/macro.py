"""
宏观因子计算
"""
import pandas as pd
import numpy as np
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


class MacroFactors:
    """宏观因子计算器"""

    def __init__(self, data_fetcher, data_storage):
        self.fetcher = data_fetcher
        self.storage = data_storage

    def calculate_all(self, date: str = None) -> pd.Series:
        """
        计算所有宏观因子

        Args:
            date: 计算日期

        Returns:
            因子值序列
        """
        factors = {}

        # SHIBOR 隔夜利率
        shibor = self.calc_shibor_factor()
        if shibor is not None:
            factors['shibor_on'] = shibor

        # 北向资金净流入
        north = self.calc_north_flow_factor()
        if north is not None:
            factors['north_flow_net'] = north

        # 市场成交量变化率
        volume = self.calc_market_volume_factor()
        if volume is not None:
            factors['market_volume_ratio'] = volume

        if not factors:
            logger.warning("未能计算任何宏观因子")
            return pd.Series()

        return pd.Series(factors)

    def calc_shibor_factor(self) -> Optional[float]:
        """
        计算SHIBOR因子

        逻辑: SHIBOR下降 → 流动性宽松 → 利好市场
        因子值: (当前值 - 均值) / 标准差，取负号
        """
        try:
            data = self.fetcher.get_shibor(days=30)
            if data is None or data.empty or '隔夜' not in data.columns:
                return None

            recent = data['隔夜'].iloc[-1]
            mean = data['隔夜'].mean()
            std = data['隔夜'].std()

            if std == 0:
                return 0

            # SHIBOR越低，因子值越高（流动性越好）
            zscore = -(recent - mean) / std
            return np.clip(zscore, -2, 2)

        except Exception as e:
            logger.error(f"计算SHIBOR因子失败: {e}")
            return None

    def calc_north_flow_factor(self, window: int = 5) -> Optional[float]:
        """
        计算北向资金净流入因子

        逻辑: 北向资金持续净流入 → 外资看好 → 利好市场
        因子值: 近N日累计净流入的Z-Score
        """
        try:
            data = self.fetcher.get_north_flow(days=30)
            if data is None or data.empty or 'north_flow_net' not in data.columns:
                return None

            # 计算累计净流入
            data['cumsum_flow'] = data['north_flow_net'].rolling(window).sum()

            recent = data['cumsum_flow'].iloc[-1]
            mean = data['cumsum_flow'].mean()
            std = data['cumsum_flow'].std()

            if std == 0:
                return 0

            zscore = (recent - mean) / std
            return np.clip(zscore, -2, 2)

        except Exception as e:
            logger.error(f"计算北向资金因子失败: {e}")
            return None

    def calc_market_volume_factor(self, window: int = 5) -> Optional[float]:
        """
        计算市场成交量因子

        逻辑: 成交量放大 → 市场活跃 → 需结合涨跌幅判断
        因子值: 成交量变化率的Z-Score
        """
        try:
            data = self.fetcher.get_market_volume(days=60)
            if data is None or data.empty or 'volume' not in data.columns:
                return None

            # 计算成交量变化率
            data['volume_ma'] = data['volume'].rolling(window).mean()
            data['volume_ratio'] = data['volume'] / data['volume_ma'] - 1

            recent = data['volume_ratio'].iloc[-1]
            mean = data['volume_ratio'].mean()
            std = data['volume_ratio'].std()

            if std == 0:
                return 0

            zscore = (recent - mean) / std
            return np.clip(zscore, -2, 2)

        except Exception as e:
            logger.error(f"计算市场成交量因子失败: {e}")
            return None

    def get_interpretation(self, factor_name: str, value: float) -> str:
        """获取因子解释"""
        interpretations = {
            'shibor_on': {
                'positive': '流动性宽松，利好市场',
                'negative': '流动性收紧，利空市场',
                'neutral': '流动性中性'
            },
            'north_flow_net': {
                'positive': '外资持续流入，市场情绪偏暖',
                'negative': '外资流出，市场压力较大',
                'neutral': '外资流入流出平衡'
            },
            'market_volume_ratio': {
                'positive': '成交量放大，市场活跃',
                'negative': '成交量萎缩，人气不足',
                'neutral': '成交正常'
            }
        }

        interp = interpretations.get(factor_name, {})
        if value > 0.5:
            return interp.get('positive', '')
        elif value < -0.5:
            return interp.get('negative', '')
        else:
            return interp.get('neutral', '')
