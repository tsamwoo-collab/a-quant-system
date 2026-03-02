"""
中观因子计算 - 行业层面
"""
import pandas as pd
import numpy as np
from typing import Dict, Optional, List
import logging

logger = logging.getLogger(__name__)


class MesoFactors:
    """中观（行业）因子计算器"""

    def __init__(self, data_fetcher, data_storage):
        self.fetcher = data_fetcher
        self.storage = data_storage

    def calculate_for_stock(self, symbol: str, industry: str) -> pd.Series:
        """
        计算单个股票的行业因子

        Args:
            symbol: 股票代码
            industry: 所属行业

        Returns:
            因子值序列
        """
        factors = {}

        # 行业相对强度
        strength = self.calc_industry_relative_strength(industry)
        if strength is not None:
            factors['industry_relative_strength'] = strength

        # 行业涨跌幅排名
        rank = self.calc_industry_rank(industry)
        if rank is not None:
            factors['industry_rank'] = rank

        # 行业PE分位数
        pe_pct = self.calc_industry_pe_percentile(industry)
        if pe_pct is not None:
            factors['industry_pe_percentile'] = pe_pct

        # 行业ROE趋势
        roe_trend = self.calc_industry_roe_trend(industry)
        if roe_trend is not None:
            factors['industry_roe_trend'] = roe_trend

        return pd.Series(factors)

    def calc_industry_relative_strength(self, industry: str, period: int = 20) -> Optional[float]:
        """
        计算行业相对强度

        逻辑: 行业涨幅 / 市场涨幅
        >1 表示跑赢市场
        """
        try:
            # 获取行业指数数据
            industry_data = self.fetcher.get_industry_daily(industry, days=period+10)
            if industry_data is None or len(industry_data) < period:
                return None

            # 获取市场数据（上证指数）
            market_data = self.fetcher.get_market_volume(days=period+10)

            if industry_data is None or market_data is None:
                return None

            # 计算涨跌幅
            industry_return = (industry_data['close'].iloc[-1] / industry_data['close'].iloc[-period] - 1)
            market_return = (market_data['close'].iloc[-1] / market_data['close'].iloc[-period] - 1)

            if market_return == 0:
                return 0

            # 相对强度
            rs = industry_return / market_return if market_return != 0 else 0

            # 转换为 -2 到 2 的因子值
            # rs > 1 表示跑赢，rs < 1 表示跑输
            factor = np.clip((rs - 1) * 2, -2, 2)
            return factor

        except Exception as e:
            logger.error(f"计算行业相对强度失败: {e}")
            return None

    def calc_industry_rank(self, industry: str) -> Optional[float]:
        """
        计算行业涨跌幅排名因子

        逻辑: 行业当日涨跌幅在所有行业中的排名分位数
        """
        try:
            # 获取所有行业指数
            all_industries = self.fetcher.get_industry_index()
            if all_industries is None or all_industries.empty:
                return None

            # 转换排名为分位数因子
            # 假设数据中有涨跌幅列
            if '涨跌幅' not in all_industries.columns:
                return None

            # 计算当前行业的排名
            current_change = all_industries[all_industries.index == industry]['涨跌幅']
            if current_change.empty:
                # 获取所有行业的涨跌幅进行排名
                changes = all_industries['涨跌幅'].values
                rank = (changes > changes.mean()).sum() / len(changes)
            else:
                rank = (all_industries['涨跌幅'] <= current_change.iloc[0]).sum() / len(all_industries)

            # 转换为 -1 到 1 的因子值
            factor = (rank - 0.5) * 2
            return np.clip(factor, -1, 1)

        except Exception as e:
            logger.error(f"计算行业排名失败: {e}")
            return None

    def calc_industry_pe_percentile(self, industry: str, lookback_days: int = 252) -> Optional[float]:
        """
        计算行业PE分位数

        逻辑: 当前PE在历史中的分位数
        低分位数表示估值便宜
        """
        try:
            # 这里需要获取历史PE数据，AkShare可能不直接提供
            # 使用近似方法：基于行业指数涨跌幅代理估值变化
            industry_data = self.fetcher.get_industry_daily(industry, days=lookback_days)
            if industry_data is None or len(industry_data) < 60:
                return None

            # 使用价格相对位置作为估值代理
            current_price = industry_data['close'].iloc[-1]
            min_price = industry_data['close'].min()
            max_price = industry_data['close'].max()

            if max_price == min_price:
                return 0

            # 价格分位数（越低表示越便宜）
            percentile = (current_price - min_price) / (max_price - min_price)

            # 转换为 -1 到 1 的因子值，价格越低因子值越高
            factor = (0.5 - percentile) * 2
            return np.clip(factor, -1, 1)

        except Exception as e:
            logger.error(f"计算行业PE分位数失败: {e}")
            return None

    def calc_industry_roe_trend(self, industry: str, window: int = 4) -> Optional[float]:
        """
        计算行业ROE趋势

        逻辑: ROE的近期趋势
        上升趋势表示行业盈利能力改善
        """
        try:
            # AkShare 行业ROE数据可能不直接可用
            # 使用行业指数的动量作为盈利能力的代理指标
            industry_data = self.fetcher.get_industry_daily(industry, days=60)
            if industry_data is None or len(industry_data) < window:
                return None

            # 计算价格动量趋势
            returns = industry_data['close'].pct_change().dropna()

            # 线性回归斜率
            x = np.arange(len(returns))
            slope = np.polyfit(x, returns, 1)[0]

            # 标准化
            factor = np.clip(slope * 100, -2, 2)
            return factor

        except Exception as e:
            logger.error(f"计算行业ROE趋势失败: {e}")
            return None

    def get_industry_mapping(self) -> Dict[str, str]:
        """
        获取股票到行业的映射关系

        Returns:
            {股票代码: 行业名称}
        """
        try:
            # 获取股票列表
            stocks = self.fetcher.get_stock_list()
            if stocks is None or stocks.empty:
                return {}

            # AkShare 股票数据中通常有行业信息
            if '行业' in stocks.columns:
                mapping = stocks.set_index('代码')['行业'].to_dict()
                return mapping

            return {}

        except Exception as e:
            logger.error(f"获取行业映射失败: {e}")
            return {}
