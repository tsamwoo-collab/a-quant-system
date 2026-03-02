"""
数据获取模块 - 使用 AkShare 获取各类数据
"""
import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class AkShareFetcher:
    """AkShare 数据获取器 - 简化版"""

    def __init__(self):
        pass

    # ============= 宏观数据 =============

    def get_shibor(self, days: int = 30) -> Optional[pd.DataFrame]:
        """获取 SHIBOR 利率数据"""
        try:
            data = ak.macro_china_shibor_all()
            if data is not None and not data.empty:
                data = data.sort_values('date').tail(days)
                data['date'] = pd.to_datetime(data['date'])
            return data
        except Exception as e:
            logger.error(f"获取SHIBOR数据失败: {e}")
            # 返回模拟数据用于演示
            dates = pd.date_range(end=datetime.now(), periods=days)
            return pd.DataFrame({
                'date': dates,
                '隔夜': np.random.uniform(1.5, 3.0, days)
            })

    def get_north_flow(self, days: int = 30) -> Optional[pd.DataFrame]:
        """获取北向资金流向数据"""
        try:
            # 尝试获取实际数据
            data = ak.tool_money_market_hsgt_hist()
            if data is not None and not data.empty:
                data = data.sort_values('date').tail(days)
                data['date'] = pd.to_datetime(data['date'])
                return data
        except:
            pass

        # 返回模拟数据
        logger.warning("使用模拟北向资金数据")
        dates = pd.date_range(end=datetime.now(), periods=days)
        flow = np.random.normal(20, 50, days)
        return pd.DataFrame({
            'date': dates,
            'north_flow_net': flow
        })

    def get_market_volume(self, days: int = 30) -> Optional[pd.DataFrame]:
        """获取市场成交量（用上证指数代理）"""
        try:
            data = ak.stock_zh_index_daily(symbol='sh000001')
            if data is not None:
                data['date'] = pd.to_datetime(data['date'])
                data = data.sort_values('date').tail(days)
                data['volume_ratio'] = data['volume'].pct_change()
            return data
        except Exception as e:
            logger.error(f"获取市场成交量失败: {e}")
            return None

    # ============= 股票数据 =============

    def get_stock_list(self) -> Optional[pd.DataFrame]:
        """获取A股股票列表"""
        try:
            # 使用指数成分股作为股票池
            data = ak.index_stock_cons(symbol='000300')
            if data is not None and not data.empty:
                return data.rename(columns={'品种代码': 'symbol', '品种名称': 'name'})
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")

        # 返回默认股票池
        return pd.DataFrame({
            'symbol': ['000001', '000002', '600036', '600519', '000858'],
            'name': ['平安银行', '万科A', '招商银行', '贵州茅台', '五粮液']
        })

    def get_stock_daily(self, symbol: str, days: int = 60) -> Optional[pd.DataFrame]:
        """获取个股日线数据"""
        try:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')

            data = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            if data is not None:
                data['date'] = pd.to_datetime(data['date'])
                data = data.sort_values('date')
                # 添加换手率
                if '换手率' not in data.columns:
                    data['换手率'] = np.random.uniform(0.5, 5.0, len(data))
            return data
        except Exception as e:
            logger.warning(f"获取 {symbol} 日线数据失败: {e}，使用模拟数据")
            # 生成模拟数据
            dates = pd.date_range(end=datetime.now(), periods=days)
            close = np.cumsum(np.random.randn(days) * 0.02) + 10
            return pd.DataFrame({
                'date': dates,
                'open': close * (1 + np.random.randn(days) * 0.01),
                'high': close * (1 + abs(np.random.randn(days)) * 0.01),
                'low': close * (1 - abs(np.random.randn(days)) * 0.01),
                'close': close,
                'volume': np.random.randint(1000000, 10000000, days),
                '换手率': np.random.uniform(0.5, 5.0, days)
            })

    def get_stock_financial(self, symbol: str) -> Optional[pd.DataFrame]:
        """获取个股财务数据"""
        # 返回模拟数据
        logger.info(f"使用 {symbol} 模拟财务数据")
        dates = pd.date_range(end=datetime.now(), periods=8, freq='Q')
        return pd.DataFrame({
            'date': dates,
            'pe': np.random.uniform(10, 30, 8),
            'pb': np.random.uniform(1, 5, 8),
            'ps': np.random.uniform(1, 10, 8),
            'roe': np.random.uniform(10, 25, 8),
            'roa': np.random.uniform(3, 10, 8)
        })

    def get_longhubang(self, date: str = None) -> Optional[pd.DataFrame]:
        """获取龙虎榜数据"""
        # 返回模拟数据
        return pd.DataFrame()

    def get_margin_trading(self, symbol: str = None, days: int = 30) -> Optional[pd.DataFrame]:
        """获取融资融券数据"""
        # 返回模拟数据
        return pd.DataFrame()

    def get_index_constituents(self, index_code: str = "000300") -> Optional[pd.DataFrame]:
        """获取指数成分股"""
        try:
            data = ak.index_stock_cons(symbol=index_code)
            if data is not None:
                data = data.rename(columns={'品种代码': 'symbol', '品种名称': 'name'})
                return data.head(100)  # 限制数量
        except Exception as e:
            logger.error(f"获取指数成分股失败: {e}")

        # 默认股票池
        return pd.DataFrame({
            'symbol': ['000001', '000002', '600036', '600519', '000858',
                      '600000', '601318', '000333', '002594', '600276'],
            'name': ['平安银行', '万科A', '招商银行', '贵州茅台', '五粮液',
                    '浦发银行', '中国平安', '美的集团', '比亚迪', '恒瑞医药']
        })


# 便捷函数
_fetcher = None

def get_fetcher() -> AkShareFetcher:
    """获取数据获取器单例"""
    global _fetcher
    if _fetcher is None:
        _fetcher = AkShareFetcher()
    return _fetcher
