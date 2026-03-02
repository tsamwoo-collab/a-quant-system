"""
数据适配器抽象基类
定义统一的数据接口，支持多种数据源切换
"""
from abc import ABC, abstractmethod
from typing import Optional, List, Dict
from datetime import datetime
import pandas as pd


class IDataAdapter(ABC):
    """数据适配器接口"""

    @property
    @abstractmethod
    def name(self) -> str:
        """数据源名称"""
        pass

    @property
    @abstractmethod
    def is_paid(self) -> bool:
        """是否为付费数据源"""
        pass

    # ============= 股票列表 =============

    @abstractmethod
    def get_stock_list(self, force_update: bool = False) -> pd.DataFrame:
        """获取股票列表

        Returns:
            DataFrame with columns: symbol, name, market, industry, list_date
        """
        pass

    @abstractmethod
    def get_index_constituents(self, index_code: str = "000300") -> pd.DataFrame:
        """获取指数成分股

        Args:
            index_code: 指数代码 (000300=沪深300, 000016=上证50, 399006=创业板指)

        Returns:
            DataFrame with columns: symbol, name, weight
        """
        pass

    # ============= 日线数据 =============

    @abstractmethod
    def get_daily_quotes(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取股票日线数据

        Args:
            symbol: 股票代码
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)

        Returns:
            DataFrame with columns: date, open, high, low, close, volume, amount, turnover_rate
        """
        pass

    @abstractmethod
    def batch_get_daily_quotes(
        self,
        symbols: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        batch_size: int = 50,
        callback=None
    ) -> Dict[str, pd.DataFrame]:
        """批量获取日线数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            batch_size: 批次大小
            callback: 进度回调函数 callback(current, total, symbol)

        Returns:
            Dict[symbol, DataFrame]
        """
        pass

    # ============= 财务数据 =============

    @abstractmethod
    def get_financial(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取财务数据

        Returns:
            DataFrame with columns: date, pe, pb, ps, roe, roa, revenue_growth, profit_growth
        """
        pass

    # ============= 宏观数据 =============

    @abstractmethod
    def get_macro_shibor(self, days: int = 30) -> pd.DataFrame:
        """获取SHIBOR利率数据

        Returns:
            DataFrame with columns: date, overnight, 1week, 2week, 1month, 3month, 6month, 9month, 1year
        """
        pass

    @abstractmethod
    def get_macro_north_flow(self, days: int = 30) -> pd.DataFrame:
        """获取北向资金流向

        Returns:
            DataFrame with columns: date, net_flow_in, total_hold
        """
        pass

    @abstractmethod
    def get_index_daily(self, index_code: str = "000001", days: int = 30) -> pd.DataFrame:
        """获取指数日线数据

        Args:
            index_code: 指数代码 (000001=上证指数, 399001=深证成指)

        Returns:
            DataFrame with columns: date, open, high, low, close, volume, amount
        """
        pass

    # ============= 行业数据 =============

    @abstractmethod
    def get_industry_list(self) -> pd.DataFrame:
        """获取行业列表

        Returns:
            DataFrame with columns: industry_code, industry_name
        """
        pass

    @abstractmethod
    def get_industry_daily(
        self,
        industry: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取行业日线数据

        Returns:
            DataFrame with columns: date, industry, close, volume, change_pct
        """
        pass

    # ============= 健康检查 =============

    @abstractmethod
    def health_check(self) -> Dict[str, any]:
        """健康检查

        Returns:
            Dict with keys: status, message, latency, rate_limit_info
        """
        pass


class AdapterConfig:
    """适配器配置"""

    def __init__(
        self,
        adapter_type: str = "akshare",
        akshare_config: Optional[Dict] = None,
        tushare_config: Optional[Dict] = None,
        cache_enabled: bool = True,
        cache_ttl: int = 3600
    ):
        self.adapter_type = adapter_type
        self.akshare_config = akshare_config or {}
        self.tushare_config = tushare_config or {}
        self.cache_enabled = cache_enabled
        self.cache_ttl = cache_ttl

    @classmethod
    def from_dict(cls, config: Dict) -> "AdapterConfig":
        """从字典创建配置"""
        return cls(
            adapter_type=config.get("adapter_type", "akshare"),
            akshare_config=config.get("akshare_config", {}),
            tushare_config=config.get("tushare_config", {}),
            cache_enabled=config.get("cache_enabled", True),
            cache_ttl=config.get("cache_ttl", 3600)
        )

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            "adapter_type": self.adapter_type,
            "akshare_config": self.akshare_config,
            "tushare_config": self.tushare_config,
            "cache_enabled": self.cache_enabled,
            "cache_ttl": self.cache_ttl
        }
