"""
Tushare 数据适配器实现
付费数据源，数据质量更高，适合生产环境

安装: pip install tushare
获取Token: https://tushare.pro/user/token
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import logging

try:
    import tushare as ts
    TUSHARE_AVAILABLE = True
except ImportError:
    TUSHARE_AVAILABLE = False

from .base import IDataAdapter

logger = logging.getLogger(__name__)


class TushareAdapter(IDataAdapter):
    """Tushare 数据适配器

    配置方式:
    1. 在 tushare.pro 注册账号
    2. 获取 API Token
    3. 在初始化时传入 token

    示例:
        adapter = TushareAdapter(token='your_token_here')
    """

    def __init__(
        self,
        token: str,
        timeout: int = 30,
        max_retries: int = 3
    ):
        if not TUSHARE_AVAILABLE:
            raise ImportError("Tushare 未安装，请运行: pip install tushare")

        self.token = token
        self.timeout = timeout
        self.max_retries = max_retries

        # 初始化 Tushare
        ts.set_token(token)
        self.pro = ts.pro_api(timeout=timeout)

    @property
    def name(self) -> str:
        return "Tushare"

    @property
    def is_paid(self) -> bool:
        return True

    # ============= 辅助方法 =============

    def _retry_request(self, func, *args, **kwargs):
        """带重试的请求"""
        last_error = None
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    logger.debug(f"请求失败，重试 {attempt + 1}/{self.max_retries}")
                else:
                    logger.error(f"请求失败: {e}")
        raise last_error

    # ============= 股票列表 =============

    def get_stock_list(self, force_update: bool = False) -> pd.DataFrame:
        """获取A股股票列表"""
        try:
            logger.info("从 Tushare 获取股票列表...")
            data = self._retry_request(
                self.pro.stock_basic,
                exchange='',
                list_status='L',
                fields='ts_code,symbol,name,area,industry,list_date'
            )

            if data is not None and not data.empty:
                # 转换为统一格式
                df = pd.DataFrame({
                    'symbol': data['symbol'].str.zfill(6),  # 补齐到6位
                    'name': data['name'],
                    'market': data['ts_code'].str[:2].str.upper(),
                    'industry': data['industry'],
                    'list_date': pd.to_datetime(data['list_date'], format='%Y%m%d', errors='coerce')
                })
                logger.info(f"✅ 获取股票列表: {len(df)} 只")
                return df

        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")

        return pd.DataFrame()

    def get_index_constituents(self, index_code: str = "000300") -> pd.DataFrame:
        """获取指数成分股

        Tushare 指数代码:
        - 000300.SH: 沪深300
        - 000016.SH: 上证50
        - 399006.SZ: 创业板指
        - 000905.SH: 中证500
        """
        try:
            logger.info(f"获取 {index_code} 成分股...")
            ts_index = f"{index_code}.SH" if index_code.startswith('0') else f"{index_code}.SZ"

            data = self._retry_request(
                self.pro.index_weight,
                index_code=ts_index,
                start_date='20200101'
            )

            if data is not None and not data.empty:
                # 取最新日期的权重
                latest_date = data['trade_date'].max()
                data = data[data['trade_date'] == latest_date]

                df = pd.DataFrame({
                    'symbol': data['con_code'].str[:6],
                    'name': '',  # 需要额外查询
                    'weight': data['weight'].values
                })
                logger.info(f"✅ 获取成分股: {len(df)} 只")
                return df

        except Exception as e:
            logger.error(f"获取指数成分股失败: {e}")

        return pd.DataFrame()

    # ============= 日线数据 =============

    def get_daily_quotes(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取股票日线数据"""
        # Tushare 格式: 600000.SH
        if symbol.startswith('6'):
            ts_code = f"{symbol}.SH"
        else:
            ts_code = f"{symbol}.SZ"

        # 日期格式转换
        if start_date:
            start_date = start_date.replace('-', '')
        if end_date:
            end_date = end_date.replace('-', '')

        try:
            def fetch():
                return self.pro.daily(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date
                )

            data = self._retry_request(fetch)

            if data is None or data.empty:
                return pd.DataFrame()

            # 转换为统一格式
            df = pd.DataFrame({
                'date': pd.to_datetime(data['trade_date'], format='%Y%m%d'),
                'open': data['open'],
                'high': data['high'],
                'low': data['low'],
                'close': data['close'],
                'volume': data['vol'],
                'amount': data['amount'],
                'turnover_rate': 0.0  # 需要额外接口获取
            })
            df['symbol'] = symbol

            logger.debug(f"✅ 获取 {symbol} 日线数据: {len(df)} 条")
            return df

        except Exception as e:
            logger.error(f"获取 {symbol} 日线数据失败: {e}")
            return pd.DataFrame()

    def batch_get_daily_quotes(
        self,
        symbols: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        batch_size: int = 100,
        callback=None
    ) -> Dict[str, pd.DataFrame]:
        """批量获取日线数据"""
        results = {}

        total = len(symbols)
        for i, symbol in enumerate(symbols):
            try:
                df = self.get_daily_quotes(symbol, start_date, end_date)
                if not df.empty:
                    results[symbol] = df

                if callback:
                    callback(len(results), total, symbol)

                # 每批次休息
                if (i + 1) % batch_size == 0 and i + 1 < total:
                    import time
                    time.sleep(0.5)

            except Exception as e:
                logger.error(f"下载 {symbol} 失败: {e}")

        logger.info(f"批量下载完成: 成功 {len(results)}/{total}")
        return results

    # ============= 财务数据 =============

    def get_financial(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取财务数据"""
        if symbol.startswith('6'):
            ts_code = f"{symbol}.SH"
        else:
            ts_code = f"{symbol}.SZ"

        try:
            # 获取最新财务指标
            data = self._retry_request(
                self.pro.fina_indicator,
                ts_code=ts_code,
                start_date=start_date.replace('-', '') if start_date else None,
                end_date=end_date.replace('-', '') if end_date else None
            )

            if data is not None and not data.empty:
                df = pd.DataFrame({
                    'date': pd.to_datetime(data['end_date'], format='%Y%m%d'),
                    'pe': data['pe'],
                    'pb': data['pb'],
                    'ps': data['ps'],
                    'roe': data['roe'],
                    'roa': data['roa'],
                    'revenue_growth': data['or_yoy'],
                    'profit_growth': data['basic_eps_yoy']
                })
                logger.info(f"✅ 获取 {symbol} 财务数据: {len(df)} 条")
                return df

        except Exception as e:
            logger.error(f"获取 {symbol} 财务数据失败: {e}")

        return pd.DataFrame()

    # ============= 宏观数据 =============

    def get_macro_shibor(self, days: int = 30) -> pd.DataFrame:
        """获取SHIBOR利率数据"""
        try:
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
            end_date = datetime.now().strftime('%Y%m%d')

            data = self._retry_request(
                self.pro.shibor,
                start_date=start_date,
                end_date=end_date
            )

            if data is not None and not data.empty:
                data['date'] = pd.to_datetime(data['date'], format='%Y%m%d')
                return data

        except Exception as e:
            logger.error(f"获取SHIBOR数据失败: {e}")

        return pd.DataFrame()

    def get_macro_north_flow(self, days: int = 30) -> pd.DataFrame:
        """获取北向资金流向"""
        try:
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
            end_date = datetime.now().strftime('%Y%m%d')

            data = self._retry_request(
                self.pro.moneyflow_hsgt,
                start_date=start_date,
                end_date=end_date
            )

            if data is not None and not data.empty:
                data['date'] = pd.to_datetime(data['trade_date'], format='%Y%m%d')
                df = pd.DataFrame({
                    'date': data['date'],
                    'net_flow_in': data['north_money'],
                    'total_hold': data['north_holding']
                })
                return df

        except Exception as e:
            logger.error(f"获取北向资金数据失败: {e}")

        return pd.DataFrame()

    def get_index_daily(self, index_code: str = "000001", days: int = 30) -> pd.DataFrame:
        """获取指数日线数据"""
        ts_index = f"{index_code}.SH" if index_code.startswith('0') else f"{index_code}.SZ"

        try:
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')

            data = self._retry_request(
                self.pro.index_daily,
                ts_code=ts_index,
                start_date=start_date
            )

            if data is not None and not data.empty:
                df = pd.DataFrame({
                    'date': pd.to_datetime(data['trade_date'], format='%Y%m%d'),
                    'open': data['open'],
                    'high': data['high'],
                    'low': data['low'],
                    'close': data['close'],
                    'volume': data['vol'],
                    'amount': data['amount']
                })
                return df

        except Exception as e:
            logger.error(f"获取指数数据失败: {e}")

        return pd.DataFrame()

    # ============= 行业数据 =============

    def get_industry_list(self) -> pd.DataFrame:
        """获取行业列表"""
        try:
            data = self._retry_request(
                self.pro.index_classify,
                level='L1',
                src='SW2021'
            )

            if data is not None and not data.empty:
                df = pd.DataFrame({
                    'industry_code': data['index_code'],
                    'industry_name': data['industry_name']
                })
                return df

        except Exception as e:
            logger.error(f"获取行业列表失败: {e}")

        return pd.DataFrame()

    def get_industry_daily(
        self,
        industry: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取行业日线数据"""
        # Tushare 暂不支持行业日线，需要通过成分股聚合
        logger.warning(f"Tushare 暂不支持行业日线数据，请使用成分股聚合")
        return pd.DataFrame()

    # ============= 健康检查 =============

    def health_check(self) -> Dict[str, any]:
        """健康检查"""
        import time
        start_time = time.time()

        try:
            # 测试获取股票列表
            df = self.get_stock_list()
            latency = (time.time() - start_time) * 1000

            return {
                'status': 'healthy',
                'message': f'{self.name} 连接正常',
                'latency': round(latency, 2),
                'rate_limit_info': f'每分钟 {self.pro.query("api,limit,per_min").iloc[0]["per_min"]} 次',
                'data_source': self.name,
                'is_paid': self.is_paid,
                'account_points': self.pro.query("api,limit,per_min").iloc[0]["per_min"]
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'message': f'{self.name} 连接异常: {str(e)}',
                'latency': None,
                'rate_limit_info': None,
                'data_source': self.name,
                'is_paid': self.is_paid
            }
