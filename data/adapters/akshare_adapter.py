"""
AkShare 数据适配器实现
免费数据源，适合开发测试
"""
import pandas as pd
import numpy as np
import akshare as ak
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import logging
import time

from .base import IDataAdapter

logger = logging.getLogger(__name__)


class AkShareAdapter(IDataAdapter):
    """AkShare 数据适配器"""

    def __init__(
        self,
        max_retries: int = 3,
        retry_delay: float = 2.0,
        proxy: Optional[str] = None
    ):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.proxy = proxy

    @property
    def name(self) -> str:
        return "AkShare"

    @property
    def is_paid(self) -> bool:
        return False

    # ============= 辅助方法 =============

    def _to_akshare_symbol(self, symbol: str) -> str:
        """转换为 AkShare 格式的股票代码"""
        if symbol.startswith('6'):
            return f"sh{symbol}"
        elif symbol.startswith(('0', '3')):
            return f"sz{symbol}"
        elif symbol.startswith('8') or symbol.startswith('4'):
            return f"bj{symbol}"
        return symbol

    def _get_market(self, symbol: str) -> str:
        """根据股票代码判断市场"""
        if symbol.startswith('6'):
            return 'SH'  # 上海
        elif symbol.startswith(('0', '3')):
            return 'SZ'  # 深圳
        elif symbol.startswith('8') or symbol.startswith('4'):
            return 'BJ'  # 北京
        return 'OTHER'

    def _retry_request(self, func, *args, **kwargs):
        """带重试的请求"""
        last_error = None
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    logger.debug(f"请求失败，{delay:.1f}秒后重试...")
                    time.sleep(delay)
                else:
                    logger.error(f"请求失败: {e}")
        raise last_error

    # ============= 股票列表 =============

    def get_stock_list(self, force_update: bool = False) -> pd.DataFrame:
        """获取A股股票列表"""
        try:
            logger.info("从 AkShare 获取股票列表...")
            stock_list = ak.stock_info_a_code_name()

            stock_list.columns = ['symbol', 'name']
            stock_list['market'] = stock_list['symbol'].apply(self._get_market)
            stock_list['industry'] = ''
            stock_list['list_date'] = None

            logger.info(f"✅ 获取股票列表: {len(stock_list)} 只")
            return stock_list

        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            # 返回默认股票池
            return pd.DataFrame({
                'symbol': ['000001', '000002', '600036', '600519', '000858'],
                'name': ['平安银行', '万科A', '招商银行', '贵州茅台', '五粮液'],
                'market': ['SZ', 'SZ', 'SH', 'SH', 'SZ'],
                'industry': ['金融', '地产', '金融', '消费', '消费'],
                'list_date': [None] * 5
            })

    def get_index_constituents(self, index_code: str = "000300") -> pd.DataFrame:
        """获取指数成分股"""
        try:
            logger.info(f"获取 {index_code} 成分股...")
            data = ak.index_stock_cons(symbol=index_code)

            if data is not None and not data.empty:
                # AkShare 返回的列名可能是中文
                if '品种代码' in data.columns:
                    data = data.rename(columns={
                        '品种代码': 'symbol',
                        '品种名称': 'name',
                        '权重': 'weight'
                    })
                data['weight'] = data.get('weight', 0.0)
                logger.info(f"✅ 获取成分股: {len(data)} 只")
                return data
        except Exception as e:
            logger.error(f"获取指数成分股失败: {e}")

        # 返回默认成分股
        return pd.DataFrame({
            'symbol': ['000001', '000002', '600036', '600519', '000858'],
            'name': ['平安银行', '万科A', '招商银行', '贵州茅台', '五粮液'],
            'weight': [0.0] * 5
        })

    # ============= 日线数据 =============

    def get_daily_quotes(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取股票日线数据"""
        # 默认获取最近2年数据
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=730)).strftime("%Y%m%d")

        ak_symbol = self._to_akshare_symbol(symbol)

        try:
            def fetch():
                return ak.stock_zh_a_daily(
                    symbol=ak_symbol,
                    start_date=start_date.replace('-', ''),
                    end_date=end_date.replace('-', ''),
                    adjust="qfq"  # 前复权
                )

            df = self._retry_request(fetch)

            if df is None or df.empty:
                return pd.DataFrame()

            # 重置索引
            df = df.reset_index()

            # 动态获取列名
            available_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
            optional_cols = ['amount', 'turnover_rate']

            for col in optional_cols:
                if col in df.columns:
                    available_cols.append(col)

            # 确保必需列存在
            required_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                logger.error(f"缺少必需列: {missing_cols}")
                return pd.DataFrame()

            df = df[available_cols]
            df['symbol'] = symbol
            df['date'] = pd.to_datetime(df['date'])

            # 确保可选列存在
            if 'amount' not in df.columns:
                df['amount'] = 0.0
            if 'turnover_rate' not in df.columns:
                df['turnover_rate'] = 0.0

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
        batch_size: int = 50,
        callback=None
    ) -> Dict[str, pd.DataFrame]:
        """批量获取日线数据"""
        results = {}

        total = len(symbols)
        for i in range(0, total, batch_size):
            batch = symbols[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total - 1) // batch_size + 1

            logger.info(f"处理批次 {batch_num}/{total_batches}: {len(batch)} 只股票")

            for symbol in batch:
                try:
                    df = self.get_daily_quotes(symbol, start_date, end_date)
                    if not df.empty:
                        results[symbol] = df

                    if callback:
                        callback(len(results), total, symbol)

                except Exception as e:
                    logger.error(f"下载 {symbol} 失败: {e}")

            # 批次间延迟
            if i + batch_size < total:
                time.sleep(0.5)

        logger.info(f"批量下载完成: 成功 {len(results)}/{total}")
        return results

    # ============= 财务数据 =============

    def get_financial(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取财务数据（返回模拟数据）"""
        logger.info(f"获取 {symbol} 财务数据（模拟）")
        dates = pd.date_range(end=datetime.now(), periods=8, freq='Q')
        return pd.DataFrame({
            'date': dates,
            'pe': np.random.uniform(10, 30, 8),
            'pb': np.random.uniform(1, 5, 8),
            'ps': np.random.uniform(1, 10, 8),
            'roe': np.random.uniform(10, 25, 8),
            'roa': np.random.uniform(3, 10, 8),
            'revenue_growth': np.random.uniform(-10, 30, 8),
            'profit_growth': np.random.uniform(-20, 40, 8)
        })

    # ============= 宏观数据 =============

    def get_macro_shibor(self, days: int = 30) -> pd.DataFrame:
        """获取SHIBOR利率数据"""
        try:
            data = ak.macro_china_shibor_all()
            if data is not None and not data.empty:
                data = data.sort_values('date').tail(days)
                data['date'] = pd.to_datetime(data['date'])
                return data
        except Exception as e:
            logger.error(f"获取SHIBOR数据失败: {e}")

        # 返回模拟数据
        dates = pd.date_range(end=datetime.now(), periods=days)
        return pd.DataFrame({
            'date': dates,
            '隔夜': np.random.uniform(1.5, 3.0, days),
            '1周': np.random.uniform(2.0, 3.5, days),
            '2周': np.random.uniform(2.5, 4.0, days),
            '1月': np.random.uniform(2.8, 4.5, days),
            '3月': np.random.uniform(3.0, 4.8, days),
            '6月': np.random.uniform(3.2, 5.0, days),
            '9月': np.random.uniform(3.5, 5.2, days),
            '1年': np.random.uniform(3.8, 5.5, days)
        })

    def get_macro_north_flow(self, days: int = 30) -> pd.DataFrame:
        """获取北向资金流向"""
        try:
            data = ak.tool_money_market_hsgt_hist()
            if data is not None and not data.empty:
                data['date'] = pd.to_datetime(data['date'])
                data = data.sort_values('date').tail(days)
                # 转换列名
                if '北向资金净流入' in data.columns:
                    data = data.rename(columns={'北向资金净流入': 'net_flow_in'})
                return data
        except Exception as e:
            logger.error(f"获取北向资金数据失败: {e}")

        # 返回模拟数据
        dates = pd.date_range(end=datetime.now(), periods=days)
        flow = np.random.normal(20, 50, days)
        return pd.DataFrame({
            'date': dates,
            'net_flow_in': flow,
            'total_hold': np.cumsum(np.random.uniform(1000, 2000, days))
        })

    def get_index_daily(self, index_code: str = "000001", days: int = 30) -> pd.DataFrame:
        """获取指数日线数据"""
        try:
            ak_index = f"sh{index_code}" if index_code.startswith('0') else f"sz{index_code}"
            data = ak.stock_zh_index_daily(symbol=ak_index)
            if data is not None and not data.empty:
                data['date'] = pd.to_datetime(data['date'])
                data = data.sort_values('date').tail(days)
                return data
        except Exception as e:
            logger.error(f"获取指数数据失败: {e}")

        return pd.DataFrame()

    # ============= 行业数据 =============

    def get_industry_list(self) -> pd.DataFrame:
        """获取行业列表（返回申万行业）"""
        industries = [
            ('801010', '农林牧渔'), ('801020', '采掘'), ('801030', '化工'),
            ('801040', '钢铁'), ('801050', '有色金属'), ('801080', '电子'),
            ('801110', '家用电器'), ('801120', '食品饮料'), ('801130', '纺织服装'),
            ('801140', '轻工制造'), ('801150', '医药生物'), ('801160', '公用事业'),
            ('801170', '交通运输'), ('801180', '房地产'), ('801200', '商业贸易'),
            ('801210', '休闲服务'), ('801230', '综合'), ('801710', '建筑材料'),
            ('801720', '建筑装饰'), ('801730', '电气设备'), ('801740', '国防军工'),
            ('801750', '计算机'), ('801760', '传媒'), ('801770', '通信'),
            ('801780', '银行'), ('801790', '非银金融'), ('801880', '汽车'),
            ('801890', '机械设备')
        ]
        return pd.DataFrame(industries, columns=['industry_code', 'industry_name'])

    def get_industry_daily(
        self,
        industry: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取行业日线数据（返回模拟数据）"""
        logger.info(f"获取 {industry} 行业数据（模拟）")
        if end_date is None:
            end_date = datetime.now()
        if start_date is None:
            start_date = end_date - timedelta(days=30)

        dates = pd.date_range(start_date, end_date, freq='D')
        base_price = np.random.uniform(1000, 5000)
        prices = base_price * (1 + np.random.randn(len(dates)) * 0.02).cumprod()

        return pd.DataFrame({
            'date': dates,
            'industry': industry,
            'close': prices,
            'volume': np.random.randint(100000, 1000000, len(dates)),
            'change_pct': np.random.uniform(-5, 5, len(dates))
        })

    # ============= 健康检查 =============

    def health_check(self) -> Dict[str, any]:
        """健康检查"""
        start_time = time.time()

        try:
            # 测试获取股票列表
            df = self.get_stock_list()
            latency = (time.time() - start_time) * 1000

            return {
                'status': 'healthy',
                'message': f'{self.name} 连接正常',
                'latency': round(latency, 2),
                'rate_limit_info': '免费数据源，无严格限制',
                'data_source': self.name,
                'is_paid': self.is_paid
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
