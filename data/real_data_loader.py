"""
实际数据加载模块 - 使用 AkShare
支持全市场股票数据获取，带网络重试机制
"""
import pandas as pd
import numpy as np
import akshare as ak
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


def get_session_with_retry(retries: int = 3, backoff: float = 1.0):
    """创建带重试机制的会话"""
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


# 配置 akshare 使用重试会话
try:
    import akshare as ak
    ak.session = get_session_with_retry(retries=3, backoff=2.0)
except:
    pass


class RealDataLoader:
    """实际数据加载器"""

    def __init__(self, db_path: str = "data/real_market.duckdb",
                 max_retries: int = 3, retry_delay: float = 2.0):
        self.db_path = db_path
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._ensure_db()

    def _ensure_db(self):
        """确保数据库存在"""
        import os
        import duckdb

        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        # 创建数据库连接
        self.conn = duckdb.connect(self.db_path)

        # 创建表结构
        self._create_tables()

        logger.info(f"数据库已初始化: {self.db_path}")

    def _create_tables(self):
        """创建数据表"""
        # 股票列表表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_list (
                symbol VARCHAR PRIMARY KEY,
                name VARCHAR,
                market VARCHAR,
                industry VARCHAR,
                list_date DATE,
                update_time TIMESTAMP
            )
        """)

        # 日线行情表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_quotes (
                date DATE,
                symbol VARCHAR,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT,
                amount FLOAT,
                turnover_rate FLOAT,
                PRIMARY KEY (date, symbol)
            )
        """)

        # 宏观数据表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS macro_data (
                date DATE PRIMARY KEY,
                shibor_1d FLOAT,
                shibor_1w FLOAT,
                north_flow FLOAT,
                market_volume_ratio FLOAT
            )
        """)

        logger.info("数据表已创建")

    def get_stock_list(self, force_update: bool = False) -> pd.DataFrame:
        """获取股票列表

        Args:
            force_update: 是否强制更新

        Returns:
            股票列表 DataFrame
        """
        # 检查是否已有数据
        existing = self.conn.execute("""
            SELECT COUNT(*) as cnt FROM stock_list
        """).fetchone()[0]

        if existing > 0 and not force_update:
            logger.info(f"使用已存储的股票列表: {existing} 只")
            return self.conn.execute("SELECT * FROM stock_list").fetchdf()

        # 从 AkShare 获取
        logger.info("从 AkShare 获取股票列表...")

        try:
            # 获取A股列表
            stock_list = ak.stock_info_a_code_name()

            # 转换列名
            stock_list.columns = ['symbol', 'name']

            # 添加市场信息
            stock_list['market'] = stock_list['symbol'].apply(self._get_market)
            stock_list['industry'] = ''
            stock_list['list_date'] = None
            stock_list['update_time'] = datetime.now()

            # 存入数据库
            self._save_stock_list(stock_list)

            logger.info(f"股票列表已更新: {len(stock_list)} 只")

            return stock_list

        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            # 如果有旧数据，返回旧数据
            if existing > 0:
                return self.conn.execute("SELECT * FROM stock_list").fetchdf()
            raise

    def _get_market(self, symbol: str) -> str:
        """根据股票代码判断市场"""
        if symbol.startswith('6'):
            return 'SH'  # 上海
        elif symbol.startswith(('0', '3')):
            return 'SZ'  # 深圳
        elif symbol.startswith('8') or symbol.startswith('4'):
            return 'BJ'  # 北京
        else:
            return 'OTHER'

    def _save_stock_list(self, df: pd.DataFrame):
        """保存股票列表到数据库"""
        # 删除旧数据
        self.conn.execute("DELETE FROM stock_list")

        # 插入新数据
        self.conn.register('stock_list_df', df)
        self.conn.execute("""
            INSERT INTO stock_list SELECT * FROM stock_list_df
        """)

    def get_daily_quotes(self, symbol: str, start_date: str = None,
                         end_date: str = None) -> pd.DataFrame:
        """获取单只股票的日线数据（带重试机制）

        Args:
            symbol: 股票代码
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)

        Returns:
            日线数据 DataFrame
        """
        # 默认获取最近一年数据
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")

        # 先检查数据库
        existing = self.conn.execute("""
            SELECT * FROM daily_quotes
            WHERE symbol = ? AND date >= ? AND date <= ?
            ORDER BY date
        """, [symbol, start_date, end_date]).fetchdf()

        if not existing.empty:
            logger.info(f"从数据库获取 {symbol} 的日线数据: {len(existing)} 条")
            return existing

        # 从 AkShare 获取（带重试）
        ak_symbol = self._to_akshare_symbol(symbol)

        for attempt in range(self.max_retries):
            try:
                logger.info(f"获取 {symbol} 日线数据 (尝试 {attempt + 1}/{self.max_retries})...")

                # 使用备用接口 stock_zh_a_daily（更稳定）
                quotes = ak.stock_zh_a_daily(
                    symbol=ak_symbol,
                    start_date=start_date.replace('-', ''),  # 20260101
                    end_date=end_date.replace('-', ''),      # 20260228
                    adjust="qfq"  # 前复权
                )

                # 重置索引
                quotes = quotes.reset_index()

                # 动态获取列名，避免列数不匹配
                logger.info(f"返回数据列: {list(quotes.columns)}")

                # 确保必需列存在
                required_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
                missing_cols = [c for c in required_cols if c not in quotes.columns]
                if missing_cols:
                    logger.error(f"缺少必需列: {missing_cols}")
                    return pd.DataFrame()

                # 选择需要的列（如果有的话）
                available_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
                optional_cols = ['amount', 'turnover_rate']
                for col in optional_cols:
                    if col in quotes.columns:
                        available_cols.append(col)

                quotes = quotes[available_cols]
                quotes['symbol'] = symbol
                quotes['date'] = pd.to_datetime(quotes['date'])

                # 确保可选列存在
                if 'amount' not in quotes.columns:
                    quotes['amount'] = 0.0
                if 'turnover_rate' not in quotes.columns:
                    quotes['turnover_rate'] = 0.0

                # 存入数据库
                self._save_quotes(quotes)

                logger.info(f"✅ 从 AkShare 获取 {symbol} 的日线数据: {len(quotes)} 条")

                return quotes

            except Exception as e:
                logger.warning(f"获取 {symbol} 失败 (尝试 {attempt + 1}/{self.max_retries}): {e}")

                if attempt < self.max_retries - 1:
                    # 指数退避延迟
                    delay = self.retry_delay * (2 ** attempt)
                    logger.info(f"等待 {delay:.1f} 秒后重试...")
                    time.sleep(delay)
                else:
                    logger.error(f"获取 {symbol} 日线数据失败: 已达到最大重试次数")
                    return pd.DataFrame()

        return pd.DataFrame()

    def _to_akshare_symbol(self, symbol: str) -> str:
        """转换为 AkShare 格式的股票代码"""
        if symbol.startswith('6'):
            return f"sh{symbol}"
        else:
            return f"sz{symbol}"

    def _save_quotes(self, df: pd.DataFrame):
        """保存日线数据到数据库"""
        # 删除重复数据
        for symbol in df['symbol'].unique():
            self.conn.execute(
                "DELETE FROM daily_quotes WHERE symbol = ?",
                [symbol]
            )

        # 插入新数据
        self.conn.register('quotes_df', df)
        self.conn.execute("""
            INSERT INTO daily_quotes SELECT * FROM quotes_df
        """)

    def batch_download_quotes(self, symbols: List[str] = None,
                               start_date: str = None, end_date: str = None,
                               batch_size: int = 50, delay: float = 1.0) -> Dict:
        """批量下载日线数据

        Args:
            symbols: 股票代码列表，None 则下载全市场
            start_date: 开始日期
            end_date: 结束日期
            batch_size: 每批处理数量
            delay: 批次间延迟（秒）

        Returns:
            下载统计信息
        """
        if symbols is None:
            stock_list = self.get_stock_list()
            symbols = stock_list['symbol'].tolist()

        logger.info(f"开始批量下载 {len(symbols)} 只股票的日线数据...")

        stats = {
            'total': len(symbols),
            'success': 0,
            'failed': 0,
            'failed_list': []
        }

        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            logger.info(f"处理批次 {i//batch_size + 1}/{(len(symbols)-1)//batch_size + 1}: {len(batch)} 只股票")

            for symbol in batch:
                try:
                    df = self.get_daily_quotes(symbol, start_date, end_date)
                    if not df.empty:
                        stats['success'] += 1
                    else:
                        stats['failed'] += 1
                        stats['failed_list'].append(symbol)
                except Exception as e:
                    logger.error(f"下载 {symbol} 失败: {e}")
                    stats['failed'] += 1
                    stats['failed_list'].append(symbol)

            # 批次间延迟
            if i + batch_size < len(symbols):
                time.sleep(delay)

        logger.info(f"批量下载完成: 成功 {stats['success']}, 失败 {stats['failed']}")

        return stats

    def get_macro_data(self, date: str = None) -> pd.DataFrame:
        """获取宏观数据

        Args:
            date: 查询日期，None 则获取最新

        Returns:
            宏观数据 DataFrame
        """
        # 先检查数据库
        if date:
            existing = self.conn.execute("""
                SELECT * FROM macro_data WHERE date = ?
            """, [date]).fetchdf()
        else:
            existing = self.conn.execute("""
                SELECT * FROM macro_data ORDER BY date DESC LIMIT 1
            """).fetchdf()

        if not existing.empty:
            return existing

        # TODO: 从 AkShare 获取宏观数据
        # 这里先返回空数据
        return pd.DataFrame()


# 便捷函数
def get_real_data_loader() -> RealDataLoader:
    """获取实盘数据加载器单例"""
    import os
    from pathlib import Path

    # 确保在项目根目录
    project_root = Path(__file__).parent.parent
    os.chdir(project_root)

    return RealDataLoader()
