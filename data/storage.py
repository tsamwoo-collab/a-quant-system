"""
数据存储模块 - 使用 DuckDB 存储时序数据
"""
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


class DataStorage:
    """数据存储管理器 - 使用 DuckDB"""

    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = Path(__file__).parent.parent / "data" / "quant.duckdb"
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.db_path))
        self._init_tables()

    def _init_tables(self):
        """初始化数据表"""
        # 宏观数据表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS macro_data (
                date DATE,
                indicator VARCHAR,
                value DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (date, indicator)
            )
        """)

        # 行业数据表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS industry_data (
                date DATE,
                industry VARCHAR,
                close DOUBLE,
                volume DOUBLE,
                change_pct DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (date, industry)
            )
        """)

        # 股票日线数据表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_daily (
                date DATE,
                symbol VARCHAR,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                amount DOUBLE,
                turnover_rate DOUBLE,
                change_pct DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (date, symbol)
            )
        """)

        # 股票财务数据表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_financial (
                date DATE,
                symbol VARCHAR,
                pe DOUBLE,
                pb DOUBLE,
                ps DOUBLE,
                roe DOUBLE,
                roa DOUBLE,
                revenue_growth DOUBLE,
                profit_growth DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (date, symbol)
            )
        """)

        # 因子数据表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS factor_data (
                date DATE,
                symbol VARCHAR,
                factor_name VARCHAR,
                factor_value DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (date, symbol, factor_name)
            )
        """)

        # 信号数据表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS signal_data (
                date DATE,
                symbol VARCHAR,
                signal DOUBLE,
                signal_level VARCHAR,
                macro_score DOUBLE,
                meso_score DOUBLE,
                micro_score DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (date, symbol)
            )
        """)

        logger.info("数据表初始化完成")

    # ============= 宏观数据 =============

    def save_macro(self, data: pd.DataFrame, indicator: str):
        """保存宏观数据"""
        if data is None or data.empty:
            return

        df = data[['date', indicator]].copy()
        df = df.rename(columns={indicator: 'value'})
        df['indicator'] = indicator

        # 删除旧数据后插入新数据
        self.conn.execute(f"DELETE FROM macro_data WHERE indicator = '{indicator}'")
        self.conn.register('macro_df', df)
        self.conn.execute("INSERT INTO macro_data SELECT * FROM macro_df")
        self.conn.unregister('macro_df')
        logger.info(f"保存宏观数据: {indicator}, {len(df)} 条")

    def get_macro(self, indicator: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """获取宏观数据"""
        sql = f"SELECT * FROM macro_data WHERE indicator = '{indicator}'"
        if start_date:
            sql += f" AND date >= '{start_date}'"
        if end_date:
            sql += f" AND date <= '{end_date}'"
        sql += " ORDER BY date"
        return self.conn.execute(sql).df()

    # ============= 行业数据 =============

    def save_industry(self, data: pd.DataFrame, industry: str = None):
        """保存行业数据"""
        if data is None or data.empty:
            return

        df = data.copy()
        if industry and 'industry' not in df.columns:
            df['industry'] = industry

        self.conn.register('industry_df', df)
        self.conn.execute("""
            INSERT OR REPLACE INTO industry_data
            SELECT date, industry, close, volume, change_pct, NOW()
            FROM industry_df
        """)
        self.conn.unregister('industry_df')
        logger.info(f"保存行业数据: {len(df)} 条")

    def get_industry(self, industry: str = None, start_date: str = None) -> pd.DataFrame:
        """获取行业数据"""
        sql = "SELECT * FROM industry_data"
        if industry:
            sql += f" WHERE industry = '{industry}'"
        if start_date:
            sql += f" AND date >= '{start_date}'"
        sql += " ORDER BY date"
        return self.conn.execute(sql).df()

    # ============= 股票数据 =============

    def save_stock_daily(self, data: pd.DataFrame, symbol: str):
        """保存股票日线数据"""
        if data is None or data.empty:
            return

        df = data.copy()
        df['symbol'] = symbol
        df['change_pct'] = df['close'].pct_change() * 100

        self.conn.register('stock_df', df)
        self.conn.execute("""
            INSERT OR REPLACE INTO stock_daily
            SELECT date, symbol, open, high, low, close, volume, amount,
                   turnover_rate, change_pct, NOW()
            FROM stock_df
        """)
        self.conn.unregister('stock_df')
        logger.debug(f"保存股票日线: {symbol}, {len(df)} 条")

    def get_stock_daily(self, symbol: str, start_date: str = None, days: int = 60) -> pd.DataFrame:
        """获取股票日线数据"""
        sql = f"SELECT * FROM stock_daily WHERE symbol = '{symbol}'"
        if start_date:
            sql += f" AND date >= '{start_date}'"
        sql += " ORDER BY date DESC"
        if days:
            sql += f" LIMIT {days}"
        return self.conn.execute(sql).df()

    def save_stock_financial(self, data: pd.DataFrame, symbol: str):
        """保存股票财务数据"""
        if data is None or data.empty:
            return

        df = data.copy()
        df['symbol'] = symbol

        self.conn.register('financial_df', df)
        self.conn.execute("""
            INSERT OR REPLACE INTO stock_financial
            SELECT date, symbol, pe, pb, ps, roe, roa,
                   revenue_growth, profit_growth, NOW()
            FROM financial_df
        """)
        self.conn.unregister('financial_df')
        logger.debug(f"保存财务数据: {symbol}, {len(df)} 条")

    def get_stock_financial(self, symbol: str) -> pd.DataFrame:
        """获取股票财务数据"""
        sql = f"SELECT * FROM stock_financial WHERE symbol = '{symbol}' ORDER BY date"
        return self.conn.execute(sql).df()

    # ============= 因子数据 =============

    def save_factor(self, data: pd.DataFrame, date: str):
        """保存因子数据"""
        if data is None or data.empty:
            return

        df = data.copy()
        df['date'] = date

        self.conn.register('factor_df', df)
        self.conn.execute("""
            INSERT OR REPLACE INTO factor_data
            SELECT date, symbol, factor_name, factor_value, NOW()
            FROM factor_df
        """)
        self.conn.unregister('factor_df')
        logger.info(f"保存因子数据: {date}, {len(df)} 条")

    def get_factor(self, date: str, factor_name: str = None) -> pd.DataFrame:
        """获取因子数据"""
        sql = f"SELECT * FROM factor_data WHERE date = '{date}'"
        if factor_name:
            sql += f" AND factor_name = '{factor_name}'"
        return self.conn.execute(sql).df()

    # ============= 信号数据 =============

    def save_signal(self, data: pd.DataFrame, date: str):
        """保存信号数据"""
        if data is None or data.empty:
            return

        df = data.copy()
        df['date'] = date

        # 删除旧数据
        self.conn.execute(f"DELETE FROM signal_data WHERE date = '{date}'")

        self.conn.register('signal_df', df)
        self.conn.execute("""
            INSERT INTO signal_data
            SELECT date, symbol, signal, signal_level,
                   macro_score, meso_score, micro_score, NOW()
            FROM signal_df
        """)
        self.conn.unregister('signal_df')
        logger.info(f"保存信号数据: {date}, {len(df)} 条")

    def get_signal(self, date: str = None, symbol: str = None) -> pd.DataFrame:
        """获取信号数据"""
        sql = "SELECT * FROM signal_data"
        conditions = []
        if date:
            conditions.append(f"date = '{date}'")
        if symbol:
            conditions.append(f"symbol = '{symbol}'")
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY signal DESC"
        return self.conn.execute(sql).df()

    # ============= 通用方法 =============

    def get_latest_date(self, table: str) -> Optional[str]:
        """获取表中最新的日期"""
        result = self.conn.execute(f"SELECT MAX(date) FROM {table}").fetchone()
        return result[0] if result and result[0] else None

    def execute_sql(self, sql: str) -> pd.DataFrame:
        """执行自定义SQL"""
        return self.conn.execute(sql).df()

    def close(self):
        """关闭连接"""
        self.conn.close()


# 便捷函数
_storage = None

def get_storage() -> DataStorage:
    """获取存储器单例"""
    global _storage
    if _storage is None:
        _storage = DataStorage()
    return _storage
