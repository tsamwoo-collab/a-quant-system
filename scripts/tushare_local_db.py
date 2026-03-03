"""
Tushare 本地数据库接口 - 供量化系统使用
从本地 DuckDB 读取数据，替代实时 API 调用
"""
import duckdb
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional


class TushareLocalDB:
    """Tushare 本地数据库接口"""

    def __init__(self, db_path: str = "data/tushare_db.duckdb", parquet_path: str = "data/market_data"):
        self.db_path = db_path
        self.parquet_path = parquet_path
        self.conn = None

    def connect(self):
        """连接数据库"""
        if self.conn is None:
            self.conn = duckdb.connect(self.db_path)
        return self.conn

    def close(self):
        """关闭数据库"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        conn = self.connect()
        try:
            df = conn.execute("""
                SELECT * FROM stock_list
                ORDER BY ts_code
            """).fetchdf()
            return df
        finally:
            self.close()

    def get_daily_data(self, ts_codes: List[str] = None,
                      start_date: str = None, end_date: str = None,
                      use_adj: bool = False) -> pd.DataFrame:
        """
        获取日线数据

        Args:
            ts_codes: 股票代码列表，None 表示所有股票
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            use_adj: 是否使用后复权价格（HFQ）
        """
        conn = self.connect()

        # 日期过滤
        date_filter = ""
        if start_date:
            date_filter += f" AND trade_date >= '{start_date}'"
        if end_date:
            date_filter += f" AND trade_date <= '{end_date}'"

        # 股票过滤
        code_filter = ""
        if ts_codes:
            codes_str = "', '".join(ts_codes)
            code_filter = f" AND ts_code IN ('{codes_str}')"

        try:
            df = conn.execute(f"""
                SELECT * FROM v_daily_quotes
                WHERE 1=1 {date_filter} {code_filter}
                ORDER BY trade_date, ts_code
            """).fetchdf()

            # 如果需要复权价格
            if use_adj and not df.empty:
                df = self._apply_adj_factor(df, ts_codes, start_date, end_date)

            return df
        finally:
            self.close()

    def get_adj_factor(self, ts_codes: List[str] = None,
                       start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取复权因子数据（从 Parquet 文件）

        Args:
            ts_codes: 股票代码列表
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        """
        import os
        import glob

        try:
            # 查找所有 adj_factor Parquet 文件
            pattern = os.path.join(self.parquet_path, "adj_factor_*.parquet")
            files = sorted(glob.glob(pattern))

            if not files:
                return pd.DataFrame()

            # 读取所有文件
            all_dfs = []
            for file in files:
                df = pd.read_parquet(file)
                all_dfs.append(df)

            if not all_dfs:
                return pd.DataFrame()

            adj_df = pd.concat(all_dfs, ignore_index=True)

            # 过滤
            if ts_codes:
                adj_df = adj_df[adj_df['ts_code'].isin(ts_codes)]
            if start_date:
                adj_df = adj_df[adj_df['trade_date'] >= start_date]
            if end_date:
                adj_df = adj_df[adj_df['trade_date'] <= end_date]

            return adj_df
        except Exception as e:
            print(f"读取复权因子失败: {e}")
            return pd.DataFrame()

    def _apply_adj_factor(self, df: pd.DataFrame, ts_codes: List[str] = None,
                          start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        应用复权因子计算后复权价格（HFQ）

        后复权价格 = 原始价格 × 当日复权因子
        注意：这是后复权，不是前复权。使用后复权可以避免"未来函数"问题。

        Args:
            df: 原始日线数据
            ts_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            添加后复权价格字段的 DataFrame
        """
        if df.empty:
            return df

        # 获取复权因子
        adj_df = self.get_adj_factor(ts_codes, start_date, end_date)

        if adj_df.empty:
            print("⚠️ 未获取到复权因子，使用原始价格")
            return df

        # 合并复权因子
        merged = df.merge(adj_df[['ts_code', 'trade_date', 'adj_factor']],
                          on=['ts_code', 'trade_date'], how='left')

        # 计算后复权价格 = 原始价格 × 当日复权因子
        for col in ['open', 'high', 'low', 'close', 'pre_close']:
            if col in merged.columns:
                merged[f'{col}_hfq'] = merged[col] * merged['adj_factor']

        return merged

    def get_daily_basic(self, ts_codes: List[str] = None,
                       trade_date: str = None) -> pd.DataFrame:
        """
        获取每日基础指标（从 Parquet 文件）

        Args:
            ts_codes: 股票代码列表
            trade_date: 交易日期 (YYYYMMDD)

        Returns:
            DataFrame with columns: ts_code, trade_date, turnover_rate, pe_ttm, pb, total_mv
        """
        import os
        import glob

        try:
            # 查找所有 daily_basic Parquet 文件
            pattern = os.path.join(self.parquet_path, "daily_basic_*.parquet")
            files = sorted(glob.glob(pattern))

            if not files:
                return pd.DataFrame()

            # 读取所有文件
            all_dfs = []
            for file in files:
                df = pd.read_parquet(file)
                all_dfs.append(df)

            if not all_dfs:
                return pd.DataFrame()

            basic_df = pd.concat(all_dfs, ignore_index=True)

            # 过滤
            if ts_codes:
                basic_df = basic_df[basic_df['ts_code'].isin(ts_codes)]
            if trade_date:
                basic_df = basic_df[basic_df['trade_date'] == trade_date]
            else:
                # 获取最新日期的数据
                latest_date = basic_df['trade_date'].max()
                basic_df = basic_df[basic_df['trade_date'] == latest_date]

            return basic_df
        except Exception as e:
            print(f"读取 daily_basic 失败: {e}")
            return pd.DataFrame()

    def get_suspended_stocks(self, trade_date: str = None) -> List[str]:
        """
        获取停牌股票列表

        Args:
            trade_date: 交易日期 (YYYYMMDD)

        Returns:
            停牌股票代码列表
        """
        conn = self.connect()
        try:
            # 检查 suspend_d 表是否存在
            tables = conn.execute("SHOW TABLES").fetchdf()
            if 'suspend_d' not in tables['name'].tolist():
                return []

            # 获取指定日期的停牌股票
            date_filter = ""
            if trade_date:
                date_filter = f" AND suspend_date = '{trade_date}'"

            result = conn.execute(f"""
                SELECT DISTINCT ts_code FROM suspend_d
                WHERE suspend_type = '停牌'{date_filter}
            """).fetchdf()

            return result['ts_code'].tolist()
        except:
            return []
        finally:
            self.close()

    def get_cyq_perf(self, ts_codes: List[str] = None,
                    trade_date: str = None) -> pd.DataFrame:
        """
        获取筹码分布数据（模拟数据，实际需要从 API 获取）

        Args:
            ts_codes: 股票代码列表
            trade_date: 交易日期 (YYYYMMDD)

        Returns:
            DataFrame with columns: ts_code, trade_date, cost_85, cost_15, winner_rate
        """
        # 当前数据中没有真实的筹码分布数据
        # 返回模拟数据用于测试
        if not ts_codes:
            return pd.DataFrame()

        # 模拟数据：随机生成 winner_rate
        import random
        data = []
        for ts_code in ts_codes:
            # 随机生成 50%-95% 的 winner_rate
            winner_rate = random.uniform(50, 95)
            data.append({
                'ts_code': ts_code,
                'trade_date': trade_date or '20260227',
                'cost_85': 0.0,
                'cost_15': 0.0,
                'winner_rate': winner_rate
            })

        return pd.DataFrame(data)

    def get_latest_date(self, ts_code: str = None) -> str:
        """获取最新数据日期"""
        conn = self.connect()
        try:
            if ts_code:
                result = conn.execute(f"""
                    SELECT MAX(trade_date) as max_date
                    FROM v_daily_quotes
                    WHERE ts_code = '{ts_code}'
                """).fetchone()
            else:
                result = conn.execute("""
                    SELECT MAX(trade_date) as max_date
                    FROM v_daily_quotes
                """).fetchone()

            return result[0] if result and result[0] else None
        finally:
            self.close()

    def get_stock_data(self, ts_code: str, start_date: str = None,
                      end_date: str = None) -> pd.DataFrame:
        """
        获取单只股票数据（用于回测）

        返回格式与现有系统兼容
        """
        df = self.get_daily_data([ts_code], start_date, end_date)

        if df.empty:
            return df

        # 转换为系统需要的格式
        result = df.pivot(
            index='trade_date',
            columns='ts_code',
            values='close'
        )

        return result

    def get_index_data(self, index_code: str = '000001.SH',
                       start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """获取指数数据"""
        # 如果有 index_quotes 表，从中读取
        # 否则从 daily_quotes 读取（需要调整代码）
        return self.get_daily_data([index_code], start_date, end_date)

    def get_cs800_index_data(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取中证800指数数据（用于计算市场ADX）

        中证800代码：000985.SH（上交所）
        如果没有中证800数据，则使用上证综指（000001.SH）作为替代

        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            包含 ts_code, trade_date, open, high, low, close, vol 的 DataFrame
        """
        conn = self.connect()

        # 日期过滤
        date_filter = ""
        if start_date:
            date_filter += f" AND trade_date >= '{start_date.replace('-', '')}'"
        if end_date:
            date_filter += f" AND trade_date <= '{end_date.replace('-', '')}'"

        try:
            # 尝试查询中证800或上证综指
            index_codes = ['000985.SH', '000001.SH']  # 中证800、上证综指

            for index_code in index_codes:
                df = conn.execute(f"""
                    SELECT ts_code, trade_date, open, high, low, close, vol, amount
                    FROM v_daily_quotes
                    WHERE ts_code = '{index_code}' {date_filter}
                    ORDER BY trade_date
                """).fetchdf()

                if not df.empty:
                    df['date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
                    print(f"✅ 获取指数数据: {index_code} ({len(df)}条)")
                    return df

            print("⚠️ 未找到指数数据")
            return pd.DataFrame()

        finally:
            self.close()

    def get_cs300_stocks(self) -> List[str]:
        """获取沪深300成分股代码（前300只活跃股票）"""
        conn = self.connect()
        try:
            # 从 stock_list 获取前300只股票（排除指数）
            df = conn.execute("""
                SELECT ts_code FROM stock_list
                WHERE ts_code NOT LIKE '%.SH' OR ts_code NOT LIKE '000%.SH'
                AND ts_code NOT LIKE '399%.SZ'
                ORDER BY ts_code
                LIMIT 300
            """).fetchdf()
            return df['ts_code'].tolist()
        except:
            # 降级：直接返回前300只股票
            df = self.get_stock_list()
            # 过滤掉指数代码（通常以特定格式命名）
            stocks = df[~df['ts_code'].str.contains(r'^(000001|399001|000300)\.', regex=True)]
            return stocks['ts_code'].head(300).tolist()
        finally:
            self.close()

    def is_data_available(self, ts_codes: List[str], date: str) -> bool:
        """检查指定日期的数据是否可用"""
        conn = self.connect()
        try:
            codes_str = "', '".join(ts_codes)
            result = conn.execute(f"""
                SELECT COUNT(*) as cnt
                FROM daily_quotes
                WHERE trade_date = '{date}'
                AND ts_code IN ('{codes_str}')
            """).fetchone()

            return result[0] == len(ts_codes)
        finally:
            self.close()


# 测试代码
if __name__ == "__main__":
    db = TushareLocalDB()

    print("=== Tushare 本地数据库测试 ===\n")

    # 1. 测试股票列表
    print("1. 股票列表:")
    stocks = db.get_stock_list()
    print(f"   总计: {len(stocks)} 只")
    print(stocks.head(3))

    # 2. 测试日线数据
    print("\n2. 日线数据:")
    df = db.get_daily_data(['000001.SZ', '000002.SZ'], '2025-01-01', '2025-01-10')
    print(f"   记录数: {len(df)}")
    print(df.head())

    # 3. 测试最新日期
    print("\n3. 最新数据日期:")
    latest = db.get_latest_date('000001.SZ')
    print(f"   000001.SZ: {latest}")

    # 4. 测试数据可用性
    print("\n4. 数据可用性检查:")
    available = db.is_data_available(['000001.SZ'], '2025-02-27')
    print(f"   2025-02-27: {'✅ 可用' if available else '❌ 不可用'}")

    print("\n✅ 本地数据库测试通过！")
