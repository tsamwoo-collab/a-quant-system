"""
Tushare 数据下载器 v2.0 - 本地化数据仓库
支持冷热分离架构:
- 热数据 (元数据/交易): DuckDB 内部表
- 冷数据 (行情数据): Parquet 文件按年分区
"""
import tushare as ts
import pandas as pd
import duckdb
import json
import os
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import time
import re


class TushareDataDownloader:
    """Tushare 数据下载器 v2.0 - 支持 Parquet 存储"""

    # 定义数据类型：热数据(元数据) vs 冷数据(行情)
    HOT_DATA_TABLES = {'stock_list', 'index_list', 'cs300_stocks', 'suspend_d',
                       'daily_signals', 'signal_history', 'positions', 'trade_history',
                       'closed_positions', 'backtest_runs', 'backtest_trades', 'backtest_metrics'}

    COLD_DATA_TABLES = {'daily_quotes', 'index_quotes', 'daily_basic', 'cyq_perf', 'broker_recommend'}

    def __init__(self, config_path: str = "config/tushare_config.json"):
        self.config_path = config_path
        self.token = None
        self.db_path = None
        self.parquet_path = None
        self.pro = None
        self.load_config()

    def load_config(self):
        """加载配置"""
        if os.path.exists(self.config_path):
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                self.token = config.get('token')
                self.db_path = config.get('db_path', 'data/tushare_db.duckdb')
                self.parquet_path = config.get('parquet_path', 'data/market_data')
        else:
            # 默认配置
            self.db_path = 'data/tushare_db.duckdb'
            self.parquet_path = 'data/market_data'

    def save_config(self):
        """保存配置"""
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        config = {
            'token': self.token,
            'db_path': self.db_path,
            'parquet_path': self.parquet_path,
            'proxy_url': 'http://lianghua.nanyangqiankun.top'
        }
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)

    def init_api(self):
        """初始化 Tushare API"""
        if not self.token:
            raise ValueError("Token 未配置，请先设置 Token")

        self.pro = ts.pro_api(self.token)
        # 关键：设置代理服务器
        self.pro._DataApi__token = self.token
        self.pro._DataApi__http_url = 'http://lianghua.nanyangqiankun.top'

        return self.pro

    def download_stock_list(self) -> pd.DataFrame:
        """下载股票列表"""
        print("=== 下载股票列表 ===")
        self.init_api()

        # 下载上市股票
        df_listed = self.pro.stock_basic(
            list_status='L',
            fields='ts_code,symbol,name,area,industry,market,list_date'
        )
        print(f"✅ 上市股票: {len(df_listed)} 只")

        # 下载退市股票（可选）
        # df_delisted = self.pro.stock_basic(
        #     list_status='D',
        #     fields='ts_code,symbol,name,area,industry,market,list_date,delist_date'
        # )
        # print(f"✅ 退市股票: {len(df_delisted)} 只")

        return df_listed

    def download_daily_data(self, ts_codes: List[str], start_date: str, end_date: str = None) -> pd.DataFrame:
        """
        下载日线数据

        Args:
            ts_codes: 股票代码列表
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)，默认为今天
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")

        print(f"=== 下载日线数据 ({start_date} - {end_date}) ===")
        self.init_api()

        all_data = []
        total = len(ts_codes)

        for i, ts_code in enumerate(ts_codes, 1):
            try:
                # 每次最多请求3000条数据（约12年日线）
                df = self.pro.daily(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date
                )

                if not df.empty:
                    all_data.append(df)
                    print(f"  [{i}/{total}] {ts_code}: {len(df)} 条", end="\r")

                # API 限速：每200次请求暂停1秒
                if i % 200 == 0:
                    time.sleep(1)

            except Exception as e:
                print(f"\n  ❌ {ts_code}: {e}")
                continue

        print()  # 换行

        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            print(f"✅ 总计: {len(result)} 条日线数据")
            return result
        else:
            print("❌ 未获取到任何数据")
            return pd.DataFrame()

    def download_index_data(self, index_codes: List[str] = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """下载指数数据"""
        if index_codes is None:
            # 默认主要指数
            index_codes = [
                '000001.SH',  # 上证综指
                '399001.SZ',  # 深证成指
                '000300.SH',  # 沪深300
                '000905.SH',  # 中证500
                '399006.SZ'   # 创业板指
            ]

        if start_date is None:
            start_date = '20200101'
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")

        print(f"=== 下载指数数据 ===")
        self.init_api()

        all_data = []
        for ts_code in index_codes:
            try:
                df = self.pro.index_daily(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date
                )
                if not df.empty:
                    all_data.append(df)
                    print(f"  ✅ {ts_code}: {len(df)} 条")
            except Exception as e:
                print(f"  ❌ {ts_code}: {e}")

        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            print(f"✅ 指数数据: {len(result)} 条")
            return result
        else:
            return pd.DataFrame()

    def download_adj_factor(self, ts_codes: List[str], start_date: str, end_date: str = None) -> pd.DataFrame:
        """
        下载复权因子数据

        Args:
            ts_codes: 股票代码列表
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")

        print(f"=== 下载复权因子 ({start_date} - {end_date}) ===")
        self.init_api()

        all_data = []
        total = len(ts_codes)

        for i, ts_code in enumerate(ts_codes, 1):
            try:
                df = self.pro.adj_factor(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date
                )
                if not df.empty:
                    all_data.append(df)
                    print(f"  [{i}/{total}] {ts_code}: {len(df)} 条", end="\r")

                if i % 200 == 0:
                    time.sleep(1)

            except Exception as e:
                print(f"\n  ❌ {ts_code}: {e}")
                continue

        print()

        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            print(f"✅ 复权因子: {len(result)} 条")
            return result
        else:
            return pd.DataFrame()

    def download_suspend_data(self, start_date: str, end_date: str = None) -> pd.DataFrame:
        """
        下载停牌数据

        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")

        print(f"=== 下载停牌数据 ({start_date} - {end_date}) ===")
        self.init_api()

        try:
            df = self.pro.suspend(
                start_date=start_date,
                end_date=end_date
            )
            if not df.empty:
                print(f"✅ 停牌数据: {len(df)} 条")
                return df
            else:
                print("无停牌数据")
                return pd.DataFrame()
        except Exception as e:
            print(f"❌ 停牌数据下载失败: {e}")
            return pd.DataFrame()

    def download_daily_basic(self, ts_codes: List[str], start_date: str, end_date: str = None) -> pd.DataFrame:
        """
        下载每日基础指标（换手率、市盈率、市净率、市值等）

        Args:
            ts_codes: 股票代码列表
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")

        print(f"=== 下载每日基础指标 ({start_date} - {end_date}) ===")
        self.init_api()

        all_data = []
        total = len(ts_codes)

        for i, ts_code in enumerate(ts_codes, 1):
            try:
                df = self.pro.daily_basic(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    fields='ts_code,trade_date,turnover_rate,pe_ttm,pb,total_mv'
                )
                if not df.empty:
                    all_data.append(df)
                    print(f"  [{i}/{total}] {ts_code}: {len(df)} 条", end="\r")

                if i % 200 == 0:
                    time.sleep(1)

            except Exception as e:
                print(f"\n  ❌ {ts_code}: {e}")
                continue

        print()

        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            print(f"✅ 每日基础指标: {len(result)} 条")
            return result
        else:
            return pd.DataFrame()

    def _extract_year_from_date(self, date_str: str) -> str:
        """从日期字符串中提取年份"""
        if isinstance(date_str, str):
            if len(date_str) == 8:  # YYYYMMDD
                return date_str[:4]
            elif len(date_str) == 10:  # YYYY-MM-DD
                return date_str[:4]
        return str(datetime.now().year)

    def save_to_parquet(self, df: pd.DataFrame, table_name: str):
        """
        保存数据到 Parquet 文件（按年份分区）

        Args:
            df: 要保存的数据
            table_name: 表名（用于生成文件名）
        """
        if df.empty:
            print("⚠️ 数据为空，跳过保存")
            return

        os.makedirs(self.parquet_path, exist_ok=True)

        # 确定日期列名
        date_col = None
        for col in ['trade_date', 'date', 'ts_date']:
            if col in df.columns:
                date_col = col
                break

        if date_col is None:
            print(f"⚠️ 未找到日期列，保存为单一文件")
            file_path = os.path.join(self.parquet_path, f"{table_name}.parquet")
            df.to_parquet(file_path, index=False, compression='snappy')
            print(f"✅ 已保存到 Parquet: {file_path} ({len(df)} 条)")
            return

        # 按年份分区保存
        df['year'] = df[date_col].apply(lambda x: self._extract_year_from_date(str(x)))
        years = df['year'].unique()

        total_saved = 0
        for year in sorted(years):
            year_df = df[df['year'] == year].drop(columns=['year'])
            file_path = os.path.join(self.parquet_path, f"{table_name}_{year}.parquet")

            # 检查文件是否存在，存在则追加
            if os.path.exists(file_path):
                existing_df = pd.read_parquet(file_path)
                merged_df = pd.concat([existing_df, year_df], ignore_index=True)
                # 去重
                if date_col in merged_df.columns and 'ts_code' in merged_df.columns:
                    merged_df = merged_df.drop_duplicates(subset=[date_col, 'ts_code'], keep='last')
                merged_df.to_parquet(file_path, index=False, compression='snappy')
            else:
                year_df.to_parquet(file_path, index=False, compression='snappy')

            count = len(year_df)
            total_saved += count
            print(f"  ✅ {year}: {file_path} ({count} 条)")

        print(f"✅ Parquet 保存完成: {len(df)} 条 -> {len(years)} 个文件")

    def save_to_database(self, df: pd.DataFrame, table_name: str, storage: str = 'auto'):
        """
        保存数据（自动选择存储方式）

        Args:
            df: 要保存的数据
            table_name: 表名
            storage: 存储方式 'auto'(自动) | 'duckdb'(热数据) | 'parquet'(冷数据)
        """
        if df.empty:
            print("⚠️ 数据为空，跳过保存")
            return

        # 自动判断存储方式
        if storage == 'auto':
            if table_name in self.HOT_DATA_TABLES:
                storage = 'duckdb'
            elif table_name in self.COLD_DATA_TABLES:
                storage = 'parquet'
            else:
                # 默认使用 DuckDB
                storage = 'duckdb'

        if storage == 'parquet':
            self.save_to_parquet(df, table_name)
        else:
            # 保存到 DuckDB
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            conn = duckdb.connect(self.db_path)

            try:
                # 删除已存在的表
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")

                # 创建表并插入数据
                conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")

                row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                print(f"✅ 已保存到 DuckDB 表 {table_name}: {row_count} 条记录")

            except Exception as e:
                print(f"❌ 保存失败: {e}")
            finally:
                conn.close()

    def create_parquet_views(self):
        """创建 DuckDB 视图，用于访问外部 Parquet 文件"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        conn = duckdb.connect(self.db_path)

        try:
            # 获取绝对路径
            abs_parquet_path = os.path.abspath(self.parquet_path)

            # 创建日线行情视图
            parquet_files = []
            if os.path.exists(abs_parquet_path):
                for file in os.listdir(abs_parquet_path):
                    if file.startswith('daily_quotes_') and file.endswith('.parquet'):
                        parquet_files.append(os.path.join(abs_parquet_path, file))

            if parquet_files:
                # 删除旧视图
                conn.execute("DROP VIEW IF EXISTS v_daily_quotes")

                # 创建新视图（联合所有年份的 Parquet 文件）
                union_sql = " UNION ALL ".join([
                    f"SELECT * FROM '{file}'"
                    for file in sorted(parquet_files)
                ])
                conn.execute(f"CREATE VIEW v_daily_quotes AS {union_sql}")
                print(f"✅ 创建视图 v_daily_quotes (关联 {len(parquet_files)} 个 Parquet 文件)")

            # 创建指数行情视图
            index_files = []
            if os.path.exists(abs_parquet_path):
                for file in os.listdir(abs_parquet_path):
                    if file.startswith('index_quotes_') and file.endswith('.parquet'):
                        index_files.append(os.path.join(abs_parquet_path, file))

            if index_files:
                conn.execute("DROP VIEW IF EXISTS v_index_quotes")
                union_sql = " UNION ALL ".join([
                    f"SELECT * FROM '{file}'"
                    for file in sorted(index_files)
                ])
                conn.execute(f"CREATE VIEW v_index_quotes AS {union_sql}")
                print(f"✅ 创建视图 v_index_quotes (关联 {len(index_files)} 个 Parquet 文件)")

            # 创建每日基础指标视图
            basic_files = []
            if os.path.exists(abs_parquet_path):
                for file in os.listdir(abs_parquet_path):
                    if file.startswith('daily_basic_') and file.endswith('.parquet'):
                        basic_files.append(os.path.join(abs_parquet_path, file))

            if basic_files:
                conn.execute("DROP VIEW IF EXISTS v_daily_basic")
                union_sql = " UNION ALL ".join([
                    f"SELECT * FROM '{file}'"
                    for file in sorted(basic_files)
                ])
                conn.execute(f"CREATE VIEW v_daily_basic AS {union_sql}")
                print(f"✅ 创建视图 v_daily_basic (关联 {len(basic_files)} 个 Parquet 文件)")

        except Exception as e:
            print(f"❌ 创建视图失败: {e}")
        finally:
            conn.close()

    def update_daily(self, date: str = None, use_today: bool = True):
        """
        增量更新最新日线数据（支持 Parquet）

        Args:
            date: 指定日期 (YYYYMMDD)，None 则自动选择
            use_today: 是否尝试获取今日数据（默认True）
        """
        if date is None:
            if use_today:
                # 尝试使用今天的日期
                date = datetime.now().strftime("%Y%m%d")
            else:
                # 使用昨天（保底方案）
                date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

        print(f"=== 增量更新 ({date}) ===")

        # 获取本地数据库中的股票列表
        conn = duckdb.connect(self.db_path)
        try:
            stocks = conn.execute("SELECT DISTINCT ts_code FROM stock_list").fetchdf()
            ts_codes = stocks['ts_code'].tolist()
            print(f"本地股票: {len(ts_codes)} 只")
        finally:
            conn.close()

        if ts_codes:
            # 下载最新数据（最近7天）
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
            df_new = self.download_daily_data(ts_codes, start_date)

            if not df_new.empty:
                # 保存到 Parquet（自动追加）
                self.save_to_parquet(df_new, "daily_quotes")

                # 同时更新复权因子和基础指标
                print("\n--- 更新复权因子 ---")
                df_adj = self.download_adj_factor(ts_codes, start_date, date)
                if not df_adj.empty:
                    self.save_to_parquet(df_adj, "adj_factor")

                print("\n--- 更新每日基础指标 ---")
                df_basic = self.download_daily_basic(ts_codes, start_date, date)
                if not df_basic.empty:
                    self.save_to_parquet(df_basic, "daily_basic")

                # 重新创建视图
                self.create_parquet_views()

                print(f"\n✅ 增量更新完成")


def create_config_template():
    """创建配置文件模板"""
    config = {
        "token": "f8ebd3ef93204454d8115ce5ae1b9a3a055b49c809cdb9690b696c26afc0",
        "db_path": "data/tushare_db.duckdb",
        "proxy_url": "http://lianghua.nanyangqiankun.top",
        "_instructions": {
            "token": "Tushare Token（已配置代理服务器）",
            "db_path": "本地数据库路径",
            "proxy_url": "代理服务器地址（必须设置）"
        }
    }

    os.makedirs("config", exist_ok=True)
    with open("config/tushare_config.json", 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

    print("✅ 配置文件已创建: config/tushare_config.json")


# 测试代码
if __name__ == "__main__":
    # 创建配置
    create_config_template()

    # 测试连接
    downloader = TushareDataDownloader()

    print("\n=== 测试 Tushare 连接 ===")
    try:
        # 测试股票列表
        df_stocks = downloader.download_stock_list()
        print(df_stocks.head(3))

        # 测试日线数据
        print("\n=== 测试日线数据下载 ===")
        df_daily = downloader.download_daily_data(
            ts_codes=['000001.SZ', '000002.SZ'],
            start_date='20240201',
            end_date='20240205'
        )
        print(df_daily)

        print("\n✅ 测试成功！可以开始批量下载")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
