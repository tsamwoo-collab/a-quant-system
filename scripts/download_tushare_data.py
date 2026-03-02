"""
批量下载 Tushare 历史数据到本地数据库 v2.0
支持冷热分离架构：元数据存 DuckDB，行情数据存 Parquet
"""
import sys
sys.path.append('/Users/candyhu/a-quant-system')

from scripts.tushare_downloader import TushareDataDownloader
from datetime import datetime, timedelta
import pandas as pd


def download_full_market():
    """下载全市场历史数据（Parquet 格式）"""
    downloader = TushareDataDownloader()

    # 1. 下载股票列表（热数据 - DuckDB）
    print("\n" + "="*50)
    print("步骤 1: 下载股票列表")
    print("="*50)
    df_stocks = downloader.download_stock_list()
    downloader.save_to_database(df_stocks, "stock_list", storage='duckdb')

    # 获取股票代码列表
    ts_codes = df_stocks['ts_code'].tolist()

    # 2. 下载日线数据（冷数据 - Parquet）
    print("\n" + "="*50)
    print("步骤 2: 下载日线数据（最近2年）")
    print("="*50)

    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=730)).strftime("%Y%m%d")  # 2年

    df_daily = downloader.download_daily_data(ts_codes, start_date, end_date)
    if not df_daily.empty:
        downloader.save_to_database(df_daily, "daily_quotes", storage='parquet')

    # 3. 下载复权因子（优化一）
    print("\n" + "="*50)
    print("步骤 3: 下载复权因子")
    print("="*50)
    df_adj = downloader.download_adj_factor(ts_codes, start_date, end_date)
    if not df_adj.empty:
        downloader.save_to_database(df_adj, "adj_factor", storage='parquet')

    # 4. 下载停牌数据（优化一）
    print("\n" + "="*50)
    print("步骤 4: 下载停牌数据")
    print("="*50)
    df_suspend = downloader.download_suspend_data(start_date, end_date)
    if not df_suspend.empty:
        downloader.save_to_database(df_suspend, "suspend_d", storage='duckdb')

    # 5. 下载每日基础指标（优化二）
    print("\n" + "="*50)
    print("步骤 5: 下载每日基础指标")
    print("="*50)
    df_basic = downloader.download_daily_basic(ts_codes, start_date, end_date)
    if not df_basic.empty:
        downloader.save_to_database(df_basic, "daily_basic", storage='parquet')

    # 6. 下载指数数据
    print("\n" + "="*50)
    print("步骤 6: 下载指数数据")
    print("="*50)
    df_index = downloader.download_index_data(start_date=start_date, end_date=end_date)
    if not df_index.empty:
        downloader.save_to_database(df_index, "index_quotes", storage='parquet')

    # 7. 创建 Parquet 视图
    print("\n" + "="*50)
    print("步骤 7: 创建 Parquet 视图")
    print("="*50)
    downloader.create_parquet_views()

    print("\n" + "="*50)
    print("✅ 数据下载完成！")
    print("="*50)
    print(f"\n数据库位置: {downloader.db_path}")
    print(f"Parquet 位置: {downloader.parquet_path}")
    print("\n数据表:")
    print("  DuckDB (热数据):")
    print("    - stock_list: 股票列表")
    print("    - suspend_d: 停牌记录")
    print("  Parquet (冷数据，按年分区):")
    print("    - daily_quotes: 日线行情")
    print("    - adj_factor: 复权因子")
    print("    - daily_basic: 每日基础指标")
    print("    - index_quotes: 指数行情")


def download_cs300():
    """下载沪深300成分股数据"""
    downloader = TushareDataDownloader()

    # 1. 下载沪深300成分股列表
    print("\n" + "="*50)
    print("步骤 1: 下载沪深300成分股列表")
    print("="*50)

    downloader.init_api()

    # 获取沪深300成分股
    df_index = downloader.pro.index_weight(ts_code='000300.SH', start_date='20250101')
    print(f"✅ 沪深300成分股: {len(df_index)} 只")

    # 保存成分股列表（转换格式）
    df_stocks = pd.DataFrame({'ts_code': df_index['con_code'].unique()})
    downloader.save_to_database(df_stocks, "cs300_stocks")

    # 获取股票代码
    ts_codes = df_stocks['ts_code'].tolist()

    # 2. 下载日线数据
    print("\n" + "="*50)
    print("步骤 2: 下载沪深300成分股日线数据（最近2年）")
    print("="*50)

    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=730)).strftime("%Y%m%d")

    df_daily = downloader.download_daily_data(ts_codes, start_date, end_date)
    if not df_daily.empty:
        downloader.save_to_database(df_daily, "daily_quotes")

    print("\n" + "="*50)
    print("✅ 沪深300数据下载完成！")
    print("="*50)


def download_recent_data():
    """下载沪深300级别数据（2年历史）- Parquet 格式"""
    downloader = TushareDataDownloader()

    # 下载股票列表（热数据）
    df_stocks = downloader.download_stock_list()
    downloader.save_to_database(df_stocks, "stock_list", storage='duckdb')

    # 下载前300只股票（沪深300级别）的最近2年数据
    ts_codes = df_stocks['ts_code'].head(300).tolist()
    print(f"📊 计划下载 {len(ts_codes)} 只股票的2年历史数据")

    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=730)).strftime("%Y%m%d")

    # 日线数据（冷数据 - Parquet）
    df_daily = downloader.download_daily_data(ts_codes, start_date, end_date)
    if not df_daily.empty:
        downloader.save_to_database(df_daily, "daily_quotes", storage='parquet')

    # 复权因子
    df_adj = downloader.download_adj_factor(ts_codes, start_date, end_date)
    if not df_adj.empty:
        downloader.save_to_database(df_adj, "adj_factor", storage='parquet')

    # 每日基础指标
    df_basic = downloader.download_daily_basic(ts_codes, start_date, end_date)
    if not df_basic.empty:
        downloader.save_to_database(df_basic, "daily_basic", storage='parquet')

    # 创建视图
    downloader.create_parquet_views()

    print("\n" + "="*50)
    print("✅ 数据下载完成！")
    print("="*50)
    print(f"\n数据库位置: {downloader.db_path}")
    print(f"Parquet 位置: {downloader.parquet_path}")


def update_database():
    """增量更新数据库（获取最新数据）"""
    downloader = TushareDataDownloader()
    downloader.update_daily()


def download_test_parquet():
    """测试 Parquet 下载功能（小规模测试）"""
    downloader = TushareDataDownloader()

    print("\n" + "="*50)
    print("测试 Parquet 下载功能")
    print("="*50)

    # 下载股票列表
    df_stocks = downloader.download_stock_list()
    downloader.save_to_database(df_stocks, "stock_list", storage='duckdb')

    # 只下载前10只股票进行测试
    ts_codes = df_stocks['ts_code'].head(10).tolist()
    print(f"📊 测试下载 {len(ts_codes)} 只股票")

    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")  # 3个月

    # 日线数据
    df_daily = downloader.download_daily_data(ts_codes, start_date, end_date)
    if not df_daily.empty:
        downloader.save_to_database(df_daily, "daily_quotes", storage='parquet')

    # 复权因子
    df_adj = downloader.download_adj_factor(ts_codes, start_date, end_date)
    if not df_adj.empty:
        downloader.save_to_database(df_adj, "adj_factor", storage='parquet')

    # 每日基础指标
    df_basic = downloader.download_daily_basic(ts_codes, start_date, end_date)
    if not df_basic.empty:
        downloader.save_to_database(df_basic, "daily_basic", storage='parquet')

    # 创建视图
    downloader.create_parquet_views()

    print("\n" + "="*50)
    print("✅ 测试完成！")
    print("="*50)
    print(f"\nParquet 文件位置: {downloader.parquet_path}")

    # 列出生成的文件
    import os
    if os.path.exists(downloader.parquet_path):
        files = [f for f in os.listdir(downloader.parquet_path) if f.endswith('.parquet')]
        print(f"生成的 Parquet 文件:")
        for f in sorted(files):
            file_path = os.path.join(downloader.parquet_path, f)
            size = os.path.getsize(file_path)
            print(f"  - {f} ({size:,} bytes)")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Tushare 数据下载 v2.0')
    parser.add_argument('--mode', choices=['full', 'cs300', 'recent', 'update', 'test'],
                      default='recent', help='下载模式')
    args = parser.parse_args()

    print("="*50)
    print("Tushare 数据下载器 v2.0 (支持 Parquet)")
    print("="*50)
    print(f"模式: {args.mode}")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*50)

    if args.mode == 'full':
        download_full_market()
    elif args.mode == 'cs300':
        download_cs300()
    elif args.mode == 'recent':
        download_recent_data()
    elif args.mode == 'update':
        update_database()
    elif args.mode == 'test':
        download_test_parquet()
