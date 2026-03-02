"""快速测试：下载最近数据"""
import sys
sys.path.append('/Users/candyhu/a-quant-system')

from scripts.tushare_downloader import TushareDataDownloader
from datetime import datetime, timedelta


def quick_test():
    """快速测试：下载前100只股票的最近3个月数据"""
    downloader = TushareDataDownloader()

    print("="*50)
    print("Tushare 数据快速测试")
    print("="*50)
    print("模式: 前100只股票，最近3个月")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*50)

    # 1. 下载股票列表
    print("\n步骤 1: 下载股票列表...")
    df_stocks = downloader.download_stock_list()
    downloader.save_to_database(df_stocks, "stock_list")

    # 只取前100只
    ts_codes = df_stocks['ts_code'].head(100).tolist()
    print(f"\n选取前 {len(ts_codes)} 只股票进行测试")

    # 2. 下载最近3个月数据
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")

    print(f"\n步骤 2: 下载日线数据 ({start_date} - {end_date})...")
    df_daily = downloader.download_daily_data(ts_codes, start_date, end_date)

    if not df_daily.empty:
        downloader.save_to_database(df_daily, "daily_quotes")

    # 3. 验证数据
    print("\n" + "="*50)
    print("数据验证")
    print("="*50)

    import duckdb
    conn = duckdb.connect(downloader.db_path)

    # 检查各表记录数
    tables = ['stock_list', 'daily_quotes']
    for table in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count:,} 条记录")
        except:
            print(f"  {table}: 未创建")

    # 显示数据示例
    print("\n数据示例 (daily_quotes):")
    result = conn.execute("""
        SELECT ts_code, trade_date, close, vol
        FROM daily_quotes
        ORDER BY trade_date DESC
        LIMIT 5
    """).fetchdf()
    print(result)

    conn.close()

    print("\n" + "="*50)
    print("✅ 测试完成！")
    print("="*50)
    print(f"\n数据库: {downloader.db_path}")
    print("\n下一步:")
    print("1. 下载全市场数据: python3 scripts/download_tushare_data.py --mode full")
    print("2. 下载沪深300: python3 scripts/download_tushare_data.py --mode cs300")
    print("3. 每日增量更新: python3 scripts/download_tushare_data.py --mode update")


if __name__ == "__main__":
    quick_test()
