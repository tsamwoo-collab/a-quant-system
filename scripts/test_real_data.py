"""
实际数据接入测试脚本
测试 AkShare 数据获取功能
"""
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.real_data_loader import RealDataLoader


def test_stock_list():
    """测试获取股票列表"""
    print("=" * 50)
    print("测试 1: 获取股票列表")
    print("=" * 50)

    loader = RealDataLoader()

    # 获取股票列表
    stock_list = loader.get_stock_list(force_update=True)

    print(f"\n✅ 获取到 {len(stock_list)} 只股票")
    print(f"\n前 10 只股票:")
    print(stock_list.head(10).to_string(index=False))

    # 统计各市场股票数量
    print(f"\n市场分布:")
    print(stock_list['market'].value_counts().to_string())

    return stock_list


def test_daily_quotes(symbol: str = "600519"):
    """测试获取日线数据"""
    print("\n" + "=" * 50)
    print(f"测试 2: 获取 {symbol} 的日线数据")
    print("=" * 50)

    loader = RealDataLoader()

    # 获取最近3个月数据
    from datetime import datetime, timedelta
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")

    quotes = loader.get_daily_quotes(symbol, start_date, end_date)

    if not quotes.empty:
        print(f"\n✅ 获取到 {len(quotes)} 条日线数据")
        print(f"\n最新 5 天数据:")
        print(quotes.tail().to_string(index=False))

        # 计算基本统计
        print(f"\n统计信息:")
        print(f"  日期范围: {quotes['date'].min()} 至 {quotes['date'].max()}")
        print(f"  收盘价区间: {quotes['close'].min():.2f} - {quotes['close'].max():.2f}")
        print(f"  平均成交量: {quotes['volume'].mean():.0f}")
    else:
        print(f"\n❌ 未获取到数据")

    return quotes


def test_batch_download():
    """测试批量下载"""
    print("\n" + "=" * 50)
    print("测试 3: 批量下载（测试5只股票）")
    print("=" * 50)

    loader = RealDataLoader()

    # 测试股票列表
    test_symbols = ['600519', '000858', '600036', '000002', '601318']

    from datetime import datetime, timedelta
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")

    stats = loader.batch_download_quotes(
        symbols=test_symbols,
        start_date=start_date,
        end_date=end_date,
        batch_size=5,
        delay=0.5
    )

    print(f"\n✅ 批量下载完成:")
    print(f"  总数: {stats['total']}")
    print(f"  成功: {stats['success']}")
    print(f"  失败: {stats['failed']}")

    if stats['failed_list']:
        print(f"  失败列表: {stats['failed_list']}")

    return stats


def main():
    """主测试函数"""
    print("\n🚀 开始测试实际数据接入功能\n")

    try:
        # 测试1: 股票列表
        stock_list = test_stock_list()

        # 测试2: 单只股票日线
        quotes = test_daily_quotes("600519")

        # 测试3: 批量下载
        stats = test_batch_download()

        print("\n" + "=" * 50)
        print("✅ 所有测试完成！")
        print("=" * 50)

        print("\n💡 提示:")
        print("  - 数据已保存到 DuckDB: data/real_market.duckdb")
        print("  - 可以使用以下命令查看数据库:")
        print("    python3 -c \"import duckdb; conn = duckdb.connect('data/real_market.duckdb'); print(conn.execute('SELECT * FROM daily_quotes LIMIT 10').fetchdf())\"")

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
