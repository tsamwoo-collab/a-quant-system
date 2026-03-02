"""
批量下载股票日线数据
支持全市场股票数据下载
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.real_data_loader import RealDataLoader
from datetime import datetime, timedelta


def batch_download():
    """批量下载股票数据"""
    print("=" * 60)
    print("🚀 批量下载股票日线数据")
    print("=" * 60)

    loader = RealDataLoader(max_retries=3, retry_delay=1.0)

    # 获取股票列表
    print("\n📋 获取股票列表...")
    stock_list = loader.get_stock_list()
    print(f"✅ 共 {len(stock_list)} 只股票")

    # 询问下载范围
    print("\n选择下载范围:")
    print("  1. 测试下载 (10只)")
    print("  2. 小规模下载 (100只)")
    print("  3. 中等规模 (500只)")
    print("  4. 全市场 (5000+只)")

    choice = input("\n请选择 (1-4): ").strip()

    ranges = {
        '1': 10,
        '2': 100,
        '3': 500,
        '4': len(stock_list)
    }

    n_stocks = ranges.get(choice, 10)
    symbols = stock_list['symbol'].head(n_stocks).tolist()

    # 时间范围
    days = input("\n下载数据天数 (默认90天): ").strip()
    try:
        n_days = int(days) if days else 90
    except:
        n_days = 90

    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=n_days)).strftime("%Y%m%d")

    print(f"\n配置:")
    print(f"  股票数量: {n_stocks}")
    print(f"  时间范围: {start_date} 至 {end_date}")
    print(f"  预计时间: {n_stocks * 2} 秒")

    confirm = input("\n确认下载? (y/n): ").strip().lower()
    if confirm != 'y':
        print("已取消")
        return

    # 开始下载
    print("\n开始下载...")

    stats = loader.batch_download_quotes(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        batch_size=10,
        delay=0.5
    )

    # 显示结果
    print("\n" + "=" * 60)
    print("📊 下载完成")
    print("=" * 60)
    print(f"  总数: {stats['total']}")
    print(f"  成功: {stats['success']}")
    print(f"  失败: {stats['failed']}")

    if stats['failed_list']:
        print(f"\n失败列表 (共{len(stats['failed_list'])}只):")
        for s in stats['failed_list'][:20]:
            print(f"  - {s}")
        if len(stats['failed_list']) > 20:
            print(f"  ... 还有 {len(stats['failed_list']) - 20} 只")

    # 数据库统计
    import duckdb
    conn = duckdb.connect('data/real_market.duckdb')

    quotes_count = conn.execute("SELECT COUNT(*) FROM daily_quotes").fetchone()[0]
    symbol_count = conn.execute("SELECT COUNT(DISTINCT symbol) FROM daily_quotes").fetchone()[0]

    print(f"\n数据库统计:")
    print(f"  总记录数: {quotes_count}")
    print(f"  股票数量: {symbol_count}")

    conn.close()

    print("\n✅ 下载数据已保存到: data/real_market.duckdb")


if __name__ == "__main__":
    try:
        batch_download()
    except KeyboardInterrupt:
        print("\n\n⚠️ 用户中断")
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()
