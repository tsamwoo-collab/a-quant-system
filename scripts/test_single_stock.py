"""
实际数据接入测试脚本 - 简化版
测试单只股票数据获取
"""
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.real_data_loader import RealDataLoader
from datetime import datetime, timedelta


def test_single_stock():
    """测试单只股票数据获取"""
    print("=" * 60)
    print("🔍 测试单只股票数据获取（带重试机制）")
    print("=" * 60)

    loader = RealDataLoader(max_retries=3, retry_delay=2.0)

    # 测试几只不同市场的股票
    test_stocks = [
        ('600519', '贵州茅台', 'SH'),
        ('000858', '五粮液', 'SZ'),
        ('300750', '宁德时代', 'SZ'),
    ]

    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")

    results = []

    for symbol, name, market in test_stocks:
        print(f"\n{'=' * 60}")
        print(f"📊 获取 {symbol} - {name} ({market})")
        print(f"   时间范围: {start_date} 至 {end_date}")
        print('=' * 60)

        try:
            quotes = loader.get_daily_quotes(symbol, start_date, end_date)

            if not quotes.empty:
                print(f"\n✅ 成功获取 {len(quotes)} 条数据")
                print(f"\n最新数据:")
                print(quotes.tail(3).to_string(index=False))

                # 统计信息
                print(f"\n统计信息:")
                print(f"  日期范围: {quotes['date'].min().strftime('%Y-%m-%d')} 至 {quotes['date'].max().strftime('%Y-%m-%d')}")
                print(f"  收盘价区间: {quotes['close'].min():.2f} - {quotes['close'].max():.2f}")
                print(f"  平均成交量: {quotes['volume'].mean():.0f}")

                results.append({
                    'symbol': symbol,
                    'name': name,
                    'success': True,
                    'count': len(quotes)
                })
            else:
                print(f"\n❌ 未获取到数据")
                results.append({
                    'symbol': symbol,
                    'name': name,
                    'success': False,
                    'count': 0
                })

        except Exception as e:
            print(f"\n❌ 异常: {e}")
            results.append({
                'symbol': symbol,
                'name': name,
                'success': False,
                'count': 0
            })

    # 汇总结果
    print("\n" + "=" * 60)
    print("📊 测试结果汇总")
    print("=" * 60)

    for r in results:
        status = "✅" if r['success'] else "❌"
        count = f"{r['count']} 条" if r['success'] else "失败"
        print(f"{status} {r['symbol']} - {r['name']}: {count}")

    success_count = sum(1 for r in results if r['success'])
    print(f"\n总计: {success_count}/{len(results)} 成功")

    return results


def test_database():
    """测试数据库查询"""
    print("\n" + "=" * 60)
    print("💾 测试数据库查询")
    print("=" * 60)

    import duckdb
    conn = duckdb.connect('data/real_market.duckdb')

    # 查询股票列表
    stock_count = conn.execute("SELECT COUNT(*) FROM stock_list").fetchone()[0]
    print(f"\n📋 股票列表: {stock_count} 只")

    # 查询日线数据
    quotes_count = conn.execute("SELECT COUNT(*) FROM daily_quotes").fetchone()[0]
    print(f"📊 日线数据: {quotes_count} 条")

    if quotes_count > 0:
        # 查询最新数据
        latest = conn.execute("""
            SELECT symbol, date, close
            FROM daily_quotes
            ORDER BY date DESC, symbol
            LIMIT 10
        """).fetchdf()

        print(f"\n最新 10 条记录:")
        print(latest.to_string(index=False))

    conn.close()


def main():
    """主测试函数"""
    print("\n🚀 实际数据接入测试（带网络重试）\n")

    try:
        # 测试单只股票
        results = test_single_stock()

        # 测试数据库
        test_database()

        # 建议
        print("\n" + "=" * 60)
        print("💡 下一步建议")
        print("=" * 60)

        success_count = sum(1 for r in results if r['success'])

        if success_count == len(results):
            print("\n✅ 所有测试通过！可以开始批量下载:")
            print("   python3 scripts/batch_download.py")
        elif success_count > 0:
            print("\n⚠️ 部分测试通过，网络可能不稳定")
            print("   建议: 稍后重试或检查网络连接")
        else:
            print("\n❌ 所有测试失败，可能原因:")
            print("   1. 网络连接问题")
            print("   2. AkShare 服务暂时不可用")
            print("   3. 需要代理才能访问")

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
