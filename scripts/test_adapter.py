"""
数据适配器测试脚本
验证适配器功能是否正常
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.adapters import get_adapter, AdapterConfig, list_adapters


def test_adapter(adapter_name: str = "akshare"):
    """测试适配器功能"""
    print("=" * 60)
    print(f"🧪 测试数据适配器: {adapter_name.upper()}")
    print("=" * 60)

    # 创建适配器
    if adapter_name == "tushare":
        token = input("请输入 Tushare Token: ").strip()
        config = AdapterConfig(
            adapter_type="tushare",
            tushare_config={"token": token}
        )
        adapter = get_adapter(config)
    else:
        adapter = get_adapter()

    print(f"\n📊 数据源信息:")
    print(f"   名称: {adapter.name}")
    print(f"   类型: {'付费' if adapter.is_paid else '免费'}")

    # 健康检查
    print(f"\n🔍 健康检查...")
    health = adapter.health_check()
    print(f"   状态: {health['status']}")
    print(f"   消息: {health['message']}")
    if health.get('latency'):
        print(f"   延迟: {health['latency']:.2f} ms")

    if health['status'] != 'healthy':
        print("\n⚠️ 数据源连接异常，跳过后续测试")
        return

    # 测试1: 获取股票列表
    print(f"\n📋 测试1: 获取股票列表...")
    stock_list = adapter.get_stock_list()
    print(f"   ✅ 获取到 {len(stock_list)} 只股票")
    if not stock_list.empty:
        print(f"   示例: {stock_list.head(3).to_string(index=False)}")

    # 测试2: 获取沪深300成分股
    print(f"\n📈 测试2: 获取沪深300成分股...")
    cs300 = adapter.get_index_constituents("000300")
    print(f"   ✅ 获取到 {len(cs300)} 只成分股")
    if not cs300.empty:
        print(f"   示例: {cs300.head(5).to_string(index=False)}")

    # 测试3: 获取单只股票日线数据
    print(f"\n📊 测试3: 获取单只股票日线数据...")
    test_symbol = "600519"  # 贵州茅台
    quotes = adapter.get_daily_quotes(
        symbol=test_symbol,
        start_date="20240101",
        end_date="20240228"
    )
    print(f"   ✅ {test_symbol} 获取到 {len(quotes)} 条数据")
    if not quotes.empty:
        print(f"   日期范围: {quotes['date'].min()} 至 {quotes['date'].max()}")
        print(f"   最新收盘价: ¥{quotes['close'].iloc[-1]:.2f}")

    # 测试4: 批量获取（小规模测试）
    print(f"\n📦 测试4: 批量获取日线数据 (5只股票)...")
    test_symbols = cs300.head(5)['symbol'].tolist()

    def progress(current, total, symbol):
        percent = current / total * 100
        print(f"   进度: {current}/{total} ({percent:.0f}%) - {symbol}")

    results = adapter.batch_get_daily_quotes(
        symbols=test_symbols,
        start_date="20240101",
        end_date="20240228",
        callback=progress
    )
    print(f"   ✅ 成功获取 {len(results)}/{len(test_symbols)} 只股票")

    # 测试5: 获取宏观数据
    print(f"\n🏛️ 测试5: 获取宏观数据...")
    shibor = adapter.get_macro_shibor(days=10)
    print(f"   ✅ SHIBOR 数据: {len(shibor)} 条")

    north_flow = adapter.get_macro_north_flow(days=10)
    print(f"   ✅ 北向资金数据: {len(north_flow)} 条")

    print("\n" + "=" * 60)
    print("✅ 所有测试完成！")
    print("=" * 60)


def main():
    """主函数"""
    print("\n可用的数据源:")
    adapters = list_adapters()
    for i, (key, info) in enumerate(adapters.items(), 1):
        paid_status = "付费" if info['is_paid'] else "免费"
        print(f"  {i}. {info['name']} - {paid_status}")

    print("\n选择要测试的数据源:")
    print("  1. AkShare (推荐，免费)")
    print("  2. Tushare (需要 token)")

    choice = input("\n请选择 (1-2, 默认1): ").strip() or "1"

    if choice == "2":
        test_adapter("tushare")
    else:
        test_adapter("akshare")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ 用户中断")
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()
