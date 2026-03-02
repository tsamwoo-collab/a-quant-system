"""
备用数据源测试
尝试使用不同的接口获取数据
"""
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta


def test_akshare_interfaces():
    """测试不同的 AkShare 接口"""
    print("=" * 60)
    print("🔍 测试 AkShare 备用接口")
    print("=" * 60)

    # 测试1: stock_zh_a_hist 接口（原接口）
    print("\n测试 1: stock_zh_a_hist")
    print("-" * 40)
    try:
        data = ak.stock_zh_a_hist(
            symbol="sh600519",
            period="daily",
            start_date="20260201",
            end_date="20260228",
            adjust=""
        )
        print(f"✅ 成功！获取 {len(data)} 条数据")
        print(data.head(3))
        return True
    except Exception as e:
        print(f"❌ 失败: {e}")

    # 测试2: stock_zh_a_daily 接口
    print("\n测试 2: stock_zh_a_daily（备用接口）")
    print("-" * 40)
    try:
        data = ak.stock_zh_a_daily(
            symbol="sh600519",
            start_date="20260201",
            end_date="20260228",
            adjust="qfq"
        )
        print(f"✅ 成功！获取 {len(data)} 条数据")
        print(data.head(3))
        return True
    except Exception as e:
        print(f"❌ 失败: {e}")

    # 测试3: stock_individual_info_em 接口（东方财富）
    print("\n测试 3: stock_individual_info_em（东方财富）")
    print("-" * 40)
    try:
        data = ak.stock_individual_info_em(symbol="600519")
        print(f"✅ 成功！获取个股信息")
        print(data.head())
        return True
    except Exception as e:
        print(f"❌ 失败: {e}")

    # 测试4: spot 接口（实时行情）
    print("\n测试 4: spot（实时行情）")
    print("-" * 40)
    try:
        data = ak.stock_zh_a_spot_em()
        print(f"✅ 成功！获取 {len(data)} 只股票实时行情")
        # 查找贵州茅台
        maotai = data[data['代码'] == '600519']
        if not maotai.empty:
            print(f"贵州茅台最新行情: {maotai.iloc[0]['最新价']}")
        return True
    except Exception as e:
        print(f"❌ 失败: {e}")

    return False


def test_simple_fetch():
    """简单测试 - 只获取指数数据"""
    print("\n" + "=" * 60)
    print("🔍 测试指数数据（通常更稳定）")
    print("=" * 60)

    try:
        # 获取上证指数
        data = ak.stock_zh_index_daily(symbol="sh000001")
        print(f"✅ 成功获取上证指数数据: {len(data)} 条")
        print("\n最新数据:")
        print(data.tail(3))
        return True
    except Exception as e:
        print(f"❌ 失败: {e}")
        return False


def main():
    """主测试函数"""
    print("\n🚀 测试 AkShare 备用数据源\n")

    # 测试不同接口
    success = test_akshare_interfaces()

    # 测试指数数据
    index_success = test_simple_fetch()

    print("\n" + "=" * 60)
    print("📊 测试结果")
    print("=" * 60)

    if success:
        print("\n✅ 找到可用的接口！可以使用该接口获取数据")
    elif index_success:
        print("\n⚠️ 股票接口不可用，但指数接口可用")
        print("   建议: 先用指数数据验证框架")
    else:
        print("\n❌ 所有接口均不可用")
        print("\n可能原因:")
        print("  1. 网络防火墙/代理问题")
        print("  2. AkShare 服务器维护")
        print("  3. 需要科学上网")
        print("\n建议:")
        print("  - 稍后重试")
        print("  - 检查网络连接")
        print("  - 考虑使用其他数据源（如 Tushare）")


if __name__ == "__main__":
    main()
