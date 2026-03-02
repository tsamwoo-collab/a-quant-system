"""测试3表写入"""
import sys
sys.path.append('/Users/candyhu/a-quant-system')

from scripts.feishu_bitable import FeishuBitable

bitable = FeishuBitable()
bitable.get_tenant_access_token()  # 刷新 token

print("=== 测试3表写入 ===\n")

# 1. 写入市场环境
print("1. 写入市场环境...")
bitable.write_market_context(
    date="2026-03-02",
    index_close=3520.50,
    adx_value=28.5,
    system_status="开启(ADX>25)",
    note="测试数据"
)

# 2. 写入买入信号
print("\n2. 写入买入信号...")
bitable.write_signal(
    date="2026-03-02",
    symbol="600519.SH",
    signal_type="买入信号",
    theoretical_price=1820.00,
    momentum_score=0.65,
    execution_status="待执行"
)

# 3. 写入卖出信号
print("\n3. 写入卖出信号...")
bitable.write_signal(
    date="2026-03-02",
    symbol="000858.SZ",
    signal_type="卖出信号",
    theoretical_price=158.00,
    momentum_score=-0.35,
    execution_status="待执行"
)

# 4. 更新持仓
print("\n4. 更新持仓...")
bitable.update_portfolio(
    symbol="600519.SH",
    entry_date="2026-03-01",
    entry_price=1800.00,
    current_price=1820.00,
    highest_price=1830.00,
    pnl_pct=0.0111,
    drawdown_pct=-0.0055,
    risk_status="正常"
)

print("\n✅ 所有测试数据写入完成！")
print("\n请检查飞书多维表格中的3个数据表：")
print("- 每日天气（Market Context）")
print("- 信号追踪（Signal Tracker）")
print("- 持仓看板（Portfolio）")
