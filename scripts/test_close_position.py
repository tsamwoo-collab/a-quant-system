"""测试平仓流程"""
import sys
sys.path.append('/Users/candyhu/a-quant-system')

from scripts.feishu_bitable import FeishuBitable

bitable = FeishuBitable()
bitable.get_tenant_access_token()

print("=== 测试平仓流程 ===\n")

# 1. 先创建一个持仓（模拟）
print("1. 创建测试持仓...")
bitable.update_portfolio(
    symbol="600519.SH",
    entry_date="2026-02-20",
    entry_price=1800.00,
    current_price=1830.00,
    highest_price=1850.00,
    pnl_pct=0.0167,
    drawdown_pct=-0.0108,
    risk_status="正常"
)

# 2. 模拟平仓信号
print("\n2. 模拟平仓（追踪止盈）...")
close_signal = {
    "symbol": "600519.SH",
    "reason": "追踪止盈",
    "pnl_pct": 0.0167,
    "current_price": 1830.00
}

# 获取持仓详情
position_details = bitable._get_portfolio_position_details("600519.SH")
if position_details:
    print(f"持仓详情: {position_details}")

    # 记录到表D
    from datetime import datetime
    bitable.record_closed_position(
        symbol="600519.SH",
        entry_date=position_details["entry_date"],
        exit_date="2026-03-02",
        entry_price=position_details["entry_price"],
        exit_price=position_details["current_price"],
        holding_days=10,
        pnl_pct=close_signal["pnl_pct"],
        pnl_amount=(position_details["current_price"] - position_details["entry_price"]) * 100,
        reason=close_signal["reason"],
        note="测试平仓"
    )

    # 从表C删除
    bitable.close_portfolio_position("600519.SH")

print("\n✅ 平仓流程测试完成！")
print("\n请检查飞书多维表格：")
print("- 表C（持仓看板）- 应该没有 600519.SH")
print("- 表D（已平仓记录）- 应该有一条平仓记录")
