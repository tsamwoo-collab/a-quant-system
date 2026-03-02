"""为3个表创建字段并配置系统"""
import json
import requests

# 配置
app_id = "cli_a92e4f3c293cdbc8"
app_secret = "WeBHtMOrX7j5u3bnQ4cmnbH0HEg2Nl4y"
app_token = "BJS4bdRWbaN0wZsY1kyco7aInhc"

# 3个表的 table_id
table_ids = {
    "market_context": "tbleCqTb2JiGGHKw",  # 每日天气
    "signal_tracker": "tblNYQLdtUEEGXez",  # 信号追踪
    "portfolio": "tblSMHGLYLc5ILwS"        # 持仓看板
}

# 获取 token
token_url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
token_response = requests.post(token_url, json={
    "app_id": app_id,
    "app_secret": app_secret
}, timeout=10)
token_result = token_response.json()
tenant_access_token = token_result.get("tenant_access_token")

headers = {
    "Authorization": f"Bearer {tenant_access_token}",
    "Content-Type": "application/json"
}

# 定义每个表需要的字段
table_fields = {
    "market_context": [
        {"field_name": "日期", "type": 5},
        {"field_name": "指数收盘", "type": 2},
        {"field_name": "ADX数值", "type": 2},
        {"field_name": "系统状态", "type": 3, "property": {"options": [{"name": "开启(ADX>25)"}, {"name": "静默(ADX<25)"}, {"name": "观望"}]}},
        {"field_name": "备注", "type": 1}
    ],
    "signal_tracker": [
        {"field_name": "信号日期", "type": 5},
        {"field_name": "股票代码", "type": 1},
        {"field_name": "信号类型", "type": 3, "property": {"options": [{"name": "买入信号"}, {"name": "卖出信号"}]}},
        {"field_name": "理论价格", "type": 2},
        {"field_name": "滑点模拟成交价", "type": 2},
        {"field_name": "动能得分", "type": 2},
        {"field_name": "执行状态", "type": 3, "property": {"options": [{"name": "待执行"}, {"name": "已同步至持仓"}, {"name": "已从持仓平盘"}]}}
    ],
    "portfolio": [
        {"field_name": "代码/名称", "type": 1},
        {"field_name": "入场日期", "type": 5},
        {"field_name": "初始成本", "type": 2},
        {"field_name": "当前价", "type": 2},
        {"field_name": "持有最高收盘价", "type": 2},
        {"field_name": "当前利润率", "type": 2},
        {"field_name": "最高点回撤", "type": 2},
        {"field_name": "风险状态", "type": 3, "property": {"options": [{"name": "正常"}, {"name": "预警"}, {"name": "强制平仓"}]}}
    ]
}

table_names = {
    "market_context": "每日天气",
    "signal_tracker": "信号追踪",
    "portfolio": "持仓看板"
}

print("开始创建字段...\n")

total_success = 0
total_skip = 0
total_fail = 0

for table_key, table_id in table_ids.items():
    table_name = table_names[table_key]
    print(f"=== {table_name} ===")

    field_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"

    for field_config in table_fields[table_key]:
        field_name = field_config["field_name"]
        print(f"  创建字段: {field_name}", end=" ")

        response = requests.post(field_url, headers=headers, json=field_config, timeout=10)
        result = response.json()

        if result.get("code") == 0:
            print("✅")
            total_success += 1
        elif result.get("code") == 99991663:  # 字段已存在
            print("⚠️  已存在")
            total_skip += 1
        else:
            print(f"❌ {result.get('msg')}")
            total_fail += 1

    print()

print("="*50)
print(f"创建完成: 成功 {total_success}, 跳过 {total_skip}, 失败 {total_fail}")

# 更新配置文件
config = {
    "app_id": app_id,
    "app_secret": app_secret,
    "app_token": app_token,
    "table_ids": table_ids
}

with open("config/feishu_bitable.json", "w", encoding="utf-8") as f:
    json.dump(config, f, ensure_ascii=False, indent=2)

print("\n✅ 配置文件已更新")
print("\n3个数据表及其字段已创建完成！")
print("- 每日天气 (Market Context)")
print("- 信号追踪 (Signal Tracker)")
print("- 持仓看板 (Portfolio)")
