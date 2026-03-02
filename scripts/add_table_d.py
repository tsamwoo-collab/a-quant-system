"""创建表D：已平仓记录"""
import json
import requests

# 配置
app_id = "cli_a92e4f3c293cdbc8"
app_secret = "WeBHtMOrX7j5u3bnQ4cmnbH0HEg2Nl4y"
app_token = "BJS4bdRWbaN0wZsY1kyco7aInhc"

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

# 1. 创建表D
print("=== 创建表D：已平仓记录 ===")
url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"

response = requests.post(url, headers=headers, json={
    "table": {
        "name": "已平仓记录",
        "default_view_name": "交易历史"
    }
}, timeout=10)

result = response.json()

if result.get("code") == 0:
    table_id = result.get("data", {}).get("table_id")
    print(f"✅ 表D创建成功: {table_id}")

    # 2. 创建字段
    field_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"

    fields = [
        {"field_name": "股票代码", "type": 1},
        {"field_name": "入场日期", "type": 5},
        {"field_name": "离场日期", "type": 5},
        {"field_name": "入场价", "type": 2},
        {"field_name": "离场价", "type": 2},
        {"field_name": "持仓时长", "type": 2},  # 天数
        {"field_name": "盈亏比例", "type": 2},  # 百分比
        {"field_name": "盈亏金额", "type": 2},
        {"field_name": "平仓原因", "type": 3, "property": {"options": [
            {"name": "初始止损(-8%)"},
            {"name": "追踪止盈(-8%)"},
            {"name": "目标止盈(30%)"}
        ]}},
        {"field_name": "备注", "type": 1}
    ]

    print("\n创建字段...")
    for field_config in fields:
        field_name = field_config["field_name"]
        response = requests.post(field_url, headers=headers, json=field_config, timeout=10)
        result = response.json()
        if result.get("code") == 0:
            print(f"  ✅ {field_name}")
        else:
            print(f"  ❌ {field_name}: {result.get('msg')}")

    # 3. 更新配置文件
    with open("config/feishu_bitable.json", "r", encoding="utf-8") as f:
        config = json.load(f)

    config["table_ids"]["closed_positions"] = table_id

    with open("config/feishu_bitable.json", "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

    print(f"\n✅ 配置文件已更新")
    print(f"\n表D (已平仓记录) 创建完成！")

else:
    print(f"❌ 创建失败: {result}")
