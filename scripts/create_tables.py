"""尝试创建3个数据表"""
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

url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"

# 尝试不同的请求格式
tables_to_create = [
    {"name": "每日天气（Market Context）"},
    {"name": "信号追踪（Signal Tracker）"},
    {"name": "持仓看板（Portfolio）"}
]

for table_config in tables_to_create:
    print(f"\n=== 创建表: {table_config['name']} ===")

    # 格式1: 直接用 name
    print("尝试格式1: 直接用 name")
    response = requests.post(url, headers=headers, json=table_config, timeout=10)
    result = response.json()
    print(f"响应: {json.dumps(result, ensure_ascii=False)}")

    if result.get("code") == 0:
        table_id = result.get("data", {}).get("table", {}).get("table_id")
        print(f"✅ 成功! table_id: {table_id}")
        continue

    # 格式2: 嵌套在 table 对象中
    print("\n尝试格式2: 嵌套在 table 对象中")
    response = requests.post(url, headers=headers, json={"table": table_config}, timeout=10)
    result = response.json()
    print(f"响应: {json.dumps(result, ensure_ascii=False)}")

    if result.get("code") == 0:
        table_id = result.get("data", {}).get("table", {}).get("table_id")
        print(f"✅ 成功! table_id: {table_id}")
        continue

    # 格式3: 添加 default_view_name
    print("\n尝试格式3: 添加 default_view_name")
    payload = {
        "table": {
            "name": table_config["name"],
            "default_view_name": "表格视图"
        }
    }
    response = requests.post(url, headers=headers, json=payload, timeout=10)
    result = response.json()
    print(f"响应: {json.dumps(result, ensure_ascii=False)}")

    if result.get("code") == 0:
        table_id = result.get("data", {}).get("table", {}).get("table_id")
        print(f"✅ 成功! table_id: {table_id}")
        continue

    print("❌ 所有格式都失败了")

print("\n" + "="*50)
print("如果创建失败，请在飞书中手动创建3个数据表")
