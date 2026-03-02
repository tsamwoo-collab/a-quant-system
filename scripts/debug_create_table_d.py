"""详细调试表D创建过程"""
import json
import requests

# 配置
app_id = "cli_a92e4f3c293cdbc8"
app_secret = "WeBHtMOrX7j5u3bnQ4cmnbH0HEg2Nl4y"
app_token = "BJS4bdRWbaN0wZsY1kyco7aInhc"

print("=== 获取访问令牌 ===")
token_url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
token_response = requests.post(token_url, json={
    "app_id": app_id,
    "app_secret": app_secret
}, timeout=10)
token_result = token_response.json()

if token_result.get("code") != 0:
    print(f"❌ 获取令牌失败: {token_result}")
    exit(1)

tenant_access_token = token_result.get("tenant_access_token")
print(f"✅ 令牌获取成功\n")

headers = {
    "Authorization": f"Bearer {tenant_access_token}",
    "Content-Type": "application/json"
}

# 先检查现有表格
print("=== 检查现有表格 ===")
list_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"
list_response = requests.get(list_url, headers=headers, timeout=10)
list_result = list_response.json()

print(f"现有表格: {len(list_result.get('data', {}).get('items', []))} 个")
for table in list_result.get("data", {}).get("items", []):
    print(f"  - {table.get('name')} (ID: {table.get('table_id')})")

# 检查是否已存在"已平仓记录"
for table in list_result.get("data", {}).get("items", []):
    if table.get("name") == "已平仓记录":
        table_id = table.get("table_id")
        print(f"\n⚠️  '已平仓记录' 表已存在: {table_id}")
        print("将使用现有表格...")
        break
else:
    print("\n=== 创建表D ===")
    create_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"

    # 尝试不同的请求格式
    payloads = [
        # 格式1: 完整格式
        {
            "table": {
                "name": "已平仓记录",
                "default_view_name": "交易历史"
            }
        },
        # 格式2: 简化格式
        {
            "name": "已平仓记录"
        },
        # 格式3: 只用 table
        {
            "table": {
                "name": "已平仓记录"
            }
        }
    ]

    for i, payload in enumerate(payloads, 1):
        print(f"\n尝试格式 {i}:")
        print(f"请求体: {json.dumps(payload, ensure_ascii=False)}")

        response = requests.post(create_url, headers=headers, json=payload, timeout=10)
        result = response.json()

        print(f"响应码: {result.get('code')}")
        print(f"响应消息: {result.get('msg')}")

        if result.get("code") == 0:
            table_id = result.get("data", {}).get("table_id")
            print(f"✅ 成功! table_id: {table_id}")
            break
        else:
            print(f"❌ 失败")
            if "error" in result:
                print(f"错误详情: {result['error']}")
