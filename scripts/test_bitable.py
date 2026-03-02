"""测试飞书 Bitable 权限"""
import json
import requests

# 配置
app_id = "cli_a92e4f3c293cdbc8"
app_secret = "WeBHtMOrX7j5u3bnQ4cmnbH0HEg2Nl4y"
app_token = "BJS4bdRWbaN0wZsY1kyco7aInhc"

# 1. 获取 tenant_access_token
print("1. 获取访问令牌...")
token_url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
token_response = requests.post(token_url, json={
    "app_id": app_id,
    "app_secret": app_secret
}, timeout=10)

token_result = token_response.json()
print(f"   响应: {token_result}")

if token_result.get("code") != 0:
    print("❌ 获取令牌失败")
    exit(1)

tenant_access_token = token_result.get("tenant_access_token")
print("✅ 令牌获取成功\n")

# 2. 获取现有表格列表
print("2. 获取现有表格列表...")
headers = {
    "Authorization": f"Bearer {tenant_access_token}",
    "Content-Type": "application/json"
}

tables_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"
tables_response = requests.get(tables_url, headers=headers, timeout=10)
tables_result = tables_response.json()
print(f"   响应: {json.dumps(tables_result, indent=2, ensure_ascii=False)}\n")

if tables_result.get("code") == 0:
    tables = tables_result.get("data", {}).get("items", [])
    print(f"✅ 找到 {len(tables)} 个数据表:")
    for table in tables:
        print(f"   - {table.get('name')} (ID: {table.get('table_id')})")
else:
    print("❌ 获取表格列表失败")

# 3. 尝试创建新表（最简格式）
print("\n3. 尝试创建新表...")
create_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"

# 最简单的创建请求
create_payload = {
    "name": "测试表"
}

print(f"   请求体: {json.dumps(create_payload, ensure_ascii=False)}")
create_response = requests.post(create_url, headers=headers, json=create_payload, timeout=10)
create_result = create_response.json()
print(f"   响应: {json.dumps(create_result, indent=2, ensure_ascii=False)}")

if create_result.get("code") == 0:
    print("✅ 表格创建成功")
else:
    print(f"❌ 表格创建失败: {create_result.get('msg')}")
