"""测试创建字段"""
import json
import requests

# 配置
app_id = "cli_a92e4f3c293cdbc8"
app_secret = "WeBHtMOrX7j5u3bnQ4cmnbH0HEg2Nl4y"
app_token = "BJS4bdRWbaN0wZsY1kyco7aInhc"
table_id = "tblXQKXD8p96ZXR2"

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

# 创建字段
print("尝试创建字段...")
create_field_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"

# 先尝试创建一个简单的文本字段
test_field = {
    "field_name": "记录类型",
    "type": 1,  # 文本
    "description": "用于区分不同类型的记录（市场环境/信号/持仓）"
}

print(f"创建字段: {test_field['field_name']}")
response = requests.post(create_field_url, headers=headers, json=test_field, timeout=10)
result = response.json()
print(f"响应: {json.dumps(result, indent=2, ensure_ascii=False)}")

if result.get("code") == 0:
    print("✅ 字段创建成功")
else:
    print(f"❌ 字段创建失败: {result.get('msg')}")
    print("\n需要在飞书中手动创建字段")
