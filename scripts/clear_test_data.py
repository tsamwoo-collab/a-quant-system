"""清空测试数据"""
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

# 获取所有记录
url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
params = {"page_size": 100}

print("正在删除测试数据...\n")

deleted_count = 0
while True:
    response = requests.get(url, headers=headers, params=params, timeout=10)
    result = response.json()

    if result.get("code") != 0:
        break

    items = result.get("data", {}).get("items", [])
    if not items:
        break

    for item in items:
        record_id = item.get("record_id")
        delete_url = f"{url}/{record_id}"
        delete_response = requests.delete(delete_url, headers=headers, timeout=10)
        if delete_response.json().get("code") == 0:
            deleted_count += 1

    print(f"已删除 {deleted_count} 条记录...", end="\r")

    if not result.get("data", {}).get("has_more", False):
        break

print(f"\n✅ 清空完成，共删除 {deleted_count} 条测试数据")
