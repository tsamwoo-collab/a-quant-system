"""清空3个表的测试数据"""
import json
import requests

# 配置
app_id = "cli_a92e4f3c293cdbc8"
app_secret = "WeBHtMOrX7j5u3bnQ4cmnbH0HEg2Nl4y"
app_token = "BJS4bdRWbaN0wZsY1kyco7aInhc"

# 3个表的 table_id
table_ids = {
    "market_context": "tbleCqTb2JiGGHKw",
    "signal_tracker": "tblNYQLdtUEEGXez",
    "portfolio": "tblSMHGLYLc5ILwS"
}

table_names = {
    "market_context": "每日天气",
    "signal_tracker": "信号追踪",
    "portfolio": "持仓看板"
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

print("正在清空3个表的测试数据...\n")

total_deleted = 0

for table_key, table_id in table_ids.items():
    table_name = table_names[table_key]
    print(f"=== {table_name} ===")

    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"

    deleted = 0
    while True:
        response = requests.get(url, headers=headers, params={"page_size": 100}, timeout=10)
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
                deleted += 1

        if not result.get("data", {}).get("has_more", False):
            break

    print(f"  删除 {deleted} 条记录")
    total_deleted += deleted

print(f"\n✅ 清空完成，共删除 {total_deleted} 条测试数据")
print("\n3个数据表现在是干净的，可以开始使用！")
