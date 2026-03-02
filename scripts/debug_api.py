"""调试飞书 API - 测试不同格式"""
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

create_field_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"

# 测试1: 最简单的文本字段
print("=== 测试1: 简单文本字段 ===")
test1 = {
    "field_name": "测试文本"
}
response = requests.post(create_field_url, headers=headers, json=test1, timeout=10)
print(f"请求: {json.dumps(test1, ensure_ascii=False)}")
print(f"响应: {json.dumps(response.json(), indent=2, ensure_ascii=False)}\n")

# 测试2: 带类型的文本字段
print("=== 测试2: 带类型的文本字段 ===")
test2 = {
    "field_name": "测试文本2",
    "type": 1
}
response = requests.post(create_field_url, headers=headers, json=test2, timeout=10)
print(f"请求: {json.dumps(test2, ensure_ascii=False)}")
print(f"响应: {json.dumps(response.json(), indent=2, ensure_ascii=False)}\n")

# 测试3: 数字字段
print("=== 测试3: 数字字段 ===")
test3 = {
    "field_name": "测试数字",
    "type": 2
}
response = requests.post(create_field_url, headers=headers, json=test3, timeout=10)
print(f"请求: {json.dumps(test3, ensure_ascii=False)}")
print(f"响应: {json.dumps(response.json(), indent=2, ensure_ascii=False)}\n")

# 测试4: 日期字段
print("=== 测试4: 日期字段 ===")
test4 = {
    "field_name": "测试日期",
    "type": 5
}
response = requests.post(create_field_url, headers=headers, json=test4, timeout=10)
print(f"请求: {json.dumps(test4, ensure_ascii=False)}")
print(f"响应: {json.dumps(response.json(), indent=2, ensure_ascii=False)}\n")

# 测试5: 单选字段（带选项）
print("=== 测试5: 单选字段 ===")
test5 = {
    "field_name": "测试单选",
    "type": 3,
    "property": {
        "options": [
            {"name": "选项A"},
            {"name": "选项B"}
        ]
    }
}
response = requests.post(create_field_url, headers=headers, json=test5, timeout=10)
print(f"请求: {json.dumps(test5, ensure_ascii=False)}")
print(f"响应: {json.dumps(response.json(), indent=2, ensure_ascii=False)}\n")
