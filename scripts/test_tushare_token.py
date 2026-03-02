"""测试 Tushare Token 连接"""
import requests
import json

token = "f8ebd3ef93204454d8115ce5ae1b9a3a055b49c809cdb9690b696c26afc0"

print("=== 测试 Tushare API 连接 ===\n")

# 测试接口：获取股票列表
url = "http://api.tushare.pro"
data = {
    "api_name": "stock_basic",
    "token": token,
    "params": {
        "list_status": "L"  # 上市股票
    },
    "fields": "ts_code,symbol,name,area,industry,list_date"
}

print("请求: stock_basic（股票列表）...")
response = requests.post(url, json=data, timeout=10)
result = response.json()

print(f"响应码: {result.get('code')}")
print(f"响应消息: {result.get('msg')}")

if result.get("code") == 0:
    data = result.get("data", {}).get("items", [])
    print(f"✅ Token 有效！获取到 {len(data)} 只股票")
    print(f"\n示例股票（前5只）:")
    for i, stock in enumerate(data[:5], 1):
        print(f"  {i}. {stock[0]} - {stock[2]}")
else:
    print(f"❌ Token 无效: {result}")
