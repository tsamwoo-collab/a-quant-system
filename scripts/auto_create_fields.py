"""自动创建所需字段"""
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

# 定义需要创建的字段
fields_to_create = [
    # 基础字段
    {"field_name": "记录类型", "type": 3, "property": {"options": [{"name": "市场环境"}, {"name": "买入信号"}, {"name": "卖出信号"}, {"name": "持仓"}]}},
    {"field_name": "日期", "type": 5},
    {"field_name": "创建时间", "type": 5},

    # 市场环境字段
    {"field_name": "指数收盘", "type": 2},
    {"field_name": "ADX数值", "type": 2},
    {"field_name": "系统状态", "type": 3, "property": {"options": [{"name": "开启(ADX>25)"}, {"name": "静默(ADX<25)"}, {"name": "观望"}]}},
    {"field_name": "备注", "type": 1},

    # 信号字段
    {"field_name": "股票代码", "type": 1},
    {"field_name": "信号类型", "type": 3, "property": {"options": [{"name": "买入信号"}, {"name": "卖出信号"}]}},
    {"field_name": "理论价格", "type": 2},
    {"field_name": "滑点模拟成交价", "type": 2},
    {"field_name": "动能得分", "type": 2},
    {"field_name": "执行状态", "type": 3, "property": {"options": [{"name": "待执行"}, {"name": "已同步至持仓"}, {"name": "已从持仓平盘"}]}},

    # 持仓字段
    {"field_name": "入场日期", "type": 5},
    {"field_name": "初始成本", "type": 2},
    {"field_name": "当前价", "type": 2},
    {"field_name": "持有最高收盘价", "type": 2},
    {"field_name": "当前利润率", "type": 2},
    {"field_name": "最高点回撤", "type": 2},
    {"field_name": "风险状态", "type": 3, "property": {"options": [{"name": "正常"}, {"name": "预警"}, {"name": "强制平仓"}]}}
]

print("开始创建字段...\n")

success_count = 0
skip_count = 0
fail_count = 0

for field_config in fields_to_create:
    field_name = field_config["field_name"]

    print(f"创建字段: {field_name}")
    response = requests.post(create_field_url, headers=headers, json=field_config, timeout=10)
    result = response.json()

    if result.get("code") == 0:
        print(f"  ✅ 成功")
        success_count += 1
    elif result.get("code") == 99991663:
        # 字段已存在
        print(f"  ⚠️  已存在，跳过")
        skip_count += 1
    else:
        print(f"  ❌ 失败: {result.get('msg')}")
        fail_count += 1

print(f"\n=== 创建完成 ===")
print(f"成功: {success_count}")
print(f"跳过: {skip_count}")
print(f"失败: {fail_count}")

if fail_count == 0:
    print("\n✅ 所有字段创建成功！现在可以运行 daily_signals.py")
